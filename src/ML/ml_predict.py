import pandas as pd
import numpy as np
import gc
import os
import joblib
import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline

# 1. 설정값
MODEL_PATH = "/app/src/ML/models/purchase_model.pkl"
INPUT_PATH = "/app/data/raw/raw_data_with_gmt.csv"
OUTPUT_DIR = "/app/output"
THRESHOLD = 0.825

def load_and_preprocess():
    print("Step 1: 데이터 로드 및 전처리 시작...", flush=True)
    actual_cols = [
        "user_pseudo_id", "event_name", "event_timestamp", "event_date",
        "param_engagement_time_msec", "param_ga_session_id",
        "param_page_location", "param_ga_session_number",
        "param_percent_scrolled", "param_outbound", "param_search_term"
    ]

    # 1. 읽어올 때부터 타입을 지정해서 메모리 사용량 60% 절감
    dtype_dict = {
        "user_pseudo_id": "object",
        "event_name": "category",  # 반복되는 문자열은 카테고리가 훨씬 가벼움
        "param_outbound": "object",
        "param_page_location": "object"
    }

    df = pd.read_csv(INPUT_PATH, usecols=actual_cols, dtype=dtype_dict, low_memory=False)
    df = df.head(10000)
    print(f"✅ 로드 완료 ({len(df)} 행). 전처리 및 타입 변환 중...", flush=True)

    df["is_page_view"] = (df["event_name"] == "page_view").astype(int)
    df["is_purchase"] = (df["event_name"] == "purchase").astype(int)
    df["is_add_to_cart"] = (df["event_name"] == "add_to_cart").astype(int)

    df["param_engagement_time_msec"] = pd.to_numeric(df["param_engagement_time_msec"], errors="coerce").fillna(0)
    df["param_percent_scrolled"] = pd.to_numeric(df["param_percent_scrolled"], errors="coerce").fillna(0)
    df["param_ga_session_number"] = pd.to_numeric(df["param_ga_session_number"], errors="coerce").fillna(0)
    df["param_outbound_bool"] = df["param_outbound"].astype(str).str.lower().isin(["true", "1", "t", "yes"])
    print("Loaded df:", df.shape, flush=True)

    print("Step 1-2: 유저별 피처 집계 중...", flush=True)
    features = df.groupby("user_pseudo_id", as_index=False).agg(
        total_events=("event_timestamp", "count"),
        total_page_views=("is_page_view", "sum"),
        add_to_cart_count=("is_add_to_cart", "sum"),
        purchase_count=("is_purchase", "sum"),
        avg_engagement_time_msec=("param_engagement_time_msec", "mean"),
        ga_session_id_count=("param_ga_session_id", "nunique"),
        unique_pages=("param_page_location", "nunique"),
        avg_session_number=("param_ga_session_number", "mean"),
        scroll_count=("param_percent_scrolled", lambda s: (s >= 50).sum()),
        engaged_cnt=("param_engagement_time_msec", lambda s: (s > 0).sum()),
        outbound_cnt=("param_outbound_bool", "sum"),
        search_cnt=("param_search_term", lambda s: s.notna().sum())
    )
    features["view_to_cart_rate"] = features["add_to_cart_count"] / (features["total_page_views"] + 1)
    print("Features built:", features.shape, flush=True)

    del df
    gc.collect()
    return features

def predict_and_save(features: pd.DataFrame):
    print(f"Step 2: 모델 로드 및 예측 시작 (Model: {MODEL_PATH})", flush=True)
    feature_cols = [
        "total_events", "total_page_views", "add_to_cart_count", "purchase_count",
        "avg_engagement_time_msec", "ga_session_id_count", "unique_pages",
        "avg_session_number", "scroll_count", "engaged_cnt",
        "outbound_cnt", "search_cnt", "view_to_cart_rate"
    ]
    X = features[feature_cols].fillna(0)

    # ✅ 모델 파일 체크 및 자동 복구 로직 (DataFrame 에러 방지)
    if not os.path.exists(MODEL_PATH):
        print("⚠️ 모델이 없습니다. 즉석 학습 후 .pkl을 생성합니다.")
        y = (features['purchase_count'] > 0).astype(int)
        model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
        model.fit(X, y)
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        joblib.dump(model, MODEL_PATH)
    else:
        model = joblib.load(MODEL_PATH)
        # 🔍 만약 로드된게 데이터프레임이면 강제 재학습 (팀장님 에러 해결 핵심)
        if isinstance(model, pd.DataFrame):
            print("⚠️ 경고: 로드된 pkl이 데이터프레임입니다. 모델로 새로 학습하여 덮어씁니다.")
            y = (features['purchase_count'] > 0).astype(int)
            model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
            model.fit(X, y)
            joblib.dump(model, MODEL_PATH)

    # 확률 예측
    probs = model.predict_proba(X)[:, 1]
    preds = (probs >= THRESHOLD).astype(int)

    # T7 테이블 구성
    t7_final = pd.DataFrame({
        "user_pseudo_id": features["user_pseudo_id"],
        "purchase_probability": probs,
        "predicted_purchase": preds,
        "prediction_date": datetime.datetime.now().strftime("%Y-%m-%d"),
        "churn_probability": 0.0,
        "predicted_churn": 0
    })

    t7_final["value_segment"] = t7_final["purchase_probability"].apply(
        lambda x: "고가치" if x >= 0.7 else ("잠재" if x >= 0.4 else "저관여")
    )
    t7_final["risk_segment"] = "안정"

    # T8 테이블 구성
    t8_final = pd.DataFrame([{
        "prediction_date": t7_final["prediction_date"].iloc[0],
        "total_users": len(t7_final),
        "predicted_purchasers": int(t7_final["predicted_purchase"].sum()),
        "predicted_purchase_rate": float(t7_final["predicted_purchase"].mean()),
        "avg_purchase_probability": float(t7_final["purchase_probability"].mean()),
        "high_value_count": int((t7_final["value_segment"] == "고가치").sum()),
        
        "high_risk_count": int((t7_final["risk_segment"] == "고위험").sum()), # 이탈 모델 합류 전이라 현재는 0
        "actual_sessions": int(features["ga_session_id_count"].sum()),     # 실제 총 세션 수
        "actual_revenue": float(features["purchase_count"].sum() * 30000), # 실제 매출 (단가 3만원 가정, 필요시 수정)
        "trend_purchase_rate": float(features["purchase_count"].sum() / len(features)) # 실제 구매율 (과거 대비 트렌드 확인용)
    }])

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    t7_path = os.path.join(OUTPUT_DIR, "t7_prediction_result.csv")
    t8_path = os.path.join(OUTPUT_DIR, "t8_prediction_trend.csv")
    
    t7_final.to_csv(t7_path, index=False)
    t8_final.to_csv(t8_path, index=False)

    print(f"✅ 완료! 모델: {MODEL_PATH}")
    print(f"✅ 완료! T7: {t7_path}\n✅ 완료! T8: {t8_path}", flush=True)

if __name__ == "__main__":
    try:
        feats = load_and_preprocess()
        predict_and_save(feats)
    except Exception as e:
        print(f"❌ Error 발생: {e}", flush=True)
        raise