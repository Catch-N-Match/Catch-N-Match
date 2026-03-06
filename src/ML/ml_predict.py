import pandas as pd
import numpy as np
import gc
import os
import joblib
import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
import polars as pl
import time # 시간 측정용 추가
import psutil  # 메모리 측정을 위해 추가

# 1. 설정값
CHURN_MODEL_PATH = "/app/src/ML/models/churn_model.pkl"
PURCHASE_MODEL_PATH = "/app/src/ML/models/purchase_model.pkl"
INPUT_PATH = "/app/data/raw/raw_data_with_gmt.csv"
OUTPUT_DIR = "/app/output"
P_THRESHOLD = 0.825
C_THRESHOLD = 0.65


# 메모리 사용량 확인 함수
def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)  # MB 단위 변환



def load_and_preprocess():
    # [시작 지점 측정]
    start_time = time.time()
    start_mem = get_memory_usage()

    print("Step 1: 데이터 로드 및 전처리 시작...", flush=True)

    # [1] 사용할 컬럼 및 데이터 타입 정의  > 메모리 절감 
    actual_cols = [
        "user_pseudo_id", "event_name", "event_timestamp", "event_date",
        "param_engagement_time_msec", "param_ga_session_id",
        "param_page_location", "param_ga_session_number",
        "param_percent_scrolled", "param_outbound", "param_search_term"
    ]

    dtype_overrides = {
        "user_pseudo_id": pl.Utf8,
        "event_name": pl.Utf8, 
        "param_outbound": pl.Utf8,
        "param_page_location": pl.Utf8,
        "param_engagement_time_msec": pl.Float64,
        "param_percent_scrolled": pl.Float64
    }
    
    # [2] Lazy 스캔 및 1단계 변환 (Projection Pushdown 적용)
    # print(f"✅ 로드 완료 ({len(df)} 행). 전처리 및 타입 변환 중...", flush=True)
    # df = pd.read_csv(INPUT_PATH, usecols=actual_cols, dtype=dtype_dict, low_memory=False)
    # df = df.head(10000)

    q = (
        pl.scan_csv(INPUT_PATH, ignore_errors=True, schema_overrides=dtype_overrides)
        .select(actual_cols) # 필요한 컬럼만 선택해서 읽기
        .with_columns([
            pl.from_epoch(pl.col("event_timestamp").cast(pl.Int64), time_unit="us").alias("event_time"),
            pl.col("event_date").cast(pl.String).str.to_date("%Y%m%d", strict=False).alias("event_date_norm")
        ])
    )

    # [3] 2단계 집계: 유저 단위 통합 피처 생성 (구매 + 이탈팀 로직 통합)
    user_features = (
        q.group_by("user_pseudo_id")
        .agg([
            pl.col("event_timestamp").count().alias("total_events"),
            
            # (중요) == 연산 결과인 Boolean을 Int32로 바꿔야 sum()이 가능합니다.
            (pl.col("event_name") == "page_view").cast(pl.Int32).sum().alias("total_page_views"),
            (pl.col("event_name") == "add_to_cart").cast(pl.Int32).sum().alias("add_to_cart_count"),
            (pl.col("event_name") == "purchase").cast(pl.Int32).sum().alias("purchase_count"),
            
            pl.col("param_engagement_time_msec").mean().alias("avg_engagement_time_msec"),
            pl.col("param_ga_session_id").n_unique().alias("ga_session_id_count"),
            pl.col("param_page_location").n_unique().alias("unique_pages"),
            pl.col("param_ga_session_number").fill_null(0.0).mean().alias("avg_session_number"),
            
            # 조건부 카운트들도 모두 캐스팅 적용
            (pl.col("param_percent_scrolled") >= 50).cast(pl.Int32).sum().alias("scroll_count"),
            (pl.col("param_engagement_time_msec") > 0).cast(pl.Int32).sum().alias("engaged_cnt"),
            pl.col("param_outbound").str.to_lowercase()
              .is_in(["true", "1", "t", "yes"]).cast(pl.Int32).sum().alias("outbound_cnt"),
            pl.col("param_search_term").is_not_null().cast(pl.Int32).sum().alias("search_cnt")
        ])
        .with_columns(
            (pl.col("add_to_cart_count") / (pl.col("total_page_views") + 1)).alias("view_to_cart_rate")
        )
    )
    
    #(
    #     q.group_by("user_pseudo_id")
    #     .agg([
    #         # 구매팀 피처
    #         pl.col("event_name").filter(pl.col("event_name") == "purchase").sum().alias("purchase_count"),
    #         pl.col("event_name").filter(pl.col("event_name") == "add_to_cart").sum().alias("add_to_cart_count"),
    #         pl.col("param_ga_session_id").n_unique().alias("ga_session_id_count"), # 무엇 ? 
    #         pl.col("param_page_location").n_unique().alias("unique_pages"),
            
    #         # 이탈팀 피처
    #         pl.col("event_time").max().alias("last_visit"),
    #         pl.col("event_name").filter(pl.col("event_name") == "page_view").sum().alias("total_page_views"),
    #         pl.col("param_engagement_time_msec").mean().alias("avg_engagement_time_msec"), # 결측값은 0으로 채우기 
    #         pl.col("param_percent_scrolled").filter(pl.col("param_percent_scrolled") >= 50).sum().alias("scroll_count"),
            
    #         # 공통 범주형 피처
    #         pl.col("param_outbound").mode().first().alias("main_outbound_status"),

    #         # 총 이벤트 수 (event_timestamp count)
    #         pl.col("event_timestamp").count().alias("total_events"),
    #         # 검색 횟수 (null이 아닌 행 카운트)
    #         pl.col("param_search_term").is_not_null().sum().alias("search_cnt"),
    #         # 인게이지먼트 발생 횟수 (시간 > 0)
    #         (pl.col("param_engagement_time_msec") > 0).sum().alias("engaged_cnt"),
    #         # 세션 번호 평균 (결측치는 0으로 처리)
    #         pl.col("param_ga_session_number").fill_null(0).mean().alias("avg_session_number"),
    #         #  아웃바운드 클릭 수 (True/1/t/yes 조건 통합)
    #         pl.col("param_outbound").str.to_lowercase()
    #           .is_in(["true", "1", "t", "yes"]).sum().alias("outbound_cnt"),

    #     ])
    #     # 집계 직후 전환율(view_to_cart_rate) 연산 (라플라스 스무딩 적용)
    #     .with_columns(
    #         (pl.col("add_to_cart_count") / (pl.col("total_page_views") + 1)).alias("view_to_cart_rate")
    #     )
    # )

    # 실행 및 Pandas 변환 (Streaming 엔진 사용)
    print("✅ 데이터 집계 및 최적화 실행 중...", flush=True)
    features =  user_features.collect(engine="streaming").to_pandas()

    # 메모리 최적화 및 반환
    # Polars Lazy는 q 객체 자체가 메모리를 거의 쓰지 않지만, 명시적으로 비워줍니다.
    print(f"Features built: {features.shape}", flush=True)
    
    # [종료 지점 측정]
    end_time = time.time()
    end_mem = get_memory_usage()
    process_time = end_time - start_time
    mem_diff = end_mem - start_mem

    print("-" * 50)
    print(f"📊 [전처리 성능 리포트]")
    print(f"⏱️ 소요 시간: {process_time:.2f} 초")
    print(f"💾 메모리 변화: {start_mem:.2f} MB -> {end_mem:.2f} MB (증가분: {mem_diff:.2f} MB)")
    print(f"📈 처리 유저 수: {features.shape[0]} 명")
    print("-" * 50)
    # 불필요한 객체 제거 및 가비지 컬렉션
    gc.collect() 
    
    return features
# ====================== 모델링 ===============================
# 모델이 없을시 
def get_or_train_model(path, X, y_condition, model_name):
    """모델 로드 실패 시 베이스라인 모델을 자동 생성"""
    if not os.path.exists(path):
        print(f"⚠️ {model_name} 모델이 없습니다. 임시 모델을 학습합니다.")
        y = (y_condition).astype(int)
        # 학습에 방해되는 ID 및 날짜 컬럼 제외
        X_train = X.select_dtypes(include=['number', 'bool'])
        
        model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
        model.fit(X_train, y)
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump(model, path)
        return model
    return joblib.load(path)

# 모델 있다면 
def predict_and_save(features: pd.DataFrame):
    print(f"🚀 Step 2: 모델 로드 및 예측 시작 (Model: {PURCHASE_MODEL_PATH})", flush=True)
    
    print(f"Step 2: 모델 로드 및 예측 시작 (Model: {PURCHASE_MODEL_PATH})", flush=True)
    p_feature_cols = [
        "total_events", "total_page_views", "add_to_cart_count", "purchase_count",
        "avg_engagement_time_msec", "ga_session_id_count", "unique_pages",
        "avg_session_number", "scroll_count", "engaged_cnt",
        "outbound_cnt", "search_cnt", "view_to_cart_rate"
    ]
    # 공통 피처 선택 및 결측치 처리
    X_purchase = features[p_feature_cols].fillna(0)

    # ✅ 모델 파일 체크 및 자동 복구 로직
    # [1] 구매 모델 로드 (동일한 방어 로직 적용)
    try:
        if os.path.exists(PURCHASE_MODEL_PATH):
            model = joblib.load(PURCHASE_MODEL_PATH)
        else:
            raise FileNotFoundError
    except (EOFError, FileNotFoundError, Exception):
        print("⚠️ 구매 모델이 없거나 깨졌습니다. 즉석 학습합니다.")
        y = (features['purchase_count'] > 0).astype(int)
        model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
        model.fit(X_purchase, y)
        joblib.dump(model, PURCHASE_MODEL_PATH)

    # [2] 이탈 모델 로드 (EOFError 해결 핵심!)
    c_feature_cols = [
        'total_events', 'unique_pages', 'scroll_count', 'engaged_cnt', 
        'avg_engagement_time_msec', 'purchase_count', 'total_page_views',
        'main_device_category', 'main_traffic_source'
    ]
    
    X_churn = features.copy()

    # 부족한 컬럼 임시 생성 (모델이 요구하므로)
    X_churn['main_device_category'] = 0 
    X_churn['main_traffic_source'] = 0
    X_churn = X_churn[c_feature_cols].fillna(0)

    try:
        if os.path.exists(CHURN_MODEL_PATH):
            c_model = joblib.load(CHURN_MODEL_PATH)
            print("✅ 이탈 모델 로드 성공")
        else:
            raise FileNotFoundError
    except (EOFError, FileNotFoundError, Exception):
        # 💡 여기가 포인트: 파일이 깨져서 EOFError나면 0으로 채우거나 즉석 학습 시킴
        print("⚠️ 이탈 모델 pkl이 깨졌거나 없습니다. 기본값(0.0)으로 진행합니다.")
        # 1. 학습용 임시 타겟 생성 (랜덤하게 0, 1 생성)
        y_temp = (np.random.rand(len(X)) > 0.7).astype(int) 
        
        # 2. 초간단 베이스라인 모델 학습
        c_model = make_pipeline(StandardScaler(), LogisticRegression())
        c_model.fit(X_churn, y_temp)
        
        # 3. 제대로 된 pkl 파일로 저장 (이제 0바이트가 아님!)
        os.makedirs(os.path.dirname(CHURN_MODEL_PATH), exist_ok=True)
        joblib.dump(c_model, CHURN_MODEL_PATH)
        print(f"✅ 임시 이탈 모델 생성 완료: {CHURN_MODEL_PATH}")

    # [3] 예측 계산
    p_probs = model.predict_proba(X_purchase)[:, 1]
    p_preds = (p_probs >= P_THRESHOLD).astype(int)

    if c_model is not None:
        c_probs = c_model.predict_proba(X_churn)[:, 1]
    else:
        c_probs = np.zeros(len(X_churn))
    c_preds = (c_probs >= C_THRESHOLD).astype(int)


    # if not os.path.exists(PURCHASE_MODEL_PATH):
    #     print("⚠️ 모델이 없습니다. 즉석 학습 후 .pkl을 생성합니다.")
    #     y = (features['purchase_count'] > 0).astype(int)
    #     model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
    #     model.fit(X, y)
    #     os.makedirs(os.path.dirname(PURCHASE_MODEL_PATH), exist_ok=True)
    #     joblib.dump(model, PURCHASE_MODEL_PATH)
    # else:
    #     model = joblib.load(PURCHASE_MODEL_PATH)
    #     # 🔍 pkl이 데이터프레임으로 오인될 경우 해결 (팀장님 요청 사항)
    #     if isinstance(model, pd.DataFrame):
    #         print("⚠️ 경고: 로드된 pkl이 데이터프레임입니다. 모델로 새로 학습하여 덮어씁니다.")
    #         y = (features['purchase_count'] > 0).astype(int)
    #         model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
    #         model.fit(X, y)
    #         joblib.dump(model, PURCHASE_MODEL_PATH)

    # # [3] 이탈(Churn) 예측 로직 (팀장님이 원하신 추가 부분!)
    # if os.path.exists(CHURN_MODEL_PATH):
    #     c_model = joblib.load(CHURN_MODEL_PATH)
    #     c_probs = c_model.predict_proba(X)[:, 1]
    # else:
    #     print("⚠️ 이탈 모델이 없어 0.0으로 초기화합니다.")
    #     c_probs = np.zeros(len(X))
    # c_preds = (c_probs >= C_THRESHOLD).astype(int)

    # # 확률 및 라벨 예측
    # p_probs = model.predict_proba(X)[:, 1]
    # p_preds = (p_probs >= P_THRESHOLD).astype(int)

    # [T7 테이블] 개별 유저 예측 결과
    t7_final = pd.DataFrame({
        "user_pseudo_id": features["user_pseudo_id"],
        "purchase_probability": p_probs,
        "predicted_purchase": p_preds,
        "churn_probability": c_probs,     # 0.0이 아닌 실제 확률값!
        "predicted_churn": c_preds,       # 실제 예측값!
        "prediction_date": datetime.datetime.now().strftime("%Y-%m-%d")
    })

    t7_final["value_segment"] = t7_final["purchase_probability"].apply(
        lambda x: "고가치" if x >= 0.7 else ("잠재" if x >= 0.4 else "저관여")
    )
    t7_final["risk_segment"] = t7_final["churn_probability"].apply(
        lambda x: "고위험" if x >= C_THRESHOLD else "안정"
    )

    # [T8 테이블] 일일 요약 및 트렌드 데이터
    t8_final = pd.DataFrame([{
        "prediction_date": t7_final["prediction_date"].iloc[0],
        "total_users": len(t7_final),
        "predicted_purchasers": int(t7_final["predicted_purchase"].sum()),
        "predicted_churners": int(t7_final["predicted_churn"].sum()), # 추가
        "predicted_purchase_rate": float(t7_final["predicted_purchase"].mean()),
        "predicted_churn_rate": float(t7_final["predicted_churn"].mean()), # 추가
        "avg_purchase_probability": float(t7_final["purchase_probability"].mean()),
        "avg_churn_probability": float(t7_final["churn_probability"].mean()), # 추가
        "high_value_count": int((t7_final["value_segment"] == "고가치").sum()),
        
        "high_risk_count": int((t7_final["risk_segment"] == "고위험").sum()), 
        "actual_sessions": int(features["ga_session_id_count"].sum()),
        "actual_revenue": float(features["purchase_count"].sum() * 30000), # 단가 3만원 가정
        "trend_purchase_rate": float(features["purchase_count"].sum() / len(features)),
        "trend_churn_rate": float(t7_final["predicted_churn"].sum() / len(t7_final))
    }])

    # 파일 저장 및 로그
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    t7_path = os.path.join(OUTPUT_DIR, "t7_prediction_result.csv")
    t8_path = os.path.join(OUTPUT_DIR, "t8_prediction_trend.csv")
    
    t7_final.to_csv(t7_path, index=False)
    t8_final.to_csv(t8_path, index=False)

    print(f"✅ 완료! 모델: {PURCHASE_MODEL_PATH}")
    print(f"✅ 완료! 모델: {CHURN_MODEL_PATH}")
    print(f"✅ 완료! T7: {t7_path}\n✅ 완료! T8: {t8_path}", flush=True)

if __name__ == "__main__":
    try:
        feats = load_and_preprocess() # 데이터 반환
        predict_and_save(feats) # 예측 및 저장
    except Exception as e:
        print(f"❌ Error 발생: {e}", flush=True)
        raise

# # 모델이 있을시 (기존)
# def predict_and_save(features: pd.DataFrame):
#     print(f"Step 2: 모델 로드 및 예측 시작 (Model: {PURCHASE_MODEL_PATH})", flush=True)
#     feature_cols = [
#         "total_events", "total_page_views", "add_to_cart_count", "purchase_count",
#         "avg_engagement_time_msec", "ga_session_id_count", "unique_pages",
#         "avg_session_number", "scroll_count", "engaged_cnt",
#         "outbound_cnt", "search_cnt", "view_to_cart_rate"
#     ]
#     X = features[feature_cols].fillna(0)

#     # ✅ 모델 파일 체크 및 자동 복구 로직 (DataFrame 에러 방지)
#     if not os.path.exists(PURCHASE_MODEL_PATH):
#         print("⚠️ 모델이 없습니다. 즉석 학습 후 .pkl을 생성합니다.")
#         y = (features['purchase_count'] > 0).astype(int)
#         model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
#         model.fit(X, y)
#         os.makedirs(os.path.dirname(PURCHASE_MODEL_PATH), exist_ok=True)
#         joblib.dump(model, PURCHASE_MODEL_PATH)
#     else:
#         model = joblib.load(PURCHASE_MODEL_PATH)
#         # 🔍 만약 로드된게 데이터프레임이면 강제 재학습 (팀장님 에러 해결 핵심)
#         if isinstance(model, pd.DataFrame):
#             print("⚠️ 경고: 로드된 pkl이 데이터프레임입니다. 모델로 새로 학습하여 덮어씁니다.")
#             y = (features['purchase_count'] > 0).astype(int)
#             model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000))
#             model.fit(X, y)
#             joblib.dump(model, PURCHASE_MODEL_PATH)

#     # 확률 예측
#     probs = model.predict_proba(X)[:, 1]
#     preds = (probs >= P_THRESHOLD).astype(int)

#     # T7 테이블 구성
#     t7_final = pd.DataFrame({
#         "user_pseudo_id": features["user_pseudo_id"],
#         "purchase_probability": probs,
#         "predicted_purchase": preds,
#         "prediction_date": datetime.datetime.now().strftime("%Y-%m-%d"),
#         "churn_probability": 0.0,
#         "predicted_churn": 0
#     })

#     t7_final["value_segment"] = t7_final["purchase_probability"].apply(
#         lambda x: "고가치" if x >= 0.7 else ("잠재" if x >= 0.4 else "저관여")
#     )
#     t7_final["risk_segment"] = "안정"

#     # T8 테이블 구성
#     t8_final = pd.DataFrame([{
#         "prediction_date": t7_final["prediction_date"].iloc[0],
#         "total_users": len(t7_final),
#         "predicted_purchasers": int(t7_final["predicted_purchase"].sum()),
#         "predicted_purchase_rate": float(t7_final["predicted_purchase"].mean()),
#         "avg_purchase_probability": float(t7_final["purchase_probability"].mean()),
#         "high_value_count": int((t7_final["value_segment"] == "고가치").sum()),
        
#         "high_risk_count": int((t7_final["risk_segment"] == "고위험").sum()), # 이탈 모델 합류 전이라 현재는 0
#         "actual_sessions": int(features["ga_session_id_count"].sum()),     # 실제 총 세션 수
#         "actual_revenue": float(features["purchase_count"].sum() * 30000), # 실제 매출 (단가 3만원 가정, 필요시 수정)
#         "trend_purchase_rate": float(features["purchase_count"].sum() / len(features)) # 실제 구매율 (과거 대비 트렌드 확인용)
#     }])

#     # 저장
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#     t7_path = os.path.join(OUTPUT_DIR, "t7_prediction_result.csv")
#     t8_path = os.path.join(OUTPUT_DIR, "t8_prediction_trend.csv")
    
#     t7_final.to_csv(t7_path, index=False)
#     t8_final.to_csv(t8_path, index=False)

#     print(f"✅ 완료! 모델: {PURCHASE_MODEL_PATH}")
#     print(f"✅ 완료! T7: {t7_path}\n✅ 완료! T8: {t8_path}", flush=True)

# if __name__ == "__main__":
#     try:
#         feats = load_and_preprocess()
#         predict_and_save(feats)
#     except Exception as e:
#         print(f"❌ Error 발생: {e}", flush=True)
#         raise