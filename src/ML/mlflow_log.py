import pandas as pd
import mlflow
import os
import sys

# MLflow 설정
MLFLOW_URI = "http://172.17.0.1:5000" # "http://mlflow:5000"
EXPERIMENT_NAME = "GA4_Inference_Monitoring"

# 고정 경로
OUTPUT_DIR = "/app/output"
MODEL_DIR = "/app/src/ML/models"

T7_PATH = os.path.join(OUTPUT_DIR, "t7_prediction_result.csv")
T8_PATH = os.path.join(OUTPUT_DIR, "t8_prediction_trend.csv")
PURCHASE_MODEL_PATH = os.path.join(MODEL_DIR, "purchase_model.pkl")
CHURN_MODEL_PATH = os.path.join(MODEL_DIR, "churn_model.pkl")

def safe_log_artifact(path: str):
    print(f"artifact 확인: {path}")
    print("exists:", os.path.exists(path)) 
    
    if os.path.exists(path):
        mlflow.log_artifact(path)
        print(f"✅ artifact 업로드: {path}")
    else:
        print(f"⚠️ artifact 파일 없음: {path}")

def log_to_mlflow(t8_path: str = T8_PATH, t7_path: str = T7_PATH,
                    purchase_model_path: str = PURCHASE_MODEL_PATH,
                    churn_model_path: str = CHURN_MODEL_PATH):

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    if not os.path.exists(t8_path):
        print(f"❌ 요약 파일이 없습니다: {t8_path}")
        return

    # T8 요약 데이터 로드 (매우 가벼움)
    summary_df = pd.read_csv(t8_path)
    if summary_df.empty:
        print("❌ T8 요약 데이터가 비어 있습니다.")
        return
    
    row = summary_df.iloc[0]
    prediction_date = str(row['prediction_date'])

    print(f"🚀 {prediction_date} 데이터 MLflow 로깅 시작...")
    
    try:
        with mlflow.start_run(run_name=f"Inference_{prediction_date}"):
            # 주요 지표 기록
            mlflow.log_metrics({
                "total_users": float(row["total_users"]),
                "predicted_purchasers": float(row["predicted_purchasers"]),
                "predicted_churners": float(row["predicted_churners"]),
                "purchase_rate": float(row["predicted_purchase_rate"]),
                "churn_rate": float(row["predicted_churn_rate"]),
                "avg_p_prob": float(row["avg_purchase_probability"]),
                "avg_c_prob": float(row["avg_churn_probability"]),
                "high_value_count": float(row.get("high_value_count", 0)),
                "high_risk_count": float(row.get("high_risk_count", 0)),
                "actual_sessions": float(row.get("actual_sessions", 0)),
                "actual_revenue": float(row.get("actual_revenue", 0)),
                "trend_purchase_rate": float(row.get("trend_purchase_rate", 0)),
                "trend_churn_rate": float(row.get("trend_churn_rate", 0)),
            })

            # 파라미터 기록
            mlflow.log_params({
                "t7_path": t7_path,
                "t8_path": t8_path,
                "purchase_model_path": purchase_model_path,
                "churn_model_path": churn_model_path,
            })
            
            # 태그 설정
            mlflow.set_tag("prediction_date", prediction_date)
            mlflow.set_tag("run_type", "inference")
            mlflow.set_tag("source", "daily_ml_pipeline")

            # artifact 업로드
            safe_log_artifact(t7_path)
            safe_log_artifact(t8_path)
            safe_log_artifact(purchase_model_path)
            safe_log_artifact(churn_model_path)
            
            print(f"✅ MLflow 로깅 완료 (Run ID: {mlflow.active_run().info.run_id})")
    
    except Exception as e:
        print(f"⚠️ MLflow 로깅 중 에러 발생: {e}")

if __name__ == "__main__":
    # 인자 없으면 고정 경로 사용
    # 1번째 인자만 오면 t8 경로로 간주
    if len(sys.argv) > 1:
        log_to_mlflow(t8_path=sys.argv[1])
    else:
        log_to_mlflow()