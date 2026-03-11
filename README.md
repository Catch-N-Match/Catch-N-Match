# Catch-N-Match

1. 프로젝트 개요

GA4 이벤트 데이터를 기반으로 사용자 행동을 분석하고,
구매 확률 예측 및 이탈 가능성 예측 결과를 생성하는 데이터/ML 파이프라인 프로젝트입니다.

주요 기능은 다음과 같습니다.

GA4 raw 데이터 적재 및 테이블 분리

사용자 단위 feature 생성

구매 예측 / 이탈 예측 모델 학습 및 추론

예측 결과 CSV 생성

대시보드 시각화 연동

2. 디렉토리 구조
CATCH-N-MATCH/
├── credentials/
├── data/
│   └── raw/
│       └── raw_data_with_gmt.csv
├── docker/
│   ├── dags/
│   │   ├── daily_bq_update.py
│   │   ├── daily_ga4_ingestion.py
│   │   └── daily_ml_pipeline.py
│   ├── logs/
│   ├── compose.yml
│   ├── Dockerfile.python
│   └── requirements.txt
├── output/
│   ├── t7_prediction_result.csv
│   └── t8_prediction_trend.csv
├── src/
│   └── ML/
│       ├── models/
│       │   ├── purchase_model.pkl
│       │   └── churn_model.pkl
│       └── ml_predict.py
├── .env.example
└── .gitignore

4. 실행 환경

Docker

Docker Compose

Python 3.11

Airflow

XGBoost / Scikit-learn / Pandas

4. Docker 컨테이너 실행
4-1. 컨테이너 빌드 및 실행
docker compose -f docker/compose.yml up -d --build
4-2. 컨테이너 상태 확인
docker ps
5. ML 추론 실행 방법

구매 예측 결과를 생성하려면 아래 명령어를 실행합니다.

docker exec -it airflow-scheduler bash -lc \
"INPUT_DATA_PATH=/app/data/raw/raw_data_with_gmt.csv \
OUTPUT_DIR=/app/output \
python /app/src/ML/ml_predict.py"
6. 출력 결과 확인

추론 실행 후 /app/output 경로에 결과 파일이 생성됩니다.

생성 파일

t7_prediction_result.csv

t8_prediction_trend.csv

컨테이너 내부에서 확인
docker exec -it airflow-scheduler bash -lc "ls -al /app/output"
결과 파일 미리보기
docker exec -it airflow-scheduler bash -lc "head -n 5 /app/output/t7_prediction_result.csv"

7. 데이터 파일 확인

원본 데이터 파일이 정상적으로 마운트되었는지 확인합니다.

docker exec -it airflow-scheduler bash -lc "ls /app/data/raw"

정상적으로 마운트되면 아래 파일이 보여야 합니다.

raw_data_with_gmt.csv

8. 모델 파일 확인

추론에 필요한 모델 파일이 정상적으로 존재하는지 확인합니다.

docker exec -it airflow-scheduler bash -lc "ls -al /app/src/ML/models"

9. Airflow

Airflow는 배치 파이프라인 실행 및 스케줄링을 위한 오케스트레이션 도구로 사용합니다.

현재 프로젝트에서는 다음과 같은 역할을 수행합니다.

데이터 처리 스케줄링

ML 추론 배치 실행

향후 재학습 파이프라인 확장 가능

※ 세부 DAG 구조 및 운영 로직은 프로젝트 진행 상황에 따라 추가될 수 있습니다.

10. Looker Studio

Looker Studio는 예측 결과 및 집계 결과를 시각화하는 용도로 사용합니다.

활용 예시:

예측 구매자 수

구매 확률 분포

일자별 예측 추이

사용자 세그먼트 요약

※ 상세 대시보드 설정은 별도 시각화 문서 또는 공유 링크 기준으로 관리합니다.

11. 주의사항

raw_data_with_gmt.csv 파일이 /app/data/raw/ 경로에 있어야 합니다.

모델 파일(purchase_model.pkl 등)이 /app/src/ML/models/ 경로에 있어야 합니다.

대용량 데이터 실행 시 WSL / Docker 환경에서 메모리 사용량이 높아질 수 있습니다.

12. 문제 해결
output 파일이 생성되지 않는 경우

데이터 파일 존재 여부 확인

docker exec -it airflow-scheduler bash -lc "ls /app/data/raw"

모델 파일 존재 여부 확인

docker exec -it airflow-scheduler bash -lc "ls -al /app/src/ML/models"

추론 스크립트 직접 실행

docker exec -it airflow-scheduler bash -lc \
"INPUT_DATA_PATH=/app/data/raw/raw_data_with_gmt.csv \
OUTPUT_DIR=/app/output \
python -u /app/src/ML/ml_predict.py"
