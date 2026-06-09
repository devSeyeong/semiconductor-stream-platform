#### Semiconductor Stream Platform

반도체 공정 데이터를 시뮬레이션하고,
Kafka 기반 스트리밍 파이프라인 및 Spark Structured Streaming 분석 환경을 구축하는 프로젝트입니다.

---

### Architecture

```text
Simulator
   ↓
Kafka
   ↓
Consumer  (PCA + Hotelling's T² 실시간 이상탐지)
   ↓
PostgreSQL
   ↓
Streamlit Dashboard
```

---

### Simulator


* Wafer State Machine
* 공정 Step 기반 데이터 생성
* Equipment Degradation
* SECOM 스타일 결측치 및 노이즈
* Defect Probability 계산


---

### Process Flow

```text
DEPOSITION
   ↓
ETCH
   ↓
LITHOGRAPHY
   ↓
INSPECTION
```

---

### Example Event

```json
{
  "wafer_id": "W00010",
  "step": "ETCH",
  "equipment_id": "ETCH-EQ-3",
  "temperature": 81.2,
  "pressure": 1.02,
  "particle_count": 5,
  "defect_probability": 0.013,
  "label": "PASS"
}
```

---

### Python 가상환경(venv)
생성
```bash
python3 -m venv .venv
```
활성화
```bash
source .venv/bin/activate
```

---

### Kafka Python Client 설치

```bash
python3 -m pip install kafka-python
```

---

### Kafka Topic 생성
접속
```bash
docker exec -it <kafka-container> bash
```

Topic생성
```bash
kafka-topics \
--create \
--topic semiconductor-events \
--bootstrap-server localhost:9092
```
Topic확인
```bash
kafka-topics \
--list \
--bootstrap-server localhost:9092
```

---

### PostgreSQL 연결

Kafka Consumer에서 수신한 이벤트를 PostgreSQL에 저장하도록 구성.

Python PostgreSQL Client 설치:

```bash
pip install psycopg2-binary
```
DB 진입
```bash
docker exec -it semiconductor-stream-platform-postgres-1 psql -U admin -d semiconductor
```

DB 연결
```python
    conn = psycopg2.connect(
        host="localhost",
        database="semiconductor",
        user="admin",
        password="admin"
    )
```
Kafka Consumer에서 메시지를 수신한 뒤  
INSERT 쿼리를 통해 semiconductor_events 테이블에 저장.
---

### 이상탐지 (PCA + Hotelling's T²)

반도체 FDC(Fault Detection & Classification)에서 실제로 쓰는 다변량
통계 기법을 적용. 여러 센서를 동시에 보고 평소 패턴에서 벗어난 정도를
하나의 T² 점수로 계산하며, 관리상한(UCL)을 넘으면 이상으로 판정한다.

```text
anomaly/pca_t2.py       # PCA + T² 모델 (numpy 직접 구현)
anomaly/train_model.py  # 스텝별 정상 데이터로 학습 → anomaly/models/*.pkl
anomaly/models/         # 학습된 스텝별 모델
```

스텝마다 센서 구성이 다르므로 **스텝별로 모델을 따로 학습**한다.
3% 결측치는 학습 평균으로 대체하고, 센서별 **기여도**로 어떤 센서가
이상을 일으켰는지 분해해 보여준다.

#### 1) 의존성 설치

```bash
pip install -r requirements.txt
```

#### 2) 모델 학습 (최초 1회)

```bash
.venv/bin/python anomaly/train_model.py
```

#### 3) 파이프라인 실행

```bash
docker compose up -d                              # Kafka + PostgreSQL
.venv/bin/python simulator/simulator.py           # 이벤트 생성
.venv/bin/python consumer/consumer.py             # 실시간 T² 계산 + 적재
.venv/bin/streamlit run dashboard/app.py          # 대시보드 (localhost:8501)
```

대시보드는 3초마다 자동 갱신되며 T² 관리도, 이상 이벤트 목록,
센서 기여도를 실시간으로 보여준다.

---

### Tech Stack

* Python
* Apache Kafka
* PostgreSQL
* NumPy (PCA + Hotelling's T²)
* Streamlit + Plotly (Dashboard)
* Docker
* AWS EC2

---

### TODO

* [x] Kafka Producer
* [x] PostgreSQL Sink
* [x] 이상탐지 (PCA + Hotelling's T²)
* [x] Dashboard
* [ ] Spark Streaming
* [ ] Airflow
* [ ] EC2 Deployment
