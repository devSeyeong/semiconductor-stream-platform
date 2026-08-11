# Semiconductor Stream Platform

> 반도체 공정(FDC) 데이터를 **실시간으로 수집·이상탐지·추론**하는 스트리밍 데이터 플랫폼.
> Kafka 스트리밍 파이프라인 위에 다변량 통계 이상탐지 → 그래프 DB → 온톨로지 → LLM(GraphRAG)까지
> 4개 레이어를 쌓아, "센서 이상"을 "근본원인 진단"까지 자동으로 잇는 것을 목표로 한 개인 프로젝트.

- **기간 / 형태**: 개인 프로젝트 (2025.05 ~ )
- **역할**: 데이터 파이프라인 설계 · 이상탐지 알고리즘 구현 · 그래프/온톨로지/LLM 레이어 · 배치 오케스트레이션 — 전 과정 단독
- **규모**: Python 약 2,000 LOC, 8개 서비스(Docker Compose), 6개 처리 단계
- **핵심 키워드**: `Kafka` `Spark Structured Streaming` `PCA + Hotelling's T²` `Neo4j` `OWL 온톨로지` `Claude GraphRAG` `Airflow`

---

## 1. 문제 정의

반도체 FDC(Fault Detection & Classification)의 실제 현장 질문은 대부분 **단변량 임계값으로는 풀리지 않고, "관계 추적"이 필요**하다.

- 여러 센서가 *동시에* 미묘하게 흔들릴 때 이상을 어떻게 하나의 지표로 잡는가?
- FAIL 난 웨이퍼들이 **공통으로 거쳐간 장비**는? (commonality analysis)
- 특정 센서 이상이 **어떤 고장모드·근본원인**을 의미하고, 무슨 조치를 해야 하는가?

이 질문들을 실시간 스트리밍 파이프라인 하나로 관통해 보는 것이 프로젝트의 목표였다.

---

## 2. 시스템 아키텍처

```text
 Simulator ──▶ Kafka ──▶ Consumer (PCA + Hotelling's T² 실시간 이상탐지)
 (웨이퍼         │              ├──────────────────┐
  상태머신)      │              ▼                  ▼
                │          PostgreSQL           Neo4j  ◀── Stage 1: 사실(그래프)
                │              │                  │
                │          Streamlit          OWL 온톨로지 ◀── Stage 2: 의미(추론)
                │          Dashboard              │
                │          (T² 관리도)         Claude LLM  ◀── Stage 3: 자연어(GraphRAG)
                │
          Airflow ──▶ 일일 배치(품질검사·재학습·그래프동기화·이상률 리포트)
```

**설계 원칙**: 실시간 스트리밍(Kafka→Consumer→PostgreSQL)이 본체이고, 이후 추가한 그래프·온톨로지·LLM·Airflow는 모두 **비침습(non-invasive) 애드온**. 새 레이어가 죽어도 본체 파이프라인은 그대로 동작하도록 **장애를 격리**했다.

---

## 3. 핵심 기능 & 기술 하이라이트

### ① 공정 시뮬레이터 — 현실적인 결함 데이터 생성
- **Wafer State Machine**으로 DEPOSITION → ETCH → LITHOGRAPHY → INSPECTION 공정 흐름 재현
- **장비 열화(degradation) 모델**: 장비 health_score가 시간에 따라 서서히 감소 → 시간이 갈수록 결함 확률 증가
- SECOM 데이터셋 스타일의 **결측치·노이즈**, 공정 스텝별 서로 다른 센서 구성

### ② 실시간 이상탐지 — PCA + Hotelling's T² (numpy 직접 구현)
scikit-learn 없이 **numpy만으로 다변량 통계 이상탐지를 직접 구현**해 알고리즘을 완전히 통제했다.
- 센서값 표준화 → 공분산 고유분해(PCA) → 누적 설명분산 90%로 주성분 개수 자동 선택
- Hotelling's `T² = zᵀ(PᵀΛ⁻¹P)z` 로 여러 센서를 **하나의 이탈 점수**로 통합, 학습 분포의 (1-α) 분위수를 관리상한(UCL)으로 사용
- **센서별 기여도 분해**: 어떤 센서가 T²를 끌어올렸는지 비율로 역산 → `top_contributor` 제공
- 스텝마다 센서 구성이 다르므로 **스텝별로 모델을 개별 학습**(`DEPOSITION/ETCH/LITHOGRAPHY/INSPECTION.pkl`)

### ③ Spark Structured Streaming — 분산 처리로 확장
- 단건 `kafka-python` 루프를 **Spark Structured Streaming**으로 이식
- `groupBy(step).applyInPandas(...)`로 스텝별 스코어링을 **워커에 분산**, 모델은 드라이버에서 로드 후 `broadcast`
- 이상탐지 코어(`score_one`)를 **그대로 재사용**해 단건 consumer와 결과 100% 동일 → 대시보드는 무수정으로 동일 테이블을 읽음

### ④ 실시간 대시보드 — Streamlit + Plotly
- 3초 자동 갱신 T² 관리도(UCL 라인), 이상 이벤트 목록, 센서 기여도 시각화

### ⑤ 그래프 + 온톨로지 + LLM — 3단 GraphRAG
관계형 self-join으로는 지옥인 계보/commonality 질의를 **그래프 한 줄 쿼리**로 전환하고, 그 위에 의미와 자연어를 얹었다.
- **Stage 1 — Neo4j 프로퍼티 그래프**: `(:Wafer)-[:HAS_RUN]->(:Run)-[:ON_EQUIPMENT]->(:Equipment)` 등으로 계보·commonality·fault propagation을 모델링. `run_key` 기반 **멱등 MERGE**로 중복 방지, backfill 안전
- **Stage 2 — OWL 온톨로지**: rdflib + owlrl 추론으로 "temperature 센서 이상 → ThermalDrift 고장모드 → 히터 이상(근본원인) → 히터 캘리브레이션(권장조치)"을 **도메인 의미로 번역** (센서 12종 → 고장모드 9종 → 근본원인 8종)
- **Stage 3 — Claude(Opus 4.8) GraphRAG**: Anthropic SDK `tool_runner`로 자연어 질의 → **읽기전용 Cypher** 실행 → RCA 내러티브 생성

> **환각 가드레일**: 앞 두 단이 LLM의 안전장치. LLM은 읽기전용 `run_cypher` 도구로 **검증된 그래프 사실만** 인용하고(쓰기 절 차단 + read 트랜잭션 이중 차단), 온톨로지 어휘를 시스템 프롬프트에 고정해 존재하지 않는 관계·센서를 지어내지 못하게 했다.

### ⑥ Airflow — 배치/MLOps 오케스트레이션
매일 새벽 2시 실행되는 `fdc_daily_maintenance` DAG (4-task):
```text
data_quality_check ──▶ [retrain_models ∥ sync_graph] ──▶ anomaly_report
 (품질 게이트)          (재학습 / 그래프 동기화 병렬)      (장비·스텝별 일일 이상률)
```
- 재학습·동기화가 **스트리밍과 동일한 스크립트를 재사용** → 로직 일치 보장
- 무거운 Airflow는 Compose **profile로 분리**해 기본 기동에서 제외

---

## 4. 기술적 의사결정 & 엔지니어링

| 결정 | 이유 |
|---|---|
| **비침습 애드온 + 장애 격리** | 그래프 dual-write를 `try/except`로 감싸고 `GRAPH_ENABLED=0`로 no-op 가능하게 → Neo4j가 죽어도 본체 파이프라인 무중단 |
| **코어 로직 단일 소스** | 이상탐지 `score_one`을 단건 consumer·Spark·Airflow 재학습이 모두 재사용 → 결과 불일치 원천 차단 |
| **numpy 직접 구현** | 라이브러리 블랙박스 대신 PCA·T²·UCL·기여도 분해를 손으로 구현해 알고리즘을 완전히 이해·통제 |
| **멱등 MERGE 설계** | `run_key`로 그래프 쓰기를 멱등화 → backfill 재실행/중복 이벤트에도 안전 |
| **LLM 가드레일 3단화** | 사실(그래프) → 의미(온톨로지) → 자연어(LLM) 분리로 환각을 구조적으로 억제 |
| **환경변수 주입 설정** | 접속 정보를 전부 env로 주입 → 로컬/컨테이너/EC2 간 이식성 확보 |

Spark 4.x가 Scala 2.13 빌드라 Kafka 커넥터 아티팩트(`spark-sql-kafka-0-10_2.13`)를 맞추고, JSONB 컬럼을 JDBC `stringtype=unspecified`로 캐스팅하는 등 실제 통합 이슈도 해결했다.

---

## 5. 기술 스택

| 분류 | 기술 |
|---|---|
| 언어 | Python |
| 스트리밍 | Apache Kafka, Apache Spark (Structured Streaming) |
| 저장소 | PostgreSQL, Neo4j |
| 분석/ML | NumPy (PCA + Hotelling's T²) |
| 시맨틱 | RDFLib, OWL / owlrl (FDC 온톨로지 추론) |
| LLM | Anthropic Claude (Opus 4.8, GraphRAG · tool_runner) |
| 시각화 | Streamlit, Plotly |
| 오케스트레이션 | Apache Airflow |
| 인프라 | Docker / Docker Compose, AWS EC2 |

---

## 6. 성과 & 배운 점

- **단변량 임계값의 한계를 다변량 통계(T²)로 극복**하고, 이상의 "원인 센서"까지 분해하는 실용적 FDC 파이프라인을 end-to-end로 완성
- 관계형으로 어려운 계보·commonality 질의를 **그래프 모델링**으로 단순화하고, 온톨로지·LLM으로 **자연어 진단**까지 연결
- 기존 파이프라인을 건드리지 않는 **비침습·장애격리 설계**로 시스템을 점진적으로 확장하는 법을 체득

**향후 계획**: EC2/EKS 배포 자동화, LLM 레이어 실호출 검증 및 평가.

---

## 7. 실행 방법 (요약)

```bash
pip install -r requirements.txt
docker compose up -d                          # Kafka + PostgreSQL + Neo4j
.venv/bin/python anomaly/train_model.py       # 스텝별 모델 학습 (최초 1회)
.venv/bin/python simulator/simulator.py       # 이벤트 생성
.venv/bin/python consumer/consumer.py         # 실시간 T² + PostgreSQL/Neo4j 적재
.venv/bin/streamlit run dashboard/app.py      # 대시보드 (localhost:8501)

# 선택 레이어
.venv/bin/python spark/spark_consumer.py      # Spark로 consumer 대체
.venv/bin/python graph/llm_rca.py ask "L003 랏에서 fail이 제일 많은 장비는?"
docker compose --profile airflow up -d --build   # Airflow (localhost:8080)
```

> 상세 설명·아키텍처 다이어그램은 [`README.md`](./README.md) 참고.
