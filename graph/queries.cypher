// =====================================================================
// Semiconductor FDC — 그래프 쿼리 모음
// Neo4j Browser(http://localhost:7474) 에 붙여넣어 실행.
// 이 쿼리들이 "왜 그래프인가"를 보여준다 — 관계형이면 self-join 지옥.
// =====================================================================

// ---------------------------------------------------------------------
// 1. Wafer Genealogy — 특정 웨이퍼가 거쳐온 전체 이력(계보)
// ---------------------------------------------------------------------
MATCH (w:Wafer {id: 'W00001'})-[:HAS_RUN]->(r:Run)-[:ON_EQUIPMENT]->(e:Equipment)
MATCH (r)-[:AT_STEP]->(s:Step)
RETURN w.id AS wafer, r.timestamp AS ts, s.name AS step,
       e.id AS equipment, r.label AS label, r.t2 AS t2, r.is_anomaly AS anomaly
ORDER BY r.timestamp;

// ---------------------------------------------------------------------
// 2. Commonality Analysis — FAIL 웨이퍼들이 공통으로 지나간 장비 (FDC 킬러)
//    "불량이 몰리는 장비"를 한 방에 찾는다.
// ---------------------------------------------------------------------
MATCH (w:Wafer)-[:HAS_RUN]->(r:Run {label: 'FAIL'})-[:ON_EQUIPMENT]->(e:Equipment)
RETURN e.id AS equipment, count(DISTINCT w) AS fail_wafers,
       count(r) AS fail_runs
ORDER BY fail_wafers DESC;

// ---------------------------------------------------------------------
// 3. 장비별 이상률 — 전체 run 대비 이상(is_anomaly) 비율
// ---------------------------------------------------------------------
MATCH (r:Run)-[:ON_EQUIPMENT]->(e:Equipment)
WITH e, count(r) AS total, sum(CASE WHEN r.is_anomaly THEN 1 ELSE 0 END) AS anomalies
RETURN e.id AS equipment, total, anomalies,
       round(100.0 * anomalies / total, 2) AS anomaly_pct
ORDER BY anomaly_pct DESC;

// ---------------------------------------------------------------------
// 4. 센서 기여 → 어떤 센서가 이상을 가장 많이 일으켰나 (근본원인 후보)
// ---------------------------------------------------------------------
MATCH (r:Run)-[:CAUSED_BY]->(s:Sensor)
RETURN s.name AS sensor, count(r) AS anomaly_count
ORDER BY anomaly_count DESC;

// ---------------------------------------------------------------------
// 5. Fault Propagation — 특정 장비를 거친 뒤 다운스트림에서 fail 난 웨이퍼
//    (a 장비 이후 공정 흐름을 따라간 run 들)
// ---------------------------------------------------------------------
MATCH (e:Equipment {id: 'ETCH-EQ-3'})<-[:ON_EQUIPMENT]-(r1:Run)<-[:HAS_RUN]-(w:Wafer)
MATCH (w)-[:HAS_RUN]->(r2:Run)-[:AT_STEP]->(s2:Step)
WHERE r2.timestamp > r1.timestamp AND r2.label = 'FAIL'
RETURN w.id AS wafer, s2.name AS downstream_step, r2.timestamp AS ts
ORDER BY ts;

// ---------------------------------------------------------------------
// 6. Lot 단위 품질 — 랏별 fail 비율
// ---------------------------------------------------------------------
MATCH (lot:Lot)<-[:BELONGS_TO]-(w:Wafer)-[:HAS_RUN]->(r:Run)
WITH lot, count(r) AS runs, sum(CASE WHEN r.label='FAIL' THEN 1 ELSE 0 END) AS fails
RETURN lot.id AS lot, runs, fails, round(100.0*fails/runs, 2) AS fail_pct
ORDER BY fail_pct DESC;

// ---------------------------------------------------------------------
// 7. 특정 장비 + 특정 센서 조합의 이상 (장비-센서 상관)
// ---------------------------------------------------------------------
MATCH (e:Equipment)<-[:ON_EQUIPMENT]-(r:Run)-[:CAUSED_BY]->(s:Sensor)
RETURN e.id AS equipment, s.name AS sensor, count(r) AS cnt
ORDER BY cnt DESC
LIMIT 20;