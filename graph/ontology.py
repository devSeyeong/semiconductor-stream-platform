# -*- coding: utf-8 -*-
"""FDC 온톨로지 로더 + 추론기.

fdc_ontology.ttl 을 읽어, 이상탐지가 지목한 센서(top_contributor)를
도메인 지식(고장모드 → 근본원인 → 권장조치)으로 번역한다.

이상탐지(PCA+T²)는 "temperature 센서가 T²를 78% 끌어올렸다"까지만 안다.
온톨로지는 "그건 ThermalDrift(온도 드리프트)이고, 근본원인은 히터 이상,
권장조치는 히터 존별 캘리브레이션"이라고 의미를 부여한다.

owlrl 로 RDFS/OWL 추론을 돌려 상위클래스(ProcessExcursion/EquipmentFault/
ContaminationEvent → FailureMode)까지 자동 분류한다.
"""

import os

from rdflib import Graph, Namespace, RDF, RDFS
from rdflib.namespace import OWL

FDC = Namespace("http://semiconductor.local/fdc#")

_ONTOLOGY_PATH = os.path.join(os.path.dirname(__file__), "fdc_ontology.ttl")


class FDCOntology:
    """온톨로지를 로드하고 센서→고장모드 추론을 제공한다."""

    def __init__(self, path: str = _ONTOLOGY_PATH):
        self.g = Graph()
        self.g.parse(path, format="turtle")
        self._apply_reasoning()

    # ------------------------------------------------------------------ #

    def _apply_reasoning(self):
        """owlrl 로 RDFS/OWL-RL 추론 (상위클래스 멤버십 등 자동 도출).

        owlrl 이 없으면 명시된 트리플만으로 동작 (핵심 매핑은 그대로 유효).
        """
        try:
            import owlrl

            owlrl.DeductiveClosure(owlrl.RDFS_OWLRL_Semantics).expand(self.g)
        except Exception as exc:
            print(f"[ontology][WARN] owlrl 추론 생략: {exc}")

    # ------------------------------------------------------------------ #

    @staticmethod
    def _label(g, subject) -> str:
        lbl = g.value(subject, RDFS.label)
        if lbl:
            return str(lbl)
        # 라벨 없으면 URI 뒷부분
        return str(subject).split("#")[-1]

    def classify_sensor(self, sensor_name: str) -> dict:
        """센서 이름 → 고장모드/근본원인/권장조치/상위분류.

        반환 예:
        {
          "sensor": "temperature",
          "failure_mode": "ThermalDrift",
          "failure_mode_label": "온도 드리프트",
          "failure_category": "ProcessExcursion",
          "root_cause": "히터/온도제어 이상",
          "recommended_action": "히터 존별 온도 캘리브레이션, 열전대 점검",
          "measured_at": ["DEPOSITION", "ETCH", "LITHOGRAPHY"]
        }
        빈 dict 이면 온톨로지에 매핑 없음.
        """
        if not sensor_name:
            return {}

        sensor = FDC[sensor_name]
        fm = self.g.value(sensor, FDC.indicatesFailureMode)
        if fm is None:
            return {}

        # 근본원인
        rc = self.g.value(fm, FDC.hasRootCause)
        action = self.g.value(fm, FDC.recommendedAction)

        # 상위분류(추론된 subClassOf 중 FailureMode 바로 아래 카테고리)
        category = None
        for cls in self.g.objects(fm, RDF.type):
            if cls in (FDC.ProcessExcursion, FDC.EquipmentFault, FDC.ContaminationEvent):
                category = str(cls).split("#")[-1]
                break

        measured = sorted(
            str(s).split("#")[-1] for s in self.g.objects(sensor, FDC.measuredAt)
        )

        return {
            "sensor": sensor_name,
            "failure_mode": str(fm).split("#")[-1],
            "failure_mode_label": self._label(self.g, fm),
            "failure_category": category,
            "root_cause": self._label(self.g, rc) if rc else None,
            "recommended_action": str(action) if action else None,
            "measured_at": measured,
        }

    # ------------------------------------------------------------------ #

    def vocabulary(self) -> dict:
        """LLM 시스템 프롬프트에 넣을 어휘 요약 (센서/고장모드/스텝 목록)."""
        sensors = sorted(
            self._label(self.g, s) for s in self.g.subjects(RDF.type, FDC.Sensor)
        )
        failure_modes = sorted(
            f"{str(fm).split('#')[-1]} ({self._label(self.g, fm)})"
            for fm in self.g.subjects(FDC.hasRootCause, None)
        )
        return {
            "sensors": sensors,
            "failure_modes": failure_modes,
            "steps": ["DEPOSITION", "ETCH", "LITHOGRAPHY", "INSPECTION"],
        }


# --------------------------------------------------------------------- #
# 데모: 센서별 분류 출력
# --------------------------------------------------------------------- #

if __name__ == "__main__":
    onto = FDCOntology()
    print("=== 센서 → 고장모드 분류 (온톨로지 추론) ===\n")
    for sensor in [
        "temperature", "pressure", "vibration", "particle_count",
        "rf_power", "alignment_error", "uv_intensity", "gas_flow",
        "thickness_nm", "surface_variance",
    ]:
        r = onto.classify_sensor(sensor)
        if not r:
            print(f"[{sensor}] 매핑 없음")
            continue
        print(
            f"[{sensor}] → {r['failure_mode']} ({r['failure_mode_label']})\n"
            f"    분류: {r['failure_category']}\n"
            f"    근본원인: {r['root_cause']}\n"
            f"    권장조치: {r['recommended_action']}\n"
            f"    측정스텝: {', '.join(r['measured_at'])}\n"
        )