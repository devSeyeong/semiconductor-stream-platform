# -*- coding: utf-8 -*-
"""스텝별 PCA + Hotelling's T2 모델 학습 스크립트.

시뮬레이터의 '정상 공정' 데이터 생성 로직을 그대로 재사용해서
(장비가 건강한 상태 = noise 0) 정상 운전 영역을 학습한다.
이렇게 만든 baseline 을 벗어나는 실시간 이벤트가 이상으로 잡힌다.

실행:
    .venv/bin/python anomaly/train_model.py
"""

import os
import sys

import numpy as np

# 프로젝트 루트를 import 경로에 추가
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "simulator"))

from simulator import STEPS, Equipment, SemiconductorSimulator  # noqa: E402
from anomaly.pca_t2 import PCAT2Model  # noqa: E402

MODEL_DIR = os.path.join(ROOT, "anomaly", "models")
N_SAMPLES = 4000  # 스텝당 학습 샘플 수


def collect_normal_data(sim, step, n):
    """건강한 장비로 정상 공정 데이터를 n개 생성한다.

    feature 이름은 스텝마다 다르므로, 첫 샘플의 키 순서로 고정한다.
    결측치(None)는 NaN 으로 변환해 모델이 처리하게 한다.
    """
    rows = []
    feature_names = None

    for _ in range(n):
        eq = Equipment(f"{step}-EQ-TRAIN")  # health_score=1.0 (정상)
        metrics = sim._generate_process_metrics(step, eq)

        if feature_names is None:
            feature_names = list(metrics.keys())

        rows.append([
            np.nan if metrics.get(f) is None else metrics.get(f)
            for f in feature_names
        ])

    return feature_names, np.array(rows, dtype=float)


def main():
    os.makedirs(MODEL_DIR, exist_ok=True)
    sim = SemiconductorSimulator()

    print(f"학습 시작 (스텝당 {N_SAMPLES} 샘플)\n")

    for step in STEPS:
        feature_names, X = collect_normal_data(sim, step, N_SAMPLES)

        model = PCAT2Model(feature_names=feature_names)
        model.fit(X)

        path = os.path.join(MODEL_DIR, f"{step}.pkl")
        model.save(path)

        print(
            f"[{step:12}] features={len(feature_names):2d}  "
            f"k={model.n_components_}  UCL={model.ucl_:8.3f}  "
            f"-> {os.path.relpath(path, ROOT)}"
        )

    print("\n완료. 모델이 anomaly/models/ 에 저장되었습니다.")


if __name__ == "__main__":
    main()