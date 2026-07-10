# -*- coding: utf-8 -*-
"""PCA + Hotelling's T2 anomaly detector (numpy only).

반도체 FDC(Fault Detection & Classification)에서 실제로 쓰는 다변량
이상탐지 기법을 scikit-learn 없이 numpy 만으로 구현한 모듈.

핵심 아이디어
-------------
1. 센서값을 표준화(z-score) 한다.
2. 공분산 행렬의 고유분해(PCA)로 주성분과 분산(eigenvalue)을 구한다.
3. Hotelling's T2 = 각 주성분 점수^2 / 분산 의 합.
   -> 여러 센서를 "동시에" 보고 평소 패턴에서 얼마나 벗어났는지 하나의 수치로.
4. UCL(관리상한)을 넘으면 이상으로 판정한다.
5. 어떤 센서가 T2 를 끌어올렸는지 기여도(contribution)로 분해한다.
"""

import pickle
import numpy as np


class PCAT2Model:

    def __init__(self, feature_names, var_threshold=0.90, alpha=0.01):
        """
        feature_names : 이 모델이 사용하는 센서 이름 목록 (순서 고정)
        var_threshold : 누적 설명분산이 이 비율을 넘도록 주성분 개수 k 선택
        alpha         : 유의수준. UCL = 학습 T2 의 (1-alpha) 분위수
        """
        self.feature_names = list(feature_names)
        self.var_threshold = var_threshold
        self.alpha = alpha

        # fit() 이후 채워지는 값들
        self.mean_ = None        # (p,) 평균 (결측치 대체에도 사용)
        self.std_ = None         # (p,) 표준편차
        self.components_ = None  # (k, p) 주성분
        self.eigvals_ = None     # (k,)  주성분 분산
        self.D_ = None           # (p, p) T2 = z^T D z 를 위한 행렬
        self.ucl_ = None         # 관리상한
        self.n_components_ = None

    # ----------------------------------------------------------------- #

    def _impute_standardize(self, X):
        """결측치를 학습 평균으로 채우고 표준화한다."""
        X = np.asarray(X, dtype=float)
        if X.ndim == 1:
            X = X.reshape(1, -1)
        Xf = np.where(np.isnan(X), self.mean_, X)
        return (Xf - self.mean_) / self.std_

    # ----------------------------------------------------------------- #

    def fit(self, X):
        X = np.asarray(X, dtype=float)

        # 결측치(NaN)를 무시하고 평균/표준편차 계산
        self.mean_ = np.nanmean(X, axis=0)
        Xf = np.where(np.isnan(X), self.mean_, X)
        self.std_ = Xf.std(axis=0, ddof=1)
        self.std_[self.std_ == 0] = 1.0  # 상수 센서 보호

        Z = (Xf - self.mean_) / self.std_

        # 공분산 고유분해 (eigh: 대칭행렬용, 오름차순 반환)
        cov = np.cov(Z, rowvar=False)
        eigvals, eigvecs = np.linalg.eigh(cov)
        order = np.argsort(eigvals)[::-1]        # 내림차순 정렬
        eigvals = np.clip(eigvals[order], 1e-12, None)
        eigvecs = eigvecs[:, order]

        # 누적 설명분산으로 주성분 개수 k 결정
        ratio = np.cumsum(eigvals) / eigvals.sum()
        k = int(np.searchsorted(ratio, self.var_threshold) + 1)
        k = max(1, min(k, len(eigvals)))

        self.components_ = eigvecs[:, :k].T   # (k, p)
        self.eigvals_ = eigvals[:k]           # (k,)
        self.n_components_ = k

        # T2 = z^T (P^T Λ^-1 P) z  ->  D = P^T Λ^-1 P
        P = self.components_
        self.D_ = P.T @ np.diag(1.0 / self.eigvals_) @ P

        # UCL: 학습 데이터 T2 분포의 (1-alpha) 분위수 (경험적 관리상한)
        t2_train = self.t2(X)
        self.ucl_ = float(np.percentile(t2_train, 100 * (1 - self.alpha)))
        return self

    # ----------------------------------------------------------------- #

    def t2(self, X):
        """각 샘플의 Hotelling's T2 값 (n,) 반환."""
        Z = self._impute_standardize(X)
        return np.einsum("ij,jk,ik->i", Z, self.D_, Z)

    def contributions(self, x):
        """단일 샘플에서 센서별 T2 기여도 (p,) 반환. 합 = T2."""
        z = self._impute_standardize(x)[0]
        return z * (self.D_ @ z)

    # ----------------------------------------------------------------- #

    def score_one(self, values_dict):
        """이벤트 dict 를 받아 이상탐지 결과를 dict 로 반환."""
        x = np.array(
            [values_dict.get(f, np.nan) for f in self.feature_names],
            dtype=float,
        )
        t2 = float(self.t2(x)[0])
        contrib = self.contributions(x)

        # 양(+)의 기여만 정규화해서 "이상에 얼마나 일조했는지" 비율로
        pos = np.clip(contrib, 0, None)
        total = pos.sum()
        frac = pos / total if total > 0 else np.zeros_like(pos)

        contrib_dict = {
            f: round(float(c), 4) for f, c in zip(self.feature_names, frac)
        }
        top = max(contrib_dict, key=contrib_dict.get) if contrib_dict else None

        return {
            "t2": round(t2, 4),
            "ucl": round(self.ucl_, 4),
            "is_anomaly": bool(t2 > self.ucl_),
            "contributions": contrib_dict,
            "top_contributor": top,
        }

    # ----------------------------------------------------------------- #

    def save(self, path):
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @staticmethod
    def load(path):
        with open(path, "rb") as f:
            return pickle.load(f)