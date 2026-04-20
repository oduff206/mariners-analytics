"""
Pillar 3: Gradient-boosted model projecting next-season WAR for free agents.

Train on 2016–2023 seasons, validate on 2024, apply to 2026 FA class.
Output: ranked FA target list with projected WAR and quantile intervals.

Status: implementation in notebooks/04_fa_projections.ipynb
"""

import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor


FEATURES = [
    "war_y0", "war_y1", "war_y2",   # trailing 3-year WAR
    "age",
    "pa_y0",                          # playing time (reliability signal)
    "woba_y0",
    "xwoba_y0",                       # Statcast quality-of-contact
    "exit_velo_avg_y0",
    "barrel_pct_y0",
    "position_enc",                   # label-encoded position
]


def build_pipeline() -> Pipeline:
    """Median (point-estimate) XGBoost pipeline with NaN imputation."""
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
        ("model",   XGBRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=4,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
        )),
    ])


def build_quantile_pipeline(alpha: float) -> Pipeline:
    """Quantile XGBoost pipeline for a given quantile level (e.g. 0.1 or 0.9)."""
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
        ("model",   XGBRegressor(
            objective="reg:quantileerror",
            quantile_alpha=alpha,
            n_estimators=300,
            learning_rate=0.05,
            max_depth=4,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
        )),
    ])


def train(X: pd.DataFrame, y: pd.Series) -> Pipeline:
    pipe = build_pipeline()
    pipe.fit(X[FEATURES], y)
    return pipe


def predict_with_intervals(
    pipe_median: Pipeline,
    pipe_low: Pipeline,
    pipe_high: Pipeline,
    X: pd.DataFrame,
) -> pd.DataFrame:
    """Return point prediction + 80% quantile interval (p10–p90)."""
    return pd.DataFrame({
        "war_proj": pipe_median.predict(X[FEATURES]),
        "war_low":  pipe_low.predict(X[FEATURES]),
        "war_high": pipe_high.predict(X[FEATURES]),
    }, index=X.index)
