"""
Pillar 1: Hierarchical Bayesian model of player true-talent WAR.

Partial pooling toward position-level means. Fits in PyMC.
Output: posterior distributions of $/WAR per Mariners player.

Status: stub — implementation in notebooks/02_bayesian_payroll.ipynb
"""

import numpy as np
import pandas as pd

# pymc import deferred — only needed when fitting
# import pymc as pm
# import arviz as az


def build_model(player_seasons: pd.DataFrame):
    """
    Build hierarchical model.

    Parameters
    ----------
    player_seasons : DataFrame with columns [mlbam_id, season, position, war, pa, age]

    Returns
    -------
    pm.Model (unfitted)
    """
    import pymc as pm

    positions = player_seasons["position"].unique().tolist()
    pos_idx = pd.Categorical(player_seasons["position"], categories=positions).codes

    with pm.Model() as model:
        # Hyperpriors (population level)
        mu_position = pm.Normal("mu_position", mu=2.0, sigma=1.5, shape=len(positions))
        sigma_position = pm.HalfNormal("sigma_position", sigma=1.0, shape=len(positions))

        # Player-level true talent (partial pooling toward position mean)
        war_true = pm.Normal(
            "war_true",
            mu=mu_position[pos_idx],
            sigma=sigma_position[pos_idx],
            shape=len(player_seasons),
        )

        # Observation noise scales with playing time (more PA = tighter)
        sigma_obs = pm.Deterministic(
            "sigma_obs",
            1.5 / np.sqrt(player_seasons["pa"].values / 600 + 0.1),
        )

        # Likelihood
        pm.Normal("war_obs", mu=war_true, sigma=sigma_obs, observed=player_seasons["war"].values)

    return model


def fit(model, draws: int = 1000, tune: int = 1000, chains: int = 2):
    import pymc as pm

    with model:
        trace = pm.sample(draws=draws, tune=tune, chains=chains, return_inferencedata=True)
    return trace
