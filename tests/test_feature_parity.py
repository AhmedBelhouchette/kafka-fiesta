"""
The most important test: the features computed at TRAINING time
(scripts/train_model.py) must exactly match the features computed at SERVING
time (the streaming predictor). Train/serve feature drift is what made the
original model worthless, so we pin it.
"""
import numpy as np


def test_train_and_serve_features_match():
    import train_model as tm
    import ml_predictor_fixed as mp

    rng = np.random.default_rng(0)
    # 12 readings, columns: vibration, temperature, pression, consommation, charge
    readings = np.column_stack([
        rng.uniform(0.5, 3.5, 12),
        rng.uniform(55, 95, 12),
        rng.uniform(3.5, 6.5, 12),
        rng.uniform(80, 150, 12),
        rng.uniform(20, 95, 12),
    ])

    # training-time features at the last step
    f_train = tm.features_at(readings, len(readings) - 1)

    # serving-time features: feed the same readings into the predictor's buffer
    predictor = mp.MLMaintenancePredictor()
    f_serve = None
    for r in readings:
        f_serve = predictor.compute_features({
            "vibration": r[0], "temperature": r[1], "pression": r[2],
            "consommation_electrique": r[3], "charge_travail": r[4],
        }, "M1")

    assert len(f_train) == 12
    assert len(f_serve) == 12
    np.testing.assert_allclose(f_train, f_serve, rtol=1e-6, atol=1e-6)
