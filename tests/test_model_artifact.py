"""Guard the committed model artifacts: right shape, right classes, and it must
actually beat the majority-class baseline on fresh unseen episodes."""
import os
import pickle

import numpy as np
from sklearn.metrics import f1_score

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load():
    with open(os.path.join(ROOT, "models", "rf_model.pkl"), "rb") as f:
        model = pickle.load(f)
    with open(os.path.join(ROOT, "models", "scaler.pkl"), "rb") as f:
        scaler = pickle.load(f)
    return model, scaler


def test_model_shape_and_classes():
    model, scaler = _load()
    assert model.n_features_in_ == 12
    assert scaler.n_features_in_ == 12
    assert sorted(int(c) for c in model.classes_) == [0, 1, 2]


def test_model_beats_majority_baseline():
    import train_model as tm
    model, scaler = _load()
    X, y, _ = tm.build_dataset(n_episodes=40, seed=7)
    pred = model.predict(scaler.transform(X))
    model_f1 = f1_score(y, pred, average="macro")
    majority_f1 = f1_score(y, np.zeros_like(y), average="macro")
    assert model_f1 > majority_f1 + 0.1, f"model {model_f1:.3f} vs majority {majority_f1:.3f}"
