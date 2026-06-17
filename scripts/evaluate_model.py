"""
Honest evaluation of the maintenance-prediction model.

Evaluates on FRESH, unseen run-to-failure episodes (no leakage) and reports the
metrics that matter for predictive maintenance:

  * 3-class confusion matrix + per-class precision/recall/F1,
  * the operational binary view: "attention needed (warn|critical) vs fine" -
    i.e. do we catch machines heading for failure?
  * PR-AUC for the imminent-failure class,
  * and a comparison against the two baselines that matter: a majority-class
    dummy and the 3-line threshold RULE (the predictor's fallback). The model is
    only worthwhile if it beats both.

Run:  python scripts/evaluate_model.py
"""
import os
import sys
import pickle

import numpy as np
from sklearn.metrics import (
    classification_report, confusion_matrix, average_precision_score,
)
from sklearn.dummy import DummyClassifier

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from train_model import build_dataset, FEATURE_NAMES, CLASS_NAMES, MODELS_DIR  # noqa: E402


def load_model():
    with open(os.path.join(MODELS_DIR, "rf_model.pkl"), "rb") as f:
        model = pickle.load(f)
    with open(os.path.join(MODELS_DIR, "scaler.pkl"), "rb") as f:
        scaler = pickle.load(f)
    return model, scaler


def rule_predict(X):
    """The predictor's fallback rule, on the *current* temperature/charge."""
    out = []
    for r in X:
        temp, charge = r[1], r[4]
        out.append(2 if (temp > 90 or charge > 90) else 1 if (temp > 75 or charge > 70) else 0)
    return np.array(out)


def report(name, y_true, y_pred):
    print(f"\n----- {name} -----")
    print(confusion_matrix(y_true, y_pred, labels=[0, 1, 2]))
    print(classification_report(y_true, y_pred, target_names=CLASS_NAMES,
                                labels=[0, 1, 2], digits=3, zero_division=0))


def main():
    model, scaler = load_model()

    # Fresh, unseen episodes (different seed → different machines)
    X, y, _ = build_dataset(n_episodes=300, seed=99999)
    Xs = scaler.transform(X)
    pred = model.predict(Xs)

    report("MODEL - RandomForest (unseen episodes)", y, pred)

    # Operational binary view: does it flag machines that need attention?
    yb, pb = (y >= 1).astype(int), (pred >= 1).astype(int)
    print("----- Operational view: attention-needed (warn|critical) vs fine -----")
    print(classification_report(yb, pb, target_names=["fine", "attention"], digits=3, zero_division=0))

    # PR-AUC for the imminent-failure class
    proba = model.predict_proba(Xs)
    cls = list(model.classes_)
    if 2 in cls:
        ap = average_precision_score((y == 2).astype(int), proba[:, cls.index(2)])
        print(f"PR-AUC (imminent failure, class 2): {ap:.3f}\n")

    report("BASELINE - 3-line threshold RULE", y, rule_predict(X))
    report("BASELINE - majority-class dummy", y, DummyClassifier(strategy="most_frequent").fit(Xs, y).predict(Xs))

    if hasattr(model, "feature_importances_"):
        print("Top feature importances:")
        for name, imp in sorted(zip(FEATURE_NAMES, model.feature_importances_), key=lambda t: -t[1])[:6]:
            print(f"  {name:26s} {imp:.3f}")

    # Headline comparison
    from sklearn.metrics import f1_score
    print("\n=== macro-F1 summary (unseen episodes) ===")
    print(f"  model (RandomForest) : {f1_score(y, pred, average='macro'):.3f}")
    print(f"  threshold rule       : {f1_score(y, rule_predict(X), average='macro'):.3f}")
    print(f"  majority dummy       : {f1_score(y, np.zeros_like(y), average='macro'):.3f}")


if __name__ == "__main__":
    main()
