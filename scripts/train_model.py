"""
Train the maintenance-prediction model on a realistic run-to-failure simulation.

Unlike a threshold on the current reading, here each machine follows a stochastic
*degradation* process and fails probabilistically as wear accumulates. The label
is the failure HORIZON — how soon failure is coming — so the task is genuinely
predictive and the rolling-trend features carry real signal:

    class 2 (maintenance / ARRETER) : failure within CRIT_H steps  -> stop now
    class 1 (pause / PAUSE)         : failure within WARN_H steps   -> ease off / inspect
    class 0 (aucune / CONTINUER)    : healthy / far from failure

Methodology that makes the numbers trustworthy:
  * data grouped by *episode* (a machine's life); train/test never share an episode
    (no temporal leakage),
  * model selection by GroupKFold cross-validation (macro-F1),
  * class imbalance handled with balanced class weights,
  * the saved artifacts (rf_model.pkl, scaler.pkl) keep the SAME 11 features and
    3 classes the streaming predictor already consumes — drop-in compatible.

Run:  python scripts/train_model.py
"""
import os
import pickle

import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.metrics import f1_score, classification_report, confusion_matrix

# 12 features, same order as the streaming predictor's feature vector.
# Raw current values + rolling aggregates + TREND slopes (the degradation signal).
FEATURE_NAMES = [
    "vibration", "temperature", "pression", "consommation_electrique", "charge_travail",
    "vibration_mean", "temperature_mean", "vibration_std", "pression_std",
    "temperature_slope", "vibration_slope", "temperature_max",
]
CLASS_NAMES = ["aucune", "pause", "maintenance"]

MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")
SEED = 42
WINDOW = 12          # rolling-window length (~1 min at 5 s/step)
CRIT_H = 6           # <= 6 steps to failure  -> class 2
WARN_H = 18          # <= 18 steps to failure -> class 1
MAX_LIFE = 160       # max steps per episode before censoring


def simulate_episode(rng):
    """One machine life: returns sensor matrix [T,5] and the failure step (or None)."""
    wear = rng.uniform(0.0, 0.05)
    base_load = rng.uniform(0.3, 0.8)
    drift = rng.uniform(0.012, 0.022)        # per-machine wear rate (heterogeneous)
    w_fail = rng.uniform(0.95, 1.15)         # per-machine failure-prone level
    rows, failure_step = [], None

    for t in range(MAX_LIFE):
        workload = np.clip(base_load + rng.normal(0, 0.12) + 0.1 * np.sin(t / 9.0), 0.05, 1.0)
        wear += rng.gamma(shape=2.0, scale=drift)        # monotone, noisy degradation

        # sensors as noisy functions of wear + operating point
        temperature = 58 + 28 * wear + 9 * workload + rng.normal(0, 4.0)
        vibration = 0.7 + 2.6 * wear ** 1.6 + (0.4 + 0.8 * wear) * abs(rng.normal(0, 1))
        pression = 5.0 + 1.1 * wear + rng.normal(0, 0.4)
        consommation = 95 + 32 * wear + 16 * workload + rng.normal(0, 8)
        charge = np.clip(100 * workload + rng.normal(0, 3), 0, 100)
        rows.append([vibration, temperature, pression, consommation, charge])

        # stochastic failure: hazard rises sharply with wear (still not a hard
        # threshold — sensor noise keeps the *instantaneous* reading a weak signal,
        # so the trend features are what carry predictive power).
        hazard = 1.0 / (1.0 + np.exp(-12.0 * (wear - w_fail)))
        if rng.random() < hazard * 0.6:
            failure_step = t
            break

    return np.array(rows), failure_step


def _slope(s):
    """Average per-step trend over the window (least-squares slope)."""
    n = len(s)
    if n < 2:
        return 0.0
    return float(np.polyfit(np.arange(n), s, 1)[0])


def features_at(readings, t):
    """The 12 rolling features at step t (mirrors the streaming predictor)."""
    win = readings[max(0, t - WINDOW + 1): t + 1]
    cur = readings[t]
    vib, temp, pres = win[:, 0], win[:, 1], win[:, 2]
    return [
        cur[0], cur[1], cur[2], cur[3], cur[4],
        vib.mean(), temp.mean(),
        vib.std() if len(vib) > 1 else 0.0,
        pres.std() if len(pres) > 1 else 0.0,
        _slope(temp), _slope(vib),
        temp.max(),
    ]


def label_at(failure_step, t):
    if failure_step is None:
        return 0
    ttf = failure_step - t
    if ttf <= CRIT_H:
        return 2
    if ttf <= WARN_H:
        return 1
    return 0


def build_dataset(n_episodes, seed):
    rng = np.random.default_rng(seed)
    X, y, groups = [], [], []
    for ep in range(n_episodes):
        readings, fstep = simulate_episode(rng)
        for t in range(len(readings)):
            X.append(features_at(readings, t))
            y.append(label_at(fstep, t))
            groups.append(ep)
    return np.array(X), np.array(y), np.array(groups)


def candidates():
    return {
        "logreg": LogisticRegression(max_iter=2000, class_weight="balanced"),
        "random_forest": RandomForestClassifier(
            n_estimators=120, max_depth=10, min_samples_leaf=20,
            class_weight="balanced", random_state=SEED, n_jobs=-1),
        "hist_gbm": HistGradientBoostingClassifier(
            max_depth=8, learning_rate=0.08, max_iter=350,
            class_weight="balanced", random_state=SEED),
    }


def main():
    print("Simulating run-to-failure episodes...")
    X, y, groups = build_dataset(n_episodes=500, seed=SEED)
    counts = {int(c): int((y == c).sum()) for c in np.unique(y)}
    print(f"  {len(X)} samples from {len(np.unique(groups))} episodes | class balance {counts}")

    # Held-out test = entirely unseen episodes
    tr, te = next(GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=SEED).split(X, y, groups))
    Xtr, Xte, ytr, yte, gtr = X[tr], X[te], y[tr], y[te], groups[tr]

    scaler = StandardScaler().fit(Xtr)
    Xtr_s, Xte_s = scaler.transform(Xtr), scaler.transform(Xte)

    print("\nModel selection (GroupKFold=5, macro-F1):")
    gkf = GroupKFold(n_splits=5)
    best_name, best_model, best_cv = None, None, -1.0
    for name, model in candidates().items():
        scores = []
        for a, b in gkf.split(Xtr_s, ytr, gtr):
            model.fit(Xtr_s[a], ytr[a])
            scores.append(f1_score(ytr[b], model.predict(Xtr_s[b]), average="macro"))
        m = float(np.mean(scores))
        print(f"  {name:16s} macro-F1 = {m:.3f}  (+/- {np.std(scores):.3f})")
        if m > best_cv:
            best_name, best_cv, best_model = name, m, model

    print(f"\nSelected: {best_name} (CV macro-F1 {best_cv:.3f})")
    best_model.fit(Xtr_s, ytr)
    print("\nHeld-out test (unseen episodes):")
    print(confusion_matrix(yte, best_model.predict(Xte_s)))
    print(classification_report(yte, best_model.predict(Xte_s),
                                target_names=CLASS_NAMES, labels=[0, 1, 2], digits=3, zero_division=0))

    # Refit on ALL data for the deployed artifacts
    scaler_full = StandardScaler().fit(X)
    best_model.fit(scaler_full.transform(X), y)
    os.makedirs(MODELS_DIR, exist_ok=True)
    with open(os.path.join(MODELS_DIR, "rf_model.pkl"), "wb") as f:
        pickle.dump(best_model, f)
    with open(os.path.join(MODELS_DIR, "scaler.pkl"), "wb") as f:
        pickle.dump(scaler_full, f)
    print(f"\nSaved {best_name} + scaler to {MODELS_DIR}")


if __name__ == "__main__":
    main()
