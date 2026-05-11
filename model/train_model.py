from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import joblib
import numpy as np
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    auc,
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fraud_pipeline import PipelineConfig, iter_transaction_events
from fraud_pipeline.features import FEATURE_COLUMNS, TXN_TYPE_CATEGORIES, build_feature_record

MODEL_DIR = Path(__file__).resolve().parent
PLOTS_DIR = MODEL_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def load_events(csv_path: str, limit: int | None) -> list:
    print(f"[INFO] Loading data from: {csv_path}")
    if limit:
        print(f"[INFO] Limit: {limit} rows")
    config = PipelineConfig()
    events = list(iter_transaction_events(csv_path, config=config, limit=limit))
    print(f"[INFO] Loaded {len(events)} transactions")
    return events


def build_features(events: list) -> tuple[np.ndarray, np.ndarray, list[str], list[str]]:
    records = [build_feature_record(ev) for ev in events]
    txn_types = [r["txn_type"] for r in records]
    labels = np.array([r["label_is_fraud"] for r in records], dtype=np.int32)

    feature_dicts = []
    for r in records:
        fv = {col: float(r[col]) for col in FEATURE_COLUMNS}
        fv.update({f"type_{cat}": int(r["txn_type"] == cat) for cat in TXN_TYPE_CATEGORIES})
        feature_dicts.append(fv)

    all_cols = FEATURE_COLUMNS + [f"type_{cat}" for cat in TXN_TYPE_CATEGORIES]
    X = np.array([[d[col] for col in all_cols] for d in feature_dicts], dtype=np.float64)

    return X, labels, txn_types, all_cols


def run_eda(X: np.ndarray, y: np.ndarray, txn_types: list[str], feature_cols: list[str]):
    print("\n" + "=" * 60)
    print("EDA SUMMARY")
    print("=" * 60)

    n_total = len(y)
    n_fraud = int(y.sum())
    n_legit = n_total - n_fraud
    fraud_rate = n_fraud / n_total * 100

    print(f"Total transactions: {n_total}")
    print(f"Fraud: {n_fraud} ({fraud_rate:.4f}%)")
    print(f"Legitimate: {n_legit} ({100 - fraud_rate:.4f}%)")
    print(f"Imbalance ratio: 1:{n_legit // max(n_fraud, 1):,}")

    from collections import Counter
    type_counts = Counter(txn_types)
    print(f"\nTransaction type distribution:")
    for t, c in type_counts.most_common():
        fraud_in_type = sum(1 for i, tt in enumerate(txn_types) if tt == t and y[i] == 1)
        print(f"  {t:>10s}: {c:>8,} ({c/n_total*100:5.2f}%)  | Fraud: {fraud_in_type} ({fraud_in_type/max(c,1)*100:.2f}%)")

    step_vals = X[:, feature_cols.index("step")]
    amount_vals = X[:, feature_cols.index("amount")]
    print(f"\nNumerical features summary:")
    print(f"  step   - min={step_vals.min():.0f}, max={step_vals.max():.0f}, mean={step_vals.mean():.1f}")
    print(f"  amount - min={amount_vals.min():.2f}, max={amount_vals.max():.2f}, mean={amount_vals.mean():.2f}")

    fraud_indices = y == 1
    legit_indices = y == 0
    if fraud_indices.sum() > 0:
        print(f"\n  Fraud amount stats:   min={amount_vals[fraud_indices].min():.2f}, "
              f"max={amount_vals[fraud_indices].max():.2f}, "
              f"mean={amount_vals[fraud_indices].mean():.2f}")
        print(f"  Legit amount stats:   min={amount_vals[legit_indices].min():.2f}, "
              f"max={amount_vals[legit_indices].max():.2f}, "
              f"mean={amount_vals[legit_indices].mean():.2f}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns

        # 1. Fraud by transaction type
        plt.figure(figsize=(10, 5))
        type_list = list(type_counts.keys())
        fraud_counts = []
        for t in type_list:
            fraud_in_type = sum(1 for i, tt in enumerate(txn_types) if tt == t and y[i] == 1)
            fraud_counts.append(fraud_in_type)
        colors = ["#ff6b6b" if f > 0 else "#51cf66" for f in fraud_counts]
        plt.bar(type_list, fraud_counts, color=colors)
        plt.title("Fraud Count by Transaction Type", fontsize=14, fontweight="bold")
        plt.xlabel("Transaction Type")
        plt.ylabel("Fraud Count")
        for i, v in enumerate(fraud_counts):
            if v > 0:
                plt.text(i, v + max(fraud_counts)*0.01, str(v), ha="center", fontweight="bold")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "fraud_by_type.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved fraud_by_type.png")

        # 2. Amount distribution
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        axes[0].hist(amount_vals[legit_indices], bins=50, alpha=0.7, color="#51cf66", label="Legitimate")
        axes[0].set_title("Amount Distribution - Legitimate")
        axes[0].set_xlabel("Amount")
        axes[0].set_ylabel("Frequency")
        axes[1].hist(amount_vals[fraud_indices], bins=50, alpha=0.7, color="#ff6b6b", label="Fraud")
        axes[1].set_title("Amount Distribution - Fraud")
        axes[1].set_xlabel("Amount")
        axes[1].set_ylabel("Frequency")
        for ax in axes:
            ax.legend()
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "amount_dist.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved amount_dist.png")

        # 3. Correlation matrix
        corr_cols = [c for c in feature_cols if not c.startswith("type_")]
        corr_indices = [feature_cols.index(c) for c in corr_cols]
        corr_X = X[:, corr_indices]
        corr_df = np.column_stack([corr_X, y])
        corr_labels = corr_cols + ["is_fraud"]
        corr_matrix = np.corrcoef(corr_df.T)
        plt.figure(figsize=(12, 10))
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        sns.heatmap(corr_matrix, mask=mask, annot=True, fmt=".2f", cmap="RdBu_r",
                    xticklabels=corr_labels, yticklabels=corr_labels, center=0,
                    square=True, linewidths=0.5)
        plt.title("Feature Correlation Matrix", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "correlation_matrix.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved correlation_matrix.png")

    except ImportError:
        print("[WARN] matplotlib/seaborn not available, skipping EDA plots")

    eda_summary = {
        "total_transactions": n_total,
        "fraud_count": n_fraud,
        "legitimate_count": n_legit,
        "fraud_rate_percent": round(fraud_rate, 4),
        "imbalance_ratio": f"1:{n_legit // max(n_fraud, 1)}",
        "transaction_types": {t: c for t, c in type_counts.most_common()},
    }
    eda_path = MODEL_DIR / "eda_summary.json"
    eda_path.write_text(json.dumps(eda_summary, indent=2), encoding="utf-8")
    print(f"[INFO] EDA summary saved to {eda_path}")


def train_model(X_train, y_train, X_test, y_test, feature_cols):
    print("\n" + "=" * 60)
    print("TRAINING")
    print("=" * 60)

    n_fraud_train = int(y_train.sum())
    print(f"Train: {len(y_train)} samples ({n_fraud_train} fraud, {n_fraud_train/len(y_train)*100:.3f}%)")
    print(f"Test:  {len(y_test)} samples ({int(y_test.sum())} fraud, {int(y_test.sum())/len(y_test)*100:.3f}%)")

    print("\n[STEP] Applying SMOTE to handle class imbalance...")
    smote = SMOTE(random_state=42)
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)
    n_resampled = int(y_train_res.sum())
    print(f"  After SMOTE: {len(y_train_res)} samples ({n_resampled} fraud, {n_resampled/len(y_train_res)*100:.1f}%)")

    print("\n[STEP] Training Random Forest...")
    rf = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
        verbose=0,
    )
    start = time.time()
    rf.fit(X_train_res, y_train_res)
    elapsed = time.time() - start
    print(f"  Training completed in {elapsed:.2f}s")

    print("\n[STEP] Evaluating on test set...")
    y_prob = rf.predict_proba(X_test)[:, 1]
    y_pred = rf.predict(X_test)

    auc_roc = roc_auc_score(y_test, y_prob)
    print(f"\n  AUC-ROC: {auc_roc:.4f}")

    precisions, recalls, thresholds = precision_recall_curve(y_test, y_prob)
    auc_pr = auc(recalls, precisions)
    print(f"  AUC-PR:  {auc_pr:.4f}")

    f1_scores = 2 * (precisions[:-1] * recalls[:-1]) / (precisions[:-1] + recalls[:-1] + 1e-8)
    best_idx = int(np.argmax(f1_scores))
    best_threshold = thresholds[best_idx]
    best_f1 = f1_scores[best_idx]
    best_precision = precisions[best_idx]
    best_recall = recalls[best_idx]
    print(f"\n  Optimal threshold: {best_threshold:.4f}")
    print(f"  F1 score:          {best_f1:.4f}")
    print(f"  Precision:         {best_precision:.4f}")
    print(f"  Recall:            {best_recall:.4f}")

    y_pred_opt = (y_prob >= best_threshold).astype(int)
    print(f"\n  Confusion Matrix (threshold={best_threshold:.4f}):")
    cm = confusion_matrix(y_test, y_pred_opt)
    print(f"    TN={cm[0, 0]:,}  FP={cm[0, 1]:,}")
    print(f"    FN={cm[1, 0]:,}  TP={cm[1, 1]:,}")

    print(f"\n  Classification Report:")
    print(classification_report(y_test, y_pred_opt, target_names=["Legit", "Fraud"], digits=4))

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        # PR Curve
        plt.figure(figsize=(8, 6))
        plt.plot(recalls, precisions, color="#4dabf7", linewidth=2, label=f"PR curve (AUC={auc_pr:.3f})")
        plt.scatter([best_recall], [best_precision], color="#ff6b6b", s=100, zorder=5,
                    label=f"Optimal threshold={best_threshold:.3f}")
        plt.xlabel("Recall", fontsize=12)
        plt.ylabel("Precision", fontsize=12)
        plt.title("Precision-Recall Curve", fontsize=14, fontweight="bold")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "pr_curve.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved pr_curve.png")

        # ROC Curve
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        plt.figure(figsize=(8, 6))
        plt.plot(fpr, tpr, color="#4dabf7", linewidth=2, label=f"ROC curve (AUC={auc_roc:.3f})")
        plt.plot([0, 1], [0, 1], "k--", alpha=0.5)
        plt.xlabel("False Positive Rate", fontsize=12)
        plt.ylabel("True Positive Rate", fontsize=12)
        plt.title("ROC Curve", fontsize=14, fontweight="bold")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "roc_curve.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved roc_curve.png")

        # Confusion Matrix
        import seaborn as sns
        plt.figure(figsize=(6, 5))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=["Legit", "Fraud"],
                    yticklabels=["Legit", "Fraud"])
        plt.title(f"Confusion Matrix (threshold={best_threshold:.3f})", fontsize=14, fontweight="bold")
        plt.ylabel("Actual")
        plt.xlabel("Predicted")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "confusion_matrix.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved confusion_matrix.png")

        # Feature Importance
        importances = rf.feature_importances_
        indices = np.argsort(importances)[::-1]
        plt.figure(figsize=(12, 8))
        colors = plt.cm.Blues(np.linspace(0.4, 1, len(indices)))
        plt.barh(range(len(indices)), importances[indices][::-1], color=colors[::-1])
        plt.yticks(range(len(indices)), [feature_cols[i] for i in indices[::-1]])
        plt.xlabel("Feature Importance", fontsize=12)
        plt.title("Random Forest Feature Importance", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.savefig(str(PLOTS_DIR / "feature_importance.png"), dpi=100)
        plt.close()
        print("[PLOT] Saved feature_importance.png")

    except ImportError:
        print("[WARN] matplotlib not available, skipping evaluation plots")

    return rf, best_threshold, {
        "auc_roc": round(float(auc_roc), 4),
        "auc_pr": round(float(auc_pr), 4),
        "optimal_threshold": round(float(best_threshold), 4),
        "f1_score": round(float(best_f1), 4),
        "precision": round(float(best_precision), 4),
        "recall": round(float(best_recall), 4),
        "train_time_seconds": round(elapsed, 2),
        "confusion_matrix": {
            "tn": int(cm[0, 0]), "fp": int(cm[0, 1]),
            "fn": int(cm[1, 0]), "tp": int(cm[1, 1]),
        },
    }


def export_model(rf, scaler, feature_cols, metrics, threshold):
    print("\n" + "=" * 60)
    print("EXPORTING MODEL ARTIFACTS")
    print("=" * 60)

    model_path = MODEL_DIR / "fraud_model_v1.pkl"
    joblib.dump(rf, str(model_path))
    print(f"[EXPORT] Model saved to {model_path}")

    scaler_path = MODEL_DIR / "scaler.pkl"
    joblib.dump(scaler, str(scaler_path))
    print(f"[EXPORT] Scaler saved to {scaler_path}")

    cols_path = MODEL_DIR / "feature_columns.json"
    cols_path.write_text(json.dumps(feature_cols, indent=2), encoding="utf-8")
    print(f"[EXPORT] Feature columns saved to {cols_path}")

    metadata = {
        "model_version": "v1_paysim_rf",
        "model_type": "RandomForestClassifier",
        "n_estimators": 100,
        "max_depth": 15,
        "min_samples_leaf": 5,
        "class_weight": "balanced",
        "feature_columns": feature_cols,
        "optimal_threshold": threshold,
        "metrics": metrics,
    }
    meta_path = MODEL_DIR / "model_metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"[EXPORT] Metadata saved to {meta_path}")

    results = {
        "metrics": metrics,
        "optimal_threshold": threshold,
        "model_version": "v1_paysim_rf",
        "plots": [p.name for p in PLOTS_DIR.glob("*.png")],
    }
    eval_path = MODEL_DIR / "eval_results.json"
    eval_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"[EXPORT] Evaluation results saved to {eval_path}")

    files = [model_path, scaler_path, cols_path, meta_path, eval_path]
    for f in files:
        print(f"  {f.name}: {f.stat().st_size / 1024:.1f} KB")


def parse_args():
    parser = argparse.ArgumentParser(description="Train Fraud Detection ML Model")
    parser.add_argument("--csv-path", required=True, help="Path to PaySim CSV dataset")
    parser.add_argument("--limit", type=int, default=10000, help="Number of rows to use (default: 10000)")
    parser.add_argument("--test-size", type=float, default=0.2, help="Test set ratio (default: 0.2)")
    parser.add_argument("--skip-eda", action="store_true", help="Skip EDA phase")
    return parser.parse_args()


def main():
    args = parse_args()

    events = load_events(args.csv_path, args.limit)

    X, y, txn_types, feature_cols = build_features(events)

    if not args.skip_eda:
        run_eda(X, y, txn_types, feature_cols)

    print("\n[STEP] Splitting data (time-based: first 80% train, last 20% test)...")
    split_idx = int(len(X) * (1 - args.test_size))
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    rf_model, best_threshold, metrics = train_model(X_train, y_train, X_test, y_test, feature_cols)

    export_model(rf_model, scaler, feature_cols, metrics, best_threshold)

    print("\n" + "=" * 60)
    print("TRAINING PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"\nModel version: v1_paysim_rf")
    print(f"Optimal threshold: {best_threshold:.4f}")
    print(f"AUC-ROC: {metrics['auc_roc']:.4f}")
    print(f"AUC-PR:  {metrics['auc_pr']:.4f}")
    print(f"F1:      {metrics['f1_score']:.4f}")
    print(f"\nPlots saved to: {PLOTS_DIR}")
    print(f"Model artifacts: {MODEL_DIR}")


if __name__ == "__main__":
    raise SystemExit(main())
