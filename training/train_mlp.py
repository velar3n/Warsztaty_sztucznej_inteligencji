import logging
from pathlib import Path
import copy
import json
import pickle
import tempfile
import os

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score

import mlflow
import mlflow.pytorch


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Set device
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
logger.info(f"Using device: {DEVICE}")

SEED = 42
torch.manual_seed(SEED)
np.random.seed(SEED)


class MLP(nn.Module):
    """Multi-layer perceptron for regression."""

    def __init__(
        self,
        input_dim: int,
        hidden_dims: list[int],
        dropout: float = 0.3,
        use_batchnorm: bool = True,
        activation: str = 'relu',
    ):
        super().__init__()
        act_fn = {'relu': nn.ReLU, 'gelu': nn.GELU, 'silu': nn.SiLU}[activation]

        layers = []
        in_dim = input_dim
        for h_dim in hidden_dims:
            layers.append(nn.Linear(in_dim, h_dim))
            if use_batchnorm:
                layers.append(nn.BatchNorm1d(h_dim))
            layers.append(act_fn())
            layers.append(nn.Dropout(dropout))
            in_dim = h_dim
        layers.append(nn.Linear(in_dim, 1))

        self.net = nn.Sequential(*layers)

    def forward(self, x):
        return self.net(x)


def evaluate_regression(y_true, y_pred):
    """Compute regression metrics."""
    mse = mean_squared_error(y_true, y_pred)
    return {
        "rmse": np.sqrt(mse),
        "r2": r2_score(y_true, y_pred),
        "mae": np.mean(np.abs(y_true - y_pred)),
    }


def early_stop_check(val_losses: list[float], patience: int = 20) -> bool:
    """Return True if val loss hasn't improved in `patience` epochs."""
    if len(val_losses) <= patience:
        return False
    return min(val_losses[:-patience]) <= min(val_losses[-patience:])


def mismatch_analysis(y_true, y_pred, smiles_list, label="model",
                      top_k=20, log_to_mlflow=True):
    """
    Analyze prediction errors and log to MLflow.
    Creates scatter plot, residual distribution, worst predictions table,
    and per-bucket metrics (low/mid/high pIC50).
    """
    residuals = y_pred - y_true
    abs_err = np.abs(residuals)

    # Scatter: predicted vs true
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    ax = axes[0]
    ax.scatter(y_true, y_pred, alpha=0.35, s=10, color='steelblue')
    lo, hi = min(y_true.min(), y_pred.min()), max(y_true.max(), y_pred.max())
    ax.plot([lo, hi], [lo, hi], 'r--', lw=1.5, label='y=x')
    ax.set_xlabel('True pIC50')
    ax.set_ylabel('Predicted pIC50')
    ax.set_title(f'{label} — Predicted vs True')
    ax.legend()

    # Residual histogram
    ax2 = axes[1]
    ax2.hist(residuals, bins=40, color='steelblue', edgecolor='white', alpha=0.8)
    ax2.axvline(0, color='red', lw=1.5, linestyle='--')
    ax2.set_xlabel('Residual (pred − true)')
    ax2.set_ylabel('Count')
    ax2.set_title(f'{label} — Residual Distribution\n'
                  f'mean={residuals.mean():.3f}  std={residuals.std():.3f}')
    plt.tight_layout()

    with tempfile.TemporaryDirectory() as td:
        fig_path = os.path.join(td, f'{label}_mismatch_scatter.png')
        plt.savefig(fig_path, dpi=120, bbox_inches='tight')
        if log_to_mlflow:
            mlflow.log_artifact(fig_path, artifact_path='mismatch')
    plt.close()

    # Top-k worst predictions table
    worst_idx = np.argsort(abs_err)[::-1][:top_k]
    df_worst = pd.DataFrame({
        'smiles': [smiles_list[i] for i in worst_idx],
        'true_pic50': y_true[worst_idx],
        'pred_pic50': y_pred[worst_idx],
        'abs_error': abs_err[worst_idx],
        'residual': residuals[worst_idx],
    })
    logger.info(f"\nTop-{top_k} worst predictions ({label}):")
    logger.info(f"\n{df_worst.to_string(index=False)}")

    with tempfile.TemporaryDirectory() as td:
        csv_path = os.path.join(td, f'{label}_worst_{top_k}.csv')
        df_worst.to_csv(csv_path, index=False)
        if log_to_mlflow:
            mlflow.log_artifact(csv_path, artifact_path='mismatch')

    # Per-bucket RMSE
    buckets = {
        'low (<6)': y_true < 6,
        'mid (6-8)': (y_true >= 6) & (y_true <= 8),
        'high (>8)': y_true > 8
    }
    bucket_metrics = {}
    for name, mask in buckets.items():
        if mask.sum() > 0:
            bm = evaluate_regression(y_true[mask], y_pred[mask])
            bucket_metrics[name] = bm
            logger.info(f"  {name:15s}  n={mask.sum():4d}  "
                       f"RMSE={bm['rmse']:.3f}  R²={bm['r2']:.3f}")
            if log_to_mlflow:
                safe = name.replace(' ', '_').replace('(', '').replace(')', '') \
                          .replace('<', 'lt').replace('>', 'gt')
                mlflow.log_metrics({
                    f'{label}_bucket_{safe}_rmse': bm['rmse'],
                    f'{label}_bucket_{safe}_r2': bm['r2'],
                })

    return df_worst, bucket_metrics


class MLPTrainer:
    """Handles MLP model training with MLflow logging."""

    def __init__(self, config: dict, data_dir: Path, split_dir: Path, models_dir: Path):
        """
        Initialize MLP trainer.

        Args:
            config: Training configuration
            data_dir: Directory containing prepared data
            split_dir: Directory containing split indices
            models_dir: Directory to save trained models
        """
        self.config = config
        self.data_dir = data_dir
        self.split_dir = split_dir
        self.models_dir = models_dir
        self.models_dir.mkdir(exist_ok=True)

        logger.info("Initialized MLPTrainer")
        logger.info(f"Config: {config}")

    def load_split_data(self, split_type: str):
        """Load data with scaffold split."""
        logger.info(f"Loading data with split: {split_type}")

        # Load features
        mlp_parquet = self.data_dir / 'mlp_features_2147_mlp.parquet'
        df_mlp_feat = pd.read_parquet(mlp_parquet)
        logger.info(f"Loaded MLP features: {df_mlp_feat.shape}")

        # Extract features and targets
        feature_cols = [c for c in df_mlp_feat.columns
                       if c not in ('canonical_smiles', 'pic50', 'n_measurements', 'mol_id')]
        X = df_mlp_feat[feature_cols].values.astype(np.float32)
        y = df_mlp_feat['pic50'].values.astype(np.float32)

        # Load split indices
        tr_idx = np.load(self.split_dir / f'{split_type}_train.npy')
        val_idx = np.load(self.split_dir / f'{split_type}_val.npy')
        te_idx = np.load(self.split_dir / f'{split_type}_test.npy')

        logger.info(f"Split sizes - train: {len(tr_idx)}, val: {len(val_idx)}, test: {len(te_idx)}")

        # Normalize physicochemical features (last 8 columns)
        fp_bits = 2048
        scaler = StandardScaler()
        X[tr_idx, fp_bits:] = scaler.fit_transform(X[tr_idx, fp_bits:])
        X[val_idx, fp_bits:] = scaler.transform(X[val_idx, fp_bits:])
        X[te_idx, fp_bits:] = scaler.transform(X[te_idx, fp_bits:])

        # Convert to tensors
        to_t = lambda idx: (
            torch.tensor(X[idx], dtype=torch.float32),
            torch.tensor(y[idx], dtype=torch.float32).unsqueeze(1),
        )

        return (*to_t(tr_idx), *to_t(val_idx), *to_t(te_idx), scaler, tr_idx, val_idx, te_idx, df_mlp_feat)

    def train(self, split_type: str = 'scaffold'):
        """Train MLP model with MLflow logging."""
        logger.info("Starting MLP training")

        # Load data
        X_tr, y_tr, X_val, y_val, X_te, y_te, scaler, tr_idx, val_idx, te_idx, df_mlp_feat = \
            self.load_split_data(split_type)

        # Create data loaders
        train_loader = DataLoader(
            TensorDataset(X_tr, y_tr),
            batch_size=self.config['batch_size'],
            shuffle=True,
        )
        val_loader = DataLoader(
            TensorDataset(X_val, y_val),
            batch_size=256,
            shuffle=False,
        )

        # Build model
        model = MLP(
            input_dim=X_tr.shape[1],
            hidden_dims=self.config['hidden_dims'],
            dropout=self.config['dropout'],
        ).to(DEVICE)

        logger.info(f"Model architecture: {model}")
        logger.info(f"Total parameters: {sum(p.numel() for p in model.parameters()):,}")

        # Optimizer and scheduler
        optimizer = torch.optim.Adam(
            model.parameters(),
            lr=self.config['lr'],
            weight_decay=1e-5
        )
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, patience=10, factor=0.5
        )
        loss_fn = nn.HuberLoss(delta=1.0)

        # MLflow tracking
        mlflow.set_experiment("FinalModels_MLP")
        run_name = f"mlp_{split_type}_{'_'.join(map(str, self.config['hidden_dims']))}_{self.config['dropout']}"

        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
            logger.info(f"MLflow run ID: {run_id}")

            # Log parameters
            mlflow.log_params({
                **self.config,
                'split': split_type,
                'model_type': 'MLP',
                'device': str(DEVICE),
                'total_params': sum(p.numel() for p in model.parameters()),
            })

            # Training loop
            best_val_loss = float('inf')
            best_state = None
            val_losses = []

            for epoch in range(1, self.config['epochs'] + 1):
                # Train
                model.train()
                train_loss = 0.0
                for xb, yb in train_loader:
                    xb, yb = xb.to(DEVICE), yb.to(DEVICE)
                    optimizer.zero_grad()
                    loss = loss_fn(model(xb), yb)
                    loss.backward()
                    optimizer.step()
                    train_loss += loss.item() * len(xb)
                train_loss /= len(train_loader.dataset)

                # Validate
                model.eval()
                val_loss = 0.0
                with torch.no_grad():
                    for xb, yb in val_loader:
                        xb, yb = xb.to(DEVICE), yb.to(DEVICE)
                        val_loss += loss_fn(model(xb), yb).item() * len(xb)
                val_loss /= len(val_loader.dataset)
                val_losses.append(val_loss)
                scheduler.step(val_loss)

                # Log metrics
                mlflow.log_metrics({
                    'train_loss': train_loss,
                    'val_loss': val_loss,
                    'learning_rate': optimizer.param_groups[0]['lr']
                }, step=epoch)

                if epoch % 10 == 0:
                    logger.info(f"Epoch {epoch:3d}: train_loss={train_loss:.4f}, "
                              f"val_loss={val_loss:.4f}")

                # Save best model
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    best_state = copy.deepcopy(model.state_dict())

                # Early stopping
                if early_stop_check(val_losses, self.config['patience']):
                    logger.info(f"Early stop at epoch {epoch}")
                    break

            # Test evaluation with best weights
            model.load_state_dict(best_state)
            model.eval()
            with torch.no_grad():
                y_pred_test = model(X_te.to(DEVICE)).cpu().numpy().flatten()
            y_true_test = y_te.numpy().flatten()

            # Compute test metrics
            test_metrics = evaluate_regression(y_true_test, y_pred_test)
            mlflow.log_metrics({f'test_{k}': v for k, v in test_metrics.items()})

            logger.info("Test Results:")
            logger.info(f"RMSE: {test_metrics['rmse']:.3f}")
            logger.info(f"R²: {test_metrics['r2']:.3f}")
            logger.info(f"MAE: {test_metrics['mae']:.3f}")

            # Mismatch analysis
            test_smiles = df_mlp_feat.iloc[te_idx]['canonical_smiles'].tolist()
            df_worst, bucket_metrics = mismatch_analysis(
                y_true_test, y_pred_test,
                smiles_list=test_smiles,
                label='mlp_final',
            )

            # Log model to MLflow
            mlflow.pytorch.log_model(model, name="model")

            # Save model checkpoint
            self.save_model(model, X_tr.shape[1], scaler, test_metrics, split_type)

            logger.info("MLP training complete")

    def save_model(self, model, input_dim, scaler, test_metrics, split_type):
        """Save model checkpoint and inference artifacts."""
        logger.info("Saving model artifacts...")

        # Model checkpoint
        ckpt = {
            'model_state_dict': model.state_dict(),
            'arch': {
                'input_dim': input_dim,
                'hidden_dims': self.config['hidden_dims'],
                'dropout': self.config['dropout'],
            },
            'config': self.config,
            'split': split_type,
            'test_metrics': test_metrics,
        }
        torch.save(ckpt, self.models_dir / 'mlp_final.pt')
        logger.info(f"Saved checkpoint: {self.models_dir / 'mlp_final.pt'}")

        # Scaler for inference
        with open(self.models_dir / 'mlp_scaler.pkl', 'wb') as f:
            pickle.dump(scaler, f)
        logger.info(f"Saved scaler: {self.models_dir / 'mlp_scaler.pkl'}")

        # Physicochemical column order
        physchem_cols = [
            'mw_freebase', 'alogp', 'hba', 'hbd',
            'psa', 'rtb', 'aromatic_rings', 'qed_weighted'
        ]
        with open(self.models_dir / 'physchem_cols.json', 'w') as f:
            json.dump(physchem_cols, f)
        logger.info(f"Saved physchem_cols: {self.models_dir / 'physchem_cols.json'}")


if __name__ == "__main__":
    # Paths
    DATA_DIR = Path(__file__).parent.parent / 'data'
    SPLIT_DIR = DATA_DIR / 'splits_experiment'
    MODELS_DIR = Path(__file__).parent.parent / 'models'

    # Best MLP configuration
    BEST_CONFIG = {
        'hidden_dims': [512, 256],
        'dropout': 0.2,
        'lr': 1e-3,
        'batch_size': 64,
        'epochs': 200,
        'patience': 30,
        'name': 'selected_mlp',
    }

    # Train
    trainer = MLPTrainer(BEST_CONFIG, DATA_DIR, SPLIT_DIR, MODELS_DIR)
    trainer.train(split_type='scaffold')
