import logging
from pathlib import Path
import copy
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
import torch.nn.functional as F
from torch_geometric.loader import DataLoader as GeoDataLoader
from torch_geometric.nn import GINEConv, global_mean_pool

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


class GINEModel(nn.Module):
    """Graph Isomorphism Network with Edge features."""

    def __init__(
        self,
        node_dim: int,
        edge_dim: int,
        hidden_dim: int = 256,
        num_layers: int = 4,
        dropout: float = 0.2,
    ):
        super().__init__()

        self.node_encoder = nn.Linear(node_dim, hidden_dim)
        self.edge_encoder = nn.Linear(edge_dim, hidden_dim)

        self.num_layers = num_layers

        self.convs = nn.ModuleList()
        self.batch_norms = nn.ModuleList()

        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, 2 * hidden_dim),
                nn.ReLU(),
                nn.Linear(2 * hidden_dim, hidden_dim),
            )

            self.convs.append(
                GINEConv(mlp, edge_dim=hidden_dim, train_eps=True)
            )
            self.batch_norms.append(nn.BatchNorm1d(hidden_dim))

        self.pool = global_mean_pool

        self.graph_head = nn.Sequential(
            nn.Linear(hidden_dim * num_layers, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = (
            data.x, data.edge_index, data.edge_attr, data.batch
        )

        x = self.node_encoder(x)
        edge_attr = self.edge_encoder(edge_attr)

        layer_outputs = []

        for conv, bn in zip(self.convs, self.batch_norms):
            x = conv(x, edge_index, edge_attr)
            x = bn(x)
            x = F.relu(x)

            layer_outputs.append(self.pool(x, batch))

        x = torch.cat(layer_outputs, dim=1)
        return self.graph_head(x)


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


class GNNTrainer:
    """Handles GNN model training with MLflow logging."""

    def __init__(self, config: dict, data_dir: Path, split_dir: Path, models_dir: Path):
        """
        Initialize GNN trainer.

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

        logger.info("Initialized GNNTrainer")
        logger.info(f"Config: {config}")

    def load_split_data(self, split_type: str):
        """Load GNN graphs with scaffold split."""
        logger.info(f"Loading data with split: {split_type}")

        # Load graphs
        gnn_pt = self.data_dir / 'gnn_graphs_2147_gnn.pt'
        graphs = torch.load(gnn_pt, weights_only=False)
        logger.info(f"Loaded GNN graphs: {len(graphs)} molecules")

        # Load split indices
        tr_idx = np.load(self.split_dir / f'{split_type}_train.npy')
        val_idx = np.load(self.split_dir / f'{split_type}_val.npy')
        te_idx = np.load(self.split_dir / f'{split_type}_test.npy')

        logger.info(f"Split sizes - train: {len(tr_idx)}, val: {len(val_idx)}, test: {len(te_idx)}")

        # Select graphs
        def select(idx):
            return [graphs[i] for i in idx]

        tr_graphs = select(tr_idx)
        val_graphs = select(val_idx)
        te_graphs = select(te_idx)

        # Get feature dimensions
        node_dim = tr_graphs[0].x.shape[1]
        edge_dim = tr_graphs[0].edge_attr.shape[1]
        phys_dim = tr_graphs[0].phys.shape[1]

        logger.info(f"Feature dimensions - node: {node_dim}, edge: {edge_dim}, phys: {phys_dim}")

        return tr_graphs, val_graphs, te_graphs, node_dim, edge_dim, phys_dim, te_idx

    def train(self, split_type: str = 'scaffold'):
        """Train GNN model with MLflow logging."""
        logger.info("Starting GNN training")

        # Load data
        tr_graphs, val_graphs, te_graphs, node_dim, edge_dim, phys_dim, te_idx = \
            self.load_split_data(split_type)

        # Create data loaders
        train_loader = GeoDataLoader(
            tr_graphs,
            batch_size=self.config['batch_size'],
            shuffle=True
        )
        val_loader = GeoDataLoader(val_graphs, batch_size=128, shuffle=False)
        test_loader = GeoDataLoader(te_graphs, batch_size=128, shuffle=False)

        # Build model
        model = GINEModel(
            node_dim=node_dim,
            edge_dim=edge_dim,
            hidden_dim=self.config['hidden_dim'],
            num_layers=self.config['n_layers'],
            dropout=self.config['dropout'],
        ).to(DEVICE)

        logger.info(f"Model architecture: {model}")
        logger.info(f"Total parameters: {sum(p.numel() for p in model.parameters()):,}")

        # Optimizer and scheduler
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=self.config['lr'],
            weight_decay=self.config.get('wd', 1e-4),
        )
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            T_max=self.config['epochs'],
            eta_min=self.config['lr'] * 0.01,
        )
        loss_fn = nn.HuberLoss(delta=1.0)

        # MLflow tracking
        mlflow.set_experiment("FinalModels_GNN")
        run_name = (f"gine_{split_type}_{self.config['hidden_dim']}h_"
                   f"{self.config['n_layers']}l")

        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
            logger.info(f"MLflow run ID: {run_id}")

            # Log parameters
            mlflow.log_params({
                **self.config,
                'split': split_type,
                'device': str(DEVICE),
                'total_params': sum(p.numel() for p in model.parameters()),
                'node_dim': node_dim,
                'edge_dim': edge_dim,
                'phys_dim': phys_dim,
            })

            # Training loop
            best_val_loss = float('inf')
            best_state = None
            val_losses = []

            for epoch in range(1, self.config['epochs'] + 1):
                # Train
                model.train()
                train_loss = 0.0

                for batch in train_loader:
                    batch = batch.to(DEVICE)

                    optimizer.zero_grad()
                    pred = model(batch)

                    loss = loss_fn(pred, batch.y.unsqueeze(1))
                    loss.backward()

                    nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                    optimizer.step()

                    train_loss += loss.item() * batch.num_graphs

                train_loss /= len(tr_graphs)
                scheduler.step()

                # Validate
                model.eval()
                val_loss = 0.0

                with torch.no_grad():
                    for batch in val_loader:
                        batch = batch.to(DEVICE)
                        val_loss += loss_fn(
                            model(batch),
                            batch.y.unsqueeze(1)
                        ).item() * batch.num_graphs

                val_loss /= len(val_graphs)
                val_losses.append(val_loss)

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

            y_preds, y_trues = [], []

            with torch.no_grad():
                for batch in test_loader:
                    batch = batch.to(DEVICE)
                    y_preds.append(model(batch).cpu().numpy().ravel())
                    y_trues.append(batch.y.cpu().numpy().ravel())

            y_pred_test = np.concatenate(y_preds)
            y_true_test = np.concatenate(y_trues)

            # Compute test metrics
            test_metrics = evaluate_regression(y_true_test, y_pred_test)
            mlflow.log_metrics({f'test_{k}': v for k, v in test_metrics.items()})

            logger.info("Test Results:")
            logger.info(f"RMSE: {test_metrics['rmse']:.3f}")
            logger.info(f"R²: {test_metrics['r2']:.3f}")
            logger.info(f"MAE: {test_metrics['mae']:.3f}")

            # Mismatch analysis
            test_smiles = [te_graphs[i].smiles for i in range(len(te_graphs))]
            df_worst, bucket_metrics = mismatch_analysis(
                y_true_test, y_pred_test,
                smiles_list=test_smiles,
                label='gine_final',
            )

            # Log model to MLflow
            mlflow.pytorch.log_model(model, name="model")

            # Save model checkpoint
            self.save_model(model, node_dim, edge_dim, phys_dim, test_metrics, split_type)

            logger.info("GNN training complete")

    def save_model(self, model, node_dim, edge_dim, phys_dim, test_metrics, split_type):
        """Save model checkpoint and inference artifacts."""
        logger.info("Saving model artifacts...")

        # Model checkpoint
        ckpt = {
            'model_state_dict': model.state_dict(),
            'arch': {
                'node_dim': node_dim,
                'edge_dim': edge_dim,
                'hidden_dim': self.config['hidden_dim'],
                'num_layers': self.config['n_layers'],
                'dropout': self.config['dropout'],
            },
            'config': self.config,
            'split': split_type,
            'test_metrics': test_metrics,
            'node_dim': node_dim,
            'edge_dim': edge_dim,
            'phys_dim': phys_dim,
        }
        torch.save(ckpt, self.models_dir / 'gine_final.pt')
        logger.info(f"Saved checkpoint: {self.models_dir / 'gine_final.pt'}")
        logger.info("Note: Physchem scaler saved during data preparation")


if __name__ == "__main__":
    # Paths
    DATA_DIR = Path(__file__).parent.parent / 'data'
    SPLIT_DIR = DATA_DIR / 'splits'
    MODELS_DIR = Path(__file__).parent.parent / 'models'

    # Best GNN configuration
    BEST_CONFIG = {
        'model_type': 'gine',
        'use_physchem': True,
        'use_residual': True,
        'hidden_dim': 512,
        'n_layers': 4,
        'dropout': 0.3,
        'lr': 1e-3,
        'batch_size': 32,
        'epochs': 250,
        'patience': 35,
        'readout_mlp': [512, 256],
        'name': 'gine_mixed_512_4l',
        'wd': 1e-4,
    }

    # Train
    trainer = GNNTrainer(BEST_CONFIG, DATA_DIR, SPLIT_DIR, MODELS_DIR)
    trainer.train(split_type='scaffold')
