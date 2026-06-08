# ══════════════════════════════════════════════════════════════════════════
# COMPLETE GNN MODEL LIBRARY
# ══════════════════════════════════════════════════════════════════════════
# Part A: Classic GINE models from literature (no plagiarism)
# Part B: Expert-inspired models with YOUR best hyperparameters
#
# Usage: Run this file to import all models and configurations
#        Or copy the classes you need into your notebook
# ══════════════════════════════════════════════════════════════════════════

"""
Extended GNN Model Library for Molecular Property Prediction

Contains:
- 5 classic GINE architectures from literature
- 6 tuned configurations based on your experimental results
- Unified training infrastructure

All models are standard implementations from published papers.
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GINEConv, global_mean_pool, global_add_pool
from torch_geometric.loader import DataLoader as GeoDataLoader
import copy
import numpy as np
from sklearn.metrics import r2_score, mean_squared_error
import math
import pandas as pd

# ══════════════════════════════════════════════════════════════════════════
# CLASSIC GINE ARCHITECTURES (LITERATURE-BASED)
# ══════════════════════════════════════════════════════════════════════════

class ClassicGINE(nn.Module):
    """
    Standard GINE following:
    - Xu et al. "How Powerful are Graph Neural Networks?" (ICLR 2019)
    - Hu et al. "Strategies for Pre-training GNNs" (ICLR 2020)

    Key features:
    - No edge encoder (passes raw edge features to GINEConv)
    - Standard 2-layer MLP in each GIN layer
    - No residual connections (classic approach)
    - Single pooling (mean or add)
    """
    def __init__(
        self,
        node_dim: int,
        edge_dim: int,
        hidden_dim: int = 256,
        num_layers: int = 5,
        dropout: float = 0.2,
        use_batchnorm: bool = True,
        pooling: str = 'mean',
    ):
        super().__init__()

        self.node_encoder = nn.Linear(node_dim, hidden_dim)

        self.convs = nn.ModuleList()
        self.batch_norms = nn.ModuleList()

        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, 2 * hidden_dim),
                nn.BatchNorm1d(2 * hidden_dim) if use_batchnorm else nn.Identity(),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(2 * hidden_dim, hidden_dim),
            )
            self.convs.append(GINEConv(mlp, edge_dim=edge_dim, train_eps=True))
            self.batch_norms.append(nn.BatchNorm1d(hidden_dim) if use_batchnorm else nn.Identity())

        self.pooling = pooling
        self.dropout = dropout

        self.graph_pred = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = data.x, data.edge_index, data.edge_attr, data.batch

        x = self.node_encoder(x)

        for conv, bn in zip(self.convs, self.batch_norms):
            x = conv(x, edge_index, edge_attr)
            x = bn(x)
            x = F.relu(x)

        if self.pooling == 'mean':
            x = global_mean_pool(x, batch)
        elif self.pooling == 'add':
            x = global_add_pool(x, batch)

        return self.graph_pred(x)


class JumpingKnowledgeGINE(nn.Module):
    """
    GIN with Jumping Knowledge Networks
    Reference: Xu et al. ICML 2018

    Key idea: Concatenate representations from ALL layers before pooling.
    Captures multi-scale structural information.
    """
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
        self.num_layers = num_layers

        self.convs = nn.ModuleList()
        self.batch_norms = nn.ModuleList()

        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, 2 * hidden_dim),
                nn.ReLU(),
                nn.Linear(2 * hidden_dim, hidden_dim),
            )
            self.convs.append(GINEConv(mlp, edge_dim=edge_dim, train_eps=True))
            self.batch_norms.append(nn.BatchNorm1d(hidden_dim))

        self.graph_pred = nn.Sequential(
            nn.Linear(hidden_dim * num_layers, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = data.x, data.edge_index, data.edge_attr, data.batch
        x = self.node_encoder(x)

        xs = []
        for conv, bn in zip(self.convs, self.batch_norms):
            x = F.relu(bn(conv(x, edge_index, edge_attr)))
            xs.append(global_mean_pool(x, batch))

        x = torch.cat(xs, dim=1)
        return self.graph_pred(x)


class VirtualNodeGINE(nn.Module):
    """
    GIN with Virtual Node (Graph-level super-node)
    Reference: Gilmer et al. ICML 2017, OGB implementation

    Key idea: Add a virtual "super-node" connected to all atoms.
    Helps with long-range dependencies in molecules.
    """
    def __init__(
        self,
        node_dim: int,
        edge_dim: int,
        hidden_dim: int = 256,
        num_layers: int = 5,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.node_encoder = nn.Linear(node_dim, hidden_dim)
        self.num_layers = num_layers

        self.virtualnode_embedding = nn.Embedding(1, hidden_dim)

        self.convs = nn.ModuleList()
        self.batch_norms = nn.ModuleList()
        self.mlp_virtualnode_list = nn.ModuleList()

        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, 2 * hidden_dim),
                nn.ReLU(),
                nn.Linear(2 * hidden_dim, hidden_dim),
            )
            self.convs.append(GINEConv(mlp, edge_dim=edge_dim, train_eps=True))
            self.batch_norms.append(nn.BatchNorm1d(hidden_dim))

            self.mlp_virtualnode_list.append(nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim),
                nn.BatchNorm1d(hidden_dim),
                nn.ReLU(),
                nn.Dropout(dropout),
            ))

        self.dropout = dropout
        self.graph_pred = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = data.x, data.edge_index, data.edge_attr, data.batch
        x = self.node_encoder(x)

        virtualnode_embedding = self.virtualnode_embedding(
            torch.zeros(batch.max().item() + 1, dtype=torch.long, device=x.device)
        )

        for layer in range(self.num_layers):
            x = x + virtualnode_embedding[batch]

            x = self.convs[layer](x, edge_index, edge_attr)
            x = self.batch_norms[layer](x)
            x = F.relu(x)

            virtualnode_embedding_temp = global_mean_pool(x, batch)
            virtualnode_embedding = virtualnode_embedding + F.dropout(
                self.mlp_virtualnode_list[layer](virtualnode_embedding_temp),
                p=self.dropout, training=self.training
            )

        return self.graph_pred(global_mean_pool(x, batch))


class DeepGINE(nn.Module):
    """
    Deep GIN with Residual Connections
    Reference: Standard ResNet-style connections for GNNs

    Key idea: Residual connections allow deeper networks without vanishing gradients.
    Good for complex patterns in small datasets.
    """
    def __init__(
        self,
        node_dim: int,
        edge_dim: int,
        hidden_dim: int = 256,
        num_layers: int = 8,
        dropout: float = 0.25,
    ):
        super().__init__()
        self.node_encoder = nn.Linear(node_dim, hidden_dim)

        self.convs = nn.ModuleList()
        self.batch_norms = nn.ModuleList()

        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, hidden_dim),
            )
            self.convs.append(GINEConv(mlp, edge_dim=edge_dim, train_eps=True))
            self.batch_norms.append(nn.BatchNorm1d(hidden_dim))

        self.dropout = dropout
        self.graph_pred = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = data.x, data.edge_index, data.edge_attr, data.batch
        x = self.node_encoder(x)

        for conv, bn in zip(self.convs, self.batch_norms):
            h = conv(x, edge_index, edge_attr)
            h = bn(h)
            h = F.relu(h)
            h = F.dropout(h, p=self.dropout, training=self.training)
            x = x + h

        return self.graph_pred(global_mean_pool(x, batch))


class DualPoolGINE(nn.Module):
    """
    GIN with Dual Pooling (mean + max concatenation)
    Reference: Common pattern in molecular GNNs

    Key idea: Combine mean and max pooling to capture both
    average properties and extreme values.
    """
    def __init__(
        self,
        node_dim: int,
        edge_dim: int,
        hidden_dim: int = 256,
        num_layers: int = 5,
        dropout: float = 0.2,
    ):
        super().__init__()
        from torch_geometric.nn import global_max_pool
        self.global_max_pool = global_max_pool

        self.node_encoder = nn.Linear(node_dim, hidden_dim)

        self.convs = nn.ModuleList()
        self.batch_norms = nn.ModuleList()

        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, 2 * hidden_dim),
                nn.ReLU(),
                nn.Linear(2 * hidden_dim, hidden_dim),
            )
            self.convs.append(GINEConv(mlp, edge_dim=edge_dim, train_eps=True))
            self.batch_norms.append(nn.BatchNorm1d(hidden_dim))

        self.graph_pred = nn.Sequential(
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = data.x, data.edge_index, data.edge_attr, data.batch
        x = self.node_encoder(x)

        for conv, bn in zip(self.convs, self.batch_norms):
            x = F.relu(bn(conv(x, edge_index, edge_attr)))

        x_mean = global_mean_pool(x, batch)
        x_max = self.global_max_pool(x, batch)
        x = torch.cat([x_mean, x_max], dim=1)

        return self.graph_pred(x)


# ══════════════════════════════════════════════════════════════════════════
# EXPERT-INSPIRED WITH YOUR BEST HYPERPARAMETERS
# ══════════════════════════════════════════════════════════════════════════

class SimplifiedGINE(nn.Module):
    """
    Simplified GINE based on expert's architecture but adapted to YOUR data.

    Key differences from expert:
    - No edge encoder (uses raw edge features)
    - Hyperparameters tuned to your scaffold results
    - Optional residual connections

    Based on your results showing:
    - Best scaffold R² = 0.743 with gine_nores_256_4l (no residual)
    - Larger hidden_dim (256) worked better than expert's 128
    - Higher dropout (0.25-0.3) better for scaffold generalization
    """
    def __init__(
        self,
        node_dim: int,
        edge_dim: int,
        hidden_dim: int = 256,
        num_layers: int = 4,
        dropout: float = 0.25,
        use_residual: bool = False,
        pooling: str = 'mean',
    ):
        super().__init__()
        self.use_residual = use_residual
        self.dropout = dropout
        self.pooling = pooling

        self.node_proj = nn.Linear(node_dim, hidden_dim)

        self.convs = nn.ModuleList()
        self.norms = nn.ModuleList()

        for _ in range(num_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, hidden_dim),
            )
            self.convs.append(GINEConv(mlp, edge_dim=edge_dim, train_eps=True))
            self.norms.append(nn.BatchNorm1d(hidden_dim))

        self.head = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = data.x, data.edge_index, data.edge_attr, data.batch

        x = self.node_proj(x)

        for conv, norm in zip(self.convs, self.norms):
            if self.use_residual:
                residual = x
                x = conv(x, edge_index, edge_attr)
                x = norm(x)
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)
                x = x + residual
            else:
                x = conv(x, edge_index, edge_attr)
                x = norm(x)
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)

        if self.pooling == 'add':
            x = global_add_pool(x, batch)
        else:
            x = global_mean_pool(x, batch)

        return self.head(x)


# ══════════════════════════════════════════════════════════════════════════
# CONFIGURATION LIBRARY
# ══════════════════════════════════════════════════════════════════════════

CLASSIC_CONFIGS = [
    dict(
        name='classic_gin_original',
        model_class=ClassicGINE,
        hidden_dim=256,
        num_layers=5,
        dropout=0.2,
        use_batchnorm=True,
        pooling='add',
        lr=1e-3,
        batch_size=32,
        epochs=300,
        patience=40,
        weight_decay=0.0,
        notes='Xu et al. ICLR 2019 - Original GIN'
    ),
    dict(
        name='classic_gin_mean',
        model_class=ClassicGINE,
        hidden_dim=256,
        num_layers=5,
        dropout=0.25,
        use_batchnorm=True,
        pooling='mean',
        lr=5e-4,
        batch_size=32,
        epochs=300,
        patience=40,
        weight_decay=1e-5,
        notes='Classic GIN adapted for regression'
    ),
    dict(
        name='jumping_knowledge_gin',
        model_class=JumpingKnowledgeGINE,
        hidden_dim=128,
        num_layers=4,
        dropout=0.25,
        lr=5e-4,
        batch_size=64,
        epochs=300,
        patience=40,
        weight_decay=1e-5,
        notes='Xu et al. ICML 2018 - Multi-scale features'
    ),
    dict(
        name='virtual_node_gin',
        model_class=VirtualNodeGINE,
        hidden_dim=256,
        num_layers=5,
        dropout=0.2,
        lr=5e-4,
        batch_size=32,
        epochs=350,
        patience=50,
        weight_decay=1e-5,
        notes='Gilmer et al. ICML 2017 - Long-range interactions'
    ),
    dict(
        name='deep_gin_residual',
        model_class=DeepGINE,
        hidden_dim=256,
        num_layers=8,
        dropout=0.3,
        lr=3e-4,
        batch_size=32,
        epochs=400,
        patience=50,
        weight_decay=1e-5,
        notes='Deep network with residual connections'
    ),
    dict(
        name='dual_pool_gin',
        model_class=DualPoolGINE,
        hidden_dim=256,
        num_layers=5,
        dropout=0.25,
        lr=5e-4,
        batch_size=32,
        epochs=300,
        patience=40,
        weight_decay=1e-5,
        notes='Mean + Max pooling concatenation'
    ),
]

TUNED_CONFIGS = [
    dict(
        name='best_from_experiments_nores',
        model_class=SimplifiedGINE,
        hidden_dim=256,
        num_layers=4,
        dropout=0.25,
        use_residual=False,
        pooling='mean',
        lr=3e-4,
        batch_size=32,
        epochs=350,
        patience=45,
        weight_decay=1e-5,
        notes='Based on your gine_nores_256_4l result (R²=0.743)'
    ),
    dict(
        name='best_high_dropout',
        model_class=SimplifiedGINE,
        hidden_dim=256,
        num_layers=4,
        dropout=0.35,
        use_residual=False,
        pooling='mean',
        lr=3e-4,
        batch_size=32,
        epochs=350,
        patience=45,
        weight_decay=1e-5,
        notes='More aggressive regularization for scaffold'
    ),
    dict(
        name='best_deeper',
        model_class=SimplifiedGINE,
        hidden_dim=256,
        num_layers=6,
        dropout=0.3,
        use_residual=False,
        pooling='mean',
        lr=2e-4,
        batch_size=32,
        epochs=400,
        patience=50,
        weight_decay=1e-5,
        notes='Deeper version of your best architecture'
    ),
    dict(
        name='best_with_residual',
        model_class=SimplifiedGINE,
        hidden_dim=256,
        num_layers=4,
        dropout=0.25,
        use_residual=True,
        pooling='mean',
        lr=3e-4,
        batch_size=32,
        epochs=350,
        patience=45,
        weight_decay=1e-5,
        notes='Your best params but WITH residual connections'
    ),
    dict(
        name='small_tuned',
        model_class=SimplifiedGINE,
        hidden_dim=128,
        num_layers=4,
        dropout=0.25,
        use_residual=False,
        pooling='mean',
        lr=3e-4,
        batch_size=64,
        epochs=350,
        patience=45,
        weight_decay=1e-5,
        notes='Expert size, your hyperparameters'
    ),
    dict(
        name='conservative_training',
        model_class=SimplifiedGINE,
        hidden_dim=256,
        num_layers=4,
        dropout=0.3,
        use_residual=False,
        pooling='mean',
        lr=1e-4,
        batch_size=64,
        epochs=500,
        patience=60,
        weight_decay=1e-6,
        notes='Conservative training for stable convergence'
    ),
]

ALL_CONFIGS = CLASSIC_CONFIGS + TUNED_CONFIGS


# ══════════════════════════════════════════════════════════════════════════
# UNIFIED TRAINING FUNCTION
# ══════════════════════════════════════════════════════════════════════════

def train_gnn_config(
    config: dict,
    train_loader,
    val_loader,
    test_loader,
    node_dim: int,
    edge_dim: int,
    device: torch.device = None,
    verbose: bool = True,
):
    """
    Train a single GNN configuration.

    Args:
        config: Configuration dictionary
        train_loader, val_loader, test_loader: PyG DataLoaders
        node_dim: Node feature dimension
        edge_dim: Edge feature dimension
        device: torch device
        verbose: Print progress

    Returns:
        dict with keys: rmse, r2, mae, best_epoch, model_state
    """
    if device is None:
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    model_class = config['model_class']
    model_kwargs = {k: v for k, v in config.items()
                   if k not in ['name', 'model_class', 'lr', 'batch_size', 'epochs',
                               'patience', 'weight_decay', 'notes']}

    model = model_class(node_dim=node_dim, edge_dim=edge_dim, **model_kwargs).to(device)

    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=config['lr'],
        weight_decay=config.get('weight_decay', 1e-5)
    )

    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=15, min_lr=1e-6
    )

    criterion = nn.MSELoss()

    best_val_loss = float('inf')
    best_state = None
    patience_counter = 0

    if verbose:
        print(f"\n{'='*70}")
        print(f"Training: {config['name']}")
        print(f"Model: {model_class.__name__}")
        print(f"Params: hidden={model_kwargs.get('hidden_dim')}, layers={model_kwargs.get('num_layers')}, dropout={model_kwargs.get('dropout')}")
        print(f"{'='*70}")

    for epoch in range(1, config['epochs'] + 1):
        # Train
        model.train()
        train_loss = 0.0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch)
            loss = criterion(out, batch.y.view(-1, 1))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            train_loss += loss.item() * batch.num_graphs

        train_loss /= len(train_loader.dataset)

        # Validate
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                out = model(batch)
                loss = criterion(out, batch.y.view(-1, 1))
                val_loss += loss.item() * batch.num_graphs

        val_loss /= len(val_loader.dataset)
        scheduler.step(val_loss)

        # Early stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = copy.deepcopy(model.state_dict())
            best_epoch = epoch
            patience_counter = 0
        else:
            patience_counter += 1

        if patience_counter >= config['patience']:
            if verbose:
                print(f"Early stopping at epoch {epoch}")
            break

        if verbose and epoch % 25 == 0:
            print(f"Epoch {epoch:3d} | Train: {train_loss:.4f} | Val: {val_loss:.4f} | Best: {best_val_loss:.4f}")

    # Evaluate on test set
    model.load_state_dict(best_state)
    model.eval()
    preds, targets = [], []

    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(device)
            out = model(batch)
            preds.append(out.cpu().numpy().flatten())
            targets.append(batch.y.cpu().numpy().flatten())

    preds = np.concatenate(preds)
    targets = np.concatenate(targets)

    rmse = math.sqrt(mean_squared_error(targets, preds))
    r2 = r2_score(targets, preds)
    mae = np.mean(np.abs(targets - preds))

    if verbose:
        print(f"\n{'─'*70}")
        print(f"TEST RESULTS: RMSE={rmse:.4f} | R²={r2:.4f} | MAE={mae:.4f}")
        print(f"Best epoch: {best_epoch}")
        print(f"{'─'*70}\n")

    return {
        'name': config['name'],
        'rmse': rmse,
        'r2': r2,
        'mae': mae,
        'best_epoch': best_epoch,
        'model_state': best_state,
        'notes': config.get('notes', ''),
    }


def run_experiments(
    configs,
    train_loader,
    val_loader,
    test_loader,
    node_dim,
    edge_dim,
):
    """
    Run multiple GNN experiments.

    Returns:
        DataFrame with all results
    """
    print(f"\n{'█'*70}")
    print(f"RUNNING {len(configs)} CONFIGURATIONS")
    print(f"{'█'*70}")

    results = []
    for i, config in enumerate(configs, 1):
        print(f"\n[{i}/{len(configs)}] {config['name']}")
        try:
            result = train_gnn_config(
                config,
                train_loader,
                val_loader,
                test_loader,
                node_dim=node_dim,
                edge_dim=edge_dim,
                verbose=True
            )
            results.append(result)
        except Exception as e:
            print(f"ERROR in {config['name']}: {e}")
            results.append({
                'name': config['name'],
                'rmse': float('nan'),
                'r2': float('nan'),
                'mae': float('nan'),
                'error': str(e),
            })

    df_results = pd.DataFrame(results)

    print(f"\n{'█'*70}")
    print(f"EXPERIMENT SUMMARY")
    print(f"{'█'*70}\n")
    print(df_results[['name', 'rmse', 'r2', 'mae']].sort_values('r2', ascending=False).to_string(index=False))
    print(f"\nBest R²: {df_results['r2'].max():.4f} ({df_results.loc[df_results['r2'].idxmax(), 'name']})")
    print(f"Best RMSE: {df_results['rmse'].min():.4f} ({df_results.loc[df_results['rmse'].idxmin(), 'name']})")

    return df_results


if __name__ == '__main__':
    print("GNN Model Library Loaded! ✓")
    print("\nAvailable configs:")
    print(f"  - {len(CLASSIC_CONFIGS)} classic literature-based models")
    print(f"  - {len(TUNED_CONFIGS)} models tuned to your data")
    print(f"  - {len(ALL_CONFIGS)} total configurations")
    print("\nUsage:")
    print("  from gnn_model_library import *")
    print("  results = run_experiments(TUNED_CONFIGS, train_loader, val_loader, test_loader, node_dim=30, edge_dim=10)")
