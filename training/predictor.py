"""
predictor.py — pIC50 regression predictor for CHEMBL2147
=========================================================
Two models are supported, both trained on a scaffold split:

  • GNN   — GNNRegressor (GINE + edge_encoder + residual, node/bond RDKit features)
  • MLP   — deep MLP with ECFP4 Morgan fingerprints (2048-bit, radius=2)

Public API (call from Streamlit or any LLM-tool backend):

    from predictor import Predictor

    p = Predictor.load("models/")          # load saved weights
    result = p.predict("CCO")              # single SMILES
    result = p.predict(["CCO", "c1ccccc1"])  # batch

    # result is always a list of dicts:
    # [{"smiles": "CCO", "pic50": 6.12, "model": "gnn", "valid": True}, ...]
    # invalid SMILES → {"smiles": "???", "pic50": None, "model": "gnn", "valid": False, "error": "..."}

Training (run once from the notebook or CLI):

    from predictor import train_and_save
    train_and_save(df_dedup, save_dir="models/")
"""

from __future__ import annotations

import copy
import json
import math
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Union

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from rdkit import Chem
from rdkit.Chem import AllChem
from rdkit.Chem import rdPartialCharges
from rdkit.Chem.Scaffolds import MurckoScaffold
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader as GeoDataLoader
from torch_geometric.nn import GINEConv, global_mean_pool
from torch.utils.data import DataLoader, TensorDataset

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Constants — must match the notebook featurisation exactly
# ---------------------------------------------------------------------------

ATOM_TYPES = ["C", "N", "O", "S", "F", "Cl", "Br", "I", "P", "Si", "B", "other"]
HYBRIDIZATIONS = [
    Chem.rdchem.HybridizationType.SP,
    Chem.rdchem.HybridizationType.SP2,
    Chem.rdchem.HybridizationType.SP3,
    Chem.rdchem.HybridizationType.SP3D,
    Chem.rdchem.HybridizationType.SP3D2,
]
CHIRAL_TAGS = [
    Chem.rdchem.ChiralType.CHI_UNSPECIFIED,
    Chem.rdchem.ChiralType.CHI_TETRAHEDRAL_CW,
    Chem.rdchem.ChiralType.CHI_TETRAHEDRAL_CCW,
    Chem.rdchem.ChiralType.CHI_OTHER,
]
BOND_TYPES = [
    Chem.rdchem.BondType.SINGLE,
    Chem.rdchem.BondType.DOUBLE,
    Chem.rdchem.BondType.TRIPLE,
    Chem.rdchem.BondType.AROMATIC,
]
BOND_STEREO = [
    Chem.rdchem.BondStereo.STEREONONE,
    Chem.rdchem.BondStereo.STEREOE,
    Chem.rdchem.BondStereo.STEREOZ,
    Chem.rdchem.BondStereo.STEREOANY,
]

NODE_DIM = 30   # atom feature dimensionality
EDGE_DIM = 10   # bond feature dimensionality
FP_BITS  = 2048
FP_RADIUS = 2

SEED = 42
VAL_FRAC  = 0.10
TEST_FRAC = 0.20

# ---------------------------------------------------------------------------
# Featurisation helpers
# ---------------------------------------------------------------------------

def _one_hot(value, choices: list) -> list[int]:
    enc = [0] * len(choices)
    idx = choices.index(value) if value in choices else len(choices) - 1
    enc[idx] = 1
    return enc


def _atom_features(atom, ring_info) -> list[float]:
    """30-dim atom feature vector (matches notebook cell 13)."""
    symbol = atom.GetSymbol()
    atom_idx = atom.GetIdx()

    smallest_ring = 0
    if ring_info is not None:
        for ring in ring_info.AtomRings():
            if atom_idx in ring:
                size = len(ring)
                if smallest_ring == 0 or size < smallest_ring:
                    smallest_ring = size

    try:
        gasteiger = float(atom.GetDoubleProp("_GasteigerCharge"))
        if gasteiger != gasteiger:
            gasteiger = 0.0
        gasteiger = max(-1.0, min(1.0, gasteiger))
    except KeyError:
        gasteiger = 0.0

    return [
        *_one_hot(symbol, ATOM_TYPES),                       # 12
        atom.GetAtomicNum() / 53.0,                          #  1
        atom.GetDegree() / 6.0,                              #  1
        atom.GetFormalCharge() / 4.0,                        #  1
        atom.GetTotalNumHs() / 4.0,                          #  1
        int(atom.IsInRing()),                                #  1
        int(atom.GetIsAromatic()),                           #  1
        *_one_hot(atom.GetHybridization(), HYBRIDIZATIONS),  #  5
        *_one_hot(atom.GetChiralTag(), CHIRAL_TAGS),         #  4
        gasteiger,                                           #  1
        smallest_ring / 8.0,                                 #  1
        atom.GetDegree() / 6.0,                              #  1
    ]


def _bond_features(bond) -> list[float]:
    """10-dim bond feature vector (matches notebook cell 13)."""
    return [
        *_one_hot(bond.GetBondType(), BOND_TYPES),   # 4
        *_one_hot(bond.GetStereo(), BOND_STEREO),    # 4
        int(bond.IsInRing()),                        # 1
        int(bond.GetIsConjugated()),                 # 1
    ]


def smiles_to_graph(smi: str, y_val: float = 0.0) -> Data | None:
    """
    Convert a SMILES string to a PyG Data object.
    Returns None for invalid / unparseable SMILES.
    y_val is set to 0.0 at inference time (ignored).
    """
    mol = Chem.MolFromSmiles(smi)
    if mol is None or mol.GetNumAtoms() == 0:
        return None

    try:
        rdPartialCharges.ComputeGasteigerCharges(mol)
    except Exception:
        pass

    ring_info = mol.GetRingInfo()
    node_feats = torch.tensor(
        [_atom_features(a, ring_info) for a in mol.GetAtoms()], dtype=torch.float
    )

    src, dst, edge_feats = [], [], []
    for bond in mol.GetBonds():
        i, j = bond.GetBeginAtomIdx(), bond.GetEndAtomIdx()
        feats = _bond_features(bond)
        src  += [i, j]
        dst  += [j, i]
        edge_feats += [feats, feats]

    if len(src) == 0:
        src, dst = [0], [0]
        edge_feats = [[0] * EDGE_DIM]

    return Data(
        x          = node_feats,
        edge_index = torch.tensor([src, dst], dtype=torch.long),
        edge_attr  = torch.tensor(edge_feats, dtype=torch.float),
        y          = torch.tensor([y_val], dtype=torch.float),
        smiles     = smi,
    )


def smiles_to_fp(smi: str) -> np.ndarray | None:
    """ECFP4 Morgan fingerprint as float32 numpy array, or None if invalid."""
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        return None
    fp = AllChem.GetMorganFingerprintAsBitVect(mol, radius=FP_RADIUS, nBits=FP_BITS)
    return np.array(fp, dtype=np.float32)


def canonicalise(smi: str) -> str | None:
    """Return RDKit canonical SMILES, or None if invalid."""
    mol = Chem.MolFromSmiles(smi)
    return Chem.MolToSmiles(mol) if mol else None


# ---------------------------------------------------------------------------
# Scaffold split (test-first greedy — matches their pipeline.py)
# ---------------------------------------------------------------------------

def _get_scaffold(smi: str) -> str:
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        return smi
    sc = MurckoScaffold.MurckoScaffoldSmiles(mol=mol, includeChirality=False)
    return sc if sc else smi


def scaffold_split(smiles_list: list[str], val_frac=VAL_FRAC, test_frac=TEST_FRAC, seed=SEED):
    """
    Greedy test-first scaffold split.
    Returns (train_idx, val_idx, test_idx) as numpy int arrays.
    """
    scaffold_to_idx: dict[str, list[int]] = defaultdict(list)
    for i, smi in enumerate(smiles_list):
        scaffold_to_idx[_get_scaffold(smi)].append(i)

    groups = sorted(scaffold_to_idx.values(), key=len, reverse=True)
    N = len(smiles_list)
    target_test = int(N * test_frac)
    target_val  = int(N * val_frac)

    train_idx, val_idx, test_idx = [], [], []
    counts = {"train": 0, "val": 0, "test": 0}

    for group in groups:
        need_test = target_test - counts["test"]
        need_val  = target_val  - counts["val"]
        if need_test > 0 and need_test >= need_val:
            test_idx.extend(group);  counts["test"]  += len(group)
        elif need_val > 0:
            val_idx.extend(group);   counts["val"]   += len(group)
        else:
            train_idx.extend(group); counts["train"] += len(group)

    return np.array(train_idx), np.array(val_idx), np.array(test_idx)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class GNNRegressor(nn.Module):
    """
    GINE with edge_encoder + residual connections + BatchNorm.
    Best config from experiments: hidden_dim=256, n_layers=4, dropout=0.15, pooling='mean'.
    """
    def __init__(
        self,
        node_dim:   int = NODE_DIM,
        edge_dim:   int = EDGE_DIM,
        hidden_dim: int = 256,
        n_layers:   int = 4,
        dropout:    float = 0.15,
        pooling:    str = "mean",
    ):
        super().__init__()
        assert pooling in {"mean", "add"}
        self.pooling   = pooling
        self.dropout_p = dropout

        self.node_proj = nn.Linear(node_dim, hidden_dim)
        self.edge_encoder = nn.Sequential(
            nn.Linear(edge_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
        )

        self.convs = nn.ModuleList()
        self.norms = nn.ModuleList()
        for _ in range(n_layers):
            mlp = nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, hidden_dim),
            )
            self.convs.append(GINEConv(mlp, edge_dim=hidden_dim))
            self.norms.append(nn.BatchNorm1d(hidden_dim))

        self.head = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(self, data):
        x, edge_index, edge_attr, batch = (
            data.x, data.edge_index, data.edge_attr, data.batch
        )
        x         = self.node_proj(x)
        edge_attr = self.edge_encoder(edge_attr)

        for conv, norm in zip(self.convs, self.norms):
            residual = x
            x = conv(x, edge_index, edge_attr)
            x = norm(x)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout_p, training=self.training)
            x = x + residual

        x = global_mean_pool(x, batch) if self.pooling == "mean" else \
            torch.nn.functional.max_pool1d(x.unsqueeze(0), x.shape[0]).squeeze()
        return self.head(x)


class MLPRegressor(nn.Module):
    """
    MLP on ECFP4 fingerprints.
    Best config from experiments: deep_3/gelu — [1024, 512, 256], dropout=0.3, GELU.
    """
    def __init__(self, input_dim: int = FP_BITS, hidden_dims=(1024, 512, 256), dropout: float = 0.3):
        super().__init__()
        layers = []
        in_d = input_dim
        for h in hidden_dims:
            layers += [nn.Linear(in_d, h), nn.BatchNorm1d(h), nn.GELU(), nn.Dropout(dropout)]
            in_d = h
        layers.append(nn.Linear(in_d, 1))
        self.net = nn.Sequential(*layers)

        for m in self.modules():
            if isinstance(m, nn.Linear):
                nn.init.kaiming_normal_(m.weight, nonlinearity="relu")
                nn.init.constant_(m.bias, 0)

    def forward(self, x):
        return self.net(x)


# ---------------------------------------------------------------------------
# Training helpers
# ---------------------------------------------------------------------------

def _evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    rmse = math.sqrt(mean_squared_error(y_true, y_pred))
    return {"rmse": rmse, "r2": r2_score(y_true, y_pred), "mae": float(np.mean(np.abs(y_true - y_pred)))}


def _early_stop(losses: list[float], patience: int = 25) -> bool:
    if len(losses) <= patience:
        return False
    return min(losses[:-patience]) <= min(losses[-patience:])


def _train_gnn(graphs_train, graphs_val, graphs_test, device,
               hidden_dim=256, n_layers=4, dropout=0.15,
               lr=3e-4, batch_size=64, epochs=300, patience=35) -> tuple[GNNRegressor, dict]:
    train_loader = GeoDataLoader(graphs_train, batch_size=batch_size, shuffle=True)
    val_loader   = GeoDataLoader(graphs_val,   batch_size=128, shuffle=False)
    test_loader  = GeoDataLoader(graphs_test,  batch_size=128, shuffle=False)

    model = GNNRegressor(hidden_dim=hidden_dim, n_layers=n_layers, dropout=dropout).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=max(2, patience // 3), min_lr=1e-6
    )
    loss_fn = nn.HuberLoss(delta=1.0)

    best_val, best_state, val_losses = float("inf"), None, []

    for epoch in range(1, epochs + 1):
        model.train()
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad(set_to_none=True)
            loss = loss_fn(model(batch), batch.y.unsqueeze(1))
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                val_loss += loss_fn(model(batch), batch.y.unsqueeze(1)).item() * batch.num_graphs
        val_loss /= len(graphs_val)
        val_losses.append(val_loss)
        scheduler.step(val_loss)

        if val_loss < best_val:
            best_val   = val_loss
            best_state = copy.deepcopy(model.state_dict())

        if _early_stop(val_losses, patience):
            print(f"  GNN early stop at epoch {epoch}  val_loss={best_val:.4f}")
            break

    model.load_state_dict(best_state)
    model.eval()
    preds, trues = [], []
    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(device)
            preds.append(model(batch).cpu().numpy().flatten())
            trues.append(batch.y.cpu().numpy().flatten())
    metrics = _evaluate(np.concatenate(trues), np.concatenate(preds))
    print(f"  GNN test  RMSE={metrics['rmse']:.3f}  R²={metrics['r2']:.3f}")
    return model, metrics


def _train_mlp(X_train, y_train, X_val, y_val, X_test, y_test, device,
               hidden_dims=(1024, 512, 256), dropout=0.3,
               lr=1e-3, batch_size=64, epochs=200, patience=30) -> tuple[MLPRegressor, dict]:

    def _to_loader(X, y, shuffle):
        return DataLoader(
            TensorDataset(torch.from_numpy(X), torch.from_numpy(y)),
            batch_size=batch_size, shuffle=shuffle
        )

    train_loader = _to_loader(X_train, y_train, shuffle=True)
    val_loader   = _to_loader(X_val,   y_val,   shuffle=False)

    model = MLPRegressor(input_dim=X_train.shape[1], hidden_dims=hidden_dims, dropout=dropout).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=10, factor=0.5)
    loss_fn   = nn.HuberLoss(delta=1.0)

    best_val, best_state, val_losses = float("inf"), None, []

    for epoch in range(1, epochs + 1):
        model.train()
        for xb, yb in train_loader:
            xb, yb = xb.to(device), yb.to(device)
            optimizer.zero_grad(set_to_none=True)
            loss_fn(model(xb), yb.unsqueeze(1)).backward()
            optimizer.step()

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for xb, yb in val_loader:
                xb, yb = xb.to(device), yb.to(device)
                val_loss += loss_fn(model(xb), yb.unsqueeze(1)).item() * len(xb)
        val_loss /= len(y_val)
        val_losses.append(val_loss)
        scheduler.step(val_loss)

        if val_loss < best_val:
            best_val   = val_loss
            best_state = copy.deepcopy(model.state_dict())

        if _early_stop(val_losses, patience):
            print(f"  MLP early stop at epoch {epoch}  val_loss={best_val:.4f}")
            break

    model.load_state_dict(best_state)
    model.eval()
    X_te_t = torch.from_numpy(X_test).to(device)
    with torch.no_grad():
        preds = model(X_te_t).cpu().numpy().flatten()
    metrics = _evaluate(y_test, preds)
    print(f"  MLP test  RMSE={metrics['rmse']:.3f}  R²={metrics['r2']:.3f}")
    return model, metrics


# ---------------------------------------------------------------------------
# Public training entry point
# ---------------------------------------------------------------------------

def train_and_save(df_dedup, save_dir: str = "models/") -> dict:
    """
    Train GNN + MLP on scaffold split and save everything to save_dir.

    Parameters
    ----------
    df_dedup : pandas DataFrame with columns:
               canonical_smiles, pic50
               (the deduplicated DataFrame from notebook cell 7)
    save_dir : directory to save model weights + metadata

    Returns
    -------
    dict with test metrics for both models
    """
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)

    torch.manual_seed(SEED)
    np.random.seed(SEED)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    smiles_list = df_dedup["canonical_smiles"].tolist()
    pic50_list  = df_dedup["pic50"].values.astype(np.float32)

    # ── Scaffold split ───────────────────────────────────────────────────────
    train_idx, val_idx, test_idx = scaffold_split(smiles_list)
    print(f"Scaffold split — train: {len(train_idx)}, val: {len(val_idx)}, test: {len(test_idx)}")

    # ── GNN ──────────────────────────────────────────────────────────────────
    print("\nBuilding graphs...")
    graphs = []
    for smi, y in zip(smiles_list, pic50_list):
        g = smiles_to_graph(smi, float(y))
        graphs.append(g)  # may be None for invalid SMILES

    # Filter Nones but keep index alignment for split idx
    valid_graphs = {i: g for i, g in enumerate(graphs) if g is not None}

    def _get_graphs(idx_arr):
        return [valid_graphs[i] for i in idx_arr if i in valid_graphs]

    graphs_train = _get_graphs(train_idx)
    graphs_val   = _get_graphs(val_idx)
    graphs_test  = _get_graphs(test_idx)
    print(f"Graphs — train: {len(graphs_train)}, val: {len(graphs_val)}, test: {len(graphs_test)}")

    print("\nTraining GNN...")
    gnn_model, gnn_metrics = _train_gnn(
        graphs_train, graphs_val, graphs_test, device,
        hidden_dim=256, n_layers=4, dropout=0.15,
        lr=3e-4, batch_size=64, epochs=300, patience=35,
    )

    # ── MLP ──────────────────────────────────────────────────────────────────
    print("\nBuilding fingerprints...")
    fps = np.array([smiles_to_fp(smi) for smi in smiles_list], dtype=object)
    valid_fp_mask = np.array([fp is not None for fp in fps])
    fp_matrix = np.vstack(fps[valid_fp_mask]).astype(np.float32)
    pic50_fp  = pic50_list[valid_fp_mask]

    # Re-run scaffold split on the valid-fp subset only (SMILES → same scaffold logic)
    smiles_valid = [s for s, v in zip(smiles_list, valid_fp_mask) if v]
    tr_fp, va_fp, te_fp = scaffold_split(smiles_valid)

    X_train, y_train = fp_matrix[tr_fp], pic50_fp[tr_fp]
    X_val,   y_val   = fp_matrix[va_fp], pic50_fp[va_fp]
    X_test,  y_test  = fp_matrix[te_fp], pic50_fp[te_fp]

    print(f"\nTraining MLP...")
    mlp_model, mlp_metrics = _train_mlp(
        X_train, y_train, X_val, y_val, X_test, y_test, device,
        hidden_dims=(1024, 512, 256), dropout=0.3, lr=1e-3, batch_size=64, epochs=200, patience=30,
    )

    # ── Save ─────────────────────────────────────────────────────────────────
    torch.save(gnn_model.state_dict(), save_path / "gnn.pt")
    torch.save(mlp_model.state_dict(), save_path / "mlp.pt")

    meta = {
        "gnn": {"hidden_dim": 256, "n_layers": 4, "dropout": 0.15, "pooling": "mean",
                "node_dim": NODE_DIM, "edge_dim": EDGE_DIM, "metrics": gnn_metrics},
        "mlp": {"hidden_dims": [1024, 512, 256], "dropout": 0.3, "input_dim": FP_BITS,
                "metrics": mlp_metrics},
    }
    with open(save_path / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\nSaved to {save_path}/")
    print(f"  GNN  R²={gnn_metrics['r2']:.3f}  RMSE={gnn_metrics['rmse']:.3f}")
    print(f"  MLP  R²={mlp_metrics['r2']:.3f}  RMSE={mlp_metrics['rmse']:.3f}")
    return {"gnn": gnn_metrics, "mlp": mlp_metrics}


# ---------------------------------------------------------------------------
# Predictor — call this from Streamlit / LLM tool
# ---------------------------------------------------------------------------

class Predictor:
    """
    Load trained models and predict pIC50 from SMILES.

    Usage:
        p = Predictor.load("models/")
        results = p.predict("CCO")
        results = p.predict(["CCO", "c1ccccc1", "invalid_smiles"])
    """

    def __init__(self, gnn_model: GNNRegressor, mlp_model: MLPRegressor, device: torch.device):
        self.gnn = gnn_model
        self.mlp = mlp_model
        self.device = device
        self.gnn.eval()
        self.mlp.eval()

    @classmethod
    def load(cls, save_dir: str = "models/") -> "Predictor":
        """Load saved weights and reconstruct models."""
        save_path = Path(save_dir)
        with open(save_path / "meta.json") as f:
            meta = json.load(f)

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        gnn_cfg = meta["gnn"]
        gnn = GNNRegressor(
            node_dim   = gnn_cfg["node_dim"],
            edge_dim   = gnn_cfg["edge_dim"],
            hidden_dim = gnn_cfg["hidden_dim"],
            n_layers   = gnn_cfg["n_layers"],
            dropout    = gnn_cfg["dropout"],
            pooling    = gnn_cfg["pooling"],
        )
        gnn.load_state_dict(torch.load(save_path / "gnn.pt", map_location=device, weights_only=True))
        gnn = gnn.to(device)

        mlp_cfg = meta["mlp"]
        mlp = MLPRegressor(
            input_dim   = mlp_cfg["input_dim"],
            hidden_dims = tuple(mlp_cfg["hidden_dims"]),
            dropout     = mlp_cfg["dropout"],
        )
        mlp.load_state_dict(torch.load(save_path / "mlp.pt", map_location=device, weights_only=True))
        mlp = mlp.to(device)

        print(f"Loaded models from {save_path}/  (device={device})")
        return cls(gnn, mlp, device)

    def predict(
        self,
        smiles: Union[str, list[str]],
        model: str = "gnn",
    ) -> list[dict]:
        """
        Predict pIC50 for one or more SMILES strings.

        Parameters
        ----------
        smiles : str or list[str]
        model  : 'gnn' | 'mlp' | 'both'
                 'both' returns predictions from both models in each result dict.

        Returns
        -------
        List of dicts, one per input SMILES:
            {
              "smiles":    original input string,
              "canonical": canonical SMILES (or None if invalid),
              "valid":     True / False,
              "pic50_gnn": float or None,
              "pic50_mlp": float or None,
              "error":     error message if invalid (absent when valid),
            }
        """
        if isinstance(smiles, str):
            smiles = [smiles]

        results = []
        use_gnn = model in ("gnn", "both")
        use_mlp = model in ("mlp", "both")

        for smi in smiles:
            entry: dict = {"smiles": smi, "canonical": None, "valid": False}

            # ── Validate ──────────────────────────────────────────────────────
            canon = canonicalise(smi)
            if canon is None:
                entry["error"] = f"RDKit could not parse SMILES: {smi!r}"
                entry["pic50_gnn"] = None
                entry["pic50_mlp"] = None
                results.append(entry)
                continue

            entry["canonical"] = canon
            entry["valid"] = True

            # ── GNN prediction ────────────────────────────────────────────────
            if use_gnn:
                g = smiles_to_graph(canon, 0.0)
                if g is None:
                    entry["pic50_gnn"] = None
                    entry["error_gnn"] = "Graph building failed"
                else:
                    loader = GeoDataLoader([g], batch_size=1)
                    batch  = next(iter(loader)).to(self.device)
                    with torch.no_grad():
                        pred = self.gnn(batch).item()
                    entry["pic50_gnn"] = round(pred, 4)
            else:
                entry["pic50_gnn"] = None

            # ── MLP prediction ────────────────────────────────────────────────
            if use_mlp:
                fp = smiles_to_fp(canon)
                if fp is None:
                    entry["pic50_mlp"] = None
                    entry["error_mlp"] = "Fingerprint generation failed"
                else:
                    x = torch.from_numpy(fp).unsqueeze(0).to(self.device)
                    with torch.no_grad():
                        pred = self.mlp(x).item()
                    entry["pic50_mlp"] = round(pred, 4)
            else:
                entry["pic50_mlp"] = None

            results.append(entry)

        return results

    def predict_single(self, smiles: str, model: str = "gnn") -> float | None:
        """
        Convenience wrapper — returns just the float pIC50 (or None if invalid).
        Designed for LLM tool-call use where you want one clean value.
        """
        res = self.predict(smiles, model=model)[0]
        if not res["valid"]:
            return None
        return res.get(f"pic50_{model}")


# ---------------------------------------------------------------------------
# Mismatch / error analysis
# ---------------------------------------------------------------------------

def mismatch_analysis(
    predictor: Predictor,
    df_dedup,
    model: str = "gnn",
    n_worst: int = 20,
    save_dir: str = "models/",
) -> "pd.DataFrame":
    """
    Run the trained model on the full dataset and report the worst-predicted
    compounds. Useful for understanding where the model fails.

    Returns a DataFrame sorted by absolute error (worst first).
    """
    import pandas as pd

    smiles_list = df_dedup["canonical_smiles"].tolist()
    pic50_true  = df_dedup["pic50"].values.astype(float)

    results = predictor.predict(smiles_list, model=model)
    key = f"pic50_{model}"

    rows = []
    for smi, y_true, res in zip(smiles_list, pic50_true, results):
        y_pred = res.get(key)
        if y_pred is None or not res["valid"]:
            continue
        rows.append({
            "smiles":    smi,
            "pic50_true":  round(y_true, 4),
            "pic50_pred":  round(y_pred, 4),
            "error":       round(y_pred - y_true, 4),
            "abs_error":   round(abs(y_pred - y_true), 4),
        })

    df = pd.DataFrame(rows).sort_values("abs_error", ascending=False).reset_index(drop=True)

    valid_preds = df["pic50_pred"].values
    valid_trues = df["pic50_true"].values
    metrics = _evaluate(valid_trues, valid_preds)
    print(f"Mismatch analysis ({model.upper()}) — "
          f"R²={metrics['r2']:.3f}  RMSE={metrics['rmse']:.3f}  MAE={metrics['mae']:.3f}")
    print(f"Worst {n_worst} predictions:")
    print(df.head(n_worst).to_string(index=False))

    df.to_csv(Path(save_dir) / f"mismatch_{model}.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python predictor.py <SMILES> [<SMILES2> ...]")
        print("       (loads models from ./models/)")
        sys.exit(1)

    p = Predictor.load("models/")
    results = p.predict(sys.argv[1:], model="both")
    for r in results:
        if r["valid"]:
            print(f"{r['smiles']}  →  GNN: {r['pic50_gnn']}  MLP: {r['pic50_mlp']}")
        else:
            print(f"{r['smiles']}  →  INVALID: {r['error']}")
