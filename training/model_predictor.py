"""
model_predictors.py
===================
Two inference classes that load the saved final models and expose a
``predict(smiles: str) -> float`` method returning a pIC50 prediction.

Usage
-----
    from model_predictors import MLPPredictor, GINEPredictor

    mlp  = MLPPredictor("models/mlp_final.pt", "models/mlp_scaler.pkl",
                        "models/physchem_cols.json")
    gine = GINEPredictor("models/gine_final.pt", "models/gine_phys_scaler.pkl")

    smi  = "CC(=O)Oc1ccccc1C(=O)O"   # aspirin
    print(mlp.predict(smi))
    print(gine.predict(smi))
"""

from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

# RDKit -----------------------------------------------------------------------
from rdkit import Chem, DataStructs
from rdkit.Chem import (
    rdFingerprintGenerator,
    rdPartialCharges,
)

# PyG -------------------------------------------------------------------------
from torch_geometric.data import Data
from torch_geometric.nn import GINEConv, global_mean_pool


# ─────────────────────────────────────────────────────────────────────────────
# Atom / bond feature helpers  (mirrors models_experiment.ipynb exactly)
# ─────────────────────────────────────────────────────────────────────────────

ATOMS = ["C", "N", "O", "S", "F", "Cl", "Br", "I", "P", "Se", "Si", "B", "unknown"]

HYBRIDIZATION_TYPES = [
    Chem.rdchem.HybridizationType.S,
    Chem.rdchem.HybridizationType.SP,
    Chem.rdchem.HybridizationType.SP2,
    Chem.rdchem.HybridizationType.SP3,
    Chem.rdchem.HybridizationType.SP3D,
    Chem.rdchem.HybridizationType.SP3D2,
    Chem.rdchem.HybridizationType.OTHER,
]

CHIRAL_TYPES = [
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


def _one_hot(value, vocab: list) -> list:
    vec = [0] * len(vocab)
    vec[vocab.index(value) if value in vocab else -1] = 1
    return vec


def _safe_norm(value: float, max_val: float) -> float:
    return min(float(value), max_val) / max_val


def _atom_features(atom, ring_info) -> list:
    symbol = atom.GetSymbol()
    idx = atom.GetIdx()

    smallest_ring = 0
    if ring_info is not None:
        for ring in ring_info.AtomRings():
            if idx in ring:
                size = len(ring)
                smallest_ring = size if smallest_ring == 0 else min(smallest_ring, size)

    try:
        g = float(atom.GetDoubleProp("_GasteigerCharge"))
        if np.isnan(g):
            g = 0.0
        g = float(np.clip(g, -1.0, 1.0))
    except Exception:
        g = 0.0

    return [
        *_one_hot(symbol, ATOMS),
        _safe_norm(atom.GetAtomicNum(), 53.0),
        _safe_norm(atom.GetDegree(), 6.0),
        _safe_norm(atom.GetTotalValence(), 6.0),
        atom.GetFormalCharge() / 4.0,
        _safe_norm(atom.GetTotalNumHs(), 4.0),
        int(atom.IsInRing()),
        int(atom.IsInRingSize(5)),
        int(atom.IsInRingSize(6)),
        int(atom.GetIsAromatic()),
        *_one_hot(atom.GetHybridization(), HYBRIDIZATION_TYPES),
        *_one_hot(atom.GetChiralTag(), CHIRAL_TYPES),
        g,
        _safe_norm(min(smallest_ring, 12), 12.0),
    ]


def _bond_features(bond) -> list:
    return [
        *_one_hot(bond.GetBondType(), BOND_TYPES),
        *_one_hot(bond.GetStereo(), BOND_STEREO),
        int(bond.IsInRing()),
        int(bond.GetIsConjugated()),
        int(bond.GetIsAromatic()),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Model architecture definitions  (must match training notebook exactly)
# ─────────────────────────────────────────────────────────────────────────────

class _MLP(nn.Module):
    """Identical to MLP in models_experiment.ipynb."""

    def __init__(
        self,
        input_dim: int,
        hidden_dims: list[int],
        dropout: float = 0.3,
        use_batchnorm: bool = True,
        activation: str = "relu",
    ):
        super().__init__()
        act_fn = {"relu": nn.ReLU, "gelu": nn.GELU, "silu": nn.SiLU}[activation]

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


class _GINEModel(nn.Module):
    """Identical to GINEModel in models_experiment.ipynb."""

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
            self.convs.append(GINEConv(mlp, edge_dim=hidden_dim, train_eps=True))
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
            data.x,
            data.edge_index,
            data.edge_attr,
            data.batch,
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


# ─────────────────────────────────────────────────────────────────────────────
# Public predictor classes
# ─────────────────────────────────────────────────────────────────────────────

class MLPPredictor:
    """
    Load a saved MLP checkpoint and predict pIC50 from a SMILES string.

    Parameters
    ----------
    checkpoint_path : str | Path
        Path to ``mlp_final.pt`` produced by the training notebook.
    scaler_path : str | Path
        Path to ``mlp_scaler.pkl`` (StandardScaler for physicochemical cols).
    physchem_cols_path : str | Path
        Path to ``physchem_cols.json`` (list of physicochemical column names).
    device : str, optional
        ``'cpu'`` or ``'cuda'``.  Defaults to CUDA if available.
    """

    def __init__(
        self,
        checkpoint_path: str | Path,
        scaler_path: str | Path,
        physchem_cols_path: str | Path,
        device: Optional[str] = None,
    ):
        self.device = torch.device(
            device if device else ("cuda" if torch.cuda.is_available() else "cpu")
        )

        # Scaler
        with open(scaler_path, "rb") as f:
            self._scaler = pickle.load(f)

        # Physchem column list (order matters)
        with open(physchem_cols_path) as f:
            self._physchem_cols = json.load(f)

        # Model
        ckpt = torch.load(checkpoint_path, map_location=self.device, weights_only=False)
        arch = ckpt["arch"]
        self._model = _MLP(
            input_dim=arch["input_dim"],
            hidden_dims=arch["hidden_dims"],
            dropout=arch["dropout"],
        ).to(self.device)
        self._model.load_state_dict(ckpt["model_state_dict"])
        self._model.eval()

        self._fp_bits = 2048
        self._mfpgen = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=self._fp_bits)

    # ------------------------------------------------------------------
    def _smiles_to_features(self, smiles: str) -> np.ndarray:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            raise ValueError(f"RDKit could not parse SMILES: {smiles!r}")

        # Morgan fingerprint
        fp = self._mfpgen.GetFingerprint(mol)
        fp_arr = np.zeros((self._fp_bits,), dtype=np.float32)
        DataStructs.ConvertToNumpyArray(fp, fp_arr)

        # Physicochemical features from RDKit descriptors
        from rdkit.Chem import Descriptors, rdMolDescriptors
        phys_map = {
            "mw_freebase":    Descriptors.MolWt(mol),
            "alogp":          Descriptors.MolLogP(mol),
            "hba":            rdMolDescriptors.CalcNumHBA(mol),
            "hbd":            rdMolDescriptors.CalcNumHBD(mol),
            "psa":            Descriptors.TPSA(mol),
            "rtb":            rdMolDescriptors.CalcNumRotatableBonds(mol),
            "aromatic_rings": rdMolDescriptors.CalcNumAromaticRings(mol),
            "qed_weighted":   Chem.QED.qed(mol),
        }
        phys_arr = np.array(
            [phys_map.get(c, 0.0) for c in self._physchem_cols], dtype=np.float32
        ).reshape(1, -1)
        phys_arr_scaled = self._scaler.transform(phys_arr).flatten()

        return np.concatenate([fp_arr, phys_arr_scaled]).astype(np.float32)

    # ------------------------------------------------------------------
    def predict(self, smiles: str) -> float:
        """
        Predict pIC50 for a single SMILES string.

        Parameters
        ----------
        smiles : str
            Input molecule as a SMILES string.

        Returns
        -------
        float
            Predicted pIC50 value.
        """
        x = self._smiles_to_features(smiles)
        tensor = torch.tensor(x, dtype=torch.float32).unsqueeze(0).to(self.device)
        with torch.no_grad():
            out = self._model(tensor)
        return float(out.squeeze().cpu().item())


# ─────────────────────────────────────────────────────────────────────────────

class GINEPredictor:
    """
    Load a saved GINEModel checkpoint and predict pIC50 from a SMILES string.

    Parameters
    ----------
    checkpoint_path : str | Path
        Path to ``gine_final.pt`` produced by the training notebook.
    phys_scaler_path : str | Path
        Path to ``gine_phys_scaler.pkl`` (StandardScaler fitted on the
        physicochemical features during graph construction).
    device : str, optional
        ``'cpu'`` or ``'cuda'``.  Defaults to CUDA if available.
    """

    _PHYSCHEM_COLS = [
        "mw_freebase", "alogp", "hba", "hbd",
        "psa", "rtb", "aromatic_rings", "qed_weighted",
    ]

    def __init__(
        self,
        checkpoint_path: str | Path,
        phys_scaler_path: str | Path,
        device: Optional[str] = None,
    ):
        self.device = torch.device(
            device if device else ("cuda" if torch.cuda.is_available() else "cpu")
        )

        # Physchem scaler
        with open(phys_scaler_path, "rb") as f:
            self._phys_scaler = pickle.load(f)

        # Model
        ckpt = torch.load(checkpoint_path, map_location=self.device, weights_only=False)
        arch = ckpt["arch"]
        self._model = _GINEModel(
            node_dim=arch["node_dim"],
            edge_dim=arch["edge_dim"],
            hidden_dim=arch["hidden_dim"],
            num_layers=arch["num_layers"],
            dropout=arch["dropout"],
        ).to(self.device)
        self._model.load_state_dict(ckpt["model_state_dict"])
        self._model.eval()

        self._node_dim = arch["node_dim"]
        self._edge_dim = arch["edge_dim"]
        self._phys_dim = ckpt.get("phys_dim", 8)

    # ------------------------------------------------------------------
    def _smiles_to_graph(self, smiles: str) -> Data:
        from rdkit.Chem import Descriptors, rdMolDescriptors

        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            raise ValueError(f"RDKit could not parse SMILES: {smiles!r}")

        try:
            rdPartialCharges.ComputeGasteigerCharges(mol)
        except Exception:
            pass

        ring_info = mol.GetRingInfo()

        node_feats = torch.tensor(
            [_atom_features(a, ring_info) for a in mol.GetAtoms()],
            dtype=torch.float,
        )

        src, dst, edge_feats = [], [], []
        for bond in mol.GetBonds():
            i, j = bond.GetBeginAtomIdx(), bond.GetEndAtomIdx()
            feats = _bond_features(bond)
            src += [i, j]
            dst += [j, i]
            edge_feats += [feats, feats]

        if len(src) == 0:
            src, dst = [0], [0]
            edge_feats = [[0.0] * self._edge_dim]

        edge_index = torch.tensor([src, dst], dtype=torch.long)
        edge_attr  = torch.tensor(edge_feats, dtype=torch.float)

        # Physicochemical features (graph-level)
        phys_map = {
            "mw_freebase":    Descriptors.MolWt(mol),
            "alogp":          Descriptors.MolLogP(mol),
            "hba":            rdMolDescriptors.CalcNumHBA(mol),
            "hbd":            rdMolDescriptors.CalcNumHBD(mol),
            "psa":            Descriptors.TPSA(mol),
            "rtb":            rdMolDescriptors.CalcNumRotatableBonds(mol),
            "aromatic_rings": rdMolDescriptors.CalcNumAromaticRings(mol),
            "qed_weighted":   Chem.QED.qed(mol),
        }
        phys_arr = np.array(
            [phys_map.get(c, 0.0) for c in self._PHYSCHEM_COLS], dtype=np.float32
        ).reshape(1, -1)
        phys_norm = self._phys_scaler.transform(phys_arr).flatten()
        phys_tensor = torch.tensor(phys_norm, dtype=torch.float).unsqueeze(0)

        return Data(
            x=node_feats,
            edge_index=edge_index,
            edge_attr=edge_attr,
            phys=phys_tensor,
            batch=torch.zeros(node_feats.size(0), dtype=torch.long),
        )

    # ------------------------------------------------------------------
    def predict(self, smiles: str) -> float:
        """
        Predict pIC50 for a single SMILES string.

        Parameters
        ----------
        smiles : str
            Input molecule as a SMILES string.

        Returns
        -------
        float
            Predicted pIC50 value.
        """
        graph = self._smiles_to_graph(smiles).to(self.device)
        with torch.no_grad():
            out = self._model(graph)
        return float(out.squeeze().cpu().item())


# ─────────────────────────────────────────────────────────────────────────────
# Quick sanity-check when run as a script
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Predict pIC50 from SMILES")
    parser.add_argument("smiles", help="SMILES string")
    parser.add_argument("--models-dir", default="models", help="Path to models/ folder")
    args = parser.parse_args()

    md = Path(args.models_dir)

    print("Loading MLP predictor …")
    mlp = MLPPredictor(
        md / "mlp_final.pt",
        md / "mlp_scaler.pkl",
        md / "physchem_cols.json",
    )
    print(f"  MLP  pIC50 = {mlp.predict(args.smiles):.3f}")

    print("Loading GINE predictor …")
    gine = GINEPredictor(
        md / "gine_final.pt",
        md / "gine_phys_scaler.pkl",
    )
    print(f"  GINE pIC50 = {gine.predict(args.smiles):.3f}")