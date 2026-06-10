import logging
from pathlib import Path
import pickle

import numpy as np
import torch
from torch import nn
import torch.nn.functional as F

from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors, rdPartialCharges

from torch_geometric.data import Data
from torch_geometric.nn import GINEConv, global_mean_pool


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Constants for atom/bond featurization (must match training)
ATOMS = ['C', 'N', 'O', 'S', 'F', 'Cl', 'Br', 'I', 'P', 'Se', 'Si', 'B', 'unknown']

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

PHYSCHEM_COLS = [
    'mw_freebase',
    'alogp',
    'hba',
    'hbd',
    'psa',
    'rtb',
    'aromatic_rings',
    'qed_weighted'
]


def one_hot(value, vocab: list) -> list[int]:
    """One-hot encode a value given a vocabulary."""
    vec = [0] * len(vocab)
    vec[vocab.index(value) if value in vocab else -1] = 1
    return vec


def safe_norm(value: float, max_val: float) -> float:
    """Normalize value to [0, 1] with clamping."""
    return min(float(value), max_val) / max_val


def atom_features(atom, ring_info) -> list[float]:
    """Generate atom feature vector (must match training featurization)."""
    symbol = atom.GetSymbol()
    idx = atom.GetIdx()

    # Smallest ring size
    smallest_ring = 0
    if ring_info is not None:
        for ring in ring_info.AtomRings():
            if idx in ring:
                size = len(ring)
                smallest_ring = size if smallest_ring == 0 else min(smallest_ring, size)

    # Gasteiger charge
    try:
        g = float(atom.GetDoubleProp('_GasteigerCharge'))
        if np.isnan(g):
            g = 0.0
        g = np.clip(g, -1.0, 1.0)
    except:
        g = 0.0

    return [
        *one_hot(symbol, ATOMS),
        safe_norm(atom.GetAtomicNum(), 53.0),
        safe_norm(atom.GetDegree(), 6.0),
        safe_norm(atom.GetTotalValence(), 6.0),
        atom.GetFormalCharge() / 4.0,
        safe_norm(atom.GetTotalNumHs(), 4.0),
        int(atom.IsInRing()),
        int(atom.IsInRingSize(5)),
        int(atom.IsInRingSize(6)),
        int(atom.GetIsAromatic()),
        *one_hot(atom.GetHybridization(), HYBRIDIZATION_TYPES),
        *one_hot(atom.GetChiralTag(), CHIRAL_TYPES),
        g,
        safe_norm(min(smallest_ring, 12), 12.0),
    ]


def bond_features(bond) -> list[float]:
    """Generate bond feature vector (must match training featurization)."""
    return [
        *one_hot(bond.GetBondType(), BOND_TYPES),
        *one_hot(bond.GetStereo(), BOND_STEREO),
        int(bond.IsInRing()),
        int(bond.GetIsConjugated()),
        int(bond.GetIsAromatic()),
    ]


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


class GNNPredictor:
    """Predictor class for loading GNN model and making pIC50 predictions."""

    def __init__(self, model_dir: str, device: str = None):
        """
        Initialize GNN predictor.

        Args:
            model_dir: Directory containing model artifacts
                       (gine_final.pt, gine_phys_scaler.pkl)
            device: Device to run model on ('cuda' or 'cpu'). If None, auto-detect.
        """
        self.model_dir = Path(model_dir)

        if device is None:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        else:
            self.device = torch.device(device)

        logger.info(f"Initializing GNNPredictor on device: {self.device}")

        # Load model
        self.model, self.node_dim, self.edge_dim = self._load_model()

        # Load physicochemical scaler
        self.phys_scaler = self._load_phys_scaler()

        logger.info("GNNPredictor initialized successfully")

    def _load_model(self):
        """Load trained GNN model from checkpoint."""
        ckpt_path = self.model_dir / 'gine_final.pt'

        if not ckpt_path.exists():
            raise FileNotFoundError(f"Model checkpoint not found: {ckpt_path}")

        logger.info(f"Loading model from: {ckpt_path}")
        ckpt = torch.load(ckpt_path, map_location=self.device)

        # Build model with saved architecture
        arch = ckpt['arch']
        model = GINEModel(
            node_dim=arch['node_dim'],
            edge_dim=arch['edge_dim'],
            hidden_dim=arch['hidden_dim'],
            num_layers=arch['num_layers'],
            dropout=arch['dropout'],
        )

        # Load weights
        model.load_state_dict(ckpt['model_state_dict'])
        model.to(self.device)
        model.eval()

        logger.info(f"Model loaded successfully")
        logger.info(f"  Architecture: {arch}")
        logger.info(f"  Test metrics: {ckpt.get('test_metrics', 'N/A')}")

        return model, arch['node_dim'], arch['edge_dim']

    def _load_phys_scaler(self):
        """Load StandardScaler for physicochemical features."""
        scaler_path = self.model_dir / 'gine_phys_scaler.pkl'

        if not scaler_path.exists():
            logger.warning(f"Phys scaler not found: {scaler_path}")
            logger.warning("Predictions will use unnormalized physicochemical features")
            return None

        logger.info(f"Loading phys scaler from: {scaler_path}")
        with open(scaler_path, 'rb') as f:
            scaler = pickle.load(f)

        return scaler

    def smiles_to_physchem(self, smi: str) -> np.ndarray | None:
        """
        Compute physicochemical descriptors from SMILES.

        Args:
            smi: SMILES string

        Returns:
            8-dimensional descriptor vector, or None if invalid SMILES
        """
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            return None

        # Compute descriptors in the same order as training
        descriptors = {
            'mw_freebase': Descriptors.MolWt(mol),
            'alogp': Descriptors.MolLogP(mol),
            'hba': rdMolDescriptors.CalcNumHBA(mol),
            'hbd': rdMolDescriptors.CalcNumHBD(mol),
            'psa': rdMolDescriptors.CalcTPSA(mol),
            'rtb': rdMolDescriptors.CalcNumRotatableBonds(mol),
            'aromatic_rings': rdMolDescriptors.CalcNumAromaticRings(mol),
            'qed_weighted': Descriptors.qed(mol),
        }

        # Extract values in correct order
        values = np.array([descriptors[col] for col in PHYSCHEM_COLS], dtype=np.float32)
        return values

    def smiles_to_graph(self, smi: str) -> Data | None:
        """
        Convert SMILES to PyG Data object.

        Args:
            smi: SMILES string

        Returns:
            PyG Data object, or None if invalid SMILES
        """
        mol = Chem.MolFromSmiles(smi)
        if mol is None or mol.GetNumAtoms() == 0:
            return None

        # Compute Gasteiger charges
        try:
            rdPartialCharges.ComputeGasteigerCharges(mol)
        except Exception:
            pass

        ring_info = mol.GetRingInfo()

        # Node features
        node_feats = torch.tensor(
            [atom_features(a, ring_info) for a in mol.GetAtoms()],
            dtype=torch.float,
        )

        # Bidirectional edges
        src, dst, edge_feats = [], [], []
        for bond in mol.GetBonds():
            i, j = bond.GetBeginAtomIdx(), bond.GetEndAtomIdx()
            feats = bond_features(bond)
            src += [i, j]
            dst += [j, i]
            edge_feats += [feats, feats]

        if len(src) == 0:
            src, dst = [0], [0]
            edge_feats = [[0.0] * self.edge_dim]

        edge_index = torch.tensor([src, dst], dtype=torch.long)
        edge_attr = torch.tensor(edge_feats, dtype=torch.float)

        # Physicochemical features
        phys_vec = self.smiles_to_physchem(smi)
        if phys_vec is None:
            phys_vec = np.zeros(len(PHYSCHEM_COLS), dtype=np.float32)

        # Normalize physicochemical features
        if self.phys_scaler is not None:
            phys_vec = self.phys_scaler.transform(phys_vec.reshape(1, -1)).flatten()

        phys_tensor = torch.tensor(phys_vec, dtype=torch.float).unsqueeze(0)

        return Data(
            x=node_feats,
            edge_index=edge_index,
            edge_attr=edge_attr,
            phys=phys_tensor,
        )

    def predict(self, smiles: str) -> float | None:
        """
        Predict pIC50 for a given SMILES string.

        Args:
            smiles: SMILES string of the molecule

        Returns:
            Predicted pIC50 value, or None if invalid SMILES
        """
        # Convert to graph
        graph = self.smiles_to_graph(smiles)
        if graph is None:
            logger.warning(f"Invalid SMILES: {smiles}")
            return None

        # Add batch dimension
        graph = graph.to(self.device)
        graph.batch = torch.zeros(graph.num_nodes, dtype=torch.long, device=self.device)

        # Predict
        with torch.no_grad():
            pred = self.model(graph).cpu().item()

        return pred


if __name__ == "__main__":
    # Example usage
    MODELS_DIR = Path(__file__).parent.parent / 'models'

    # Initialize predictor
    predictor = GNNPredictor(model_dir=str(MODELS_DIR))

    # Test predictions
    test_smiles = [
        "CCOc1nc(NC(C)=O)cc(N)c1C#N",  # Example from training data
        "CC(C)Cc1ccc(cc1)C(C)C(O)=O",  # Ibuprofen
        "INVALID_SMILES",  # Should return None
    ]

    logger.info("\n" + "=" * 60)
    logger.info("Testing GNN predictions:")
    logger.info("=" * 60)

    for smi in test_smiles:
        pred = predictor.predict(smi)
        if pred is not None:
            logger.info(f"SMILES: {smi}")
            logger.info(f"Predicted pIC50: {pred:.3f}")
        else:
            logger.info(f"SMILES: {smi}")
            logger.info(f"Predicted pIC50: INVALID")
        logger.info("-" * 60)
