import warnings
warnings.filterwarnings('ignore')

import logging
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import torch

from rdkit import Chem, DataStructs
from rdkit.Chem import rdFingerprintGenerator, rdPartialCharges

from sklearn.preprocessing import StandardScaler

from torch_geometric.data import Data


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Constants for atom/bond featurization
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

SEED = 42


def canonicalise(smi: str):
    """Canonicalise SMILES using RDKit."""
    mol = Chem.MolFromSmiles(smi)
    return Chem.MolToSmiles(mol) if mol else None


def smiles_to_fingerprint(smi: str) -> np.ndarray | None:
    """Convert SMILES to Morgan fingerprint (ECFP4, 2048 bits)."""
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        return None

    mfpgen = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=2048)
    fp = mfpgen.GetFingerprint(mol)

    arr = np.zeros((2048,), dtype=np.float32)
    DataStructs.ConvertToNumpyArray(fp, arr)
    return arr


def one_hot(value, vocab: list) -> list[int]:
    """One-hot encode value from vocabulary."""
    vec = [0] * len(vocab)
    vec[vocab.index(value) if value in vocab else -1] = 1
    return vec


def safe_norm(value: float, max_val: float) -> float:
    """Normalize to [0, 1] with clamping."""
    return min(float(value), max_val) / max_val


def atom_features(atom, ring_info) -> list[float]:
    """
    Generate atom feature vector for GNN.
    Includes atom type, atomic number, degree, valence, charge,
    hydrogens, ring info, hybridization, chirality, and Gasteiger charge.
    """
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
    """
    Generate bond feature vector for GNN.
    Includes bond type, stereo, ring membership, conjugation, and aromaticity.
    """
    return [
        *one_hot(bond.GetBondType(), BOND_TYPES),
        *one_hot(bond.GetStereo(), BOND_STEREO),
        int(bond.IsInRing()),
        int(bond.GetIsConjugated()),
        int(bond.GetIsAromatic()),
    ]


def get_atom_feature_dim() -> int:
    """Calculate atom feature dimensionality."""
    mol = Chem.MolFromSmiles("CC")
    rdPartialCharges.ComputeGasteigerCharges(mol)
    ring_info = mol.GetRingInfo()
    return len(atom_features(next(mol.GetAtoms()), ring_info))


def get_edge_feature_dim() -> int:
    """Calculate edge feature dimensionality."""
    mol = Chem.MolFromSmiles("CC")
    return len(bond_features(mol.GetBondBetweenAtoms(0, 1)))


def smiles_to_graph(smi: str, y_val: float, phys_vec: np.ndarray | None = None) -> Data | None:
    """
    Convert SMILES to PyG Data object with atom/bond features.
    phys_vec: optional physicochemical descriptors (graph-level attribute)
    Returns None if SMILES is invalid.
    """
    mol = Chem.MolFromSmiles(smi)
    if mol is None or mol.GetNumAtoms() == 0:
        return None

    # Compute Gasteiger charges (needed by extended atom_features)
    try:
        rdPartialCharges.ComputeGasteigerCharges(mol)
    except Exception:
        pass   # fallback: charges stay 0 in atom_features

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
        edge_feats = [[0.0] * get_edge_feature_dim()]

    edge_index = torch.tensor([src, dst], dtype=torch.long)
    edge_attr = torch.tensor(edge_feats, dtype=torch.float)

    # Physicochemical graph-level features
    if phys_vec is None:
        phys_vec = np.zeros(len(PHYSCHEM_COLS), dtype=np.float32)
    phys_tensor = torch.tensor(phys_vec, dtype=torch.float).unsqueeze(0)

    return Data(
        x = node_feats,
        edge_index = edge_index,
        edge_attr = edge_attr,
        y = torch.tensor([y_val], dtype=torch.float),
        phys = phys_tensor,
        smiles = smi,
    )


class DataPreparation:
    """Main class for data preparation pipeline."""

    def __init__(self, raw_parquet_path: str, output_dir: str):
        """
        Initialize data preparation.

        Args:
            raw_parquet_path: Path to raw ChEMBL parquet file
            output_dir: Directory to save prepared datasets
        """
        self.raw_parquet_path = raw_parquet_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        np.random.seed(SEED)
        torch.manual_seed(SEED)

        logger.info(f"Initialized DataPreparation")
        logger.info(f"Raw data: {raw_parquet_path}")
        logger.info(f"Output directory: {self.output_dir}")

    def load_and_clean_data(self) -> pd.DataFrame:
        """Load raw data and perform basic cleaning."""
        
        logger.info("Loading raw data...")
        df_raw = pd.read_parquet(self.raw_parquet_path)
        logger.info(f"Raw rows: {len(df_raw):,}")

        # Drop rows without SMILES or pic50
        df = df_raw.dropna(subset=['canonical_smiles', 'pic50']).copy()
        logger.info(f"After dropping null SMILES/pic50: {len(df):,}")

        # RDKit-validate SMILES
        df['canonical_smiles'] = df['canonical_smiles'].apply(canonicalise)
        invalid = df['canonical_smiles'].isna().sum()
        df = df.dropna(subset=['canonical_smiles'])
        logger.info(f"Invalid SMILES dropped: {invalid}")
        logger.info(f"Remaining: {len(df):,}")

        return df

    def deduplicate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate by canonical SMILES, taking median pic50."""
        logger.info("Deduplicating by SMILES (median pic50)...")

        # Aggregate physicochemical columns by taking the first
        physchem_first = df.groupby('canonical_smiles')[PHYSCHEM_COLS].first()
        pic50_median = df.groupby('canonical_smiles')['pic50'].median().rename('pic50')
        pic50_std = df.groupby('canonical_smiles')['pic50'].std().rename('pic50_std')
        n_measurements = df.groupby('canonical_smiles')['pic50'].count().rename('n_measurements')

        df_dedup = pd.concat([pic50_median, pic50_std, n_measurements, physchem_first], axis=1).reset_index()

        # Canonical global index for later correct splitting
        df_dedup = df_dedup.reset_index(drop=True)
        df_dedup["mol_id"] = np.arange(len(df_dedup))

        logger.info(f"After deduplication: {len(df_dedup):,} unique compounds")
        return df_dedup

    def prepare_mlp_features(self, df_dedup: pd.DataFrame) -> tuple:
        """
        Prepare MLP features: Morgan fingerprints + physicochemical descriptors.

        Returns:
            df_mlp: DataFrame with all features
            X_mlp: Feature matrix (fingerprints + physchem)
            y_mlp: Target values (pic50)
        """
        logger.info("Generating MLP features (Morgan fingerprints + physchem)...")

        # Generate fingerprints
        fingerprints = df_dedup['canonical_smiles'].apply(smiles_to_fingerprint)
        valid_mask = fingerprints.apply(lambda x: x is not None)
        logger.info(f"Valid fingerprints: {valid_mask.sum()} / {len(df_dedup)}")

        df_mlp = df_dedup[valid_mask].copy().reset_index(drop=True)
        df_mlp["mol_id"] = np.arange(len(df_mlp))
        fp_matrix = np.vstack(fingerprints[valid_mask].values).astype(np.float32)

        # Physicochemical features
        phys_matrix = df_mlp[PHYSCHEM_COLS].values.astype(np.float32)

        # Impute median for any remaining NaNs
        col_medians = np.nanmedian(phys_matrix, axis=0)
        nan_mask = np.isnan(phys_matrix)
        phys_matrix[nan_mask] = np.take(col_medians, np.where(nan_mask)[1])

        # Concatenate: [2048 fp bits | 8 physchem]
        X_mlp = np.concatenate([fp_matrix, phys_matrix], axis=1)
        y_mlp = df_mlp['pic50'].values.astype(np.float32)

        logger.info(f"MLP feature matrix shape: {X_mlp.shape}")
        logger.info(f"Target vector shape: {y_mlp.shape}")

        return df_mlp, X_mlp, y_mlp

    def save_mlp_features(self, df_mlp: pd.DataFrame, X_mlp: np.ndarray, y_mlp: np.ndarray):
        """Save MLP features as parquet file."""
        fp_col_names = [f'fp_{i}' for i in range(2048)]
        feature_cols = fp_col_names + PHYSCHEM_COLS

        df_mlp_out = pd.DataFrame(X_mlp, columns=feature_cols)
        df_mlp_out.insert(0, 'canonical_smiles', df_mlp['canonical_smiles'].values)
        df_mlp_out.insert(1, 'pic50', y_mlp)
        df_mlp_out.insert(2, 'n_measurements', df_mlp['n_measurements'].values)
        df_mlp_out.insert(3, 'mol_id', df_mlp['mol_id'].values)

        output_path = self.output_dir / 'mlp_features_2147_mlp.parquet'
        df_mlp_out.to_parquet(output_path, index=False)
        logger.info(f"Saved MLP features: {output_path} ({df_mlp_out.shape})")

        return output_path

    def prepare_gnn_features(self, df_mlp: pd.DataFrame) -> tuple:
        """
        Prepare GNN features: atom/bond graphs with physicochemical attributes.

        Returns:
            graph_list: List of PyG Data objects
            phys_scaler: StandardScaler fitted on physicochemical features
        """
        logger.info("Generating GNN features (atom/bond graphs)...")

        # Build phys matrix aligned to df_mlp
        phys_matrix = df_mlp[PHYSCHEM_COLS].values.astype(np.float32)

        col_medians = np.nanmedian(phys_matrix, axis=0)
        nan_mask = np.isnan(phys_matrix)
        phys_matrix[nan_mask] = np.take(col_medians, np.where(nan_mask)[1])

        # Normalise physicochemical features
        phys_scaler = StandardScaler().fit(phys_matrix)
        phys_matrix_norm = phys_scaler.transform(phys_matrix)

        graph_list = []
        graph_mol_ids = []

        for i, row in df_mlp.iterrows():
            smi = row["canonical_smiles"]
            pic50 = row["pic50"]
            mol_id = row["mol_id"]

            g = smiles_to_graph(smi, float(pic50), phys_matrix_norm[i])

            if g is not None:
                graph_list.append(g)
                graph_mol_ids.append(mol_id)

        logger.info(f"Graphs built: {len(graph_list)}")

        # Log graph statistics
        num_nodes_list = [g.num_nodes for g in graph_list]
        logger.info(f"Atoms per molecule — mean: {np.mean(num_nodes_list):.1f}, "
                   f"min: {np.min(num_nodes_list)}, max: {np.max(num_nodes_list)}")
        logger.info(f"Node feature dim: {graph_list[0].x.shape[1]}")
        logger.info(f"Edge feature dim: {graph_list[0].edge_attr.shape[1]}")
        logger.info(f"Physchem dim: {graph_list[0].phys.shape[1]}")

        return graph_list, phys_scaler

    def save_gnn_features(self, graph_list: list, phys_scaler):
        """Save GNN features as PyTorch file and physicochemical scaler."""
        # Save graphs
        graphs_path = self.output_dir / 'gnn_graphs_2147_gnn.pt'
        torch.save(graph_list, graphs_path)
        logger.info(f"Saved GNN graphs: {graphs_path}")

        # Save physicochemical scaler for inference
        scaler_path = self.output_dir.parent / 'models' / 'gine_phys_scaler.pkl'
        scaler_path.parent.mkdir(exist_ok=True)
        with open(scaler_path, 'wb') as f:
            pickle.dump(phys_scaler, f)
        logger.info(f"Saved phys scaler: {scaler_path}")

        return graphs_path

    def run(self):
        """Execute full data preparation pipeline."""
        logger.info("Starting data preparation")

        # Load and clean
        df = self.load_and_clean_data()

        # Deduplicate
        df_dedup = self.deduplicate_data(df)

        # Prepare MLP features
        df_mlp, X_mlp, y_mlp = self.prepare_mlp_features(df_dedup)
        mlp_path = self.save_mlp_features(df_mlp, X_mlp, y_mlp)

        # Prepare GNN features
        graph_list, phys_scaler = self.prepare_gnn_features(df_mlp)
        gnn_path = self.save_gnn_features(graph_list, phys_scaler)

        logger.info("Data preparation complete")
        logger.info(f"MLP features: {mlp_path}")
        logger.info(f"GNN graphs: {gnn_path}")


if __name__ == "__main__":
    # Paths
    RAW_PARQUET = r'C:\Users\natalia\Documents\Studia\Warsztaty_sztucznej_inteligencji\Warsztaty_sztucznej_inteligencji\data\chembl_joined_2147_20260604_215317.parquet'
    DATA_DIR = Path(__file__).parent.parent / 'data'

    # Run preparation
    prep = DataPreparation(RAW_PARQUET, str(DATA_DIR))
    prep.run()
