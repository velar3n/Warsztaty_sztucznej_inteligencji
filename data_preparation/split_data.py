"""
Scaffold-based splitting for MLP and GNN datasets.
Uses Bemis-Murcko scaffolds for 80/10/10 train/val/test split.
"""

import logging
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd
import torch

from rdkit import Chem
from rdkit.Chem.Scaffolds import MurckoScaffold


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


SEED = 42


def get_scaffold(smi: str) -> str:
    """
    Compute Bemis-Murcko scaffold SMILES.
    Falls back to full SMILES for acyclic molecules.
    """
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        return smi
    scaffold = MurckoScaffold.MurckoScaffoldSmiles(mol=mol, includeChirality=False)
    return scaffold if scaffold else smi


class ScaffoldSplitter:
    """Performs scaffold-based splitting of molecular datasets."""

    def __init__(self, mlp_parquet_path: str, gnn_pt_path: str, output_dir: str,
                 val_frac: float = 0.10, test_frac: float = 0.10):
        """
        Initialize scaffold splitter.

        Args:
            mlp_parquet_path: Path to prepared MLP features parquet
            gnn_pt_path: Path to prepared GNN graphs .pt file
            output_dir: Directory to save split indices
            val_frac: Fraction for validation set
            test_frac: Fraction for test set
        """
        self.mlp_parquet_path = mlp_parquet_path
        self.gnn_pt_path = gnn_pt_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.val_frac = val_frac
        self.test_frac = test_frac

        np.random.seed(SEED)

        logger.info("Initialized ScaffoldSplitter")
        logger.info(f"MLP features: {mlp_parquet_path}")
        logger.info(f"GNN graphs: {gnn_pt_path}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Split fractions - val: {val_frac:.2f}, test: {test_frac:.2f}")

    def load_data(self) -> tuple:
        """
        Load prepared datasets and verify alignment.

        Returns:
            df_mlp_feat: MLP features dataframe
            gnn_graphs: List of PyG Data objects
            smiles_list: List of canonical SMILES
        """
        logger.info("Loading prepared datasets...")

        # Load MLP features
        df_mlp_feat = pd.read_parquet(self.mlp_parquet_path)
        logger.info(f"Loaded MLP features: {df_mlp_feat.shape}")

        # Load GNN graphs
        gnn_graphs = torch.load(self.gnn_pt_path, weights_only=False)
        logger.info(f"Loaded GNN graphs: {len(gnn_graphs)} molecules")

        # Verify alignment
        assert len(df_mlp_feat) == len(gnn_graphs), \
            f"Size mismatch: MLP={len(df_mlp_feat)}, GNN={len(gnn_graphs)}"

        for i in range(len(df_mlp_feat)):
            assert df_mlp_feat.iloc[i]["canonical_smiles"] == gnn_graphs[i].smiles, \
                f"SMILES mismatch at index {i}"

        smiles_list = df_mlp_feat['canonical_smiles'].tolist()
        logger.info(f"Total compounds: {len(smiles_list)}")

        return df_mlp_feat, gnn_graphs, smiles_list

    def compute_scaffold_split(self, smiles_list: list) -> dict:
        """
        Perform standard Bemis-Murcko scaffold split.

        Args:
            smiles_list: List of canonical SMILES strings

        Returns:
            Dictionary with 'train', 'val', 'test' keys mapping to index arrays
        """
        logger.info("Computing scaffold split (standard Bemis-Murcko)...")

        N = len(smiles_list)
        target_test = int(N * self.test_frac)
        target_val = int(N * self.val_frac)

        # Group indices by scaffold
        scaffold_to_indices = defaultdict(list)
        for idx, smi in enumerate(smiles_list):
            scaffold_to_indices[get_scaffold(smi)].append(idx)

        # Sort scaffolds by size (largest first)
        scaffold_sets = sorted(scaffold_to_indices.values(), key=len, reverse=True)

        logger.info(f"Found {len(scaffold_sets)} unique scaffolds")

        # Assign scaffolds to splits
        sc_train, sc_val, sc_test = [], [], []
        counts = {'train': 0, 'val': 0, 'test': 0}

        for group in scaffold_sets:
            if counts['test'] < target_test:
                sc_test.extend(group)
                counts['test'] += len(group)
            elif counts['val'] < target_val:
                sc_val.extend(group)
                counts['val'] += len(group)
            else:
                sc_train.extend(group)
                counts['train'] += len(group)

        scaffold_split = {
            'train': np.array(sc_train),
            'val': np.array(sc_val),
            'test': np.array(sc_test),
        }

        logger.info(f"Scaffold split sizes:")
        logger.info(f"  Train: {len(sc_train)} ({len(sc_train)/N*100:.1f}%)")
        logger.info(f"  Val:   {len(sc_val)} ({len(sc_val)/N*100:.1f}%)")
        logger.info(f"  Test:  {len(sc_test)} ({len(sc_test)/N*100:.1f}%)")

        return scaffold_split

    def save_splits(self, scaffold_split: dict):
        """
        Save split indices to disk.

        Args:
            scaffold_split: Dictionary with split indices
        """
        logger.info("Saving split indices...")

        for split_name, idx in scaffold_split.items():
            output_path = self.output_dir / f'scaffold_{split_name}.npy'
            np.save(output_path, idx)
            logger.info(f"  Saved {split_name}: {output_path}")

    def verify_splits(self, df_mlp_feat: pd.DataFrame, scaffold_split: dict):
        """
        Verify splits by checking pic50 distributions.

        Args:
            df_mlp_feat: MLP features dataframe
            scaffold_split: Dictionary with split indices
        """
        logger.info("Verifying splits (pic50 distribution)...")

        y_all = df_mlp_feat['pic50'].values

        for name, idx in scaffold_split.items():
            y_split = y_all[idx]
            logger.info(f"  {name:6s}: mean={y_split.mean():.3f}, std={y_split.std():.3f}, "
                       f"min={y_split.min():.3f}, max={y_split.max():.3f}")

    def run(self):
        """Execute splitting pipeline."""
        logger.info("Starting scaffold splitting")

        # Load data
        df_mlp_feat, gnn_graphs, smiles_list = self.load_data()

        # Compute scaffold split
        scaffold_split = self.compute_scaffold_split(smiles_list)

        # Save splits
        self.save_splits(scaffold_split)

        # Verify splits
        self.verify_splits(df_mlp_feat, scaffold_split)

        logger.info("Scaffold splitting complete")


if __name__ == "__main__":
    # Paths
    DATA_DIR = Path(__file__).parent.parent / 'data'
    MLP_PARQUET = DATA_DIR / 'mlp_features_2147_mlp.parquet'
    GNN_PT = DATA_DIR / 'gnn_graphs_2147_gnn.pt'
    SPLIT_DIR = DATA_DIR / 'splits'

    # Run splitting
    splitter = ScaffoldSplitter(
        mlp_parquet_path=str(MLP_PARQUET),
        gnn_pt_path=str(GNN_PT),
        output_dir=str(SPLIT_DIR)
    )
    splitter.run()
