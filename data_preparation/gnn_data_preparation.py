import logging
import sys
import time
import torch
import pandas as pd
import pyarrow.parquet as pq

from pathlib import Path
from datetime import datetime
from torch_geometric.data import Data
from rdkit import Chem
from rdkit.Chem import rdchem


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# Encoding constants
ATOMIC_NUM_LIST = [1, 5, 6, 7, 8, 9, 14, 15, 16, 17, 34, 35, 53]

HYBRIDIZATION_TYPES = [
    rdchem.HybridizationType.SP,
    rdchem.HybridizationType.SP2,
    rdchem.HybridizationType.SP3,
    rdchem.HybridizationType.SP3D,
    rdchem.HybridizationType.SP3D2,
]

BOND_TYPES = [
    rdchem.BondType.SINGLE,
    rdchem.BondType.DOUBLE,
    rdchem.BondType.TRIPLE,
    rdchem.BondType.AROMATIC,
]

BOND_STEREO_TYPES = [
    rdchem.BondStereo.STEREONONE,
    rdchem.BondStereo.STEREOZ,
    rdchem.BondStereo.STEREOE,
    rdchem.BondStereo.STEREOCIS,
    rdchem.BondStereo.STEREOTRANS,
]

CHIRAL_TAGS = [
    rdchem.ChiralType.CHI_UNSPECIFIED,
    rdchem.ChiralType.CHI_TETRAHEDRAL_CW,
    rdchem.ChiralType.CHI_TETRAHEDRAL_CCW,
]


def one_hot_encode(value, categories: list) -> list[int]:
    return [1 if value == cat else 0 for cat in categories]


def extract_node_features(mol: Chem.Mol) -> list[list[float]] | None:
    node_features = []

    for atom in mol.GetAtoms():
        # Atomic number one-hot (13 dimensions)
        atomic_num = atom.GetAtomicNum()
        if atomic_num not in ATOMIC_NUM_LIST:
            return None  # Unknown atom type
        one_hot_atomic = one_hot_encode(atomic_num, ATOMIC_NUM_LIST)

        # Hybridization one-hot (5 dimensions)
        hybridization = atom.GetHybridization()
        if hybridization not in HYBRIDIZATION_TYPES:
            one_hot_hybrid = [0] * len(HYBRIDIZATION_TYPES)
        else:
            one_hot_hybrid = one_hot_encode(hybridization, HYBRIDIZATION_TYPES)

        # Chiral tag one-hot (3 dimensions)
        chiral_tag = atom.GetChiralTag()
        if chiral_tag not in CHIRAL_TAGS:
            one_hot_chiral = [0] * len(CHIRAL_TAGS)
        else:
            one_hot_chiral = one_hot_encode(chiral_tag, CHIRAL_TAGS)

        # Continuous features (7 dimensions)
        degree = atom.GetTotalDegree() / 4.0
        formal_charge = (atom.GetFormalCharge() + 4) / 8.0
        num_hs = atom.GetTotalNumHs() / 4.0
        total_valence = atom.GetTotalValence() / 8.0
        num_radical = atom.GetNumRadicalElectrons() / 2.0
        is_aromatic = float(atom.GetIsAromatic())
        is_in_ring = float(atom.IsInRing())

        # Combine all features
        features = (
            one_hot_atomic +
            one_hot_hybrid +
            one_hot_chiral +
            [degree, formal_charge, num_hs, total_valence, num_radical, is_aromatic, is_in_ring]
        )

        node_features.append(features)

    return node_features


def extract_edge_features(mol: Chem.Mol) -> tuple[list[list[int]], list[list[float]]]:
    edge_index = []
    edge_features = []

    for bond in mol.GetBonds():
        u = bond.GetBeginAtomIdx()
        v = bond.GetEndAtomIdx()

        # Bond type one-hot 
        bond_type = bond.GetBondType()
        if bond_type not in BOND_TYPES:
            one_hot_bond = [0] * len(BOND_TYPES)
        else:
            one_hot_bond = one_hot_encode(bond_type, BOND_TYPES)

        # Stereo one-hot
        stereo = bond.GetStereo()
        if stereo not in BOND_STEREO_TYPES:
            one_hot_stereo = [0] * len(BOND_STEREO_TYPES)
        else:
            one_hot_stereo = one_hot_encode(stereo, BOND_STEREO_TYPES)

        # Additional features
        is_conjugated = float(bond.GetIsConjugated())
        is_in_ring = float(bond.IsInRing())

        # Combine features
        bond_features = one_hot_bond + one_hot_stereo + [is_conjugated, is_in_ring]

        # Add bidirectional edges
        edge_index.append([u, v])
        edge_features.append(bond_features)
        edge_index.append([v, u])
        edge_features.append(bond_features)

    return edge_index, edge_features


def smiles_to_graph(smiles: str, activity_id: int, pic50: float) -> Data | None:
    if not smiles:
        return None

    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None

        # Check for zero atoms
        if mol.GetNumAtoms() == 0:
            return None

        # Extract node features
        node_features = extract_node_features(mol)
        if node_features is None:
            return None

        # Extract edge features
        edge_index, edge_features = extract_edge_features(mol)

        # Convert to tensors
        x = torch.tensor(node_features, dtype=torch.float)
        y = torch.tensor([pic50], dtype=torch.float)

        if len(edge_index) > 0:
            edge_index_t = torch.tensor(edge_index, dtype=torch.long).t().contiguous()
            edge_attr_t = torch.tensor(edge_features, dtype=torch.float)
        else:
            # No bonds: empty edge tensors
            edge_index_t = torch.empty((2, 0), dtype=torch.long)
            edge_attr_t = torch.empty((0, 11), dtype=torch.float)

        return Data(
            x=x,
            edge_index=edge_index_t,
            edge_attr=edge_attr_t,
            y=y,
            smiles=smiles,
            activity_id=activity_id
        )

    except Exception:
        return None


def process_chunk(chunk: pd.DataFrame) -> tuple[list[Data], int]:
    graphs = []
    num_failed = 0

    for _, row in chunk.iterrows():
        graph = smiles_to_graph(
            row['canonical_smiles'],
            row['activity_id'],
            row['pic50']
        )

        if graph is None:
            num_failed += 1
            continue

        graphs.append(graph)

    return graphs, num_failed


def find_latest_source_file(data_dir: Path) -> Path:
    pattern = "chembl_joined_2147_*.parquet"
    files = sorted(data_dir.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError(f"No {pattern} found in {data_dir}")
    return files[0]


def main():
    start_time = time.time()

    # Setup paths
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"

    logger.info("Starting GNN data preparation...")

    try:
        # Find source file
        source_path = find_latest_source_file(data_dir)
        logger.info(f"Source file: {source_path}")

        # Process data in chunks
        batch_graphs = []  # Accumulator for current batch
        total_graphs = 0
        total_rows = 0
        total_failed = 0
        chunk_num = 0
        batch_num = 0
        total_nodes = 0
        total_edges = 0
        output_files = []

        chunksize = 100_000
        chunks_per_save = 10  # Save every 1M rows
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info(f"Processing in chunks of {chunksize:,} rows...")
        logger.info(f"Saving to parquet every {chunks_per_save * chunksize:,} rows...")

        # Use PyArrow ParquetFile for chunked reading
        parquet_file = pq.ParquetFile(source_path)

        for batch in parquet_file.iter_batches(batch_size=chunksize, columns=['activity_id', 'canonical_smiles', 'pic50']):
            # Convert to pandas DataFrame
            chunk = batch.to_pandas()

            # Filter nulls
            chunk = chunk.dropna(subset=['canonical_smiles', 'pic50'])

            chunk_num += 1
            total_rows += len(chunk)

            # Transform chunk
            graphs, num_failed = process_chunk(chunk)
            batch_graphs.extend(graphs)
            total_failed += num_failed

            # Collect stats
            for g in graphs:
                total_nodes += g.x.shape[0]
                total_edges += g.edge_index.shape[1]

            # Save every 10 chunks (1M rows)
            if chunk_num % chunks_per_save == 0 and batch_graphs:
                batch_num += 1
                output_filename = f"gnn_graphs_{timestamp}_part{batch_num:04d}.pt"
                output_path = data_dir / output_filename
                temp_path = output_path.with_suffix('.tmp')

                logger.info(f"Saving batch {batch_num}: {len(batch_graphs):,} graphs to {output_filename}...")
                torch.save(batch_graphs, temp_path, pickle_protocol=4)
                temp_path.rename(output_path)

                output_files.append(output_path)
                total_graphs += len(batch_graphs)
                batch_graphs = []  # Reset accumulator

            # Progress logging every 1M rows
            if total_rows % 1_000_000 < chunksize:
                logger.info(f"Processed {total_rows:,} rows ({chunk_num} chunks), {total_failed:,} failures so far")

        # Save remaining graphs if any
        if batch_graphs:
            batch_num += 1
            output_filename = f"gnn_graphs_{timestamp}_part{batch_num:04d}.pt"
            output_path = data_dir / output_filename
            temp_path = output_path.with_suffix('.tmp')

            logger.info(f"Saving final batch {batch_num}: {len(batch_graphs):,} graphs to {output_filename}...")
            torch.save(batch_graphs, temp_path, pickle_protocol=4)
            temp_path.rename(output_path)

            output_files.append(output_path)
            total_graphs += len(batch_graphs)

        # Summary statistics
        elapsed_time = time.time() - start_time
        total_size_gb = sum(f.stat().st_size for f in output_files) / (1024**3)
        avg_nodes = total_nodes / total_graphs if total_graphs else 0
        avg_edges = total_edges / total_graphs if total_graphs else 0

        logger.info("GNN Data Preparation Complete")
        logger.info(f"Total rows processed: {total_rows:,}")
        logger.info(f"Successful transformations: {total_graphs:,}")
        logger.info(f"Failed transformations: {total_failed:,}")
        logger.info(f"Success rate: {total_graphs/total_rows*100:.2f}%")
        logger.info(f"Average nodes per graph: {avg_nodes:.2f}")
        logger.info(f"Average edges per graph: {avg_edges:.2f}")
        logger.info(f"Number of output files: {len(output_files)}")
        logger.info(f"Output files: {', '.join(f.name for f in output_files)}")
        logger.info(f"Total output size: {total_size_gb:.2f} GB")
        logger.info(f"Elapsed time: {elapsed_time/60:.2f} minutes")

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())