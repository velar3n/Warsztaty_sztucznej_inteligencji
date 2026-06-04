import sys
import time
import logging
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pathlib import Path
from datetime import datetime
from rdkit import Chem
from rdkit.Chem import rdFingerprintGenerator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# Initialize fingerprint generator (radius=2, 2048 bits) - internet sais it's a solid default :>
mfp_gen = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=2048)


def find_latest_source_file(data_dir: Path) -> Path:
    pattern = "chembl_joined_*.parquet"
    files = sorted(data_dir.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError(f"No {pattern} found in {data_dir}")
    return files[0]


# We can't feed strings to the model, makes no sense, so we transform the SMILES into numerical vectors (fingerprints)
def smiles_to_fingerprint(smiles: str) -> np.ndarray | None:
    if not smiles:
        return None

    # SMILES -> RDKit molecule -> Fingerprint -> np array for smarter saving
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None

        fp = mfp_gen.GetFingerprintAsNumPy(mol)
        return fp.astype(np.float32)
    except Exception:
        return None


def process_chunk(chunk: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    results = []
    num_failed = 0

    for _, row in chunk.iterrows():
        fp = smiles_to_fingerprint(row['canonical_smiles'])

        if fp is None:
            num_failed += 1
            continue

        results.append({
            'activity_id': row['activity_id'],
            'fingerprint': fp,
            'pic50': row['pic50']
        })

    return pd.DataFrame(results), num_failed


def main():
    start_time = time.time()

    # Setup paths
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"

    logger.info("Starting MLP data preparation...")

    try:
        # Find source file
        source_path = find_latest_source_file(data_dir)
        logger.info(f"Source file: {source_path}")

        # Process data in chunks
        batch_chunks = []  # Accumulator for current batch
        total_records = 0
        total_rows = 0
        total_failed = 0
        chunk_num = 0
        batch_num = 0
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
            transformed_chunk, num_failed = process_chunk(chunk)
            batch_chunks.append(transformed_chunk)
            total_failed += num_failed

            # Save every 10 chunks (1M rows)
            if chunk_num % chunks_per_save == 0 and batch_chunks:
                batch_num += 1
                batch_df = pd.concat(batch_chunks, ignore_index=True)

                output_filename = f"mlp_features_{timestamp}_part{batch_num:04d}.parquet"
                output_path = data_dir / output_filename
                temp_path = output_path.with_suffix('.tmp')

                logger.info(f"Saving batch {batch_num}: {len(batch_df):,} records to {output_filename}...")
                batch_df.to_parquet(temp_path, engine='pyarrow', index=False)
                temp_path.rename(output_path)

                output_files.append(output_path)
                total_records += len(batch_df)
                batch_chunks = []  # Reset accumulator

            # Progress logging every 1M rows
            if total_rows % 1_000_000 < chunksize:
                logger.info(f"Processed {total_rows:,} rows ({chunk_num} chunks), {total_failed:,} failures so far")

        # Save remaining chunks if any
        if batch_chunks:
            batch_num += 1
            batch_df = pd.concat(batch_chunks, ignore_index=True)

            output_filename = f"mlp_features_{timestamp}_part{batch_num:04d}.parquet"
            output_path = data_dir / output_filename
            temp_path = output_path.with_suffix('.tmp')

            logger.info(f"Saving final batch {batch_num}: {len(batch_df):,} records to {output_filename}...")
            batch_df.to_parquet(temp_path, engine='pyarrow', index=False)
            temp_path.rename(output_path)

            output_files.append(output_path)
            total_records += len(batch_df)

        # Summary statistics
        elapsed_time = time.time() - start_time
        total_size_gb = sum(f.stat().st_size for f in output_files) / (1024**3)

        logger.info("=" * 60)
        logger.info("MLP Data Preparation Complete")
        logger.info(f"Total rows processed: {total_rows:,}")
        logger.info(f"Successful transformations: {total_records:,}")
        logger.info(f"Failed transformations: {total_failed:,}")
        logger.info(f"Success rate: {total_records/total_rows*100:.2f}%")
        logger.info(f"Number of output files: {len(output_files)}")
        logger.info(f"Output files: {', '.join(f.name for f in output_files)}")
        logger.info(f"Total output size: {total_size_gb:.2f} GB")
        logger.info(f"Elapsed time: {elapsed_time/60:.2f} minutes")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())