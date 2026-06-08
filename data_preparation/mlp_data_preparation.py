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


PHYSCHEM_COLS = ["mw_freebase", "alogp", "hba", "hbd", "psa", "rtb", "aromatic_rings", "qed_weighted"]
MORGAN_RADIUS = 2
MORGAN_NBITS  = 2048
PIC50_COL     = "pic50"

mfp_gen = rdFingerprintGenerator.GetMorganGenerator(radius=MORGAN_RADIUS, fpSize=MORGAN_NBITS)


def find_latest_source_file(data_dir: Path) -> Path:
    pattern = "chembl_joined_2147_*.parquet"
    files = sorted(data_dir.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError(f"No {pattern} found in {data_dir}")
    return files[0]


def smiles_to_fingerprint(smiles: str) -> np.ndarray | None:
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return None
        return mfp_gen.GetFingerprintAsNumPy(mol).astype(np.float32)
    except Exception:
        return None


def load_and_prepare(source_path: Path) -> pd.DataFrame:
    required_cols = ["activity_id", "canonical_smiles", PIC50_COL, "has_validity_comment"] + PHYSCHEM_COLS

    chunks       = []
    total_rows   = 0
    total_failed = 0

    for batch in pq.ParquetFile(source_path).iter_batches(batch_size=10_000, columns=required_cols):
        chunk = batch.to_pandas()

        # Drop missing targets / SMILES / flagged rows
        chunk = chunk.dropna(subset=["canonical_smiles", PIC50_COL])
        chunk = chunk[chunk[PIC50_COL].between(3, 12)].reset_index(drop=True)

        total_rows += len(chunk)

        # Compute fingerprints
        fingerprints = chunk["canonical_smiles"].apply(smiles_to_fingerprint)
        valid_mask   = fingerprints.notna()
        total_failed += (~valid_mask).sum()

        chunk        = chunk[valid_mask].copy()
        fingerprints = np.vstack(fingerprints[valid_mask].values)  # (n, 2048)

        # Concatenate fingerprint + physchem into a single feature vector per row
        physchem = chunk[PHYSCHEM_COLS].fillna(chunk[PHYSCHEM_COLS].median()).values.astype(np.float32)
        features = np.concatenate([fingerprints, physchem], axis=1)  # (n, 2056)

        result = pd.DataFrame({
            "activity_id": chunk["activity_id"].values,
            PIC50_COL:     chunk[PIC50_COL].values.astype(np.float32),
            "features":    list(features),
        })
        chunks.append(result)

    df = pd.concat(chunks, ignore_index=True)
    logger.info(f"Processed {total_rows:,} rows → {len(df):,} valid ({total_failed} failed fingerprints)")
    return df


def main() -> int:
    start_time   = time.time()
    project_root = Path(__file__).parent.parent
    data_dir     = project_root / "data"
    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("Starting MLP data preparation...")

    try:
        source_path = find_latest_source_file(data_dir)
        logger.info(f"Source: {source_path}")

        df = load_and_prepare(source_path)

        # Save as two arrays — features matrix and targets vector
        # NOTE: physchem features are NOT scaled here intentionally.
        #       Scale AFTER splitting to avoid leakage (fit scaler on train only).
        X = np.vstack(df["features"].values)              # (n, 2056)
        y = df[PIC50_COL].values.astype(np.float32)       # (n,)
        ids = df["activity_id"].values                     # (n,)  — useful for debugging

        output_dir = data_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f"mlp_prepared_2147_{timestamp}.parquet"
        temp_path   = output_dir / "mlp_prepared.tmp.parquet"

        df_out = pd.DataFrame({
            "activity_id": ids,
            "pic50":       y,
            "features":    list(X),   # stored as list of arrays — loads back cleanly
        })

        df_out.to_parquet(temp_path, engine="pyarrow", index=False)
        temp_path.rename(output_path)

        logger.info("=" * 60)
        logger.info(f"X shape : {X.shape}  (2048 Morgan + 8 physchem)")
        logger.info(f"y shape : {y.shape}")
        logger.info(f"Output  : {output_dir}")
        logger.info(f"Elapsed : {time.time() - start_time:.1f}s")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())