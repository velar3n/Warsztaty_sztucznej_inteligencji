import logging
import os
import sys
import time
import pandas as pd
import psycopg2

from datetime import datetime
from psycopg2 import OperationalError, DatabaseError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def get_db_connection() -> psycopg2.extensions.connection:
    logger.info("Connecting to ChEMBL database...")

    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="chembl",
            user="postgres",
            password="postgres"
        )
        logger.info("Connection established successfully")
        return conn
    except OperationalError as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def build_query() -> str:
    query = """
    SELECT
        -- Activity measurements
        act.activity_id,
        act.molregno,
        act.standard_value,
        CASE
            WHEN act.standard_units IS NULL
                AND act.standard_value IS NOT NULL
                AND act.standard_value >= 0.01
                AND act.standard_value <= 1e6
            THEN 'nM'::TEXT
            ELSE act.standard_units
        END AS standard_units,
        act.standard_type,
        act.standard_relation,
        act.pchembl_value,
        CASE
            WHEN (
                CASE
                    WHEN act.standard_units IS NULL
                        AND act.standard_value IS NOT NULL
                        AND act.standard_value >= 0.01
                        AND act.standard_value <= 1e6
                    THEN 'nM'
                    ELSE act.standard_units
                END
            ) = 'nM' AND act.standard_value IS NOT NULL AND act.standard_value > 0
            THEN -LOG(act.standard_value * 1e-9)
            ELSE act.pchembl_value
        END AS pIC50,
        CASE
            WHEN act.data_validity_comment IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS has_validity_comment,

        -- Assay information
        asy.assay_type,
        asy.assay_organism,
        asy.relationship_type AS assay_relationship,
        asy.confidence_score,

        -- Target information
        td.chembl_id AS target_chembl_id,
        LOWER(td.pref_name) AS target_name,
        LOWER(td.organism) AS organism,

        -- Chemical structure
        cs.canonical_smiles,

        -- Compound properties
        cp.mw_freebase,
        cp.alogp,
        cp.hba,
        cp.hbd,
        cp.psa,
        cp.rtb,
        cp.aromatic_rings,
        cp.qed_weighted

    FROM activities act
    INNER JOIN assays asy ON act.assay_id = asy.assay_id
    INNER JOIN target_dictionary td ON asy.tid = td.tid
    INNER JOIN compound_structures cs ON act.molregno = cs.molregno
    INNER JOIN compound_properties cp ON act.molregno = cp.molregno

    WHERE
        -- Only Alzheimer proteins
        td.chembl_id IN ('CHEMBL220', 'CHEMBL4822', 'CHEMBL3177')
    
        -- Filter potential duplicates
        AND (act.potential_duplicate IS NULL OR act.potential_duplicate = 0)

        -- Filter valid pIC50
        AND (
            CASE
                WHEN (
                    CASE
                        WHEN act.standard_units IS NULL
                            AND act.standard_value IS NOT NULL
                            AND act.standard_value >= 0.01
                            AND act.standard_value <= 1e6
                        THEN 'nM'
                        ELSE act.standard_units
                    END
                ) = 'nM' AND act.standard_value IS NOT NULL AND act.standard_value > 0
                THEN -LOG(act.standard_value * 1e-9)
                ELSE act.pchembl_value
            END
        ) IS NOT NULL

        -- Filter valid SMILES
        AND cs.canonical_smiles IS NOT NULL
    """

    return query


def load_chembl_data(conn: psycopg2.extensions.connection, chunksize: int = 100000) -> pd.DataFrame:
    logger.info("Executing SQL query...")

    try:
        query = build_query()

        logger.info("Query executed, fetching results in chunks...")
        chunks = []
        chunk_num = 0

        for chunk in pd.read_sql(query, conn, chunksize=chunksize):
            chunk_num += 1
            chunks.append(chunk)
            logger.info(f"Fetched chunk {chunk_num} with {len(chunk)} rows")

        if not chunks:
            logger.warning("No data returned from query!")
            return pd.DataFrame()

        # Concatenate all chunks
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"Total rows fetched: {len(df)}")

        # Data quality checks
        logger.info(f"pIC50 range: {df['pic50'].min():.2f} - {df['pic50'].max():.2f}")
        logger.info(f"Number of unique compounds: {df['molregno'].nunique()}")

        if len(df) < 100000:
            logger.warning(f"Row count ({len(df)}) is unexpectedly low. Check data quality.")

        return df

    except DatabaseError as e:
        logger.error(f"Query execution failed: {e}")
        raise


def save_to_parquet(df: pd.DataFrame, output_dir: str = "data") -> str:
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"chembl_joined_alzheimer_{timestamp}.parquet"
    filepath = os.path.join(output_dir, filename)

    logger.info(f"Writing to parquet: {filepath}")

    try:
        df.to_parquet(filepath, engine='pyarrow', index=False)
        logger.info(f"Parquet file written successfully: {filepath}")
        logger.info(f"File contains {len(df)} rows and {len(df.columns)} columns")
        return filepath

    except Exception as e:
        logger.error(f"Failed to write parquet file: {e}")
        raise IOError(f"Parquet write failed: {e}")


if __name__ == "__main__":
    start_time = time.time()
    conn = None
    try:
        conn = get_db_connection()

        # Load data with chunked processing
        df = load_chembl_data(conn)

        # Check if data was loaded
        if df.empty:
            logger.error("No data loaded. Exiting.")
            sys.exit(1)

        # Save to parquet
        output_path = save_to_parquet(df)

        # Report execution time
        elapsed_time = time.time() - start_time
        logger.info(f"Execution completed in {elapsed_time:.2f} seconds")
        logger.info(f"Output file: {output_path}")

        sys.exit(0)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

    finally:
        # Close database connection
        if conn is not None:
            conn.close()
            logger.info("Database connection closed")