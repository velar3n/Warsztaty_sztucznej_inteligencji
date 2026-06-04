import logging
import os
import sys
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
    # Impute missing units - if there is no standard unit defined, but the value is in a nM range - set it as nM
    imputed_units = """
        CASE
            WHEN act.standard_units IS NULL
                AND act.standard_value IS NOT NULL
                AND act.standard_value BETWEEN 0.01 AND 1e6
            THEN 'nM'::TEXT
            ELSE act.standard_units::TEXT
        END
    """.strip()

    # Calculate pIC50 as a standarized scale kinda and use pchembl value as fallback
    pic50_calculation = f"""
        CASE
            WHEN ({imputed_units}) = 'nM'
                AND act.standard_value IS NOT NULL
                AND act.standard_value > 0
            THEN (-LOG(act.standard_value * 1e-9))::REAL
            ELSE act.pchembl_value::REAL
        END
    """.strip()


    query = f"""
    SELECT
        -- Activities
        act.activity_id::INT               AS activity_id,
        act.molregno::INT                  AS molregno,
        act.standard_value::REAL           AS standard_value,
        {imputed_units}                    AS standard_units,
        act.standard_type::TEXT            AS standard_type,
        act.standard_relation::TEXT        AS standard_relation,
        act.pchembl_value::REAL            AS pchembl_value,
        {pic50_calculation}                AS pIC50,

        -- Just mark as boolean value if the data has a data validity comment (usually indicates sth is sus about it)
        (act.data_validity_comment IS NOT NULL)::BOOLEAN AS has_validity_comment,

        -- Assay information
        asy.assay_type::TEXT               AS assay_type,
        asy.assay_organism::TEXT           AS assay_organism,
        asy.relationship_type::TEXT        AS assay_relationship,
        asy.confidence_score::INT          AS confidence_score,

        -- Target information
        td.chembl_id::TEXT                 AS target_chembl_id,
        LOWER(td.pref_name)::TEXT          AS target_name,
        LOWER(td.organism)::TEXT           AS organism,

        -- Chemical structure
        cs.canonical_smiles::TEXT          AS canonical_smiles,

        -- Compound properties
        cp.mw_freebase::REAL               AS mw_freebase,
        cp.alogp::REAL                     AS alogp,
        cp.hba::INT                        AS hba,
        cp.hbd::INT                        AS hbd,
        cp.psa::REAL                       AS psa,
        cp.rtb::INT                        AS rtb,
        cp.aromatic_rings::INT             AS aromatic_rings,
        cp.qed_weighted::REAL              AS qed_weighted

    FROM activities act
    INNER JOIN assays asy ON act.assay_id = asy.assay_id
    INNER JOIN target_dictionary td ON asy.tid = td.tid
    INNER JOIN compound_structures cs ON act.molregno = cs.molregno
    INNER JOIN compound_properties cp ON act.molregno = cp.molregno

    WHERE
        -- Filter only chosen target group
        td.chembl_id IN ('CHEMBL2147')

        -- Filter potential duplicates
        AND (act.potential_duplicate IS NULL OR act.potential_duplicate = 0)
        
        -- Filter valid pIC50
        AND {pic50_calculation} IS NOT NULL
        
        -- Filter valid SMILES
        AND cs.canonical_smiles IS NOT NULL

        -- Filter assay organism to homo sapiens
        AND LOWER(asy.assay_organism) = 'homo sapiens'
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

        return df

    except DatabaseError as e:
        logger.error(f"Query execution failed: {e}")
        raise


def save_to_parquet(df: pd.DataFrame, output_dir: str = "data") -> str:
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"chembl_joined_{timestamp}.parquet"
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
    conn = None
    try:
        conn = get_db_connection()

        df = load_chembl_data(conn)
        if df.empty:
            logger.error("No data loaded. Exiting.")
            sys.exit(1)

        output_path = save_to_parquet(df)
        logger.info(f"Output file: {output_path}")

        sys.exit(0)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

    finally:
        if conn is not None:
            conn.close()
            logger.info("Database connection closed")