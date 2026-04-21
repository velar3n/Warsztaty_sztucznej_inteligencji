"""
1. Connect to the database (PostgreSQL). 
    The database connection can be a class in another file, e.g., db_connection.py, which can be imported in the Spark jobs.
    This class will handle the connection to the database and provide methods for reading and writing data.

2. Load the data from PostgreSQL:
SELECT 
    act.activity_id::INT AS activity_id,
    act.molregno::INT AS molregno,
    cs.canonical_smiles::TEXT AS canonical_smiles,
    cp.mw_freebase::REAL AS mw_freebase,
    cp.alogp::REAL AS alogp,
    cp.hba::INT AS hba,
    cp.hbd::INT AS hbd,
    cp.psa::REAL AS psa,
    cp.rtb::INT AS rtb,
    cp.aromatic_rings::INT AS aromatic_rings,
    cp.qed_weighted::REAL AS qed_weighted,
    act.standard_value::REAL AS standard_value,
    act.standard_units::TEXT AS standard_units,
    act.standard_type::TEXT AS standard_type,
    act.standard_relation::TEXT AS standard_relation,
    act.pchembl_value::REAL AS pchembl_value,
    td.chembl_id::TEXT AS target_chembl_id,
    td.pref_name::TEXT AS target_name,
    ass.confidence_score::INT AS confidence_score
FROM activities act
JOIN assays ass ON act.assay_id = ass.assay_id
JOIN target_dictionary td ON ass.tid = td.tid
JOIN compound_structures cs ON act.molregno = cs.molregno
JOIN compound_properties cp ON act.molregno = cp.molregno
WHERE LOWER(td.organism) = 'homo sapiens'
  AND cs.canonical_smiles IS NOT NULL
  AND (act.potential_duplicate IS NULL OR act.potential_duplicate = 0)
  AND (act.pchembl_value IS NOT NULL OR act.standard_value IS NOT NULL)
LIMIT 1000000;

3. Units unity like this:
class DataMisc:
    @staticmethod
    def compute_pIC50(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.when(
                (pl.col("standard_units") == "nM") & pl.col("standard_value").is_not_null()
            )
            .then(-(pl.col("standard_value") * 1e-9).log10())
            .otherwise(pl.col("pchembl_value")) # If no calculation is possible, use the ready pchembl_value
            .alias("pIC50")
        )

    @staticmethod
    def impute_units(df: pl.DataFrame) -> pl.DataFrame:
        mask_missing = pl.col("standard_units").is_null() & pl.col("standard_value").is_not_null()
        mask_range = (pl.col("standard_value") >= 0.01) & (pl.col("standard_value") <= 1e6)

        return df.with_columns(
            pl.when(mask_missing & mask_range)
            .then(pl.lit("nM"))
            .otherwise(pl.col("standard_units"))
            .alias("standard_units")
        )

4. Final cleaning
        df_clean = (
            df.filter(
                pl.col("pIC50").is_not_null() & 
                pl.col("pIC50").is_infinite().not_()
            )
            .unique(subset=["canonical_smiles"]) # Remove duplicate structures
        )

5. output_path = output_dir / "ChEMBL_processed.parquet"
final_df.write_parquet(output_path, compression="zstd")

7. logging.info("ETL succeeded")
logging.info(f"Saved {final_df.height} records into: {output_path}")


"""