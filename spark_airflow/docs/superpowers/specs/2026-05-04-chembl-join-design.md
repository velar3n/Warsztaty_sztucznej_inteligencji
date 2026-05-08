# ChEMBL Table Join Job Design

**Date:** 2026-05-04  
**Purpose:** Create a Spark job to join cleaned ChEMBL tables and produce a unified dataset for machine learning analysis

## Overview

This design specifies a new Spark job (`join_chembl_tables.py`) that consolidates five cleaned ChEMBL parquet tables into a single joined dataset. The job applies data quality filters to ensure only valid, complete records are included in the final output.

## Input Data

The job reads from five cleaned parquet files in `/opt/spark/data/cleaned/`:
1. `activities_clean.parquet` - Contains activity measurements with pre-computed pIC50 values
2. `assays_clean.parquet` - Assay metadata and confidence scores
3. `target_dictionary_clean.parquet` - Target protein information
4. `compound_structures_clean.parquet` - Chemical structure representations (SMILES)
5. `compound_properties_clean.parquet` - Physicochemical properties of compounds

Note: Unit imputation and pIC50 computation are already performed in `load_activities.py`, so this job consumes pre-transformed activity data.

## Join Strategy

**Join Type:** Inner joins across all tables (only complete records with all relationships present will survive)

**Join Sequence:**
```
activities (base table)
  ↓ join on assay_id
assays
  ↓ join on tid
target_dictionary
  ↓ join on molregno
compound_structures
  ↓ join on molregno
compound_properties
```

**Rationale:** Inner joins ensure data completeness - every record in the final dataset has matching activity, assay, target, structure, and property information. This matches the reference Polars implementation pattern.

## Column Selection

The final joined dataset includes:

**Identifiers:**
- `activity_id` - Unique activity measurement ID
- `molregno` - Molecule registry number (compound ID)

**Activity Measurements (from activities):**
- `standard_value` - Measured value
- `standard_units` - Units (e.g., nM)
- `standard_type` - Measurement type (e.g., IC50, Ki)
- `standard_relation` - Relation symbol (e.g., =, <, >)
- `pchembl_value` - Pre-computed pChEMBL value
- `pIC50` - Computed pIC50 = -log10(M), already calculated in load_activities.py
- `has_validity_comment` - Boolean flag for data validity warnings

**Assay Information (from assays):**
- `assay_type` - Type of biological assay
- `assay_organism` - Organism used in assay
- `assay_relationship` - Relationship type to target
- `confidence_score` - Assay confidence level

**Target Information (from target_dictionary):**
- `chembl_id` - Target ChEMBL identifier (renamed to target_chembl_id)
- `pref_name` - Target preferred name (renamed to target_name)
- `organism` - Target organism

**Chemical Structure (from compound_structures):**
- `canonical_smiles` - Canonical SMILES representation

**Compound Properties (from compound_properties):**
- `mw_freebase` - Molecular weight
- `alogp` - Calculated logP
- `hba` - Hydrogen bond acceptors
- `hbd` - Hydrogen bond donors
- `psa` - Polar surface area
- `rtb` - Rotatable bonds
- `aromatic_rings` - Number of aromatic rings
- `qed_weighted` - Quantitative estimate of drug-likeness

## Data Quality Filters

Filters are applied in the following order after joining:

1. **Duplicate Removal:**
   - Filter: `(potential_duplicate IS NULL) OR (potential_duplicate = 0)`
   - Removes records flagged as potential duplicates in the activities table

2. **pIC50 Validity:**
   - Filter: `(pIC50 IS NOT NULL) AND isfinite(pIC50)`
   - Removes records with NULL or infinite pIC50 values

3. **Structure Presence:**
   - Filter: `canonical_smiles IS NOT NULL`
   - Ensures chemical structure information exists

**Rationale:** These filters ensure the final dataset contains only high-quality, complete records suitable for machine learning model training.

## Output

**File:** `/opt/spark/data/cleaned/chembl_joined.parquet`  
**Format:** Parquet (columnar storage)  
**Write Mode:** Overwrite existing data

The output represents a denormalized, analysis-ready dataset combining activity measurements with compound properties, target information, and assay metadata.

## Implementation Details

**Job Structure:**
- File: `spark/jobs/join_chembl_tables.py`
- Pattern: Matches existing load job structure (logging, SparkSession, environment variables)
- Spark App Name: "Join ChEMBL Tables"

**Code Pattern:**
```python
def run_joining():
    # 1. Create Spark session
    # 2. Load 5 parquet files
    # 3. Perform inner joins in sequence
    # 4. Apply filters
    # 5. Select and rename columns
    # 6. Write output parquet
```

**Airflow Integration:**
The job will be added to `clean_chembl_tables_dag.py` as a new task:
- Task ID: `join_all_tables`
- Dependency: Runs after all 5 load tasks complete
- Followed by: Updated `verify_outputs` task to check the joined parquet file

**DAG Structure Update:**
```
check_spark >> [load_compound_properties, load_compound_structures, 
                load_target_dictionary, load_assays, load_activities]
            >> join_all_tables
            >> verify_outputs
```

## Success Criteria

1. Job successfully loads all 5 cleaned parquet files
2. Joins execute without errors
3. Filters reduce dataset to valid records only
4. Output parquet file is created at specified path
5. Logging provides clear visibility into row counts at each step
6. Job integrates cleanly into Airflow DAG workflow

## Trade-offs

**Single-stage vs Multi-stage:**
- Chosen: Single-stage join with all operations in one job
- Trade-off: Requires sufficient memory but is simpler and matches project patterns

**Inner vs Left joins:**
- Chosen: Inner joins (only complete records)
- Trade-off: May lose some activity data but ensures data quality and completeness

**In-memory vs Checkpointing:**
- Chosen: In-memory processing without checkpointing
- Trade-off: Cannot resume from intermediate state but avoids I/O overhead
