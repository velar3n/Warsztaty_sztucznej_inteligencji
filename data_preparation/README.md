# ChEMBL Data Preparation

This directory contains scripts to prepare ChEMBL molecular data for machine learning models.

## Scripts

### `mlp_data_preparation.py`
Transforms SMILES strings to Morgan fingerprints (2048-bit binary vectors) for MLP models.

**Usage:**
```bash
python data_preparation/mlp_data_preparation.py
```

**Input:** Latest `data/chembl_joined_*.parquet` file

**Output:** `data/mlp_features_YYYYMMDD_HHMMSS.parquet` with columns:
- `activity_id`: Link to original data
- `fingerprint`: 2048-dimensional float32 array
- `pic50`: Target variable

**Runtime:** ~30-60 minutes for 13M molecules

### `gnn_data_preparation.py`
Transforms SMILES strings to molecular graphs (PyTorch Geometric Data objects) for GNN models.

**Usage:**
```bash
python data_preparation/gnn_data_preparation.py
```

**Input:** Latest `data/chembl_joined_*.parquet` file

**Output:** `data/gnn_graphs_YYYYMMDD_HHMMSS.pt` containing list of graph objects

**Runtime:** ~2-4 hours for 13M molecules

## Requirements

```
rdkit
torch
torch-geometric
pandas
pyarrow
numpy
```

## Testing

```bash
pytest tests/test_mlp_data_preparation.py -v
pytest tests/test_gnn_data_preparation.py -v
```

## Memory Usage

Both scripts process data in chunks of 100K rows to keep memory usage under 10GB.

## Error Handling

Invalid SMILES strings are skipped silently. Final statistics show success/failure counts.