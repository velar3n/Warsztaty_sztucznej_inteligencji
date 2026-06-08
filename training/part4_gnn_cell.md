---
# Part 4 — Extended GNN Model Library (NEW)

**A complete library of GNN models is now available** in `gnn_model_library.py`.

This includes:
- **6 classic GINE architectures** from literature (no plagiarism)
- **6 tuned configurations** based on YOUR experimental results
- **Unified training infrastructure** for systematic testing

## Quick Start

To use the library, run:
```python
# Import the complete library
from gnn_model_library import (
    ALL_CONFIGS,
    CLASSIC_CONFIGS, 
    TUNED_CONFIGS,
    run_experiments
)

# Run your best tuned configs on scaffold split
tr_graphs, val_graphs, te_graphs = load_gnn_split('scaffold')
train_loader = GeoDataLoader(tr_graphs, batch_size=32, shuffle=True)
val_loader = GeoDataLoader(val_graphs, batch_size=128, shuffle=False)
test_loader = GeoDataLoader(te_graphs, batch_size=128, shuffle=False)

# Test top 3 candidates
results = run_experiments(
    TUNED_CONFIGS[:3],  # best_from_experiments_nores, best_high_dropout, best_deeper
    train_loader,
    val_loader,
    test_loader,
    node_dim=NODE_DIM,  # 30
    edge_dim=EDGE_DIM   # 10
)

results.to_csv('extended_gnn_results.csv', index=False)
```

See `gnn_model_library.py` for full details and all model architectures!
