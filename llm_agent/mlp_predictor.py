"""
MLP Predictor for pIC50 prediction from SMILES.

This class:
1. Loads a trained MLP model from checkpoint
2. Loads the StandardScaler for physicochemical features
3. Provides a predict() method that takes SMILES and returns pIC50
"""

import logging
from pathlib import Path
import json
import pickle

import numpy as np
import torch
from torch import nn

from rdkit import Chem, DataStructs
from rdkit.Chem import Descriptors, rdMolDescriptors, rdFingerprintGenerator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLP(nn.Module):
    """Multi-layer perceptron for regression."""

    def __init__(
        self,
        input_dim: int,
        hidden_dims: list[int],
        dropout: float = 0.3,
        use_batchnorm: bool = True,
        activation: str = 'relu',
    ):
        super().__init__()
        act_fn = {'relu': nn.ReLU, 'gelu': nn.GELU, 'silu': nn.SiLU}[activation]

        layers = []
        in_dim = input_dim
        for h_dim in hidden_dims:
            layers.append(nn.Linear(in_dim, h_dim))
            if use_batchnorm:
                layers.append(nn.BatchNorm1d(h_dim))
            layers.append(act_fn())
            layers.append(nn.Dropout(dropout))
            in_dim = h_dim
        layers.append(nn.Linear(in_dim, 1))

        self.net = nn.Sequential(*layers)

    def forward(self, x):
        return self.net(x)


class MLPPredictor:
    """Predictor class for loading MLP model and making pIC50 predictions."""

    def __init__(self, model_dir: str, device: str = None):
        """
        Initialize MLP predictor.

        Args:
            model_dir: Directory containing model artifacts
                       (mlp_final.pt, mlp_scaler.pkl, physchem_cols.json)
            device: Device to run model on ('cuda' or 'cpu'). If None, auto-detect.
        """
        self.model_dir = Path(model_dir)

        if device is None:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        else:
            self.device = torch.device(device)

        logger.info(f"Initializing MLPPredictor on device: {self.device}")

        # Load model
        self.model = self._load_model()

        # Load scaler
        self.scaler = self._load_scaler()

        # Load physicochemical column order
        self.physchem_cols = self._load_physchem_cols()

        logger.info("MLPPredictor initialized successfully")

    def _load_model(self):
        """Load trained MLP model from checkpoint."""
        ckpt_path = self.model_dir / 'mlp_final.pt'

        if not ckpt_path.exists():
            raise FileNotFoundError(f"Model checkpoint not found: {ckpt_path}")

        logger.info(f"Loading model from: {ckpt_path}")
        ckpt = torch.load(ckpt_path, map_location=self.device)

        # Build model with saved architecture
        arch = ckpt['arch']
        model = MLP(
            input_dim=arch['input_dim'],
            hidden_dims=arch['hidden_dims'],
            dropout=arch['dropout'],
        )

        # Load weights
        model.load_state_dict(ckpt['model_state_dict'])
        model.to(self.device)
        model.eval()

        logger.info(f"Model loaded successfully")
        logger.info(f"  Architecture: {arch}")
        logger.info(f"  Test metrics: {ckpt.get('test_metrics', 'N/A')}")

        return model

    def _load_scaler(self):
        """Load StandardScaler for physicochemical features."""
        scaler_path = self.model_dir / 'mlp_scaler.pkl'

        if not scaler_path.exists():
            raise FileNotFoundError(f"Scaler not found: {scaler_path}")

        logger.info(f"Loading scaler from: {scaler_path}")
        with open(scaler_path, 'rb') as f:
            scaler = pickle.load(f)

        return scaler

    def _load_physchem_cols(self):
        """Load physicochemical column order."""
        cols_path = self.model_dir / 'physchem_cols.json'

        if not cols_path.exists():
            raise FileNotFoundError(f"Physchem columns not found: {cols_path}")

        logger.info(f"Loading physchem columns from: {cols_path}")
        with open(cols_path, 'r') as f:
            cols = json.load(f)

        return cols

    def smiles_to_fingerprint(self, smi: str) -> np.ndarray | None:
        """
        Convert SMILES to Morgan fingerprint (ECFP4) bit vector.

        Args:
            smi: SMILES string

        Returns:
            2048-dimensional bit vector, or None if invalid SMILES
        """
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            return None

        mfpgen = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=2048)
        fp = mfpgen.GetFingerprint(mol)

        arr = np.zeros((2048,), dtype=np.float32)
        DataStructs.ConvertToNumpyArray(fp, arr)
        return arr

    def smiles_to_physchem(self, smi: str) -> np.ndarray | None:
        """
        Compute physicochemical descriptors from SMILES.

        Args:
            smi: SMILES string

        Returns:
            8-dimensional descriptor vector, or None if invalid SMILES
        """
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            return None

        # Compute descriptors in the same order as training
        descriptors = {
            'mw_freebase': Descriptors.MolWt(mol),
            'alogp': Descriptors.MolLogP(mol),
            'hba': rdMolDescriptors.CalcNumHBA(mol),
            'hbd': rdMolDescriptors.CalcNumHBD(mol),
            'psa': rdMolDescriptors.CalcTPSA(mol),
            'rtb': rdMolDescriptors.CalcNumRotatableBonds(mol),
            'aromatic_rings': rdMolDescriptors.CalcNumAromaticRings(mol),
            'qed_weighted': Descriptors.qed(mol),
        }

        # Extract values in correct order
        values = np.array([descriptors[col] for col in self.physchem_cols], dtype=np.float32)
        return values

    def predict(self, smiles: str) -> float | None:
        """
        Predict pIC50 for a given SMILES string.

        Args:
            smiles: SMILES string of the molecule

        Returns:
            Predicted pIC50 value, or None if invalid SMILES
        """
        # Generate fingerprint
        fp = self.smiles_to_fingerprint(smiles)
        if fp is None:
            logger.warning(f"Invalid SMILES: {smiles}")
            return None

        # Generate physicochemical features
        physchem = self.smiles_to_physchem(smiles)
        if physchem is None:
            logger.warning(f"Failed to compute descriptors for: {smiles}")
            return None

        # Normalize physicochemical features
        physchem_norm = self.scaler.transform(physchem.reshape(1, -1))

        # Concatenate features: [2048 fp bits | 8 physchem]
        features = np.concatenate([fp, physchem_norm.flatten()]).astype(np.float32)

        # Convert to tensor
        x = torch.tensor(features, dtype=torch.float32).unsqueeze(0).to(self.device)

        # Predict
        with torch.no_grad():
            pred = self.model(x).cpu().item()

        return pred

    def predict_batch(self, smiles_list: list[str]) -> list[float | None]:
        """
        Predict pIC50 for a batch of SMILES strings.

        Args:
            smiles_list: List of SMILES strings

        Returns:
            List of predicted pIC50 values (None for invalid SMILES)
        """
        predictions = []

        for smi in smiles_list:
            pred = self.predict(smi)
            predictions.append(pred)

        return predictions


if __name__ == "__main__":
    # Example usage
    MODELS_DIR = Path(__file__).parent.parent / 'models'

    # Initialize predictor
    predictor = MLPPredictor(model_dir=str(MODELS_DIR))

    # Test predictions
    test_smiles = [
        "CCOc1nc(NC(C)=O)cc(N)c1C#N",  # Example from training data
        "CC(C)Cc1ccc(cc1)C(C)C(O)=O",  # Ibuprofen
        "INVALID_SMILES",  # Should return None
    ]

    logger.info("\n" + "=" * 60)
    logger.info("Testing MLP predictions:")
    logger.info("=" * 60)

    for smi in test_smiles:
        pred = predictor.predict(smi)
        if pred is not None:
            logger.info(f"SMILES: {smi}")
            logger.info(f"Predicted pIC50: {pred:.3f}")
        else:
            logger.info(f"SMILES: {smi}")
            logger.info(f"Predicted pIC50: INVALID")
        logger.info("-" * 60)
