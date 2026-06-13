import base64
import io
import logging

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

# Global references to predictors (set by app.py on startup)
_mlp_predictor = None
_gnn_predictor = None

def set_predictors(mlp, gnn):
    """Set the global predictor instances (called from app.py)."""
    global _mlp_predictor, _gnn_predictor
    _mlp_predictor = mlp
    _gnn_predictor = gnn
    logger.info("Predictors configured for agent tools")


@tool
def validate_smiles(smiles: str) -> dict:
    """
    Validate whether a given SMILES string is chemically valid.
    ALWAYS call this tool before any other tool that requires a SMILES input to ensure the string is valid and canonicalized.

    Args:
        smiles: The SMILES string to validate.

    Returns:
        A dict with keys:
            - valid (bool): True if the SMILES is valid.
            - smiles (str): The canonical SMILES (if valid), else the original.
            - error (str | None): Error message if invalid.
    """
    logger.info(f"[TOOL]: Validating SMILES: {smiles}")
    try:
        from rdkit import Chem  # lazy import – rdkit is heavy

        mol = Chem.MolFromSmiles(smiles.strip())
        if mol is None:
            return {"valid": False, "smiles": smiles, "error": "RDKit could not parse the SMILES."}

        canonical = Chem.MolToSmiles(mol)
        return {"valid": True, "smiles": canonical, "error": None}

    except Exception as exc:
        return {"valid": False, "smiles": smiles, "error": str(exc)}


@tool
def predict_pic50(smiles: str) -> dict:
    """
    Predict the pIC50 activity value for a validated SMILES string.
    Call this tool ONLY after validate_smiles confirms the SMILES is valid.
    Always call this tool if the SMILE is valid. Do NOT skip this step, as the pIC50 value is essential for the agent's response.

    Args:
        smiles: A valid (canonical) SMILES string.

    Returns:
        A dict with keys:
            - smiles (str): The input SMILES.
            - mlp_pic50 (float): MLP model prediction.
            - gnn_pic50 (float): GNN model prediction.
            - average_pic50 (float): Average of both predictions.
            - activity_label (str): Human-readable activity class based on average.
    """
    logger.info(f"[TOOL]: Predicting pIC50 for SMILES: {smiles}")

    if _mlp_predictor is None or _gnn_predictor is None:
        logger.error("Predictors not initialized")
        return {
            "smiles": smiles,
            "mlp_pic50": None,
            "gnn_pic50": None,
            "average_pic50": None,
            "activity_label": "Error: Models not loaded",
            "error": "Predictors not initialized",
        }

    try:
        # Run predictions with both models
        mlp_pred = _mlp_predictor.predict(smiles)
        gnn_pred = _gnn_predictor.predict(smiles)

        if mlp_pred is None or gnn_pred is None:
            return {
                "smiles": smiles,
                "mlp_pic50": mlp_pred,
                "gnn_pic50": gnn_pred,
                "average_pic50": None,
                "activity_label": "Error: Prediction failed",
                "error": "One or both models failed to predict",
            }

        # Average the predictions
        avg_pred = (mlp_pred + gnn_pred) / 2.0

        # Classify activity
        if avg_pred >= 6.0:
            label = "Active"
        elif avg_pred >= 5.0:
            label = "Moderately active"
        else:
            label = "Inactive"

        logger.info(f"MLP: {mlp_pred:.3f}, GNN: {gnn_pred:.3f}, Average: {avg_pred:.3f}")

        return {
            "smiles": smiles,
            "mlp_pic50": round(mlp_pred, 3),
            "gnn_pic50": round(gnn_pred, 3),
            "average_pic50": round(avg_pred, 3),
            "activity_label": label,
        }

    except Exception as exc:
        logger.error(f"Prediction error: {exc}")
        return {
            "smiles": smiles,
            "mlp_pic50": None,
            "gnn_pic50": None,
            "average_pic50": None,
            "activity_label": "Error",
            "error": str(exc),
        }


@tool
def visualize_structure(smiles: str) -> dict:
    """
    Generate a 2-D structural image for a SMILES string using RDKit.
    Returns the image as a base64-encoded PNG so Streamlit can display it.

    Args:
        smiles: A valid (canonical) SMILES string.

    Returns:
        A dict with keys:
            - success (bool): Whether image generation succeeded.
            - image_b64 (str | None): Base64-encoded PNG string if successful.
            - error (str | None): Error message if generation failed.
    """
    logger.info(f"[TOOL]: Visualizing structure for SMILES: {smiles}")
    
    try:
        from rdkit import Chem
        from rdkit.Chem.Draw import rdMolDraw2D

        mol = Chem.MolFromSmiles(smiles.strip())
        if mol is None:
            return {"success": False, "image_b64": None, "error": "Invalid SMILES – cannot draw."}

        drawer = rdMolDraw2D.MolDraw2DSVG(400, 300)
        drawer.drawOptions().addStereoAnnotation = True
        drawer.DrawMolecule(mol)
        drawer.FinishDrawing()
        svg_text = drawer.GetDrawingText()

        # Convert SVG → PNG via Pillow so Streamlit can render it easily
        try:
            import cairosvg  # optional, fast SVG→PNG
            png_bytes = cairosvg.svg2png(bytestring=svg_text.encode())
        except ImportError:
            # Fallback: use rdkit's PNG drawer instead
            drawer2 = rdMolDraw2D.MolDraw2DCairo(400, 300)
            drawer2.DrawMolecule(mol)
            drawer2.FinishDrawing()
            png_bytes = drawer2.GetDrawingText()

        image_b64 = base64.b64encode(png_bytes).decode("utf-8")
        return {"success": True, "image_b64": image_b64, "error": None}

    except Exception as exc:
        return {"success": False, "image_b64": None, "error": str(exc)}
    
@tool
def calculate_molecular_properties(smiles: str) -> dict:
    """
    Calculate basic physicochemical descriptors for a SMILES string using RDKit:
    - molecular weight, 
    - LogP (lipophilicity)
    - topological polar surface area (TPSA)
    - number of hydrogen bond donors
    - number of hydrogen bond acceptors
    Call this tool ONLY after validate_smiles confirms the SMILES is valid.
 
    Args:
        smiles: A valid (canonical) SMILES string.
 
    Returns:
        A dict with keys:
            - success (bool): Whether the calculation succeeded.
            - smiles (str): The input SMILES.
            - molecular_weight (float | None): Molecular weight in g/mol.
            - logp (float | None): Calculated LogP (Crippen).
            - tpsa (float | None): Topological polar surface area (Å²).
            - num_h_donors (int | None): Number of hydrogen bond donors.
            - num_h_acceptors (int | None): Number of hydrogen bond acceptors.
            - error (str | None): Error message if calculation failed.
    """
    logger.info(f"[TOOL]: Calculating molecular properties for SMILES: {smiles}")
 
    try:
        from rdkit import Chem
        from rdkit.Chem import Descriptors, Crippen, Lipinski
 
        mol = Chem.MolFromSmiles(smiles.strip())
        if mol is None:
            return {
                "success": False,
                "smiles": smiles,
                "molecular_weight": None,
                "logp": None,
                "tpsa": None,
                "num_h_donors": None,
                "num_h_acceptors": None,
                "error": "Invalid SMILES – cannot calculate properties.",
            }
 
        mol_weight = Descriptors.MolWt(mol)
        logp = Crippen.MolLogP(mol)
        tpsa = Descriptors.TPSA(mol)
        h_donors = Lipinski.NumHDonors(mol)
        h_acceptors = Lipinski.NumHAcceptors(mol)
 
        logger.info(
            f"MW: {mol_weight:.3f}, LogP: {logp:.3f}, TPSA: {tpsa:.3f}, "
            f"HBD: {h_donors}, HBA: {h_acceptors}"
        )
 
        return {
            "success": True,
            "smiles": smiles,
            "molecular_weight": round(mol_weight, 3),
            "logp": round(logp, 3),
            "tpsa": round(tpsa, 3),
            "num_h_donors": h_donors,
            "num_h_acceptors": h_acceptors,
            "error": None,
        }
 
    except Exception as exc:
        logger.error(f"Property calculation error: {exc}")
        return {
            "success": False,
            "smiles": smiles,
            "molecular_weight": None,
            "logp": None,
            "tpsa": None,
            "num_h_donors": None,
            "num_h_acceptors": None,
            "error": str(exc),
        }
 
 
# Export tools for agent registration
TOOLS = [validate_smiles, predict_pic50, visualize_structure, calculate_molecular_properties]