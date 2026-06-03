import base64
import io
import logging

from langchain_core.tools import tool

logger = logging.getLogger(__name__)


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
            - pic50 (float): Predicted pIC50 value.
            - activity_label (str): Human-readable activity class.
    """
    logger.info(f"[TOOL]: Predicting pIC50 for SMILES: {smiles}")

    # TODO: replace the hardcoded value with real model inference, e.g.:
    
    prediction = 0.75

    if prediction >= 6.0:
        label = "Active"
    elif prediction >= 5.0:
        label = "Moderately active"
    else:
        label = "Inactive"

    return {
        "smiles": smiles,
        "pic50": prediction,
        "activity_label": label,
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


# Export tools for agent registration
TOOLS = [validate_smiles, predict_pic50, visualize_structure]