# LLM setup

LangGraph + Ollama (Qwen) agent with a Streamlit frontend.

## Project structure

```
llm/
├── app.py                  # Streamlit UI
├── requirements.txt
├── agent/
│   ├── __init__.py
│   └── graph.py            # LangGraph agent definition
└── tools/
    ├── __init__.py
    └── bio_tools.py        # validate_smiles, predict_pic50, visualize_structure
```

## Setup

```bash
# 1. Install Ollama based on docs
# a) Winodws
irm https://ollama.com/install.ps1 | iex

# b) macOS / Linux
curl -fsSL https://ollama.com/install.sh | sh

# 2. Install dependencies
pip install -r requirements.txt

# 3. Pull the Qwen model (or other selected)
ollama pull qwen3.5:4b

# 4. Make sure Ollama is running (should run automatically after the pull though)
ollama list # to check models
ollama serve # for manual start, default on http://localhost:11434

# 4. Run the app
streamlit run app.py
```

## Environment variables (optional overrides)

| Variable          | Default                     | Description               |
|-------------------|-----------------------------|---------------------------|
| `OLLAMA_MODEL`    | `qwen3.5:4b`                | Ollama model tag to use   |
| `OLLAMA_BASE_URL` | `http://localhost:11434`    | Ollama server URL         |

## Memory

By default `MemorySaver` keeps conversation history **in-process** (lost on restart).

To persist across restarts switch to `SqliteSaver` in `graph.py`:

```python
# agent/graph.py  – replace MemorySaver with:
from langgraph.checkpoint.sqlite import SqliteSaver
memory = SqliteSaver.from_conn_string("agent_memory.db")
```

## Replacing the hardcoded pIC50 predictor

Open `tools.py` and find the `predict_pic50` function.
Replace the `prediction = 0.75` line with your real model inference:

```python
from your_model_module import load_model, featurize
_model = load_model("weights/your_model.pkl")

prediction = float(_model.predict([featurize(smiles)])[0])
```

## Notes on structure visualisation

RDKit can render via two backends:
- **MolDraw2DCairo** (built into RDKit) – always available, outputs PNG directly.
- **cairosvg** – optional; install with `pip install cairosvg` for SVG→PNG conversion.

The tool auto-detects which backend is available.