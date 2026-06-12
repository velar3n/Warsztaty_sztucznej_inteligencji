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