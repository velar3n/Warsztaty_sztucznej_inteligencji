import base64
import sys
import os
import uuid
import logging

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import streamlit as st
from graph import run_agent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Page config
st.set_page_config(
    page_title="AI Worshop",
    layout="wide",
)

# Main page styling
st.markdown("<h1 style='text-align: center;'>AI Workshop Project</h1>", unsafe_allow_html=True)


# Session state
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

if "messages" not in st.session_state:
    st.session_state.messages = []  # list of {role, content, image_b64}


@st.cache_resource(show_spinner="Loading Agent…")
def load_agent():
    return run_agent

_run_agent = load_agent()

# Sidebar
with st.sidebar:
    st.header("Session")
    st.code(f"Thread ID:\n{st.session_state.thread_id}", language="text")

    if st.button("Start new conversation", use_container_width=True):
        st.session_state.thread_id = str(uuid.uuid4())
        st.session_state.messages = []
        st._rerun()

    st.divider()
    st.header("Info")
    st.markdown("""
    **SMILES Format Examples**:
    - Ethanol: `CCO`
    - Benzine: `c1ccccc1`
    - Caffeine: `Cn1cnc2c1c(=O)n(C)c(=O)n2C`
    - Ibuprofen: `CC(C)Cc1ccc(cc1)C(C)C(=O)O`
                
    **Usage**:
    - Enter a SMILES string to predict its activity and visualize its structure.
    - You can also ask questions about the SMILES or the predictions.
                
    **Important**: 
    - The agent is able to process exactly one SMILES string per message.
    - The agent does not have internet access and relies solely on the provided tools and its internal knowledge.
    """)


# Render chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg.get("content"))
        if msg.get("image_b64"):
            _bytes = base64.b64decode(msg["image_b64"])
            st.image(_bytes, caption="2D Molecular Structure", width=360)

# Chat input 
if prompt := st.chat_input("Enter a SMILES…"):
    logger.info("Chat input received")

    # Store and display the user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Call agent
    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            logger.info(f"Invoking agent with user message: {prompt}, thread_id: {st.session_state.thread_id}")

            try:
                result = _run_agent(
                    user_message=prompt,
                    thread_id=st.session_state.thread_id,
                )
                logger.info(f"Agent response: {result}")
                
                reply_text = result.get("text")
                image_b64 = result.get("image_b64")
            except Exception as exc:
                logger.error(f"Error occurred while invoking agent: {exc}")
                reply_text = f"Agent error: {exc}"
                image_b64 = None

        st.markdown(reply_text)
        if image_b64:
            _bytes = base64.b64decode(image_b64)
            st.image(_bytes, caption="2D Molecular Structure", width=360)

    # Persist to history
    st.session_state.messages.append(
        {"role": "assistant", "content": reply_text, "image_b64": image_b64}
    )
