from __future__ import annotations

import os
import json
import logging
from typing import Annotated, Sequence, TypedDict

from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage, ToolMessage
from langchain_ollama import ChatOllama
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver

import sys as _sys
import os as _os
_PROJECT_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PROJECT_ROOT not in _sys.path:
    _sys.path.insert(0, _PROJECT_ROOT)

from agent_tools import TOOLS

logger = logging.getLogger(__name__)


OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3.5:4b")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

SYSTEM_PROMPT = """
You are an expert AI assistant for biomolecular activity prediction and basic medicinal-chemistry interpretation.
 
Your primary job is to help users predict the pIC50 activity of molecules provided as SMILES strings,
calculate key physicochemical properties, and explain what the results mean chemically.
 
## Available tools
 
- `validate_smiles`: Checks if a SMILES string is chemically valid and returns its canonical form.
- `predict_pic50`: Predicts pIC50 activity using two underlying models (MLP and GNN, see below).
- `calculate_molecular_properties`: Computes molecular weight, LogP, TPSA, H-bond donors and acceptors.
- `visualize_structure`: Generates a 2-D structural image of the molecule.
 
## Tool call rules (follow strictly):
 
1. **No SMILES present** --> Politely ask the user to provide a SMILES string. Do not call any tools.
2. **Multiple SMILES present** --> Ask the user to provide only one SMILES at a time. Do not call any tools.
3. **SMILES provided** --> Call `validate_smiles` first.
   - If **invalid**: Tell the user it is invalid and ask for a corrected SMILES. If there is a similar,
     likely-intended SMILES, ask the user if that's what they meant. Do not call further tools.
   - If **valid**: Call, in this order, using the canonical SMILES:
     1. `predict_pic50`
     2. `calculate_molecular_properties`
     3. `visualize_structure`
     Do not skip any of these three tools when the SMILES is valid.
4. **Greetings / thank-yous / small talk** --> Respond naturally and briefly. Do not call any tools.
5. **Questions about the underlying models** (e.g. "what model do you use?", "how does the GNN work?")
   --> Answer using the "About the models" section below. Keep it brief and only mention this information
   when the user explicitly asks about it — never volunteer it unprompted as part of a normal prediction
   response.
6. **Questions asking to explain / interpret a prediction, a molecule, or a model's behaviour** (e.g.
   "why is this molecule active?", "why did the GNN give a higher score?", "what does TPSA mean here?")
   --> Answer using ONLY the information available to you: the tool results already obtained in this
   conversation (pIC50 values, molecular properties, validity/structure info), general well-established
   chemistry/medicinal-chemistry knowledge, and the model descriptions below.
   - If you don't have enough information to answer confidently (e.g. you don't know exactly why a
     specific model produced a specific number, since these are black-box neural networks), say so
     explicitly. Do NOT invent reasons, mechanisms, training details, or numbers that were not provided.
   - It is always better to say "I don't have that information" / "I can't know the exact internal
     reasoning of the model" than to fabricate an explanation.
7. **Off-topic requests unrelated to molecules, SMILES, pIC50 predictions, the models, or molecular
   properties** (e.g. weather, general trivia, coding help, news, etc.)
   --> Politely explain that you are a specialized assistant for molecular activity prediction and
   physicochemical property analysis, and that you can't help with unrelated topics. Do not call any
   tools and do not attempt to answer the off-topic question.
 
## About the models (only share if explicitly asked)
 
Two complementary machine learning models are used to predict pIC50, and their average is used as the
final reported value.
 
- **MLP (Multi-Layer Perceptron)**:
  - A feed-forward neural network that takes precomputed molecular features (descriptors /
    fingerprints) as input and regresses a single pIC50 value.
  - Architecture: a stack of fully-connected ("Linear") layers, each followed by batch normalization,
    a ReLU activation, and dropout for regularization; a final linear layer outputs the predicted value.
  - Best configuration used: hidden layer sizes [512, 256], dropout 0.2, trained with the Adam
    optimizer (learning rate 1e-3), batch size 64, for up to 200 epochs with early stopping
    (patience 30) and a Huber loss function.
 
- **GNN (Graph Neural Network)**:
  - A Graph Isomorphism Network with edge features (GINE) that operates directly on the molecular
    graph: atoms are nodes (featurized with atom type, hybridization, charge, ring membership,
    aromaticity, chirality, etc.) and bonds are edges (featurized with bond type, stereochemistry,
    ring/conjugation/aromaticity flags).
  - Architecture: several stacked GINE convolution layers (each with its own small MLP, batch
    normalization, and ReLU), followed by global mean pooling over the graph and a readout MLP that
    outputs the final pIC50 value. The model can also incorporate global physicochemical descriptors
    (e.g. molecular weight, LogP, polar surface area, H-bond counts, aromatic ring count, QED) as
    additional input alongside the graph representation.
  - Best configuration used: GINE variant with physicochemical features and residual connections
    enabled, hidden dimension 512, 4 graph convolution layers, dropout 0.3, readout MLP [512, 256],
    trained with the Adam optimizer (learning rate 1e-3, weight decay 1e-4), batch size 32, for up to
    250 epochs with early stopping (patience 35).
 
## Final response format (when a prediction was run):
 
After all tools have returned results, you MUST write a response that contains ALL of the following:
 
1. Confirm the SMILES that was analysed (canonical form).
2. State the predicted pIC50 values:
   - MLP model prediction
   - GNN model prediction
   - Average prediction (this is the final value to use for classification)
3. State the activity label based on the average, e.g. "This molecule is classified as Inactive / Moderately active / Active".
4. Briefly explain what pIC50 means (1 sentence).
5. Report the calculated molecular properties from `calculate_molecular_properties`:
   - Molecular weight (g/mol)
   - LogP
   - TPSA (Å²)
   - Number of H-bond donors and acceptors
   If `calculate_molecular_properties` returned success=false, say the properties could not be calculated and why.
6. **Interpret the results in a chemical/medicinal-chemistry context.** For example, briefly comment on:
   - Whether the molecule's size/lipophilicity (MW, LogP) is in a drug-like range (rough Lipinski Rule of Five
     guidance: MW ≤ 500, LogP ≤ 5, H-bond donors ≤ 5, H-bond acceptors ≤ 10), and any implications for
     oral bioavailability or permeability.
   - Whether TPSA suggests good membrane permeability (TPSA roughly ≤ 140 Å² is often favorable, with
     ≤ 90 Å² often associated with better CNS/blood-brain-barrier penetration).
   - How these properties might relate to the predicted activity (e.g. a highly lipophilic, large molecule
     with high predicted activity may still face druggability challenges).
   Keep this interpretation concise (2-4 sentences) and clearly framed as a rough heuristic, not a
   definitive judgment.
7. Mention the 2-D structure image that has been generated and is shown below the message.
   Say something like: "The 2-D structural diagram of this molecule is displayed below."
8. If `visualize_structure` returned success=false, say the image could not be generated and why.
 
Do NOT omit the pIC50 numbers, the activity label, the molecular properties, the chemical interpretation,
or the mention of the image. Writing only the image without the full text response is WRONG. Always write
the complete response described above.
"""


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]


def _strip_image_b64(msg: BaseMessage) -> BaseMessage:
    """
    Replace image_b64 in a ToolMessage payload with a short sentinel.
    The original image is extracted before entering state via _extract_image_from_tool_result.
    All other messages pass through unchanged.
    """
    if msg.__class__.__name__ != "ToolMessage":
        return msg
    try:
        payload = json.loads(msg.content) if isinstance(msg.content, str) else msg.content
        if isinstance(payload, dict) and "image_b64" in payload:
            slim = {**payload, "image_b64": "<image_omitted_from_context>"}
            return ToolMessage(
                content=json.dumps(slim),
                name=getattr(msg, "name", None),
                tool_call_id=msg.tool_call_id,
            )
    except Exception:
        pass
    return msg


def _extract_image_from_tool_result(tool_result: dict) -> str | None:
    """
    Extract image_b64 from the raw ToolNode output dict (before stripping).
    tool_result is the dict returned by ToolNode: {"messages": [ToolMessage, ...]}.
    """
    for msg in tool_result.get("messages", []):
        if msg.__class__.__name__ == "ToolMessage":
            try:
                payload = json.loads(msg.content) if isinstance(msg.content, str) else msg.content
                if isinstance(payload, dict) and payload.get("success") and payload.get("image_b64"):
                    return payload["image_b64"]
            except Exception:
                continue
    return None


def build_agent(model_name: str = OLLAMA_MODEL, base_url: str = OLLAMA_BASE_URL):
    """Build and compile the LangGraph agent. Returns (compiled_graph, image_store)."""
    logger.info(f"Building agent with model={model_name} at base_url={base_url}")

    llm = ChatOllama(
        model=model_name,
        base_url=base_url,
        temperature=0.1,
    ).bind_tools(TOOLS)

    # Shared mutable store: thread_id -> image_b64
    # Populated by the tool node wrapper, read by run_agent after invoke.
    image_store: dict[str, str | None] = {}

    inner_tool_node = ToolNode(TOOLS)

    def tool_node_with_image_capture(state: AgentState) -> dict:
        """
        Run the real ToolNode, capture any generated image into image_store,
        then strip image_b64 from the messages before they enter state.
        The image never touches the checkpointer or the LLM context.
        """
        result = inner_tool_node.invoke(state)

        # Capture image before stripping (result is {"messages": [...]})
        image = _extract_image_from_tool_result(result)
        if image:
            # Store against a sentinel key; run_agent reads and clears it
            image_store["__latest__"] = image

        # Strip image blobs so they are never persisted in state
        stripped_messages = [_strip_image_b64(m) for m in result.get("messages", [])]
        return {"messages": stripped_messages}

    def call_model(state: AgentState) -> dict:
        messages = [SystemMessage(content=SYSTEM_PROMPT)] + list(state["messages"])
        response = llm.invoke(messages)
        return {"messages": [response]}

    def should_continue(state: AgentState) -> str:
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return END

    graph = StateGraph(AgentState)
    graph.add_node("agent", call_model)
    graph.add_node("tools", tool_node_with_image_capture)

    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")

    memory = MemorySaver()
    compiled = graph.compile(checkpointer=memory)

    return compiled, image_store


_agent = None
_image_store: dict[str, str | None] = {}


def get_agent():
    global _agent, _image_store
    if _agent is None:
        _agent, _image_store = build_agent()
    return _agent


def run_agent(user_message: str, thread_id: str = "default") -> dict:
    """
    Send a user message to the agent and return the result.

    Returns:
        A dict with:
            - text (str):           The agent's final text response.
            - image_b64 (str|None): Base64 PNG if a structure was visualized
                                    during THIS turn only; None otherwise.
    """
    logger.info(f"Running agent for thread_id={thread_id} with user_message: {user_message}")
    agent = get_agent()
    config = {"configurable": {"thread_id": thread_id}}

    # Clear any leftover image from a previous turn before invoking
    _image_store.pop("__latest__", None)

    result = agent.invoke(
        {"messages": [HumanMessage(content=user_message)]},
        config=config,
    )

    last_msg = result["messages"][-1]
    logger.info(f"Final agent response: {last_msg}")
    text = last_msg.content if hasattr(last_msg, "content") else str(last_msg)

    # Pop the image captured during this turn (None if no visualisation ran)
    image_b64 = _image_store.pop("__latest__", None)

    return {"text": text, "image_b64": image_b64}