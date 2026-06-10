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

SYSTEM_PROMPT = """You are BioAgent, an expert AI assistant for biomolecular activity prediction.

Your primary job is to help users predict the pIC50 activity of molecules provided as SMILES strings.

## Tool call rules (follow strictly):

1. **No SMILES present** → Politely ask the user to provide a SMILES string. Do not call any tools.
2. **Multiple SMILES present** → Ask the user to provide only one SMILES at a time. Do not call any tools.
3. **SMILES provided** → Call `validate_smiles` first.
   - If **invalid**: Tell the user it is invalid and ask for a corrected SMILES. If there is a similar SMILES to the one the user proveided, 
        ask the user if he meant that SMILES instead. Do not call further tools.
   - If **valid**: Call `predict_pic50`, then call `visualize_structure` (both with the canonical SMILES).
4. **Greetings / thank-yous / small talk** → Respond naturally and briefly. Do not call any tools.

## Final response format (when prediction was run):

After all tools have returned results, you MUST write a response that contains ALL of the following:

1. Confirm the SMILES that was analysed.
2. State the predicted pIC50 values from both models:
   - MLP model prediction
   - GNN model prediction
   - Average prediction (this is the final value to use for classification)
3. State the activity label based on the average, e.g. "This molecule is classified as Inactive / Moderately active / Active".
4. Briefly explain what pIC50 means (1 sentence).
5. Mention the 2-D structure image that has been generated and is shown below the message.
   Say something like: "The 2-D structural diagram of this molecule is displayed below."
6. If visualize_structure returned success=false, say the image could not be generated and why.

Do NOT omit the pIC50 numbers. Do NOT omit the activity label. Do NOT omit the mention of the image.
Writing only the image without the prediction text is WRONG. Always write the full text response.
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