import streamlit as st
import json
import re
from datetime import datetime, timezone
import os
import html
import base64
from typing import List, Optional

# local modules
from query_Generator import generate_and_send
from script_Runner import extract_code_blocks, run_python_code
from result_Summarizer import summarize_execution_results

# --- Page config ---
st.set_page_config(page_title="Mini Chat UI", layout="wide")

# --- Sidebar UI ---
with st.sidebar:
    st.markdown("<div class='sidebar-title'>Mini Chat UI</div>", unsafe_allow_html=True)
    st.caption("A small Streamlit page that mimics a chat layout")
    model_options = ["gemini-2.5-flash"]
    model = st.selectbox("Model (fixed)", model_options, index=0)
    st.session_state.selected_model = "gemini-2.5-flash"
    temp = st.slider("Temperature", 0.0, 1.0, 0.2)
    
    st.write("---")
    if st.button("Clear conversation"):
        st.session_state.history = []
    
    st.write("---")
    st.markdown("Export or download")
    if st.button("Export JSON"):
        data = json.dumps(st.session_state.get("history", []), indent=2)
        st.download_button("Download conversation (JSON)", data, file_name="conversation.json", mime="application/json")
    
    allow_code_execution = st.checkbox("Allow executing generated Python code (unsafe)", value=True)

# --- Session state setup ---
if "history" not in st.session_state:
    st.session_state.history = [
        {
            "role": "assistant",
            "text": "Hello — this is a mini Chat UI. Type something below to start.",
            "time": datetime.now(timezone.utc).isoformat()
        }
    ]

# --- Function to add a message ---
def add_message(role: str, text: str):
    st.session_state.history.append({
        "role": role,
        "text": text,
        "time": datetime.utcnow().isoformat()
    })


def _handle_user_query(user_text: str, model_name: str, temperature: float, allow_exec: bool):
    try:
        assistant_text = generate_and_send(user_text, model=model_name, temperature=temperature)
    except Exception as e:
        assistant_text = f"[Error calling generation: {e}]"
    # store the raw assistant text in history, but we'll replace the visible assistant message
    # with a human-friendly summary. Keep the raw details in session_state for the hidden expander.
    add_message("assistant", assistant_text)

    # extract code blocks
    blocks = extract_code_blocks(assistant_text)
    exec_results = []
    raw_details = {"assistant_text": assistant_text, "blocks": blocks, "exec_results": []}

    if blocks and allow_exec:
        for i, b in enumerate(blocks):
            if b.get("language", "").lower() in ("py", "python", "python3", ""):
                res = run_python_code(b["code"]) 
                exec_results.append({"block_index": i, "result": res})
                raw_details["exec_results"].append({"block_index": i, "result": res})
            else:
                raw_details["exec_results"].append({"block_index": i, "skipped": True, "language": b.get("language")})
    elif blocks and not allow_exec:
        raw_details["note"] = "Code blocks detected but execution is disabled in sidebar"

    # Ask the summarizer to produce a plain-language explanation of the exec results
    try:
        summary = summarize_execution_results(exec_results, blocks, user_text, model=model_name, temperature=temperature)
    except Exception as e:
        summary = f"[Error creating human-friendly summary: {e}]"

    # Replace the last assistant entry's visible text with the summary, but keep raw details stored
    # in session_state under a unique key so the UI can show it in a hidden expander if requested.
    # Attach a small metadata id to link summary <-> raw details (use timestamp)
    import time
    meta_id = str(int(time.time() * 1000))
    # update the last assistant message to the summary text with meta id appended invisibly
    st.session_state.history[-1]["text"] = f"{summary}\n\n[RAW_ID:{meta_id}]"

    if "raw_results" not in st.session_state:
        st.session_state.raw_results = {}
    st.session_state.raw_results[meta_id] = raw_details

    return summary, blocks, exec_results

# --- Custom CSS ---
st.markdown(
    f"""
    <style>
    .msg {{
        padding: 10px;
        margin-bottom: 1px;
        border-radius: 10px;
        max-width: 75%;
        word-wrap: break-word;
        display: flex;
        flex-direction: column;
    }}
    .user {{
        background-color: #7dd3fc;
        color: #012;
        margin-left: auto;
    }}
    .assistant {{
        background-color: #e5e7eb;
        color: #111;
        margin-right: auto;
    }}
    .meta {{
        font-size: 0.75rem;
        color: #6b7280;
        margin-bottom: 4px;
    }}
    .sidebar-title {{
        font-weight: bold;
        font-size: 1.1rem;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# --- Conversation Header ---
st.markdown("## Conversation")

# --- Chat History Rendering ---
chat_box = st.container()
with chat_box:
    code_block_re = re.compile(r"```(?:([a-zA-Z0-9_+-]+)\n)?(.*?)```", re.S)

    def _render_message_as_html(role: str, text: str, time_str: str, inline_images: Optional[List[str]] = None) -> str:
        """Return a single HTML string for the message bubble. Converts ```code``` blocks to
        <pre><code> with proper HTML escaping so the entire content stays inside the bubble.
        """
        who = "You" if role == "user" else "Assistant"

        def _code_repl(m: re.Match) -> str:
            lang = m.group(1) or "python"
            code = html.escape(m.group(2))
            return f"<pre><code class='lang-{lang}'>{code}</code></pre>"
        # Remove RAW_ID token from visible text so it doesn't display
        body_text = re.sub(r"\[RAW_ID:\d+\]", "", text).strip()
        # Replace code fences with escaped <pre><code>
        body = code_block_re.sub(_code_repl, body_text)

        # Escape any remaining text that wasn't inside a code block
        # To avoid double-escaping code, only escape text outside code tags by splitting on our inserted tags.
        parts = re.split(r"(<pre><code class='lang-[^']+'>.*?</code></pre>)", body, flags=re.S)
        out_parts = []
        for part in parts:
            if part.startswith("<pre><code"):
                out_parts.append(part)
            else:
                out_parts.append(html.escape(part).replace('\n', '<br/>'))
        body_html = ''.join(out_parts)

        # If inline images are provided, embed them as base64 <img> tags inside the bubble
        if inline_images:
            for img_path in inline_images:
                try:
                    with open(img_path, 'rb') as f:
                        b = f.read()
                    b64 = base64.b64encode(b).decode('ascii')
                    # try to infer mime type from extension
                    ext = os.path.splitext(img_path)[1].lower()
                    mime = 'image/png'
                    if ext in ('.jpg', '.jpeg'):
                        mime = 'image/jpeg'
                    elif ext == '.gif':
                        mime = 'image/gif'
                    elif ext == '.svg':
                        mime = 'image/svg+xml'
                    img_tag = f"<div style='margin-top:8px'><img src=\"data:{mime};base64,{b64}\" style=\"max-width:100%;height:auto;border-radius:6px;\"/></div>"
                    body_html += img_tag
                except Exception:
                    # ignore image embedding failures and continue
                    pass

        cls = 'user' if role == 'user' else 'assistant'
        return f"<div class='msg {cls}'><div class='meta'>{who} • {time_str}</div>{body_html}</div>"

    for msg in st.session_state.history:
        role = msg["role"]
        text = msg["text"]
        time = datetime.fromisoformat(msg["time"]).strftime("%H:%M:%S")

        # For assistant messages, gather inline images from raw exec results if available
        inline_imgs = []
        if role != "user":
            raw_id_m = re.search(r"\[RAW_ID:(\d+)\]", text)
            if raw_id_m:
                rid = raw_id_m.group(1)
                raw = st.session_state.get("raw_results", {}).get(rid)
                if raw:
                    # gather image files created during execution (from exec_results)
                    for e in raw.get('exec_results', []):
                        res = e.get('result') or {}
                        for p in res.get('files', []) or []:
                            try:
                                name = os.path.basename(p)
                            except Exception:
                                name = str(p)
                            candidate = os.path.join(os.path.dirname(__file__), "Run_Space", name)
                            if os.path.isfile(candidate) and name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
                                inline_imgs.append(candidate)

        # Render the whole message as one HTML block so other Streamlit elements don't break out of it
        html_block = _render_message_as_html(role, text, time, inline_images=inline_imgs if inline_imgs else None)
        st.markdown(html_block, unsafe_allow_html=True)
        # For assistant messages, still provide the expander with code & exec details (unchanged)
        if role != "user":
            raw_id_m = re.search(r"\[RAW_ID:(\d+)\]", text)
            if raw_id_m:
                rid = raw_id_m.group(1)
                raw = st.session_state.get("raw_results", {}).get(rid)
                if raw:
                    with st.expander("Show generated code & execution details"):
                        blocks = raw.get("blocks", [])
                        if blocks:
                            st.markdown("**Code blocks:**")
                            for i, b in enumerate(blocks):
                                lang = b.get("language", "python") or "python"
                                st.code(b.get("code", ""), language=lang)

                        execs = raw.get("exec_results", [])
                        if execs:
                            st.markdown("**Execution results:**")
                            for e in execs:
                                idx = e.get("block_index")
                                res = e.get("result") or {}
                                st.markdown(f"-- Block {idx} --")
                                # show only stdout (hide stderr and return code per user request)
                                st.code(res.get("stdout", "(no stdout)"), language="")
                                # show produced filenames and images if present in Run_Space
                                files = res.get('files') or []
                                if files:
                                    st.markdown("**Produced files (names):**")
                                    rs_path = os.path.join(os.path.dirname(__file__), "Run_Space")
                                    for p in files:
                                        try:
                                            name = os.path.basename(p)
                                        except Exception:
                                            name = str(p)
                                        st.markdown(f"- {name}")
                                        candidate = os.path.join(rs_path, name)
                                        if os.path.isfile(candidate) and name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
                                            try:
                                                st.image(candidate, caption=name)
                                            except Exception:
                                                pass

# --- User Input ---
user_input = st.chat_input("Type a message...")

# --- Handle Input ---
if user_input:
    # Save the message immediately and rerun
    add_message("user", user_input)
    st.session_state.pending_query = user_input
    st.rerun()

# After rerun, check if we have a pending query
if "pending_query" in st.session_state:
    query = st.session_state.pop("pending_query")
    selected = st.session_state.get("selected_model", model)
    assistant_text, blocks, exec_results = _handle_user_query(query, selected, temp, allow_code_execution)
    st.rerun()

# --- Footer ---
st.caption("Built with Streamlit — lightweight chat UI demo")
