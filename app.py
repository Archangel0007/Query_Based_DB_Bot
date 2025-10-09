import streamlit as st
import os
import html
from datetime import datetime, timezone
from metadata import generate_metadata
from schema_Generator import generate_schema, schema_correction

st.set_page_config(page_title="Schema Builder Assistant", layout="wide")

# ------------------------- SIDEBAR -------------------------
with st.sidebar:
    st.markdown("### Schema Builder Assistant")
    st.caption("Upload CSVs or provide a SharePoint link to generate schema automatically.")
    if st.button("Open Schema Chat"):
        st.session_state.show_schema_chat = True

# ------------------------- SESSION STATE -------------------------
if "show_schema_chat" not in st.session_state:
    st.session_state.show_schema_chat = False

if "schema_chat_history" not in st.session_state:
    st.session_state.schema_chat_history = [
        {
            "role": "assistant",
            "text": "Hi! Upload your CSVs or paste a SharePoint link to get started.",
            "time": datetime.now(timezone.utc).isoformat()
        }
    ]

if "general_chat_history" not in st.session_state:
    st.session_state.general_chat_history = [
        {
            "role": "assistant",
            "text": "Welcome! This is your general assistant chat.",
            "time": datetime.now(timezone.utc).isoformat()
        }
    ]

if "latest_schema_png" not in st.session_state:
    st.session_state.latest_schema_png = None


# ------------------------- HELPER FUNCTIONS -------------------------
def add_msg(history_key, role, text):
    """Append a chat message to session history."""
    st.session_state[history_key].append({
        "role": role,
        "text": text,
        "time": datetime.utcnow().isoformat()
    })


def handle_user_upload(uploaded_files):
    """Save uploaded CSVs into a local Run_Space directory."""
    if not os.path.exists("Run_Space"):
        os.makedirs("Run_Space")

    for file in uploaded_files:
        file_path = os.path.join("Run_Space", file.name)
        with open(file_path, "wb") as f:
            f.write(file.getbuffer())
    return "Run_Space"


def process_source(source):
    """Run metadata and schema generation from a given source."""
    add_msg("schema_chat_history", "assistant", "Generating metadata and schema...")
    try:
        generate_metadata(source)
        add_msg("schema_chat_history", "assistant", "✅ Metadata generated successfully.")
        generate_schema()
        add_msg("schema_chat_history", "assistant", "✅ Schema generated successfully.")

        schema_png = os.path.join("Run_Space", "relationship_schema.png")
        if os.path.exists(schema_png):
            st.session_state.latest_schema_png = schema_png
            add_msg("schema_chat_history", "assistant", "Schema image is ready below.")
        else:
            add_msg("schema_chat_history", "assistant", "⚠️ Schema image not found after generation.")
    except Exception as e:
        add_msg("schema_chat_history", "assistant", f"❌ Error during generation: {e}")


# ------------------------- MAIN INTERFACE -------------------------
if st.session_state.show_schema_chat:
    st.markdown("## 🧠 Schema Chat")

    # Display chat history
    for msg in st.session_state.schema_chat_history:
        role = msg["role"]
        who = "You" if role == "user" else "Assistant"
        color = "#dbeafe" if role == "user" else "#f3f4f6"
        st.markdown(
            f"""
            <div style='background-color:{color};border-radius:10px;padding:8px;margin:4px 0;'>
                <b>{who}</b><br>{html.escape(msg["text"]).replace("\n", "<br>")}
            </div>
            """,
            unsafe_allow_html=True
        )

    # Display schema image if exists
    if st.session_state.latest_schema_png and os.path.exists(st.session_state.latest_schema_png):
        st.markdown("<div style='text-align:center;'>", unsafe_allow_html=True)
        st.image(st.session_state.latest_schema_png, caption="Generated Relationship Schema", width=350)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("### 🔧 Refine or correct the schema")

        # Persistent correction input key
        correction_input = st.text_input(
            "Describe corrections (start with 'yes' or 'no'):",
            key="correction_input_field"
        )

        if st.button("Submit Correction", key="submit_correction_btn"):
            if correction_input.strip():
                add_msg("schema_chat_history", "user", correction_input)

                try:
                    result = schema_correction(correction_input)
                    add_msg("schema_chat_history", "assistant", f"Correction processed: {result}")

                    # If correction requires regeneration (user said "no")
                    if correction_input.lower().startswith("no"):
                        schema_correction(correction_input)
                        png_path = os.path.join("Run_Space", "relationship_schema.png")
                        if os.path.exists(png_path):
                            st.session_state.latest_schema_png = png_path
                            add_msg("schema_chat_history", "assistant", "🔄 Updated schema generated successfully.")
                        else:
                            add_msg("schema_chat_history", "assistant", "⚠️ Could not find regenerated schema image.")

                    elif correction_input.lower().startswith("yes"):
                        add_msg("schema_chat_history", "assistant", "✅ Schema confirmed as correct.")
                    else:
                        add_msg("schema_chat_history", "assistant", "⚠️ Please start your response with 'yes' or 'no'.")

                except Exception as e:
                    add_msg("schema_chat_history", "assistant", f"❌ Error during schema correction: {e}")

                # Rerun to update chat immediately
                st.rerun()
            else:
                add_msg("schema_chat_history", "assistant", "⚠️ Please enter a correction message before submitting.")
                st.rerun()

    else:
        st.markdown("### 📂 Upload your data source")

        uploaded_files = st.file_uploader("Upload CSV file(s)", type=["csv"], accept_multiple_files=True)
        sharepoint_link = st.text_input("Or paste your SharePoint CSV link:")

        if st.button("Generate Schema", key="generate_schema_btn"):
            if uploaded_files:
                add_msg("schema_chat_history", "user", f"Uploaded {len(uploaded_files)} CSV file(s).")
                source = handle_user_upload(uploaded_files)
                process_source(source)
            elif sharepoint_link.strip():
                add_msg("schema_chat_history", "user", f"Provided SharePoint link: {sharepoint_link.strip()}")
                process_source(sharepoint_link.strip())
            else:
                add_msg("schema_chat_history", "assistant", "⚠️ Please upload CSVs or provide a SharePoint link first.")
            st.rerun()

    if st.button("Back to General Chat", key="back_general_chat"):
        st.session_state.show_schema_chat = False
        st.rerun()

else:
    st.markdown("## 💬 General Chat")

    for msg in st.session_state.general_chat_history:
        role = msg["role"]
        who = "You" if role == "user" else "Assistant"
        color = "#bbf7d0" if role == "user" else "#f8fafc"
        st.markdown(
            f"""
            <div style='background-color:{color};border-radius:10px;padding:8px;margin:4px 0;'>
                <b>{who}</b><br>{html.escape(msg["text"]).replace("\n", "<br>")}
            </div>
            """,
            unsafe_allow_html=True
        )

    user_input = st.text_input("Type your message here:", key="general_input_field")
    if st.button("Send", key="general_send_btn"):
        if user_input.strip():
            add_msg("general_chat_history", "user", user_input)
            add_msg("general_chat_history", "assistant", f"You said: '{user_input}'.")
            st.rerun()

    st.caption("This is your general chat. Use the sidebar to switch to Schema Chat.")

st.caption("🧩 Built with Streamlit — interactive chat-driven schema builder.")
