import streamlit as st
import os
import html
import subprocess
from datetime import datetime, timezone
from metadata import generate_metadata 
from conceptual_Designer import generate_dimensional_model
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

if "schema_confirmed" not in st.session_state:
    st.session_state.schema_confirmed = False

if "model_generated" not in st.session_state:
    st.session_state.model_generated = False


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


def process_source(source, context_text):
    """Run metadata and dimensional model generation from a given source."""
    add_msg("schema_chat_history", "assistant", "Generating metadata and dimensional model...")
    try:
        generate_metadata(source)
        add_msg("schema_chat_history", "assistant", "✅ Metadata extracted from source files.")

        # Save the context to the file expected by the dimensional modeler
        context_file_path = os.path.join("Run_Space", "refined_User_Query.txt")
        with open(context_file_path, "w", encoding="utf-8") as f:
            f.write(context_text)

        generate_dimensional_model()
        output_json_path = os.path.join("Run_Space", "dimensional_model.json")
        if os.path.exists(output_json_path):
            add_msg("schema_chat_history", "assistant", f"✅ Dimensional model generated successfully.")
            st.session_state.model_generated = True

            # Now, generate the visual schema
            add_msg("schema_chat_history", "assistant", "🎨 Generating visual schema diagram...")
            png_path = generate_schema(schema_context=context_text)
            st.session_state.latest_schema_png = png_path
            add_msg("schema_chat_history", "assistant", "✅ Visual schema generated. Please review it below.")
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

    # --- UI FLOW ---

    # STEP 3: Generate Supabase script after schema is confirmed
    if st.session_state.get("schema_confirmed"):
        st.markdown("### 🚀 Step 3: Generate Supabase Python Script")
        if st.button("Generate Script", key="generate_script_btn"):
            from query_Generator import generate_supabase_script
            with st.spinner("⏳ Generating Supabase script..."):
                try:
                    output_file = generate_supabase_script(metadata_file="Run_Space/dimensional_model.json", plantuml_file="Run_Space/relationship_schema.puml")
                    add_msg("schema_chat_history", "assistant", f"✅ Python script generated: {output_file}")
                    st.success(f"✅ Python script generated: {output_file}")
                except Exception as e:
                    add_msg("schema_chat_history", "assistant", f"❌ Error generating script: {e}")
                    st.error(f"❌ Error generating script: {e}")
            st.rerun()

    # STEP 2: Display schema and ask for confirmation/correction
    elif st.session_state.get("model_generated"):
        st.markdown("### 🔎 Step 2: Review and Confirm Schema")
        if st.session_state.latest_schema_png and os.path.exists(st.session_state.latest_schema_png):
            st.image(st.session_state.latest_schema_png, caption="Generated ER Diagram")
        
        correction_input = st.text_area("Is this schema correct? Type 'yes' to confirm, or 'no' followed by your corrections (e.g., 'no, change the relationship between Orders and Customers to one-to-many').", key="correction_input")
        if st.button("Submit Feedback", key="submit_correction_btn"):
            if correction_input.strip():
                add_msg("schema_chat_history", "user", correction_input)
                response = schema_correction(correction_input)
                add_msg("schema_chat_history", "assistant", response)
                if correction_input.strip().lower() == "yes":
                    st.session_state.schema_confirmed = True
            st.rerun()

    # STEP 1: Initial state - get data source and context
    else:
        col1, col2 = st.columns([1, 3]) # Use columns to make the uploader less wide
        with col1:
            uploaded_files = st.file_uploader("Upload CSV file(s)", type=["csv"], accept_multiple_files=True, label_visibility="collapsed")
        sharepoint_link = st.text_input("Or paste your SharePoint CSV link:")
        schema_context = st.text_area("Provide context about your schema (required):",
                                      help="Describe relationships, primary keys, or business logic. E.g., 'A customer can have many orders. An order belongs to one customer.'")

        if st.button("Generate Dimensional Model", key="generate_model_btn"):
            source_provided = uploaded_files or sharepoint_link.strip()
            context_provided = schema_context.strip()

            if source_provided and context_provided:
                if uploaded_files:
                    add_msg("schema_chat_history", "user", f"Uploaded {len(uploaded_files)} CSV file(s).")
                    source = handle_user_upload(uploaded_files)
                else:
                    source = sharepoint_link.strip()
                    add_msg("schema_chat_history", "user", f"Provided SharePoint link: {source}")
                add_msg("schema_chat_history", "user", f"Context: {context_provided}")
                process_source(source, context_text=context_provided)
            else:
                add_msg("schema_chat_history", "assistant", "⚠️ Please provide a data source (CSV or link) and the required schema context before generating.")
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
