import streamlit as st
import os
import html
import subprocess
from datetime import datetime, timezone
from metadata import generate_metadata 
from conceptual_Designer import generate_dimensional_model
from schema_Generator import generate_schema, schema_correction
from schema_Testing import run_phase1, run_phase2
from schema_Correction import correction
from sql_Create_Generator import generate_create_script

from script_Runner import run_python_code
import time  
from PIL import Image
from query_Cleaner import clean_text
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

if "processing_step" not in st.session_state:
    st.session_state.processing_step = "idle"


# ------------------------- HELPER FUNCTIONS -------------------------
def add_msg(history_key, role, text):
    """Append a chat message to session history."""
    st.session_state[history_key].append({
        "role": role,
        "text": text,
        "time": datetime.utcnow().isoformat()
    })

def clear_csv_from_run_space():
    """Deletes all .csv files from the Run_Space directory."""
    run_space_dir = "Run_Space"
    if not os.path.isdir(run_space_dir):
        return

    for file_name in os.listdir(run_space_dir):
        if file_name.lower().endswith('.csv'):
            file_path = os.path.join(run_space_dir, file_name)
            try:
                os.remove(file_path)
            except Exception as e:
                add_msg("schema_chat_history", "assistant", f"⚠️ Could not delete CSV file: {file_name}. Error: {e}")

def handle_user_upload(uploaded_files):
    """Save uploaded CSVs into a local Run_Space directory."""
    if not os.path.exists("Run_Space"):
        os.makedirs("Run_Space")

    for file in uploaded_files:
        file_path = os.path.join("Run_Space", file.name)
        with open(file_path, "wb") as f:
            f.write(file.getbuffer())
    return "Run_Space"

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
            from sql_Create_Generator import generate_supabase_script
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
        #schema_context = clean_text(schema_context)
        if st.button("Generate Dimensional Model", key="generate_model_btn"):
            source_provided = uploaded_files or sharepoint_link.strip()
            context_provided = schema_context.strip()
    
            if source_provided and context_provided:
                if uploaded_files:
                    add_msg("schema_chat_history", "user", f"Uploaded {len(uploaded_files)} CSV file(s).")
                    source = handle_user_upload(uploaded_files)
                    st.session_state.source = source
                else:
                    source = sharepoint_link.strip()
                    st.session_state.source = source
                    add_msg("schema_chat_history", "user", f"Provided SharePoint link: {source}")
                
                add_msg("schema_chat_history", "user", f"Context: {context_provided}")
                st.session_state.schema_context = context_provided
                st.session_state.processing_step = "start"
                st.rerun()
            else:
                add_msg("schema_chat_history", "assistant", "⚠️ Please provide a data source (CSV or link) and the required schema context before generating.")
                st.rerun()

    # --- PROCESSING PIPELINE ---
    # This runs after the "Generate" button is clicked and steps through the process
    if st.session_state.processing_step != "idle":
        try:
            if st.session_state.processing_step == "start":
                with st.spinner("Extracting metadata from source files..."):
                    generate_metadata(st.session_state.source)
                add_msg("schema_chat_history", "assistant", "✅ Metadata extracted from source files.")
                st.session_state.processing_step = "generate_model"
                st.rerun()

            elif st.session_state.processing_step == "generate_model":
                with st.spinner("Generating dimensional model..."):
                    context_file_path = os.path.join("Run_Space", "refined_User_Query.txt")
                    with open(context_file_path, "w", encoding="utf-8") as f:
                        f.write(st.session_state.schema_context)
                    generate_dimensional_model()
                add_msg("schema_chat_history", "assistant", "✅ Dimensional model generated successfully.")
                time.sleep(10)
                st.session_state.processing_step = "generate_schema"
                st.rerun()

            elif st.session_state.processing_step == "generate_schema":
                with st.spinner("🎨 Generating visual schema diagram..."):
                    png_path = generate_schema(schema_context=st.session_state.schema_context)
                    st.session_state.latest_schema_png = png_path
                add_msg("schema_chat_history", "assistant", "✅ Visual schema generated.")
                time.sleep(10)
                st.session_state.processing_step = "run_phase1"
                st.rerun()

            elif st.session_state.processing_step == "run_phase1":
                with st.spinner("🤖 Running Phase 1: Generating test cases..."):
                    user_query_path = os.path.join("Run_Space", "refined_User_Query.txt")
                    run_phase1(user_query_path)
                add_msg("schema_chat_history", "assistant", "✅ Phase 1 testing complete: Test cases generated.")
                time.sleep(10)
                st.session_state.processing_step = "run_phase2"
                st.rerun()

            elif st.session_state.processing_step == "run_phase2":
                with st.spinner("🤖 Running Phase 2: Validating schema against test cases..."):
                    plantuml_code_path = os.path.join("Run_Space", "relationship_schema.puml")
                    run_phase2(plantuml_code_path)
                add_msg("schema_chat_history", "assistant", "✅ Phase 2 testing complete: Schema validated against test cases.")
                time.sleep(10)
                st.session_state.processing_step = "correction"
                st.rerun()

            elif st.session_state.processing_step == "correction":
                with st.spinner("🧜‍♀️ Running correction: Making schema changes using error test cases..."):
                    plantuml_code_path = os.path.join("Run_Space", "relationship_schema.puml")
                    error_json_path = os.path.join("Run_Space", "errors.json")
                    user_query_path = os.path.join("Run_Space", "refined_User_Query.txt")
                    correction(error_json_path, plantuml_code_path, user_query_path)
                add_msg("schema_chat_history", "assistant", "✅ Corrections applied to schema based on error test cases.")
                time.sleep(10)
                st.session_state.processing_step = "UserReview"
                st.rerun()

            elif st.session_state.processing_step == "UserReview":
                image_path = os.path.join("Run_Space", "relationship_schema.png")  # Replace with your image file name or dynamic logic
                if os.path.exists(image_path):
                    # Open the image
                    image = Image.open(image_path)
                    # Display the image in Streamlit
                    st.image(image, caption="Schema Preview", use_column_width=True)
                else:
                    st.warning("⚠️ Image not found in the directory!")
                corrections_required = st.radio("Do you require corrections to the schema?", options=["Yes", "No"])
                if corrections_required == "Yes":
                    user_input = st.text_area("Please provide your feedback or corrections here:", height=150)
                    with open("Run_Space/user_feedback.txt", "w", encoding="utf-8") as f:
                        f.write(user_input)
                    with st.spinner("🧑‍🎓 Taking corrections : Making schema changes using error test cases..."):
                        plantuml_code_path = os.path.join("Run_Space", "relationship_schema.puml")
                        error_json_path = os.path.join("Run_Space", "errors.json")
                        user_query_path = os.path.join("Run_Space", "user_feedback.txt")
                        correction(error_json_path, plantuml_code_path, user_query_path)
                    if user_input:
                        st.session_state.user_feedback = user_input  
                        add_msg("schema_chat_history", "user", f"User feedback: {user_input}")  
                    add_msg("schema_chat_history", "assistant", "✅ Corrections applied to schema based on User instructions.")
                    time.sleep(10)
                    st.session_state.processing_step = "run_phase1"
                else:
                    add_msg("schema_chat_history", "assistant", "No corrections needed. Proceeding as is.")
                    st.session_state.processing_step = "Code_Generation"
                st.rerun()
        
            elif st.session_state.processing_step == "Code_Generation":
                with st.spinner("\n⚙️ Generating Scripts: generating python scripts for Table creation..."):
                    metadata_path = os.path.join("Run_Space", "metadata.json")
                    plantuml_code_path = os.path.join("Run_Space", "relationship_schema.puml")
                    generate_create_script(metadata_file=metadata_path, plantuml_file=plantuml_code_path)
                add_msg("schema_chat_history", "assistant", "✅ Code Generated.")
                st.session_state.processing_step = "Table_Creation"
                st.rerun()

            elif st.session_state.processing_step == "Table_Creation":
                with st.spinner("\n⚙️ Creating Tables: Running python scripts for Table creation..."):
                    metadata_path = os.path.join("Run_Space", "metadata.json")
                    plantuml_code_path = os.path.join("Run_Space", "relationship_schema.puml")
                    code_path = os.path.join("Run_Space", "create_Database_Script.py")
                    run_python_code(code=open(code_path, "r").read())
                add_msg("schema_chat_history", "assistant", "✅ Tables created in the database.")
                st.session_state.processing_step = "cleanup"
                st.rerun()
            #=> continue adding states here as needed
            
            elif st.session_state.processing_step == "cleanup":
                clear_csv_from_run_space()
                add_msg("schema_chat_history", "assistant", "🧹 Source CSV files have been cleared from the workspace.")
                add_msg("schema_chat_history", "assistant", "Please review the schema below.")
                st.session_state.model_generated = True
                st.session_state.processing_step = "idle" # End of pipeline
        except Exception as e:
            add_msg("schema_chat_history", "assistant", f"❌ Error during generation: {e}")
            st.session_state.processing_step = "idle"
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
