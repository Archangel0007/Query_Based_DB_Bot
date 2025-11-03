import os
import sys
import time
import uuid
import threading
from datetime import datetime, timezone
from flask import Flask, render_template, request, jsonify, send_from_directory
import shutil
import json
import json
import logging
import builtins

# -------------------- INITIAL SETUP --------------------
print("[INIT] Starting Flask pipeline service...")

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
print(f"[INIT] Project root: {project_root}")

# -------------------- MODULE IMPORTS --------------------
try:
    from modules.conversions import process_uploaded_files, convert_html_to_csv
    from modules.metadata import generate_metadata, get_csv_files_from_directory
    from modules.conceptual_Designer import generate_dimensional_model
    from modules.schema_Generator import generate_schema
    from modules.schema_Testing import run_phase1, run_phase2
    from modules.schema_Correction import correction
    from modules.sql_Create_Generator import generate_create_script
    from modules.sql_Insert_Generator import generate_insert_script
    from modules.script_Runner import run_python_code
    from modules.data_Fetch import fetch_from_dynamodb, fetch_from_s3, fetch_from_cosmosdb
    print("[INIT] All module imports successful.")
except Exception as e:
    print(f"[ERROR] Failed to import modules: {e}")
    raise

# -------------------- FLASK CONFIG --------------------
app = Flask(__name__)
#app.config['UPLOAD_FOLDER'] = 'Run_Space'
app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), "Run_Space")
app.config['TEMPLATES_AUTO_RELOAD'] = True

print(f"[CONFIG] Upload folder set to: {app.config['UPLOAD_FOLDER']}")

tasks = {}
# map task_id -> threading.Event used to pause/resume background pipeline at approval points
approval_events = {}
# Thread-local to keep track of the currently active task for print capture
current_task = threading.local()

# Save original print and override it to capture terminal output into task system logs
_original_print = builtins.print
def _attach_system_log(task_id, message):
    try:
        if task_id in tasks:
            tasks[task_id].setdefault('system_logs', []).append({
                'time': datetime.now(timezone.utc).isoformat(),
                'text': message
            })
    except Exception:
        # avoid raising from logging helpers
        pass

def _custom_print(*args, **kwargs):
    # Write to original stdout
    _original_print(*args, **kwargs)
    try:
        msg = ' '.join(str(a) for a in args)
        task_id = getattr(current_task, 'task_id', None)
        if task_id and task_id in tasks:
            _attach_system_log(task_id, msg)
    except Exception:
        pass

# Monkeypatch builtins.print to capture prints. Keep idempotent check.
if builtins.print is not _custom_print:
    builtins.print = _custom_print

# Logging handler to capture logging module output and attach it to the current task
class TaskLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    def emit(self, record):
        try:
            msg = self.format(record)
            task_id = getattr(current_task, 'task_id', None)
            if task_id and task_id in tasks:
                # Use record.created (epoch) converted to ISO to preserve original time
                ts = datetime.fromtimestamp(record.created, timezone.utc).isoformat()
                tasks[task_id].setdefault('system_logs', []).append({
                    'time': ts,
                    'text': msg
                })
        except Exception:
            # Never let logging capture raise
            pass

# Attach our handler to the root logger so library logs (and Flask/werkzeug) are captured.
_task_log_handler = TaskLogHandler()
logging.getLogger().addHandler(_task_log_handler)


def generate_and_register_schema(task_id, schema_context):
    """Generate a schema PUML + PNG and register the PNG in the task record.

    This creates a timestamped PNG (so previous images are preserved) and
    updates tasks[task_id]["schema_image_url"] and tasks[task_id]["images"].
    """
    task = tasks.get(task_id)
    if task is None:
        raise RuntimeError(f"Unknown task: {task_id}")

    base = app.config['UPLOAD_FOLDER']
    task_dir = os.path.join(base, task_id)

    def get_path(filename):
        return os.path.join(task_dir, filename)

    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    png_name = f"relationship_schema.png"
    puml_name = "relationship_schema.puml"  # keep canonical PUML filename

    # Ensure output dir exists
    os.makedirs(task_dir, exist_ok=True)

    print(f"[SCHEMA] Generating schema image: {png_name} (task {task_id[:8]})")
    generate_schema(
        dimensional_model_path=get_path("dimensional_model.json"),
        output_puml_path=get_path(puml_name),
        output_png_path=get_path(png_name),
        schema_context=schema_context,
    )

    # Also keep a canonical filename 'relationship_schema.png' for UI preview
    canonical_png = get_path("relationship_schema.png")
    try:
        # Copy timestamped PNG to canonical path (overwrite existing)
        shutil.copy2(get_path(png_name), canonical_png)
    except Exception as e:
        print(f"[WARN] Failed to copy generated PNG to canonical path: {e}")

    # Register image in task state: keep history (timestamped) but expose canonical URL
    ts_url = f"/{app.config['UPLOAD_FOLDER']}/{task_id}/{png_name}"
    canonical_url = f"/{app.config['UPLOAD_FOLDER']}/{task_id}/relationship_schema.png"
    task.setdefault("images", []).append(ts_url)
    task["schema_image_url"] = canonical_url
    add_log(task_id, f"✅ Schema image generated: {png_name}")
    return canonical_url

# -------------------- LOGGING UTILITIES --------------------
def add_log(task_id, text, role="assistant"):
    print(f"[LOG] ({role}) Task {task_id[:8]}: {text}")
    if task_id in tasks:
        tasks[task_id]["logs"].append({
            "role": role,
            "text": text,
            "time": datetime.now(timezone.utc).isoformat()
        })

def set_task_status(task_id, status):
    print(f"[STATUS] Task {task_id[:8]}: {status}")
    if task_id in tasks:
        tasks[task_id]["status"] = status


def create_task_dir(task_id):
    """Create task directory under UPLOAD_FOLDER and copy db_utils.py into it if present."""
    base = app.config['UPLOAD_FOLDER']
    task_dir = os.path.join(base, task_id)
    os.makedirs(task_dir, exist_ok=True)

    # Copy db_utils.py from project root into task folder for convenience
    src = os.path.join(project_root, 'db_utils.py')
    dst = os.path.join(task_dir, 'db_utils.py')
    try:
        if os.path.exists(src):
            shutil.copy2(src, dst)
            print(f"[INIT] Copied db_utils.py to: {dst}")
        else:
            print(f"[WARN] db_utils.py not found at {src}; skipping copy")
    except Exception as e:
        print(f"[WARN] Failed to copy db_utils.py to {dst}: {e}")

    return task_dir

# -------------------- FILE HANDLING --------------------
def handle_user_upload(files, task_id):
    """Save uploaded files into the task-specific Run_Space subfolder."""
    base = app.config['UPLOAD_FOLDER']
    task_dir = os.path.join(base, task_id)
    print(f"[UPLOAD] Saving files to: {task_dir}")
    # ensure task dir exists and helper files are seeded
    create_task_dir(task_id)

    for file in files:
        file_path = os.path.join(task_dir, file.filename)
        print(f"[UPLOAD] Saving file: {file.filename}")
        file.save(file_path)

    print("[UPLOAD] Running process_uploaded_files()...")
    process_uploaded_files(task_dir)
    print("[UPLOAD] File processing complete.")
    return task_dir

# -------------------- CORRECTION LOOP --------------------
def run_correction_loop(task_id, feedback):
    print(f"[CORRECTION] Starting correction loop for task {task_id[:8]}")
    # mark current thread prints as belonging to this task
    current_task.task_id = task_id
    task_dir = create_task_dir(task_id)

    def get_path(filename):
        return os.path.join(task_dir, filename)

    try:
        set_task_status(task_id, "Applying user feedback...")
        add_log(task_id, f"User Feedback: {feedback}", role="user")

        feedback_path = get_path("user_feedback.txt")
        print(f"[CORRECTION] Writing feedback to: {feedback_path}")
        with open(feedback_path, "w", encoding="utf-8") as f:
            f.write(feedback)

        print("[CORRECTION] Running correction()...")
        correction(
            errors_path=get_path("errors.json"),
            puml_path=get_path("relationship_schema.puml"),
            query_path=feedback_path
        )
        add_log(task_id, "✅ Corrections applied based on user feedback.")
        run_testing_and_review(task_id, context=tasks[task_id]['context'])
        print("[CORRECTION] Completed successfully.")

    except Exception as e:
        print(f"[ERROR] Correction loop failed: {e}")
        set_task_status(task_id, f"Error: {e}")
        add_log(task_id, f"❌ Error during correction loop: {e}")
        app.logger.error(f"Error in task {task_id}: {e}", exc_info=True)
    finally:
        # clear thread-local association
        try:
            del current_task.task_id
        except Exception:
            pass

# -------------------- MAIN PIPELINE --------------------
def run_processing_pipeline(task_id, source_path, context):
    print(f"[PIPELINE] Starting processing pipeline for Task {task_id[:8]}")
    # ensure prints inside this background thread are attributed to this task
    current_task.task_id = task_id
    base_run_space = app.config['UPLOAD_FOLDER']
    task_dir = create_task_dir(task_id)

    def get_path(filename):
        return os.path.join(task_dir, filename)

    try:
        set_task_status(task_id, "Extracting metadata...")
        print(f"[STEP 1] Running generate_metadata() with source: {source_path}")
        generate_metadata(source_path, output_path=get_path("metadata.json"))
        add_log(task_id, "✅ Metadata extracted from uploaded files.")

        set_task_status(task_id, "Generating dimensional model...")
        print("[STEP 2] Generating dimensional model...")
        user_context_path = get_path("refined_User_Query.txt")
        with open(user_context_path, "w", encoding="utf-8") as f:
            f.write(context)

        generate_dimensional_model(
            metadata_file=get_path("metadata.json"),
            user_context_file=user_context_path,
            output_json=get_path("dimensional_model.json")
        )
        add_log(task_id, "✅ Dimensional model generated successfully.")

        print("[STEP 3] Moving to testing and review phase...")
        run_testing_and_review(task_id, context)

    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        set_task_status(task_id, f"Error: {e}")
        add_log(task_id, f"❌ Error during generation: {e}")
        app.logger.error(f"Error in task {task_id}: {e}", exc_info=True)
    finally:
        # clear thread-local association
        try:
            del current_task.task_id
        except Exception:
            pass

# -------------------- TESTING AND REVIEW --------------------
def run_testing_and_review(task_id, context):
    print(f"[TESTING] Running schema testing and review for Task {task_id[:8]}")
    # attribute prints to this task while running tests
    current_task.task_id = task_id
    base_run_space = app.config['UPLOAD_FOLDER']
    task_dir = os.path.join(base_run_space, task_id)

    def get_path(filename):
        return os.path.join(task_dir, filename)

    set_task_status(task_id, "Generating visual schema diagram...")
    print("[TESTING] Running generate_schema()...")
    try:
        img_url = generate_and_register_schema(task_id, context)
        add_log(task_id, "✅ Schema diagram generated.")
    except Exception as e:
        add_log(task_id, f"❌ Schema generation failed: {e}")
        set_task_status(task_id, f"Error: Schema generation failed: {e}")
        return

    set_task_status(task_id, "Running Phase 1 tests...")
    print("[TESTING] Running run_phase1()...")
    phase1_ok = run_phase1(
        user_query_path=get_path("refined_User_Query.txt"),
        output_path=get_path("testcases_prompt.json")
    )
    if not phase1_ok:
        set_task_status(task_id, "Error: Phase 1 test generation failed")
        add_log(task_id, "❌ Phase 1 failed — testcases_prompt.json was not created or is invalid. Check model output in logs.")
        # Do not proceed to Phase 2 if Phase 1 did not produce usable output
        return
    add_log(task_id, "✅ Phase 1 complete.")

    set_task_status(task_id, "Running Phase 2 validation...")
    print("[TESTING] Running run_phase2()...")
    run_phase2(
        plantuml_code_path=get_path("relationship_schema.puml"),
        testcases_path=get_path("testcases_prompt.json"),
        output_dir=task_dir
    )
    add_log(task_id, "✅ Phase 2 validation complete.")

    set_task_status(task_id, "Applying automated corrections...")
    print("[TESTING] Running correction() for auto-fix...")
    correction(
        errors_path=get_path("errors.json"),
        puml_path=get_path("relationship_schema.puml"),
        query_path=get_path("refined_User_Query.txt")
    )
    add_log(task_id, "✅ Automated corrections applied.")

    # After automated corrections, regenerate the schema image so the
    # corrected diagram is available (preserve the previous image too).
    try:
        corrected_img = generate_and_register_schema(task_id, context)
        add_log(task_id, f"✅ Corrected schema image generated: {corrected_img}")
    except Exception as e:
        add_log(task_id, f"❌ Failed to generate corrected schema image: {e}")

    set_task_status(task_id, "Awaiting user review")
    print("[TESTING] Awaiting user feedback...")
    add_log(task_id, "Please review the schema: type 'yes' to continue, or 'no' + corrections.")
    try:
        pass
    finally:
        try:
            del current_task.task_id
        except Exception:
            pass

# -------------------- CONTINUE PIPELINE --------------------
def continue_pipeline(task_id):
    print(f"[CONTINUE] Continuing pipeline for Task {task_id[:8]}")
    # attribute prints in this thread to the task
    current_task.task_id = task_id
    base_run_space = app.config['UPLOAD_FOLDER']
    task_dir = os.path.join(base_run_space, task_id)

    def get_path(filename):
        return os.path.join(task_dir, filename)

    try:
        set_task_status(task_id, "Generating CREATE script...")
        print("[STEP 8] Generating CREATE script...")
        generate_create_script(
            metadata_file=get_path("metadata.json"),
            plantuml_file=get_path("relationship_schema.puml"),
            output_file=get_path("create_Database_Script.py")
        )
        add_log(task_id, "✅ CREATE script generated.")

                # --- PAUSE FOR CREATE APPROVAL ---
        print("[STEP 9] CREATE script generated and ready. Pausing for user approval before execution.")
        add_log(task_id, "CREATE script generated and ready for execution. Awaiting user approval to create tables.", role="assistant")
        # mark awaiting approval in task state (this will be visible to frontend via /status)
        tasks[task_id]['awaiting_approval'] = 'create'
        set_task_status(task_id, "Awaiting approval: create_tables")

        # create/attach an Event object that the approve endpoint will set
        evt = threading.Event()
        approval_events[task_id] = evt

        # Wait until the front-end calls /approve_action/<task_id> and sets the event
        evt.wait()  # indefinite wait — user must approve to continue

        # cleanup awaiting flag and continue
        tasks[task_id].pop('awaiting_approval', None)
        set_task_status(task_id, "Creating tables...")
        print("[STEP 9] User approved. Executing CREATE script now...")

        output_path = os.path.join(app.config['UPLOAD_FOLDER'], task_id, "create_Database_Script.py")

        with open(output_path,"r", encoding="utf-8") as f:
            python_code = f.read()

        # Execute the script, ensuring it runs within its own directory
        result = run_python_code(python_code, run_space_dir=task_dir)
        add_log(task_id, "✅ Tables created.")


        set_task_status(task_id, "Generating INSERT script...")
        print("[STEP 10] Generating INSERT script...")
        generate_insert_script(
            metadata_file=get_path("metadata.json"),
            plantuml_file=get_path("relationship_schema.puml"),
            output_file=get_path("insert_Data_Script.py")
        )
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], task_id, "insert_Data_Script.py")

        add_log(task_id, "✅ INSERT script generated.")

                # --- PAUSE FOR INSERT APPROVAL ---
        print("[STEP 11] INSERT script generated and ready. Pausing for user approval before executing inserts.")
        add_log(task_id, "INSERT script generated and ready for execution. Awaiting user approval to insert data.", role="assistant")
        tasks[task_id]['awaiting_approval'] = 'insert'
        set_task_status(task_id, "Awaiting approval: insert_data")

        evt = threading.Event()
        approval_events[task_id] = evt

        # wait for approval
        evt.wait()

        # cleanup awaiting flag and continue
        tasks[task_id].pop('awaiting_approval', None)
        set_task_status(task_id, "Inserting data...")
        print("[STEP 11] User approved. Executing INSERT script now...")

        with open(output_path,"r", encoding="utf-8") as f:
            python_code = f.read()
        # Execute the script, ensuring it runs within its own directory
        result = run_python_code(python_code, run_space_dir=task_dir)
        if result and result.get('returncode', 1) != 0:
            raise Exception(result['stderr'])
        add_log(task_id, "✅ Data inserted.")


        set_task_status(task_id, "Completed")
        print(f"[COMPLETE] Task {task_id[:8]} finished successfully.")
        add_log(task_id, "🎉 Pipeline completed successfully!")

    except Exception as e:
        print(f"[ERROR] Continue pipeline failed: {e}")
        set_task_status(task_id, f"Error: {e}")
        add_log(task_id, f"❌ Error: {e}")
        app.logger.error(f"Error in task {task_id}: {e}", exc_info=True)
    finally:
        try:
            del current_task.task_id
        except Exception:
            pass

# -------------------- ROUTES --------------------
@app.route('/')
def upload():
    print("[ROUTE] GET / - Upload page requested.")
    return render_template('upload.html', active_page='upload')

@app.route('/dashboard')
def dashboard():
    print("[ROUTE] GET /dashboard")
    return render_template('dashboard.html', active_page='dashboard')

@app.route('/Run_Space/<path:filename>')
def run_space_files(filename):
    print(f"[ROUTE] Serving file from Run_Space: {filename}")
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


from markupsafe import escape

from flask import send_file, abort
import os

from flask import render_template, abort
import os

from flask import render_template, abort
import os

from flask import send_file, abort
import os

# Add imports at top if not already present
from werkzeug.utils import safe_join
from flask import send_from_directory, abort

# --- Download route ---
@app.route('/download_raw/<task_id>/<path:filename>')
def download_raw(task_id, filename):
    """
    Securely serve a file from Run_Space/<task_id> as an attachment (download).
    Prevents path-traversal and only serves files that exist inside the task directory.
    """
    print(f"[ROUTE] GET /download_raw/{task_id[:8]}/{filename}")

    # Ensure task exists (optional — makes errors clearer)
    if task_id not in tasks:
        print("[ERROR] download_raw: Task not found")
        abort(404, description="Task not found")

    upload_folder = app.config.get('UPLOAD_FOLDER', os.path.join(os.getcwd(), "Run_Space"))
    task_dir = os.path.join(upload_folder, task_id)

    # Safe join prevents path traversal (../)
    try:
        full_path = safe_join(task_dir, filename)
    except Exception as e:
        app.logger.warning("download_raw: unsafe filename or join failed: %s", e)
        abort(404)

    # Ensure the file actually exists under the task dir
    if not os.path.isfile(full_path):
        print(f"[ERROR] download_raw: file not found: {full_path}")
        abort(404, description="File not found")

    # send_from_directory will stream the file as an attachment
    try:
        return send_from_directory(task_dir, filename, as_attachment=True)
    except Exception as e:
        app.logger.exception("download_raw: failed to send file")
        abort(500, description=str(e))



@app.route('/view_script/<task_id>/<which>')
def view_script(task_id, which):
    """
    View generated script content in a readable web page.
    which: 'create' or 'insert'
    """
    print(f"[ROUTE] GET /view_script/{task_id[:8]}/{which}")

    # Validate task exists
    if task_id not in tasks:
        return "Task not found", 404

    # Only allow the two script types
    if which not in ('create', 'insert'):
        return "Invalid script type", 400

    upload_folder = app.config.get('UPLOAD_FOLDER', os.path.join(os.getcwd(), "Run_Space"))
    task_dir = os.path.join(upload_folder, task_id)

    # Map expected filenames
    filenames = {
        'create': 'create_Database_Script.py',
        'insert': 'insert_Data_Script.py'
    }
    filename = filenames[which]
    path = os.path.join(task_dir, filename)

    # compute which files actually exist in the task folder so template can render download buttons
    candidate_files = ['create_Database_Script.py', 'insert_Data_Script.py', 'insert_script.sql']  # add other names you sometimes generate
    available_files = []
    for f in candidate_files:
        if os.path.isfile(os.path.join(task_dir, f)):
            available_files.append(f)

    # If the specific requested file doesn't exist, render template with script_exists=False
    if not os.path.exists(path):
        return render_template(
            'view_script.html',
            task_id=task_id,
            which=which,
            script_exists=False,
            script_content="",
            filename=filename,
            UPLOAD_FOLDER=os.path.basename(upload_folder),
            available_files=available_files
        )

    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        app.logger.exception("Failed to read script file")
        return f"Failed to read script: {e}", 500

    return render_template(
        'view_script.html',
        task_id=task_id,
        which=which,
        script_exists=True,
        script_content=content,
        filename=filename,
        UPLOAD_FOLDER=os.path.basename(upload_folder),
        available_files=available_files
    )





@app.route('/start_generation', methods=['POST'])
def start_generation():
    print("[ROUTE] POST /start_generation")
    task_id = str(uuid.uuid4())
    print(f"[TASK] New task created: {task_id}")
    # attribute prints during this request to the created task
    current_task.task_id = task_id

    data_medium = request.form.get('data_medium')
    files = request.files.getlist('csv_files')
    context = request.form['schema_context']
    base = app.config['UPLOAD_FOLDER']
    os.makedirs(base, exist_ok=True)
    task_dir = create_task_dir(task_id)

    files_uploaded = False
    fetch_attempted = False
    if data_medium == 'direct_file_drop' and files:
        print(f"[UPLOAD] Handling {len(files)} uploaded files.")
        handle_user_upload(files, task_id)
        files_uploaded = True

        csv_files = get_csv_files_from_directory(task_dir)
        print(f"[CHECK] Found CSV files: {csv_files}")
        if not csv_files:
            process_uploaded_files(task_dir)
            csv_files = get_csv_files_from_directory(task_dir)
            print(f"[CHECK] Rechecked CSV files: {csv_files}")

        if not csv_files:
            saved_files = os.listdir(task_dir)
            print(f"[ERROR] No CSV files found after upload. Saved files: {saved_files}")
            return jsonify({
                "error": "No CSV files found after upload.",
                "saved_files": saved_files,
                "hint": "Upload CSV files or JSON files that can be converted to CSV."
            }), 400

    task_data_path = None
    if data_medium in ('dynamodb', 'aws_dynamodb'):
        print("[FETCH] Data source: DynamoDB")
        try:
            # Support two forms: either individual fields (preferred) or a
            # combined connection string in 'aws_dynamodb_connection' like
            # "region=us-east-1,table=my_table,access_key=...,secret_key=...".
            # Require explicit structured DynamoDB inputs from the client form
            access_key = request.form.get('dynamodb_access_key')
            secret_key = request.form.get('dynamodb_secret_key')
            region = request.form.get('dynamodb_region')
            table_name = request.form.get('dynamodb_table_name')

            if not table_name:
                return jsonify({"error": "Missing DynamoDB table name (provide 'dynamodb_table_name' in the form)."}), 400

            fetch_attempted = True
            items = fetch_from_dynamodb(
                access_key=access_key,
                secret_key=secret_key,
                region=region,
                table_name=table_name
            )
            # use resolved table_name (may have come from parsed connection)
            task_data_path = os.path.join(task_dir, f"{table_name}.json")
            with open(task_data_path, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=2)
            # mark that we've placed files into the task folder
            files_uploaded = True
            add_log(task_id, f"✅ Fetched {len(items)} items from DynamoDB table '{table_name}'.")
        except Exception as e:
            return jsonify({"error": f"DynamoDB fetch failed: {e}"}), 500

    elif data_medium in ('s3', 's3_bucket'):
        print("[FETCH] Data source: S3")
        try:
            # Support either separate fields or a single s3_bucket_path like
            # s3://bucket/key/to/object.ext
            # Require explicit structured S3 inputs from the client form
            if 's3_object_key' in request.form and 's3_bucket_name' in request.form:
                bucket = request.form['s3_bucket_name']
                object_key = request.form['s3_object_key']
            else:
                return jsonify({"error": "Missing S3 bucket name or object key (provide 's3_bucket_name' and 's3_object_key')."}), 400

            local_filename = os.path.join(task_dir, os.path.basename(object_key) or 's3_object')
            fetch_attempted = True
            fetch_from_s3(
                access_key=request.form.get('s3_access_key'),
                secret_key=request.form.get('s3_secret_key'),
                region=request.form.get('s3_region'),
                bucket_name=bucket,
                object_key=object_key,
                local_filename=local_filename
            )
            # mark that we've placed files into the task folder
            files_uploaded = True
            add_log(task_id, f"✅ Fetched file from S3 bucket '{bucket}' to '{local_filename}'.")
        except Exception as e:
            return jsonify({"error": f"S3 fetch failed: {e}"}), 500

    elif data_medium in ('azure_cosmosdb', 'cosmosdb'):
        print("[FETCH] Data source: Azure Cosmos DB")
        try:
            # Expect either separate fields: cosmos_uri, cosmos_db, cosmos_collection
            # or a combined connection string in 'azure_cosmosdb_connection' (less preferred)
            # Require explicit structured Cosmos inputs from the client form
            uri = request.form.get('cosmos_uri')
            db_name = request.form.get('cosmos_db')
            collection = request.form.get('cosmos_collection')

            if not (uri and db_name and collection):
                return jsonify({"error": "Missing CosmosDB connection details. Provide cosmos_uri, cosmos_db, and cosmos_collection."}), 400

            fetch_attempted = True
            docs = fetch_from_cosmosdb(uri=uri, db_name=db_name, collection_name=collection)
            task_data_path = os.path.join(task_dir, f"{db_name}__{collection}.json")
            with open(task_data_path, 'w', encoding='utf-8') as f:
                json.dump(docs, f, indent=2)
            add_log(task_id, f"✅ Fetched {len(docs)} documents from CosmosDB {db_name}/{collection}.")
        except Exception as e:
            return jsonify({"error": f"CosmosDB fetch failed: {e}"}), 500

    elif data_medium in ('website', 'Website/HTML', 'website_html'):
        print("[FETCH] Data source: Website/HTML")
        try:
            website_url = request.form.get('website_link') or request.form.get('website_url')
            if not website_url:
                return jsonify({"error": "Missing website URL (provide 'website_link')."}), 400

            fetch_attempted = True
            add_log(task_id, f"Fetching and converting website: {website_url}")
            # Ask converter to write directly into the task folder and return absolute paths
            written_files = convert_html_to_csv(website_url, output_dir=task_dir)

            # Ensure the returned files actually exist and record basenames.
            # Some converters may have small write delays; poll briefly for files to appear.
            moved = [os.path.basename(p) for p in written_files if os.path.exists(p)]
            files_uploaded = bool(moved)
            task_data_path = task_dir
            add_log(task_id, f"✅ Website conversion initial result: {moved}")

            if not moved:
                # Poll for a short time to allow converter to finish writing files to disk.
                max_wait = 12.0  # seconds
                interval = 0.5
                waited = 0.0
                add_log(task_id, f"Waiting up to {max_wait}s for converter to write files...")
                while waited < max_wait and not moved:
                    time.sleep(interval)
                    waited += interval
                    # re-check for any CSV files in the task folder
                    try:
                        current_csvs = [f for f in os.listdir(task_dir) if f.lower().endswith('.csv') and not f.startswith('.')]
                    except Exception:
                        current_csvs = []
                    if current_csvs:
                        moved = current_csvs
                        files_uploaded = True
                        add_log(task_id, f"Detected CSVs after waiting: {moved}")
                        break

            if not moved:
                # Conversion completed (or returned) but produced no files; return clear error to client
                try:
                    saved = [f for f in os.listdir(task_dir) if not f.startswith('.')]
                except Exception:
                    saved = []
                app.logger.error(f"Website conversion produced no files. Written: {written_files}; Saved in task dir: {saved}")
                return jsonify({
                    "error": "Website conversion completed but no CSVs were produced.",
                    "written_files": written_files,
                    "saved_files": saved,
                    "hint": "Check the target URL, page access, converter logs on the server, or increase the wait timeout."
                }), 500
        except Exception as e:
            app.logger.error(f"Website conversion failed: {e}", exc_info=True)
            return jsonify({"error": f"Website conversion failed: {e}"}), 500

    # After fetching, all data (from any source) should be a local file in the task folder.
    # If a fetch was attempted but nothing was written into the task folder, report an error.
    try:
        saved_files = [f for f in os.listdir(task_dir) if not f.startswith('.')]
    except Exception:
        saved_files = []

    # Exclude the seeded helper file db_utils.py from the check
    visible_files = [f for f in saved_files if f != 'db_utils.py']
    if fetch_attempted and not visible_files:
        print(f"[ERROR] Fetch attempted but no files were written to {task_dir}. Saved files: {saved_files}")
        return jsonify({
            "error": "Data fetch attempted but no files were written into the task Run_Space directory.",
            "saved_files": saved_files,
            "hint": "Check credentials, table/object names, and network access."
        }), 500

    # The rest of the pipeline expects CSVs, so run conversion (this may convert JSON -> CSV etc.)
    converted = process_uploaded_files(task_dir)
    add_log(task_id, "Running file conversion to ensure all data is in CSV format.")

    # After conversion, ensure there are CSV files. If a fetch was attempted but conversion produced none,
    # return an explicit error so the client can surface the fetch failure.
    csv_files = get_csv_files_from_directory(task_dir)
    if fetch_attempted and not csv_files:
        saved_files = [f for f in os.listdir(task_dir) if not f.startswith('.')]
        print(f"[ERROR] Fetch/convert attempted but no CSV files found in {task_dir}. Saved files: {saved_files}; Converted: {converted}")
        return jsonify({
            "error": "Data fetch/convert completed but no CSV files were produced.",
            "saved_files": saved_files,
            "converted_files": converted
        }), 500

    if not (files_uploaded or data_medium != 'direct_file_drop') or not context:
        print("[ERROR] Invalid input: missing data source or context.")
        return jsonify({"error": "Please provide a data source and context."}), 400

    if files_uploaded:
        source_path = task_dir
    else:
        sharepoint_link = request.form.get('sharepoint_link')
        source_path = sharepoint_link if sharepoint_link else task_dir

    tasks[task_id] = {
        "status": "Starting...",
        "logs": [],
        # system terminal logs captured for debugging and display
        "system_logs": [],
        # images will be registered as they are generated; keep list for history
        "images": [],
        "schema_image_url": "",
        "context": context
    }

    add_log(task_id, f"User Context: {context}", role="user")
    print(f"[THREAD] Launching background thread for task {task_id[:8]}...")
    thread = threading.Thread(target=run_processing_pipeline, args=(task_id, source_path, context))
    thread.start()

    # leave request; thread will capture subsequent background prints
    return jsonify({"task_id": task_id})

@app.route('/approve_action/<task_id>', methods=['POST'])
def approve_action(task_id):
    """
    Called by the frontend to approve a paused action (create / insert).
    Expected JSON body: { "action": "create" } OR { "action": "insert" }
    """
    print(f"[ROUTE] POST /approve_action/{task_id[:8]}")
    data = request.get_json() or {}
    action = data.get('action')

    if not action or action not in ('create', 'insert'):
        return jsonify({"error": "Invalid action. Use 'create' or 'insert'."}), 400

    # Ensure task exists
    if task_id not in tasks:
        return jsonify({"error": "Task not found."}), 404

    # Signal the waiting background thread by setting the event
    evt = approval_events.get(task_id)
    if not evt:
        # No event means either it's already approved or not currently awaiting approval
        add_log(task_id, f"❌ Approve called for {action}, but pipeline was not awaiting approval.", role="assistant")
        return jsonify({"ok": False, "message": "No approval awaited for this task."}), 409

    try:
        add_log(task_id, f"User approved: {action}.", role="user")
        set_task_status(task_id, f"User approved: {action}. Resuming...")
        evt.set()  # resume the background thread
        # remove the event from mapping — background thread will clean up too
        approval_events.pop(task_id, None)
        return jsonify({"ok": True, "message": f"Approved {action}."})
    except Exception as e:
        app.logger.exception("Error in approve_action")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/status/<task_id>')
def task_status(task_id):
    print(f"[ROUTE] GET /status/{task_id[:8]}")
    task = tasks.get(task_id)
    if not task:
        print("[ERROR] Task not found.")
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)

@app.route('/submit_review/<task_id>', methods=['POST'])
def submit_review(task_id):
    print(f"[ROUTE] POST /submit_review for Task {task_id[:8]}")
    data = request.get_json() or {}
    feedback = data.get('feedback')

    if not feedback or not isinstance(feedback, str) or not feedback.strip():
        print("[ERROR] Empty feedback submitted.")
        return jsonify({"error": "Please provide feedback text (start with 'yes' or 'no <details>')."}), 400

    feedback = feedback.strip()
    feedback_lower = feedback.lower()

    if feedback_lower == 'yes':
        print("[REVIEW] User approved schema.")
        add_log(task_id, "User approved schema.", role="user")
        thread = threading.Thread(target=continue_pipeline, args=(task_id,))
        thread.start()
        return jsonify({"message": "Approval received. Continuing pipeline."})

    if feedback_lower.startswith('no'):
        correction_details = feedback[2:].strip()
        print(f"[REVIEW] User requested corrections: {correction_details}")
        if not correction_details:
            return jsonify({"error": "Please provide correction details after 'no'."}), 400
        add_log(task_id, f"User requested corrections: {correction_details}", role="user")
        thread = threading.Thread(target=run_correction_loop, args=(task_id, correction_details))
        thread.start()
        return jsonify({"message": "Corrections received. Applying corrections."})

    print("[ERROR] Invalid feedback format.")
    return jsonify({"error": "Invalid feedback. Please start with 'yes' or 'no <details>'."}), 400

# -------------------- APP START --------------------
if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    print(f"[STARTUP] Flask app running on port 5001...")
    app.run(debug=True, port=5001)
