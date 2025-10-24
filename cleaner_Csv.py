import os

def clear_csv_from_run_space():
    run_space_dir = "Run_Space"
    if not os.path.isdir(run_space_dir):
        return

    for file_name in os.listdir(run_space_dir):
        if not(file_name.lower().endswith('.py')):
            file_path = os.path.join(run_space_dir, file_name)
            try:
                os.remove(file_path)
            except Exception as e:
                print("schema_chat_history", "assistant", f"⚠️ Could not delete CSV file: {file_name}. Error: {e}")

if __name__ == "__main__":
    clear_csv_from_run_space()
   
"""import os

def clear_files_from_run_space():
    run_space_dir = "Run_Space"

    if not os.path.isdir(run_space_dir):
        return

    for file_name in os.listdir(run_space_dir):

        if file_name.lower() == 'readme.md':
            continue

        file_path = os.path.join(run_space_dir, file_name)

        try:

            os.remove(file_path)
            print(f"Deleted: {file_name}")
        except Exception as e:
            print("schema_chat_history", "assistant", f"⚠️ Could not delete file: {file_name}. Error: {e}")

if __name__ == "__main__":
    clear_files_from_run_space()"""