import re
import os
import shutil
import glob
import subprocess
import sys
import tempfile
import time
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)


def extract_code_blocks(text: str) -> List[Dict[str, str]]:
    pattern = re.compile(r"```(\w+)?\n(.*?)```", re.S)
    blocks = []
    for m in pattern.finditer(text):
        lang = m.group(1) or ""
        code = m.group(2).strip()
        blocks.append({"language": lang, "code": code})
    logger.info("extract_code_blocks: found %d code block(s)", len(blocks))
    return blocks


def _wait_for_new_files_stable(before_set: set, directory: str, max_wait_s: float = 5.0, stability_interval: float = 0.3):
    """
    After a script runs, detect which files are newly created in `directory` (not present in before_set),
    wait for each new file to reach a stable size (no growth) for stability_interval, up to max_wait_s total.
    Return list of new file full paths (stable ones only).
    """
    start = time.time()
    stable_files = []
    attempted = set()
    while True:
        now = time.time()
        after_names = set(os.listdir(directory))
        new_names = after_names - before_set
        # exclude the script itself if present (we don't care)
        new_paths = [os.path.join(directory, n) for n in new_names]
        # For each candidate file, wait for size stability
        for p in new_paths:
            if p in attempted:
                continue
            attempted.add(p)
            # only consider files (not directories)
            if not os.path.isfile(p):
                continue
            try:
                # wait up to (remaining_time) for size to stabilize
                last_size = os.path.getsize(p)
                stable_since = time.time()
                while True:
                    time.sleep(stability_interval)
                    try:
                        cur_size = os.path.getsize(p)
                    except Exception:
                        # file might have been removed; break out and try later
                        break
                    if cur_size == last_size:
                        # still same size across stability_interval -> treat as stable
                        stable_files.append(p)
                        break
                    last_size = cur_size
                    if time.time() - start > max_wait_s:
                        # timed out waiting for this file, give up on stability
                        logger.warning("Timed out waiting for file stability: %s", p)
                        break
            except Exception as e:
                logger.debug("Exception while waiting for file stability: %s", e)
        # finish when we've checked all new items or time exhausted
        if time.time() - start > max_wait_s:
            break
        # small sleep before re-evaluating new files
        time.sleep(0.05)
        # break early if we've found any stable files and no more new names are appearing rapidly
        # (helps return faster in common cases)
        if stable_files and (time.time() - start) > 0.1:
            break
    # Filter stable_files to unique and existing
    return [p for p in dict.fromkeys(stable_files) if os.path.exists(p)]


def run_python_code(code: str, outfile: Optional[str] = None, timeout: int = 10000, run_space_dir: Optional[str] = None) -> Dict[str, object]:
    if "```python" in code:
        # defensively extract the inner python block if present
        try:
            code = code.split("```python", 1)[1].split("```", 1)[0]
        except Exception:
            # fallback: remove any fences via regex
            code = re.sub(r'^```(?:python)?\s*', '', code, flags=re.IGNORECASE)
            code = re.sub(r'\s*```\s*$', '', code, flags=re.IGNORECASE)

    logger.info("run_python_code: entry (timeout=%s, run_space_dir=%s)", timeout, run_space_dir)

    # Prefer executing directly inside Run_Space so any generated files land there.
    if run_space_dir is None:
        candidate = os.path.join(os.path.dirname(__file__), "Run_Space")
        if os.path.isdir(candidate):
            run_space_dir = candidate
        else:
            run_space_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Run_Space"))

    logger.info("Executing script with run_space_dir=%s", run_space_dir)

    # Normalize the run_space_dir to an absolute path and defensively collapse
    # accidental repeated path segments (e.g. Run_Space/.../Run_Space/...).
    try:
        run_space_dir = os.path.abspath(run_space_dir)
        comps = run_space_dir.split(os.sep)
        for n in range(1, len(comps) // 2 + 1):
            if comps[-n:] == comps[-2 * n:-n]:
                comps = comps[:-n]
                new_path = os.sep.join(comps)
                logger.warning("Detected duplicated path suffix; collapsing %s -> %s", run_space_dir, new_path)
                run_space_dir = new_path
                break
    except Exception:
        logger.exception("Failed to normalize run_space_dir, proceeding with original value.")

    # Ensure Run_Space exists
    if os.path.isdir(run_space_dir):
        # Record files before write to detect new outputs later
        try:
            before_files = set(os.listdir(run_space_dir))
        except Exception:
            before_files = set()

        # Write the script into Run_Space and run it there
        script_path = os.path.join(run_space_dir, "generated_script.py")
        try:
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(code)
        except Exception as e:
            logger.error("Failed to write script to %s: %s", script_path, e)
            return {"returncode": -2, "stdout": "", "stderr": f"Script write failed: {e}", "path": script_path, "files": [], "copied": []}

        logger.info("Executing script in Run_Space (cwd=%s): %s", run_space_dir, script_path)

        # Sanity check: ensure the script we just wrote exists and is readable
        if not os.path.exists(script_path):
            logger.error("Script file was not found after write: %s", script_path)
            return {"returncode": -2, "stdout": "", "stderr": f"Script file not found: {script_path}", "path": script_path, "files": [], "copied": []}

        command = [sys.executable, os.path.basename(script_path)]
        logger.info("Executing command: %s in CWD: %s", " ".join(command), run_space_dir)

        # Execute and capture output reliably
        try:
            proc = subprocess.Popen(command, cwd=run_space_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', close_fds=os.name != 'nt')
            try:
                stdout, stderr = proc.communicate(timeout=timeout)
                returncode = proc.returncode
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate(timeout=5)
                return {"returncode": -1, "stdout": stdout or "", "stderr": f"Timeout after {timeout}s\n{stderr or ''}", "path": script_path, "files": [], "copied": []}
        except Exception as e:
            logger.exception("Error while executing subprocess: %s", e)
            return {"returncode": -3, "stdout": "", "stderr": str(e), "path": script_path, "files": [], "copied": []}

        # After process exit, detect newly created files and wait for them to become stable.
        # This prevents the parent from reading files that the child is still writing/flushing.
        try:
            produced_candidates = _wait_for_new_files_stable(before_files, run_space_dir, max_wait_s=6.0, stability_interval=0.25)
        except Exception as e:
            logger.warning("Error while waiting for new files to stabilize: %s", e)
            produced_candidates = []

        # Filter produced list to include interesting file types only (like you had before)
        produced = []
        for full in produced_candidates:
            name = os.path.basename(full)
            if name == os.path.basename(script_path):
                continue
            if os.path.isfile(full) and name.lower().endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif', '.pdf', '.csv', '.txt', '.sql')):
                produced.append(full)

        result = {
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
            "path": script_path,
            "files": produced,
            "copied": [],
        }
        logger.info("run_python_code: returning result with stdout length=%d, files=%s", len(result['stdout'] or ""), result['files'])
        return result
    else:
        # Fallback: isolated temp dir execution (unchanged, but also uses communicate)
        with tempfile.TemporaryDirectory() as d:
            copied_files = []
            if os.path.isdir("Run_Space"):
                try:
                    shutil.copytree("Run_Space", d, dirs_exist_ok=True)
                except Exception:
                    # older shutil may not have dirs_exist_ok, fallback:
                    try:
                        for name in os.listdir("Run_Space"):
                            src = os.path.join("Run_Space", name)
                            dst = os.path.join(d, name)
                            if os.path.isdir(src):
                                shutil.copytree(src, dst)
                            else:
                                shutil.copy2(src, dst)
                    except Exception:
                        pass
                for entry in os.listdir("Run_Space"):
                    copied_files.append(os.path.join(d, entry))

            script_path = os.path.join(d, "generated_script.py")
            try:
                with open(script_path, "w", encoding="utf-8") as f:
                    f.write(code)
            except Exception as e:
                logger.error("Failed to write script in temp dir: %s", e)
                return {"returncode": -2, "stdout": "", "stderr": f"Script write failed: {e}", "path": script_path, "files": [], "copied": copied_files}

            logger.info("Executing script in temp dir: %s", script_path)
            if not os.path.exists(script_path):
                logger.error("Script file was not found in temp dir after write: %s", script_path)
                return {"returncode": -2, "stdout": "", "stderr": f"Script file not found: {script_path}", "path": script_path, "files": [], "copied": copied_files}
            try:
                completed = subprocess.run([sys.executable, os.path.basename(script_path)], capture_output=True, text=True, timeout=timeout, cwd=d)
            except subprocess.TimeoutExpired as e:
                logger.warning("Script timeout after %s seconds", timeout)
                return {"returncode": -1, "stdout": e.stdout or "", "stderr": f"Timeout after {timeout}s", "path": script_path, "files": [], "copied": copied_files}

            produced = []
            try:
                for name in os.listdir(d):
                    full = os.path.join(d, name)
                    if full == script_path:
                        continue
                    if os.path.isfile(full) and name.lower().endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif', '.pdf', '.csv', '.txt')):
                        produced.append(full)
            except Exception:
                produced = []

            result = {
                "returncode": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
                "path": script_path,
                "files": produced,
                "copied": copied_files,
            }
            logger.info("run_python_code: returning result with stdout length=%d", len(completed.stdout or ""))
            return result


def save_generated_code(code: str, filename: str = "generated_code.py") -> None:
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(code)
        logger.info(f"Saved generated code to {filename}")
    except Exception as e:
        logger.error(f"Failed to save generated code: {e}")


if __name__ == "__main__":

    filepath = '../Run_Space/Test_Runner/generated_table_converter.py'

    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        sys.exit(1)

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            python_code = f.read()

        logger.info(f"Executing code from {filepath}")
        result = run_python_code(python_code, run_space_dir=os.path.join('..', "Run_Space", "Test_Runner"))

        print("\n--- Execution Result ---")
        print(f"Return Code: {result['returncode']}")
        print("\n--- STDOUT ---")
        print(result['stdout'] or "")
        print("\n--- STDERR ---")
        print(result['stderr'] or "")
        print(f"\nNew files created: {result['files']}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
