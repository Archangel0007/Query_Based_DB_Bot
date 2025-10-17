import re
import os
import shutil
import glob
import subprocess
import sys
import tempfile
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

def run_python_code(code: str, outfile: Optional[str] = None, timeout: int = 10, run_space_dir: Optional[str] = None) -> Dict[str, object]:
	"""Run Python code in an isolated temp dir.

	If a Run_Space directory is available (either passed in `run_space_dir` or a
	sibling directory named 'Run_Space'), its contents will be copied into the
	temp dir so the executed code can access dataset files like CSVs.
	"""
	logger.info("run_python_code: entry (timeout=%s, run_space_dir=%s)", timeout, run_space_dir)

	if run_space_dir is None:
		candidate = os.path.join(os.path.dirname(__file__), "Run_Space")
		if os.path.isdir(candidate):
			run_space_dir = candidate
	logger.info("Using Run_Space: %s", run_space_dir)

	with tempfile.TemporaryDirectory() as d:
		copied_files = []
		if run_space_dir and os.path.isdir(run_space_dir):
			# Use copytree with dirs_exist_ok to handle existing temp dir
			shutil.copytree(run_space_dir, d, dirs_exist_ok=True)
			# Log what was copied
			for entry in os.listdir(run_space_dir):
				copied_files.append(os.path.join(d, entry))
		logger.info("Copied %d file(s)/dirs from Run_Space into temp dir", len(copied_files))



		script_path = os.path.join(d, "generated_script.py")
		with open(script_path, "w", encoding="utf-8") as f:
			f.write(code)

		logger.info("Executing script in temp dir: %s", script_path)
		try:
			completed = subprocess.run([sys.executable, script_path], capture_output=True, text=True, timeout=timeout, cwd=d)
			logger.info("Script finished with returncode=%s", completed.returncode)
		except subprocess.TimeoutExpired as e:
			logger.warning("Script timeout after %s seconds", timeout)
			return {"returncode": -1, "stdout": e.stdout or "", "stderr": f"Timeout after {timeout}s", "path": script_path, "files": [], "copied": copied_files}

		produced = []
		for name in os.listdir(d):
			full = os.path.join(d, name)
			if full == script_path:
				continue

			if os.path.isfile(full) and name.lower().endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif', '.pdf', '.csv', '.txt')):
				produced.append(full)
		logger.info("Collected %d produced file(s) in temp dir", len(produced))
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

	filepath = os.path.join("Run_Space", "create_Database_Script.py")

	if not os.path.exists(filepath):
		logger.error(f"File not found: {filepath}")
		sys.exit(1)

	try:
		with open(filepath, "r", encoding="utf-8") as f:
			python_code = f.read()
		
		logger.info(f"Executing code from {filepath}")
		result = run_python_code(python_code)

		print("\n--- Execution Result ---")
		print(f"Return Code: {result['returncode']}")
		print("\n--- STDOUT ---")
		print(result['stdout'])

		print("\n--- STDERR ---")
		print(result['stderr'])
		print(f"\nNew files created: {result['files']}")

	except Exception as e:
		logger.error(f"An error occurred: {e}")
		sys.exit(1)