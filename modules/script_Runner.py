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

def run_python_code(code: str, outfile: Optional[str] = None, timeout: int = 10000, run_space_dir: Optional[str] = None) -> Dict[str, object]:
	if("```python" in code):
		code = code.split("```python")[1].split("```")[0]
	logger.info("run_python_code: entry (timeout=%s, run_space_dir=%s)", timeout, run_space_dir)
	print(code)
	# Prefer executing directly inside Run_Space so any generated files land there.
	if run_space_dir is None:
		candidate = os.path.join(os.path.dirname(__file__), "Run_Space")
		if os.path.isdir(candidate):
			run_space_dir = candidate
		else:
			run_space_dir = "../Run_Space"

	logger.info("Executing script with run_space_dir=%s", run_space_dir)

	# Normalize the run_space_dir to an absolute path and defensively collapse
	# accidental repeated path segments (e.g. Run_Space/.../Run_Space/...).
	try:
		run_space_dir = os.path.abspath(run_space_dir)
		# Split into components and detect a duplicated suffix (A/A) at the end.
		comps = run_space_dir.split(os.sep)
		for n in range(1, len(comps) // 2 + 1):
			if comps[-n:] == comps[-2 * n:-n]:
				# collapse the duplicated segment
				comps = comps[:-n]
				new_path = os.sep.join(comps)
				logger.warning("Detected duplicated path suffix; collapsing %s -> %s", run_space_dir, new_path)
				run_space_dir = new_path
				break
	except Exception:
		# be defensive: if anything goes wrong during normalization, keep original
			logger.exception("Failed to normalize run_space_dir, proceeding with original value.")

	# Ensure Run_Space exists
	if os.path.isdir(run_space_dir):
		# Write the script into Run_Space and run it there
		script_path = os.path.join(run_space_dir, "generated_script.py")
		with open(script_path, "w", encoding="utf-8") as f:
			f.write(code)

		logger.info("Executing script in Run_Space (cwd=%s): %s", run_space_dir, script_path)

		# Sanity check: ensure the script we just wrote exists and is readable
		if not os.path.exists(script_path):
			logger.error("Script file was not found after write: %s", script_path)
			return {"returncode": -2, "stdout": "", "stderr": f"Script file not found: {script_path}", "path": script_path, "files": [], "copied": []}

		# Use basename when invoking the interpreter and set cwd to run_space_dir.
		# This avoids accidental duplicated-path issues when passing absolute paths
		# to subprocess on some platforms.
		command = [sys.executable, os.path.basename(script_path)]
		logger.info(f"Executing command: {' '.join(command)} in CWD: {run_space_dir}")

		try:
			# Use Popen to stream output in real-time
			process = subprocess.Popen(command, cwd=run_space_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
			
			stdout_lines = []
			stderr_lines = []

			# Read output line by line
			for line in process.stdout:
				print(f"[SCRIPT STDOUT] {line.strip()}", flush=True)
				stdout_lines.append(line)
			
			for line in process.stderr:
				print(f"[SCRIPT STDERR] {line.strip()}", flush=True)
				stderr_lines.append(line)

			process.wait(timeout=timeout)
			logger.info("Script finished with returncode=%s", process.returncode)
			
		except subprocess.TimeoutExpired as e:
			logger.warning("Script timeout after %s seconds", timeout)
			return {"returncode": -1, "stdout": "".join(stdout_lines), "stderr": f"Timeout after {timeout}s", "path": script_path, "files": [], "copied": []}

		produced = []
		for name in os.listdir(run_space_dir):
			full = os.path.join(run_space_dir, name)
			if full == script_path:
				continue
			if os.path.isfile(full) and name.lower().endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif', '.pdf', '.csv', '.txt')):
				produced.append(full)

		result = {
			"returncode": process.returncode,
			"stdout": "".join(stdout_lines),
			"stderr": "".join(stderr_lines),
			"path": script_path,
			"files": produced,
			"copied": [],
		}
		logger.info("run_python_code: returning result with stdout length=%d", len(result['stdout']))
		return result
	else:
		# Fallback: isolated temp dir execution
		with tempfile.TemporaryDirectory() as d:
			copied_files = []
			if os.path.isdir("Run_Space"):
				shutil.copytree("Run_Space", d, dirs_exist_ok=True)
				for entry in os.listdir("Run_Space"):
					copied_files.append(os.path.join(d, entry))

			script_path = os.path.join(d, "generated_script.py")
			with open(script_path, "w", encoding="utf-8") as f:
				f.write(code)

			logger.info("Executing script in temp dir: %s", script_path)
			# Sanity check
			if not os.path.exists(script_path):
				logger.error("Script file was not found in temp dir after write: %s", script_path)
				return {"returncode": -2, "stdout": "", "stderr": f"Script file not found: {script_path}", "path": script_path, "files": [], "copied": copied_files}
			try:
				completed = subprocess.run([sys.executable, os.path.basename(script_path)], capture_output=True, text=True, timeout=timeout, cwd=d)
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

	filepath = 'Run_Space/0b84baf9-ec67-43a9-9e1c-c4254f449825/insert_Data_Script.py'

	if not os.path.exists(filepath):
		logger.error(f"File not found: {filepath}")
		sys.exit(1)

	try:
		with open(filepath, "r", encoding="utf-8") as f:
			python_code = f.read()
		
		logger.info(f"Executing code from {filepath}")
		result = run_python_code(python_code,'..Run_Space/0b84baf9-ec67-43a9-9e1c-c4254f449825')

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