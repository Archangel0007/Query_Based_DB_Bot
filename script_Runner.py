import re
import os
import shutil
import glob
import subprocess
import sys
import tempfile
from typing import List, Dict, Optional
from query_Generator import generate_and_send
import logging

# configure simple logging to stdout
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)


def fetch_gemini_flash_output(user_query: str, temperature: float = 0.2) -> str:
	logger.info("Calling Gemini Flash generate for query (len=%d)", len(user_query))
	resp = generate_and_send(user_query, model="gemini-2.5-flash", temperature=temperature)
	logger.info("Gemini call complete (response length=%d)", len(resp) if isinstance(resp, str) else 0)
	return resp


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
	# detect default Run_Space next to this file
	if run_space_dir is None:
		candidate = os.path.join(os.path.dirname(__file__), "Run_Space")
		if os.path.isdir(candidate):
			run_space_dir = candidate
	logger.info("Using Run_Space: %s", run_space_dir)

	# Run the code inside a temporary directory so any generated files (plots/images)
	# are contained and can be discovered and returned.
	# If Run_Space exists, run directly there (no copying); otherwise use a temp dir
	if run_space_dir and os.path.isdir(run_space_dir):
		logger.info("Running generated script directly inside Run_Space: %s", run_space_dir)
		# snapshot files before execution so we can detect newly created outputs
		# snapshot files (with mtime and size) before execution so we can detect newly created or modified outputs
		before = {}
		for name in os.listdir(run_space_dir):
			full = os.path.join(run_space_dir, name)
			try:
				if os.path.isfile(full):
					before[name] = (os.path.getmtime(full), os.path.getsize(full))
				else:
					before[name] = None
			except Exception:
				before[name] = None
		# Prepare code wrapper to ensure matplotlib uses Agg in headless runs and
		# auto-saves figures.
		prelude = """
import matplotlib
matplotlib.use('Agg')
"""

		postlude = """
# Auto-save any open matplotlib figures to files so headless execution still produces images.
try:
    import matplotlib.pyplot as _plt
    import os as _os
    for i, fig_num in enumerate(_plt.get_fignums()):
        fig = _plt.figure(fig_num)
        out = _os.path.join(_os.getcwd(), f'figure_{i}.png')
        try:
            fig.savefig(out, bbox_inches='tight')
        except Exception:
            pass
        try:
            _plt.close(fig)
        except Exception:
            pass
except Exception:
    pass
"""

		final_code = prelude + "\n" + code + "\n" + postlude

		script_path = os.path.join(run_space_dir, "generated_script.py")
		with open(script_path, "w", encoding="utf-8") as f:
			f.write(final_code)

		logger.info("Executing script: %s", script_path)
		try:
			completed = subprocess.run([sys.executable, script_path], capture_output=True, text=True, timeout=timeout, cwd=run_space_dir)
			logger.info("Script finished with returncode=%s", completed.returncode)
		except subprocess.TimeoutExpired as e:
			logger.warning("Script timeout after %s seconds", timeout)
			return {"returncode": -1, "stdout": e.stdout or "", "stderr": f"Timeout after {timeout}s", "path": script_path, "files": [], "copied": []}

		# detect newly created or modified files in Run_Space (exclude script itself)
		after = {}
		for name in os.listdir(run_space_dir):
			full = os.path.join(run_space_dir, name)
			try:
				if os.path.isfile(full):
					after[name] = (os.path.getmtime(full), os.path.getsize(full))
				else:
					after[name] = None
			except Exception:
				after[name] = None

		new_files = []
		for name, stat in after.items():
			if name == os.path.basename(script_path):
				continue
			full = os.path.join(run_space_dir, name)
			# consider created if not in before
			if name not in before:
				if stat is not None and name.lower().endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif', '.pdf', '.csv', '.txt')):
					new_files.append(full)
			else:
				# existed before; include if mtime or size changed
				bstat = before.get(name)
				if stat is not None and bstat is not None:
					if stat[0] > bstat[0] + 1e-6 or stat[1] != bstat[1]:
						if name.lower().endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif', '.pdf', '.csv', '.txt')):
							new_files.append(full)

		logger.info("Detected %d new produced file(s) in Run_Space", len(new_files))
		result = {
			"returncode": completed.returncode,
			"stdout": completed.stdout,
			"stderr": completed.stderr,
			"path": script_path,
			"files": new_files,
			"copied": [],
		}
		logger.info("run_python_code: returning result with stdout length=%d", len(completed.stdout or ""))
		return result
	else:
		# fallback: run in an isolated temp dir and copy Run_Space contents into it
		with tempfile.TemporaryDirectory() as d:
			copied_files = []
			if run_space_dir and os.path.isdir(run_space_dir):
				for entry in os.listdir(run_space_dir):
					src = os.path.join(run_space_dir, entry)
					dst = os.path.join(d, entry)
					try:
						if os.path.isdir(src):
							shutil.copytree(src, dst)
						else:
							shutil.copy2(src, dst)
						copied_files.append(dst)
					except Exception:
						pass
			logger.info("Copied %d file(s)/dirs from Run_Space into temp dir", len(copied_files))

			prelude = """
import matplotlib
matplotlib.use('Agg')
"""

			postlude = """
# Auto-save any open matplotlib figures to files so headless execution still produces images.
try:
    import matplotlib.pyplot as _plt
    import os as _os
    for i, fig_num in enumerate(_plt.get_fignums()):
        fig = _plt.figure(fig_num)
        out = _os.path.join(_os.getcwd(), f'figure_{i}.png')
        try:
            fig.savefig(out, bbox_inches='tight')
        except Exception:
            pass
        try:
            _plt.close(fig)
        except Exception:
            pass
except Exception:
    pass
"""

			final_code = prelude + "\n" + code + "\n" + postlude

			script_path = os.path.join(d, "generated_script.py")
			with open(script_path, "w", encoding="utf-8") as f:
				f.write(final_code)

			logger.info("Executing script (temp dir): %s", script_path)
			try:
				completed = subprocess.run([sys.executable, script_path], capture_output=True, text=True, timeout=timeout, cwd=d)
				logger.info("Script finished with returncode=%s", completed.returncode)
			except subprocess.TimeoutExpired as e:
				logger.warning("Script timeout after %s seconds", timeout)
				return {"returncode": -1, "stdout": e.stdout or "", "stderr": f"Timeout after {timeout}s", "path": script_path, "files": [], "copied": copied_files}

			# collect files produced in the temp dir (exclude the script file)
			produced = []
			for name in os.listdir(d):
				full = os.path.join(d, name)
				if full == script_path:
					continue
				# include common image and plot file types or any other files produced
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
			logger.info("run_python_code (temp): returning result with stdout length=%d", len(completed.stdout or ""))
			return result

def save_generated_code(code: str, filename: str = "generated_code.py") -> None:
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(code)
        logger.info(f"Saved generated code to {filename}")
    except Exception as e:
        logger.error(f"Failed to save generated code: {e}")

if __name__ == "__main__":
    sample = """```python
print('hello from generated code')
```"""
    blocks = extract_code_blocks(sample)
    print(blocks)
    if blocks:
        save_generated_code(blocks[0]["code"], filename="last_generated_code.py")
        out = run_python_code(blocks[0]["code"])
        print(out)

