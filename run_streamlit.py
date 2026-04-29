"""Run Streamlit using the current Python interpreter.

Usage:
    python run_streamlit.py
or
    python run_streamlit.py -- some_streamlit_args

This wrapper calls: python -m streamlit run genie_web_app.py
"""
import subprocess
import sys
import os

def main():
    repo_dir = os.path.dirname(__file__)
    app_path = os.path.join(repo_dir, "genie_web_app.py")
    args = [sys.executable, "-m", "streamlit", "run", app_path] + sys.argv[1:]
    print("Executando:", " ".join(args))
    return subprocess.run(args).returncode

if __name__ == '__main__':
    raise SystemExit(main())
