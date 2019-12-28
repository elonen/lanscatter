#!/bin/bash
set -e

PYTHON=python3.7
REQ=requirements.txt
ACTIVATE=venv/bin/activate

if (uname | grep -q -E '(CYGWIN|MINGW)'); then
  echo "NOTE: Windows OS detected. Using 'python' instead of '$PYTHON'."
  PYTHON=python
  ACTIVATE=venv/Scripts/activate
fi

if (uname | grep -q -E 'Linux'); then
  echo "NOTE: Linux OS detected. Skipping installing GUI packages (failing ATM)."
  REQ=requirements.cli.txt
fi

if [ ! -e venv ]; then
  $PYTHON -m venv venv
fi

source $ACTIVATE || { echo "Venv activation failed."; exit 1; }
pip install -r $REQ
python ./setup.py develop

echo " "
echo "---"
echo "Done. First run 'source $ACTIVATE'"
echo "Then try 'lanscatter_master --help', 'lanscatter_peer' or 'lanscatter_gui'."
