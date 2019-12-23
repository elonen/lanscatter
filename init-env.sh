#!/bin/bash
if [ ! -e venv ]; then
  python3.7 -m venv venv
fi
source venv/bin/activate || { echo "Venv activation failed."; exit 1; }
pip install -r requirements.txt
python ./setup.py develop

echo " "
echo "---"
echo "Done. First run 'source venv/bin/activate'"
echo "Then try 'lanscatter_master --help', 'lanscatter_peer' or 'lanscatter_gui'."
