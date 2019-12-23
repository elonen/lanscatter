#!/bin/bash
if [ ! -e venv ]; then
  python3.7 -m venv venv
fi
source venv/bin/activate || { echo "Venv activation failed."; exit 1; }
pip install -r requirements.txt
pytest || { echo "Tests failed, will not continue with pyinstaller."; exit 1; }
python ./setup.py develop
python ./setup.py pyinstaller -- --noconsole --onefile --add-data 'hmq.png:.' --icon hmq.ico
