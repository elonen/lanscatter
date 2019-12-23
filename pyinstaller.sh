#!/bin/bash
if [ ! -e venv ]; then
  python3.7 -m venv venv
  pip install -r requirements.txt
fi
source venv/bin/activate
pytest || { echo "Tests failed, will not continue with pyinstaller."; exit 1; }
python ./setup.py develop
python ./setup.py pyinstaller -- --windowed --paths venv/lib/python3.7/site-packages  --add-data 'hmq.png:.'
