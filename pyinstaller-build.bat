IF NOT EXIST venv (python -m venv venv)
CALL venv\Scripts\activate
venv\Scripts\pip install -r requirements.txt
venv\Scripts\python ./setup.py develop
venv\Scripts\python setup.py pyinstaller -- --noconsole --onefile --add-data "hmq.png;." --icon hmq.ico
