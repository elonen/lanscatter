#!/bin/bash
PYTHON=python3.7
REQ=requirements.txt
ACTIVATE=venv/bin/activate
set -e

VER=$(git describe --exact-match 2> /dev/null || echo "`git symbolic-ref HEAD 2> /dev/null | cut -b 12-`-`git log --pretty=format:\"%h\" -1`")
echo "Current version string is: '$VER'"

if (uname | grep -q -E '(CYGWIN|MINGW)'); then
  echo "NOTE: Windows OS detected. Using 'python' instead of '$PYTHON'."
  PYTHON=python
  ACTIVATE=venv/Scripts/activate
fi

if (uname | grep -q -E 'Linux'); then
  echo "NOTE: Linux OS detected. Skipping installing GUI packages (failing ATM)."
  REQ=requirements.cli.txt
fi

source $ACTIVATE || { echo "Venv activation failed."; exit 1; }
pip install -r $REQ

echo " "
pytest || { echo "Tests failed, will not continue with pyinstaller."; exit 1; }
python ./setup.py develop

if (uname | grep -q -E '(CYGWIN|MINGW)'); then
  ZIPFILE="lanscatter-win32_$VER.zip"
  python setup.py pyinstaller -- --noconsole --onefile --add-data "gfx/*;gfx" --icon gfx/icon.ico
  cp dist/lanscatter_gui.exe ./
  rm -rf dist
  python setup.py pyinstaller -- --onefile
  cp dist/lanscatter_master.exe dist/lanscatter_peer.exe ./
  rm -f lanscatter-win32.zip
  python -c "import zipfile, glob; z=zipfile.ZipFile('$ZIPFILE', 'w'); [z.write(f) for f in glob.glob('*.exe')]"
  rm -- *.exe
  echo "--- DONE. Windows binaries built and packaged into $ZIPFILE"

else
  echo "--- NOTE: We are not on Windows, so binaries will stay in dist/."
  python ./setup.py pyinstaller -- --noconsole --onefile --add-data 'gfx/*:gfx' --icon gfx/icon.ico
fi

rm -rf dist
rm -f lanscatter_*.spec
