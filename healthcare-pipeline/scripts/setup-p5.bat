@echo off
set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
cd /d "C:\Bi g Data\Kafka\healthcare-pipeline"
"%PY%" -m pip install -r requirements.txt
"%PY%" register_schemas.py
"%PY%" monitor.py
