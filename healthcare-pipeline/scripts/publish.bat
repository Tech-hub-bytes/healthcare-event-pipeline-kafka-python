@echo off
set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
cd /d "C:\Bi g Data\Kafka\healthcare-pipeline"
set DATABRICKS_CONFIG_PROFILE=dbc-7c3eed4c
"%PY%" publish_to_databricks.py
if errorlevel 1 exit /b 1
"%PY%" refresh_chatbot.py
exit /b 0
