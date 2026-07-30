@echo off
set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
cd /d "C:\Bi g Data\Kafka\healthcare-pipeline"
"%PY%" retention_job.py
