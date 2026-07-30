@echo off
set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
cd /d "C:\Bi g Data\Kafka\healthcare-pipeline"
if "%~1"=="" (
  "%PY%" produce_ccda.py
) else (
  "%PY%" produce_ccda.py "%~1"
)
