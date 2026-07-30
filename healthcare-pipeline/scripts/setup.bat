@echo off
setlocal
set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
cd /d "C:\Bi g Data\Kafka\healthcare-pipeline"
"%PY%" -m pip install -r requirements.txt
"%PY%" create_topics.py
echo.
echo Setup complete. Next:
echo   scripts\worker.bat
echo   scripts\produce.bat
echo   scripts\demo.bat
