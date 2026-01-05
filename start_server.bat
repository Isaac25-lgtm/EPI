@echo off
echo Stopping all Python processes...
taskkill /F /IM python.exe 2>nul
timeout /t 2 /nobreak >nul

echo Starting Flask server...
C:\Users\USER\AppData\Local\Programs\Python\Python312\python.exe app.py

pause

