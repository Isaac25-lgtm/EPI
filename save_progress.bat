@echo off
echo ========================================
echo     SAVING YOUR PROGRESS TO GITHUB
echo ========================================
echo.

cd /d "F:\MY FILES\DATA SCIENCE\AG\anc_dashboard"

echo Adding all changes...
git add -A

echo.
echo Committing changes...
git commit -m "Auto-save: %date% %time%"

echo.
echo Pushing to GitHub...
git push origin main

echo.
echo ========================================
echo     PROGRESS SAVED SUCCESSFULLY!
echo ========================================
pause




