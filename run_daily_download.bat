@echo off
cd /d D:\Charu\IB

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" download_all.py --period 3mo --interval 1d
) else (
    python download_all.py --period 3mo --interval 1d
)
