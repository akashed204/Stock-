@echo off
schtasks /Create /TN "IB_NIFTY200_Daily_Download" /SC DAILY /ST 18:00 /TR "\"D:\Charu\IB\run_daily_download.bat\"" /F
echo Daily NIFTY 200 download task created for 18:00.
pause
