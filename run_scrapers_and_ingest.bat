@echo off
echo ================================================================
echo  SCRAPERS + INGEST - AUTOMATED SCRIPT
echo ================================================================
echo.

REM Activate virtual environment
echo [1/3] Activating virtual environment...
call .venv\Scripts\activate

REM Run scrapers
echo.
echo [2/3] Running scrapers for future events...
cd data_collection\scrapers
python run_all_future_scrapers.py

REM Go back
cd ..\..

REM Ingest to database
echo.
echo [3/3] Ingesting data to database...
python ingest_all_csvs.py

REM Show results
echo.
echo ================================================================
echo  DONE!
echo ================================================================
echo.
echo Check database with: python check_database.py
echo.

pause
