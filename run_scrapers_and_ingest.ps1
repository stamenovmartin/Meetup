# PowerShell script for running scrapers and ingesting data
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host " SCRAPERS + INGEST - AUTOMATED SCRIPT" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# Activate virtual environment
Write-Host "[1/3] Activating virtual environment..." -ForegroundColor Yellow
& .\.venv\Scripts\Activate.ps1

# Run scrapers
Write-Host ""
Write-Host "[2/3] Running scrapers for future events..." -ForegroundColor Yellow
Set-Location -Path "data_collection\scrapers"
python run_all_future_scrapers.py

# Go back to root
Set-Location -Path "..\..\"

# Ingest to database
Write-Host ""
Write-Host "[3/3] Ingesting data to database..." -ForegroundColor Yellow
python ingest_all_csvs.py

# Show results
Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host " DONE!" -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Check database with: python check_database.py" -ForegroundColor Cyan
Write-Host ""

# Wait for user
Read-Host "Press Enter to exit"
