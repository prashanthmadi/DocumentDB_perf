# Setup Python virtual environment and install dependencies

Write-Host "Creating virtual environment..." -ForegroundColor Green
python -m venv .venv

Write-Host "Activating virtual environment..." -ForegroundColor Green
.\.venv\Scripts\Activate.ps1

Write-Host "Installing dependencies..." -ForegroundColor Green
pip install -r requirements.txt

Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green
Write-Host "Virtual environment is activated. To activate it manually in the future, run:" -ForegroundColor Yellow
Write-Host ".\.venv\Scripts\Activate.ps1" -ForegroundColor Cyan
