@echo off
echo 🔄 Loading Docker image...
docker load < crm-data-extract-app.tar

echo 🚀 Starting CRM Data Extracting app...
docker run -p 8501:8501 crm-data-extract-app
pause
