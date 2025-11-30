# PowerShell script to run CDK commands in Docker
# Usage: .\docker-cdk.ps1 <cdk-command>
# Example: .\docker-cdk.ps1 synth
# Example: .\docker-cdk.ps1 deploy

param(
    [Parameter(Mandatory=$false)]
    [string[]]$Command = @("--version")
)

# Check if Docker is running
$dockerRunning = docker ps 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Build the image if it doesn't exist or if Dockerfile changed
Write-Host "Building/updating CDK Docker image..." -ForegroundColor Yellow
docker compose build --quiet

# Run the CDK command
Write-Host "Running: cdk $($Command -join ' ')" -ForegroundColor Green
docker compose run --rm cdk cdk $Command

