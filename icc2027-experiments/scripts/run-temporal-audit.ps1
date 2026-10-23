param(
    [Parameter(Mandatory = $true)]
    [string]$CsvRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputDir,
    [string]$Rules = (Join-Path $PSScriptRoot "..\configs\temporal-normalization-rules.json"),
    [int]$BucketSeconds = 60
)

$ErrorActionPreference = "Stop"
$module = Join-Path $PSScriptRoot "..\src\temporal_audit.py"
python $module --mode audit-only --csv-root $CsvRoot --rules $Rules --output-dir $OutputDir --bucket-seconds $BucketSeconds
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

