[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Config,
    [Parameter(Mandatory)] [string] $CsvRoot,
    [Parameter(Mandatory)] [string] $RunId,
    [string] $OutputRoot = (Join-Path $PSScriptRoot '..\runs'),
    [ValidateSet('raw', 'normalized')] [string] $TemporalMode = 'raw',
    [ValidateSet('exclude', 'raw-local')] [string] $AmbiguousPolicy = 'exclude',
    [string] $TemporalRules = (Join-Path $PSScriptRoot '..\configs\temporal-normalization-rules.json')
)

$ErrorActionPreference = 'Stop'
$project = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$configPath = (Resolve-Path $Config).Path
$csvPath = (Resolve-Path $CsvRoot).Path
$outputRootPath = if ([IO.Path]::IsPathRooted($OutputRoot)) {
    [IO.Path]::GetFullPath($OutputRoot)
} else {
    [IO.Path]::GetFullPath((Join-Path $project $OutputRoot))
}
$outputPath = Join-Path $outputRootPath $RunId

if (Test-Path -LiteralPath $outputPath) {
    throw "Run directory already exists: $outputPath. Choose a new -RunId; prior evidence is immutable."
}

New-Item -ItemType Directory -Force -Path $outputRootPath | Out-Null
& python (Join-Path $project 'src\icc2027_pipeline.py') `
    --config $configPath --csv-root $csvPath --output-dir $outputPath --run-id $RunId `
    --temporal-mode $TemporalMode --temporal-rules $TemporalRules --ambiguous-policy $AmbiguousPolicy
if ($LASTEXITCODE -ne 0) { throw "Generator failed with exit code $LASTEXITCODE" }

Write-Host "Manifest written to $(Join-Path $outputPath 'manifest.json')"



