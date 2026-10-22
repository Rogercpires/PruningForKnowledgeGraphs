[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $RunId,
    [Parameter(Mandatory)] [string] $Endpoint,
    [Parameter(Mandatory)] [string] $QueryDirectory,
    [Parameter(Mandatory)] [string] $OutputCsv,
    [ValidateRange(1, 100)] [int] $Warmups = 1,
    [ValidateRange(1, 100)] [int] $Repetitions = 5
)

$ErrorActionPreference = 'Stop'
$queryPath = (Resolve-Path $QueryDirectory).Path
if (Test-Path -LiteralPath $OutputCsv) { throw "Refusing to overwrite existing observations: $OutputCsv" }

$records = [System.Collections.Generic.List[object]]::new()
Get-ChildItem -LiteralPath $queryPath -Filter '*.rq' -File | Sort-Object Name | ForEach-Object {
    $queryName, $query = $_.Name, (Get-Content -Raw -LiteralPath $_.FullName)
    for ($trial = 1; $trial -le ($Warmups + $Repetitions); $trial++) {
        $watch = [Diagnostics.Stopwatch]::StartNew()
        try {
            $response = Invoke-WebRequest -Method Post -Uri $Endpoint -ContentType 'application/sparql-query' -Body $query -UseBasicParsing
            $status, $error = $response.StatusCode, ''
        } catch {
            $status, $error = 0, $_.Exception.Message
        }
        $watch.Stop()
        $records.Add([pscustomobject]@{
            run_id = $RunId; query = $queryName; phase = if ($trial -le $Warmups) { 'warmup' } else { 'measurement' }
            trial = $trial; status_code = $status; elapsed_ms = $watch.Elapsed.TotalMilliseconds
            timestamp_utc = [DateTime]::UtcNow.ToString('o'); error = $error
        })
    }
}
$records | Export-Csv -NoTypeInformation -Encoding utf8 -LiteralPath $OutputCsv
Write-Host "Saved $($records.Count) observations to $OutputCsv"



