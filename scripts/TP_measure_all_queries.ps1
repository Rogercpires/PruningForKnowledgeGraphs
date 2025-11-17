###############################################################################
# Script para medir tempo de resposta SPARQL no Apache Jena Fuseki
# Autor: Roger Pires (gerado via ChatGPT)
# Executa benchmarks em 5 datasets e 3 consultas SPARQL
# Cada consulta é executada 3 vezes por dataset
# Resultados são registrados em CSV
###############################################################################

Write-Host "Iniciando medições de tempo SPARQL..." -ForegroundColor Cyan

# ================================
# CONFIGURAÇÕES
# ================================

# Diretório contendo os arquivos .sparql
$QueryDir = "CAMINHO\TemporalPruning"

# Arquivos SPARQL
$Queries = @(
    "01_top_dst_ipaddress.sparql",
    "02_count_flows_prot.sparql",
    "03_incident_per_hour.sparql"
)

# Datasets (nome lógico → URL)
$Datasets = @{
    "Baseline" = "http://localhost:3030/CICIDS2017_Temporal_baseline/query";
    "Temporal_6h"  = "http://localhost:3030/CICIDS2017_Temporal_6h/query";
    "Temporal_12h" = "http://localhost:3030/CICIDS2017_Temporal_12h/query";
    "Temporal_24h" = "http://localhost:3030/CICIDS2017_Temporal_24h/query";
    "Temporal_48h" = "http://localhost:3030/CICIDS2017_Temporal_48h/query"
}

# Número de repetições de cada consulta
$Repeticoes = 3

# Arquivo final de saída
$LogFile = "$PSScriptRoot\resultados_tempos_sparql.csv"

# Cabeçalho do CSV
"Dataset,Consulta,Execucao,Segundos" | Out-File -FilePath $LogFile -Encoding utf8


# ================================
# EXECUÇÃO DOS TESTES
# ================================

foreach ($dsName in $Datasets.Keys) {

    $endpoint = $Datasets[$dsName]

    Write-Host "`n--- Testando dataset: $dsName ---" -ForegroundColor Yellow

    foreach ($q in $Queries) {

        $QueryPath = Join-Path $QueryDir $q

        Write-Host "`n Executando consulta: $q" -ForegroundColor Green

        for ($i = 1; $i -le $Repeticoes; $i++) {

            Write-Host "  Execução $i ..." -ForegroundColor White

            $m = Measure-Command {
                curl.exe -X POST `
                    -H "Content-Type: application/sparql-query" `
                    --data-binary "@$QueryPath" `
                    $endpoint > $null
            }

            # Registrar no CSV
            "$dsName,$q,$i,$($m.TotalSeconds)" | Add-Content -Path $LogFile
        }
    }
}

Write-Host "`n===================================================="
Write-Host " Testes concluídos!"
Write-Host " Resultados salvos em: $LogFile"
Write-Host "====================================================" -ForegroundColor Cyan
