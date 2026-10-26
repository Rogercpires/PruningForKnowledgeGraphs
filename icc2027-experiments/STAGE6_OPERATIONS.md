# Operação após a auditoria — PowerShell

Executar na raiz do checkout `PruningForKnowledgeGraphs-ICC2027`, branch ICC2027,
com Python 3.12, Java e Jena disponíveis. Estes exemplos usam as interfaces reais.
Os caminhos podem ser substituídos. Cada saída deve ser nova; nunca reutilizar um
diretório de saída existente. Nenhum comando de campanha abaixo foi disparado
automaticamente na Etapa 6.

## Preparação comum

```powershell
$pkg = Join-Path (Get-Location).Path 'icc2027-experiments'
$jena = 'C:\Apache\apache-jena-5.6.0'
$runs = 'C:\UFRGS\ICC2027_Runs'
$stores = 'C:\UFRGS\ICC2027_TDB2'
$csv = 'D:\UFRGS\Extras\Gerencia de Redes\Dataset\CIC-IDS-2017\CSVs\GeneratedLabelledFlows\TrafficLabelling'
$spec = Join-Path $pkg 'configs\benchmark-sensitivity-r04.json'
$outRoot = 'C:\UFRGS\ICC2027_Stage6_Operations'
New-Item -ItemType Directory -Path $outRoot -Force | Out-Null
$id = (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N')
```

Confirme a localização dos oito CSVs canônicos antes de usar `$csv`.
`$runs` e `$stores` devem conter os nomes/caminhos do registro
`configs/evaluation-r04.json`; editar uma cópia versionada desse registro se os
nomes forem outros. As consultas são somente sobre bases existentes: não geram
RDF, não fazem nova carga e não devem concorrer com um escritor Jena na mesma base.

## A. Avaliar as 13 bases TDB2 existentes

```powershell
$evaluation = Join-Path $outRoot "evaluation-$id"
python -B "$pkg\src\evaluate_queries.py" run `
  --registry "$pkg\configs\evaluation-r04.json" `
  --runs-root $runs --tdb-root $stores --jena-home $jena `
  --jvm-arg=-Xmx4g --timeout 300 --output $evaluation
if ($LASTEXITCODE -ne 0) { throw 'Avaliação incompleta: conferir logs e status; erros não são respostas vazias.' }
```

São três consultas por política e uma contagem de triplas distintas por base.
Para omitir apenas esta contagem adicional, usar `--skip-stored-count`. Saída:
`bundle.json`, stdout/stderr originais, metadados, `metrics.json`, CSVs e relatório
de tamanho **lógico dos arquivos**, não ocupação física do SSD. Há timeout por
consulta. `run` já produz os relatórios do item B.

## B. Reproduzir métricas e preservação sem Jena

```powershell
# Pode apontar também para $evaluation do item A.
$saved = Join-Path $pkg 'reports\stage3-jena-r04'
$replay = Join-Path $outRoot "replay-$id"
python -B "$pkg\src\evaluate_queries.py" report --input $saved --output $replay
if ($LASTEXITCODE -ne 0) { throw 'Replay inválido: conferir integridade e resultados.' }
```

Reproduz respostas e manifestos salvos com verificação de hashes. Não apresenta
tempos antigos como novos. Métricas JSON schema 2: overlap tem cota fixa N e recall
separado; diferentes probabilidades de Sampling têm resumos separados. Não
agregar diferentes sementes e repetições de desempenho como se fossem a mesma dimensão.

## C. Teste pequeno de benchmarking com Jena real

```powershell
$env:ICC2027_JENA_HOME = $jena
python -B -m unittest discover `
  -s "$pkg\tests\integration" -p test_benchmark_jena.py -v
if ($LASTEXITCODE -ne 0) { throw 'Falha no teste pequeno do runner.' }
```

Esse teste cria 24 fluxos sintéticos em diretório temporário e duas bases pequenas,
verifica geração, ingestão, warm-up de consulta, medição, validação, relatórios e
retomada sem duplicação. Não usa os CSVs CICIDS2017 completos. Os testes unitários
do runner exercitam também warm-up de geração, cinco repetições, interrupção e
retentativa com backend explicitamente simulado. Não são medições científicas.

## D. Repetição controlada: Baseline e Aggregation 60s

```powershell
$plan = Join-Path $outRoot "performance-plan-$id.json"
$campaign = Join-Path $outRoot "performance-$id"
python -B "$pkg\src\benchmark_runner.py" plan --spec $spec --csv-root $csv `
  --config-id aggregate-60s --measured-repetitions 5 --warmup-repetitions 0 `
  --output $plan
if ($LASTEXITCODE -ne 0) { throw 'Plano inválido.' }
$design = Get-Content -Raw $plan | ConvertFrom-Json
if (-not $design.storage_budget.sufficient) { throw 'Capacidade insuficiente: mudar destino ou escopo antes de executar.' }
# Inicia carga real de dados. Executar somente quando a campanha for desejada.
python -B "$pkg\src\benchmark_runner.py" run --spec $spec --csv-root $csv `
  --config-id aggregate-60s --measured-repetitions 5 --warmup-repetitions 0 `
  --jena-home $jena --jvm-arg=-Xmx4g --output $campaign
if ($LASTEXITCODE -ne 0) { throw 'Campanha incompleta; preservar diretório e inspecionar state.json.' }
```

O seletor mantém Baseline: 10 tarefas de geração/carga, cinco por configuração.
O protocolo desta execução declara zero warm-ups de geração; conserva um warm-up
e cinco medições por consulta do spec padrão. Para incluir um warm-up de geração
por política, mudar `--warmup-repetitions` para 1 **nos dois comandos**, reavaliar
capacidade e registrar como outro protocolo. Cinco repetições são ponto de partida,
não justificativa universal de precisão. Alterar esses flags não altera a semente
de Sampling. O spec resolvido, spec original e argv ficam na campanha.

O orçamento conservador deste lote é 90 GiB (10 x 8 GiB + 10 GiB de reserva),
sem garantia do consumo final. O guard volta a verificar espaço durante a execução.
Guardar logs e medições individuais; não descartar outliers automaticamente.
O perfil exige checkout limpo. Fonte, regras, entradas e ambiente ficam congelados
e verificados para retomada. Novos processos Java não limpam cache do SO.

```powershell
# Para interrupção; não apaga tentativas anteriores.
python -B "$pkg\src\benchmark_runner.py" resume --campaign $campaign
# Se o estado for failed, a retentativa é explicitamente autorizada por este flag:
python -B "$pkg\src\benchmark_runner.py" resume --campaign $campaign --retry-failed
python -B "$pkg\src\benchmark_runner.py" summarize --campaign $campaign
```

## E. Sensibilidade de Sampling, Top-K e Temporal

Planejar a matriz completa sem executar geração/carga:

```powershell
$fullPlan = Join-Path $outRoot "sensitivity-plan-$id.json"
python -B "$pkg\src\benchmark_runner.py" plan --spec $spec --csv-root $csv --output $fullPlan
if ($LASTEXITCODE -ne 0) { throw 'Plano de sensibilidade inválido.' }
Get-Content -Raw $fullPlan | ConvertFrom-Json | Select-Object task_count,storage_budget
```

A matriz padrão inclui p=0,005/0,01/0,05/0,10, sementes 1001–1005, K=10/20/50,
janelas 6/12/24/48h, Baseline, Labels e Aggregation 60s: 30 configurações, 180
tarefas com um warm-up e cinco repetições. Exige orçamento de aproximadamente
1,42 TiB; não cabe no volume C observado na auditoria. Provisionar destino adequado
ou escolher lotes. O seguinte exemplo prepara **um lote**, com as mesmas cinco
repetições e sem warm-up de geração de D:

```powershell
$case = 'sample-p0d01-s1001'
# Outros IDs válidos: sample-p0d005-s1001, sample-p0d05-s1005,
# sample-p0d1-s1005, topk-10, topk-20, topk-50,
# temporal-6h, temporal-12h, temporal-24h, temporal-48h.
$batchPlan = Join-Path $outRoot "sensitivity-$case-plan-$id.json"
$batchRun = Join-Path $outRoot "sensitivity-$case-$id"
python -B "$pkg\src\benchmark_runner.py" plan --spec $spec --csv-root $csv `
  --config-id $case --measured-repetitions 5 --warmup-repetitions 0 --output $batchPlan
if ($LASTEXITCODE -ne 0) { throw 'Lote inválido.' }
$batchDesign = Get-Content -Raw $batchPlan | ConvertFrom-Json
if (-not $batchDesign.storage_budget.sufficient) { throw 'Sem espaço: não iniciar o lote.' }
# Iniciar futuramente, após revisar o plano e garantir armazenamento cumulativo:
python -B "$pkg\src\benchmark_runner.py" run --spec $spec --csv-root $csv `
  --config-id $case --measured-repetitions 5 --warmup-repetitions 0 `
  --jena-home $jena --jvm-arg=-Xmx4g --output $batchRun
if ($LASTEXITCODE -ne 0) { throw 'Lote incompleto; preservar evidências.' }
```

Repetir explicitamente para cada configuração/semente desejada, com novo ID e
destino, mantendo condições comparáveis. Baseline é retido em cada lote. Lotes
distintos **não equivalem** a uma única randomização global: registrar ordem,
período, host, cache e bloco; não misturar tempos entre protocolos distintos.
Nenhum resultado é apagado automaticamente para liberar espaço. Planejar retenção
e armazenamento cumulativos antes de iniciar uma série de lotes. Se houver
capacidade para a matriz inteira, usar `run` com o mesmo spec e sem seletores,
em um novo diretório naquele volume; primeiro repetir o `plan` nesse destino.

## Validação geral e interpretação

```powershell
python -B -m unittest discover -s "$pkg\tests" -v
$env:ICC2027_JENA_HOME = $jena
python -B -m unittest discover -s "$pkg\tests\integration" -v
```

Tempos sintéticos apenas validam infraestrutura. Resultados de referência são da
campanha anterior e estão preservados; novos resultados científicos só existirão
após a campanha real. Consultar BENCHMARKING.md para definições de durações,
estatísticas, intervalos de confiança condicionais e limites metodológicos.
