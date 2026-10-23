# Integração da normalização temporal R04 ao pipeline

## Modos e política para ambiguidade

`icc2027_pipeline.py` agora recebe `--temporal-mode raw|normalized` e usa exclusivamente a camada de interpretação de `temporal_audit.py`.

- `raw`: preserva o timestamp civil bruto, sem aplicar regras R04. Serve apenas como controle histórico-comparativo; não declara que o valor seja UTC.
- `normalized`: aplica `temporal-normalization-rules.json` versão `2026-10-22.2-r04` a todos os leitores usados por Baseline, Temporal, Labels, Sample, Top-K e Aggregation.

A política padrão de ambiguidade em `normalized` é `exclude`: as duas linhas R04 ambíguas não recebem timestamp, não entram na geração RDF e são contabilizadas no manifesto como `ambiguous_excluded`. A alternativa explícita `raw-local` mantém o horário bruto local para uma análise de sensibilidade, mas não é a configuração recomendada para resultados principais.

Nenhuma regra depende de `Label`, Flow ID, IP ou ordem da linha. As sete exceções de Wednesday são identificadas por `(arquivo, linha, timestamp_original)` no arquivo versionado de regras.

## Convenção RDF atualizada

| Propriedade anterior | R04 integrada | Motivo |
| --- | --- | --- |
| `ex:timestamp` com sufixo `Z` | `xsd:dateTime` civil local, sem offset | o dataset não estabelece fuso horário verificável |
| `ex:timestampEpoch` | removida | não há base para declarar epoch absoluto |
| `ex:bucketStart` inteiro epoch | `xsd:dateTime` local | bucket determinístico de 60 s, calculado por arredondamento civil local |
| `ex:firstSeen`, `ex:lastSeen` inteiros epoch | `xsd:dateTime` local | preserva ordenação sem afirmar UTC |

Os identificadores SHA-256 de agregados continuam determinísticos. A chave continua semanticamente equivalente a `(bucket_start, source_ip, destination_ip, protocol, destination_port)`, mas `bucket_start` agora é o texto local ISO sem offset.

## Manifesto e tempos

O manifesto versão 2 inclui `temporal_interpretation` com modo, política de ambiguidade, versão e SHA-256 das regras, convenção temporal, contagens de timestamps e, em Temporal Pruning, `reference_local` e `cutoff_local`. A referência é o maior horário civil local elegível segundo o modo/política; a janela é calculada como `reference_local - window_hours`.

As métricas separam `input_verification_seconds` (hash e metadados de CSV), `temporal_preflight_seconds`, `generation_seconds` (emissão RDF) e `total_seconds`. Essas medidas de geração não são benchmarks de Fuseki.

## Validação reduzida executada

A suíte de 10 testes criou CSVs sintéticos para os oito nomes canônicos e executou as seis políticas. Ela verificou:

- transição não cronológica de meio-dia preservada;
- exceção legítima de madrugada preservada;
- exclusão das duas linhas ambíguas sob `exclude`;
- comparação Baseline: 11 fluxos em `raw` e 10 em `normalized` no fixture;
- buckets de 60 s, URIs determinísticas e distribuição de rótulos;
- leitura dos N-Triples emitidos, terminação ` .`, ausência de `timestampEpoch` e ausência de sufixo `Z` nos literais temporais.

Os invariantes completos continuam estabelecidos pela auditoria R04: 2.830.743 fluxos elegíveis, 557.646 ataques e 14 classes. O dataset completo não foi reprocessado nesta etapa.

## Comandos para futuras execuções completas

Somente após aprovação metodológica, use identificadores novos sob `C:\UFRGS\ICC2027_Runs`:

```powershell
Set-Location 'C:\Users\roger.pires\OneDrive\Documentos\Meus Documentos\Documentos Roger\Meus Cursos\Documentos UFRGS\ICC 2027\PruningForKnowledgeGraphs-ICC2027\icc2027-experiments'

.\scripts\run-experiment.ps1 `
  -Config .\configs\baseline.json `
  -CsvRoot 'D:\UFRGS\Extras\Gerencia de Redes\Dataset\CIC-IDS-2017\CSVs\GeneratedLabelledFlows\TrafficLabelling' `
  -OutputRoot 'C:\UFRGS\ICC2027_Runs' `
  -RunId 'baseline-normalized-r01' `
  -TemporalMode normalized -AmbiguousPolicy exclude

.\scripts\run-experiment.ps1 `
  -Config .\configs\aggregate-60s.json `
  -CsvRoot 'D:\UFRGS\Extras\Gerencia de Redes\Dataset\CIC-IDS-2017\CSVs\GeneratedLabelledFlows\TrafficLabelling' `
  -OutputRoot 'C:\UFRGS\ICC2027_Runs' `
  -RunId 'aggregate60-normalized-r01' `
  -TemporalMode normalized -AmbiguousPolicy exclude
```

Cada `RunId` deve ser inédito; o script recusa diretórios existentes. Esses comandos não devem ser executados como parte desta entrega.
