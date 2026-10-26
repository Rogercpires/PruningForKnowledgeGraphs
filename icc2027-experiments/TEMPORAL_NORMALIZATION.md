# Normalização temporal auditável — modo inicial `audit-only`

## Escopo e garantia de não intervenção

`src/temporal_audit.py` é uma etapa de diagnóstico independente do gerador RDF. Ele abre os oito CSVs canônicos apenas para leitura e cria um diretório novo de evidências; recusa reutilizar um diretório já existente. Não altera CSVs, RDF, manifests experimentais, TDB2 ou medições de Fuseki. Portanto, este modo não executa Baseline, Aggregation Pruning nem Temporal Pruning.

Os timestamps são tratados como **horários civis locais sem offset**. A origem não estabelece um fuso horário verificável; por isso, esta etapa não usa o sufixo `Z`, não calcula epoch e não afirma instantes UTC. Para a futura agregação de 60 s, o valor determinístico proposto é o arredondamento para baixo do horário civil local (`YYYY-MM-DDTHH:MM:00` para buckets de 60 s). Uma conversão para UTC só pode ser ativada mediante convenção temporal documentada e justificada.

## Regras propostas

As regras estão versionadas em `configs/temporal-normalization-rules.json`. A versão inicial é deliberadamente estreita:

| Arquivos | Condição temporal | Proposta | Confiança |
| --- | --- | --- | --- |
| Monday, Tuesday e Wednesday WorkingHours | horário bruto de 01h a 05h | adicionar 12 h, apenas se o resultado estiver entre 13h e 17h | alta |
| Thursday Afternoon Infilteration e Friday Afternoon (PortScan, DDoS) | horário bruto de 01h a 05h | adicionar 12 h, apenas se o resultado estiver entre 13h e 17h | alta |
| Demais horários ou arquivos sem regra explícita | qualquer | preservar valor original | preservado |
| Timestamp incompatível com D/M/Y documentado | qualquer | sem proposta | inválido |

A condição depende somente do arquivo/sessão e do relógio local. Ela não usa `Label`, `Flow ID`, posição da linha ou a presença anterior de 12h. Os arquivos com a sessão explícita `Afternoon` são tratados pela sua identidade canônica de sessão. Em particular, registros de 08h–11h depois de uma linha de 12h continuam preservados: a não ordenação do CSV é medida como evidência, mas nunca aciona correção.

### Exceções R04 da quarta-feira

A versão `2026-10-22.2-r04` introduz sete exceções estritas para `Wednesday-workingHours.pcap_ISCX.csv`, identificadas por arquivo, número de linha e texto original do timestamp. Elas são avaliadas antes da regra geral 01h–05h:

- linhas 6.560, 26.702–26.705 (`02:24`/`02:25`): `preserved` com confiança `provisional`; a documentação complementar de Engelen, Rimmer e Joosen informa janela Slowloris de 02:23:00 AM a 10:12:59 AM em 05/07/2017;
- linhas 55.504 e 55.523 (`01:43`): `ambiguous`, sem timestamp ou bucket proposto, pois a inspeção não fornece evidência temporal independente para decidir entre 01:43 e 13:43.

O rótulo foi usado somente para a explicação humana da auditoria, não como condição de código. A R04 está resumida em `AUDITORIA_TEMPORAL_R04.md`.

O fundamento é a documentação de coleta em horário de trabalho e a análise posterior do dataset, que descreve ambiguidades de formato e exemplos de horários de tarde registrados sem AM/PM. A regra não afirma recuperar entidades de fluxo individuais; ela somente produz uma hipótese explícita para o timestamp agregado.

## Saídas de uma auditoria

O comando cria, em diretório novo:

- `temporal-audit.csv.gz`: CSV completo, uma linha por registro, com arquivo, linha, Flow ID, timestamp original, proposta, bucket local, regra, classificação e elegibilidade. É compactado para não duplicar centenas de megabytes do dataset; seu conteúdo é CSV UTF-8 e possui ordem determinística.
- `temporal-differences.csv`: subconjunto legível com registros corrigidos, ambíguos ou inválidos.
- `wednesday-pre-noon-neighborhoods.csv`: as linhas vizinhas (±2) dos registros de 01h–05h localizados antes da primeira observação bruta de 12h na quarta-feira, incluindo identificadores de comunicação. Essa seleção é somente investigativa.
- `summary.json`: totais por arquivo, indicadores de não ordenação, classificação, versão e SHA-256 das regras, convenção temporal e verificação dos invariantes do dataset.

As classificações são:

- `corrected` / `high`: há uma proposta configurada por sessão de arquivo, sem depender do rótulo;
- `preserved`: o horário bruto foi mantido;
- `ambiguous`: uma hipótese não pode ser escolhida de forma autorizada;
- `invalid`: a data/hora não seguiu os formatos suportados.

## Execução e validação

```powershell
Set-Location 'C:\Users\roger.pires\OneDrive\Documentos\Meus Documentos\Documentos Roger\Meus Cursos\Documentos UFRGS\ICC 2027\PruningForKnowledgeGraphs-ICC2027\icc2027-experiments'
python -m unittest discover -s tests -p 'test_*.py'

.\scripts\run-temporal-audit.ps1 `
  -CsvRoot 'D:\UFRGS\Extras\Gerencia de Redes\Dataset\CIC-IDS-2017\CSVs\GeneratedLabelledFlows\TrafficLabelling' `
  -OutputDir '.\temporal-audit\audit-20261022-r01'
```

Antes de qualquer ativação no pipeline, revisar `temporal-differences.csv`, os sete casos da quarta-feira em `wednesday-pre-noon-neighborhoods.csv` e os três invariantes em `summary.json`: 2.830.743 fluxos elegíveis, 557.646 ataques rotulados e 14 classes de ataque. Uma divergência é um resultado de auditoria a investigar, não uma justificativa para alterar contagens.

O CSV de Web Attacks pode conter linhas estruturais completamente vazias depois dos registros de fluxo. Elas permanecem na trilha de auditoria como `invalid`, não são elegíveis e não podem ser reinterpretadas como timestamps ausentes de fluxos reais.

## Limitações e próximo ponto de decisão

Esta reconstrução não prova o fuso horário, não estabelece ordem cronológica de linhas e não corrige todos os relógios de 12 horas. O conjunto de regras atual é uma proposta conservadora e auditável. Sua ativação no gerador RDF exige uma revisão explícita das diferenças, uma decisão documentada sobre fuso horário e novos testes de equivalência do Baseline. Tempos desta auditoria não são dados de benchmark.

