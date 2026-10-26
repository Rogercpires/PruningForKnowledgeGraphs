# Validação da Etapa 6 — 26/10/2026

Base inicial: `94bce1cb666aa735ec3d7de1904a235b38cb410f`, branch ICC2027.
Evidências anteriores permanecem intactas. Os testes foram executados sobre as
correções ainda locais, antes dos commits desta etapa. Os hashes finais e a
sincronização são informados na entrega; este arquivo não tenta registrar o hash
do próprio commit que o contém.

## Execuções realizadas

| Verificação | Resultado e evidência |
|---|---|
| `python -B -m unittest discover -s icc2027-experiments/tests -v` | **88 testes, zero falhas/erros/pulados**, 59,320 s; `unit-tests.txt`. Inclui oito regressões finais, mais 80 testes existentes. |
| `ICC2027_JENA_HOME=C:\Apache\apache-jena-5.6.0`; `python -B -m unittest discover -s icc2027-experiments/tests/integration -v` | **6 testes, zero falhas/erros/pulados**, 315,969 s; `integration-tests.txt`. Dois usam Jena real e quatro verificam arquivos/referências arquivados. |
| Compatibilidade R04 | Regras e `temporal_audit.py` sem alterações. Seis políticas preservam hashes SHA-256 RDF das fixtures Etapa 3. Exceções ambíguas, modos exclude/raw-local, preflight v2 e determinismo no mesmo modo são testados. |
| Métricas e SPARQL | As 39 respostas Jena arquivadas são iguais às referências exploratórias; os dois CSVs de preservação reproduzem números/colunas a dez casas de comparação numérica. Aggregation, Top-K e cinco sementes mantêm valores esperados. |
| Runner | Backend simulado cobre warm-up de geração, cinco repetições, randomização/blocos, capacidade, exclusão de warm-up, falhas, timeout, interrupção, retomada e drift; teste real cria 24 fluxos e duas TDB2 novas, faz carga, warm-up de consulta, medições e no-op resume. SPARQL inválida dá execution_error/result null. |
| CLI nova | `plan` e `run` com seletores e repetições são exercitados; Baseline automático, spec original preservado, IDs inválidos rejeitados e estimativas inalteradas. Execução dessa regressão usa backend simulado, explicitamente. |
| Replay manual | `evaluate_queries.py report --input .../reports/stage3-jena-r04 --output .../evaluations/stage6-replay-20261026`: sucesso, 13 políticas. Resumo em `validation-summary.json`; saída completa local ignorada pelo Git. |
| Bases existentes | `load_plan(..., require_stores=True)` encontrou os 13 caminhos e manifestos esperados. Não foram reexecutadas consultas CICIDS2017 nesta etapa. |
| Planos secos | `full-plan.json`: 180 tarefas, 150 medidas, orçamento 1.556.925.644.800 bytes, insufficient no volume C. `batch-plan.json`: Baseline + aggregate-60s, zero warm-ups de geração e cinco repetições, 10 tarefas, 96.636.764.160 bytes, sufficient na observação. Planos só leem nomes/tamanhos dos CSVs; não fazem hash completo, RDF ou Jena. |
| Sintaxe | Todos os módulos src passam no parser AST Python sem gerar bytecode; os nove blocos PowerShell em STAGE6_OPERATIONS.md passaram no parser PowerShell, zero erros. Isso não significa execução das campanhas descritas. |
| Cobertura | Nova execução dos 88 testes unitários com `trace` da biblioteca padrão: 88,24% das linhas executáveis dos cinco módulos Python alterados, mínimo por módulo 82,24%; `coverage.json`. Não é cobertura de branches e não inclui linhas de subprocessos. Todos os testes dessa segunda execução também passaram. |
| Preservação | Diff sem alterações em regras, normalizador, seis consultas, arquivos das Etapas 3 e 4; inventário desses caminhos em validation-summary.json. O artigo submetido não foi editado. |

## Novas regressões

`tests/test_stage6_audit.py` confirma o defeito CP1252/mojibake e sua correção,
rejeita rótulos incorretos na grade, distingue overlap fixo/recall, verifica
preflight sem dupla contagem, separa probabilidades no resumo e CSV, exercita
seleção de lotes/CLI e demonstra a dependência da amostra em relação à elegibilidade.
Casos de erro deliberados geram mensagens de falha no log mesmo com o teste aprovado.

## Limites

Nenhum benchmark completo CICIDS2017, nova geração completa, nova carga completa
ou nova campanha de desempenho foi executado. Os tempos dos testes não são
resultados científicos. As referências numéricas são da campanha anterior.
Capacidade é uma observação pontual, não reserva de espaço nem garantia futura.
O guard continua impedindo a matriz completa no disco observado. Warm-up/cache,
replicação, overhead e limites das três consultas continuam conforme
STAGE6_AUDIT_DISPOSITION.md e STAGE6_RESEARCH_SUMMARY.md.

Não foram adicionados dados de tráfego, RDF gerado, bases TDB2 ou diretórios
temporários ao Git: somente código, configuração, documentação e evidências
compactas. Não há nova dependência Python ou mudança de algoritmo/R04.
