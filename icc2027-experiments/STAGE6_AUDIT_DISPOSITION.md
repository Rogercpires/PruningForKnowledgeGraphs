# Fechamento da auditoria independente R04 — Etapa 6

Auditoria consultada: `AUDITORIA_FINAL_R04_ANTIGRAVITY.md`, do Antigravity,
base `94bce1cb666aa735ec3d7de1904a235b38cb410f`.
SHA-256 do documento recebido:
`f88e740839269b84b485f95900eeb071ddb01e4164f0a457a22efaf88e5cb2f0`.
O original está fora do Git, em
`C:\Users\roger.pires\.gemini\antigravity\brain\f5f9cae1-5af6-4b80-9aa1-87c90054c165\AUDITORIA_FINAL_R04_ANTIGRAVITY.md`.
Este registro distingue defeitos, restrições operacionais e escolhas metodológicas.
Não implica execução de nova campanha CICIDS2017 nem revisão do artigo submetido.

## Disposição de cada achado

| Achado | Evidência e procedência | Tratamento e validação |
|---|---|---|
| B1: rótulos Web Attack corrompidos | Confirmado no JSON de sensibilidade: U+00E2/U+20AC/U+201C em vez de U+2013. `normalized_label` não pode recuperar esse erro de decodificação. | Rótulos copiados da configuração canônica, serializados com escapes Unicode. O gerador da grade CICIDS2017 rejeita rótulos desconhecidos. Regressão CP1252: versão corrompida retém somente DDoS; versão correta retém DDoS e os três Web Attack. Configurações explícitas continuam permitindo outros vocabulários; o novo guard se aplica à grade CICIDS2017. |
| B2: orçamento de disco da matriz | Restrição real de capacidade, não defeito do guard. 180 tarefas, 8 GiB por tarefa e 10 GiB de reserva exigem 1.556.925.644.800 bytes. | `plan` e `run` aceitam `--config-id` repetível, `--measured-repetitions` e `--warmup-repetitions`. Cada lote retém Baseline automaticamente, orçamento conservador, fonte original e configuração resolvida. Testes validam seleção, CLI e execução sintética. A matriz completa continua bloqueada em qualquer destino sem capacidade; lotes retidos também consomem espaço cumulativo. Não reduzimos estimativas artificialmente nem eliminamos resultados. |
| I1: overlap em rankings menores que N | Comportamento confirmado, já definido em EVALUATION.md como cota fixa N; não é recall relativo ao Baseline. | Mantido `overlap = comuns/N` para compatibilidade histórica; JSON agora explicita denominador/definição e acrescenta `baseline_recall = comuns/tamanho(topN(B))`. Para dois rankings idênticos de 2 itens com N=20: overlap=0,1, recall=1. Baseline vazio: indefinidos. Regressão para esse caso. CSV anterior mantém sua definição. |
| I2: instrumentação por fluxo | Confirmado uso de context managers por registro e inclusão de seu custo no tempo medido. | Mantido perfil compatível para todas as políticas. Os 13,18 microssegundos e 110–140 segundos são sonda/extrapolação da auditoria, não medições novas do pipeline completo. Não subtraímos esse número dos tempos. Uma campanha de throughput nativo requer perfil separado, calibração local e novo identificador de protocolo. |
| I3: PRNG depende da elegibilidade | Confirmado: `rows()` filtra antes do sorteio Bernoulli. Inclusão de um registro altera a associação dos sorteios seguintes aos fluxos. | Preservado algoritmo para não invalidar sementes e arquivos históricos. Regressão verifica reprodução no mesmo modo e mudança ao alternar exclude/raw-local. Mesma semente só identifica a mesma amostra se entradas, elegibilidade, ordem e versão forem idênticas. Comparações entre modos não são amostras pareadas idênticas. |
| I4: preflight e resíduos legados | Confirmados ambíguos excluídos contados como elegíveis e timestamp inválido contado duas vezes; ambos eram metadados, não seleção. Scripts legados continuam fisicamente executáveis, embora fora do runner atual. | Preflight usa o timestamp retornado por `temporal_value`, conta inválidos uma vez e acrescenta `counting_version: 2`. Testes cobrem exclude/raw-local e rótulos de ataque. Manifestos anteriores permanecem intactos. Regressões SHA-256 das seis políticas verificam RDF inalterado. Usar somente as interfaces atuais; scripts legados não foram removidos nem apresentados como desabilitados no SO. |
| Opcional 1: mistura de probabilidades | Confirmado: avaliador independente agrupava todos os samples; runner já estratificava. | Corrigido no avaliador: `sampling_statistics_by_probability` e CSV próprio. Em probabilidades mistas, resumo agregado `sampling_statistics=null` com motivo explícito. Para probabilidade única, números e CSV anterior são preservados. JSON de métricas passa a versão 2; bundles de entrada permanecem versão 1. Testes de mistura, replay e referências históricas. |
| Opcional 2: consulta sem LIMIT para empates | Limitação confirmada: LIMIT 20 não revela o 21º destino. | Não aplicada ao workload histórico: retirar LIMIT mudaria custo, respostas e contrato do benchmark. `tie_expanded_overlap` permanece null com motivo; `top_with_ties` serve para distribuições completas. Criar futuramente workload separado e versionado com estudo de custo. Não afirmar preservação de todos os empates com essas três consultas. |
| Opcional 3: UCASE em BENIGN | Robustez potencial para outros vocabulários, sem defeito demonstrado no contrato CICIDS2017 canônico. | Não aplicada às consultas existentes para preservar workload e plano de comparação. O contrato exige BENIGN em maiúsculas; validação das respostas verifica populações. Para fontes externas, validar/normalizar rótulos ou versionar consulta própria antes de comparar tempos. |

## Interpretação científica

As fórmulas auditadas de redução, instâncias/classes, TVD, L1, Horvitz–Thompson,
quantis e Student-t foram mantidas. L1 sobre contagens brutas expressa perda de
população e continua útil; TVD trata composição normalizada e HT estima totais.
TVD pequeno não significa preservação perfeita de toda a informação. Cinco sementes
não confirmam com precisão um modelo probabilístico nem garantem cobertura de
classes raras. A probabilidade analítica de uma classe com n instâncias desaparecer
em Bernoulli é `(1-p)^n`, sob suas hipóteses; é distinta de uma frequência observada
em cinco amostras.

R04 é uma interpretação temporal documentada, não prova da hora real de captura.
Os percentuais e a justificativa bibliográfica citados pelo auditor não foram
reverificados na fonte externa nesta etapa; não são elevados a novas conclusões.
Label-Based usa rótulos supervisionados, Top-K seleciona destinos por origem,
enquanto a consulta ordena destinos globais. Aggregation preserva exatamente o
workload de três consultas por construção, sem garantir propriedades individuais.
Não adotamos a recomendação de modificar o artigo ICC já submetido.

## Compatibilidade e limites de liberação

Sem nova dependência; Python e biblioteca padrão permanecem a arquitetura do
pipeline de pesquisa. Regras R04, algoritmos de seleção, seis consultas, configurações
de referência, manifestos e resultados experimentais anteriores são preservados.
A mudança do preflight é identificável pela versão dos contadores. Consumidores
de métricas JSON precisam aceitar schema 2 e usar as estatísticas estratificadas
quando o resumo geral for null; arquivos históricos schema 1 não são regravados.

O código pode ser liberado após os testes registrados em
`reports/stage6-validation/README.md`. A liberação do código não autoriza afirmar
que a matriz completa cabe no disco atual ou que já produziu novas medições.
Os comandos operacionais estão em [STAGE6_OPERATIONS.md](STAGE6_OPERATIONS.md);
as próximas questões científicas, em [STAGE6_RESEARCH_SUMMARY.md](STAGE6_RESEARCH_SUMMARY.md).
