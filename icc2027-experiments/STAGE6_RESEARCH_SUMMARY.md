# Síntese técnica da pesquisa e próxima campanha NOMS

## Implementação disponível

Pipeline Python de seis políticas (Baseline, Aggregation, Temporal, Label-Based,
Top-K e Sampling), normalização R04 versionada e exclusão explícita de registros
ambíguos. Aggregation mantém contagens por rótulo; Top-K seleciona destinos por
endereço de origem; Sampling Bernoulli conserva semente e probabilidades nominal
e efetiva. O workload de investigação tem três consultas em representações Flow
e Aggregate, executadas sobre TDB2 existente, com JSON SPARQL, saídas Jena brutas,
timeout, status e integridade por hash. Falhas não viram contagens zero.

O runner serial produz ID/diretório exclusivo, configuração original e resolvida,
manifestos, logs, tentativas imutáveis e relatórios. Captura hardware/software,
Git, JVM, hashes de regras/configurações/entradas/fonte e condições declaradas de
cache. Faz warm-up separado, repetições, ordem reproduzível e blocos configuráveis.
Não executa cargas concorrentes na mesma base. Congela contexto para retomada e
rejeita drift. O operador continua responsável por impedir escritores externos
nas bases utilizadas pelo avaliador independente.

## Métricas e tempos

Reduções de fluxos, declarações RDF emitidas e triplas distintas são separadas.
Retenção de instâncias difere de cobertura de classes e preservação por classe.
Há overlap de cota fixa N, recall relativo ao Baseline, ranking com desempate
determinístico, deslocamento dos destinos comuns, erros absoluto/relativo/L1,
TVD de distribuições e estimativas Horvitz–Thompson restritas a Bernoulli.
Indefinições são null com contagens de observações definidas/indefinidas.
Probabilidades diferentes não são agrupadas em um resumo único de Sampling.
Tamanho lógico TDB2 inclui componentes e não representa alocação física no SSD.

Instrumentação monotônica discrimina verificações, preflight, referência temporal,
seleção, Top-K, geração, validação RDF, carga TDB2, consultas e total. Durações
internas exclusivas e tempos externos inclusivos de subprocessos não são somados
como fases independentes. Instrumentação por fluxo tem custo incluído e as
múltiplas passagens Top-K/Temporal são explícitas. Estatísticas: média, mediana,
desvio amostral, extremos, quartis, IQR e CV quando definido; CI Student-t exige
justificativa explícita de suas hipóteses. Sem remoção automática de outliers.

## Resultados anteriores preservados — não são novos experimentos

As respostas e relatórios da Etapa 3 permanecem em `reports/stage3-jena-r04` e
`reports/stage3-reference-r04`. A integração da Etapa 6 recalcula as referências
a partir das respostas arquivadas, separadamente de consultas sintéticas no Jena.

| Referência anterior | Resultado exploratório |
|---|---|
| Aggregation 60s | Respostas exatas nas três consultas; TVDs 0%; Top-20 igual ao Baseline. Equivalência por construção para esse workload. |
| Top-K 20 por origem | 20/20 destinos e ranking global Top-20 idêntico; quatro ataques descartados; L1 normalizado de ataques por hora aproximadamente 0,0007%. |
| Sampling 1%, cinco sementes | TVD média protocolos aproximadamente 0,3435%; ataques/hora 1,0552%; L1 médio reescalado HT protocolos 0,7458%, ataques/hora 2,3020%; 11–12 das 14 classes presentes. |

Essas aproximações são referências de regressão, não constantes da implementação.
Testes sintéticos novos validam as correções; não acrescentam evidência científica
de desempenho ou novos resultados sobre os fluxos completos. Os rótulos corrigidos
na grade futura não reescrevem nem substituem resultados anteriores.

## Condições e riscos científicos remanescentes

- Reproduzir exige mesmos hashes, interpretação temporal, exclusão, ordem, versões,
  sementes e workload. Mesma semente em modos temporais diferentes não produz
  necessariamente a mesma amostra. R04 não estabelece ground truth da captura.
- Label-Based depende de informação supervisionada indisponível em muitos cenários
  operacionais. Retenção de ataques nesse método não é desempenho de detector.
- Top-K local por origem e ranking global de consulta medem objetos distintos.
  LIMIT 20 não permite medir todos os empates além do corte.
- Três consultas não demonstram preservação semântica geral; Aggregation não conserva
  todas as propriedades/identidades individuais. TVD pequeno não implica preservação
  de classes raras nem exatidão de totais; HT e contagens observadas devem coexistir.
- Cinco sementes não comprovam adequação estatística nem o modelo teórico. Para uma
  classe rara com n instâncias, desaparecimento Bernoulli tem probabilidade `(1-p)^n`.
  Analisar incerteza, cobertura por classe e múltiplas realizações separadamente.
- Tempos incluem instrumentação. Cache do SO não é limpo por reinício da JVM;
  competição por CPU/disco e aquecimento podem confundir comparações. Não somar
  tempos aninhados nem tratar cargas/consultas como o mesmo custo.
- A matriz completa excede o orçamento livre observado em C. Batching resolve
  escopo operacional de uma execução, não capacidade cumulativa de retenção nem
  automaticamente efeitos de bloco. A estimativa de 8 GiB por tarefa requer piloto.

## Próxima campanha para NOMS

1. Fazer piloto de capacidade e custo em volume dedicado, preservando medições
   individuais, logs e arquivos lógicos; definir estratégia de retenção sem apagar
   resultados. Validar com plano seco antes de cada execução.
2. Fixar host/JVM/carga externa, documentar cache conforme evidência e separar
   warm-up de geração e consulta. Manter protocolo comum e registrar ordem/blocos.
   Reavaliar número de repetições a partir da variabilidade, sem assumir que cinco bastam.
3. Separar eixo estocástico (sementes) de eixo de desempenho (repetições da mesma
   semente). Medir p=0,005/0,01/0,05/0,10, K=10/20/50 e 6/12/24/48h; manter
   Aggregation 60s como referência, sem declarar ótimo qualquer parâmetro a priori.
4. Relacionar redução estrutural/armazenamento com retenção de ataques, classes,
   SPARQL, TVD, HT, geração, ingestão e consultas; relatar dispersão e falhas.
5. Investigar hipóteses R04 em análise separada, distinguindo alteração de população
   de alteração de amostra; estudar classes raras com mais realizações quando necessário.
6. Versionar workloads adicionais para atributos não preservados pela agregação,
   empates além do Top-20 e fontes com vocabulários distintos. Calibrar instrumentação
   em perfil separado antes de afirmar throughput nativo.

Não foram consultados nesta etapa documentos específicos do PPGC ou regras de
submissão NOMS. As recomendações acima derivam do código, da auditoria e dos
limites experimentais registrados, sem atribuição a materiais não consultados.
O artigo já submetido ao ICC 2027 não foi alterado.
