# PruningForKnowledgeGraphs

Projeto dedicado ao estudo e implementação de técnicas de *Pruning* aplicadas à ingestão de Grafos de Conhecimento (Knowledge Graphs), utilizando o dataset **CICIDS2017** e armazenamento no **Apache Jena Fuseki**.

## 🎯 Objetivo do Projeto
Investigar o impacto de diferentes **técnicas de pruning aplicadas durante a ingestão de eventos de rede** para construção de Grafos de Conhecimento, avaliando:

- Redução no volume de triplas
- Preservação de informação relevante para incidentes
- Custo computacional
- Reprodutibilidade do pipeline de ingestão
---

## 📁 Estrutura do Repositório

PruningForKnowledgeGraphs/
│
├── scripts/
│ ├── ingest_cicids2017.py # Script principal de ingestão + pruning
│ ├── utils/ # (Opcional) funções auxiliares
│
├── sparql/
│ ├── baseline_validation.sparql
│ ├── pruning_validation.sparql
│ ├── queries_documentation.md # Descrição das querys de validação
│
├── ontology.ttl # Ontologia utilizada no KG
├── ontology-commented.ttl # Versão comentada para explicação acadêmica
│
├── config.yaml # Configurações gerais (paths, parâmetros)
├── LICENSE
├── README.md # Documento principal do projeto

---

## 🚀 Técnicas de Pruning Implementadas

As seguintes técnicas foram implementadas no projeto:

### 1. **Baseline (sem pruning)**
Gera todas as triplas dos CSVs originais. Serve como referência para comparações.

### 2. **Temporal Pruning**
Mantém apenas eventos ocorridos dentro de uma janela temporal, ex. `24h`.

Reduz triplas descartando fluxos antigos → útil em SIEMs com janelas deslizantes.

### 3. **Label Pruning**
Seleciona somente tipos específicos de ataque, via parâmetro:
--labels suspicious,ddos,portscan
Remove labels irrelevantes e reduz tamanho semanticamente.

### 4. **Sample Pruning**
Amostragem estatística uniforme:
Mantém apenas *p%* dos fluxos.

Útil quando o dataset é massivo e suficientemente redundante.

### 5. **Top-K Backbone (Top-K Pruning)**
Seleciona as arestas com maior volume de comunicação (src → dst).

Reduz ruído e mantém estrutura essencial do grafo.

### 6. **Aggregation Pruning**
Agrega fluxos similares por janelas de tempo, criando *AggregatedFlow*.

Compacta dados preservando tendências macro.

---

## 🧪 Validação dos Dados Importados (SPARQL)

O repositório contém todos os scripts SPARQL utilizados, incluindo:

- Contagem de triplas por grafo
- Distribuição de labels
- Distribuição de protocolos
- Checagem de consistência de literais e datatypes
- Distribuição temporal de fluxos (*quando aplicável*)
- Contagem de classes instanciadas
- Checagem de predicados usados

Essas consultas podem ser executadas em qualquer dataset Fuseki via interface `/sparql`.

---

## 📦 Reprodutibilidade do Experimento

Para reproduzir fielmente o estudo:

### ✔ 1. Dataset original CICIDS2017  
Instruções para download e organização dos arquivos.

### ✔ 2. Script de ingestão
`ingest_cicids2017.py`  
Inclui todas as técnicas de pruning via linha de comando.

### ✔ 3. Políticas de execução
Exemplos:
python ingest_cicids2017.py --csv-root dataset/ --baseline
python ingest_cicids2017.py --csv-root dataset/ --temporal 24h
python ingest_cicids2017.py --csv-root dataset/ --labels suspicious,ddos
python ingest_cicids2017.py --csv-root dataset/ --sample 0.01
python ingest_cicids2017.py --csv-root dataset/ --topk 20
python ingest_cicids2017.py --csv-root dataset/ --aggregate 60s


### ✔ 4. Script SPARQL de validação  
Todos organizados na pasta `/sparql`.

### ✔ 5. Ontologia  
ontology.ttl: ontologia minimalista projetada para representar:
    NetworkFlow
    IPAddress
    timestamps, ports, labels, protocolos, etc.
ontology-commented.ttl: versão com explicações linha a linha.

A ontologia deve ser carregada antes ou depois dos arquivos .nt.gz, pois ela está em grafo separado.

### ✔ 6. Logs de execução  
Um diretório pode ser adicionado, ex:
/logs/
baseline_output.txt
pruning_temporal_output.txt


---

## 📊 Resultados Esperados (resumo)

Cada técnica proporciona:

| Técnica           | Redução Esperada | Perda Semântica | Custo Computacional |
|------------------|------------------|------------------|----------------------|
| Baseline         | 0%               | 0%               | Alto                 |
| Temporal         | Médio–Alto       | Baixa            | Médio                |
| Labels           | Alto (dependente)| Baixa-Média      | Baixo                |
| Sample           | Muito Alta       | Alta             | Baixo                |
| Top-K            | Médio            | Média            | Médio–Alto           |
| Aggregation      | Muito Alta       | Baixa–Média      | Alto                 |

---

👥 Autores

Roger Pires - roger.pires@inf.ufrgs.br
Douglas Nascimento - doug@inf.ufrgs.br

---

## 📚 Referências

- CICIDS2017 Dataset  
- Apache Jena Fuseki  
- Papers sobre pruning de grafos e stream reasoning  
- Trabalhos sobre SIEM/SOC e retenção temporal  

---

## 📝 Licença

Este projeto está licenciado sob a licença GLP.

