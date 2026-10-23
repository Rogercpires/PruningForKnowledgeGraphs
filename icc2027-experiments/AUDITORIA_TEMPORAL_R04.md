# Resultado da auditoria temporal `audit-20261022-r04`

## Estado e rastreabilidade

A R04 é uma nova execução `audit-only`, em diretório próprio, e preserva a R03 sem qualquer modificação. Não foram gerados RDF, TDB2/Fuseki ou medições de benchmark. As regras aplicadas são `2026-10-22.2-r04`, SHA-256 `5b0ffc8205a415cefdc3df3eb61e3474685b7fedf0ded3815abc00210c0df892`.

O ajuste é baseado na documentação complementar de Engelen, Rimmer e Joosen (2021), que informa para DoS Slowloris em 05/07/2017 a janela de **02:23:00 AM a 10:12:59 AM**. Essa evidência temporal independente permite preservar provisoriamente os cinco timestamps de 02h24–02h25. A regra de exceção identifica somente `(arquivo, linha, timestamp_original)`; ela não lê `Label`, Flow ID, IP, porta ou posição relativa a 12h para tomar a decisão.

## Invariantes e totais

| Métrica | R04 | Esperado | Situação |
| --- | ---: | ---: | --- |
| Fluxos elegíveis | 2.830.743 | 2.830.743 | confere |
| Ataques rotulados | 557.646 | 557.646 | confere |
| Classes de ataque | 14 | 14 | confere |
| Corrigidos | 1.471.709 | — | proposta de `+12h` autorizada pela regra de sessão |
| Preservados | 1.359.032 | — | inclui 5 preservações provisórias de madrugada |
| Ambíguos | 2 | — | sem timestamp proposto |
| Inválidos | 288.602 | — | linhas estruturais vazias de Web Attacks |

## Comparação R03 → R04: os sete registros modificados

| Linha | Timestamp bruto | R03 | R04 | Justificativa R04 |
| ---: | --- | --- | --- | --- |
| 6.560 | `5/7/2017 2:24` | `14:24`, corrigido | `02:24`, preservado/provisório | janela Slowloris começa às 02:23 |
| 26.702 | `5/7/2017 2:24` | `14:24`, corrigido | `02:24`, preservado/provisório | mesma exceção por linha e timestamp |
| 26.703 | `5/7/2017 2:24` | `14:24`, corrigido | `02:24`, preservado/provisório | mesma exceção por linha e timestamp |
| 26.704 | `5/7/2017 2:24` | `14:24`, corrigido | `02:24`, preservado/provisório | mesma exceção por linha e timestamp |
| 26.705 | `5/7/2017 2:25` | `14:25`, corrigido | `02:25`, preservado/provisório | dentro da janela 02:23–10:12:59 |
| 55.504 | `5/7/2017 1:43` | `13:43`, corrigido | sem proposta, ambíguo | não há evidência temporal independente para escolher 01:43 ou 13:43 |
| 55.523 | `5/7/2017 1:43` | `13:43`, corrigido | sem proposta, ambíguo | não há evidência temporal independente para escolher 01:43 ou 13:43 |

Logo, em relação à R03, `corrected` diminuiu em 7, `preserved` aumentou em 5 e `ambiguous` aumentou em 2. As demais 3.119.338 linhas mantêm a mesma decisão de auditoria.

## Limite metodológico

Os cinco registros preservados não validam o rótulo nem reconstroem entidades individuais de fluxo; apenas evitam uma correção temporal que contradiz a janela de madrugada publicada. Os dois registros ambíguos permanecem fora de qualquer transformação até que PCAPs, log de coleta ou outra fonte temporal independente fundamente uma escolha. A representação continua em horário civil local sem offset, sem `Z` e sem epoch.

Fonte: G. Engelen, V. Rimmer e W. Joosen, *Troubleshooting an Intrusion Detection Dataset: the CICIDS2017 Case Study*, documentação complementar, tabela “Labelling”, 2021.

