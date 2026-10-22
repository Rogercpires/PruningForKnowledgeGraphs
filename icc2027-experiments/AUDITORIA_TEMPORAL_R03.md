# Resultado da auditoria temporal `audit-20261022-r03`

## Estado da execução

Esta é uma execução exclusivamente de `audit-only`. Nenhum CSV de origem, RDF, manifesto de experimento, TDB2/Fuseki ou benchmark foi modificado/executado. Os arquivos de evidência estão em `temporal-audit/audit-20261022-r03/` (diretório ignorado pelo Git):

- `summary.json`;
- `temporal-audit.csv.gz`, a trilha completa por linha;
- `temporal-differences.csv`, as linhas corrigidas, inválidas ou ambíguas;
- `wednesday-pre-noon-neighborhoods.csv`, inspeção contextual dos sete casos de quarta-feira.

As regras usadas são `2026-10-22.1`, SHA-256 `4871b6d0555ebb23af4a363f37696fa607e896ba5e7bac38c35e6cb67a6ec92d`.

## Resultado verificável

| Métrica | Observado | Esperado | Situação |
| --- | ---: | ---: | --- |
| Fluxos elegíveis | 2.830.743 | 2.830.743 | confere |
| Ataques rotulados | 557.646 | 557.646 | confere |
| Classes de ataque | 14 | 14 | confere |
| Timestamps propostos como corrigidos | 1.471.716 | — | alta confiança, somente proposta |
| Timestamps preservados | 1.359.027 | — | nenhuma regra acionada |
| Timestamps ambíguos | 0 | — | nenhum caso fora das regras/configuração atual |
| Linhas inválidas | 288.602 | — | linhas estruturais vazias, não fluxos |

As 288.602 linhas inválidas pertencem ao final de `Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv`. Elas não contêm Flow ID, IPs, rótulo ou timestamp e, portanto, não são fluxos elegíveis. Elas foram mantidas no relatório como evidência de qualidade da fonte, mas não entram nas métricas do dataset.

## Distribuição das propostas de correção

| Arquivo/sessão | Propostas `+12 h` | Preservados | Evidência de não ordenação |
| --- | ---: | ---: | --- |
| Monday WorkingHours | 222.799 | 307.119 | 96.542 registros 08h–11h após a primeira linha 12h |
| Tuesday WorkingHours | 220.766 | 225.143 | 85.205 registros 08h–11h após a primeira linha 12h |
| Wednesday WorkingHours | 227.337 | 465.366 | 88.899 registros 08h–11h após a primeira linha 12h |
| Thursday Afternoon Infilteration | 288.602 | 0 | sessão identificada como `Afternoon` |
| Friday Afternoon PortScan | 286.467 | 0 | sessão identificada como `Afternoon` |
| Friday Afternoon DDoS | 225.745 | 0 | sessão identificada como `Afternoon` |
| Friday Morning | 0 | 191.033 | — |
| Thursday Morning Web Attacks | 0 | 170.366 | linhas vazias separadas como inválidas |

Os 270.646 registros 08h–11h que aparecem depois da primeira ocorrência de 12h nos arquivos Monday–Wednesday permanecem preservados. A posição no CSV é uma métrica de auditoria, nunca uma condição da normalização.

## Os sete casos de quarta-feira

O relatório de vizinhança confirma sete registros de 01h–05h antes da primeira observação bruta de 12h:

| Linhas CSV | Timestamp original | Comunicação/rotulagem observada | Proposta auditável |
| --- | --- | --- | --- |
| 6.560 | `5/7/2017 2:24` | `172.16.0.1:49631 → 192.168.10.50:80`, DoS slowloris | `2017-07-05T14:24:00` |
| 26.702–26.704 | `5/7/2017 2:24` | três portas de origem consecutivas para `192.168.10.50:80`, DoS slowloris | `2017-07-05T14:24:00` |
| 26.705 | `5/7/2017 2:25` | `172.16.0.1:49633 → 192.168.10.50:80`, DoS slowloris | `2017-07-05T14:25:00` |
| 55.504 e 55.523 | `5/7/2017 1:43` | `172.16.0.1:443 → 192.168.10.51`, BENIGN | `2017-07-05T13:43:00` |

O rótulo e o Flow ID foram inspecionados para explicar os casos, mas **não** são entradas da regra. A decisão é função somente do arquivo/sessão e do horário bruto 01h–05h.

## Convenção temporal e condição para ativação futura

As propostas são horários civis locais, sem offset: por exemplo, `2017-07-05T14:24:00`, nunca `Z`. O bucket de 60 s é calculado por arredondamento local para baixo, também sem epoch. A auditoria não demonstra o fuso horário nem recupera entidades individuais de fluxo.

Ainda não é autorizado ativar a normalização no gerador RDF. Antes disso, revisar `temporal-differences.csv`, decidir e documentar a convenção de fuso horário, e executar testes de equivalência contra o Baseline em uma execução separada. O tempo desta auditoria não é uma medição de desempenho.
