# Match Events Partition Migration Execution

## 1. Estado inicial

Data da execucao: `2026-04-01`

Estado vivo confirmado antes da migracao fisica:

- tabela pai: `raw.match_events`
- estrategia de particionamento: `LIST (season)`
- particoes existentes:
  - `raw.match_events_2024`
  - `raw.match_events_default`
- total de rows: `284210`
- PK viva distinta: `284210`
- indices no `default` antes do cleanup: `10`

Distribuicao inicial por season e particao:

| season | particao inicial | rows |
| --- | --- | ---: |
| 2020 | `raw.match_events_default` | 37742 |
| 2021 | `raw.match_events_default` | 54189 |
| 2022 | `raw.match_events_default` | 51986 |
| 2023 | `raw.match_events_default` | 57591 |
| 2024 | `raw.match_events_2024` | 61326 |
| 2025 | `raw.match_events_default` | 21376 |

## 2. Estrategia escolhida

Estrategia aplicada: `A`

Fluxo executado por season:

1. copiar a slice da season do `default` para staging fora da arvore de particao;
2. validar contagem e unicidade da slice;
3. remover a slice do `default`;
4. criar a particao explicita da season;
5. reinserir a slice pela tabela pai `raw.match_events`;
6. validar particao final, contagem total, PK, FK e indices;
7. seguir para a proxima season apenas se a anterior ficasse verde.

Justificativa:

- mais segura que `ATTACH PARTITION` neste contexto;
- mais simples para rollback por season, porque cada migration roda em transacao propria;
- evita lidar com compatibilizacao manual de indices ao anexar tabela isolada;
- garante que as rows retornem pelo roteamento oficial da tabela pai.

## 3. Execucao por season

Migrations aplicadas:

- `20260401113000_match_events_partition_2025.sql`
- `20260401113100_match_events_partition_2020.sql`
- `20260401113200_match_events_partition_2022.sql`
- `20260401113300_match_events_partition_2021.sql`
- `20260401113400_match_events_partition_2023.sql`
- `20260401113500_match_events_default_index_cleanup.sql`

Tempos observados no `dbmate`:

- `2025`: `1.572 s`
- `2020`: `1.831 s`
- `2022`: `2.219 s`
- `2021`: `2.360 s`
- `2023`: `2.403 s`
- cleanup de indices: `0.026 s`

Resumo de execucao por season:

| season | default_before | slice_staging | particao final | default_after | total_after | PK distinta after | FK orphans | status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2025 | 21376 | 21376 | 21376 | 0 | 284210 | 284210 | 0 | `COMPLETO` |
| 2020 | 37742 | 37742 | 37742 | 0 | 284210 | 284210 | 0 | `COMPLETO` |
| 2022 | 51986 | 51986 | 51986 | 0 | 284210 | 284210 | 0 | `COMPLETO` |
| 2021 | 54189 | 54189 | 54189 | 0 | 284210 | 284210 | 0 | `COMPLETO` |
| 2023 | 57591 | 57591 | 57591 | 0 | 284210 | 284210 | 0 | `COMPLETO` |

Observacao operacional:

- a tentativa anterior de criar a particao explicita antes de esvaziar a slice do `default` foi corretamente abandonada;
- a execucao verde desta rodada aconteceu somente com a estrategia compativel com `DEFAULT partition`.

## 4. Validacoes de integridade

Validacoes obrigatorias cumpridas em cada season:

- contagem `before` da season no `default`
- contagem da slice em staging
- `DELETE` efetivo da slice no `default`
- criacao correta da particao explicita
- contagem `after` na particao final
- `default_after = 0` para a season drenada
- contagem total preservada
- PK `(provider, season, fixture_id, event_id)` permaneceu unica
- FK para `raw.fixtures(fixture_id)` permaneceu integra

Evidencia consolidada de integridade final:

- total final: `284210`
- PK distinta final: `284210`
- FK orphans nas seasons drenadas:
  - `2020`: `0`
  - `2021`: `0`
  - `2022`: `0`
  - `2023`: `0`
  - `2025`: `0`
- `raw.match_events_default`: `0` rows totais
- `raw.match_events_default` com seasons alvo: `0` rows

## 5. Catalogo final de particoes

Estado final do catalogo:

| particao | bound | rows |
| --- | --- | ---: |
| `raw.match_events_2020` | `FOR VALUES IN (2020)` | 37742 |
| `raw.match_events_2021` | `FOR VALUES IN (2021)` | 54189 |
| `raw.match_events_2022` | `FOR VALUES IN (2022)` | 51986 |
| `raw.match_events_2023` | `FOR VALUES IN (2023)` | 57591 |
| `raw.match_events_2024` | `FOR VALUES IN (2024)` | 61326 |
| `raw.match_events_2025` | `FOR VALUES IN (2025)` | 21376 |
| `raw.match_events_default` | `DEFAULT` | 0 |

Distribuicao final por season:

| season | particao final | rows |
| --- | --- | ---: |
| 2020 | `raw.match_events_2020` | 37742 |
| 2021 | `raw.match_events_2021` | 54189 |
| 2022 | `raw.match_events_2022` | 51986 |
| 2023 | `raw.match_events_2023` | 57591 |
| 2024 | `raw.match_events_2024` | 61326 |
| 2025 | `raw.match_events_2025` | 21376 |

## 6. Revisao de indices duplicados

Revisao executada apenas no final, apos drenagem completa do `default`.

Estado antes:

- `raw.match_events_default`: `10` indices
- duplicidade local confirmada:
  - `idx_raw_match_events_default_fixture_id`
  - `idx_raw_match_events_default_player_id`
  - `idx_raw_match_events_default_team_id`

Acao executada:

- remocao das tres duplicidades locais acima

Estado depois:

- `raw.match_events_default`: `7` indices
- familia final coerente com as demais particoes:
  - `match_events_default_pkey`
  - `match_events_default_assist_id_idx`
  - `match_events_default_competition_key_season_label_idx`
  - `match_events_default_fixture_id_idx`
  - `match_events_default_fixture_id_type_idx`
  - `match_events_default_player_id_idx`
  - `match_events_default_team_id_idx`

Contagem final de indices por particao:

| tabela | indices |
| --- | ---: |
| `match_events_2020` | 7 |
| `match_events_2021` | 7 |
| `match_events_2022` | 7 |
| `match_events_2023` | 7 |
| `match_events_2024` | 7 |
| `match_events_2025` | 7 |
| `match_events_default` | 7 |

## 7. Riscos remanescentes

- `raw.match_events_default` ficou vazio, mas continua como catch-all para qualquer season futura ainda sem particao explicita;
- novas seasons que precisarem sair do `default` vao exigir a mesma disciplina de migracao por slice;
- nao houve alteracao de outras tabelas, parametros ou arquitetura, por desenho.

## 8. Conclusao objetiva sobre a Onda 4

Resultado final:

- as seasons historicas `2025`, `2020`, `2022`, `2021` e `2023` foram drenadas fisicamente do `default`;
- as particoes explicitas correspondentes foram criadas e validadas;
- a contagem total foi preservada;
- a PK viva permaneceu unica;
- a FK permaneceu integra;
- a duplicidade local de indices do `default` foi removida com seguranca;
- o catalogo final de `raw.match_events` ficou coerente com o desenho particionado esperado.

Conclusao:

- a pendencia fisica real da Onda 4 em `raw.match_events` foi concluida com sucesso.
