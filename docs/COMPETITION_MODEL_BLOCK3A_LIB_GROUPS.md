# Competition Model — Bloco 3A (`libertadores` grupos)

## Escopo fechado
- Entidade `group` criada de forma aditiva via `mart.dim_group`.
- `mart.fact_group_standings` criado com grain `competition + season + stage + group + round + team`.
- `mart.fact_standings_snapshots` passou a carregar `group_id` e `group_sk` quando o `stage_format` e `group_table`.
- `mart.fact_matches` passou a carregar `group_id` quando os dois lados pertencem ao mesmo grupo no mesmo `stage`.

## Fonte e inferência
- A origem `raw.standings_snapshots.payload` expõe `group_id`.
- O projeto nao encontrou `group_name` nativo no payload disponivel.
- `group_name` e inferido de forma auditavel a partir de `group_order`:
  - `1 -> Group A`
  - `2 -> Group B`
  - ...
- Todas as linhas de `dim_group` sao marcadas com `is_inferred = true`.

## Evidencia objetiva das temporadas-prova
- `2024`: 8 grupos, 4 times por grupo, rodada final de grupos em `round_key = 6`.
- `2025`: 8 grupos, 4 times por grupo, rodada final de grupos em `round_key = 6`.
- `fact_group_standings`: `288` linhas materializadas.
- `fact_matches` para grupos da `libertadores`:
  - `2024`: `96` partidas com `group_id` preenchido em `96/96`
  - `2025`: `96` partidas com `group_id` preenchido em `96/96`

## Validacoes fechadas
- Nenhum time apareceu em mais de um grupo no mesmo `stage`.
- Nenhuma partida de fase de grupos ficou sem `group_id`.
- Totais finais de standings (`points`, `won`, `draw`, `lost`, `goals_for`, `goals_against`, `goal_diff`) batem com a recomputacao a partir de `fact_matches` para `2024` e `2025`.
- Posicoes finais por grupo ficaram densas e completas (`1..4`) nas duas temporadas-prova.

## Boundary preservado
- Nao foi criada logica por `competition_key` para determinar comportamento estrutural.
- O comportamento segue `stage_format = group_table` e memberships derivados do scoping de standings.
- Nenhuma logica de progressao entre fases entrou neste bloco; isso permanece no bloco seguinte.
