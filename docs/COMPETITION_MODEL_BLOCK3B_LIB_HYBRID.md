# Competition Model — Bloco 3B (`libertadores` hibrida ponta a ponta)

## Escopo fechado
- Progressao entre fases separada do boundary de fase via `control.stage_progression_config`.
- `mart.fact_stage_progression` criado para materializar progressoes por time e fase.
- `mart.competition_structure_hub` criado como hub estrutural minimo consultavel por dados.
- Modelo de `tie` expandido para fases eliminatorias de edicoes `hybrid`, sem duplicar logica do piloto knockout.

## Boundary de progressao
- A regra de progressao nao foi absorvida por `dim_stage`.
- A regra de progressao nao foi absorvida por `competition_season_config`.
- O boundary novo guarda apenas:
  - origem estrutural (`from_stage_code`, `from_stage_format`)
  - tipo de progressao (`qualified`, `eliminated`, `repechage`)
  - destino estrutural (`to_stage_code`, quando existe)
  - escopo da regra (`group_position` ou `tie_outcome`)

## Evidencia objetiva das temporadas-prova
- `fact_tie_results` para `libertadores`:
  - `2024`: `30` confrontos
  - `2025`: `30` confrontos
- `fact_stage_progression` para `libertadores`:
  - `2024`: `45 qualified`, `34 eliminated`, `12 repechage`
  - `2025`: `45 qualified`, `34 eliminated`, `12 repechage`

## Sequencia estrutural validada
- `1st_round -> 2nd_round`
- `2nd_round -> 3rd_round`
- `3rd_round -> group_stage`
- `group_stage -> round_of_16`
- `round_of_16 -> quarter_finals`
- `quarter_finals -> semi_finals`
- `semi_finals -> final`

## Validacoes fechadas
- Top 2 de cada grupo entram exatamente nas oitavas.
- Nao classificados da fase de grupos nao aparecem nas oitavas.
- Vencedores de confrontos eliminatorios aparecem na fase de destino esperada.
- Eliminados e repechage de confrontos nao aparecem na fase seguinte da mesma competicao.
- O mesmo contrato de `tie` continua valido para `copa_do_brasil` e passou a cobrir as fases eliminatorias de torneios `hybrid`.

## Observacao semantica
- `repechage` fica com `to_stage_id = null` quando o destino sai da edicao atual da competicao.
- Isso preserva o contrato interno da edicao sem inventar `stage` ficticio para competicao externa.
