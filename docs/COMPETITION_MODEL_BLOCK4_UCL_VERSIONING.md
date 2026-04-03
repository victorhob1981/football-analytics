# Competition Model — Bloco 4 (`champions_league` por temporada)

## Escopo fechado
- A familia `ucl_group_knockout_v1` materializa fase inicial com grupos reais.
- A familia `ucl_league_table_knockout_v1` materializa fase inicial como tabela unica (`league_table`) sem grupos ficticios.
- O mesmo contrato de `tie` e reutilizado nas fases eliminatorias das duas familias.
- `competition_structure_hub` e `fact_stage_progression` passaram a cobrir as duas familias de `champions_league` via `season_format_code`.

## Evidencia objetiva do corte
- `2023_24`
  - `fact_group_standings`: `32` linhas no `Group Stage`
  - `group_id` distintos: `8`
  - `fact_standings_snapshots` no `Group Stage`: `32/32` com `group_id`
- `2024_25`
  - `fact_standings_snapshots` no `League Stage`: `36` linhas
  - `group_id` preenchido: `0`
  - `League Stage` final cobre posicoes `1..36`

## Progressao validada
- `2023_24`
  - `group_stage -> round_of_16`: `16 qualified`
  - `round_of_16 -> quarter_finals`: `8`
  - `quarter_finals -> semi_finals`: `4`
  - `semi_finals -> final`: `2`
- `2024_25`
  - `league_stage -> round_of_16`: `8 qualified`
  - `league_stage -> knockout_round_play_offs`: `16 qualified`
  - `league_stage -> terminal`: `12 eliminated`
  - `knockout_round_play_offs -> round_of_16`: `8 qualified`

## Validacoes fechadas
- `2023_24` gera grupos e nao gera `league_table` inicial.
- `2024_25` gera `league_table` inicial e nao gera `group_id` nesse stage.
- As fases eliminatorias das duas temporadas convergem para o mesmo contrato de confronto.
- Nenhum branch estrutural por `competition_key = champions_league` foi encontrado em `dbt/models`; as ocorrencias ficaram restritas ao catalogo/configuracao de temporada.
