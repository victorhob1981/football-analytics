# Block 2 - Copa do Brasil Tie Derivation

## Escopo

Este artefato documenta apenas o piloto de mata-mata puro da `copa_do_brasil`.
Nao cobre `libertadores`, `champions_league`, API, frontend ou configuracao de produto.

## Evidencia objetiva usada na derivacao

- O recorte `2021-2025` da `copa_do_brasil` fecha em particao deterministica por `competition_key + season_label + stage_id + unordered team pair`.
- Em todas as temporadas disponiveis, cada confronto dessa particao tem exatamente `1` ou `2` partidas.
- Nenhum confronto dessas temporadas fica sem vencedor quando a inferencia usa:
  - placar agregado quando ele fecha o vencedor;
  - status oficial do fixture (`FT`, `AET`, `FTP`) para identificar `single_match`, `extra_time` ou `penalties`;
  - participacao exclusiva do classificado no `stage` seguinte quando o placar isolado nao basta.

## Regras materializadas

- `tie_id` e `leg_number` sao derivados e marcados via `is_inferred=true` porque a origem atual nao expõe entidade nativa de confronto.
- `home_side_team_id` e `away_side_team_id` sao ancorados na orientacao do primeiro leg oficial do confronto.
- `resolution_type` segue a hierarquia observavel no dado:
  - `penalties` quando algum leg termina com `status_short=FTP`;
  - `extra_time` quando algum leg termina com `status_short=AET` e nao houve `FTP`;
  - `single_match` quando o confronto tem um unico jogo e o vencedor fecha no placar;
  - `aggregate` quando o confronto tem mais de um jogo e o vencedor fecha no agregado;
  - `stage_rule` quando o confronto fecha vencedor por regra estrutural observavel via progressao, sem `FTP` ou `AET`.

## Observacoes criticas do piloto

- `2024` primeira fase tem `9` empates em jogo unico com `status=FT`; em todos os casos o classificado observado esta apenas no `stage` seguinte.
- `2025` primeira fase muda o comportamento observado: empates em jogo unico aparecem com `status=FTP`.
- `2022` introduz casos com `status=AET`; o modelo materializa esse estado como `resolution_type=extra_time`, sem inventar detalhe ausente de placar de pênaltis.

## Limites assumidos

- A progressao neste bloco e apenas a basica do piloto puro: vencedor do confronto e seu `next_stage_id`.
- O contrato canonico completo de `progression` entre fases continua reservado para o bloco hibrido posterior.
