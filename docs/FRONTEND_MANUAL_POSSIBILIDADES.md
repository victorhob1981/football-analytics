# FRONTEND MANUAL - ESPECIFICACAO FUNCIONAL (VERSAO REVISADA)

Data de referencia: 2026-02-21  
Escopo: consolidar tudo que ja foi ingerido no `football-analytics` e transformar em um plano funcional completo para frontend analitico, comparativos e rankings, com consumo via BFF/API interna e base principal em `mart.*`.

## Indice

1. Premissas de arquitetura e consumo de dados  
2. Inventario completo de dados ingeridos  
3. Catalogo funcional por tabela (raw + mart)  
4. Mapa entidade -> tabelas fonte  
5. Estrutura de navegacao (IA/UX) e widgets por pagina  
6. Clubes e jogadores: filtros, comparativos e drilldown  
7. Central de rankings: recortes e limitacoes  
8. Time intelligence (recortes temporais globais)  
9. Cobertura de dados, qualidade e estados da UI  
10. Contratos conceituais da API interna (BFF)  
11. O que da para entregar AGORA vs backlog de dados/modelagem  
12. Prioridade de execucao recomendada  
13. Matriz Completa de Metricas Disponiveis no Banco  
14. Metricas Derivadas e Indices Analiticos Propostos  
15. Camada de Insight e Deteccao de Tendencias

## 1) Premissas de arquitetura e consumo de dados

- O frontend nao consulta banco diretamente.
- O frontend consome apenas endpoints da BFF/API interna.
- A BFF prioriza `mart.*` para todas as telas de produto.
- `raw.*` entra apenas em dois cenarios:
- Auditoria de cobertura de dados (percentual de fixtures com stats/eventos/lineups).
- Investigacao operacional (qualidade/lag de ingestao).
- Toda tela deve declarar claramente:
- Fonte principal (`mart.*`).
- Fontes auxiliares (`raw.*`, quando necessario).
- Estado de cobertura (completo, parcial, vazio).

## 2) Inventario completo de dados ingeridos

### 2.1 Snapshot atual de volume (raw + mart)

| Dataset | Linhas |
|---|---:|
| `mart.coach_performance_summary` | 15 |
| `mart.dim_coach` | 85 |
| `mart.dim_competition` | 2 |
| `mart.dim_date` | 240 |
| `mart.dim_player` | 1,355 |
| `mart.dim_round` | 38 |
| `mart.dim_stage` | 1 |
| `mart.dim_team` | 40 |
| `mart.dim_venue` | 56 |
| `mart.fact_fixture_lineups` | 5,534 |
| `mart.fact_fixture_player_stats` | 2,767 |
| `mart.fact_match_events` | 6,972 |
| `mart.fact_matches` | 441 |
| `mart.fact_standings_snapshots` | 20 |
| `mart.head_to_head_summary` | 450 |
| `mart.league_summary` | 2 |
| `mart.player_90_metrics` | 604 |
| `mart.player_match_summary` | 2,767 |
| `mart.player_season_summary` | 604 |
| `mart.standings_evolution` | 760 |
| `mart.team_match_goals_monthly` | 176 |
| `mart.team_monthly_stats` | 176 |
| `mart.team_performance_monthly` | 0 |
| `raw.competition_leagues` | 1 |
| `raw.competition_rounds` | 38 |
| `raw.competition_seasons` | 1 |
| `raw.competition_stages` | 1 |
| `raw.fixture_lineups` | 17,278 |
| `raw.fixture_player_statistics` | 17,229 |
| `raw.fixtures` | 380 |
| `raw.head_to_head_fixtures` | 4,529 |
| `raw.match_events` | 6,972 |
| `raw.match_events_2024` | 6,972 |
| `raw.match_statistics` | 760 |
| `raw.player_season_statistics` | 869 |
| `raw.player_transfers` | 5,634 |
| `raw.provider_entity_map` | 0 |
| `raw.provider_sync_state` | 12 |
| `raw.standings_snapshots` | 20 |
| `raw.team_coaches` | 140 |
| `raw.team_sidelined` | 16 |

### 2.2 Notas de cobertura importantes

- `raw.match_events` e `mart.fact_match_events` estao alinhadas em volume (`6,972`), o que habilita timeline de partida.
- `raw.fixtures` cobre 2024 com 380 fixtures.
- Campos de enriquecimento de fixture (`attendance`, `weather`, `referee`, `stage_id`, `round_id`) estao com cobertura parcial e devem ter fallback de UI para "nao informado".
- Existem 49 registros de lineup sem `player_id`; a UI deve mostrar slot sem link de perfil.
- `mart.team_performance_monthly` esta vazio (`0`), entao widgets que dependam dele ficam como proposta futura ou fallback para `mart.team_monthly_stats`.
- `raw.provider_entity_map` esta vazio (`0`); telas de auditoria de reconciliacao multi-provider ficam como futura.

## 3) Catalogo funcional por tabela (raw + mart)

## 3.1 Camada raw

| Tabela | Proposito | Granularidade | Chaves provaveis | Como consumir no frontend |
|---|---|---|---|---|
| `raw.competition_leagues` | Cadastro de ligas/competicoes | por liga | `league_id` | Filtros globais de competicao e contexto de liga |
| `raw.competition_rounds` | Cadastro de rodadas | por rodada | `round_id`, `season_id`, `league_id` | Navegacao por rodada, calendario e ordenacao de standings |
| `raw.competition_seasons` | Cadastro de temporadas | por temporada | `season_id`, `league_id` | Filtro de temporada e escopo temporal principal |
| `raw.competition_stages` | Cadastro de fases/estagios | por stage | `stage_id`, `season_id` | Segmentacao por fase quando aplicavel |
| `raw.fixture_lineups` | Escalacoes e participacao por fixture | por fixture-jogador | `fixture_id`, `team_id`, `player_id` | Auditoria detalhada de elenco usado por jogo |
| `raw.fixture_player_statistics` | Estatisticas de jogador por fixture | por fixture-jogador | `fixture_id`, `team_id`, `player_id` | Conferencia de metricas individuais antes do mart |
| `raw.fixtures` | Cadastro e metadados de partidas | por fixture | `fixture_id`, `league_id`, `season_id`, `round_id`, `stage_id` | Base para agenda, placares e metadados de partida |
| `raw.head_to_head_fixtures` | Historico bruto de confrontos | por fixture em confronto de pares | `fixture_id`, `home_team_id`, `away_team_id` | Auditoria de h2h e validacao de agregados |
| `raw.match_events` | Eventos de partida | por evento | `event_id`, `fixture_id`, `team_id`, `player_id` | Base de auditoria para timeline e eventos |
| `raw.match_events_2024` | Recorte bruto de eventos 2024 | por evento | `event_id`, `fixture_id` | Apoio operacional para recargas e troubleshooting |
| `raw.match_statistics` | Estatisticas por time em partida | por fixture-time | `fixture_id`, `team_id` | Conferencia de team stats por jogo |
| `raw.player_season_statistics` | Acumulado de jogador na temporada | por jogador-temporada | `player_id`, `season_id`, `team_id` | Auditoria de resumo por temporada no nivel raw |
| `raw.player_transfers` | Historico de transferencias | por transferencia | `player_id`, chave de transferencia, data | Mercado: trilha de movimentacao e janelas |
| `raw.provider_entity_map` | Mapeamento tecnico entre providers e IDs internos | por entidade mapeada | chave tecnica de mapeamento | Futura auditoria de reconciliacao multi-provider |
| `raw.provider_sync_state` | Estado de sincronizacao de ingestao | por entidade/scope de sync | chave tecnica de sync | Painel operacional de ingestao e lag |
| `raw.standings_snapshots` | Classificacao oficial por rodada/snapshot | por time-rodada | `season_id`, `round_id`, `team_id` | Auditoria de standings e trilha historica |
| `raw.team_coaches` | Historico de tecnicos por clube | por tecnico-clube-periodo | `coach_id`, `team_id`, janela temporal | Base para modulo de tecnicos |
| `raw.team_sidelined` | Registros de indisponibilidade por clube | por jogador-indisponibilidade | `team_id`, `player_id`, periodo | Base para modulo de disponibilidade |

## 3.2 Camada mart

| Tabela | Proposito | Granularidade | Chaves provaveis | Como consumir no frontend |
|---|---|---|---|---|
| `mart.coach_performance_summary` | Performance agregada de tecnicos | por tecnico-periodo ou tecnico-time | `coach_id`, `team_id`, `season_id` | Rankings e perfil de tecnicos |
| `mart.dim_coach` | Dimensao de tecnicos | por tecnico | `coach_id` | Cadastros e metadados de tecnicos |
| `mart.dim_competition` | Dimensao de competicao | por competicao/temporada | `league_id`, `season_id` | Filtros globais e labels de competicao |
| `mart.dim_date` | Dimensao calendario | por data | chave de data | Time intelligence e agrupamentos temporais |
| `mart.dim_player` | Dimensao de jogadores | por jogador | `player_id` | Perfil de jogador e join para stats |
| `mart.dim_round` | Dimensao de rodadas | por rodada | `round_id`, `season_id` | Ordenacao de rodada e navegacao |
| `mart.dim_stage` | Dimensao de estagios/fases | por stage | `stage_id` | Filtro por fase |
| `mart.dim_team` | Dimensao de clubes/times | por time | `team_id` | Perfil de clube e filtros |
| `mart.dim_venue` | Dimensao de estadios | por venue | `venue_id` | Contexto de local de jogo |
| `mart.fact_fixture_lineups` | Fato de uso de lineup | por fixture-time-jogador | `fixture_id`, `team_id`, `player_id` | Blocos de escalacao e elenco utilizado |
| `mart.fact_fixture_player_stats` | Fato de stats de jogador na partida | por fixture-jogador | `fixture_id`, `team_id`, `player_id` | Match center, scouting por jogo, rankings |
| `mart.fact_match_events` | Fato de eventos | por evento | `event_id`, `fixture_id`, `team_id`, `player_id` | Timeline e mapa de eventos |
| `mart.fact_matches` | Fato principal de partidas | por fixture | `fixture_id`, `league_id`, `season_id`, `home_team_id`, `away_team_id` | Calendario, resultados, placares, base da home |
| `mart.fact_standings_snapshots` | Fato de classificacao por snapshot | por time-rodada | `season_id`, `round_id`, `team_id` | Tabela de classificacao por rodada |
| `mart.head_to_head_summary` | Agregado de confrontos diretos | por par de times (e possivel recorte temporal) | `team_a_id`, `team_b_id`, `season_id` | Tela de h2h e ranking de dominancia |
| `mart.league_summary` | KPIs agregados de liga | por liga-temporada | `league_id`, `season_id` | Cards executivos e resumo da competicao |
| `mart.player_90_metrics` | Metricas normalizadas por 90 minutos | por jogador-temporada | `player_id`, `season_id` | Rankings justos por minutagem |
| `mart.player_match_summary` | Resumo de jogador por jogo | por fixture-jogador | `fixture_id`, `player_id`, `team_id` | Serie temporal de forma e comparativos |
| `mart.player_season_summary` | Resumo agregado de jogador por temporada | por jogador-temporada | `player_id`, `season_id` | Lista de jogadores e perfil consolidado |
| `mart.standings_evolution` | Evolucao rodada a rodada de classificacao/pontos | por time-rodada | `season_id`, `team_id`, `round_key` | Graficos de evolucao no campeonato |
| `mart.team_match_goals_monthly` | Serie mensal de gols por time | por time-mes | `team_id`, periodo mensal | Tendencias ofensivas/defensivas por mes |
| `mart.team_monthly_stats` | Serie mensal de estatisticas de time | por time-mes | `team_id`, periodo mensal | Rankings e comparativos mensais |
| `mart.team_performance_monthly` | Performance mensal consolidada (vazio hoje) | por time-mes | `team_id`, periodo mensal | Proposta futura apos popular dados |

## 4) Mapa entidade -> tabelas fonte

| Entidade funcional | Fonte principal (`mart.*`) | Fonte auxiliar/auditoria (`raw.*`) | Observacao de uso |
|---|---|---|---|
| Competicao / Temporada / Rodada | `mart.dim_competition`, `mart.dim_round`, `mart.dim_stage`, `mart.league_summary`, `mart.standings_evolution`, `mart.fact_standings_snapshots` | `raw.competition_leagues`, `raw.competition_seasons`, `raw.competition_stages`, `raw.competition_rounds`, `raw.standings_snapshots` | Base de filtros e evolucao de classificacao |
| Partida | `mart.fact_matches`, `mart.fact_match_events`, `mart.fact_fixture_player_stats`, `mart.fact_fixture_lineups`, `mart.dim_venue` | `raw.fixtures`, `raw.match_events`, `raw.match_statistics`, `raw.fixture_lineups`, `raw.fixture_player_statistics` | Match center completo com fallback de cobertura |
| Time / Clube | `mart.dim_team`, `mart.fact_matches`, `mart.team_monthly_stats`, `mart.team_match_goals_monthly`, `mart.standings_evolution` | `raw.match_statistics`, `raw.fixture_lineups` | Perfil de clube e comparativos |
| Jogador | `mart.dim_player`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics`, `mart.fact_fixture_player_stats` | `raw.fixture_player_statistics`, `raw.player_season_statistics` | Scouting e ranking |
| Tecnico | `mart.dim_coach`, `mart.coach_performance_summary` | `raw.team_coaches` | Perfil e ranking de tecnicos |
| Evento de partida (timeline) | `mart.fact_match_events` | `raw.match_events`, `raw.match_events_2024` | Sequencia minuto a minuto e eventos chave |
| Lineup / Minutos | `mart.fact_fixture_lineups` | `raw.fixture_lineups` | Escalacao, banco e participacao |
| Transferencias | (consumo direto via BFF) | `raw.player_transfers` | Modulo de mercado com trilha historica |
| Lesoes / Suspensoes | (consumo direto via BFF) | `raw.team_sidelined` | Modulo de disponibilidade |
| Head-to-head | `mart.head_to_head_summary` | `raw.head_to_head_fixtures` | Dominancia historica e recortes por periodo |
| Standings (snapshot/evolucao) | `mart.fact_standings_snapshots`, `mart.standings_evolution` | `raw.standings_snapshots` | Tabela oficial por rodada e tendencia temporal |

## 5) Estrutura de navegacao (IA/UX) e widgets por pagina

## 5.1 Home / Visao Geral (executivo)

- Objetivo: leitura rapida do campeonato e da saude dos dados.
- Widget: KPI de temporada (jogos, gols, media por jogo).
- Fonte: `mart.fact_matches`, `mart.league_summary`.
- Widget: top ataques e defesas.
- Fonte: `mart.team_monthly_stats`, `mart.team_match_goals_monthly`, `mart.fact_matches`.
- Widget: evolucao de pontos dos principais clubes.
- Fonte: `mart.standings_evolution`.
- Widget: top jogadores do momento (recorte ultimos N jogos).
- Fonte: `mart.player_match_summary`, `mart.player_90_metrics`.
- Widget: cobertura de dados do produto.
- Fonte: `mart.fact_matches` + `mart.fact_match_events` + `mart.fact_fixture_player_stats` + `mart.fact_fixture_lineups` + auditoria em `raw.*`.

## 5.2 Competicao (liga/temporada/rodada)

- Objetivo: navegar campeonato com profundidade por rodada.
- Widget: classificacao por rodada (snapshot).
- Fonte: `mart.fact_standings_snapshots`, `mart.dim_round`, `mart.dim_team`.
- Widget: curva de posicao e curva de pontos acumulados.
- Fonte: `mart.standings_evolution`.
- Widget: calendario da rodada e resultados.
- Fonte: `mart.fact_matches`, `mart.dim_round`.
- Widget: filtros por fase e rodada.
- Fonte: `mart.dim_stage`, `mart.dim_round`, `mart.dim_competition`.

## 5.3 Partidas (calendario + match center)

- Objetivo: explorar jogos passados e futuros e abrir detalhamento de cada fixture.
- Widget: lista/calendario de partidas com status.
- Fonte: `mart.fact_matches`, `mart.dim_competition`, `mart.dim_team`, `mart.dim_date`.
- Widget: match center (placar, contexto de venue, arbitragem quando houver).
- Fonte: `mart.fact_matches`, `mart.dim_venue`, fallback em `raw.fixtures`.
- Widget: team stats da partida.
- Fonte: `raw.match_statistics` (auditoria), consolidacao em `mart.fact_matches` quando disponivel no mart.
- Widget: player stats por partida.
- Fonte: `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.dim_player`.
- Widget: lineups titulares/banco.
- Fonte: `mart.fact_fixture_lineups`, `mart.dim_player`.
- Widget: timeline de eventos minuto a minuto.
- Fonte: `mart.fact_match_events`, fallback em `raw.match_events`.

## 5.4 Clubes (perfil + elenco + comparativos)

- Objetivo: perfil completo de clube e comparacao lado a lado.
- Widget: resumo de performance da temporada.
- Fonte: `mart.fact_matches`, `mart.standings_evolution`, `mart.team_monthly_stats`.
- Widget: serie temporal de resultados e gols pro/contra.
- Fonte: `mart.fact_matches`, `mart.team_match_goals_monthly`.
- Widget: disciplina (cartoes/eventos disciplinares).
- Fonte: `mart.fact_match_events`.
- Widget: elenco utilizado e rotacao.
- Fonte: `mart.fact_fixture_lineups`, `mart.dim_player`.
- Widget: top contribuidores (gols, assistencias, participacao).
- Fonte: `mart.player_match_summary`, `mart.player_season_summary`.

## 5.5 Jogadores (lista, perfil, comparativos)

- Objetivo: scouting robusto por temporada e por janela curta.
- Widget: lista de jogadores com filtros.
- Fonte: `mart.player_season_summary`, `mart.player_90_metrics`, `mart.dim_player`.
- Widget: perfil individual (resumo temporada + forma recente).
- Fonte: `mart.player_season_summary`, `mart.player_match_summary`, `mart.player_90_metrics`.
- Widget: historico partida a partida.
- Fonte: `mart.player_match_summary`, `mart.fact_fixture_player_stats`.
- Widget: contexto de clube atual/recente.
- Fonte: `mart.dim_team`, `mart.player_match_summary`.
- Widget: mercado e disponibilidade associados ao jogador.
- Fonte: `raw.player_transfers`, `raw.team_sidelined` (quando houver `player_id`).

## 5.6 Rankings (central unica)

- Objetivo: concentrar liderancas com multiplos recortes.
- Widget: ranking de jogadores por volume (temporada e ultimos N jogos).
- Fonte: `mart.player_season_summary`, `mart.player_match_summary`.
- Widget: ranking por 90 minutos.
- Fonte: `mart.player_90_metrics`.
- Widget: ranking de clubes por desempenho mensal.
- Fonte: `mart.team_monthly_stats`, `mart.team_match_goals_monthly`.
- Widget: ranking de clubes na classificacao.
- Fonte: `mart.standings_evolution`, `mart.fact_standings_snapshots`.
- Widget: ranking de tecnicos.
- Fonte: `mart.coach_performance_summary`, `mart.dim_coach`.
- Widget: ranking de confronto direto (dominancia h2h).
- Fonte: `mart.head_to_head_summary`.
- Widget: ranking de mercado (movimentacao de transferencias).
- Fonte: `raw.player_transfers`.
- Widget: ranking de disponibilidade (times com mais afastamentos).
- Fonte: `raw.team_sidelined`.

## 5.7 Mercado e Disponibilidade

- Objetivo: trilha de movimentacao e risco de indisponibilidade.
- Widget: feed de transferencias por periodo.
- Fonte: `raw.player_transfers`.
- Widget: ranking de clubes por entradas/saidas.
- Fonte: `raw.player_transfers`.
- Widget: painel de indisponiveis por clube.
- Fonte: `raw.team_sidelined`.
- Widget: tendencia temporal de indisponibilidade.
- Fonte: `raw.team_sidelined` (com agregacao na BFF).

## 5.8 Head-to-head

- Objetivo: historico completo de confronto entre dois clubes.
- Widget: resumo agregado de vitorias/empates/derrotas.
- Fonte: `mart.head_to_head_summary`.
- Widget: recorte por periodo (temporada, ultimos N jogos).
- Fonte: `mart.head_to_head_summary`, complemento em `raw.head_to_head_fixtures`.
- Widget: timeline de resultados do confronto.
- Fonte: `raw.head_to_head_fixtures`, `mart.fact_matches`.

## 5.9 Tecnicos

- Objetivo: impacto de tecnico no rendimento de clubes.
- Widget: perfil do tecnico.
- Fonte: `mart.dim_coach`, `raw.team_coaches`.
- Widget: performance agregada sob comando.
- Fonte: `mart.coach_performance_summary`.
- Widget: ranking comparativo de tecnicos.
- Fonte: `mart.coach_performance_summary`.

## 6) Clubes e jogadores: filtros, comparativos e drilldown

## 6.1 Pagina de clube - especificacao funcional

- Bloco: cabecalho do clube.
- Fonte: `mart.dim_team`.
- Bloco: performance geral na temporada (pontos, saldo, ritmo de pontuacao).
- Fonte: `mart.standings_evolution`, `mart.fact_matches`.
- Bloco: forma recente (ultimos N jogos).
- Fonte: `mart.fact_matches`.
- Bloco: distribuicao casa vs fora.
- Fonte: `mart.fact_matches`.
- Bloco: gols pro/contra por mes.
- Fonte: `mart.team_match_goals_monthly`.
- Bloco: estatisticas mensais agregadas.
- Fonte: `mart.team_monthly_stats`.
- Bloco: contribuicao por jogador no clube.
- Fonte: `mart.player_match_summary`, `mart.player_season_summary`.
- Bloco: disciplina da equipe (eventos disciplinares).
- Fonte: `mart.fact_match_events`.
- Bloco: elenco usado e continuidade da base titular.
- Fonte: `mart.fact_fixture_lineups`.

## 6.2 Comparativo Clube vs Clube

- Recortes suportados agora:
- Temporada completa (via `mart.fact_matches`, `mart.standings_evolution`).
- Ultimos N jogos (via `mart.fact_matches`).
- Por rodada (via `mart.standings_evolution`, `mart.fact_standings_snapshots`).
- Por mes (via `mart.team_monthly_stats`, `mart.team_match_goals_monthly`).
- Casa/fora (via `mart.fact_matches`).
- Blocos comparativos:
- Resultado agregado.
- Tendencia temporal.
- Contribuidores chave por time.
- Historial de confronto direto.
- Fontes:
- `mart.fact_matches`
- `mart.standings_evolution`
- `mart.team_monthly_stats`
- `mart.team_match_goals_monthly`
- `mart.player_match_summary`
- `mart.head_to_head_summary`

## 6.3 Lista de jogadores - filtros avancados

- Filtros recomendados:
- Competicao (`league_id`) e temporada (`season_id`) via `mart.dim_competition`.
- Clube (`team_id`) via `mart.dim_team`.
- Janela temporal (temporada inteira, ultimos N jogos).
- Minimo de minutos/partidas para reduzir ruido.
- Ordenacao por metrica total e por metrica por 90.
- Fontes:
- `mart.player_season_summary`
- `mart.player_90_metrics`
- `mart.player_match_summary`
- `mart.dim_player`

## 6.4 Perfil de jogador - escopo completo

- Bloco: cabecalho (nome, clube, metadados basicos).
- Fonte: `mart.dim_player`, `mart.dim_team`.
- Bloco: resumo da temporada.
- Fonte: `mart.player_season_summary`.
- Bloco: serie por partida.
- Fonte: `mart.player_match_summary`, `mart.fact_fixture_player_stats`.
- Bloco: eficiencia por 90.
- Fonte: `mart.player_90_metrics`.
- Bloco: participacao em lineups.
- Fonte: `mart.fact_fixture_lineups`.
- Bloco: eventos de impacto no jogo (gols/cartoes/substituicoes quando relacionados).
- Fonte: `mart.fact_match_events`.
- Bloco: transferencias do jogador.
- Fonte: `raw.player_transfers`.
- Bloco: indisponibilidades (quando houver correspondencia de `player_id`).
- Fonte: `raw.team_sidelined`.

## 6.5 Comparativo Jogador vs Jogador

- Modos comparativos:
- Temporada inteira.
- Ultimos N jogos.
- Por 90 minutos.
- Por clube atual.
- Matriz de comparacao:
- Totais por temporada (`mart.player_season_summary`).
- Forma recente (`mart.player_match_summary`).
- Metricas normalizadas (`mart.player_90_metrics`).
- Contexto de lineup (`mart.fact_fixture_lineups`).
- Proposta futura claramente marcada:
- Comparacao por posicao fina depende de padronizacao robusta de posicao no mart (hoje pode variar por qualidade dos dados de origem).

## 7) Central de rankings: recortes e limitacoes

## 7.1 Rankings de jogadores (quantidade)

- Gols, assistencias, participacoes ofensivas, contribuicoes defensivas e outras metricas ja presentes nos resumos.
- Fontes:
- `mart.player_season_summary` (temporada).
- `mart.player_match_summary` (janela curta, ultimos N jogos).
- `mart.fact_fixture_player_stats` (drilldown por partida).

## 7.2 Rankings de jogadores (por 90 minutos)

- Objetivo: reduzir vies de minutagem.
- Fonte principal:
- `mart.player_90_metrics`.
- Complemento de elegibilidade:
- `mart.player_match_summary` e `mart.player_season_summary` para aplicar minimo de minutos/jogos.

## 7.3 Rankings de clubes

- Ranking por pontos e evolucao no campeonato.
- Fonte: `mart.standings_evolution`, `mart.fact_standings_snapshots`.
- Ranking por ritmo mensal de performance.
- Fonte: `mart.team_monthly_stats`, `mart.team_match_goals_monthly`.
- Ranking por disciplina (eventos disciplinares de equipe).
- Fonte: `mart.fact_match_events`.

## 7.4 Rankings de eficiencia/acuracia

- Estado atual:
- Possivel quando a metrica de taxa/percentual ja estiver pronta no `mart.*`.
- Dependencia futura:
- Se necessario, modelar derivacoes adicionais a partir de `raw.match_statistics` e `raw.fixture_player_statistics` para expor indicadores de eficiencia em `mart.*` de forma padronizada e consistente.

## 7.5 Rankings por periodo

- Temporada inteira:
- `mart.player_season_summary`, `mart.standings_evolution`, `mart.fact_standings_snapshots`.
- Por mes:
- `mart.team_monthly_stats`, `mart.team_match_goals_monthly`, `mart.dim_date`.
- Por rodada:
- `mart.standings_evolution`, `mart.fact_standings_snapshots`, `mart.dim_round`.
- Ultimos N jogos:
- `mart.fact_matches`, `mart.player_match_summary`.
- Casa/fora:
- `mart.fact_matches`.

## 7.6 Rankings especiais (mercado, disponibilidade, h2h, tecnicos)

- Transferencias:
- `raw.player_transfers`.
- Disponibilidade:
- `raw.team_sidelined`.
- Dominancia head-to-head:
- `mart.head_to_head_summary` (com auditoria em `raw.head_to_head_fixtures`).
- Tecnicos:
- `mart.coach_performance_summary` + `mart.dim_coach`.

## 8) Time intelligence (recortes temporais globais)

## 8.1 Padrao de Time Range global na UI

- Temporada.
- Rodada(s).
- Mes.
- Ultimos N jogos.
- Intervalo customizado (data inicial/final).

## 8.2 Matriz de recortes por pagina

| Pagina | Temporada | Rodada(s) | Mes | Ultimos N jogos | Intervalo customizado | Fonte principal |
|---|---|---|---|---|---|---|
| Home / Visao Geral | Sim | Parcial | Sim | Sim | Sim | `mart.league_summary`, `mart.fact_matches`, `mart.standings_evolution` |
| Competicao | Sim | Sim | Parcial | Sim | Sim | `mart.fact_standings_snapshots`, `mart.standings_evolution`, `mart.dim_round` |
| Partidas | Sim | Sim | Sim | Sim | Sim | `mart.fact_matches`, `mart.fact_match_events`, `mart.fact_fixture_player_stats` |
| Clubes | Sim | Sim | Sim | Sim | Sim | `mart.fact_matches`, `mart.team_monthly_stats`, `mart.standings_evolution` |
| Jogadores | Sim | Parcial | Parcial | Sim | Sim | `mart.player_season_summary`, `mart.player_match_summary`, `mart.player_90_metrics` |
| Rankings | Sim | Sim | Sim | Sim | Sim | `mart.player_season_summary`, `mart.team_monthly_stats`, `mart.standings_evolution` |
| Mercado/Disponibilidade | Sim | Nao | Parcial | Parcial | Sim | `raw.player_transfers`, `raw.team_sidelined` |
| Head-to-head | Sim | Nao | Parcial | Sim | Sim | `mart.head_to_head_summary`, `raw.head_to_head_fixtures` |
| Tecnicos | Sim | Nao | Parcial | Sim | Sim | `mart.coach_performance_summary`, `raw.team_coaches` |

## 8.3 Dependencias futuras para recortes mais sofisticados

- Recorte por janela de mercado oficial: depende de regras de janela modeladas na camada mart ou tabela de calendario especifica.
- Recorte por fase detalhada em todas as telas: depende de maior cobertura de `stage_id` em `raw.fixtures` e propagacao consistente para `mart.fact_matches`.

## 9) Cobertura de dados, qualidade e estados da UI

## 9.1 Regras de exibicao para cobertura parcial

- Se `attendance` nao existir para fixture, mostrar "Nao informado".
- Se `weather` nao existir para fixture, mostrar "Nao informado".
- Se `referee` nao existir para fixture, mostrar "Nao informado".
- Se lineup vier sem `player_id`, mostrar nome/slot sem link de perfil.
- Se um modulo nao tiver dados para o recorte atual, mostrar estado `empty` com explicacao do filtro aplicado.

## 9.2 Auditoria de cobertura visivel para o usuario

- Painel de cobertura por modulo na Home e na tela de administracao:
- `% fixtures com eventos` usando base `mart.fact_matches` e cobertura `mart.fact_match_events`.
- `% fixtures com player stats` usando base `mart.fact_matches` e cobertura `mart.fact_fixture_player_stats`.
- `% fixtures com lineups` usando base `mart.fact_matches` e cobertura `mart.fact_fixture_lineups`.
- `% fixtures com standings` usando base de rodadas e `mart.fact_standings_snapshots`.
- `% fixtures com team stats` com apoio de `raw.match_statistics`.
- Indicar visualmente:
- Verde: cobertura alta.
- Amarelo: cobertura parcial.
- Vermelho: cobertura baixa.

## 9.3 Estados de UI obrigatorios em todas as paginas

- `loading`: skeleton e placeholders.
- `success`: dados completos para o recorte.
- `partial`: dados parciais com badge explicito.
- `empty`: zero dados para o recorte.
- `error`: erro tecnico com opcao de tentar novamente.

## 10) Contratos conceituais da API interna (BFF)

Padrao geral:
- Filtros comuns: `leagueId`, `season`, `round`, `teamId`, `playerId`, `startDate`, `endDate`, `lastN`.
- Ordenacao comum: `sortBy`, `sortDir`.
- Paginacao comum: `page`, `pageSize`.
- Fonte principal deve ser `mart.*`; `raw.*` entra apenas quando o modulo ainda nao esta materializado no mart ou para auditoria.

| Endpoint conceitual | Proposito | Parametros principais | Tabelas fonte | Cache recomendado | Paginacao / ordenacao |
|---|---|---|---|---|---|
| `/api/filters/global` | carregar filtros globais de liga/temporada/rodada/time | `leagueId`, `season` | `mart.dim_competition`, `mart.dim_round`, `mart.dim_team`, `mart.dim_stage` | TTL 1h por `leagueId+season` | sem paginacao; ordenacao por hierarquia |
| `/api/home/overview` | cards executivos e cobertura | `leagueId`, `season`, `lastN` | `mart.league_summary`, `mart.fact_matches`, `mart.standings_evolution`, auditoria `raw.*` | TTL 5-10 min por filtro | sem paginacao; ordenacao fixa |
| `/api/competition/standings` | classificacao por rodada | `leagueId`, `season`, `round` | `mart.fact_standings_snapshots`, `mart.dim_round`, `mart.dim_team` | TTL 5-10 min | sem paginacao; ordenacao por posicao |
| `/api/competition/evolution` | evolucao de pontos/posicao | `leagueId`, `season`, `teamId` | `mart.standings_evolution`, `mart.dim_round` | TTL 10 min | sem paginacao; ordem por rodada |
| `/api/matches` | lista/calendario de partidas | `leagueId`, `season`, `round`, `teamId`, `startDate`, `endDate`, `page`, `pageSize` | `mart.fact_matches`, `mart.dim_team`, `mart.dim_date` | TTL 5 min | paginada; ordem por data |
| `/api/matches/{fixtureId}` | detalhe de partida | `fixtureId` | `mart.fact_matches`, `mart.dim_venue`, fallback `raw.fixtures` | TTL 5 min | sem paginacao |
| `/api/matches/{fixtureId}/events` | timeline de eventos | `fixtureId` | `mart.fact_match_events`, fallback `raw.match_events` | TTL 5 min | sem paginacao; ordem cronologica |
| `/api/matches/{fixtureId}/lineups` | escalacoes e banco | `fixtureId` | `mart.fact_fixture_lineups`, `mart.dim_player`, `mart.dim_team` | TTL 15 min | sem paginacao; ordem por time |
| `/api/matches/{fixtureId}/player-stats` | stats individuais por partida | `fixtureId`, `sortBy`, `sortDir` | `mart.fact_fixture_player_stats`, `mart.dim_player`, `mart.dim_team` | TTL 10 min | paginacao opcional; ordenacao dinamica |
| `/api/teams` | lista de clubes | `leagueId`, `season`, `page`, `pageSize` | `mart.dim_team`, `mart.fact_matches` | TTL 1h | paginada; ordem alfabetica |
| `/api/teams/{teamId}/profile` | perfil do clube | `teamId`, `season` | `mart.dim_team`, `mart.fact_matches`, `mart.standings_evolution`, `mart.team_monthly_stats` | TTL 10 min | sem paginacao |
| `/api/teams/{teamId}/form` | forma recente do clube | `teamId`, `season`, `lastN` | `mart.fact_matches`, `mart.team_match_goals_monthly` | TTL 5 min | sem paginacao; ordem cronologica |
| `/api/teams/compare` | comparativo clube vs clube | `teamA`, `teamB`, `season`, `lastN` | `mart.fact_matches`, `mart.standings_evolution`, `mart.team_monthly_stats`, `mart.head_to_head_summary` | TTL 5 min | sem paginacao; ordem por metrica |
| `/api/players` | lista de jogadores com filtros | `leagueId`, `season`, `teamId`, `page`, `pageSize`, `sortBy`, `sortDir` | `mart.player_season_summary`, `mart.player_90_metrics`, `mart.dim_player` | TTL 10 min | paginada; ordenacao dinamica |
| `/api/players/{playerId}/profile` | perfil consolidado de jogador | `playerId`, `season` | `mart.dim_player`, `mart.player_season_summary`, `mart.player_90_metrics` | TTL 10 min | sem paginacao |
| `/api/players/{playerId}/matches` | historico de partidas do jogador | `playerId`, `season`, `lastN`, `page`, `pageSize` | `mart.player_match_summary`, `mart.fact_fixture_player_stats` | TTL 5 min | paginada; ordem por data desc |
| `/api/players/compare` | comparativo jogador vs jogador | `playerIds`, `season`, `lastN`, `mode` | `mart.player_season_summary`, `mart.player_match_summary`, `mart.player_90_metrics` | TTL 5 min | sem paginacao; ordenacao por metrica |
| `/api/rankings/players` | liderancas de jogadores | `leagueId`, `season`, `metric`, `lastN`, `page`, `pageSize` | `mart.player_season_summary`, `mart.player_match_summary`, `mart.player_90_metrics` | TTL 5 min | paginada; ordenacao por metrica desc |
| `/api/rankings/teams` | liderancas de clubes | `leagueId`, `season`, `metric`, `month`, `page`, `pageSize` | `mart.team_monthly_stats`, `mart.team_match_goals_monthly`, `mart.standings_evolution` | TTL 10 min | paginada; ordenacao por metrica desc |
| `/api/rankings/coaches` | ranking de tecnicos | `leagueId`, `season`, `metric`, `page`, `pageSize` | `mart.coach_performance_summary`, `mart.dim_coach` | TTL 15 min | paginada; ordenacao por metrica desc |
| `/api/rankings/h2h` | ranking de dominancia em confrontos | `leagueId`, `season`, `page`, `pageSize` | `mart.head_to_head_summary` | TTL 15 min | paginada; ordenacao por indice de dominancia |
| `/api/market/transfers` | feed e ranking de transferencias | `season`, `teamId`, `playerId`, `startDate`, `endDate`, `page`, `pageSize` | `raw.player_transfers` | TTL 30 min | paginada; ordem por data desc |
| `/api/availability/sidelined` | indisponibilidade por time/jogador | `season`, `teamId`, `playerId`, `page`, `pageSize` | `raw.team_sidelined`, `mart.dim_team`, `mart.dim_player` | TTL 30 min | paginada; ordem por data/status |
| `/api/h2h` | confronto entre dois times | `teamA`, `teamB`, `season`, `lastN` | `mart.head_to_head_summary`, `raw.head_to_head_fixtures`, `mart.fact_matches` | TTL 10 min | sem paginacao; ordem cronologica |
| `/api/coaches/{coachId}` | perfil de tecnico | `coachId`, `season` | `mart.dim_coach`, `mart.coach_performance_summary`, `raw.team_coaches` | TTL 30 min | sem paginacao |
| `/api/audit/coverage` | painel tecnico de cobertura | `leagueId`, `season` | `mart.fact_matches`, `mart.fact_match_events`, `mart.fact_fixture_player_stats`, `mart.fact_fixture_lineups`, apoio `raw.*` | TTL 5 min | sem paginacao |

## 11) O que da para entregar AGORA vs backlog de dados/modelagem

## 11.1 Entregavel AGORA (com dados atuais)

- Home executiva com KPIs de temporada e cobertura.
- Competicao com standings por rodada e evolucao de pontos.
- Calendario de partidas e match center com lineups, player stats e eventos.
- Perfil de clube com forma, desempenho mensal e contribuidores.
- Lista e perfil de jogadores com comparativo e ranking por 90.
- Central de rankings de jogadores, clubes, tecnicos e head-to-head.
- Modulo de mercado com `raw.player_transfers`.
- Modulo de disponibilidade com `raw.team_sidelined`.

## 11.2 Backlog para ficar ainda mais completo

- Cobertura mais alta de metadados de fixture (`attendance`, `weather`, `referee`) em `raw.fixtures` e propagacao consistente para `mart.fact_matches`.
- Popular `mart.team_performance_monthly` (hoje com `0` linhas) para ampliar rankings mensais prontos.
- Fortalecer padronizacao de posicao/minutagem para comparativos por funcao de jogo em telas de jogadores.
- Consolidar indicadores de eficiencia/acuracia diretamente em `mart.*` quando dependerem de transformacao adicional a partir de `raw.match_statistics` e `raw.fixture_player_statistics`.
- Popular `raw.provider_entity_map` para trilhas completas de reconciliacao entre providers em auditoria.

## 12) Prioridade de execucao recomendada

1. Entregar Home, Competicao, Partidas e Jogadores com filtros globais e cobertura explicita.  
2. Entregar Clubes e Rankings central (jogadores, clubes, tecnicos, h2h).  
3. Entregar Mercado/Disponibilidade e modulo de auditoria de cobertura.  
4. Fechar backlog de modelagem para eficiencia/acuracia avancada e enriquecer metadados de fixture.  

Resultado esperado: um frontend analitico completo, com navegacao clara, comparativos profundos e rastreabilidade de dados por tabela, sem depender de suposicoes fora do que ja esta ingerido hoje.

## 13) Matriz Completa de Metricas Disponiveis no Banco

Esta secao eleva o manual para nivel analitico, detalhando metrica por metrica com rastreabilidade de origem.

### 13.1 Criterios da matriz

- `Ranking direto`: a metrica pode ser ordenada diretamente em endpoint/lista sem transformacao adicional.
- `Permite normalizacao`: a metrica pode ser ajustada por minutos, partidas ou periodo (ou ja nasce normalizada por 90).
- `Limitacao de cobertura`: observacao baseada no preenchimento atual das tabelas (non-null e/ou atividade da metrica).
- Prioridade de consumo: `mart.*` para produto; `raw.*` para auditoria, cobertura e fallback.

### 13.2 Metricas de Jogador (por jogo)

Fontes principais: `mart.fact_fixture_player_stats` e `mart.player_match_summary`.

| Metrica funcional | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura atual |
|---|---|---|---|---|---|---|
| Titularidade | `is_starter` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Nao (filtro/segmentacao) | Nao se aplica | preenchida 100%; `TRUE` em 48,46% |
| Minutos jogados | `minutes_played` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (base para por 90) | preenchida em 69,93% |
| Gols | `goals` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90, por partida) | preenchida 100%; valor > 0 em 5,02% |
| Assistencias | `assists` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90, por partida) | preenchida 100%; valor > 0 em 4,08% |
| Finalizacoes totais | `shots_total` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim | preenchida em 33,25% |
| Finalizacoes no alvo | `shots_on_goal` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim | preenchida em 16,01% |
| Passes totais | `passes_total` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90) | preenchida em 68,77% |
| Passes-chave | `key_passes` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90) | preenchida em 27,79% |
| Desarmes | `tackles` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90) | preenchida em 37,51% |
| Interceptacoes | `interceptions` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90) | preenchida em 24,43% |
| Duelos | `duels` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90) | preenchida em 58,87% |
| Faltas cometidas | `fouls_committed` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90) | preenchida em 32,82% |
| Cartoes amarelos | `yellow_cards` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (taxa por jogo) | preenchida em 9,76% |
| Cartoes vermelhos | `red_cards` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (taxa por jogo) | preenchida em 0,33% |
| Defesas do goleiro | `goalkeeper_saves` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por 90 para goleiros) | preenchida em 4,12% |
| Clean sheet | `clean_sheets` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (taxa por jogo) | nao preenchida atualmente (0%) |
| xG individual | `xg` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (xG por 90) | nao preenchida atualmente (0%) |
| Rating | `rating` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (media movel) | preenchida em 68,38% |
| Pacote bruto de stats | `statistics` (JSON) | jogador-partida | `mart.fact_fixture_player_stats` | Nao direto (exige parse) | Sim, apos parse | `statistics` preenchida 100%, mas com conteudo nao-vazio em 70,15% |

### 13.3 Metricas de Jogador (temporada)

Fonte principal: `mart.player_season_summary`.

| Metrica funcional | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura atual |
|---|---|---|---|---|---|---|
| Jogos | `matches` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (filtro minimo de amostra) | preenchida 100% |
| Minutos jogados | `minutes_played` | jogador-temporada | `mart.player_season_summary` | Sim | Sim | preenchida 100%; > 0 em 81,46% |
| Gols | `goals` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 16,06% |
| Assistencias | `assists` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 15,89% |
| Finalizacoes totais | `shots_total` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 58,77% |
| Finalizacoes no alvo | `shots_on_goal` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 39,57% |
| Passes totais | `passes_total` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 80,13% |
| Passes-chave | `key_passes` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 53,81% |
| Desarmes | `tackles` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 61,26% |
| Interceptacoes | `interceptions` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 49,67% |
| Duelos | `duels` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90) | preenchida 100%; > 0 em 75,66% |
| Faltas cometidas | `fouls_committed` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por jogo) | preenchida 100%; > 0 em 60,60% |
| Cartoes amarelos | `yellow_cards` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (taxa por jogo) | preenchida 100%; > 0 em 33,44% |
| Cartoes vermelhos | `red_cards` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (taxa por jogo) | preenchida 100%; > 0 em 1,49% |
| Defesas do goleiro | `goalkeeper_saves` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (por 90 de goleiro) | preenchida 100%; > 0 em 5,13% |
| Clean sheets | `clean_sheets` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (taxa por jogo) | preenchida 100%, mas sem valores > 0 |
| xG individual | `xg` | jogador-temporada | `mart.player_season_summary` | Sim | Sim (xG por 90) | preenchida 100%, mas sem valores > 0 |
| Rating medio | `avg_rating` | jogador-temporada | `mart.player_season_summary` | Sim | Nao (ja e agregado) | preenchida em 79,97% |

### 13.4 Metricas de Jogador (por 90)

Fonte principal: `mart.player_90_metrics`.

| Metrica funcional | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura atual |
|---|---|---|---|---|---|---|
| Jogos | `matches` | jogador-temporada | `mart.player_90_metrics` | Sim | Sim (filtro de amostra) | preenchida 100% |
| Minutos jogados | `minutes_played` | jogador-temporada | `mart.player_90_metrics` | Sim | Base para por 90 | preenchida 100%; > 0 em 81,46% |
| Gols (total) | `goals` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| Assistencias (total) | `assists` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| Finalizacoes totais (total) | `shots_total` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| Finalizacoes no alvo (total) | `shots_on_goal` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| Passes-chave (total) | `key_passes` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| Desarmes (total) | `tackles` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| Interceptacoes (total) | `interceptions` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| Duelos (total) | `duels` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100% |
| xG (total) | `xg` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja possui equivalente por 90 | preenchida 100%, sem valores > 0 |
| Rating medio | `avg_rating` | jogador-temporada | `mart.player_90_metrics` | Sim | Nao (ja e media) | preenchida em 79,97% |
| Gols por 90 | `goals_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46% |
| Assistencias por 90 | `assists_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46% |
| Finalizacoes por 90 | `shots_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46% |
| Finalizacoes no alvo por 90 | `shots_on_goal_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46% |
| Passes-chave por 90 | `key_passes_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46% |
| Desarmes por 90 | `tackles_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46% |
| Interceptacoes por 90 | `interceptions_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46% |
| xG por 90 | `xg_per_90` | jogador-temporada | `mart.player_90_metrics` | Sim | Ja normalizada | preenchida em 81,46%, sem valores > 0 |

### 13.5 Metricas de Time (por jogo)

Fonte principal: `raw.match_statistics` (camada de auditoria/team stats por fixture-time).

| Metrica funcional | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura atual |
|---|---|---|---|---|---|---|
| Finalizacoes no alvo | `shots_on_goal` | time-partida | `raw.match_statistics` | Sim | Sim (por jogo, por 90 agregado) | preenchida 100% |
| Finalizacoes para fora | `shots_off_goal` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Finalizacoes totais | `total_shots` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Chutes bloqueados | `blocked_shots` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Chutes dentro da area | `shots_inside_box` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Chutes fora da area | `shots_outside_box` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Faltas | `fouls` | time-partida | `raw.match_statistics` | Sim | Sim (taxa por jogo) | preenchida 100% |
| Escanteios | `corner_kicks` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Impedimentos | `offsides` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida em 93,68% |
| Posse de bola (%) | `ball_possession` | time-partida | `raw.match_statistics` | Sim | Nao (ja e taxa) | preenchida 100% |
| Cartoes amarelos | `yellow_cards` | time-partida | `raw.match_statistics` | Sim | Sim (taxa por jogo) | preenchida em 99,74% |
| Cartoes vermelhos | `red_cards` | time-partida | `raw.match_statistics` | Sim | Sim (taxa por jogo) | preenchida em 30,79% |
| Defesas do goleiro | `goalkeeper_saves` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Passes totais | `total_passes` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Passes corretos | `passes_accurate` | time-partida | `raw.match_statistics` | Sim | Sim | preenchida 100% |
| Precisao de passes (%) | `passes_pct` | time-partida | `raw.match_statistics` | Sim | Nao (ja e taxa) | preenchida 100% |

### 13.6 Metricas de Time (mensal)

Fonte principal: `mart.team_monthly_stats`.

| Metrica funcional | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura atual |
|---|---|---|---|---|---|---|
| Gols marcados no mes | `goals_for` | time-mes | `mart.team_monthly_stats` | Sim | Sim (por jogo do mes) | preenchida 100% |
| Gols sofridos no mes | `goals_against` | time-mes | `mart.team_monthly_stats` | Sim | Sim (por jogo do mes) | preenchida 100% |
| Jogos no mes | `matches` | time-mes | `mart.team_monthly_stats` | Sim | Base para taxa mensal | preenchida 100% |
| Vitorias no mes | `wins` | time-mes | `mart.team_monthly_stats` | Sim | Sim (taxa de vitoria) | preenchida 100% |
| Empates no mes | `draws` | time-mes | `mart.team_monthly_stats` | Sim | Sim (taxa de empate) | preenchida 100% |
| Derrotas no mes | `losses` | time-mes | `mart.team_monthly_stats` | Sim | Sim (taxa de derrota) | preenchida 100% |
| Pontos no mes | `points` | time-mes | `mart.team_monthly_stats` | Sim | Sim (`pontos/jogo`) | preenchida 100% |
| Saldo de gols no mes | `goal_diff` | time-mes | `mart.team_monthly_stats` | Sim | Sim (saldo/jogo) | preenchida 100% |

### 13.7 Metricas de Jogador em JSON bruto (`raw.fixture_player_statistics`)

Fonte principal: `raw.fixture_player_statistics`.

Observacao: aqui a coluna tecnica e semiestruturada (`statistics`). As metricas abaixo existem hoje no banco como chaves dentro de `statistics[].developer_name`.

| Metrica bruta observada | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura atual |
|---|---|---|---|---|---|---|
| Accurate Crosses | `statistics[].developer_name = ACCURATE_CROSSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim, apos parse | presente em 13,39% das linhas |
| Accurate Passes | `statistics[].developer_name = ACCURATE_PASSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim, apos parse | presente em 68,14% |
| Accurate Passes Percentage | `statistics[].developer_name = ACCURATE_PASSES_PERCENTAGE` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao (ja e taxa) | presente em 66,54% |
| Aerials | `statistics[].developer_name = AERIALS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,29% |
| Aerials Lost | `statistics[].developer_name = AERIALS_LOST` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 6,80% |
| Aerials Won | `statistics[].developer_name = AERIALS_WON` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 33,24% |
| Aerials Won Percentage | `statistics[].developer_name = AERIALS_WON_PERCENTAGE` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao (ja e taxa) | presente em 0,29% |
| Assists | `statistics[].developer_name = ASSISTS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 3,63% |
| Backward Passes | `statistics[].developer_name = BACKWARD_PASSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,78% |
| Ball Recovery | `statistics[].developer_name = BALL_RECOVERY` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,73% |
| Big Chances Created | `statistics[].developer_name = BIG_CHANCES_CREATED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 21,33% |
| Big Chances Missed | `statistics[].developer_name = BIG_CHANCES_MISSED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 4,35% |
| Blocked Shots | `statistics[].developer_name = BLOCKED_SHOTS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 11,71% |
| Captain | `statistics[].developer_name = CAPTAIN` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao se aplica (flag) | presente em 4,39% |
| Chances Created | `statistics[].developer_name = CHANCES_CREATED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,23% |
| Clearance Offline | `statistics[].developer_name = CLEARANCE_OFFLINE` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,37% |
| Clearances | `statistics[].developer_name = CLEARANCES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 34,93% |
| Dispossessed | `statistics[].developer_name = DISPOSSESSED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 25,99% |
| Dribbled Attempts | `statistics[].developer_name = DRIBBLED_ATTEMPTS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 32,42% |
| Dribbled Past | `statistics[].developer_name = DRIBBLED_PAST` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 23,18% |
| Duels Lost | `statistics[].developer_name = DUELS_LOST` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 58,19% |
| Duels Won | `statistics[].developer_name = DUELS_WON` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 58,99% |
| Duels Won Percentage | `statistics[].developer_name = DUELS_WON_PERCENTAGE` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao (ja e taxa) | presente em 0,26% |
| Error Lead To Goal | `statistics[].developer_name = ERROR_LEAD_TO_GOAL` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao se aplica (evento raro) | presente em 0,44% |
| Fouls | `statistics[].developer_name = FOULS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 34,86% |
| Fouls Drawn | `statistics[].developer_name = FOULS_DRAWN` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 32,79% |
| Goalkeeper Goals Conceded | `statistics[].developer_name = GOALKEEPER_GOALS_CONCEDED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 3,28% |
| Goals | `statistics[].developer_name = GOALS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 5,01% |
| Goals Conceded | `statistics[].developer_name = GOALS_CONCEDED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 38,97% |
| Good High Claim | `statistics[].developer_name = GOOD_HIGH_CLAIM` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,03% |
| Hit Woodwork | `statistics[].developer_name = HIT_WOODWORK` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 1,47% |
| Interceptions | `statistics[].developer_name = INTERCEPTIONS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 24,47% |
| Key Passes | `statistics[].developer_name = KEY_PASSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 27,34% |
| Long Balls | `statistics[].developer_name = LONG_BALLS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 50,58% |
| Long Balls Won | `statistics[].developer_name = LONG_BALLS_WON` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 41,46% |
| Long Balls Won Percentage | `statistics[].developer_name = LONG_BALLS_WON_PERCENTAGE` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao (ja e taxa) | presente em 0,57% |
| Minutes Played | `statistics[].developer_name = MINUTES_PLAYED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 69,69% |
| Offsides | `statistics[].developer_name = OFFSIDES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 5,53% |
| Own Goals | `statistics[].developer_name = OWN_GOALS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,13% |
| Passes | `statistics[].developer_name = PASSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 68,73% |
| Passes In Final Third | `statistics[].developer_name = PASSES_IN_FINAL_THIRD` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,71% |
| Penalties Committed | `statistics[].developer_name = PENALTIES_COMMITTED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,59% |
| Penalties Misses | `statistics[].developer_name = PENALTIES_MISSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,15% |
| Penalties Saved | `statistics[].developer_name = PENALTIES_SAVED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,13% |
| Penalties Scored | `statistics[].developer_name = PENALTIES_SCORED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,43% |
| Penalties Won | `statistics[].developer_name = PENALTIES_WON` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,45% |
| Possession Lost | `statistics[].developer_name = POSSESSION_LOST` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,88% |
| Punches | `statistics[].developer_name = PUNCHES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 1,16% |
| Rating | `statistics[].developer_name = RATING` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao (ja e score) | presente em 68,32% |
| Red Cards | `statistics[].developer_name = REDCARDS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,60% |
| Saves | `statistics[].developer_name = SAVES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 4,19% |
| Saves Inside Box | `statistics[].developer_name = SAVES_INSIDE_BOX` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 3,67% |
| Shots Blocked | `statistics[].developer_name = SHOTS_BLOCKED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 12,57% |
| Shots Off Target | `statistics[].developer_name = SHOTS_OFF_TARGET` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 18,64% |
| Shots On Target | `statistics[].developer_name = SHOTS_ON_TARGET` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 15,92% |
| Shots Total | `statistics[].developer_name = SHOTS_TOTAL` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 33,41% |
| Successful Crosses Percentage | `statistics[].developer_name = SUCCESSFUL_CROSSES_PERCENTAGE` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao (ja e taxa) | presente em 0,15% |
| Successful Dribbles | `statistics[].developer_name = SUCCESSFUL_DRIBBLES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 21,65% |
| Tackles | `statistics[].developer_name = TACKLES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 38,26% |
| Tackles Won | `statistics[].developer_name = TACKLES_WON` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,41% |
| Tackles Won Percentage | `statistics[].developer_name = TACKLES_WON_PERCENTAGE` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Nao (ja e taxa) | presente em 0,41% |
| Through Balls | `statistics[].developer_name = THROUGH_BALLS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 1,32% |
| Through Balls Won | `statistics[].developer_name = THROUGH_BALLS_WON` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,71% |
| Total Crosses | `statistics[].developer_name = TOTAL_CROSSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 30,40% |
| Total Duels | `statistics[].developer_name = TOTAL_DUELS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 64,22% |
| Touches | `statistics[].developer_name = TOUCHES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 54,25% |
| Yellow Cards | `statistics[].developer_name = YELLOWCARDS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 11,23% |
| Yellow-Red Cards | `statistics[].developer_name = YELLOWRED_CARDS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto (parse obrigatorio) | Sim | presente em 0,27% |

### 13.8 Metricas Disciplinares

| Metrica | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura |
|---|---|---|---|---|---|---|
| Cartoes amarelos (jogador-jogo) | `yellow_cards` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por jogo) | cobertura parcial (9,76%) |
| Cartoes vermelhos (jogador-jogo) | `red_cards` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim (por jogo) | cobertura muito baixa (0,33%) |
| Faltas cometidas (jogador-jogo) | `fouls_committed` | jogador-partida | `mart.fact_fixture_player_stats`, `mart.player_match_summary` | Sim | Sim | cobertura parcial (32,82%) |
| Cartoes amarelos (time-jogo) | `yellow_cards` | time-partida | `raw.match_statistics` | Sim | Sim (taxa por jogo) | cobertura alta (99,74%) |
| Cartoes vermelhos (time-jogo) | `red_cards` | time-partida | `raw.match_statistics` | Sim | Sim (taxa por jogo) | cobertura parcial (30,79%) |
| Faltas (time-jogo) | `fouls` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura alta (100%) |
| Cartao amarelo (JSON bruto) | `statistics[].developer_name = YELLOWCARDS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 11,23% |
| Cartao vermelho (JSON bruto) | `statistics[].developer_name = REDCARDS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 0,60% |
| Cartao amarelo-vermelho (JSON bruto) | `statistics[].developer_name = YELLOWRED_CARDS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 0,27% |
| Penalti cometido (JSON bruto) | `statistics[].developer_name = PENALTIES_COMMITTED` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 0,59% |

### 13.9 Metricas Ofensivas

| Metrica | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura |
|---|---|---|---|---|---|---|
| Gols | `goals` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim (por 90) | cobertura alta em agregados; ativa em minoria dos jogos |
| Assistencias | `assists` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim (por 90) | cobertura alta em agregados; ativa em minoria dos jogos |
| Finalizacoes totais | `shots_total` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim (por 90) | parcial no jogo, alta no agregado |
| Finalizacoes no alvo | `shots_on_goal` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim (por 90) | parcial no jogo |
| Passes-chave | `key_passes` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim (por 90) | parcial no jogo |
| xG | `xg`, `xg_per_90` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Ja normalizada em `_per_90` | indisponivel atualmente (sem valores > 0) |
| Chutes totais de time | `total_shots` | time-partida | `raw.match_statistics` | Sim | Sim (por jogo) | cobertura alta |
| Chutes no alvo de time | `shots_on_goal` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura alta |
| Chutes fora de time | `shots_off_goal` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura alta |
| Chutes dentro/fora da area | `shots_inside_box`, `shots_outside_box` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura alta |

### 13.10 Metricas Defensivas

| Metrica | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura |
|---|---|---|---|---|---|---|
| Desarmes | `tackles`, `tackles_per_90` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim | parcial no jogo, alta no agregado |
| Interceptacoes | `interceptions`, `interceptions_per_90` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim | parcial no jogo, alta no agregado |
| Duelos | `duels` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim | parcial no jogo, alta no agregado |
| Defesas do goleiro (jogador) | `goalkeeper_saves` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary` | Sim | Sim (por 90 goleiros) | cobertura baixa no jogo (4,12%), adequada para universo de goleiros |
| Defesas do goleiro (time) | `goalkeeper_saves` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura alta |
| Gols sofridos no mes | `goals_against` | time-mes | `mart.team_monthly_stats` | Sim | Sim (por jogo mensal) | cobertura alta |
| Saldo de gols | `goal_diff` | time-mes | `mart.team_monthly_stats` | Sim | Sim | cobertura alta |
| Clean sheets | `clean_sheets` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary` | Sim | Sim | indisponivel atualmente (sem preenchimento efetivo) |

### 13.11 Metricas de Posse/Construcao

| Metrica | Nome tecnico da coluna | Granularidade | Tabela fonte | Ranking direto | Permite normalizacao | Limitacao de cobertura |
|---|---|---|---|---|---|---|
| Passes totais (jogador) | `passes_total` | jogador-partida / jogador-temporada | `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.player_season_summary` | Sim | Sim (por 90) | cobertura parcial no jogo (68,77%) |
| Passes totais (time) | `total_passes` | time-partida | `raw.match_statistics` | Sim | Sim (por jogo) | cobertura alta |
| Passes corretos | `passes_accurate` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura alta |
| Precisao de passes | `passes_pct` | time-partida | `raw.match_statistics` | Sim | Nao (ja e taxa) | cobertura alta |
| Posse de bola | `ball_possession` | time-partida | `raw.match_statistics` | Sim | Nao (ja e taxa) | cobertura alta |
| Escanteios | `corner_kicks` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura alta |
| Impedimentos | `offsides` | time-partida | `raw.match_statistics` | Sim | Sim | cobertura parcial (93,68%) |
| Construcao longa (JSON bruto) | `statistics[].developer_name = LONG_BALLS` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 50,58% |
| Cruzamentos totais (JSON bruto) | `statistics[].developer_name = TOTAL_CROSSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 30,40% |
| Cruzamentos certos (JSON bruto) | `statistics[].developer_name = ACCURATE_CROSSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 13,39% |
| Passes para tras (JSON bruto) | `statistics[].developer_name = BACKWARD_PASSES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 0,78% |
| Toques na bola (JSON bruto) | `statistics[].developer_name = TOUCHES` | jogador-partida | `raw.fixture_player_statistics` | Nao direto | Sim, apos parse | cobertura 54,25% |

## 14) Metricas Derivadas e Indices Analiticos Propostos

Objetivo: sair do nivel descritivo e construir uma camada de scorecards comparaveis entre clubes, jogadores e confrontos.

| Indice proposto | Formula conceitual (sem SQL) | Tabelas utilizadas | Calculavel hoje apenas com `mart.*` | Exige transformacao adicional | Limitacoes atuais |
|---|---|---|---|---|---|
| Indice Ofensivo de Clube | combinacao ponderada de `gols_for por jogo`, `total_shots por jogo`, `shots_on_goal por jogo`, `key_passes do elenco` | `mart.team_monthly_stats`, `raw.match_statistics`, `mart.player_match_summary` | Parcial | Sim (padronizar pesos e unir niveis time/jogador) | dependencia de `raw.match_statistics` para volume de finalizacao por jogo |
| Indice Defensivo de Clube | combinacao inversa de `gols_against por jogo`, `goalkeeper_saves`, `interceptions/tackles agregados` | `mart.team_monthly_stats`, `raw.match_statistics`, `mart.player_match_summary` | Parcial | Sim | `clean_sheets` nao esta preenchido no mart de jogadores |
| Indice de Contribuicao Ofensiva do Jogador | `z(goals_per_90) + z(assists_per_90) + z(key_passes_per_90) + z(shots_per_90)` | `mart.player_90_metrics`, `mart.player_match_summary` | Sim | Sim (normalizacao z-score por liga/temporada) | sensivel a jogadores com poucos minutos se nao houver filtro minimo |
| Indice de Disciplina (Jogador) | penalizacao por `yellow_cards` e `red_cards` por 90 + `fouls_committed` por jogo | `mart.player_season_summary`, `mart.player_90_metrics` | Sim | Sim | eventos disciplinares sao esparsos em nivel jogo |
| Indice de Disciplina (Clube) | penalizacao por `yellow_cards` e `red_cards` por jogo do time | `raw.match_statistics`, `mart.team_monthly_stats` | Nao | Sim | `red_cards` em `raw.match_statistics` com cobertura parcial |
| Indice de Dominancia H2H | combinacao de `%vitorias`, saldo medio e pontos medios no confronto direto | `mart.head_to_head_summary`, `mart.fact_matches` | Sim | Sim (padrao de escala e janela temporal) | depende de definir janela fixa (historico total vs ultimos N jogos) |
| Indice de Forma Recente (Clube) | media movel ponderada de pontos nos ultimos N jogos + tendencia de saldo | `mart.fact_matches`, `mart.standings_evolution` | Sim | Sim | exige regra de janela N e peso temporal |
| Indice de Forma Recente (Jogador) | media movel ponderada de `rating`, `goals`, `assists`, `minutes_played` nos ultimos N jogos | `mart.player_match_summary` | Sim | Sim | `rating` com cobertura parcial no nivel jogo |
| Ritmo de Pontuacao Ajustado | `points por jogo` ajustado por volatilidade mensal e fase da temporada | `mart.team_monthly_stats`, `mart.standings_evolution` | Sim | Sim | requer padrao de ajuste de volatilidade |
| Indice de Eficiencia de Finalizacao (Clube) | `shots_on_goal / total_shots` combinado com `goals_for / total_shots` | `raw.match_statistics`, `mart.team_monthly_stats` | Nao | Sim | mistura granularidade de jogo e mes; requer consolidacao no mart |
| Indice de Criacao de Chances (Jogador) | `key_passes_per_90 + big chances created` (quando parseado de JSON bruto) | `mart.player_90_metrics`, `raw.fixture_player_statistics` | Parcial | Sim | `BIG_CHANCES_CREATED` ainda em JSON bruto |
| Indice de Solidez Defensiva (Clube) | `-gols_against por jogo + interceptacoes/desarmes agregados + defesas` | `mart.team_monthly_stats`, `raw.match_statistics`, `mart.player_season_summary` | Parcial | Sim | consolidacao intercamadas necessaria |
| Indice de Dependencia de Jogador | `% dos gols/assistencias do time explicados por um jogador` | `mart.player_season_summary`, `mart.team_monthly_stats`, `mart.fact_matches` | Sim | Sim | requer reconciliar gols de time entre fontes |
| Indice de Consistencia de Performance | `1 - coeficiente de variacao` da metrica alvo (rating, pontos, gols) em janela | `mart.player_match_summary`, `mart.fact_matches`, `mart.team_monthly_stats` | Sim | Sim | janelas curtas geram ruido |
| Indice de Volatilidade | desvio-padrao normalizado de pontos/gols/rating em janela movel | `mart.fact_matches`, `mart.player_match_summary` | Sim | Sim | depende de filtros minimos de amostra |

## 15) Camada de Insight e Deteccao de Tendencias

Objetivo: transformar o produto de visualizacao descritiva em plataforma analitica orientada a sinais, mudancas de tendencia e anomalias.

### 15.1 Regras de insight (conceitual)

| Insight automatico | Regra conceitual | Fonte principal | Granularidade | Implementacao agora |
|---|---|---|---|---|
| Alerta de subida ofensiva de clube | disparar quando `goals_for por jogo` subir acima de X% vs media dos ultimos 3 meses | `mart.team_monthly_stats` | time-mes | Sim |
| Alerta de queda ofensiva de clube | disparar quando `goals_for por jogo` cair acima de X% vs media movel | `mart.team_monthly_stats` | time-mes | Sim |
| Alerta de queda de eficiencia de finalizacao | queda de `shots_on_goal/total_shots` acima de limiar em janela de N jogos | `raw.match_statistics` | time-jogo | Sim (via BFF com consolidacao) |
| Outlier positivo de jogador (liga) | jogador acima de +2 desvios em `goals_per_90`, `assists_per_90` ou `rating` | `mart.player_90_metrics` | jogador-temporada | Sim |
| Outlier negativo de jogador (liga) | jogador abaixo de -2 desvios em metricas-chave da posicao | `mart.player_90_metrics`, `mart.player_season_summary` | jogador-temporada | Parcial (posicao fina depende de padronizacao) |
| Tendencia de pontos do clube | classificar como melhora/estavel/queda pela inclinacao de `points_accumulated` nas ultimas N rodadas | `mart.standings_evolution` | time-rodada | Sim |
| Tendencia de forma de jogador | classificar melhora/queda por media movel de `rating` e contribuicoes nos ultimos N jogos | `mart.player_match_summary` | jogador-jogo | Sim |
| Comparacao contra media da competicao (clube) | diferenca percentual de metrica do clube vs media da liga no mesmo recorte | `mart.team_monthly_stats`, `mart.fact_matches` | time-periodo | Sim |
| Comparacao contra media da competicao (jogador) | diferenca de jogador vs media dos pares de mesma competicao/temporada | `mart.player_season_summary`, `mart.player_90_metrics` | jogador-temporada | Sim |
| Destaques automaticos de Home | selecionar top historias da rodada/mes por variacao relevante e impacto | `mart.standings_evolution`, `mart.player_match_summary`, `mart.team_monthly_stats` | liga-periodo | Sim |
| Indicador de consistencia do clube | score baseado em baixa variancia de pontos e saldo por janela | `mart.fact_matches`, `mart.team_monthly_stats` | time-periodo | Sim |
| Indicador de volatilidade do clube | score baseado em alta variancia de pontos e gols por janela | `mart.fact_matches`, `mart.team_monthly_stats` | time-periodo | Sim |
| Indicador de consistencia do jogador | score de estabilidade de `rating` e participacao ao longo dos jogos | `mart.player_match_summary` | jogador-jogo | Sim |
| Indicador de dependencia de jogador | `% gols do time` e `% participacao ofensiva` concentrados em 1 atleta | `mart.player_season_summary`, `mart.team_monthly_stats`, `mart.fact_matches` | time-temporada | Sim |
| Alerta disciplinar de risco | disparar quando media de cartoes/faltas cresce acima do baseline do clube | `raw.match_statistics`, `mart.player_match_summary` | time-jogo / jogador-jogo | Sim |
| Alerta de fragilidade defensiva | detectar aumento consistente de `goals_against` e queda de recuperacao defensiva | `mart.team_monthly_stats`, `raw.match_statistics`, `mart.player_match_summary` | time-periodo | Sim |
| Alerta de goleiro em destaque | detectar sequencia de jogos com `goalkeeper_saves` acima da media da liga | `raw.match_statistics`, `mart.player_season_summary` | jogador-jogo / jogador-temporada | Parcial (identificacao de goleiros por posicao exige cuidado) |
| Insight de dominancia em confronto direto | classificar h2h em equilibrado, vantagem leve, dominancia forte | `mart.head_to_head_summary` | par de times | Sim |
| Alerta de sobrecarga de elenco | queda de performance associada a alta rotacao e baixa minutagem media dos titulares | `mart.fact_fixture_lineups`, `mart.fact_matches`, `mart.player_match_summary` | time-jogo | Sim |
| Alerta de qualidade de dado para UI | sinalizar quando cobertura de modulo cair abaixo de limiar (eventos, lineups, player stats) | `mart.fact_matches`, `mart.fact_match_events`, `mart.fact_fixture_lineups`, `mart.fact_fixture_player_stats` | liga-temporada | Sim |

### 15.2 Como operacionalizar a camada de insight na BFF

- Cada insight deve ser calculado na BFF e entregue como objeto tipado, com:
- `insight_id`
- `severidade` (`info`, `warning`, `critical`)
- `explicacao` (texto curto, sem SQL)
- `evidencias` (metricas numericas usadas)
- `periodo_referencia`
- `fonte_dados` (`mart.*` e, quando necessario, `raw.*`)
- O frontend consome insights como componente reutilizavel de cards e feed analitico.
- Recomendacao de cache para insights:
- Home/Rankings: TTL 5 a 10 min.
- Perfil de clube/jogador: TTL 5 min.
- Auditoria de cobertura: TTL 5 min com invalidador pos-ingestao.

### 15.3 Dependencias futuras para evoluir a inteligencia analitica

- Popular `xg` e `xg_per_90` com valores efetivos no mart para habilitar modelos ofensivos mais robustos.
- Popular `clean_sheets` efetivamente em nivel de jogador/goleiro para indice defensivo mais fiel.
- Materializar no `mart.*` parte das metricas que hoje estao apenas no JSON de `raw.fixture_player_statistics` (ex.: `BIG_CHANCES_CREATED`, `SUCCESSFUL_DRIBBLES`, `TOTAL_CROSSES`).
- Padronizar segmentacao por posicao para insights por funcao de jogo com menor ruido.

