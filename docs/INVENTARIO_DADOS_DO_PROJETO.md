# Inventario de Dados do Projeto (Estado Atual + Curto Prazo Sustentado)

Data de referencia: `2026-03-24`  
Projeto: `football-analytics`

## 1) Objetivo deste inventario

Este documento e a base de trabalho para planejamento de produto/frontend no estado atual do projeto.

Ele foi escrito para evitar dois erros comuns:

1. ficar preso a snapshot antigo (escopo pequeno e mono-competicao);
2. assumir como pronto algo que ainda depende de consolidacao de camada de consumo.

Leitura oficial usada aqui:

- Waves `4`, `5`, `6` e `7` estao fechadas.
- `raw` cobre o escopo portfolio planejado.
- caveats residuais sao majoritariamente `PROVIDER_COVERAGE_GAP`.
- `mart` ja tem nucleo factual portfolio-wide materializado, mas ainda ha tabelas auxiliares/legadas parciais.

---

## 2) Escopo real do projeto hoje

## 2.1 Competicoes no portfolio

Escopo consolidado em `10` competicoes:

- Premier League
- La Liga
- Serie A (Italia)
- Bundesliga
- Ligue 1
- Campeonato Brasileiro Serie A
- UEFA Champions League
- CONMEBOL Libertadores
- Copa do Brasil
- Campeonato Brasileiro Serie B

## 2.2 Temporadas no portfolio

- Europa + Champions League: `2020/21`, `2021/22`, `2022/23`, `2023/24`, `2024/25`
- Brasil + Libertadores + Copa do Brasil: `2021`, `2022`, `2023`, `2024`, `2025`

## 2.3 O que esse escopo significa para produto

- O frontend precisa nascer multi-competicao e multi-temporada.
- Filtros globais por competicao/temporada nao sao opcionais; sao obrigatorios.
- Historico, comparativos e drilldown por recorte temporal ja sao viaveis de forma realista.
- O limite atual e mais de exposicao/consumo (`mart` + BFF) do que de ingestao em `raw`.

---

## 3) Leitura por camada de dados

| Camada | Papel no projeto | Estado atual |
|---|---|---|
| Bronze | payload bruto por endpoint/provider | operacional e validada durante as waves |
| Silver | normalizacao intermediaria | operacional e usada em todos os dominos das waves |
| Raw (`raw.*`) | base factual canonica para ingestao e verificacao semantica | forte e portfolio-wide |
| Mart (`mart.*`) | camada de consumo orientada a produto/BI | core factual materializado em escala portfolio; tabelas auxiliares/legadas ainda heterogeneas |
| BFF/API | contrato de consumo para frontend | funcional, com cobertura heterogenea por modulo |

Resumo pratico:

- para verdade factual: priorizar `raw`;
- para experiencia de produto: priorizar `mart` core ja materializada + fallback controlado nas tabelas auxiliares ainda parciais;
- para planejamento: considerar o que ja e sustentado pelo warehouse mesmo quando a API ainda nao expoe tudo.

---

## 4) Criterios de status usados neste inventario

- **Disponivel hoje**: dado ja sustentado no estado atual com uso confiavel em produto.
- **Disponivel com caveat**: dado utilizavel, mas com lacunas conhecidas (normalmente provider coverage).
- **Em consolidacao / proximo de ficar disponivel**: dado existe no warehouse, mas depende de evolucao de `mart`/BFF para escala portfolio.
- **Ainda nao assumir**: depende de modelagem/exposicao ainda nao estabilizada.

---

## 5) Cobertura factual macro (base real)

Fonte primaria: consultas SQL diretas no `football_dw` em `2026-03-24`.  
Baseline historico complementar: `docs/DATA_COVERAGE_AUDIT_20260320.md`.

## 5.1 Volumes e cobertura agregada

- `raw.competition_leagues`: `10` competicoes
- `raw.competition_seasons`: `50` escopos competicao-temporada
- `raw.fixtures`: `15265` fixtures (`15262` finalizados)

Cobertura dos dominios fixture-level sobre fixtures finalizados:

- `match_statistics`: `15216 / 15262` (`missing=46`)
- `head_to_head`: `15262 / 15262` (`missing=0`)
- `lineups`: `15183 / 15262` (`missing=79`)
- `match_events`: `15257 / 15262` (`missing=5`)
- `fixture_player_statistics`: `15217 / 15262` (`missing=45`)

Cobertura de `player_season_statistics` (seed por lineup players):

- lineup players distintos: `20775`
- players em `raw.player_season_statistics`: `20722`
- gap agregado: `53`

## 5.2 Integridade semantica agregada

- `orphan_rows`: `0` nos dominios auditados
- `outside_catalog_rows`: `0` em `match_statistics`, `lineups`, `fixture_player_statistics`
- `match_events_duplicate_groups`: `0`

Leitura:

- sem evidencia de contaminacao semantica sistemica no `raw`;
- gaps remanescentes estao concentrados em cobertura de provider e nao em quebra estrutural de pipeline.

## 5.3 Inventario fisico resumido no banco

`raw` hoje:

- `raw.competition_leagues`: `10`
- `raw.competition_seasons`: `50`
- `raw.competition_stages`: `165`
- `raw.competition_rounds`: `1367`
- `raw.standings_snapshots`: `1010`
- `raw.fixtures`: `15265`
- `raw.match_statistics`: `30432`
- `raw.head_to_head_fixtures`: `15265`
- `raw.fixture_lineups`: `652798`
- `raw.player_season_statistics`: `55109`
- `raw.match_events`: `267590`
- `raw.fixture_player_statistics`: `651169`
- `raw.team_coaches`: `140`
- `raw.team_sidelined`: `16`
- `raw.player_transfers`: `5634`
- `raw.provider_sync_state`: `309`

Detalhe util para leitura de particao:

- `raw.match_events_2024`: `53008`
- `raw.match_events_default`: `214582`

`mart` hoje (materializacoes relevantes para produto):

- `mart.fact_matches`: `15265` rows (`50/50` escopos)
- `mart.fact_match_events`: `267590` rows (`50/50` escopos)
- `mart.fact_fixture_lineups`: `649695` rows (`50/50` escopos)
- `mart.fact_fixture_player_stats`: `651169` rows (`50/50` escopos)
- `mart.player_match_summary`: `651169` rows (`50/50` escopos)
- `mart.player_season_summary`: `53221` rows (`50/50` escopos)
- `mart.player_90_metrics`: `53221` rows
- `mart.league_summary`: `50` rows (`50/50` escopos)
- `mart.team_monthly_stats`: `8219` rows (`50/50` escopos)
- `mart.head_to_head_summary`: `3943` rows
- `mart.fact_standings_snapshots`: `1010` rows (`45/50` escopos; faltam `copa_do_brasil/2021-2025`)
- `mart.coach_performance_summary`: `126` rows
- `mart.team_match_goals_monthly`: `176` rows (legado; apenas `season=2024`)
- `mart.team_performance_monthly`: `0` rows

Estruturas fisicas de suporte que existem no banco:

- `mart.dim_competition`: `10`
- `mart.dim_team`: `558`
- `mart.dim_player`: `21654`
- `mart.dim_venue`: `622`
- `mart.dim_stage`: `165`
- `mart.dim_round`: `1367`
- `mart.dim_date`: `1962`
- `mart.dim_coach`: `85`
- `mart.dim_season`: `1`
- `control.competition_provider_map`: `10`
- `control.competitions`: `10`
- `control.season_catalog`: `50`
- `control.backfill_manifest`: `0`
- `control.backfill_runs`: `0`
- `raw.provider_entity_map`: `0`

## 5.4 Verificacao cruzada manual x banco

**Bloco 1 — O que estava no manual e nao foi encontrado no banco**

- nenhum dominio factual citado no manual ficou sem representacao fisica no banco;
- confirmados no banco: `raw.competition_leagues`, `raw.competition_seasons`, `raw.competition_stages`, `raw.competition_rounds`, `raw.standings_snapshots`, `raw.fixtures`, `raw.match_statistics`, `raw.head_to_head_fixtures`, `raw.fixture_lineups`, `raw.player_season_statistics`, `raw.match_events`, `raw.fixture_player_statistics`, `raw.team_coaches`, `raw.team_sidelined`, `raw.player_transfers`, `raw.provider_sync_state`;
- caveat de escopo: `quality gates`, `data_quality_checks` e `raw_checkpoint=success=True` aparecem no manual como evidencias operacionais, mas nao correspondem a uma tabela unica dentro de `football_dw`.

**Bloco 2 — O que existe no banco e nao estava registrado no manual**

- `mart.fact_matches`: `15265` rows (`50/50` escopos)
- `mart.fact_match_events`: `267590` rows (`50/50` escopos)
- `mart.fact_fixture_lineups`: `649695` rows (`50/50` escopos)
- `mart.fact_fixture_player_stats`: `651169` rows (`50/50` escopos)
- `mart.player_match_summary`: `651169` rows (`50/50` escopos)
- `mart.player_season_summary`: `53221` rows (`50/50` escopos)
- `mart.player_90_metrics`: `53221` rows
- `mart.team_monthly_stats`: `8219` rows (`50/50` escopos)
- `mart.fact_standings_snapshots`: `1010` rows (`45/50` escopos)
- `mart.coach_performance_summary`: `126` rows
- tabelas fisicas de suporte ausentes do manual anterior: `mart.dim_*`, `control.*`, particoes `raw.match_events_*` e `raw.provider_entity_map`

---

## 6) Inventario detalhado por dominio

## 6.1 Estrutura de competicao (`competition_leagues`, `competition_seasons`, `competition_stages`, `competition_rounds`)

**O que e**  
Catalogo estrutural do campeonato: liga, temporada, fase e rodada.

**Grain principal**

- liga
- liga-temporada
- fase
- rodada

**Que perguntas responde no produto**

- quais competicoes e temporadas existem para navegacao;
- como montar filtro de fase/rodada;
- como ordenar fixtures e standings por contexto oficial.

**Estado**

- dado: **Disponivel hoje**
- consumo frontend em escala portfolio: **Em consolidacao** (depende de padronizacao `mart` + BFF)

**Caveats**

- cobertura de metadados de fase/rodada pode variar por competicao;
- frontend deve prever fallback para valores ausentes em alguns contextos.

**Dependencia de mart**

- nao bloqueia IA de navegacao (pode usar BFF sobre `raw`);
- melhora quando `mart` estiver rematerializada em escala.

## 6.2 Standings (`standings_snapshots`)

**O que e**  
Classificacao oficial por rodada/snapshot.

**Grain principal**

- time x rodada x temporada x competicao

**Que perguntas responde**

- tabela atual;
- evolucao de posicao e pontos por rodada;
- comparacao de desempenho no campeonato.

**Estado**

- dado: **Disponivel hoje**
- consumo: **Disponivel hoje** (via `raw`/BFF; em `mart`, `fact_standings_snapshots` ainda nao cobre `copa_do_brasil/2021-2025`)

**Caveats**

- depende da qualidade de calendario/rodada para UX mais rica;
- alguns recortes avancados ainda pedem ajuste de camada de consumo.

## 6.3 Fixtures (`fixtures`)

**O que e**  
Registro central de partidas e metadados de contexto.

**Grain principal**

- uma linha por fixture

**Que perguntas responde**

- calendario e resultados;
- status da partida;
- contexto de mando, placar e metadados.

**Estado**

- dado: **Disponivel hoje**
- consumo: **Disponivel hoje**

**Caveats**

- colunas de contexto (`attendance`, `weather`, `referee`, etc.) podem ser incompletas por recorte;
- UI deve tratar como "nao informado", nao como erro tecnico.

## 6.4 Match Statistics (`match_statistics`)

**O que e**  
Estatisticas de time por partida (posse, passe, finalizacao, disciplina etc.).

**Grain principal**

- fixture x time

**Que perguntas responde**

- comparativo mandante x visitante;
- perfil ofensivo/defensivo por partida;
- rankings de posse/passe/finalizacoes.

**Estado**

- dado: **Disponivel com caveat** (`missing=46` em finalizados)
- consumo: **Disponivel com caveat**

**Caveats**

- parciais em subconjunto de escopos;
- nao ha sinal de outside_catalog para esse dominio na auditoria factual.

## 6.5 Head-to-Head (`head_to_head_fixtures` + resumo)

**O que e**  
Historico de confrontos por pares de times.

**Grain principal**

- fixture dentro do par de times
- agregado por par (na camada de resumo)

**Que perguntas responde**

- dominancia historica do confronto;
- saldo de vit/emp/der;
- recortes por janela historica.

**Estado**

- dado: **Disponivel hoje** (`15262/15262`)
- consumo: **Disponivel hoje** (com espaco para UX mais rica)

**Caveats**

- zero gap agregado no raw; foco agora e produto/visualizacao.

## 6.6 Lineups (`fixture_lineups`)

**O que e**  
Escalacao e participacao de jogadores por fixture.

**Grain principal**

- fixture x time x jogador/slot de lineup

**Que perguntas responde**

- titulares e banco;
- formacao de equipe;
- participacao e minutos.

**Estado**

- dado: **Disponivel com caveat** (`missing=79`)
- consumo: **Disponivel com caveat**

**Caveats**

- casos de provider coverage gap ja classificados (ex.: Copa do Brasil 2021-2023 em waves);
- linhas com `player_id` nulo existem e devem virar estado de UI, nao erro fatal.

## 6.7 Player Season Statistics (`player_season_statistics`)

**O que e**  
Acumulado de jogador por temporada.

**Grain principal**

- jogador x temporada x time (dependendo do payload)

**Que perguntas responde**

- ranking sazonal de jogador;
- consolidado por temporada;
- comparativos por recorte temporal.

**Estado**

- dado: **Disponivel com caveat** (gap de players distintos: `53`)
- consumo: **Disponivel com caveat**

**Caveats**

- seed depende fortemente de lineup coverage;
- parte do gap e efeito downstream de coverage, nao bug automatico.

## 6.8 Match Events (`match_events`)

**O que e**  
Timeline de eventos da partida.

**Grain principal**

- evento por fixture

**Que perguntas responde**

- narrativa minuto a minuto;
- eventos chave (gols, cartoes, substituicoes conforme tipo/detalhe);
- contexto de virada, momentum e cronologia.

**Estado**

- dado: **Disponivel com caveat leve** (`missing=5`)
- consumo: **Disponivel com caveat**

**Caveats**

- residual final classificado como provider coverage gap em escopos especificos;
- sem duplicate groups semanticos na auditoria final.

## 6.9 Fixture Player Statistics (`fixture_player_statistics`)

**O que e**  
Estatisticas de jogador por partida.

**Grain principal**

- fixture x time x jogador

**Que perguntas responde**

- tabela de performance individual no match center;
- scouting por jogo;
- base para perfis e rankings de jogador.

**Estado**

- dado: **Disponivel com caveat** (`missing=45`)
- consumo: **Disponivel com caveat**

**Caveats**

- casos de payload vazio e/ou sem `player.id` em subconjuntos;
- inconsistencias de metadata semantica no raw ainda precisam reconciliacao dedicada.

## 6.10 Team Coaches (`team_coaches`)

**O que e**  
Historico de tecnicos por clube e periodo.

**Grain principal**

- tecnico x clube x janela temporal

**Que perguntas responde**

- quem comandava o clube em cada recorte;
- comparativo de performance por tenure (quando agregado).

**Estado**

- dado: **Disponivel hoje**
- consumo: **Em consolidacao**

**Caveats**

- dominio ja existe no warehouse, mas ainda com baixa maturidade de exposicao frontend.

## 6.11 Team Sidelined (`team_sidelined`)

**O que e**  
Disponibilidade de elenco (lesao/suspensao/afastamento conforme categoria).

**Grain principal**

- jogador x clube x periodo de indisponibilidade

**Que perguntas responde**

- quais jogadores estao indisponiveis;
- impacto de ausencias por clube;
- contexto para leitura de lineup e performance.

**Estado**

- dado: **Disponivel hoje**
- consumo: **Em consolidacao**

**Caveats**

- padronizacao de UX e contratos BFF ainda precisa evolucao.

## 6.12 Player Transfers (`player_transfers`)

**O que e**  
Movimentacao de mercado por jogador.

**Grain principal**

- transferencia (evento de movimentacao)

**Que perguntas responde**

- entradas e saidas por clube;
- trilha de carreira;
- recortes por janela de mercado.

**Estado**

- dado: **Disponivel hoje**
- consumo: **Em consolidacao**

**Caveats**

- dado existe para produto, mas modulo de mercado ainda nao esta maduro no frontend.

## 6.13 Dominios de controle/qualidade (`provider_sync_state`, quality gates)

**O que e**  
Camada operacional para observabilidade e gates finais.

**Que perguntas responde**

- status por escopo/dominio durante execucao;
- saude geral da wave;
- validacao final de consistencia.

**Estado**

- `raw.provider_sync_state`: **Disponivel hoje** (`309` rows no banco)
- quality gates/checkpoints finais: **Disponivel hoje** como evidencia operacional, mas fora de uma tabela unica no `football_dw`
- consumo produto: **Ainda nao assumir** como modulo de usuario final (uso primario e operacional)

---

## 7) Inventario por entidade funcional (foco produto/frontend)

## 7.1 Competicao

**Ja sustentado**

- catalogo de competicoes;
- resumo por temporada;
- standings e historico de resultados.

**Com caveat**

- alguns modulos de analytics dependem da consolidacao mart para escala portfolio.

**Proximo de ficar disponivel**

- dashboards executivos completos por competicao via mart rematerializada.

## 7.2 Temporada

**Ja sustentado**

- recorte temporal oficial por competicao;
- navegacao para calendario, standings e rankings.

**Com caveat**

- comparativos mais avancados entre temporadas ainda dependem de camada de consumo mais uniforme.

## 7.3 Fase e Rodada

**Ja sustentado**

- estrutura de rodada/fase em boa parte do portfolio;
- base para ranking por rodada e navegacao de matchday.

**Com caveat**

- completude de metadados varia por competicao.

## 7.4 Partida

**Ja sustentado**

- metadados da fixture;
- timeline de eventos;
- lineups;
- stats de time e stats de jogador.

**Com caveat**

- coverage gaps pontuais por provider em lineups/events/fps.

## 7.5 Clube

**Ja sustentado**

- historico de partidas;
- desempenho no campeonato;
- contexto de lineup e jogadores.

**Com caveat**

- perfis avancados dependem de consolidacao mart e contratos BFF mais estaveis.
- a mesma entidade pode existir em mais de uma competicao na mesma temporada; frontend nao deve modelar perfil de clube apenas como `teamId + season`.

## 7.6 Jogador

**Ja sustentado**

- performance por partida (`fixture_player_statistics`);
- resumo sazonal (`player_season_statistics`);
- base para ranking.

**Com caveat**

- gaps pontuais por coverage de lineup/fps;
- campos derivados mais avancados exigem camada adicional.
- o mesmo jogador pode aparecer em mais de uma competicao na mesma temporada; frontend nao deve modelar perfil apenas como `playerId + season`.

## 7.7 Confronto entre times (H2H)

**Ja sustentado**

- historico consistente por par de times;
- dominio com cobertura agregada forte no raw.

**Com caveat**

- consolidacao de visualizacoes historicas comparativas ainda e evolucao de produto.

## 7.8 Tecnico

**Ja sustentado**

- historico de tecnicos por clube.

**Em consolidacao**

- ranking e perfil de tecnico como modulo front dedicado.

## 7.9 Disponibilidade / elenco

**Ja sustentado**

- registros de sidelined para contexto de ausencia.

**Em consolidacao**

- experiencia de produto para disponibilidade ainda nao esta fechada.

## 7.10 Mercado

**Ja sustentado**

- historico de transferencias no raw.

**Em consolidacao**

- feed e analytics de mercado no frontend dependem de contrato BFF dedicado.

---

## 8) Casos de uso e possibilidades de frontend (expandidas)

## 8.1 Competicao/temporada

- hub da competicao com filtro de temporada;
- tabela de classificacao por rodada;
- curva de pontos e posicao;
- calendario da rodada com status de partida;
- resumo executivo por temporada.

## 8.2 Match center

- cabecalho da partida (placar, status, mando, local);
- timeline de eventos minuto a minuto;
- lineups por time (titulares/banco);
- player stats por equipe;
- comparativo de team stats (posse, passe, finalizacao, disciplina);
- bloco de cobertura da partida por secao (events/lineups/player stats).

## 8.3 Clube

- rota canonica recomendada: `/competitions/{competitionKey}/seasons/{seasonLabel}/teams/{teamId}`;
- pagina de perfil com forma recente;
- comparativo casa x fora;
- historico por temporada;
- trend mensal (gols, desempenho);
- elenco mais utilizado;
- confrontos recorrentes (h2h).

## 8.4 Jogador

- rota canonica recomendada: `/competitions/{competitionKey}/seasons/{seasonLabel}/players/{playerId}`;
- perfil sazonal com estatisticas agregadas;
- historico partida a partida;
- comparativo de jogadores por recorte temporal;
- ranking por metrica e por periodo;
- destaque de desempenho recente.

## 8.5 Ranking center

- ranking de jogadores por gols, assistencias, participacoes;
- ranking de times por posse/passe/finalizacao;
- ranking por janela (temporada completa vs ultimos N jogos);
- ranking por competicao e por temporada.

## 8.6 Mercado, tecnicos e disponibilidade

- feed de transferencias por janela;
- perfil de tecnico com historico por clube;
- painel de ausencias do elenco;
- cards de risco de indisponibilidade por clube.

---

## 9) Matriz de maturidade (dominio x caso de uso)

| Dominio/caso | Status de dado | Status para frontend | Observacao |
|---|---|---|---|
| Estrutura de competicao | Disponivel hoje | Disponivel hoje | base de navegacao ja sustentada |
| Standings | Disponivel hoje | Disponivel hoje | forte para tabela e evolucao |
| Fixtures | Disponivel hoje | Disponivel hoje | nucleo de calendario/match center |
| Match statistics | Disponivel com caveat | Disponivel com caveat | gaps pontuais por escopo |
| Head-to-head | Disponivel hoje | Disponivel hoje | bom para modulo dedicado |
| Lineups | Disponivel com caveat | Disponivel com caveat | coverage parcial em recortes especificos |
| Player season statistics | Disponivel com caveat | Disponivel com caveat | gap de players herdado de coverage |
| Match events | Disponivel com caveat leve | Disponivel com caveat | residual pequeno de provider coverage |
| Fixture player statistics | Disponivel com caveat | Disponivel com caveat | residual + metadata semantica a reconciliar |
| Coaches | Disponivel hoje | Em consolidacao | dominio pronto para evolucao de produto |
| Sidelined | Disponivel hoje | Em consolidacao | valor alto para contexto de lineup |
| Transfers | Disponivel hoje | Em consolidacao | modulo de mercado depende de contrato BFF |
| Home portfolio executiva | Disponivel com caveat | Em consolidacao | `league_summary` e `team_monthly_stats` cobrem `50/50`; agregados auxiliares ainda heterogeneos |

---

## 10) Lacunas e pendencias (detalhadas por natureza)

## 10.1 Limitacao real de dado

- gaps residuais em alguns dominios fixture-level;
- parciais em subconjuntos de competicoes/temporadas.

## 10.2 Limitacao de provider coverage

- casos oficialmente classificados como `PROVIDER_COVERAGE_GAP` devem ser carregados para UX;
- principal regra de produto: coverage gap nao deve aparecer como "erro de sistema".

## 10.3 Limitacao de exposicao/BFF

- nem todo dominio ja tem contrato frontend robusto;
- modulos de mercado/tecnicos/disponibilidade ainda precisam camada de consumo dedicada;
- metadados de cobertura por endpoint ainda devem ser padronizados.

## 10.4 Limitacao de mart

- `mart` ja contem core portfolio-wide em `fact_matches`, `fact_match_events`, `fact_fixture_lineups`, `fact_fixture_player_stats`, `player_match_summary`, `player_season_summary`, `league_summary` e `team_monthly_stats`;
- gaps remanescentes concentram-se em `fact_standings_snapshots` (`45/50` escopos), `team_performance_monthly` (`0` rows), `dim_season` (`1` row) e legado `team_match_goals_monthly` (`season=2024`);
- coexistencia de legado/deprecated e modelos atuais continua sendo o principal ponto de ambiguidade.

## 10.5 Backlog funcional

- padronizar coverage-state em todos os modulos;
- ampliar contratos para comparativos avancados;
- consolidar home executiva portfolio;
- estabilizar modulos adicionais (mercado, tecnico, disponibilidade) para uso pleno.

---

## 11) O que ja deve entrar no planejamento frontend agora (para evitar retrabalho)

1. Arquitetura de navegacao multi-competicao e multi-temporada desde o inicio.
2. Filtros globais por competicao, temporada, fase/rodada e janela temporal.
3. Paginas de clube/jogador ancoradas em `competition + season + entity`, nunca so em `season + entity`.
4. Match center modular com estados independentes por secao.
5. Coverage-state como requisito funcional de todos os modulos.
6. Contratos de BFF com metadados de fonte e cobertura.
7. Estrategia de rollout por maturidade (`hoje`, `com caveat`, `em consolidacao`).

---

## 12) O que ainda nao deve ser assumido como pronto

- `mart` 100% portfolio-ready sem rematerializacao dirigida;
- cobertura uniformemente completa em todo dominio profundo;
- eliminacao total de fallback para `raw` no curto prazo;
- modulos de produto secundarios plenamente estaveis sem evolucao de consumo.

---

## 13) Pontos que ainda dependem de validacao futura para ficar 100% exatos

- janela exata de rematerializacao portfolio da `mart`;
- grau final de cobertura dos modulos apos consolidacao de consumo;
- reconciliacao completa da metadata semantica de `raw.fixture_player_statistics`;
- padronizacao final de `provider_sync_state` para observabilidade por escopo.

---

## 14) Referencias usadas

- `docs/DATA_COVERAGE_AUDIT_20260320.md`
- `docs/MART_FINAL_COVERAGE_AUDIT.md`
- `docs/MART_FRONTEND_BFF_CONTRACTS.md`
- `docs/WAVES_4_TO_7_FINAL_CLOSURE.md`
- `docs/FRONTEND_ARCHITECTURE.md`
- consultas SQL diretas no `football_dw` executadas em `2026-03-24`
