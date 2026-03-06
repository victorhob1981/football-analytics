# Football Analytics Platform — Plano Completo de Páginas

> Data: 2026-02-21  
> Propósito: definir o mapa definitivo de páginas do produto, com todas as funcionalidades, seções, widgets e qual parte do manual funcional cada uma absorve.  
> Total de páginas: **16 páginas** (entre rotas únicas e dinâmicas)

---

## Visão Geral do Mapa de Páginas

```
/                              → Home Executiva
/competition                   → Competição
/matches                       → Lista de Partidas
/matches/[matchId]             → Match Center (dinâmica)
/clubs                         → Lista de Clubes
/clubs/[clubId]                → Perfil do Clube (dinâmica)
/clubs/compare                 → Comparativo Clube vs Clube
/players                       → Lista de Jogadores
/players/[playerId]            → Perfil do Jogador (dinâmica)
/players/compare               → Comparativo Jogador vs Jogador
/rankings                      → Central de Rankings
/head-to-head                  → Head-to-Head
/market                        → Mercado e Disponibilidade
/coaches                       → Lista de Técnicos
/coaches/[coachId]             → Perfil do Técnico (dinâmica)
/audit                         → Auditoria de Cobertura
```

**16 rotas = 10 páginas únicas + 3 dinâmicas + 3 de comparativo/listagem especial**

---

## Mapa de Seções do Manual → Páginas

| Seção do manual | Página(s) onde aparece |
|---|---|
| §1 Premissas de arquitetura | Transversal (toda a plataforma) |
| §2 Inventário de dados | Transversal — informa badges de cobertura |
| §3 Catálogo funcional por tabela | Transversal — alimenta o metrics.registry |
| §4 Mapa entidade → tabelas fonte | Transversal — informa services e query keys |
| §5.1 Home/Visão Geral | `/` |
| §5.2 Competição | `/competition` |
| §5.3 Partidas | `/matches` e `/matches/[matchId]` |
| §5.4 Clubes | `/clubs`, `/clubs/[clubId]`, `/clubs/compare` |
| §5.5 Jogadores | `/players`, `/players/[playerId]`, `/players/compare` |
| §5.6 Rankings | `/rankings` |
| §5.7 Mercado e Disponibilidade | `/market` |
| §5.8 Head-to-Head | `/head-to-head` |
| §5.9 Técnicos | `/coaches`, `/coaches/[coachId]` |
| §6.1 Perfil de clube detalhado | `/clubs/[clubId]` |
| §6.2 Comparativo Clube vs Clube | `/clubs/compare` |
| §6.3 Lista de jogadores com filtros | `/players` |
| §6.4 Perfil de jogador completo | `/players/[playerId]` |
| §6.5 Comparativo Jogador vs Jogador | `/players/compare` |
| §7 Central de Rankings | `/rankings` |
| §8 Time Intelligence | Transversal — GlobalFilterBar em todas as páginas |
| §9 Cobertura e estados da UI | Transversal + `/audit` |
| §10 Contratos da API (BFF) | Transversal — informa services layer |
| §11 O que entregar agora vs backlog | Informa status de cada widget nas páginas |
| §12 Prioridade de execução | Informa ordem de implementação |
| §13 Matriz de métricas | Transversal — alimenta metrics.registry e coverage badges |
| §14 Métricas derivadas e índices | `/rankings`, `/clubs/compare`, `/players/compare` |
| §15 Camada de Insight | `/` (feed), todas as páginas de perfil, `/rankings` |

---

## Páginas em Detalhe

---

### PÁGINA 1 — Home Executiva
**Rota:** `/`  
**Manual:** §5.1, §15 (insights de destaque da rodada)

**Objetivo:** leitura executiva rápida do campeonato. Primeira tela que qualquer usuário vê. Sem drilldown profundo — cada widget é um convite para uma seção específica.

**Layout global (presente em todas as páginas):**

**TopNavBar** — barra de navegação fixa no topo
- Logo/ícone da plataforma (link para `/`)
- Links para as páginas principais: Home, Competição, Partidas, Clubes, Jogadores, Rankings, Head-to-Head, Mercado, Técnicos
- Link da rota ativa destacado visualmente
- Responsivo: menu hambúrguer no mobile

---

**Seções e widgets da Home:**

**Seção 0 — GlobalFilterBar (filtros da temporada)**
- Seletor de competição (liga + temporada)
- Seletor de recorte temporal (temporada / lastN / intervalo)
- Filtros disponíveis nesta página: Temporada ✅ | Rodada ⚠️ parcial | Mês ✅ | LastN ✅ | Intervalo ✅
- ⚠️ Os filtros vivem **apenas na Home** — nas demais páginas cada seção tem seus próprios filtros locais

**Seção 1 — KPIs da temporada**
- Cards: total de partidas disputadas, total de gols, média de gols por jogo, total de times
- Fonte: `mart.fact_matches`, `mart.league_summary`
- Estado: skeleton de 4 cards durante carregamento

**Seção 2 — Top ataques e defesas**
- Duas listas side-by-side: times que mais marcaram vs times que menos sofreram gols
- Cada linha: logo do time, nome, valor da métrica, sparkline de tendência mensal
- Link para o perfil do clube
- Fonte: `mart.team_monthly_stats`, `mart.team_match_goals_monthly`, `mart.fact_matches`

**Seção 3 — Evolução de pontos dos principais clubes**
- LineChart com curva de pontos acumulados por rodada, top 6 times
- Legenda interativa para mostrar/ocultar times
- Fonte: `mart.standings_evolution`

**Seção 4 — Top jogadores do momento**
- Tabela compacta: nome, clube, métrica principal, variação vs período anterior
- Recorte padrão: últimos N jogos (configurável pelo filtro global)
- Métricas destacadas: gols, assistências, rating
- Link para perfil do jogador
- Fonte: `mart.player_match_summary`, `mart.player_90_metrics`

**Seção 5 — Feed de Insights da rodada**
- InsightFeed vertical com os highlights automáticos mais relevantes
- Ordenação: critical → warning → info
- Inclui: alertas de subida/queda ofensiva, outliers de jogadores, tendência de pontos, alerta de qualidade de dado
- Fonte: insights calculados pela BFF a partir de `mart.standings_evolution`, `mart.player_match_summary`, `mart.team_monthly_stats`
- TTL: 5-10 min

**Seção 6 — Painel de cobertura de dados (resumo)**
- Mini-painel com 4 indicadores: % fixtures com eventos, com player stats, com lineups, com team stats
- Cores: verde/amarelo/vermelho conforme threshold
- Link para `/audit` (painel completo)
- Fonte: `mart.fact_matches`, `mart.fact_match_events`, `mart.fact_fixture_player_stats`, `mart.fact_fixture_lineups`

**Widgets de status:** loading skeleton por seção, erro por seção (não derruba a página), empty state por widget

---

### PÁGINA 2 — Competição
**Rota:** `/competition`  
**Manual:** §5.2, §8 (recortes de rodada e temporada)

**Objetivo:** navegar o campeonato com profundidade por rodada. Tabela de classificação como centro, com contexto histórico.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✅ | Mês ⚠️ parcial | LastN ✅ | Intervalo ✅

**Seção 1 — Navegador de rodadas**
- Seletor de rodada (dropdown ou pills horizontais com as 38 rodadas)
- Indica rodada atual ativa vs histórico
- Fonte: `mart.dim_round`, `mart.dim_competition`

**Seção 2 — Tabela de classificação por rodada (snapshot)**
- DataTable com colunas: posição, time (logo + nome), P, J, V, E, D, GP, GC, SG, últimos 5 (badges W/D/L)
- Ordenação fixa por posição (o snapshot já vem ordenado)
- Badge de zona: título, libertadores, rebaixamento
- Clique em time → `/clubs/[clubId]`
- Fonte: `mart.fact_standings_snapshots`, `mart.dim_round`, `mart.dim_team`
- Estado: se rodada não tiver snapshot, banner "dados indisponíveis para esta rodada"

**Seção 3 — Curva de pontos acumulados**
- LineChart: eixo X = rodadas, eixo Y = pontos acumulados
- Múltiplas séries (todos os times ou seleção de times)
- Toggle: pontos acumulados vs posição no campeonato
- Fonte: `mart.standings_evolution`

**Seção 4 — Calendário da rodada**
- Lista de partidas da rodada selecionada com resultado, data, times
- Clique em partida → `/matches/[matchId]`
- Fonte: `mart.fact_matches`, `mart.dim_round`

**Seção 5 — Filtro por fase (stage)**
- Dropdown de fase disponível quando `stage_id` tem cobertura
- CoverageBadge informando nível de cobertura de stage_id
- Fonte: `mart.dim_stage`, `mart.dim_competition`

---

### PÁGINA 3 — Lista de Partidas
**Rota:** `/matches`  
**Manual:** §5.3 (lista/calendário)

**Objetivo:** explorar todas as partidas com filtros, navegar para o match center.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✅ | Mês ✅ | LastN ✅ | Intervalo ✅

**Seção 1 — Filtros locais**
- Filtro por time (ambos os lados, casa ou fora)
- Filtro por status (encerrada/futura)
- Toggle de visualização: lista vs calendário mensal

**Seção 2 — Lista/Calendário de partidas**
- Modo lista: cartões por rodada, agrupados, com placar, times, venue quando disponível
- Modo calendário: grade mensal com fixtures nos dias
- Paginação server-side (pageSize configurável)
- Cada card: clique → `/matches/[matchId]`
- Fonte: `mart.fact_matches`, `mart.dim_competition`, `mart.dim_team`, `mart.dim_date`
- Fallback de venue: "Não informado" quando ausente
- Fallback de referee: "Não informado" quando ausente

---

### PÁGINA 4 — Match Center
**Rota:** `/matches/[matchId]`  
**Manual:** §5.3 (match center completo), §9.1 (cobertura parcial)

**Objetivo:** análise completa de uma partida. A página mais densa do produto.

**Filtros disponíveis nesta página:** nenhum filtro global relevante (dados fixos por fixture)

**Seção 1 — Header da partida**
- Times (logo, nome), placar, data, rodada, venue
- Campos com fallback: attendance → "Não informado", weather → "Não informado", referee → "Não informado"
- Fonte: `mart.fact_matches`, `mart.dim_venue`, fallback em `raw.fixtures`

**Seção 2 — Team Stats (comparativo de times)**
- Tabela comparativa lado a lado: finalizações, posse, passes, escanteios, faltas, cartões
- CoverageBadge por métrica (cobertura de `raw.match_statistics` é alta mas não total)
- Fonte: `raw.match_statistics` (consolidado pela BFF), `mart.fact_matches`

**Seção 3 — Timeline de eventos**
- Visualização cronológica (linha do tempo vertical ou horizontal)
- Eventos: gols, cartões, substituições, minutos
- Ícones por tipo de evento
- Fonte: `mart.fact_match_events`, fallback em `raw.match_events`
- Estado: se sem eventos, empty state com CoverageBadge

**Seção 4 — Lineups (escalação)**
- Representação visual de escalação titulares (formação quando disponível) + banco
- Slots sem `player_id` renderizados sem link (conforme §9.1 do manual)
- Tabs: Time A / Time B
- Fonte: `mart.fact_fixture_lineups`, `mart.dim_player`

**Seção 5 — Player Stats da partida**
- DataTable com um tab por time
- Colunas: jogador, minutos, gols, assistências, chutes, passes-chave, desarmes, interceptações, duelos, rating
- CoverageBadge no cabeçalho de colunas com cobertura baixa (ex: rating 68%, tackles 37%)
- Ordenação por qualquer coluna (client-side)
- Clique no jogador → `/players/[playerId]`
- Fonte: `mart.fact_fixture_player_stats`, `mart.player_match_summary`, `mart.dim_player`

**Seção 6 — Insights da partida**
- InsightFeed específico do fixture (se disponíveis)
- Fonte: BFF com dados daquele `fixture_id`

---

### PÁGINA 5 — Lista de Clubes
**Rota:** `/clubs`  
**Manual:** §5.4 (intro), §6.1 (contexto de clube)

**Objetivo:** ponto de entrada para o módulo de clubes. Navegação e busca.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ⚠️ | Mês ⚠️ | LastN ✅ | Intervalo ✅

**Seção 1 — Barra de busca e filtros locais**
- Busca por nome do clube
- Filtro local: ordenar por pontos, gols marcados, gols sofridos

**Seção 2 — Grid de clubes**
- Cards de clube: logo, nome, posição na classificação atual, pontos, saldo de gols, forma recente (últimos 5)
- Clique → `/clubs/[clubId]`
- Botão "Comparar" em cada card → adiciona ao comparison.store → aciona botão flutuante de comparação
- Fonte: `mart.dim_team`, `mart.fact_matches`, `mart.standings_evolution`

**Seção 3 — Botão flutuante de comparação**
- Aparece quando 2 clubes estão selecionados no comparison.store
- "Comparar clubes selecionados" → `/clubs/compare`

---

### PÁGINA 6 — Perfil do Clube
**Rota:** `/clubs/[clubId]`  
**Manual:** §5.4, §6.1 completo, §15 (insights de clube)

**Objetivo:** análise completa de um clube. A página de domínio mais rica do produto para clubes.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✅ | Mês ✅ | LastN ✅ | Intervalo ✅

**Seção 1 — Header do clube**
- Logo, nome, temporada, posição atual, pontos, saldo
- Botão "Comparar" → adiciona ao comparison.store
- Fonte: `mart.dim_team`, `mart.standings_evolution`

**Seção 2 — Insights do clube**
- InsightFeed com insights específicos: tendência de pontos, alerta ofensivo/defensivo, volatilidade, consistência, dependência de jogador
- Fonte: BFF calculado a partir de `mart.standings_evolution`, `mart.team_monthly_stats`, `mart.fact_matches`
- TTL: 5 min

**Seção 3 — Performance geral da temporada**
- KPI cards: pontos, vitórias/empates/derrotas, saldo de gols, aproveitamento
- Mini LineChart de pontos acumulados por rodada
- Fonte: `mart.standings_evolution`, `mart.fact_matches`

**Seção 4 — Forma recente**
- Sequência dos últimos N jogos: badges W/D/L com resultado e adversário
- Gráfico de barras de gols pro/contra por jogo recente
- Recorte configurável pelo filtro global lastN
- Fonte: `mart.fact_matches`

**Seção 5 — Distribuição casa vs fora**
- Barras comparativas: aproveitamento em casa vs fora, gols marcados/sofridos em cada contexto
- Fonte: `mart.fact_matches`

**Seção 6 — Gols pro/contra por mês**
- BarChart agrupado: gols marcados (verde) vs gols sofridos (vermelho) por mês
- Fonte: `mart.team_match_goals_monthly`

**Seção 7 — Estatísticas mensais agregadas**
- DataTable ou cards por mês: finalizações, posse média, passes médios
- CoverageState: `mart.team_monthly_stats`
- Fonte: `mart.team_monthly_stats`
- Se vazio (`mart.team_performance_monthly` = 0 linhas): seção oculta com banner "Em breve"

**Seção 8 — Top contribuidores do clube**
- Mini-ranking dos jogadores com mais gols, assistências e participações ofensivas naquele clube
- Clique em jogador → `/players/[playerId]`
- Fonte: `mart.player_match_summary`, `mart.player_season_summary`

**Seção 9 — Disciplina da equipe**
- Barras com total de cartões amarelos/vermelhos e faltas por período
- Timeline de eventos disciplinares relevantes (grandes jogos)
- CoverageBadge: cartões vermelhos têm cobertura 30% em `raw.match_statistics`
- Fonte: `mart.fact_match_events`

**Seção 10 — Elenco utilizado e rotação**
- Lista de jogadores que participaram de lineups, com contagem de jogos e minutos
- Slots sem `player_id`: exibidos como "Jogador não identificado" sem link
- Fonte: `mart.fact_fixture_lineups`, `mart.dim_player`

---

### PÁGINA 7 — Comparativo Clube vs Clube
**Rota:** `/clubs/compare`  
**Manual:** §6.2 completo, §14 (índices analíticos em comparativo)

**Objetivo:** análise lado a lado de dois clubes com múltiplos recortes temporais.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✅ | Mês ✅ | LastN ✅ | Intervalo ✅ + filtro de casa/fora local

**Seção 1 — Seletor de clubes**
- Dois dropdowns de busca de clube (preenchidos automaticamente se vieram do comparison.store)
- Seletor de recorte: temporada completa / lastN / por rodada / por mês / casa vs fora
- Botão "Comparar"

**Seção 2 — Resultado agregado**
- ComparisonLayout lado a lado: KPI cards para cada time (pontos, aproveitamento, gols marcados/sofridos, saldo)
- DeltaIndicator mostrando diferença absoluta e relativa entre os dois times
- Fonte: `mart.fact_matches`, `mart.standings_evolution`

**Seção 3 — Tendência temporal**
- LineChart com duas séries (uma por time): pontos acumulados ou saldo de gols ao longo do período
- Toggle para alternar métrica do eixo Y
- Fonte: `mart.standings_evolution`, `mart.team_monthly_stats`

**Seção 4 — Comparativo de métricas detalhadas**
- RadarChart multidimensional com índices: ofensivo, defensivo, disciplina, consistência, forma recente
- Fonte: índices calculados na BFF a partir de `mart.team_monthly_stats`, `mart.fact_matches` (§14 do manual)
- CoverageBadge: índices que dependem de dados parciais

**Seção 5 — Contribuidores chave de cada time**
- Dois painéis side-by-side: top 5 jogadores de cada clube no recorte
- Clique no jogador → `/players/[playerId]`
- Fonte: `mart.player_match_summary`, `mart.player_season_summary`

**Seção 6 — Historial de confronto direto (H2H)**
- Mini-resumo do H2H: vitórias/empates/derrotas, saldo de gols histórico
- Link para `/head-to-head?teamA=X&teamB=Y` para detalhamento completo
- Fonte: `mart.head_to_head_summary`

---

### PÁGINA 8 — Lista de Jogadores
**Rota:** `/players`  
**Manual:** §5.5, §6.3 completo

**Objetivo:** scouting. Filtrar, ordenar e encontrar jogadores com base em métricas.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ⚠️ parcial | Mês ⚠️ parcial | LastN ✅ | Intervalo ✅

**Seção 1 — Filtros locais de jogador**
- Filtro por clube (`team_id`)
- Toggle: ordenar por total vs por 90 minutos
- Filtro de mínimo de minutos/partidas (input numérico — reduz ruído para métricas por 90)
- Seletor de métrica principal (gols, assistências, chutes, passes-chave, desarmes, interceptações, rating, duelos)
- Busca por nome

**Seção 2 — DataTable de jogadores**
- Colunas fixas: nome (link), clube (logo), jogos, minutos
- Colunas de métricas dinâmicas conforme seletor: total e por 90 em paralelo
- CoverageBadge no header de colunas com cobertura < 80%
- Ordenação client-side nas colunas (já paginada server-side por métrica principal)
- Linha com `player_id` → clique → `/players/[playerId]`
- Botão "Comparar" em cada linha → comparison.store → botão flutuante
- Fonte: `mart.player_season_summary`, `mart.player_90_metrics`, `mart.dim_player`

**Seção 3 — Botão flutuante de comparação**
- Aparece com 2 jogadores selecionados → `/players/compare`

---

### PÁGINA 9 — Perfil do Jogador
**Rota:** `/players/[playerId]`  
**Manual:** §5.5, §6.4 completo, §15 (insights de jogador)

**Objetivo:** análise individual completa de um jogador. A página mais rica do módulo de jogadores.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ⚠️ parcial | Mês ⚠️ parcial | LastN ✅ | Intervalo ✅

**Seção 1 — Header do jogador**
- Foto (se disponível), nome, clube atual, posição, nacionalidade, idade
- Botão "Comparar" → comparison.store
- Fonte: `mart.dim_player`, `mart.dim_team`

**Seção 2 — Insights do jogador**
- InsightFeed: outlier positivo/negativo de liga, tendência de forma, alerta disciplinar, consistência, dependência de jogador pelo clube
- Fonte: BFF a partir de `mart.player_90_metrics`, `mart.player_match_summary`, `mart.player_season_summary`

**Seção 3 — Resumo da temporada**
- KPI cards: jogos, minutos, gols, assistências, rating médio, chutes, passes-chave
- Campos sem valor > 0 (xG, clean_sheets): exibidos com CoverageBadge "Indisponível no momento"
- Fonte: `mart.player_season_summary`

**Seção 4 — Eficiência por 90 minutos**
- Radar ou barras comparativas: métricas do jogador vs média da liga na mesma posição
- CoverageBadge: comparação por posição depende de padronização (PartialDataBanner se cobertura insuficiente)
- Fonte: `mart.player_90_metrics`

**Seção 5 — Série por partida (histórico de jogos)**
- DataTable paginada: data, adversário, resultado, minutos, gols, assistências, rating, outras métricas
- Sparkline de rating ao longo dos jogos
- Clique na linha → `/matches/[matchId]`
- Fonte: `mart.player_match_summary`, `mart.fact_fixture_player_stats`

**Seção 6 — Participação em lineups**
- Barras ou timeline: jogos como titular vs banco vs ausente
- Total de minutos por contexto
- Fonte: `mart.fact_fixture_lineups`

**Seção 7 — Eventos de impacto**
- Lista compacta de gols marcados, cartões recebidos, substituições importantes
- Fonte: `mart.fact_match_events`

**Seção 8 — Transferências**
- Timeline de transferências: clube origem → clube destino, data, tipo (definitiva/empréstimo)
- Fonte: `raw.player_transfers`

**Seção 9 — Indisponibilidades**
- Lista de períodos de afastamento quando `player_id` tem correspondência
- Tipo de afastamento quando disponível
- Empty state com explicação se sem registros
- Fonte: `raw.team_sidelined`

---

### PÁGINA 10 — Comparativo Jogador vs Jogador
**Rota:** `/players/compare`  
**Manual:** §6.5 completo, §14 (Índice de Contribuição Ofensiva, Índice de Consistência)

**Objetivo:** comparação profunda entre dois jogadores com múltiplos modos de análise.

**Filtros disponíveis nesta página:** Temporada ✅ | LastN ✅ | Intervalo ✅ + seletor de modo local

**Seção 1 — Seletor de jogadores e modo**
- Dois dropdowns de busca (preenchidos se vieram do comparison.store)
- Seletor de modo: temporada inteira / lastN / por 90 / por clube atual
- Botão "Comparar"
- PartialDataBanner se comparação por posição não estiver disponível (GAP-1 da análise de compatibilidade)

**Seção 2 — KPI comparativo**
- ComparisonLayout lado a lado: gols, assistências, chutes, passes-chave, minutos, jogos, rating
- DeltaIndicator em cada métrica
- Fonte: `mart.player_season_summary` (temporada) ou `mart.player_match_summary` (lastN)

**Seção 3 — Radar de perfil multidimensional**
- RadarChart: ofensivo, defensivo, passes, disciplina, consistência (índices por 90)
- Dois polígonos sobrepostos (um por jogador)
- Fonte: `mart.player_90_metrics`, índices calculados na BFF

**Seção 4 — Forma recente (série por partida)**
- Dois LineCharts paralelos: rating ou gols ao longo dos últimos N jogos
- Fonte: `mart.player_match_summary`

**Seção 5 — Contexto de lineup**
- Comparativo de minutos jogados, jogos como titular, aproveitamento dos times no período
- Fonte: `mart.fact_fixture_lineups`, `mart.fact_matches`

---

### PÁGINA 11 — Central de Rankings
**Rota:** `/rankings/[rankingType]` — rota dinâmica  
**Manual:** §5.6, §7 completo (7.1 a 7.6), §14 (índices em rankings), §15 (InsightBadge em outliers)

**Objetivo:** concentrar todos os líderes do produto em uma única central com navegação por categoria e recorte.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✅ | Mês ✅ | LastN ✅ | Intervalo ✅

**Arquitetura de implementação:**
> ⚠️ Em vez de tabs JavaScript em uma única página, o frontend usa **rotas dinâmicas** (`/rankings/[rankingType]`) onde cada categoria de ranking é uma URL própria, linkável e compativel com back-button.
> A fonte de verdade de quais rankings existem é o arquivo `src/config/ranking.registry.ts`.
> Adicionar um novo ranking = adicionar uma entrada no registry, sem criar novo componente.

**Rankings registrados atualmente no `ranking.registry.ts`:**

| URL (`/rankings/[id]`) | Label | Entidade | Métrica |
|---|---|---|---|
| `/rankings/player-goals` | Artilharia | player | gols |
| `/rankings/player-assists` | Assistências | player | assistências |
| `/rankings/player-shots-total` | Finalizações | player | chutes totais |
| `/rankings/player-shots-on-target` | Finalizações no Alvo | player | chutes no alvo |
| `/rankings/player-pass-accuracy` | Precisão de Passe | player | % acerto passe |
| `/rankings/player-rating` | Rating | player | rating médio |
| `/rankings/player-yellow-cards` | Cartões Amarelos | player | cartões amarelos |
| `/rankings/team-possession` | Posse de Bola | team | % posse média |
| `/rankings/team-pass-accuracy` | Precisão de Passe do Time | team | % acerto passe |

**Estrutura da página:** NavBar de categorias (links para cada `/rankings/[id]`) + DataTable dinâmica

---

**Rankings de jogadores planejados (a adicionar no registry):**
- Gols | Assistências | Part. Ofensivas | Chutes | Passes-chave | Desarmes | Interceptações | Duelos | Faltas | Rating | Cartões
- Versões `/90 min`: Gols/90 | Assist/90 | Chutes/90 | Passes-chave/90 | Desarmes/90 | Interceptações/90
- Filtro local: mínimo de minutos (reduz ruído)
- InsightBadge em linhas de outliers (+2 desvios-padrão)
- CoverageBadge em métricas com cobertura < 80%
- Fonte: `mart.player_season_summary` (temporada), `mart.player_match_summary` (lastN), `mart.player_90_metrics` (por 90)
- Status: parcialmente disponível (goals, assists, shots, pass_accuracy, rating, yellow_cards já no registry)

**Rankings de clubes planejados (a adicionar no registry):**
- Classificação: Pontos | Vitórias | Saldo de Gols | Aproveitamento | Gols Marcados | Gols Sofridos
- Desempenho mensal: gols marcados/sofridos, saldo, pontos no período
- Disciplina: cartões amarelos/jogo, cartões vermelhos/jogo, faltas/jogo
- Eficiência (backlog): % acerto passes, % finalização no alvo — depende de modelagem adicional
- Fonte: `mart.standings_evolution`, `mart.fact_standings_snapshots`, `mart.team_monthly_stats`, `mart.fact_match_events`
- Já no registry: `team-possession`, `team-pass-accuracy`

**Rankings de técnicos (a adicionar no registry):**
- Aproveitamento sob comando, gols por jogo, vitórias
- Clique no técnico → `/coaches/[coachId]`
- Fonte: `mart.coach_performance_summary`, `mart.dim_coach`

**Rankings de Dominância H2H (a adicionar no registry):**
- Pares de times por índice de dominância (§14 — Índice de Dominância H2H)
- Clique no par → `/head-to-head?teamA=X&teamB=Y`
- Fonte: `mart.head_to_head_summary`

**Rankings de Mercado e Disponibilidade (a adicionar no registry):**
- Clubes por volume de transferências / mais afastamentos
- Fonte: `raw.player_transfers`, `raw.team_sidelined`

**Tab 9 — Mercado (transferências)**
- Ranking de clubes por volume de transferências (entradas/saídas)
- Recorte por temporada ou intervalo de datas
- Fonte: `raw.player_transfers`
- Status: disponível agora

**Tab 10 — Disponibilidade**
- Ranking de times com mais afastamentos no período
- Fonte: `raw.team_sidelined`
- Status: disponível agora

---

### PÁGINA 12 — Head-to-Head
**Rota:** `/head-to-head`  
**Manual:** §5.8 completo, §14 (Índice de Dominância H2H)

**Objetivo:** histórico completo e análise de dominância entre dois clubes específicos.

**Filtros disponíveis nesta página:** Temporada ✅ | Mês ⚠️ parcial | LastN ✅ | Intervalo ✅ | Rodada ✗

**Seção 1 — Seletor de confronto**
- Dois dropdowns de busca de clube
- Pode ser pré-preenchido por URL params (`?teamA=X&teamB=Y`) vindo de `/clubs/compare` ou `/rankings`
- Seletor de recorte: histórico total / temporada / lastN

**Seção 2 — Resumo agregado**
- 3 métricas principais: vitórias time A / empates / vitórias time B
- Visual de dominância: barra horizontal proporcional
- Índice de Dominância H2H (§14): score numérico com classificação (equilibrado / vantagem leve / dominância forte)
- Saldo médio de gols, gols marcados/sofridos por jogo
- Fonte: `mart.head_to_head_summary`

**Seção 3 — Recorte por período**
- Dropdown de temporada específica ou intervalo
- Métricas do seção 2 recarregam conforme recorte
- Fonte: `mart.head_to_head_summary`, `raw.head_to_head_fixtures`

**Seção 4 — Timeline de confrontos**
- DataTable cronológica: data, competição, local, resultado, gols
- Clique no jogo → `/matches/[matchId]` quando `fixture_id` disponível
- Fonte: `raw.head_to_head_fixtures`, `mart.fact_matches`

**Seção 5 — Desempenho nos confrontos por clube**
- Dois painéis: aproveitamento de cada time especificamente neste confronto vs aproveitamento geral no mesmo período
- Contexto de "como cada time joga contra esse adversário específico"
- Fonte: `mart.head_to_head_summary`, `mart.fact_matches`

---

### PÁGINA 13 — Mercado e Disponibilidade
**Rota:** `/market`  
**Manual:** §5.7 completo, §7.6 (rankings especiais de mercado e disponibilidade)

**Objetivo:** trilha de movimentações de jogadores e painel de risco de indisponibilidade.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✗ | Mês ⚠️ parcial | LastN ⚠️ parcial | Intervalo ✅

**Seção 1 — Tabs: Transferências / Disponibilidade**

**Tab Transferências:**
- Sub-seção: Feed de transferências cronológico (jogador, clube origem → destino, tipo, data)
- Sub-seção: Ranking de clubes por entradas (quem mais contratou no período)
- Sub-seção: Ranking de clubes por saídas (quem mais vendeu/cedeu)
- Filtro local: por clube, por tipo (definitiva/empréstimo/retorno), por período
- Paginação server-side
- Fonte: `raw.player_transfers`

**Tab Disponibilidade:**
- Sub-seção: Painel de indisponíveis por clube — lista de jogadores afastados com tipo e período estimado
- Sub-seção: Ranking de clubes com mais afastamentos simultâneos
- Sub-seção: Tendência temporal de indisponibilidade (linha do tempo de afastamentos)
- Filtro local: por clube, por período
- Fonte: `raw.team_sidelined`, `mart.dim_team`, `mart.dim_player`

---

### PÁGINA 14 — Lista de Técnicos
**Rota:** `/coaches`  
**Manual:** §5.9, §7.6 (ranking de técnicos)

**Objetivo:** ponto de entrada para o módulo de técnicos, com ranking e busca.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✗ | Mês ⚠️ parcial | LastN ✅ | Intervalo ✅

**Seção 1 — Filtros locais**
- Busca por nome do técnico
- Ordenação: aproveitamento / gols marcados por jogo / vitórias

**Seção 2 — DataTable de técnicos**
- Colunas: técnico (nome, link), clube, período no clube, J, V, E, D, aproveitamento, gols/jogo
- Clique → `/coaches/[coachId]`
- Fonte: `mart.coach_performance_summary`, `mart.dim_coach`

---

### PÁGINA 15 — Perfil do Técnico
**Rota:** `/coaches/[coachId]`  
**Manual:** §5.9 completo

**Objetivo:** análise de impacto de um técnico no desempenho dos clubes que comandou.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ✗ | Mês ⚠️ parcial | LastN ✅ | Intervalo ✅

**Seção 1 — Header do técnico**
- Nome, foto (se disponível), nacionalidade, histórico de clubes recentes
- Fonte: `mart.dim_coach`, `raw.team_coaches`

**Seção 2 — Performance agregada sob comando**
- KPI cards: jogos comandados, aproveitamento, gols marcados/sofridos por jogo, vitórias/empates/derrotas
- Por clube (se comandou múltiplos clubes): detalhamento por período
- Fonte: `mart.coach_performance_summary`

**Seção 3 — Comparativo com outros técnicos**
- Mini-ranking: posição do técnico por aproveitamento vs outros técnicos da competição/temporada
- Link para `/rankings` (Tab Técnicos) para ranking completo
- Fonte: `mart.coach_performance_summary`

**Seção 4 — Histórico de clubes**
- Timeline: clube, período de início/fim, jogos, aproveitamento naquele clube
- Fonte: `raw.team_coaches`

---

### PÁGINA 16 — Auditoria de Cobertura
**Rota:** `/audit`  
**Manual:** §9 completo (cobertura e estados da UI), §2.2 (notas de cobertura), §10 (`/api/audit/coverage`)

**Objetivo:** painel técnico e operacional. Visibilidade completa sobre qualidade e disponibilidade dos dados. Página para uso interno/analítico — não é a página principal do produto para usuários finais.

**Filtros disponíveis nesta página:** Temporada ✅ | Rodada ⚠️ | Mês ✗ | LastN ✗ | Intervalo ✗

**Seção 1 — Cobertura por módulo**
- Cards por módulo com percentual e código de cor (verde/amarelo/vermelho):
  - % fixtures com eventos (`mart.fact_match_events`)
  - % fixtures com player stats (`mart.fact_fixture_player_stats`)
  - % fixtures com lineups (`mart.fact_fixture_lineups`)
  - % fixtures com standings (`mart.fact_standings_snapshots`)
  - % fixtures com team stats (`raw.match_statistics`)
- Clique em cada card expande detalhamento por rodada
- Fonte: `mart.fact_matches` como base, cobertura de cada módulo
- TTL: 5 min

**Seção 2 — Estado de sincronização de ingestão**
- Tabela: entidade, último sync, status (ok/atraso/erro), volume atual
- Indicador de lag de ingestão
- Fonte: `raw.provider_sync_state`
- Nota: `raw.provider_entity_map` ainda vazio — seção de reconciliação multi-provider exibida como "Em breve"

**Seção 3 — Cobertura de métricas específicas**
- Lista de métricas com cobertura baixa/problemática, conforme §13 e §2.2 do manual:
  - `clean_sheets`: 0% de valores > 0 nos marts de jogador
  - `xg` / `xg_per_90`: 0% de valores > 0
  - Lineups com 49 slots sem `player_id`
  - `mart.team_performance_monthly`: 0 linhas
- Cada item: status atual, impacto nos módulos do produto, ação recomendada
- Fonte: BFF a partir de `mart.*` e `raw.*`

**Seção 4 — Cobertura do JSON bruto de player statistics**
- Tabela de métricas extraídas de `raw.fixture_player_statistics` com cobertura por developer_name
- Colunas: developer_name, cobertura (%), módulos afetados, status (materializado no mart / pendente)
- Ordenado por cobertura descendente
- Fonte: `/api/audit/coverage`

---

## Resumo Final das Páginas

| # | Rota | Tipo | Seções do manual | Status |
|---|---|---|---|---|
| 1 | `/` | Única | §5.1, §15 | Agora |
| 2 | `/competition` | Única | §5.2, §8 | Agora |
| 3 | `/matches` | Única | §5.3 (lista) | Agora |
| 4 | `/matches/[matchId]` | Dinâmica | §5.3 (match center), §9.1 | Agora |
| 5 | `/clubs` | Única | §5.4 (entrada) | Agora |
| 6 | `/clubs/[clubId]` | Dinâmica | §5.4, §6.1, §15 | Agora |
| 7 | `/clubs/compare` | Única | §6.2, §14 | Agora |
| 8 | `/players` | Única | §5.5, §6.3 | Agora |
| 9 | `/players/[playerId]` | Dinâmica | §5.5, §6.4, §15 | Agora |
| 10 | `/players/compare` | Única | §6.5, §14 | Agora |
| 11 | `/rankings/[rankingType]` | Dinâmica | §5.6, §7.1–7.6, §14, §15 | Parcial (9 tipos no registry) |
| 12 | `/head-to-head` | Única | §5.8, §14 | Agora |
| 13 | `/market` | Única | §5.7, §7.6 | Agora |
| 14 | `/coaches` | Única | §5.9, §7.6 | Agora |
| 15 | `/coaches/[coachId]` | Dinâmica | §5.9 completo | Agora |
| 16 | `/audit` | Única | §2.2, §9, §10, §13 | Agora |

**Elementos com presença transversal (não são uma página, permeiam tudo):**
- **TopNavBar** → componente de navegação fixo no topo de todas as páginas (`shared/components/navigation/TopNavBar.tsx`)
- §1 Premissas → regra BFF-first em todos os services
- §2 Inventário de dados → coverage badges em todas as páginas
- §3 Catálogo funcional → metrics.registry.ts (fonte de verdade de métricas)
- §4 Mapa entidade → tabelas → query keys e services de cada feature
- §8 Time Intelligence → `GlobalFilterBar` na Home + `pageFilterConfig` em cada rota
- §9 Estados da UI → loading/error/partial/empty em todo componente de dados
- §10 Contratos da API → services layer + types
- §11 Agora vs Backlog → `availableNow` e `status` no metrics.registry e ranking.registry
- §12 Prioridade → ordem de sprint (não afeta estrutura de páginas)

**Métricas derivadas (§14) aparecem em:**
- `/clubs/compare` — índices ofensivo, defensivo, dominância
- `/players/compare` — índice de contribuição ofensiva, consistência
- `/rankings` — índices como coluna adicional nos rankings de eficiência (Tab 6, backlog)
- `/clubs/[clubId]` — índice de forma recente, volatilidade, dependência de jogador
- `/players/[playerId]` — índice de forma recente, consistência, disciplina

**Camada de Insight (§15) aparece em:**
- `/` — InsightFeed principal (destaques da rodada)
- `/clubs/[clubId]` — InsightFeed do clube
- `/players/[playerId]` — InsightFeed do jogador
- `/rankings` — InsightBadge em linhas de outliers
- `/matches/[matchId]` — InsightFeed da partida (quando disponível)
