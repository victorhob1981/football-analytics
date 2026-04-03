# FRONTEND_MAPA_COMPLETO_DE_PAGINAS

Data de referencia: 2026-03-30

Escopo desta analise:
- frontend real em `frontend/src/app`, `frontend/src/features`, `frontend/src/shared`
- navegacao real da shell, breadcrumbs, filtros globais, CTAs e links internos
- contratos BFF e docs de arquitetura/status/expansao para capturar paginas implicitas com evidencia
- sem implementacao de feature e sem alteracao de comportamento

Base principal de evidencia:
- rotas reais: `frontend/src/app/**/page.tsx`
- shell e navegacao: `frontend/src/app/(platform)/PlatformShell.tsx`, `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- geracao de caminhos canonicos: `frontend/src/shared/utils/context-routing.ts`
- status/arquitetura: `docs/FRONTEND_IMPLEMENTATION_STATUS.md`, `docs/FRONTEND_ARCHITECTURE.md`, `docs/FRONTEND_DELIVERY_PLAN.md`
- gaps e expansoes: `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`, `docs/analise de conteudo pouco aproveitado.md`, `docs/FRONTEND_REFERENCE_INVENTORY.md`
- contratos BFF reais: `docs/BFF_API_CONTRACT.md`, `api/src/routers/*.py`

## 1. Visao geral do mapa do produto

### 1.1. Areas reais do sistema hoje

- Entrada e descoberta:
  - `/`
  - overlay global de busca
  - `/competitions`
  - `/matches`
  - `/players`
  - `/teams`
  - `/rankings/[rankingType]`
- Backbone canonico do produto:
  - `/competitions/[competitionKey]`
  - `/competitions/[competitionKey]/seasons/[seasonLabel]`
- Detalhe contextualizado por entidade:
  - `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]`
  - `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]`
- Rotas curtas e compatibilidade:
  - `/teams/[teamId]`
  - `/players/[playerId]`
  - `/clubs`
  - `/clubs/[clubId]`
  - `/competition/[competitionId]`
- Modulos secundarios ainda nao fechados:
  - `/market`
  - `/head-to-head`
  - `/coaches/[coachId]`
  - `/landing`
  - `/audit`

### 1.2. Agrupamento por dominio

- Home:
  - home executiva em `/`
  - curadoria editorial dentro da propria home
- Competicoes:
  - catalogo de competicoes
  - competition hub por competicao
  - season hub por competicao/temporada
- Partidas:
  - lista de partidas
  - match center com abas
- Clubes/times:
  - lista de times
  - resolver curto
  - perfil canonico com abas
- Jogadores:
  - lista de jogadores
  - resolver curto
  - perfil canonico com abas
  - comparativo de jogadores sem rota propria
- Rankings:
  - familia de paginas dinamicas por `rankingType`
- Busca:
  - overlay global, sem rota propria
- Comparativos e secundarios:
  - `head-to-head`, `market`, `coaches`, `landing`, `audit`

### 1.3. Cobertura atual vs. cobertura pretendida

Cobertura atual forte:
- `/`
- `/competitions`
- competition hub
- season hub (`calendar`, `standings`, `rankings`)
- `/matches`
- `/matches/[matchId]`
- `/players`
- `/teams`
- perfis canonicos de player/team
- `/rankings/[rankingType]`
- busca global para `competition`, `team`, `player`, `match`

Cobertura parcial ou incompleta:
- analytics estruturais avancados do `season hub` dependem de `competition-structure`
- `team profile` tem `squad`, mas ainda sem availability/sidelined
- `rankings` tem family route viva, mas ao menos `player-pass-accuracy` esta marcada como `unsupported` no BFF

Cobertura pretendida, mas nao fechada:
- `head-to-head` como modulo analitico real
- `market/transfers` como modulo real
- `coaches` com pagina indice e detalhe real
- `landing` editorial/institucional
- availability como secao forte do squad e possivelmente tela global

### 1.4. Diferenca entre sistema vivo e blueprint antigo

Ha divergencia objetiva entre docs antigas e app vivo:
- docs antigas ainda falam em `/home`; o app vivo usa `/`
- docs antigas ainda falam em `/h2h`; o app vivo usa `/head-to-head`
- docs antigas ainda falam em `/more/market`, `/more/coaches`, `/more/availability`; o app vivo usa `/market`, `/coaches/[coachId]` e nao possui rota global de availability

## 2. Inventario completo de paginas

### 2.1. Home executiva

- Pagina: Home executiva
- Rota/url: `/`
- Status: `implementada`
- Objetivo: ser entrada principal do produto, resumir o acervo e distribuir para competicoes, rankings, partidas e entidades
- O que tem hoje:
  - hero com `archiveSummary`
  - CTAs para `/competitions` e ranking padrao
  - cards de competicoes nacionais e continentais
  - quick links para `rankings`, `head-to-head`, `clubs` e `matches`
  - curadoria editorial via `editorialHighlights`
  - footer com links para `competitions`, `matches`, `players`, `teams`
- O que deveria ter para cumprir bem seu papel:
  - quick links coerentes com a IA canonica atual
  - curadoria mais profunda e mais ampla que o bloco atual
  - separacao clara entre home executiva e futura landing institucional
- Entidades/dados envolvidos:
  - `archiveSummary`
  - `competitions`
  - `editorialHighlights`
  - links para player canonico, competition hub e modulos principais
- Origem da evidencia:
  - `frontend/src/app/(platform)/(home)/page.tsx`
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
  - `frontend/src/features/home/hooks/useHomePage.ts`
  - `frontend/src/features/home/types/home.types.ts`
  - `api/src/routers/home.py`
- Observacoes relevantes:
  - o quick link "Clubes" aponta para `/clubs`, nao para `/teams`
  - isso reaproveita uma rota legada e gera ambiguidade de navegacao

### 2.2. Busca global

- Pagina: Busca global em overlay
- Rota/url: sem rota propria; abertura a partir da shell (`Ctrl/Cmd + K`)
- Status: `implementada`
- Objetivo: entrada transversal para `competition`, `team`, `player` e `match`
- O que tem hoje:
  - overlay global
  - agrupamento por categoria
  - links para competition hub, team resolver, player resolver e match center
  - estados `loading`, `empty`, `error`, `partial`
- O que deveria ter para cumprir bem seu papel:
  - cobrir tambem `coach` quando esse dominio fechar
  - manter semantica de navegacao consistente com as rotas canonicas
- Entidades/dados envolvidos:
  - resultados de competicao, time, jogador e partida
  - `defaultContext` para resolver caminhos canonicos
- Origem da evidencia:
  - `frontend/src/app/(platform)/PlatformShell.tsx`
  - `frontend/src/features/search/components/GlobalSearchOverlay.tsx`
  - `api/src/routers/search.py`
  - `docs/BFF_API_CONTRACT.md`
- Observacoes relevantes:
  - `SearchType` no BFF e restrito a `competition`, `team`, `player`, `match`
  - nao ha suporte atual a `coach`

### 2.3. Catalogo de competicoes

- Pagina: Catalogo de competicoes
- Rota/url: `/competitions`
- Status: `implementada`
- Objetivo: abrir o catalogo principal e distribuir para competition hub ou temporada mais recente
- O que tem hoje:
  - hero de catalogo
  - card destacado
  - grid de competicoes suportadas
  - CTA para abrir competicao e CTA para temporada mais recente
- O que deveria ter para cumprir bem seu papel:
  - continuar como catalogo e nao virar home duplicada
  - manter acesso rapido para temporada recente e hub por competicao
- Entidades/dados envolvidos:
  - `SUPPORTED_COMPETITIONS`
  - `listSeasonsForCompetition`
  - `getLatestSeasonForCompetition`
- Origem da evidencia:
  - `frontend/src/app/(platform)/competitions/page.tsx`
  - `frontend/src/app/(platform)/PlatformShell.tsx`
  - `docs/FRONTEND_IMPLEMENTATION_STATUS.md`
- Observacoes relevantes:
  - e rota de entrada principal da shell

### 2.4. Competition hub

- Pagina: Competition hub
- Rota/url: `/competitions/[competitionKey]`
- Status: `implementada`
- Objetivo: distribuir para temporadas da competicao e para atalhos da temporada ativa
- O que tem hoje:
  - breadcrumb simples
  - hero com metadados basicos da competicao
  - lista de temporadas disponiveis
  - quick links para `calendar`, `rankings` e `standings` da temporada atual
- O que deveria ter para cumprir bem seu papel:
  - permanecer como hub de distribuicao
  - nao absorver analise profunda que pertence ao season hub
- Entidades/dados envolvidos:
  - catalogo de competicoes
  - lista de temporadas
  - `competition-structure` apenas para rotulo de formato da temporada
- Origem da evidencia:
  - `frontend/src/app/(platform)/competitions/[competitionKey]/page.tsx`
  - `frontend/src/features/competitions/components/CompetitionHubContent.tsx`
  - `docs/FRONTEND_ARCHITECTURE.md`
- Observacoes relevantes:
  - a pagina esta coerente com o papel de hub e nao de analytics profundo

### 2.5. Season hub

- Pagina: Season hub
- Rota/url: `/competitions/[competitionKey]/seasons/[seasonLabel]`
- Status: `implementada`
- Objetivo: concentrar a competicao/temporada como contexto canonico do produto
- O que tem hoje:
  - hero da temporada
  - tabs internas `calendar`, `standings`, `rankings`
  - atalhos para `/matches`, `/rankings/player-goals` e volta ao competition hub
  - sync de contexto canonico com filtro global
- O que deveria ter para cumprir bem seu papel:
  - continuar sendo o centro canonico da temporada
  - nao virar deposito de modulos secundarios externos ao contexto
- Entidades/dados envolvidos:
  - `competition-structure`
  - `matches`
  - `standings`
  - `rankings`
  - `competition-analytics`
- Origem da evidencia:
  - `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/page.tsx`
  - `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`
  - `frontend/src/shared/utils/context-routing.ts`
- Observacoes relevantes:
  - o season hub e a principal ancora de contexto do app

#### Subsuperficies reais do season hub

- `?tab=calendar`:
  - Status: `implementada`
  - Tem hoje: lista de partidas, estado de coverage, links para match center
  - Evidencia: `SeasonHubContent.tsx`, `useMatchesList`, `api/src/routers/matches.py`
- `?tab=standings`:
  - Status: `implementada`
  - Tem hoje: standings simples e estruturado por fase/grupo quando `competition-structure` existe
  - Evidencia: `SeasonHubContent.tsx`, `useStandingsTable`, `useGroupStandingsTable`, `api/src/routers/standings.py`, `api/src/routers/competition_hub.py`
- `?tab=rankings`:
  - Status: `implementada`
  - Tem hoje: preview de rankings principais + bloco avancado de analytics da competicao
  - Evidencia: `SeasonHubContent.tsx`, `SeasonCompetitionAnalyticsSection.tsx`, `api/src/routers/rankings.py`, `api/src/routers/competition_hub.py`
- analytics estruturais avancados dentro de `?tab=rankings`:
  - Status: `parcial`
  - Tem hoje: analytics por fase, comparativo historico entre temporadas e bracket estrutural
  - Limite atual: dependem de `competition-structure`; sem isso o app mostra aviso e nao materializa comparativos/bracket completos
  - Evidencia: `SeasonCompetitionAnalyticsSection.tsx`, `SeasonHubContent.tsx`, `api/src/routers/competition_hub.py`

### 2.6. Lista de partidas

- Pagina: Lista de partidas
- Rota/url: `/matches`
- Status: `implementada`
- Objetivo: descoberta de jogos com filtros globais e entrada para o match center
- O que tem hoje:
  - hero da superficie
  - agrupamento/ordenacao de partidas
  - filtros locais
  - prefetch do match center
  - links para `/matches/[matchId]`
- O que deveria ter para cumprir bem seu papel:
  - continuar como superficie de descoberta transversal
  - manter contexto explicito na URL ao entrar no detalhe
- Entidades/dados envolvidos:
  - `GET /api/v1/matches`
  - contexto global e janela temporal
- Origem da evidencia:
  - `frontend/src/app/(platform)/matches/page.tsx`
  - `api/src/routers/matches.py`
  - `docs/FRONTEND_IMPLEMENTATION_STATUS.md`
- Observacoes relevantes:
  - e superficie canonicamente transversal; nao depende de competition hub para existir

### 2.7. Match center

- Pagina: Match center
- Rota/url: `/matches/[matchId]`
- Status: `implementada`
- Objetivo: consolidar leitura de uma partida em um unico detalhe
- O que tem hoje:
  - header contextual com links para clubes
  - tabs internas
  - coverage por secao
  - links de retorno para `matches`, `season hub` e `rankings`
- O que deveria ter para cumprir bem seu papel:
  - continuar como detalhe canonico da partida
  - manter independencia de carregamento por secao
- Entidades/dados envolvidos:
  - `GET /api/v1/matches/{matchId}`
  - timeline, lineups, teamStats, playerStats
- Origem da evidencia:
  - `frontend/src/app/(platform)/matches/[matchId]/page.tsx`
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx`
  - `frontend/src/features/matches/components/MatchCenterHeader.tsx`
  - `api/src/routers/matches.py`
- Observacoes relevantes:
  - o detalhe trava o contexto da partida e distribui para clubes, jogadores e rankings

#### Subsuperficies reais do match center

- `summary`:
  - Status: `implementada`
  - Tem hoje: placar, destaques, cards de entrada para as demais abas
- `timeline`:
  - Status: `implementada`
  - Tem hoje: eventos cronologicos e links contextuais para player/team
- `lineups`:
  - Status: `implementada`
  - Tem hoje: titulares, banco e formacao quando disponivel
- `team-stats`:
  - Status: `implementada`
  - Tem hoje: comparativo lado a lado dos times
- `player-stats`:
  - Status: `implementada`
  - Tem hoje: atuacao individual por jogador
- Evidencia comum:
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx`
  - `frontend/src/features/matches/components/*`

### 2.8. Lista de times

- Pagina: Lista de times
- Rota/url: `/teams`
- Status: `implementada`
- Objetivo: descoberta de times no recorte atual e entrada para o perfil canonico
- O que tem hoje:
  - hero com cobertura e resumo do contexto
  - filtro local por nome
  - cards de time com KPIs e CTA para perfil
  - caminho canonico quando ha contexto; resolver curto quando nao ha
- O que deveria ter para cumprir bem seu papel:
  - manter o papel de descoberta
  - nao competir com competition hub
- Entidades/dados envolvidos:
  - `GET /api/v1/teams`
  - contexto global
- Origem da evidencia:
  - `frontend/src/app/(platform)/teams/page.tsx`
  - `frontend/src/features/teams/components/TeamsPageContent.tsx`
  - `api/src/routers/teams.py`
- Observacoes relevantes:
  - esta e a rota viva de times; `clubs` existe so como legado/compatibilidade

### 2.9. Resolver curto de time

- Pagina: Resolver curto de time
- Rota/url: `/teams/[teamId]`
- Status: `implementada`
- Objetivo: resolver o melhor contexto e redirecionar para o perfil canonico
- O que tem hoje:
  - loading state
  - consulta a `GET /api/v1/teams/{teamId}/contexts`
  - redirect para `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]`
  - fallback com mensagens de erro/indisponibilidade
- O que deveria ter para cumprir bem seu papel:
  - continuar sendo apenas camada de compatibilidade/deep link
  - nao virar tela de dados
- Entidades/dados envolvidos:
  - `defaultContext`
  - filtros retidos na query
- Origem da evidencia:
  - `frontend/src/app/(platform)/teams/[teamId]/page.tsx`
  - `frontend/src/app/(platform)/teams/[teamId]/TeamRouteResolver.tsx`
  - `api/src/routers/teams.py`
  - `docs/BFF_API_CONTRACT.md`
- Observacoes relevantes:
  - o resolver existe tambem indiretamente em `/clubs/[clubId]`

### 2.10. Perfil canonico de time

- Pagina: Perfil canonico de time
- Rota/url: `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]`
- Status: `implementada`
- Objetivo: detalhar campanha, elenco, jornada, partidas e stats do time no contexto atual
- O que tem hoje:
  - hero do clube
  - breadcrumbs
  - links estruturais para `players`, `rankings`, `matches`
  - tabs internas
  - `coverage` por secao
- O que deveria ter para cumprir bem seu papel:
  - fechar melhor a leitura de elenco com availability/sidelined
  - preservar contexto canonico em todo link de saida
- Entidades/dados envolvidos:
  - `GET /api/v1/teams/{teamId}`
  - `GET /api/v1/matches` filtrado por `teamId`
  - `GET /api/v1/team-journey-history`
- Origem da evidencia:
  - `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]/page.tsx`
  - `frontend/src/features/teams/components/TeamProfileContent.tsx`
  - `frontend/src/features/teams/components/TeamJourneySection.tsx`
  - `api/src/routers/teams.py`
  - `api/src/routers/competition_hub.py`
- Observacoes relevantes:
  - esta e a rota real de dados; nao usar `/teams/[teamId]` como pagina final

#### Subsuperficies reais do team profile

- `overview`:
  - Status: `implementada`
  - Tem hoje: resumo competitivo, forma, standing e atalhos estruturais
- `journey`:
  - Status: `implementada`
  - Tem hoje: jornada historica por temporadas/fases dentro da competicao
- `squad`:
  - Status: `parcial`
  - Tem hoje: elenco com minutos/aparicoes/ultima aparicao
  - Falta principal: availability/sidelined
- `matches`:
  - Status: `implementada`
  - Tem hoje: historico de partidas com links para match center
- `stats`:
  - Status: `implementada`
  - Tem hoje: agregados e tendencia mensal
- Evidencia comum:
  - `frontend/src/features/teams/components/TeamProfileContent.tsx`
  - `frontend/src/features/teams/components/TeamOverviewSection.tsx`
  - `frontend/src/features/teams/components/TeamJourneySection.tsx`
  - `frontend/src/features/teams/components/TeamSquadSection.tsx`
  - `frontend/src/features/teams/components/TeamMatchesSection.tsx`
  - `frontend/src/features/teams/components/TeamStatsSection.tsx`

### 2.11. Lista de jogadores

- Pagina: Lista de jogadores
- Rota/url: `/players`
- Status: `implementada`
- Objetivo: descoberta de jogadores e entrada para o perfil canonico
- O que tem hoje:
  - hero com leitura do contexto
  - filtros locais
  - tabela/lista com metricas
  - caminho canonico quando o contexto esta resolvido
  - comparativo de jogadores habilitado a partir da selecao
- O que deveria ter para cumprir bem seu papel:
  - manter a descoberta por contexto e o comparativo leve
  - nao depender de rota separada para comparacao se o painel atual continuar suficiente
- Entidades/dados envolvidos:
  - `GET /api/v1/players`
  - prefetch de `GET /api/v1/players/{playerId}`
- Origem da evidencia:
  - `frontend/src/app/(platform)/players/page.tsx`
  - `api/src/routers/players.py`
  - `docs/FRONTEND_IMPLEMENTATION_STATUS.md`
- Observacoes relevantes:
  - a lista ja opera como superficie de entrada e de comparacao

### 2.12. Comparativo de jogadores

- Pagina: Comparativo de jogadores
- Rota/url: sem rota propria; painel global embutido abaixo do conteudo principal
- Status: `implementada`
- Objetivo: comparar ate dois jogadores selecionados a partir da lista
- O que tem hoje:
  - estado global em store
  - selecao de ate dois ids
  - leitura lado a lado
  - deltas por metrica
  - coverage combinada
- O que deveria ter para cumprir bem seu papel:
  - continuar subordinado ao contexto da lista atual
  - so virar pagina propria se surgir necessidade de compartilhamento/deep link
- Entidades/dados envolvidos:
  - perfis resumidos de dois jogadores
  - metricas derivadas do summary
- Origem da evidencia:
  - `frontend/src/features/players/components/PlayerComparisonPanel.tsx`
  - `frontend/src/shared/stores/comparison.store.ts`
  - `frontend/src/app/(platform)/PlatformShell.tsx`
  - `frontend/src/app/(platform)/players/page.tsx`
- Observacoes relevantes:
  - e uma superficie real de produto, mas sem URL propria

### 2.13. Resolver curto de jogador

- Pagina: Resolver curto de jogador
- Rota/url: `/players/[playerId]`
- Status: `implementada`
- Objetivo: resolver contexto e redirecionar para o perfil canonico
- O que tem hoje:
  - loading state
  - consulta a `GET /api/v1/players/{playerId}/contexts`
  - redirect para a rota canonica contextualizada
  - fallback controlado
- O que deveria ter para cumprir bem seu papel:
  - continuar apenas como deep link e compatibilidade
  - nao virar tela final de dados
- Entidades/dados envolvidos:
  - `defaultContext`
  - filtros retidos na query
- Origem da evidencia:
  - `frontend/src/app/(platform)/players/[playerId]/page.tsx`
  - `frontend/src/app/(platform)/players/[playerId]/PlayerRouteResolver.tsx`
  - `api/src/routers/players.py`
  - `docs/BFF_API_CONTRACT.md`
- Observacoes relevantes:
  - a mesma regra de compatibilidade aparece nos docs de arquitetura/entrega

### 2.14. Perfil canonico de jogador

- Pagina: Perfil canonico de jogador
- Rota/url: `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]`
- Status: `implementada`
- Objetivo: detalhar jogador no contexto atual da temporada
- O que tem hoje:
  - hero do jogador
  - breadcrumbs
  - links para time, season hub, rankings e matches
  - tabs internas
  - bloco de insights no overview
- O que deveria ter para cumprir bem seu papel:
  - continuar honesto sobre limite de historico real
  - manter contexto canonico consistente em toda navegacao
- Entidades/dados envolvidos:
  - `GET /api/v1/players/{playerId}`
  - `GET /api/v1/insights`
- Origem da evidencia:
  - `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]/page.tsx`
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`
  - `frontend/src/features/players/components/*`
  - `api/src/routers/players.py`
  - `api/src/routers/insights.py`
- Observacoes relevantes:
  - esta e a rota canonica de dados; `/players/[playerId]` e so resolver

#### Subsuperficies reais do player profile

- `overview`:
  - Status: `implementada`
  - Tem hoje: resumo competitivo, ultima participacao e insights
- `history`:
  - Status: `implementada`
  - Tem hoje: historico por contextos comprovados do projeto
- `matches`:
  - Status: `implementada`
  - Tem hoje: partidas recentes e links para match center
- `stats`:
  - Status: `implementada`
  - Tem hoje: agregados e tendencia mensal
- Evidencia comum:
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`
  - `frontend/src/features/players/components/PlayerOverviewSection.tsx`
  - `frontend/src/features/players/components/PlayerHistorySection.tsx`
  - `frontend/src/features/players/components/PlayerMatchesSection.tsx`
  - `frontend/src/features/players/components/PlayerStatsSection.tsx`

### 2.15. Familia de paginas de rankings

- Pagina: Ranking detalhado por metrica
- Rota/url: `/rankings/[rankingType]`
- Status: `implementada`
- Objetivo: leitura detalhada de lideres por metrica, com saida para team/player
- O que tem hoje:
  - pagina dinamica unica
  - fallback quando `rankingType` e invalido
  - links para entidades e rankings relacionados
  - contexto preservado na query
- O que deveria ter para cumprir bem seu papel:
  - tratar rankings `unsupported` de modo claro ou ocultar da navegacao principal
  - avaliar se faz sentido um hub de rankings no futuro; hoje nao ha evidencia forte suficiente para exigir `/rankings`
- Entidades/dados envolvidos:
  - `GET /api/v1/rankings/{rankingType}`
  - registry de metricas/rankings
- Origem da evidencia:
  - `frontend/src/app/(platform)/rankings/[rankingType]/page.tsx`
  - `frontend/src/config/ranking.registry.ts`
  - `api/src/routers/rankings.py`
- Observacoes relevantes:
  - a shell navega para `player-goals` como ranking padrao; nao existe rota indice `/rankings`

Ranking types vivos no registry hoje:
- `player-goals`
- `player-assists`
- `player-shots-total`
- `player-shots-on-target`
- `player-pass-accuracy`
- `player-rating`
- `player-yellow-cards`
- `team-possession`
- `team-pass-accuracy`

Observacao relevante:
- `player-pass-accuracy` esta marcada como `unsupported` em `api/src/routers/rankings.py`

### 2.16. Mercado

- Pagina: Mercado
- Rota/url: `/market`
- Status: `placeholder`
- Objetivo: seria o dominio de transferencias
- O que tem hoje:
  - `PlatformStateSurface`
  - aviso de indisponibilidade
  - saida para `players`, `teams` e `matches`
- O que deveria ter para cumprir bem seu papel:
  - pagina real de transferencias
  - filtros por janela/time/jogador
  - BFF dedicado
- Entidades/dados envolvidos:
  - `player_transfers` apenas como evidencia de backend/dado potencial
- Origem da evidencia:
  - `frontend/src/app/(platform)/market/page.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- Observacoes relevantes:
  - nao encontrei link ativo da shell principal para `/market`

### 2.17. Head-to-head

- Pagina: Head-to-head
- Rota/url: `/head-to-head`
- Status: `placeholder`
- Objetivo: seria o comparativo historico entre dois times
- O que tem hoje:
  - `PlatformStateSurface`
  - aviso de indisponibilidade
  - saida para `matches`, `rankings` e `competitions`
- O que deveria ter para cumprir bem seu papel:
  - comparativo real entre dois clubes
  - recortes por periodo e mando
  - BFF dedicado
- Entidades/dados envolvidos:
  - `head_to_head` no acervo de dados
- Origem da evidencia:
  - `frontend/src/app/(platform)/head-to-head/page.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- Observacoes relevantes:
  - ha CTA da home para essa rota, mas a pagina ainda nao entrega o dominio prometido

### 2.18. Tecnicos - detalhe

- Pagina: Perfil de tecnico
- Rota/url: `/coaches/[coachId]`
- Status: `placeholder`
- Objetivo: seria o detalhe de um tecnico
- O que tem hoje:
  - `PlatformStateSurface`
  - aviso de indisponibilidade
  - saida para `teams`, `matches` e `competitions`
- O que deveria ter para cumprir bem seu papel:
  - detalhe real por tecnico
  - historico de passagens
  - conexao com times/competicoes
- Entidades/dados envolvidos:
  - `team_coaches` no acervo
- Origem da evidencia:
  - `frontend/src/app/(platform)/coaches/[coachId]/page.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- Observacoes relevantes:
  - nao existe rota indice `/coaches` no app real

### 2.19. Auditoria

- Pagina: Auditoria
- Rota/url: `/audit`
- Status: `placeholder`
- Objetivo: rota interna/nao publica
- O que tem hoje:
  - `PlatformStateSurface`
  - aviso de indisponibilidade
  - saida para `home`, `competitions` e `matches`
- O que deveria ter para cumprir bem seu papel:
  - ou permanecer interna e sem descoberta publica
  - ou ser removida da arvore navegavel publica
- Entidades/dados envolvidos:
  - nenhuma superficie de negocio encontrada
- Origem da evidencia:
  - `frontend/src/app/(platform)/audit/page.tsx`
  - `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- Observacoes relevantes:
  - o proprio copy afirma que a rota nao faz parte da experiencia publica

### 2.20. Landing editorial/institucional

- Pagina: Landing editorial/institucional
- Rota/url: `/landing`
- Status: `placeholder`
- Objetivo: separar apresentacao institucional/editorial da home executiva
- O que tem hoje:
  - placeholder bruto com `TODO`
- O que deveria ter para cumprir bem seu papel:
  - explicar o produto
  - mostrar cobertura do acervo
  - orientar entrada no app principal
- Entidades/dados envolvidos:
  - potencialmente baixo acoplamento a BFF; foco mais editorial
- Origem da evidencia:
  - `frontend/src/app/(marketing)/landing/page.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `docs/FRONTEND_IMPLEMENTATION_STATUS.md`
- Observacoes relevantes:
  - nao encontrei link ativo do app para `/landing`

### 2.21. Rotas legadas e de compatibilidade

#### `/clubs`

- Pagina: Clubs legado
- Rota/url: `/clubs`
- Status: `implementada`
- Objetivo: compatibilidade legada
- O que tem hoje:
  - redirect para `/competitions`
- O que deveria ter para cumprir bem seu papel:
  - ou parar de aparecer em CTAs publicos
  - ou redirecionar para `/teams` se a intencao continuar sendo "clubes"
- Entidades/dados envolvidos:
  - nenhuma tela propria
- Origem da evidencia:
  - `frontend/src/app/(platform)/clubs/page.tsx`
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
- Observacoes relevantes:
  - o quick link "Clubes" da home hoje cai nesta rota e acaba abrindo `competitions`, nao `teams`

#### `/clubs/[clubId]`

- Pagina: Clube legado por id
- Rota/url: `/clubs/[clubId]`
- Status: `implementada`
- Objetivo: compatibilidade legada para detalhe curto
- O que tem hoje:
  - encaminha para `TeamRouteResolver`
- O que deveria ter para cumprir bem seu papel:
  - permanecer apenas como camada de compatibilidade
- Entidades/dados envolvidos:
  - time id
- Origem da evidencia:
  - `frontend/src/app/(platform)/clubs/[clubId]/page.tsx`
  - `frontend/src/app/(platform)/teams/[teamId]/TeamRouteResolver.tsx`

#### `/competition/[competitionId]`

- Pagina: Competicao legada por id
- Rota/url: `/competition/[competitionId]`
- Status: `implementada`
- Objetivo: compatibilidade legada
- O que tem hoje:
  - redirect para `/competitions/[competitionKey]`
- O que deveria ter para cumprir bem seu papel:
  - permanecer so como alias tecnico
- Entidades/dados envolvidos:
  - mapeamento `competitionId -> competitionKey`
- Origem da evidencia:
  - `frontend/src/app/(platform)/competition/[competitionId]/page.tsx`
  - `frontend/src/shared/utils/context-routing.ts`

## 3. Paginas implicitas ou faltantes

### 3.1. Evidencia forte

#### `/coaches`

- Status: `nao criada`
- Porque faz sentido existir:
  - o dominio de tecnicos ja aparece como modulo de produto em multiplos docs
  - ja existe detalhe placeholder em `/coaches/[coachId]`
  - o plano recente pede explicitamente criar a rota indice
- Evidencia:
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - ausencia objetiva de `frontend/src/app/(platform)/coaches/page.tsx`
- Base da inferencia:
  - inferencia forte; a rota e explicitamente nomeada mais de uma vez e o detalhe ja existe

#### Availability/sidelined dentro do squad do team profile

- Status: `implicita`
- Porque faz sentido existir:
  - o team profile ja tem aba `squad`
  - a documentacao recente convergiu para availability primeiro como secao do squad
  - o dado `team_sidelined` existe no acervo
- Evidencia:
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_ARCHITECTURE.md`
  - `docs/FRONTEND_DELIVERY_PLAN.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `frontend/src/features/teams/components/TeamSquadSection.tsx`
  - ausencia de shape `availability` em `frontend/src/features/teams/types/teams.types.ts`
- Base da inferencia:
  - inferencia forte; a superficie canonica ja existe e a lacuna e de conteudo/contrato, nao de IA

#### Modulo real de `head-to-head`

- Status: `implicita`
- Porque faz sentido existir:
  - a rota existe
  - a home ja anuncia comparativos
  - o dado `head_to_head` ja aparece como disponivel em docs do projeto
- Evidencia:
  - `frontend/src/app/(platform)/head-to-head/page.tsx`
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- Base da inferencia:
  - inferencia forte; a rota e publica, tem CTA e tem dado citado

#### Modulo real de `market`

- Status: `implicita`
- Porque faz sentido existir:
  - a rota existe
  - ha dado de `player_transfers`
  - varios docs tratam isso como pagina dedicada futura
- Evidencia:
  - `frontend/src/app/(platform)/market/page.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- Base da inferencia:
  - inferencia forte; nao e ideia abstrata, ja ha rota e dominio nomeado

#### Pagina real de coach detail

- Status: `implicita`
- Porque faz sentido existir:
  - ja existe `/coaches/[coachId]` placeholder
  - dominio de coaches esta previsto com dado proprio
- Evidencia:
  - `frontend/src/app/(platform)/coaches/[coachId]/page.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- Base da inferencia:
  - inferencia forte; a casca da rota ja foi criada

#### Landing institucional real

- Status: `implicita`
- Porque faz sentido existir:
  - ha placeholder em `/landing`
  - docs separam home executiva de landing institucional
- Evidencia:
  - `frontend/src/app/(marketing)/landing/page.tsx`
  - `docs/analise de conteudo pouco aproveitado.md`
  - `docs/FRONTEND_IMPLEMENTATION_STATUS.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
- Base da inferencia:
  - inferencia forte; a rota ja existe e a separacao de papel e explicita

### 3.2. Evidencia media

#### Busca global com `coach`

- Status: `implicita`
- Porque faz sentido existir:
  - docs antigas e planos de produto citam tecnico como entidade de busca
  - o dominio de coaches esta previsto
- Evidencia:
  - `docs/FRONTEND_DELIVERY_PLAN.md`
  - `api/src/routers/search.py` limita `SearchType` a `competition`, `team`, `player`, `match`
- Base da inferencia:
  - inferencia media; a necessidade aparece nos planos, mas ainda nao ha implementacao parcial no frontend

#### Global availability route

- Status: `nao criada`
- Porque faz sentido existir:
  - docs antigas falam em availability como tela global
  - docs mais recentes priorizam primeiro o encaixe no squad do team profile
- Evidencia:
  - `docs/FRONTEND_ARCHITECTURE.md`
  - `docs/FRONTEND_DELIVERY_PLAN.md`
  - `docs/FRONTEND_REFERENCE_INVENTORY.md`
  - `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - ausencia de qualquer `page.tsx` de availability em `frontend/src/app`
- Base da inferencia:
  - inferencia media e ambigua; o dominio existe, mas a forma final ainda oscila entre secao interna e tela global

### 3.3. Sinais fracos ou ambiguos

#### `/rankings` como hub proprio

- Status: `nao criada`
- Porque pode fazer sentido existir:
  - o produto trata rankings como area principal
  - a shell mostra "Rankings" como categoria
- Evidencia:
  - `frontend/src/app/(platform)/PlatformShell.tsx` navega para `/rankings/player-goals`
  - nao existe `frontend/src/app/(platform)/rankings/page.tsx`
- Base da inferencia:
  - sinal fraco; o app atual resolveu isso com um ranking default, nao com um hub ausente

## 4. Mapa hierarquico de navegacao

### 4.1. Navegacao principal da shell

```text
/
├── /
├── /competitions
├── /rankings/player-goals   # ranking padrao da shell
├── /matches
├── /players
└── /teams
```

### 4.2. Fluxo canonico do produto

```text
/
├── competicoes
│   └── /competitions
│       └── /competitions/[competitionKey]
│           └── /competitions/[competitionKey]/seasons/[seasonLabel]
│               ├── ?tab=calendar
│               │   └── /matches/[matchId]
│               ├── ?tab=standings
│               │   └── /competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]
│               └── ?tab=rankings
│                   ├── /rankings/[rankingType]
│                   ├── /competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]
│                   └── /competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]
├── /matches
│   └── /matches/[matchId]
│       ├── ?tab=summary
│       ├── ?tab=timeline
│       ├── ?tab=lineups
│       ├── ?tab=team-stats
│       └── ?tab=player-stats
├── /teams
│   ├── /teams/[teamId]  # resolver
│   └── /competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]
│       ├── ?tab=overview
│       ├── ?tab=journey
│       ├── ?tab=squad
│       ├── ?tab=matches
│       └── ?tab=stats
└── /players
    ├── /players/[playerId]  # resolver
    └── /competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]
        ├── ?tab=overview
        ├── ?tab=history
        ├── ?tab=matches
        └── ?tab=stats
```

### 4.3. Entradas transversais

```text
Busca global
├── competition -> /competitions/[competitionKey]
├── team -> /teams/[teamId] -> rota canonica
├── player -> /players/[playerId] -> rota canonica
└── match -> /matches/[matchId]

Home executiva
├── competicoes -> /competitions
├── rankings -> /rankings/player-goals
├── comparativos -> /head-to-head
├── clubes -> /clubs -> /competitions   # inconsistente
├── partidas -> /matches
└── editorial highlights -> player canonico
```

### 4.4. Rotas secundarias, legadas e auxiliares

```text
Secundarias atuais
├── /market
├── /head-to-head
├── /coaches/[coachId]
├── /landing
└── /audit

Legado/compatibilidade
├── /clubs -> /competitions
├── /clubs/[clubId] -> /teams/[teamId] -> rota canonica
└── /competition/[competitionId] -> /competitions/[competitionKey]
```

## 5. Lacunas, sobreposicoes e inconsistencias

### 5.1. Divergencia entre blueprint e app vivo

- docs antigas ainda apontam `/home`, `/h2h` e `/more/*`
- app vivo usa `/`, `/head-to-head`, `/market` e detalhe isolado `/coaches/[coachId]`
- a rota global de availability nao existe no app vivo

### 5.2. CTA "Clubes" da home esta semanticamente quebrada

- evidencia:
  - `HomeExecutivePage.tsx` envia "Clubes" para `/clubs`
  - `/clubs` redireciona para `/competitions`
- efeito:
  - a CTA promete lista de clubes, mas leva para competicoes

### 5.3. Modulos secundarios existem como rota, mas nao como produto

- `/market`
- `/head-to-head`
- `/coaches/[coachId]`
- `/landing`
- `/audit`

Todas essas rotas ja estao materializadas como casca visual, mas nao como superficie funcional do dominio.

### 5.4. Descoberta desigual dos modulos

- `/head-to-head` tem pelo menos um CTA na home
- `/market` nao apareceu ligado por CTA relevante na shell/home durante a inspecao
- `/coaches/[coachId]` nao tem rota indice e nao apareceu ligado por CTA publico
- `/landing` nao apareceu ligada a partir do app
- `/audit` explicitamente nao e rota publica

### 5.5. Ausencia de `/coaches`

- o detalhe existe
- o dominio e citado em docs
- mas nao ha pagina indice
- isso quebra a navegabilidade portfolio-wide do dominio

### 5.6. Availability aparece como promessa recorrente, mas nao como superficie real

- aparece em docs de arquitetura, entrega, inventario e expansao
- nao ha rota global
- nao ha shape em tipos de team profile
- o encaixe real mais coerente hoje e `team profile ?tab=squad`

### 5.7. Rankings sem hub proprio

- a shell chama a area de "Rankings"
- o app abre `player-goals` como default
- nao ha `page.tsx` em `/rankings`
- nao e bug por si so, mas e uma escolha de IA que pode confundir expectativa de "area de rankings"

### 5.8. Promessa desigual dentro do season hub

- `calendar`, `standings` e `rankings` base estao fechados
- comparativos/bracket/analytics avancados dentro de `rankings` dependem de `competition-structure`
- resultado: a promessa de profundidade nao e uniforme entre temporadas/competicoes

## 6. Matriz final resumida

Escala de prioridade usada aqui:
- `P0`: nucleo vivo do produto
- `P1`: lacuna forte em rota ja publica
- `P2`: superficie implicita forte, dependente de contrato/dados
- `P3`: legado, institucional ou auxiliar

| Pagina | Rota | Status | Prioridade | Evidencia | Observacao curta |
| --- | --- | --- | --- | --- | --- |
| Home executiva | `/` | `implementada` | `P0` | `HomeExecutivePage.tsx`, `home.py` | Entrada principal real do produto |
| Busca global | sem rota propria | `implementada` | `P0` | `GlobalSearchOverlay.tsx`, `search.py` | So busca `competition/team/player/match` |
| Catalogo de competicoes | `/competitions` | `implementada` | `P0` | `competitions/page.tsx` | Entrada principal da shell |
| Competition hub | `/competitions/[competitionKey]` | `implementada` | `P0` | `CompetitionHubContent.tsx` | Hub de distribuicao de temporadas |
| Season hub | `/competitions/[competitionKey]/seasons/[seasonLabel]` | `implementada` | `P0` | `SeasonHubContent.tsx` | Contexto canonico do produto |
| Analytics avancados da temporada | `/competitions/[competitionKey]/seasons/[seasonLabel]?tab=rankings` | `parcial` | `P1` | `SeasonCompetitionAnalyticsSection.tsx` | Dependem de `competition-structure` |
| Lista de partidas | `/matches` | `implementada` | `P0` | `matches/page.tsx` | Descoberta transversal de jogos |
| Match center | `/matches/[matchId]` | `implementada` | `P0` | `MatchCenterContent.tsx`, `matches.py` | Detalhe canonico de partida |
| Lista de times | `/teams` | `implementada` | `P0` | `TeamsPageContent.tsx`, `teams.py` | Superficie viva de descoberta |
| Resolver curto de time | `/teams/[teamId]` | `implementada` | `P3` | `TeamRouteResolver.tsx` | Compatibilidade/deep link |
| Perfil canonico de time | `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]` | `implementada` | `P0` | `TeamProfileContent.tsx` | Rota real de dados de time |
| Aba `squad` com availability | mesma rota do team profile | `parcial` | `P1` | `TeamSquadSection.tsx`, docs de availability | Elenco existe; availability ainda nao |
| Lista de jogadores | `/players` | `implementada` | `P0` | `players/page.tsx`, `players.py` | Descoberta + entrada para comparativo |
| Comparativo de jogadores | sem rota propria | `implementada` | `P1` | `PlayerComparisonPanel.tsx` | Superficie real, sem deep link |
| Resolver curto de jogador | `/players/[playerId]` | `implementada` | `P3` | `PlayerRouteResolver.tsx` | Compatibilidade/deep link |
| Perfil canonico de jogador | `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]` | `implementada` | `P0` | `PlayerProfileContent.tsx` | Rota real de dados de player |
| Ranking detalhado | `/rankings/[rankingType]` | `implementada` | `P0` | `rankings/[rankingType]/page.tsx`, registry | Family route viva com 9 tipos |
| Head-to-head | `/head-to-head` | `placeholder` | `P1` | `head-to-head/page.tsx` | Tem CTA publico, mas nao tem produto real |
| Market | `/market` | `placeholder` | `P1` | `market/page.tsx` | Dominio previsto, sem modulo real |
| Coaches index | `/coaches` | `nao criada` | `P1` | docs + ausencia de `page.tsx` | Dominio previsto, indice inexistente |
| Coach detail | `/coaches/[coachId]` | `placeholder` | `P1` | `coaches/[coachId]/page.tsx` | Detalhe ja exposto, ainda vazio |
| Landing institucional | `/landing` | `placeholder` | `P2` | `landing/page.tsx` | Rota existe, conteudo nao |
| Availability global | sem rota real; docs oscilam entre `/availability` e `/more/availability` | `nao criada` | `P2` | docs de arquitetura/entrega/expansao | Forte como dominio, ambigua como IA final |
| Clubs legado | `/clubs` | `implementada` | `P3` | `clubs/page.tsx` | Redirect legado para `/competitions` |
| Club detail legado | `/clubs/[clubId]` | `implementada` | `P3` | `clubs/[clubId]/page.tsx` | Encaminha para resolver de team |
| Competition legado | `/competition/[competitionId]` | `implementada` | `P3` | `competition/[competitionId]/page.tsx` | Alias tecnico para hub canonico |

## 7. Veredito operacional

Leitura objetiva do estado atual:
- o produto real ja tem um nucleo claro e navegavel: home, competicoes, temporada, partidas, rankings, times e jogadores
- a arquitetura viva e muito mais madura do que os modulos secundarios
- a principal lacuna nao esta no nucleo; esta nos dominios `head-to-head`, `market`, `coaches`, `landing` e `availability`
- ha tambem lacunas de consistencia de IA, especialmente em rotas antigas ainda expostas por CTAs ou por docs desatualizadas

Se a meta for priorizacao futura, o recorte mais fiel e:
- preservar o nucleo vivo como esta
- corrigir primeiro inconsistencias de navegacao/semantica
- depois fechar os dominios secundarios que ja tem evidencia forte e rota prevista
