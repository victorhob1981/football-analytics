# FRONTEND_IMPLEMENTATION_STATUS

Data de atualização: 2026-03-22  
Critério: estado real do código validado no working tree, com teste focado, E2E e typecheck.  
Escopo deste registro: núcleo estrutural `competition -> season` + fechamento do núcleo funcional de `busca global` + convergência da shell global + fechamento do fluxo `matches -> match center` + fechamento dos domínios `teams` e `players` + fechamento da `home executiva` + convergência visual sistêmica dos fluxos já verdes + consolidação das superfícies globais de sistema + hardening técnico/readiness de entrega do `frontend/` atual.

## 1. Resumo executivo

O frontend continua híbrido no nível de cobertura total do produto, mas o **eixo principal já verde agora está enquadrado por uma shell global convergida e validada por navegação real**. Os fluxos de partidas, `teams` e `players` deixaram de ser telas isoladas, a rota inicial passou a ser uma **home executiva real** e as superfícies principais agora compartilham a mesma linguagem de hero, tabs, states e densidade, sem app paralelo e sem migração big bang.

Fechado neste núcleo:

- `/competitions`
- `competition hub`
- `season hub`
- `calendar`
- `standings`
- `rankings`
- navegação e contexto canônicos entre esses pontos
- `coverage-state` funcional nesses módulos

Ainda fora deste fechamento:

- módulos secundários (`market`, `coaches`, `availability`, `head-to-head`)
- home editorial/institucional separada da entrada executiva de produto

## 2. Matriz do núcleo competição/temporada

| Item | Status | Evidência curta |
| --- | --- | --- |
| Shell mínima do fluxo | `COMPLETO` | `frontend/src/app/(platform)/PlatformShell.tsx` substituiu a sidebar legada por header global + launcher persistente de busca, mantendo `GlobalFilterBar` no mesmo app. |
| `/competitions` | `COMPLETO` | Entrada consolidada em `frontend/src/app/(platform)/competitions/page.tsx`. |
| `competition hub` | `COMPLETO` | Página e conteúdo ativos em `frontend/src/app/(platform)/competitions/[competitionKey]/page.tsx` e `frontend/src/features/competitions/components/CompetitionHubContent.tsx`. |
| `season hub` | `COMPLETO` | Página canônica ativa em `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/page.tsx` e `SeasonHubContent.tsx`. |
| `calendar` | `COMPLETO` | Aba integrada ao hub reaproveitando `GET /api/v1/matches`. |
| `standings` | `COMPLETO` | Aba integrada ao hub usando `GET /api/v1/standings` com stage + rounds metadata reais. |
| `rankings` | `COMPLETO` | Aba integrada ao hub reaproveitando contratos já verdes de rankings. |
| Navegação canônica | `COMPLETO` | Paths e sync de contexto em `frontend/src/shared/utils/context-routing.ts`, `CanonicalRouteContextSync.tsx` e `CompetitionRouteContextSync.tsx`. |
| `coverage-state` do núcleo | `COMPLETO` | `calendar`, `standings` e `rankings` usam `useQueryWithCoverage` e feedback explícito de `loading/empty/partial/error`. |
| Resolvers curtos (`/players/:id`, `/teams/:id`) | `COMPLETO` | `PlayerRouteResolver.tsx` e `TeamRouteResolver.tsx` agora re-normalizam contextos do BFF antes de montar rotas canônicas split-year. |

## 3. Núcleo matches + match center

| Item | Status | Evidência curta |
| --- | --- | --- |
| `/matches` | `COMPLETO` | Lista segue como superfície de descoberta e entrada para o detalhe em `frontend/src/app/(platform)/matches/page.tsx`, preservando recorte explícito na URL. |
| Entrada no `match center` | `COMPLETO` | Links de partida montam `/matches/:matchId` com `competitionId`, `seasonId` e filtros extras relevantes, mantendo o contexto do fluxo. |
| Header + summary | `COMPLETO` | `MatchCenterHeader.tsx` e `MatchCenterContent.tsx` usam o payload real da partida e mantêm retorno estrutural para `competitions`, `season hub` e `matches`. |
| `timeline` | `COMPLETO` | `MatchTimelinePlaceholder.tsx` passou a operar como seção real, com eventos cronológicos, links contextuais e `coverage-state` próprio. |
| `lineups` | `COMPLETO` | `MatchLineupsPlaceholder.tsx` agora usa `formationPosition`, `formationField`, `minutesPlayed`, titulares/banco e `coverage-state` honesto. |
| `player stats` | `COMPLETO` | `MatchPlayerStatsPlaceholder.tsx` usa contrato expandido com `positionName`, `shotsOnGoal`, `keyPasses`, `xg`, `cards` e links canônicos. |
| Cobertura por seção | `COMPLETO` | `GET /api/v1/matches/{matchId}` retorna `sectionCoverage.timeline|lineups|playerStats`, além de `meta.coverage` agregado do match center. |
| Navegação contextual | `COMPLETO` | Match center trava `competition/season` no `GlobalFilterBar`, preserva filtros extras e mantém links para `players`, `teams`, `matches` e `season hub`. |

## 4. Contratos BFF relevantes para o núcleo

| Contrato | Status | Observação |
| --- | --- | --- |
| `GET /api/v1/matches` | `COMPLETO` | Reutilizado no `calendar`. |
| `GET /api/v1/matches/{matchId}` | `COMPLETO` | Contrato consolidado do match center, com `timeline`, `lineups`, `teamStats`, `playerStats` e `sectionCoverage` reais por secao. |
| `GET /api/v1/rankings/{rankingType}` | `COMPLETO` | Reutilizado no `rankings`. |
| `GET /api/v1/standings` | `COMPLETO` | Novo contrato real para tabela canônica de temporada com `selectedRound`, `currentRound` e `rounds`. |
| `GET /api/v1/search` | `COMPLETO` | Contrato real da busca global para `competition`, `team`, `player` e `match`, sempre restrito a contextos canônicos suportados e com `meta.coverage` de navegabilidade. |

## 4.1. Domínio teams + team profile

| Item | Status | Evidência curta |
| --- | --- | --- |
| `/teams` | `COMPLETO` | Nova superfície de descoberta em `frontend/src/app/(platform)/teams/page.tsx`, ligada à shell e ao recorte global. |
| Entrada no `team profile` | `COMPLETO` | A lista e os links contextuais agora fecham diretamente na rota canônica quando o contexto está resolvido; sem contexto, o resolver curto continua disponível. |
| `overview` | `COMPLETO` | `TeamOverviewSection.tsx` consolida standing, forma, resumo competitivo e links estruturais no mesmo perfil. |
| `squad` | `COMPLETO` | `TeamSquadSection.tsx` usa `squad` agregado de `fact_fixture_lineups`, com links para player profile e coverage honesto quando há caveat de provider. |
| `matches` | `COMPLETO` | `TeamMatchesSection.tsx` reaproveita `GET /api/v1/matches` com `teamId`, preservando o recorte explícito até o match center. |
| `stats` | `COMPLETO` | `TeamStatsSection.tsx` usa agregados estáveis e trend mensal do contrato expandido do time. |
| Cobertura por seção | `COMPLETO` | `GET /api/v1/teams/{teamId}` agora retorna `sectionCoverage.overview|squad|stats`, além de `meta.coverage` agregado do perfil. |
| Navegação contextual | `COMPLETO` | O domínio mantém links válidos para `season hub`, `rankings`, `players`, `matches` e `match center`, todos no mesmo recorte. |

## 4.2. Contratos BFF relevantes para teams

| Contrato | Status | Observação |
| --- | --- | --- |
| `GET /api/v1/teams` | `COMPLETO` | Lista de times com paginação, ordenação estável e coverage explícito. |
| `GET /api/v1/teams/{teamId}` | `COMPLETO` | Contrato consolidado do team profile com `summary`, `recentMatches`, `squad`, `stats` e `sectionCoverage` reais por seção. |
| `GET /api/v1/teams/{teamId}/contexts` | `COMPLETO` | Resolver curto mantido para busca global, links externos e rotas legadas. |

## 4.3. Domínio players + player profile

| Item | Status | Evidência curta |
| --- | --- | --- |
| `/players` | `COMPLETO` | Superfície de descoberta já integrada à shell, agora com links canônicos que preservam filtros extras do recorte (`roundId`, `venue`, `lastN`, `dateRange`). |
| Entrada no `player profile` | `COMPLETO` | A lista, a busca global, `teams/squad` e links contextuais fecham diretamente na rota canônica quando o contexto está resolvido; sem contexto, o resolver curto continua disponível. |
| `overview` | `COMPLETO` | `PlayerOverviewSection.tsx` consolida resumo competitivo, KPIs estáveis, última participação e links estruturais para `team`, `matches`, `season hub` e `rankings`. |
| `history` | `COMPLETO` | `PlayerHistorySection.tsx` resume participação real por competição/temporada/time sem fingir career history além do que o projeto sustenta. |
| `matches` | `COMPLETO` | `PlayerMatchesSection.tsx` usa `recentMatches` reais, preserva o recorte explícito e entra no mesmo fluxo já verde de `matches + match center`. |
| `stats` | `COMPLETO` | `PlayerStatsSection.tsx` usa agregados estáveis por 90 minutos e trend mensal do contrato expandido do jogador. |
| Cobertura por seção | `COMPLETO` | `GET /api/v1/players/{playerId}` agora retorna `sectionCoverage.overview|history|matches|stats`, além de `meta.coverage` agregado do perfil. |
| Navegação contextual | `COMPLETO` | O domínio mantém links válidos para `team`, `matches`, `match center`, `season hub`, `rankings` e busca global, todos no mesmo recorte. |

## 4.4. Contratos BFF relevantes para players

| Contrato | Status | Observação |
| --- | --- | --- |
| `GET /api/v1/players` | `COMPLETO` | Lista de jogadores com paginação, ordenação estável, coverage explícito e navegação contextual preservando filtros extras. |
| `GET /api/v1/players/{playerId}` | `COMPLETO` | Contrato consolidado do player profile com `summary`, `recentMatches`, `history`, `stats` e `sectionCoverage` reais por seção. |
| `GET /api/v1/players/{playerId}/contexts` | `COMPLETO` | Resolver curto mantido para busca global, links externos e rotas legadas. |

## 5. Home executiva

| Item | Status | Evidência curta |
| --- | --- | --- |
| Rota inicial do app | `COMPLETO` | `frontend/src/app/(platform)/(home)/page.tsx` agora delega para uma home executiva real, em vez de um feed neutro de insights. |
| Hero e framing inicial | `COMPLETO` | `HomeExecutivePage.tsx` apresenta narrativa curta, contexto ativo, KPIs de preview e CTAs reais para season hub, competições, partidas e busca. |
| Bloco de competições | `COMPLETO` | A home reaproveita `SUPPORTED_COMPETITIONS` e links reais para `competition hub`, mantendo o eixo canônico como ponto principal de entrada. |
| Blocos de dados executivos | `COMPLETO` | `matches`, `teams`, `players` e `insights` são carregados com hooks e contratos já verdes, cada um com cobertura e links reais para seus domínios. |
| Integração com busca global | `COMPLETO` | A home abre o mesmo overlay real da shell por evento compartilhado, sem modal paralelo ou busca fake. |
| Estados e cobertura | `COMPLETO` | Cada bloco trata `loading`, `empty`, `error` e `partial` com feedback explícito e sem mascarar gap real de cobertura. |
| Navegação real | `COMPLETO` | E2E cobre home -> competições, home -> match center, home -> team profile, home -> player profile e abertura da busca global. |

## 6. Shell global convergida + busca global

| Item | Status | Evidência curta |
| --- | --- | --- |
| Header global | `COMPLETO` | Header persistente e orientado por rota em `frontend/src/app/(platform)/PlatformShell.tsx`, com navegação principal e launcher de busca sempre visíveis. |
| Framing global do produto | `COMPLETO` | `PlatformShellFrame.tsx` e `usePlatformShellState.ts` consolidam breadcrumbs, título de superfície, escopo ativo e atalhos estruturais coerentes por rota. |
| Persistência explícita de contexto | `COMPLETO` | Atalhos principais e estruturais agora carregam `competitionId`, `seasonId` e filtros extras relevantes na URL quando a superfície não é canônica; rotas canônicas mantêm contexto travado no path e extras na query. |
| Estados globais da shell | `COMPLETO` | `PlatformStateSurface`, `error.tsx`, `loading.tsx`, `not-found.tsx` e `GlobalErrorBoundary.tsx` alinham `loading/error/not-found` na mesma casca estrutural. |
| Limpeza estrutural mínima | `COMPLETO` | Entradas legadas conflitantes foram reencaminhadas para a estrutura nova (`/competition/:id -> /competitions/:key`, `/clubs -> /competitions`, `/clubs/:id -> /teams/:id`) preservando query relevante. |
| Busca global overlay | `COMPLETO` | Overlay real em `frontend/src/features/search/components/GlobalSearchOverlay.tsx`, acionado pelo header e por atalho `Ctrl/Cmd + K`. |
| Contrato da busca | `COMPLETO` | `competition`, `team`, `player` e `match` em `GET /api/v1/search`, com `defaultContext` canônico para resultados navegáveis e ordenação minimamente coerente por nome/contexto/atividade. |
| Navegação canônica a partir da busca | `COMPLETO` | Resultados de `player/team` usam resolver curto e fecham na rota `/competitions/:competitionKey/seasons/:seasonLabel/...`; `match` entra em `/matches/:matchId?competitionId&seasonId`. |
| Estados da busca | `COMPLETO` | Overlay cobre `loading`, `empty`, `error` e `partial` usando `meta.coverage` do endpoint e feedback explícito no rodapé/banner. |
| Cobertura da busca | `COMPLETO` | O núcleo fechado cobre `competition`, `team`, `player` e `match`. `coach`, histórico e atalhos avançados continuam fora de escopo deste ciclo, sem bloquear o fluxo principal. |

## 6.1. Convergência visual sistêmica

| Item | Status | Evidência curta |
| --- | --- | --- |
| Headers e heroes principais | `COMPLETO` | `competitions`, `season hub`, `match center`, `team profile`, `player profile`, `teams` e `home` agora compartilham hero executivo, tags de contexto e ritmo de KPI coerentes. |
| Tabs e navegação local | `COMPLETO` | `ProfileTabs` em `frontend/src/shared/components/profile/ProfilePrimitives.tsx` consolidou a barra local de `season hub`, `match center`, `team profile` e `player profile`. |
| Estados visuais | `COMPLETO` | `EmptyState` e `PartialDataBanner` deixaram de usar styling genérico legado e passaram a seguir a mesma linguagem visual das superfícies principais. |
| Descoberta e listas | `COMPLETO` | `teams list` e `/competitions` foram alinhados à hierarquia já usada por `matches`, `players` e `home executiva`, sem mexer em contratos. |
| Busca global | `COMPLETO` | O overlay passou a compartilhar tags, agrupamento e blocos visuais mais próximos das demais superfícies do app, sem abrir modal paralelo. |
| Auditoria visual | `COMPLETO` | Specs `profile-visual-audit`, `matches-list-visual-audit` e `match-center-rankings-visual-audit` geram artefatos atuais da implementação e referência sem regressão funcional. |

## 6.2. Superfícies globais de sistema

| Item | Status | Evidência curta |
| --- | --- | --- |
| `not-found` global + plataforma | `COMPLETO` | `frontend/src/app/not-found.tsx` e `frontend/src/app/(platform)/not-found.tsx` agora usam a mesma linguagem do produto, com saídas úteis para `home`, `competitions` e `matches`. |
| `loading` global + plataforma | `COMPLETO` | `frontend/src/app/loading.tsx`, `frontend/src/app/(platform)/loading.tsx` e `frontend/src/app/(platform)/(home)/loading.tsx` compartilham a mesma superfície de carregamento na casca. |
| `error` global + plataforma | `COMPLETO` | `frontend/src/app/global-error.tsx`, `frontend/src/app/(platform)/error.tsx`, `frontend/src/app/(platform)/(home)/error.tsx` e `GlobalErrorBoundary.tsx` alinham mensagem, detalhe técnico controlado e saídas seguras. |
| Resolvers curtos | `COMPLETO` | `PlayerRouteResolver.tsx` e `TeamRouteResolver.tsx` deixaram de renderizar fallback cru e passaram a usar `PlatformStateSurface` para loading, redirecionamento e erro. |
| Rotas legadas auxiliares | `COMPLETO` | `/market`, `/head-to-head`, `/coaches/:coachId` e `/audit` saíram de placeholders brutos e passaram a expor superfícies coerentes com o framing da shell. |
| Reentrada legada com query | `COMPLETO` | `/clubs` e `/competition/:id` agora preservam query relevante na transição para a rota canônica ou hub correspondente. |

## 6.3. Hardening técnico e readiness

| Item | Status | Evidência curta |
| --- | --- | --- |
| `lint` do frontend | `COMPLETO` | Migração de `next lint` para ESLint CLI com flat config em `frontend/eslint.config.mjs`; `pnpm lint` agora fecha limpo com `--max-warnings=0`. |
| Boundaries e imports indevidos | `COMPLETO` | A composição de shell/home foi movida para `src/app`, e o consumo de `/matches` no domínio `teams` foi isolado em contrato local, sem relaxar regra de boundaries. |
| Warning de `allowedDevOrigins` | `COMPLETO` | `frontend/next.config.ts` agora explicita `allowedDevOrigins: [\"127.0.0.1\"]`; o warning deixou de reproduzir nos Playwrights do projeto. |
| Readiness da shell global | `COMPLETO` | `GlobalFilterBar` passou a expor `data-url-hydrated=\"true\"` quando a URL está pronta para interação; a regressão de `filtro global -> players` ficou estável no fluxo consolidado. |
| Smoke de release | `COMPLETO` | `pnpm test:smoke` cobre `home`, `competition-season`, `busca`, `teams`, `players`, `match center` e `system surfaces` com uma base enxuta e forte. |
| Regressão E2E consolidada | `COMPLETO` | `pnpm test:regression` roda os 20 cenários E2E relevantes do frontend já convergido, incluindo auditorias visuais e fluxos críticos. |
| Build de produção | `COMPLETO` | `pnpm build` fecha limpo em Next 15.5.12, com geração das rotas principais do app e sem warning novo de config. |
| Entry point operacional de release | `COMPLETO` | `pnpm validate:release` agrega `lint + typecheck + smoke` e fecha limpo no working tree atual. |
| Gate local de release/demo | `COMPLETO` | `tools/frontend_release_gate.py` executa `validate:release + build` e, no modo `full`, inclui `test:regression`, gravando resumo em `artifacts/`. |
| Workflow CI do frontend | `COMPLETO` | `.github/workflows/frontend-release.yml` reproduz o gate completo do frontend em PR, `main` e execução manual, sem abrir pipeline de deploy. |
| Checklist operacional curto | `COMPLETO` | `docs/FRONTEND_RELEASE_READINESS.md` registra comandos, bloqueantes e checklist mínimo de demo/release. |

## 7. Riscos e limites residuais

- `PROVIDER_COVERAGE_GAP` continua existindo para algumas competições/temporadas. No núcleo novo isso aparece como `coverage.status = empty`, não como quebra de fluxo.
- `seasonId` no BFF continua sendo o identificador de query. Em ligas cross-year, a rota canônica usa `seasonLabel` exibível (`2024/2025`), enquanto o filtro enviado ao BFF é `2024`.
- `GET /api/v1/search` continua fora de escopo para `coach`, histórico de consultas, atalhos avançados e entidades secundárias. Isso não bloqueia o núcleo atual de busca.
- A home executiva cobre a entrada real do produto com os domínios já verdes, mas não substitui uma home editorial/institucional nem fecha módulos secundários fora do recorte atual.
- O frontend agora tem gate local e workflow CI de release, mas ainda não existe automação de deploy nem promotion entre ambientes neste escopo.

## 8. Veredito

O **núcleo estrutural competição/temporada está fechado**.

A **busca global está fechada no escopo funcional deste ciclo**: contrato real, ordenação útil, overlay utilizável, navegação segura para `competition/team/player/match` e estados `loading/empty/error/partial` validados por E2E.

A **shell global está convergida no recorte estrutural atual**: header, framing global, filtros, estados de erro/carregamento, rotas legadas relevantes e navegação explícita entre `competitions`, `season hub`, `players`, `teams`, `matches`, `match center`, `rankings` e `busca`.

A **home executiva está fechada no escopo deste ciclo**: rota inicial real, narrativa de entrada, blocos executivos úteis, integração com competições, partidas, times, jogadores e busca global, além de estados `loading/empty/error/partial` validados por E2E.

A **convergência visual sistêmica dos fluxos já verdes está fechada no escopo deste ciclo**: heroes, tabs, states, listas principais e busca global agora compartilham linguagem visual coerente, com auditoria visual e E2E de navegação preservados.

O **fluxo `matches + match center` está fechado no escopo deste ciclo**: lista de partidas, entrada no detalhe, summary, timeline, lineups, team stats, player stats, coverage-state por seção e navegação contextual validada por E2E.

O **domínio `teams + team profile` está fechado no escopo deste ciclo**: lista de times, entrada no perfil canônico, abas reais de overview/squad/matches/stats, coverage-state por seção e navegação contextual validada por E2E.

O **domínio `players + player profile` está fechado no escopo deste ciclo**: lista de jogadores, entrada no perfil canônico, abas reais de overview/history/matches/stats, coverage-state por seção e navegação contextual validada por E2E.

O app inteiro ainda não está fechado do ponto de vista de cobertura total de produto; módulos secundários e convergência visual sistêmica continuam fora deste escopo por decisão explícita.
