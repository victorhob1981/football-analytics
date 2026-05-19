# Frontend Replacement Integration Plan

## 1. Escopo e fonte de verdade

Este documento cobre apenas a substituicao planejada do frontend antigo pelo novo frontend vindo do zip local, com foco em integracao progressiva com backend/BFF.

Escopo explicitamente fora deste documento:

- rebuild de pipeline
- revisao de qualidade do `mart`
- implementacao de codigo

Fontes de verdade usadas:

- frontend antigo em [frontend/package.json](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\package.json) e `frontend/src/**`
- BFF real atual em [api/src/main.py](C:\Users\Vitinho\Desktop\Projetos\football-analytics\api\src\main.py) e `api/src/routers/**`
- arquitetura e plano de frontend em [docs/FRONTEND_ARCHITECTURE.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\FRONTEND_ARCHITECTURE.md), [docs/FRONTEND_DELIVERY_PLAN.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\FRONTEND_DELIVERY_PLAN.md) e [docs/FRONTEND_MANUAL_POSSIBILIDADES.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\FRONTEND_MANUAL_POSSIBILIDADES.md)
- inventario/contratos de dados em [docs/INVENTARIO_DADOS_DO_PROJETO.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\INVENTARIO_DADOS_DO_PROJETO.md), [docs/BFF_API_CONTRACT.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\BFF_API_CONTRACT.md) e [docs/MART_FRONTEND_BFF_CONTRACTS.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\MART_FRONTEND_BFF_CONTRACTS.md)
- material de referencia ja extraido em `reference design/`

Estado objetivo considerado:

- `mart` e quality gates finais ja foram validados e podem ser tratados como base principal de consumo via BFF, com caveats de provider coverage.
- o BFF real atual publica apenas `matches`, `players`, `rankings`, `insights` e `health`.
- `api/src/main.py` nao inclui router de `teams`, e `api/src/routers/players.py` hoje aceita `competitionId` e `seasonId` apenas como filtros opcionais.
- o zip novo nao e um app executavel; e um pacote de prototipos estaticos.

### 1.1 Decisao fechada para contexto de clube/jogador

Problema real:

- `teamId + seasonLabel` nao elimina ambiguidade quando o mesmo clube aparece em mais de uma competicao na mesma temporada.
- `playerId + seasonLabel` tambem nao elimina ambiguidade pelo mesmo motivo.

Decisao arquitetural:

- rotas canonicas de dados para clube e jogador passam a carregar contexto explicito de competicao e temporada:
  - `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId`
  - `/competitions/:competitionKey/seasons/:seasonLabel/players/:playerId`
- `/teams/:teamId` e `/players/:playerId` continuam apenas como rotas resolver/redirect para deep link, busca global e compatibilidade; elas nao sao telas canonicas de dados.
- contratos BFF usados por telas canonicas de clube/jogador passam a exigir contexto de competicao + temporada.
- no curto prazo, o frontend resolve `competitionKey -> competitionId` e `seasonLabel -> seasonId` antes de chamar o BFF atual.
- a integracao de BFF precisa de endpoints leves de contexto:
  - `GET /api/v1/teams/{teamId}/contexts`
  - `GET /api/v1/players/{playerId}/contexts`

Efeito pratico:

- links saindo de season hub, match center, rankings e squad devem apontar direto para a rota canonica contextualizada.
- busca global e links externos entram primeiro na rota curta/resolver e depois redirecionam para o contexto canonico.

## 2. Diagnostico do frontend antigo

### 2.1 Stack e organizacao

Evidencia: [frontend/package.json](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\package.json)

- Next.js 15
- React 19
- TypeScript
- Tailwind CSS
- TanStack Query
- Zustand
- Recharts
- Vitest e Playwright

Estrutura observada:

- `frontend/src/app` com App Router
- `frontend/src/features` com modulos de `matches`, `players`, `rankings`, `insights`
- `frontend/src/shared` com infra, componentes e hooks
- `frontend/src/config` com registries de metricas/rankings

### 2.2 O que existe de fato

Rotas com implementacao real ou semi-real:

- `/` home
- `/matches`
- `/matches/[matchId]`
- `/players`
- `/players/[playerId]`
- `/rankings/[rankingType]`

Rotas placeholder ou desalinhadas com a arquitetura alvo:

- `/competition/[competitionId]`
- `/clubs`
- `/clubs/[clubId]`
- `/head-to-head`
- `/market`
- `/coaches/[coachId]`
- `/audit`

Evidencia objetiva:

- [frontend/src/app/(platform)/layout.tsx](<C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\app(platform)\layout.tsx>) ainda navega para `/competition/placeholder`, `/clubs`, `/head-to-head`, `/market`, `/coaches/placeholder`.
- [frontend/src/app/(platform)/clubs/page.tsx](<C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\app(platform)\clubs\page.tsx>), [frontend/src/app/(platform)/clubs/[clubId]/page.tsx](<C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\app(platform)\clubs[clubId]\page.tsx>), [frontend/src/app/(platform)/competition/[competitionId]/page.tsx](<C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\app(platform)\competition[competitionId]\page.tsx>) e [frontend/src/app/(platform)/head-to-head/page.tsx](<C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\app(platform)\head-to-head\page.tsx>) sao placeholders explicitos.

### 2.3 O que vale reaproveitar do frontend antigo

Infra/BFF:

- [frontend/src/shared/services/api-client.ts](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\services\api-client.ts)
- [frontend/src/shared/services/query-client.ts](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\services\query-client.ts)
- [frontend/src/app/(platform)/providers.tsx](<C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\app(platform)\providers.tsx>)

Cobertura e estados:

- [frontend/src/shared/hooks/useQueryWithCoverage.ts](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\hooks\useQueryWithCoverage.ts)
- [frontend/src/shared/components/coverage/CoverageBadge.tsx](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\components\coverage\CoverageBadge.tsx)
- [frontend/src/shared/components/coverage/PartialDataBanner.tsx](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\components\coverage\PartialDataBanner.tsx)
- [frontend/src/shared/components/feedback/EmptyState.tsx](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\components\feedback\EmptyState.tsx)
- [frontend/src/shared/components/feedback/LoadingSkeleton.tsx](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\components\feedback\LoadingSkeleton.tsx)

Filtros globais e sincronizacao de contexto:

- [frontend/src/shared/components/filters/GlobalFilterBar.tsx](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\shared\components\filters\GlobalFilterBar.tsx)
- `frontend/src/shared/stores/globalFilters.store.ts`
- `frontend/src/shared/hooks/useGlobalFilters.ts`

Fetchers/hook de features ja conectados ao BFF atual:

- [frontend/src/features/matches/services/matches.service.ts](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\features\matches\services\matches.service.ts)
- [frontend/src/features/players/services/players.service.ts](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\features\players\services\players.service.ts)
- [frontend/src/features/rankings/services/rankings.service.ts](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\features\rankings\services\rankings.service.ts)

Componentes com valor de integracao:

- `frontend/src/shared/components/charts/**`
- `frontend/src/shared/components/data-display/**`
- `frontend/src/shared/components/comparison/**`

Conclusao:

- o frontend antigo tem valor alto como base tecnica de integracao
- o frontend antigo tem valor baixo como referencia visual final

### 2.4 O que deve ser descartado ou despriorizado no frontend antigo

Descartar do alvo final:

- shell visual atual e tema atual em [frontend/src/app/globals.css](C:\Users\Vitinho\Desktop\Projetos\football-analytics\frontend\src\app\globals.css)
- navegacao com rotas desalinhadas do plano (`/competition`, `/clubs`, `/head-to-head`)
- placeholders de rotas ainda sem produto real

Despriorizar:

- qualquer UX que assuma `insights` prontos, porque o endpoint atual ainda devolve vazio em [api/src/routers/insights.py](C:\Users\Vitinho\Desktop\Projetos\football-analytics\api\src\routers\insights.py)
- qualquer ranking baseado em `player-pass-accuracy`, porque o BFF atual marca essa metrica como `unsupported` em [api/src/routers/rankings.py](C:\Users\Vitinho\Desktop\Projetos\football-analytics\api\src\routers\rankings.py)

## 3. Diagnostico do novo frontend do zip

### 3.1 O que o zip realmente contem

Inspecao objetiva do arquivo `C:\Users\Vitinho\Desktop\stitch_home_football_analytics (1).zip`:

- `16` arquivos `code.html`
- `16` arquivos `screen.png`
- `1` arquivo `DESIGN.md`
- `0` `package.json`
- `0` configuracao de build
- `0` estrutura de app React/Next

Uso tecnico observado nos HTMLs:

- Tailwind via CDN em todos os HTMLs
- fontes externas via CDN
- muitas imagens remotas
- muitos links `href="#"` placeholder
- markup repetido entre telas, sem componentes compartilhados reais

Conclusao objetiva:

- o zip nao e um frontend pronto para entrar e rodar no projeto
- o zip e um pacote de prototipos visuais de alta fidelidade

### 3.2 O que ja esta bom no novo frontend

Bom para ser trazido como referencia de produto/design:

- direcao visual e sistema de estilo descritos em `emerald_pitch/DESIGN.md`
- shell premium/editorial mais aderente ao posicionamento do produto
- composicao de paginas e hierarquia visual para:
  - home executiva
  - season hub standings/calendar/rankings
  - match center summary
  - player profile
  - club profile
  - h2h
  - market
  - coaches
  - availability
  - global search overlay

### 3.3 O que ainda nao esta pronto no novo frontend

Adaptacao obrigatoria antes de integracao real:

- converter HTML estatico em componentes/rotas do app real
- substituir dependencias CDN por assets e setup locais
- transformar markup repetido em layout/componentes compartilhados
- alinhar nomenclatura de rotas com a arquitetura do projeto
- conectar tudo a contratos BFF reais
- suportar `meta.coverage`, loading, empty, partial, unsupported

## 4. Aderencia do novo frontend ao planejamento do projeto

### 4.1 Alinhamento forte

O zip esta alinhado ao plano em nivel de produto:

- season hub com standings, calendar e rankings
- perfis de clube e jogador
- match center
- h2h
- modulos secundarios (`market`, `coaches`, `availability`)
- home executiva
- busca global

Isso converge com [docs/FRONTEND_ARCHITECTURE.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\FRONTEND_ARCHITECTURE.md) e [docs/FRONTEND_DELIVERY_PLAN.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\FRONTEND_DELIVERY_PLAN.md).

### 4.2 Lacunas e conflitos

Conflitos do zip com a arquitetura/documentacao real:

- nao ha projeto executavel; logo nao existe aderencia tecnica, so aderencia visual
- nao ha evidencia de camada de fetch/BFF
- nao ha estados de coverage implementados de forma sistemica
- nao ha competicoes/lista de competicoes explicitamente como tela executavel
- match center no zip aparece como `summary`; nao ha evidencias equivalentes de timeline, lineups e player stats como modulos prontos
- a documentacao antiga ainda tem trechos que tratam `raw` como base mais forte; para consumo atual, o documento mais novo a prevalecer e [docs/MART_FRONTEND_BFF_CONTRACTS.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\MART_FRONTEND_BFF_CONTRACTS.md)

Conflitos do frontend antigo com a arquitetura/documentacao:

- arquitetura alvo documentada usa `/competitions`, `/teams`, `/h2h`, `/more/*`
- implementacao antiga atual usa `/competition`, `/clubs`, `/head-to-head`, `/market`, `/coaches/[coachId]`

## 5. Classificacao objetiva

### 5.1 Reaproveitavel do frontend antigo

`ALTO VALOR`

- infra de app em Next.js ja existente
- providers e query client
- cliente HTTP/BFF
- hooks e componentes de coverage
- filtros globais e sincronizacao de query params
- fetchers e tipagens de `matches`, `players`, `rankings`
- testes de hooks/stores como padrao de validacao

`VALOR MEDIO`

- tabelas, cards, componentes de grafico e feedback
- parte da modelagem de tipos

`VALOR BAIXO`

- layout visual atual
- navegacao atual
- placeholders

### 5.2 Obsoleto do frontend antigo

- mapa de rotas desalinhado do produto final
- paginas placeholder que nao agregam contrato nem UX
- camada visual atual, que nao entrega a proposta do novo front
- qualquer promessa de `insights` ou ranking `player-pass-accuracy` como feature pronta

### 5.3 O que vem pronto do novo frontend

`PRONTO COMO REFERENCIA VISUAL`

- direcao estetica
- composicao visual de telas-chave
- hierarquia de informacao
- linguagem de cards, listas, blocos, navegação e destaque

`NAO PRONTO COMO CODIGO DE PRODUTO`

- routing
- providers
- fetch layer
- estado global
- coverage-state
- loading/error/empty
- contratos de dados

### 5.4 O que o novo frontend precisa adaptar

- normalizar a arvore de rotas para a arquitetura do projeto
- virar app/componentes reais dentro do repo
- integrar com os contratos BFF existentes e futuros
- tratar gaps de provider como estado de produto
- localização de assets e eliminação de dependencias remotas
- separar blocos ja integraveis de blocos ainda mock

## 6. Integracao com backend/BFF: o que entra primeiro

### 6.1 Ja integravel com o BFF atual

Evidencia: [api/src/main.py](C:\Users\Vitinho\Desktop\Projetos\football-analytics\api\src\main.py) e `api/src/routers/**`

Paginas/modulos que ja podem ser integrados sem criar novos routers:

- lista de partidas
- detalhe de partida
- timeline de eventos no detalhe da partida
- lineups no detalhe da partida
- player stats no detalhe da partida
- lista de jogadores
- perfil de jogador
- rankings suportados hoje

Observacoes:

- match center ja tem `includeTimeline`, `includeLineups` e `includePlayerStats` em [api/src/routers/matches.py](C:\Users\Vitinho\Desktop\Projetos\football-analytics\api\src\routers\matches.py)
- player profile e players list ja existem em [api/src/routers/players.py](C:\Users\Vitinho\Desktop\Projetos\football-analytics\api\src\routers\players.py)
- rankings suportados ja existem em [api/src/routers/rankings.py](C:\Users\Vitinho\Desktop\Projetos\football-analytics\api\src\routers\rankings.py)

### 6.2 Dependem de contratos/BFF adicionais

- lista de competicoes
- competition hub
- season hub completo
- standings como tela/painel canonicamente integrado
- team profile
- resolver de contexto para team/player
- h2h rico
- coaches
- availability
- market
- home executiva real

Motivo:

- essas telas aparecem no plano e/ou no zip, mas nao existem como routers reais hoje no `api/`
- parte delas ja esta coberta pelo `mart`, mas ainda exige composicao BFF

### 6.3 Podem ficar com mock/fallback temporario

- home executiva
- busca global
- market
- coaches
- availability

Regra:

- mock temporario so onde nao existe contrato real ainda
- nao simular completude de cobertura
- exibir estado explicito de `indisponivel`, `partial`, `coverage do provider` ou `unsupported`

### 6.4 Como isso conversa com `mart` e `raw`

- o frontend novo deve ser integrado via BFF
- o BFF deve usar `mart` como camada principal de consumo
- `raw` continua apenas como apoio pontual onde o contrato atual ainda depende dele
- o frontend nao deve acoplar diretamente em `raw`

## 7. Plano de substituicao e integracao

### Etapa 0. Trazer o zip para dentro do repo sem ativar como app

Objetivo:

- preservar o pacote novo como referencia auditavel
- evitar migracao baguncada

Acao recomendada:

- manter `reference design/` como pasta oficial de referencia interna
- manter isso fora da arvore executavel do app
- registrar um indice tela -> rota alvo -> status de integracao em `docs/FRONTEND_REFERENCE_INVENTORY.md`

Decisao:

- `SIM`, vale trazer o novo frontend para dentro do repo
- `NAO`, nao vale tentar rodar o zip como app do projeto

### Etapa 1. Manter um unico app executavel e substituir por dentro

Recomendacao:

- manter `frontend/` como app executavel unico durante a migracao
- nao abrir um segundo app de producao em paralelo sem necessidade

Justificativa:

- o frontend antigo ja tem a infra de BFF, coverage e filtros
- o zip novo nao traz infra reaproveitavel
- duplicar app agora criaria retrabalho e divergencia

Convivencia old + new:

- sim, por um periodo
- mas a convivencia deve ser por referencia visual + substituicao de shell/paginas dentro do mesmo app
- nao por dois frontends ativos com stacks paralelos

### Etapa 2. Canonicalizar o mapa de rotas

Objetivo:

- alinhar implementacao com [docs/FRONTEND_ARCHITECTURE.md](C:\Users\Vitinho\Desktop\Projetos\football-analytics\docs\FRONTEND_ARCHITECTURE.md)

Mapa alvo:

- `/home`
- `/competitions`
- `/competitions/:competitionKey`
- `/competitions/:competitionKey/seasons/:seasonLabel`
- `/matches/:fixtureId`
- `/teams/:teamId` (resolver-only)
- `/players/:playerId` (resolver-only)
- `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId`
- `/competitions/:competitionKey/seasons/:seasonLabel/players/:playerId`
- `/h2h`
- `/more/market`
- `/more/coaches`
- `/more/availability`

Impacto:

- `/competition`, `/clubs`, `/head-to-head` devem ser tratados como legado e removidos gradualmente da navegacao
- links internos para clube/jogador precisam nascer com contexto de competicao e temporada, nao apenas com id da entidade

### Etapa 3. Transplantar o design system e o shell do novo front

Objetivo:

- substituir primeiro a base visual sem quebrar dados ja conectados

Entradas:

- `emerald_pitch/DESIGN.md`
- HTMLs do zip como referencia de composicao

Saidas esperadas:

- tokens visuais
- tipografia
- shell principal
- header/sidebar/nav
- componentes-base de card/lista/tabela/badge

Regra:

- preservar a infra de fetch, query, coverage e filtros do frontend antigo

### Etapa 4. Conectar primeiro o core que ja tem backend hoje

Primeiro bloco de integracao real:

- lista de partidas
- match center
- lista de jogadores
- perfil de jogador
- rankings suportados

Motivo:

- ja existe BFF real
- ja existem services/hooks no frontend antigo
- sao blocos suficientes para validar o shell novo com dados reais

Status atual do bloco visual controlado:

- `2026-03-21`: `TeamProfile` e `PlayerProfile` iniciaram a substituicao visual localizada dentro de `frontend/`, reaproveitando contratos canonicos e coverage-state ja estabilizados
- `2026-03-21`: uma auditoria visual com screenshots reais e comparativos lado a lado refinou as duas telas para aproximar a composicao do Stitch sem abrir shell global nem reabrir contratos BFF
- `2026-03-21`: a lista de jogadores recebeu a proxima wave visual localizada, combinando a linguagem de descoberta do `busca_global_overlay_bloco_7_pt_br` com a estrutura editorial/tabular de `perfil_do_clube_elenco_bloco_10_pt_br`, sem reabrir `GET /api/v1/players`
- `2026-03-21`: a lista de partidas entrou na mesma wave visual localizada, usando `season_hub_calendar_bloco_2` como referencia principal e preservando o link para `Match center` com `competitionId` e `seasonId` quando o contexto esta disponivel
- `2026-03-21`: o `Match center` passou a usar a nova linguagem visual apenas no recorte `summary/header`, tomando `match_center_summary_bloco_2` como referencia e mantendo timeline, escalacoes e stats profundas fora desta wave
- `2026-03-21`: `RankingTable` virou o template visual localizado para todas as rotas `/rankings/[rankingType]`, com referencia primaria em `season_hub_rankings_bloco_5_pt_br` e sem reabrir contratos do BFF
- `2026-03-21`: `timeline`, `lineups` e `player stats` do `Match center` deixaram de ser placeholders crus e passaram a usar a mesma casca local do recorte ja migrado, ainda sobre o payload atual e sem abrir contrato novo
- a migracao ficou encapsulada em primitives de perfil e em um variant local de tabela; o shell global do app continua fora deste bloco
- `match center`, `home`, `standings`, `calendar` e modulos secundarios continuam fora do escopo desta wave

### Etapa 5. Integrar os hubs que exigem BFF adicional

Segundo bloco:

- lista de competicoes
- competition hub
- season hub com standings/calendar/rankings
- team profile

Dependencias:

- novos contratos BFF baseados no `mart`
- normalizacao de season/competition/team

### Etapa 6. Fechar modulos caveat-driven e secundarios

Terceiro bloco:

- h2h
- coaches
- availability
- market
- home executiva
- busca global

Regra:

- so promover para producao quando houver contrato real ou fallback explicitamente aprovado

### Etapa 7. Retirada controlada do legado

Retirar o frontend antigo por partes, nunca em big bang:

- remover rotas placeholder primeiro
- remover layout antigo depois que o shell novo estiver estabilizado
- remover componentes legados sem uso apenas apos paridade de tela e validacao

Criterio de retirada:

- rota nova integrada
- coverage-state tratado
- loading/error/empty tratados
- navegação e links internos revalidados

## 8. Ordem recomendada de execucao

1. Importar o zip para pasta de referencia dentro do repo.
2. Fechar a decisao canonica de contexto para clube/jogador.
3. Fechar um inventario tela-do-zip -> rota-alvo -> contrato BFF -> reaproveitamento.
4. Congelar o mapa de rotas canonico.
5. Substituir shell e design system dentro de `frontend/`.
6. Revestir `matches`, `match center`, `players`, `player profile`, `rankings` com a nova linguagem visual.
7. Abrir bloco de BFF para `competitions`, `season hub`, `teams`, `team/player contexts`, `standings`.
8. Integrar os modulos secundarios por ultimo.
9. Retirar rotas legadas e placeholders quando houver paridade.

## 9. Primeiro passo pratico recomendado

Primeiro passo seguro:

- tratar `reference design/` como referencia interna organizada, nao como app
- fechar a decisao de contexto canonico para clube/jogador
- criar um inventario objetivo `tela do zip -> rota canonica -> status de dados/BFF -> reaproveitamento do frontend antigo` em `docs/FRONTEND_REFERENCE_INVENTORY.md`

Por que esse e o primeiro passo certo:

- nao quebra o app atual
- nao descarta nada util do frontend antigo
- evita tratar prototipo HTML como codigo pronto
- remove a ambiguidade real de clube/jogador antes de abrir migracao visual
- prepara a substituicao por blocos sem retrabalho

## 10. Decisao executiva

Recomendacao objetiva:

- nao substituir o frontend antigo por um segundo app novo agora
- usar o frontend antigo como base tecnica
- usar o zip novo como base visual/produtiva
- migrar dentro do `frontend/` existente, por blocos, com mapa de rotas canonico e contratos BFF explicitamente priorizados
- tratar clube e jogador como telas contextualizadas por `competitionKey + seasonLabel + entityId`
- manter `/teams/:teamId` e `/players/:playerId` apenas como resolvers de contexto

Essa abordagem minimiza risco, preserva o que ja funciona e evita uma migracao big bang sem ganho tecnico real.
