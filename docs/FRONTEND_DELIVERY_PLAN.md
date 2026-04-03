# football-analytics — Frontend Delivery Plan

> Refinamento e expansão da arquitetura de frontend.  
> Data de referência: `2026-03-20`  
> Estado: planejamento estrutural e plano de entrega — sem código, sem wireframe.

---

## 1. Visão geral

Este documento transforma a arquitetura de frontend já definida em um plano de entrega prático. Ele cobre a estrutura de produto, o layout base do app, o mapa de rotas, os dados mínimos por tela, as dependências de BFF, os estados de cobertura e a ordem de implementação por blocos.

O ponto de partida são três decisões arquiteturais já tomadas e mantidas aqui:

1. O frontend é multi-competição e multi-temporada por natureza — filtros globais de contexto não são opcionais.
2. Coverage-state é um requisito funcional de primeiro nível, não tratamento de erro.
3. Standings, Calendar e Rankings vivem como abas dentro do Season hub, não como telas independentes.

A principal adição neste documento é a **busca global**, que quebra a dependência do fluxo hierárquico de navegação e permite acesso transversal a qualquer entidade do sistema.

---

## 2. Princípios arquiteturais

**Multi-contexto desde o início.** Toda tela e módulo é projetado para funcionar com qualquer combinação de competição e temporada do portfolio. Nunca assumir mono-competição.

**Entidades são cidadãs de primeira classe.** Clube, jogador, partida e competição têm URLs próprias e podem ser acessados diretamente, não apenas via fluxo descendente.

**Cobertura é informação, não falha.** Cada módulo declara seu próprio estado de cobertura. Uma tela nunca entra em estado de erro total por causa de um módulo parcial.

**BFF como camada anti-corrupção.** O frontend nunca lida com heterogeneidade de origem de dado. A BFF normaliza, compõe e entrega metadados de cobertura junto com cada resposta.

**Incrementalidade sem retrabalho.** A arquitetura é projetada para que fases posteriores adicionem profundidade sem exigir refatoração das telas já construídas.

---

## 3. Busca global

### Papel no produto

A busca global é o mecanismo de navegação transversal do app. Ela existe porque o fluxo hierárquico — competição → temporada → rodada → partida — é o caminho natural para descoberta, mas não é o mais eficiente quando o usuário já sabe o que quer. Sem busca, um usuário que quer ir direto ao perfil de um jogador específico precisa saber em qual competição ele atua, em qual temporada e navegar manualmente até encontrá-lo.

A busca global resolve isso. Ela é um atalho universal, um mecanismo de discovery e um ponto de entrada alternativo para qualquer entidade do sistema.

### Onde ela vive

A busca vive na shell global, sempre visível no header, independente de qual tela o usuário estiver. Ela não é uma tela — é um componente persistente que se expande em overlay sobre o conteúdo atual quando ativado.

O comportamento é o seguinte: o usuário clica no ícone/campo de busca no header → um overlay de busca se abre sobre o conteúdo → o usuário digita → resultados aparecem em tempo real categorizados → o usuário clica em um resultado e é levado à tela correspondente → o overlay fecha.

### O que ela deve permitir encontrar

| Categoria | Exemplos | Destino ao clicar |
|---|---|---|
| Competição | "Premier League", "Brasileirão" | Competition hub |
| Clube | "Flamengo", "Barcelona", "Bayern" | Club profile |
| Jogador | "Vini Jr", "Mbappé", "Gabi" | Player profile |
| Partida | "Flamengo x Fluminense", "El Clásico 2024" | Match center |
| Técnico | "Ancelotti", "Tite" | Coach profile (fase 3) |

### Como ela se relaciona com o contexto global

A busca opera de forma **independente do contexto de competição/temporada selecionado nos filtros globais**. Ela busca no portfolio inteiro. Isso é intencional — o usuário pode estar navegando na Premier League e querer buscar um jogador do Brasileirão sem ter que trocar o filtro global primeiro.

Quando o usuário acessa uma entidade via busca, a tela de destino carrega com o contexto padrão daquela entidade (temporada mais recente, competição mais relevante), não com o contexto do filtro global ativo.

### Comportamento dos resultados

Os resultados são exibidos em grupos por categoria, com no máximo 3–4 itens por grupo antes de um "ver mais". A ordem de prioridade dos grupos é: Partidas recentes > Jogadores > Clubes > Competições > Técnicos.

Para partidas, o resultado mostra os dois times, a competição e a data — contexto suficiente para o usuário identificar sem abrir.

Para jogadores e clubes, o resultado mostra o nome, o clube atual (ou a competição no caso de clubes) e a temporada de referência.

### O que a busca não faz (na fase 1)

- Não faz busca semântica ou por estatística ("jogador com mais gols no Brasileirão 2024") — isso é funcionalidade de ranking, não de busca.
- Não filtra por cobertura — pode retornar entidades com dados parciais; a tela de destino é responsável por comunicar isso.
- Não substitui os filtros globais — é um atalho de navegação, não um sistema de filtros avançado.

---

## 4. Layout base do app

```
┌─────────────────────────────────────────────────────────┐
│  HEADER                                                  │
│  logo | nav principal | [busca global] | settings        │
├──────────────┬──────────────────────────────────────────┤
│              │  FILTROS GLOBAIS (contextual)             │
│              │  competição | temporada | fase/rodada     │
│  NAV         ├──────────────────────────────────────────┤
│  LATERAL     │  BREADCRUMB                              │
│  (ou top)    │  Home > Premier League > 2024/25 > ...   │
│              ├──────────────────────────────────────────┤
│  - Home      │                                          │
│  - Competit. │  ÁREA DE CONTEÚDO                        │
│  - H2H       │                                          │
│  - More      │  módulos independentes por tela          │
│              │  cada módulo com seu coverage-state      │
│              │                                          │
└──────────────┴──────────────────────────────────────────┘
```

### Header

Presente em todas as telas. Contém: logo (link para Home), navegação principal (links de nível 1), campo de busca global e acesso a configurações/preferências.

### Navegação principal

Itens fixos no header ou nav lateral (a decidir na fase de design):

- Home
- Competições (abre dropdown ou leva para /competitions)
- Head-to-head
- More (agrupa Market, Coaches, Availability na fase 3)

Clubes e jogadores não ficam na navegação principal — são acessados via busca, via links dentro de telas ou via fluxo de competição.

### Filtros globais

Aparecem abaixo do header quando o contexto é relevante — ou seja, em qualquer tela que seja sensível a competição/temporada. Na Home os filtros globais ficam ocultos (a Home mostra o portfolio inteiro). No Match center o filtro global fica travado no contexto da partida (não faz sentido trocar a temporada dentro de um jogo específico).

Componentes do filtro global: seletor de competição + seletor de temporada + seletor de fase/rodada (quando aplicável) + janela temporal (quando aplicável).

### Breadcrumb

Sempre visível quando o usuário está em contexto específico. Permite navegar para qualquer nível superior da hierarquia com um clique. Exemplos:

```
Home > Premier League > 2024/25 > Standings
Home > Premier League > 2024/25 > Arsenal vs Chelsea > Escalações
Home > Arsenal > 2024/25
```

### Área de conteúdo

Ocupa o espaço restante. Cada tela renderiza seus módulos de forma independente. Módulos com loading individual, erro individual e coverage-state individual — nunca um spinner ou erro cobrindo a tela inteira por causa de um módulo secundário.

### Pontos fixos de coverage-state

- Badge discreto no card de cada competição na Home
- Banner no topo da área de conteúdo quando a cobertura da tela inteira está comprometida
- Badge inline por módulo/aba quando apenas aquele módulo tem cobertura parcial
- Tooltip com razão (`PROVIDER_COVERAGE_GAP`, `PIPELINE_LIMIT`, etc.) ao hover do badge

---

## 5. Árvore de rotas

```
/
├── /home
│
├── /competitions
│   └── /:competitionKey
│       └── /seasons/:seasonLabel
│           ├── /standings                    [aba]
│           ├── /calendar                     [aba]
│           └── /rankings                     [aba]
│
├── /matches/:fixtureId
│   ├── /summary                              [aba padrão]
│   ├── /timeline                             [aba]
│   ├── /lineups                              [aba]
│   ├── /team-stats                           [aba]
│   └── /player-stats                         [aba]
│
├── /teams/:teamId                            [resolver/redirect]
│
├── /competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId
│   ├── /overview                             [aba padrão]
│   ├── /matches                              [aba]
│   ├── /squad                                [aba]
│   └── /stats                                [aba]
│
├── /players/:playerId                        [resolver/redirect]
│
├── /competitions/:competitionKey/seasons/:seasonLabel/players/:playerId
│   ├── /overview                             [aba padrão]
│   ├── /matches                              [aba]
│   └── /history                              [aba]
│
├── /h2h
│   └── ?teamA=:id&teamB=:id                  [query params]
│
└── /more
    ├── /market
    ├── /coaches
    │   └── /:coachId                         [fase 3]
    └── /availability
```

Regra de contexto:

- clube e jogador so sao telas de dados quando a rota ja carrega `competitionKey + seasonLabel + entityId`.
- `/teams/:teamId` e `/players/:playerId` existem para busca global, link externo e compatibilidade, resolvendo depois para a rota canonica.

### Pontos de entrada laterais (não hierárquicos)

Esses são os caminhos que o usuário pode tomar sem seguir o fluxo hierárquico principal:

| Atalho | Como chega lá |
|---|---|
| Busca global → qualquer entidade | Disponível em toda tela via header |
| Link de time em qualquer tela | Match center, Standings, Rankings, H2H |
| Link de jogador em qualquer tela | Match center, Rankings, Club profile |
| Link de partida em qualquer tela | Calendar, Club profile, Player profile, H2H |
| Link H2H no cabeçalho do Match center | Match center |
| Filtro global de competição/temporada | Shell — qualquer tela |

Nos links internos, clube e jogador devem apontar direto para a rota canonica contextualizada. So busca global e links externos passam antes pelo resolver curto.

---

## 6. Mapa de telas e módulos

### Home — `/home`

Tela de orientação e lançamento. Sem filtros globais de contexto (mostra o portfolio inteiro).

**Módulos**:
- Grid de cards de competição (cobertura + temporada atual + link)
- Faixa de destaques do momento (líderes de métricas por competição)
- Atalhos para jogos mais recentes

**Evolução fase 3**: dashboard executivo com comparativos cross-competition e feed de insights.

---

### Competition hub — `/competitions/:competitionKey`

Hub de distribuição de uma competição. Sem abas — distribui para o Season hub.

**Módulos**:
- Cabeçalho (nome, temporadas disponíveis)
- Seletor de temporada → leva ao Season hub
- Cards de resumo da temporada ativa
- Links rápidos para standings, calendário, rankings

---

### Season hub — `/competitions/:competitionKey/seasons/:seasonLabel`

Contexto canônico de navegação. Três abas independentes.

**Aba Standings**: tabela oficial + seletor de rodada + sparklines de posição + comparador de times.

**Aba Calendar**: lista de partidas por rodada + filtros de status e time + link para Match center + badge de cobertura por rodada.

**Aba Rankings**: ranking de jogadores por métrica + ranking de times por estatística + seletor de janela temporal + coverage-state por ranking.

---

### Match center — `/matches/:fixtureId`

Tela mais densa do produto. Cabeçalho fixo + cinco abas independentes.

**Cabeçalho fixo**: times, placar, status, mando, data, estádio, links para club profiles, cobertura geral.

**Aba Summary** (padrão): gols com autores e minuto, stats principais lado a lado, eventos principais resumidos.

**Aba Timeline**: eventos minuto a minuto, filtro por tipo, coverage-state quando parcial.

**Aba Lineups**: titulares e banco espelhados, formação quando disponível, slots sem ID explicitados, coverage-state independente por time.

**Aba Team stats**: comparativo de ataque / passe / defesa / disciplina em barras lado a lado.

**Aba Player stats**: tabela individual ordenável, links para player profiles, coverage-state quando fixture_player_statistics parcial.

---

### Club profile — `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId`

Perfil de um clube em uma temporada específica. Quatro abas.

`/teams/:teamId` nao e tela final; e apenas a rota que resolve o contexto e redireciona para esta URL canonica.

**Aba Overview** (padrão): forma recente, posição na tabela, comparativo casa/fora, próximo e último jogo.

**Aba Matches**: histórico de partidas com resultado e links, filtros por mando e resultado.

**Aba Squad**: jogadores por minutos jogados, links para player profiles, badge de indisponibilidade.

**Aba Stats**: trend mensal de gols, métricas agregadas, comparativo entre temporadas (quando mart disponível).

---

### Player profile — `/competitions/:competitionKey/seasons/:seasonLabel/players/:playerId`

Perfil de um jogador em uma temporada. Três abas.

`/players/:playerId` nao e tela final; e apenas a rota que resolve o contexto e redireciona para esta URL canonica.

**Aba Overview** (padrão): stats agregadas (gols, assistências, minutos, rating), clube, posição, destaques recentes.

**Aba Matches**: histórico de partidas com stats individuais, filtro por competição e temporada, links para match center.

**Aba History**: comparativo entre temporadas, evolução de métricas, coverage-state quando gaps em player_season_statistics.

---

### Head-to-head — `/h2h?teamA=:id&teamB=:id`

Tela de confronto direto entre dois times. Sem abas — conteúdo vertical.

**Módulos**: saldo histórico (V/E/D), últimos N confrontos com links, tendência recente, filtro por competição e janela temporal.

---

### Módulos secundários — `/more/*`

**Market**: feed de transferências, filtros por clube/período/tipo. Fase 3.

**Coaches**: timeline de passagens por clubes, desempenho agregado. Fase 3.

**Availability**: painel de indisponíveis por clube, filtros por categoria. Fase 3 como tela; fase 2 como seção no Club profile / aba Squad.

---

## 7. Dados mínimos por tela

### Home

| Dado | Tipo | Observação |
|---|---|---|
| Lista de competições + temporada ativa | Núcleo obrigatório | Sem isso a tela não existe |
| Status de cobertura por competição | Núcleo obrigatório | Define o que mostrar em cada card |
| Standings snapshot (líder atual) | Enriquecimento | Um item por competição |
| Fixtures recentes / próximos | Enriquecimento | Atalhos de navegação |
| Destaques de métricas (artilheiro, etc.) | Enriquecimento fase 3 | Depende de mart portfolio-wide |

---

### Competition hub

| Dado | Tipo | Observação |
|---|---|---|
| Metadados da competição | Núcleo obrigatório | Nome, logo, temporadas |
| Lista de temporadas disponíveis | Núcleo obrigatório | Para o seletor de temporada |
| Resumo da temporada ativa | Enriquecimento | Cards de contexto |

---

### Season hub

| Dado | Tipo | Observação |
|---|---|---|
| Metadados da competição/temporada | Núcleo obrigatório | Contexto de toda a tela |
| Standings snapshots | Núcleo obrigatório (aba Standings) | Depende de round metadata |
| Fixtures com status e placar | Núcleo obrigatório (aba Calendar) | Base do calendário |
| Player / team aggregates por métrica | Núcleo obrigatório (aba Rankings) | |
| Round metadata (ordenação e label) | Enriquecimento | Melhora navegação por rodada |
| Sparklines de posição por time | Enriquecimento | Depende de série histórica de standings |
| Janela temporal em rankings | Enriquecimento | Últimos N jogos vs temporada completa |

---

### Match center

| Dado | Tipo | Observação |
|---|---|---|
| Fixture base (times, placar, status, data) | Núcleo obrigatório | Sem isso a tela não existe |
| Match events | Núcleo obrigatório (aba Timeline) | Coverage-state se parcial |
| Fixture lineups | Núcleo obrigatório (aba Lineups) | Coverage-state se parcial; slots sem ID explicitados |
| Match statistics (team-level) | Núcleo obrigatório (aba Team stats) | Coverage-state se parcial |
| Fixture player statistics | Núcleo obrigatório (aba Player stats) | Coverage-state se parcial |
| Metadados de estádio, árbitro, público | Enriquecimento | Tratado como "não informado" se ausente |
| Head-to-head summary | Enriquecimento | Bloco opcional no cabeçalho |

---

### Club profile

| Dado | Tipo | Observação |
|---|---|---|
| Contexto canonico (`competitionKey`, `seasonLabel`, `teamId`) | Núcleo obrigatório | Sem isso a tela fica semanticamente ambigua |
| Metadados do clube | Núcleo obrigatório | Nome, escudo, temporada |
| Fixtures da temporada | Núcleo obrigatório | Base de partidas e forma recente |
| Standings snapshot atual | Núcleo obrigatório | Posição e pontos |
| Lineup players da temporada | Núcleo obrigatório (aba Squad) | Coverage-state se parcial |
| Stats agregadas da temporada | Enriquecimento (aba Stats) | Depende de mart de resumo |
| Comparativo entre temporadas | Enriquecimento fase 2/3 | Depende de mart multi-season |
| Sidelined / availability | Enriquecimento | Seção na aba Squad |

---

### Player profile

| Dado | Tipo | Observação |
|---|---|---|
| Contexto canonico (`competitionKey`, `seasonLabel`, `playerId`) | Núcleo obrigatório | Sem isso a tela fica semanticamente ambigua |
| Metadados do jogador | Núcleo obrigatório | Nome, posição, clube |
| Player season statistics | Núcleo obrigatório | Coverage-state se gaps |
| Fixtures com participação do jogador | Núcleo obrigatório (aba Matches) | |
| Fixture player statistics por jogo | Enriquecimento (aba Matches) | Stats individuais por partida |
| Histórico multi-temporada | Enriquecimento (aba History) | Depende de player_season_statistics completo |

---

### Head-to-head

| Dado | Tipo | Observação |
|---|---|---|
| Par de times identificados | Núcleo obrigatório | Sem isso a tela não existe |
| H2H fixtures históricos | Núcleo obrigatório | Cobertura sólida no raw |
| Saldo V/E/D agregado | Núcleo obrigatório | |
| Filtro por competição | Enriquecimento | Segmenta o histórico |
| Filtro por janela temporal | Enriquecimento | Últimos N jogos |

---

## 8. Dependências de BFF por tela

### Princípios gerais de BFF

Todos os endpoints devolvem o contrato base:

```
{
  data: { ... },
  meta: {
    scope: { competitionKey, seasonLabel, stageId, roundId },
    coverage: { status, reasonCode, coveragePct },
    source: { primaryLayer, fallbackLayer, generatedAt }
  }
}
```

O frontend nunca precisa saber se o dado veio de `mart` ou `raw`. Isso é responsabilidade da BFF.

Regra adicional para clube/jogador:

- a URL canonica do frontend usa `competitionKey` e `seasonLabel`.
- o BFF atual ainda opera majoritariamente com `competitionId` e `seasonId`.
- enquanto os aliases por chave/label nao existirem no BFF, o loader da rota resolve `competitionKey -> competitionId` e `seasonLabel -> seasonId` antes da chamada.

---

### Home

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/competitions` | raw.competition_leagues | Disponível |
| `GET /v1/competitions/:key/seasons` | raw.competition_seasons | Disponível |
| `GET /v1/standings?competitionKey&seasonLabel&limit=1` | raw.standings_snapshots | Disponível |
| `GET /v1/matches?competitionKey&seasonLabel&limit=3&status=recent` | raw.fixtures | Disponível |
| Agregados cross-competition para destaques | mart portfolio-wide | Em consolidação (fase 3) |

---

### Competition hub

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/competitions/:key` | raw.competition_leagues | Disponível |
| `GET /v1/competitions/:key/seasons` | raw.competition_seasons | Disponível |
| `GET /v1/competitions/:key/seasons/:season/summary` | mart + raw | Em consolidação |

---

### Season hub

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/standings?competitionKey&seasonLabel&roundId` | raw.standings_snapshots | Disponível |
| `GET /v1/matches?competitionKey&seasonLabel&roundId` | raw.fixtures | Disponível |
| `GET /v1/rankings?competitionKey&seasonLabel&metric&window` | raw + mart | Disponível com caveat |
| `GET /v1/competitions/:key/seasons/:season/rounds` | raw.competition_rounds | Disponível com caveat |

---

### Match center

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/matches/:fixtureId` | raw.fixtures | Disponível |
| `GET /v1/matches/:fixtureId/events` | raw.match_events | Disponível com caveat leve |
| `GET /v1/matches/:fixtureId/lineups` | raw.fixture_lineups | Disponível com caveat |
| `GET /v1/matches/:fixtureId/team-stats` | raw.match_statistics | Disponível com caveat |
| `GET /v1/matches/:fixtureId/player-stats` | raw.fixture_player_statistics | Disponível com caveat |
| `GET /v1/h2h?teamA&teamB&limit=1` (bloco cabeçalho) | raw.head_to_head_fixtures | Disponível |

---

### Club profile

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/teams/:teamId/contexts` | raw + mart | Novo; obrigatorio para resolver rota curta e busca global |
| `GET /v1/teams/:teamId?competitionId&seasonId` | raw + mart | Em consolidação |
| `GET /v1/teams/:teamId/season-summary?competitionId&seasonId` | mart + raw | Em consolidação |
| `GET /v1/matches?teamId&competitionId&seasonId` | raw.fixtures | Disponível |
| `GET /v1/teams/:teamId/squad?competitionId&seasonId` | raw.fixture_lineups (agregado) | Disponível com caveat |
| `GET /v1/availability/sidelined?teamId` | raw.team_sidelined | Em consolidação |

Observacao:

- `competitionId` e `seasonId` deixam de ser filtros opcionais no consumo canonico do club profile.

---

### Player profile

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/players/:playerId/contexts` | raw + mart | Novo; obrigatorio para resolver rota curta e busca global |
| `GET /v1/players/:playerId?competitionId&seasonId` | raw + mart | Disponível com caveat |
| `GET /v1/players/:playerId/season-stats?competitionId&seasonId` | raw.player_season_statistics | Disponível com caveat |
| `GET /v1/players/:playerId/matches?competitionId&seasonId` | raw.fixture_player_statistics | Disponível com caveat |
| `GET /v1/players/:playerId/history` | raw.player_season_statistics (multi-season) | Disponível com caveat |

Observacao:

- `competitionId` e `seasonId` deixam de ser filtros opcionais no consumo canonico do player profile.

---

### Head-to-head

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/h2h?teamA&teamB` | raw.head_to_head_fixtures | Disponível |
| `GET /v1/h2h/summary?teamA&teamB` | mart.h2h_summary | Disponível com espaço para UX mais rica |

---

### Busca global

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/search?q=:query&types=competition,team,player,match,coach` | índice BFF sobre raw | Depende de endpoint dedicado |

A busca global precisa de um endpoint de busca dedicado na BFF que indexe entidades de múltiplos domínios e devolva resultados paginados por categoria. Esse endpoint é novo — não é uma composição de endpoints existentes. É uma dependência de implementação de BFF específica para essa feature.

---

### Módulos secundários

| Contrato | Fonte esperada | Estado |
|---|---|---|
| `GET /v1/market/transfers?teamId&window` | raw.player_transfers | Em consolidação |
| `GET /v1/coaches?teamId&competitionKey&seasonLabel` | raw.team_coaches | Em consolidação |
| `GET /v1/availability/sidelined?teamId&competitionKey` | raw.team_sidelined | Em consolidação |

---

## 9. Estados de cobertura por tela

### Comportamento padrão por estado

| Estado | Comportamento visual | Ação de UX |
|---|---|---|
| `complete` | Render normal, sem badge | — |
| `partial` | Render do módulo + badge âmbar | Tooltip com reasonCode e escopo afetado |
| `unavailable` | Empty state contextualizado | CTA para trocar competição, temporada ou recorte |
| `unknown` | Render conservador sem afirmar completude | Nota discreta de cobertura não confirmada |

---

### Home

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Card de competição | Exibe card com badge; stats disponíveis parcialmente | Card existe mas sem stats; link para competition hub mantido | Card simplificado sem afirmar completude |
| Destaques do momento | Exibe líderes disponíveis + nota de cobertura parcial | Oculta faixa de destaques | Oculta faixa |

---

### Season hub — aba Standings

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Tabela de classificação | Tabela com times disponíveis + badge por linha afetada | Empty state + CTA trocar rodada/temporada | Tabela sem ordenação garantida + nota |
| Sparklines de posição | Exibe série disponível + trunca no gap | Oculta sparklines; mantém tabela | Oculta sparklines |

---

### Season hub — aba Calendar

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Lista de partidas | Exibe partidas disponíveis + badge por rodada afetada | Empty state por rodada + CTA trocar rodada | Exibe sem garantir completude |
| Status de partida | Exibe status disponível; trata ausente como "não informado" | Campo omitido | Campo omitido |

---

### Season hub — aba Rankings

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Ranking de jogadores | Exibe posições disponíveis + nota de base incompleta | Empty state + CTA trocar métrica ou janela | Oculta ordenação global; exibe por competição isolada |
| Ranking de times | Idem | Idem | Idem |

---

### Match center

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Cabeçalho | Exibe com campos ausentes marcados como "—" | Não aplicável (fixture sempre existe) | — |
| Timeline | Exibe eventos disponíveis + badge na aba | Empty state na aba + explicação | Badge de cobertura não confirmada |
| Lineups | Exibe titulares disponíveis + slots "não identificado" | Empty state na aba | Badge; não afirmar formação completa |
| Team stats | Exibe categorias disponíveis + badge por categoria ausente | Empty state na aba | Badge; não comparar times se base assimétrica |
| Player stats | Exibe jogadores disponíveis + nota de cobertura | Empty state na aba + explicação | Badge; ocultar ordenações que exigem base completa |

---

### Club profile

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Forma recente | Exibe jogos disponíveis + trunca na lacuna | Empty state no bloco | Badge |
| Posição na tabela | Exibe com nota se standings incompleto | Campo omitido | Badge |
| Aba Squad | Exibe jogadores com dados; omite sem identificação | Empty state + explicação de coverage | Badge |
| Aba Stats | Exibe métricas disponíveis; omite derivadas sem base | Empty state da aba | Badge; não exibir comparativo entre temporadas |

---

### Player profile

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Stats agregadas | Exibe métricas disponíveis + badge | Empty state + CTA trocar temporada | Badge; não exibir totais que dependem de completude |
| Aba Matches | Exibe partidas com stats disponíveis; "—" nas ausentes | Empty state da aba | Badge |
| Aba History | Exibe temporadas com dados; omite temporadas sem cobertura | Empty state da aba | Badge; não traçar linha de evolução com gaps |

---

### Head-to-head

| Módulo | Partial | Unavailable | Unknown |
|---|---|---|---|
| Saldo histórico | Exibe com nota de escopo parcial | Empty state + CTA trocar filtro | Badge; não afirmar dominância histórica |
| Últimos confrontos | Exibe disponíveis + trunca | Empty state da seção | Badge |

---

## 10. Frontend Delivery Plan — blocos de implementação

### Bloco 0 — Infraestrutura de app

**O que entra:**
- Setup do projeto frontend (estrutura de pastas, roteamento, estado global)
- Shell global: header, navegação principal, área de conteúdo
- Sistema de filtros globais (seletor de competição e temporada)
- Breadcrumb persistente
- Sistema de coverage-state: componentes de badge, tooltip e empty state reutilizáveis
- Contrato base de resposta da BFF (tipagem e parseamento de `meta.coverage`)

**Por que primeiro:** Tudo o que vem depois depende disso. Sem shell, filtros e sistema de coverage-state, cada tela posterior precisaria reinventar essas peças.

**Valor entregue:** App navegável com estrutura. Ainda sem conteúdo real.

**Dependência de BFF:** Nenhuma crítica. Pode usar mocks.

---

### Bloco 1 — Esqueleto de competições e temporadas

**O que entra:**
- Competition hub (`/competitions/:competitionKey`)
- Season hub com estrutura de abas (sem conteúdo ainda)
- `GET /v1/competitions` e `GET /v1/competitions/:key/seasons`

**Por que segundo:** Define o eixo principal de navegação. Permite testar o fluxo competição → temporada antes de adicionar conteúdo.

**Valor entregue:** Navegação de primeiro nível funcional. Usuário consegue selecionar competição e temporada.

**Dependência de BFF:** Endpoints de competições e temporadas — os mais simples e já sustentados.

---

### Bloco 2 — Calendário e partidas

**O que entra:**
- Season hub aba Calendar
- Match center — Cabeçalho + aba Summary
- `GET /v1/matches` (lista) e `GET /v1/matches/:fixtureId` (detalhe)

**Por que terceiro:** Fixtures são o dado mais sólido e de maior cobertura do projeto. Entrega o fluxo central do produto: competição → temporada → jogo.

**Valor entregue:** Usuário consegue navegar pelo calendário e abrir uma partida. Primeiro conteúdo real de valor.

**Dependência de BFF:** Endpoints de matches com coverage-state no meta.

---

### Bloco 3 — Standings

**O que entra:**
- Season hub aba Standings
- `GET /v1/standings?competitionKey&seasonLabel&roundId`

**Por que quarto:** Dado muito sólido, alto valor percebido, implementação relativamente direta.

**Valor entregue:** Tabela de classificação navegável por rodada. Produto começa a parecer completo.

**Dependência de BFF:** Endpoint de standings com round metadata.

---

### Bloco 4 — Match center completo

**O que entra:**
- Match center abas: Timeline, Lineups, Team stats, Player stats
- `GET /v1/matches/:fixtureId/events`
- `GET /v1/matches/:fixtureId/lineups`
- `GET /v1/matches/:fixtureId/team-stats`
- `GET /v1/matches/:fixtureId/player-stats`
- Coverage-state independente por aba

**Por que quinto:** Complementa o bloco 2. O match center sem abas de detalhe é incompleto; com elas vira a tela mais rica do produto.

**Valor entregue:** Análise completa de partida. Tela principal do produto funcionando de ponta a ponta.

**Dependência de BFF:** Quatro endpoints novos, todos com coverage-state obrigatório no meta.

---

### Bloco 5 — Rankings

**O que entra:**
- Season hub aba Rankings
- `GET /v1/rankings?competitionKey&seasonLabel&metric&window`
- Coverage-state específico para rankings com base incompleta

**Por que sexto:** Fecha o Season hub. Dado disponível, implementação direta, alto valor para o usuário.

**Valor entregue:** Season hub completo com três abas funcionais.

**Dependência de BFF:** Endpoint de rankings com suporte a filtro de métrica e janela temporal.

---

### Bloco 6 — Home / portfolio

**O que entra:**
- Home com grid de cards de competição
- Faixa de destaques (versão simples: líderes de ranking já disponíveis)
- `GET /v1/competitions` com coverage-state por competição

**Por que sexto (após ter conteúdo):** A Home agrega conteúdo de outras telas. Só faz sentido construí-la depois que há conteúdo real para agregar.

**Valor entregue:** Ponto de entrada do produto funcionando. Usuário consegue ver o portfolio e navegar.

**Dependência de BFF:** Composição do endpoint de competições com dados agregados de standings e fixtures.

---

### Bloco 7 — Busca global

**O que entra:**
- Componente de busca no header (overlay de resultados)
- `GET /v1/search?q&types`
- Categorias: competições, times, jogadores, partidas

**Por que sétimo:** Depende de haver entidades suficientes no sistema para os resultados fazerem sentido. O endpoint de busca é novo e exige trabalho de BFF.

**Valor entregue:** Navegação transversal. Usuário não precisa mais seguir fluxo hierárquico para encontrar o que quer.

**Dependência de BFF:** Endpoint de busca dedicado — nova implementação na BFF.

---

### Bloco 8 — Club profile

**O que entra:**
- Club profile abas Overview e Matches (versão 1)
- `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId`
- rota curta `/teams/:teamId` atuando apenas como resolver
- `GET /v1/teams/:teamId/contexts`, `GET /v1/teams/:teamId?competitionId&seasonId` e `GET /v1/matches?teamId&competitionId&seasonId`

**Por que oitavo:** Navegável a partir do match center (bloco 4). Dependência de contratos BFF de times que precisam de trabalho de consolidação.

**Valor entregue:** Perfil de clube funcional. Fecha o loop entre jogo e time.

**Dependência de BFF:** Resolver de contexto + endpoint de team summary — em consolidação na mart.

---

### Bloco 9 — Player profile

**O que entra:**
- Player profile abas Overview e Matches (versão 1)
- `/competitions/:competitionKey/seasons/:seasonLabel/players/:playerId`
- rota curta `/players/:playerId` atuando apenas como resolver
- `GET /v1/players/:playerId/contexts`, `GET /v1/players/:playerId/season-stats?competitionId&seasonId` e `GET /v1/players/:playerId/matches?competitionId&seasonId`

**Por que nono:** Navegável a partir do match center (aba player stats) e do club profile (aba squad).

**Valor entregue:** Perfil de jogador funcional. Fecha o loop entre jogo e jogador.

**Dependência de BFF:** Resolver de contexto + endpoint de player com player_season_statistics — disponível com caveat.

---

### Bloco 10 — Club profile e Player profile — expansão

**O que entra:**
- Club profile abas Squad e Stats
- Player profile aba History
- Head-to-head (`/h2h`)
- Seção de availability na aba Squad do club profile
- `GET /v1/teams/:teamId/squad?competitionId&seasonId`
- `GET /v1/players/:playerId/history`
- `GET /v1/h2h?teamA&teamB`
- `GET /v1/availability/sidelined?teamId`

**Por que décimo:** Expande os perfis de entidade com dados que dependem de consolidação parcial de mart e contratos BFF mais completos.

**Valor entregue:** Perfis completos. Head-to-head funcional. Availability como contexto de elenco.

**Dependência de BFF:** Múltiplos contratos em consolidação.

---

### Bloco 11 — Módulos secundários (fase 3)

**O que entra:**
- Market (`/more/market`)
- Coaches (`/more/coaches`)
- Availability como tela global (`/more/availability`)
- Home executiva com comparativos cross-competition e feed de insights

**Por que último:** Dependem de contratos BFF dedicados e de mart consolidada em escala portfolio.

**Valor entregue:** Cobertura completa do portfolio de dados. Produto amadurecido.

**Dependência de BFF:** Contratos de market, coaches e availability estabilizados. Mart rematerializada portfolio-wide.

---

## 11. Sumário de dependências críticas

| Bloco | Dependência crítica de BFF | Estado |
|---|---|---|
| 0–1 | Competições e temporadas | Disponível |
| 2–3 | Fixtures e standings | Disponível |
| 4 | Match center (events, lineups, stats) | Disponível com caveat |
| 5 | Rankings com filtros | Disponível com caveat |
| 6 | Home com agregados | Em consolidação parcial |
| 7 | Endpoint de busca global | Novo — a implementar |
| 8–9 | Team e player summaries | Em consolidação |
| 10 | Squad, history, h2h, availability | Em consolidação |
| 11 | Market, coaches, mart portfolio-wide | Em consolidação / fase 3 |

---

## 12. Referências

- `INVENTARIO_DADOS_DO_PROJETO.md`
- `FRONTEND_MANUAL_POSSIBILIDADES.md`
- `FRONTEND_ARCHITECTURE.md` (documento anterior)
- Blueprint inicial gerado em `2026-03-20`
