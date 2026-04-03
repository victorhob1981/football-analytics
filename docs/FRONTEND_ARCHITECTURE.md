# football-analytics — Arquitetura de Frontend

> Refinamento do blueprint inicial.  
> Data de referência: `2026-03-20`  
> Estado: planejamento estrutural — sem código, sem wireframe detalhado.

---

## 1. Visão geral da arquitetura

### Princípios que moldam a estrutura

O frontend é organizado em torno de dois eixos de navegação:

- **Eixo de contexto**: competição → temporada → fase/rodada. É o filtro global que atravessa toda a aplicação.  
- **Eixo de entidade**: clube → jogador. São perfis navegáveis que existem sempre dentro de um contexto de competição/temporada.

Esses dois eixos se cruzam na partida — o Match center é o ponto de encontro entre um evento do calendário e os dados de entidades (times, jogadores) que participaram dele.

### Camadas estruturais

```
Shell global
└── Telas principais (rotas de nível 1)
    └── Subtelas ou variações de contexto (rotas de nível 2)
        └── Módulos internos (seções, abas, blocos)
            └── Seções reutilizáveis (componentes de dado compartilhados entre telas)
```

A **shell global** é o único elemento permanente: mantém os filtros de contexto (competição / temporada) e o breadcrumb sempre visíveis, independente de onde o usuário estiver.

As **telas principais** são rotas com URL própria e propósito único. Cada uma pode ter **subtelas** quando o conteúdo muda substancialmente por parâmetro (ex: `/competitions/brasileirao` vs `/competitions/premier-league` são a mesma tela, contextos diferentes).

**Módulos internos** são agrupamentos de conteúdo dentro de uma tela — podem se manifestar como abas, seções verticais ou blocos colapsáveis. Não têm URL própria.

**Seções reutilizáveis** são unidades de dado que aparecem em mais de uma tela com a mesma lógica (ex: o bloco de forma recente de um clube aparece no club profile e no cabeçalho do match center).

### Decisão central sobre coverage-state

Coverage-state não é tratamento de erro — é dado de produto. Cada módulo interno carrega e exibe seu próprio estado de cobertura de forma independente. Uma tela nunca entra em estado de falha total por causa de um módulo parcial.

---

## 2. Árvore de navegação

```
/
├── /home                                   # Home / portfolio
│
├── /competitions                           # Lista de competições
│   └── /competitions/:competitionKey       # Competition hub
│       └── /competitions/:competitionKey/seasons/:seasonLabel   # Season hub
│           ├── /standings                  # Tabela de classificação (aba)
│           ├── /calendar                   # Calendário / lista de partidas (aba)
│           └── /rankings                   # Rankings da temporada (aba)
│
├── /matches/:fixtureId                     # Match center
│
├── /teams/:teamId                          # Resolver/redirect de contexto
│
├── /competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId
│                                           # Club profile canonico
│
├── /players/:playerId                      # Resolver/redirect de contexto
│
├── /competitions/:competitionKey/seasons/:seasonLabel/players/:playerId
│                                           # Player profile canonico
│
├── /h2h                                    # Head-to-head (query params: teamA, teamB)
│
└── /more                                   # Módulos secundários (fase 3)
    ├── /more/market                        # Transferências
    ├── /more/coaches                       # Técnicos
    └── /more/availability                  # Disponibilidade de elenco
```

Regra de contexto:

- clube e jogador so sao telas de dados quando a URL ja carrega `competitionKey + seasonLabel + entityId`.
- `/teams/:teamId` e `/players/:playerId` nao carregam a tela final; apenas resolvem o contexto e redirecionam.

### Pontos de entrada de navegação

| Ponto de entrada | De onde vem | Para onde vai |
|---|---|---|
| Card de competição | Home | Competition hub |
| Seletor de temporada | Competition hub | Season hub |
| Partida no calendário | Season hub / calendar | Match center |
| Nome de time (qualquer tela) | Match center, Standings, Rankings | Club profile canonico no contexto ativo |
| Nome de jogador (qualquer tela) | Match center, Rankings, Club profile | Player profile canonico no contexto ativo |
| Par de times | Club profile, Match center | Head-to-head |
| Filtro global de competição/temporada | Shell | Recarrega contexto na tela atual |

---

## 3. Mapa de telas e módulos

### 3.1 Home / portfolio — `/home`

**Função**: orientação e lançamento. Responde "o que está acontecendo no portfolio?"

**Módulos internos**:
- Grid de cards de competição (um card por competição, com estado de cobertura)
- Faixa de destaques do momento (artilheiro, aproveitamento recente, gol do portfolio)
- Atalhos rápidos para jogos mais recentes de cada competição

**Seções reutilizadas por outras telas**: nenhuma — é a tela mais autossuficiente.

**Evolução prevista**: na fase 1 é um launcher com cards simples. Na fase 3 evolui para dashboard executivo com comparativos cross-competition e feed de insights.

---

### 3.2 Competition hub — `/competitions/:competitionKey`

**Função**: ponto de entrada de uma competição específica. Apresenta o recorte atual e distribui para as subáreas.

**Módulos internos**:
- Cabeçalho da competição (nome, logo, temporadas disponíveis)
- Seletor de temporada (leva ao Season hub)
- Cards de resumo da temporada atual (total de jogos, rodada atual, líder)
- Navegação rápida para standings, calendário e rankings

**Observação**: Competition hub não tem abas — sua função é ser um hub de distribuição, não um container de conteúdo profundo.

---

### 3.3 Season hub — `/competitions/:competitionKey/seasons/:seasonLabel`

**Função**: visão concentrada daquela competição naquela temporada. É o contexto canônico de navegação.

**Estrutura em abas**:

#### Aba: Standings
- Tabela oficial de classificação
- Seletor de rodada (para ver a tabela em qualquer ponto da temporada)
- Mini-sparklines de posição e pontos por time (colapsável)
- Comparador de dois times na tabela

#### Aba: Calendar
- Lista de partidas agrupadas por rodada ou data
- Filtros: status (realizado / pendente), time
- Card de partida com placar e link para Match center
- Indicador de cobertura por rodada (se os dados estão completos)

#### Aba: Rankings
- Ranking de jogadores por métrica (gols, assistências, rating, minutos)
- Ranking de times por estatística (posse, finalizações, passes)
- Seletor de janela temporal (temporada completa / últimas N rodadas)
- Coverage-state por ranking quando a base comparável não está confirmada

**Observação**: Standings, Calendar e Rankings eram propostos como telas separadas no blueprint inicial. Aqui são abas dentro do Season hub. Isso faz mais sentido porque (a) os três dependem do mesmo contexto de competição/temporada e (b) o usuário raramente vai direto para standings sem passar pela competição — o fluxo natural é descendente.

---

### 3.4 Match center — `/matches/:fixtureId`

**Função**: análise completa de uma partida específica. É a tela mais densa do produto.

**Estrutura em abas/seções independentes**:

#### Seção fixa (sempre visível): Cabeçalho
- Times, placar, status, mando, data, estádio
- Links para os club profiles dos dois times
- Indicador de cobertura geral da partida

#### Aba: Resumo
- Placares parciais, gols marcados com autores e minuto
- Estatísticas principais lado a lado (posse, finalizações, cartões)
- Eventos principais da partida em ordem cronológica resumida

#### Aba: Timeline
- Eventos minuto a minuto (gols, cartões, substituições, faltas)
- Filtro por tipo de evento
- Coverage-state quando o dado de eventos está parcial

#### Aba: Escalações
- Titulares e banco de cada time em layout espelhado
- Formação tática quando disponível
- Slots sem identificação exibidos explicitamente (não ocultados)
- Coverage-state independente por time

#### Aba: Stats do jogo
- Comparativo de estatísticas de time (barras lado a lado)
- Categorias: ataque, passe, defesa, disciplina
- Coverage-state quando stats de time estão parciais

#### Aba: Stats de jogadores
- Tabela de desempenho individual de cada time
- Ordenável por coluna
- Link para player profile de cada jogador
- Coverage-state quando fixture_player_statistics está parcial

**Observação**: cada aba carrega seus dados de forma independente. O usuário pode acessar a aba de Escalações mesmo que a aba de Stats de jogadores esteja com cobertura parcial.

---

### 3.5 Club profile — `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId`

**Função**: perfil de um clube dentro de um contexto de competição/temporada.

**Regra de entrada**:

- esta e a URL canonica de dados.
- `/teams/:teamId` existe apenas para resolver o contexto certo e redirecionar.
- match center, standings, rankings e season hub devem sempre linkar diretamente para esta rota, usando o contexto atual.

**Estrutura em abas**:

#### Aba: Visão geral
- cabeçalho contextual com `competitionKey`, `seasonLabel` e nome da competicao/temporada
- Forma recente (últimas N partidas)
- Posição atual na tabela (bloco reutilizável do Standings)
- Comparativo casa/fora (aproveitamento, gols pró/contra)
- Próximo jogo e último jogo

#### Aba: Partidas
- Histórico de partidas da temporada com resultado e link para match center
- Filtro por mando, resultado, competição

#### Aba: Elenco
- Jogadores utilizados na temporada ordenados por minutos
- Link para player profile de cada um
- Indicador de indisponibilidade (sidelined) quando disponível

#### Aba: Estatísticas
- Trend mensal de gols marcados/sofridos
- Métricas de desempenho agregadas por temporada
- Comparativo com outras temporadas do mesmo clube (quando mart disponível)

**Necessidade adicional de BFF**:

- endpoint de contexto (`/teams/:teamId/contexts`) para resolver a rota curta e para busca global.

**Seções reutilizadas em outras telas**:
- "Forma recente" → aparece no cabeçalho do Match center
- "Posição na tabela" → referenciada no Season hub / Standings

---

### 3.6 Player profile — `/competitions/:competitionKey/seasons/:seasonLabel/players/:playerId`

**Função**: perfil de um jogador com estatísticas por temporada.

**Regra de entrada**:

- esta e a URL canonica de dados.
- `/players/:playerId` existe apenas para resolver o contexto certo e redirecionar.
- links saindo de rankings, match center e club profile devem carregar o contexto ativo e apontar direto para esta rota.

**Estrutura em abas**:

#### Aba: Visão geral
- cabeçalho contextual com `competitionKey`, `seasonLabel`, clube e posicao
- Estatísticas agregadas da temporada selecionada (gols, assistências, minutos, rating)
- Clube atual e posição
- Destaques de desempenho recente

#### Aba: Partidas
- Histórico de partidas com stats individuais por jogo
- Filtro por competição e temporada
- Link para match center de cada partida

#### Aba: Histórico
- Comparativo entre temporadas do mesmo jogador
- Evolução de métricas ao longo do tempo
- Coverage-state quando player_season_statistics tem gaps

**Necessidade adicional de BFF**:

- endpoint de contexto (`/players/:playerId/contexts`) para resolver a rota curta e para busca global.

**Seções reutilizadas em outras telas**:
- Card compacto de jogador → aparece nas abas de escalação e stats do Match center

---

### 3.7 Head-to-head — `/h2h?teamA=:id&teamB=:id`

**Função**: histórico de confrontos entre dois times específicos.

**Estrutura** (tela sem abas — conteúdo vertical):
- Saldo histórico (vitórias, empates, derrotas)
- Últimos N confrontos com resultado e link para match center
- Tendência recente (forma nos últimos confrontos)
- Filtro por competição e janela temporal

**Pontos de entrada**: club profile (link "ver H2H"), cabeçalho do match center (link entre os dois times).

---

### 3.8 Módulos secundários — `/more/*`

Agrupados em uma área separada da navegação principal por ainda dependerem de contratos BFF em consolidação.

**Market** (`/more/market`): feed de transferências com filtros por clube, período e tipo. Tela simples, sem abas na fase inicial.

**Coaches** (`/more/coaches`): timeline de passagens de técnicos por clube. Navegável por competição/temporada.

**Availability** (`/more/availability`): painel de indisponíveis agrupados por clube. Vinculado naturalmente ao club profile — na fase 2, o bloco de disponibilidade aparece como seção dentro do club profile / aba Elenco.

---

## 4. Critérios de classificação: tela vs aba vs seção

### Quando algo é uma tela (rota com URL própria)

- Tem propósito e identidade independente — faz sentido o usuário favoritar ou compartilhar aquela URL.
- Pode ser acessada por múltiplos caminhos de navegação diferentes.
- Não depende de estar "dentro" de outra tela para ter significado.
- Exemplos: Match center, Club profile, Player profile, H2H.

### Quando algo é uma aba dentro de uma tela

- O conteúdo depende do mesmo contexto da tela pai (mesma competição/temporada/fixture).
- Não faz sentido navegar direto para aquele conteúdo sem passar pelo contexto.
- Divide a tela com outros conteúdos de mesma natureza e mesmo nível hierárquico.
- Exemplos: Standings, Calendar e Rankings dentro do Season hub; Timeline, Escalações e Stats dentro do Match center.

### Quando algo é uma seção reutilizável

- O mesmo bloco de dado aparece em mais de uma tela com a mesma lógica de exibição.
- Não tem URL própria nem é suficientemente complexo para justificar uma aba.
- Pode ser embutido em telas diferentes com contexto ligeiramente diferente.
- Exemplos: "Forma recente" (Club profile + Match center), "Card compacto de jogador" (Match center + Rankings + Club profile), "Badge de coverage-state" (presente em todos os módulos).

### Resumo das revisões em relação ao blueprint inicial

| Elemento original | Classificação anterior | Classificação revisada | Motivo |
|---|---|---|---|
| Standings | Tela | Aba no Season hub | Depende do contexto de competição/temporada; não faz sentido como destino direto |
| Calendar | Tela | Aba no Season hub | Mesma razão |
| Rankings | Tela | Aba no Season hub | Mesma razão; rankings globais ficam para fase 3 |
| Timeline de eventos | Tela/módulo | Aba no Match center | Não tem sentido sem o contexto da partida |
| Lineups | Módulo | Aba no Match center | Idem |
| Season | Subtela | Season hub com abas | Centraliza standings, calendar e rankings num único contexto |
| Availability | Tela secundária | Seção no Club profile (fase 2) + tela em /more (fase 3) | Valor maior como contexto de clube do que como destino autônomo |

---

## 5. Ordem recomendada de implementação

A sequência segue três critérios em paralelo: maturidade do dado disponível, valor de produto entregue e dependências entre telas.

### Fase 1 — Fundação navegável

Objetivo: o usuário consegue navegar pelo portfolio, ver a classificação, acessar jogos e entrar no match center.

| Ordem | Entregável | Justificativa |
|---|---|---|
| 1 | Shell global + filtros de contexto | Sem isso nada funciona; é a base de toda navegação |
| 2 | Competition hub | Ponto de entrada de qualquer competição |
| 3 | Season hub (estrutura + aba Calendar) | Dados de fixtures são os mais sólidos; entrega valor imediato |
| 4 | Season hub (aba Standings) | Dado mais consolidado do projeto; alto valor percebido |
| 5 | Match center — Cabeçalho + Resumo + Timeline | Core do produto; dados de events têm boa cobertura |
| 6 | Match center — Escalações + Stats | Abas adicionais com coverage-state já preparado |
| 7 | Season hub (aba Rankings) | Fecha o Season hub com o terceiro eixo de análise |
| 8 | Home / portfolio | Mais útil quando já há conteúdo para agregar |

### Fase 2 — Expansão de entidades

Objetivo: o usuário consegue explorar clubes e jogadores com profundidade.

| Ordem | Entregável | Justificativa |
|---|---|---|
| 9 | Club profile — Visão geral + Partidas | Navegável a partir do Match center; dado bem sustentado |
| 10 | Player profile — Visão geral + Partidas | Navegável a partir do Match center e Club profile |
| 11 | Club profile — Elenco + Estatísticas | Depende de marts de resumo; versão incremental |
| 12 | Player profile — Histórico | Depende de player_season_statistics com boa cobertura |
| 13 | Head-to-head | Dado com cobertura completa; baixo esforço relativo |
| 14 | Seção Availability no Club profile | Completa o contexto de elenco; dado sustentado no raw |

### Fase 3 — Módulos secundários e Home executiva

Objetivo: cobrir os módulos que dependem de consolidação de camada de consumo.

| Ordem | Entregável | Dependência crítica |
|---|---|---|
| 15 | Home portfolio — versão executiva | Mart consolidada em escala portfolio |
| 16 | Market | Contrato BFF dedicado estabilizado |
| 17 | Coaches | Contrato BFF + marts de resumo |
| 18 | Availability como tela em /more | Contrato BFF dedicado |
| 19 | Rankings globais cross-competition | Mart portfolio-wide + estratégia de comparabilidade |

---

## 6. Referências

- `INVENTARIO_DADOS_DO_PROJETO.md`
- `FRONTEND_MANUAL_POSSIBILIDADES.md`
- Blueprint inicial gerado em `2026-03-20`
