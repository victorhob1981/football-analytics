# FRONTEND_REFINEMENT_EXECUTION_LOG

Data de referencia: 2026-03-30

Objetivo desta rodada:
- executar o refinamento/fechamento do frontend inteiro sem reabrir o nucleo vivo;
- normalizar naming, navegacao, rotas publicas, aliases legados e superficies secundarias;
- registrar cada bloco com status, evidencia objetiva e risco residual.

Fontes prioritarias de verdade:
- `docs/FRONTEND_MAPA_COMPLETO_DE_PAGINAS.md`
- frontend vivo em `frontend/src/app`, `frontend/src/features`, `frontend/src/shared`
- contratos atuais em `docs/BFF_API_CONTRACT.md`

Skills aplicadas:
- `Emil Design Eng`
- `execute-block-plan`
- `validate-before-after`

## 1. Diagnostico inicial consolidado

Estado inicial confirmado com evidencia objetiva:
- o nucleo vivo do produto ja esta fechado em home executiva, competicoes, season hub, rankings family, matches, teams e players;
- a maior parte do risco atual nao esta no core, e sim em ambiguidade de navegacao, naming publico e superficies secundarias rasas;
- `head-to-head` tem rota publica e base segura para V1 reaproveitando `GET /api/v1/matches` com `teamId`;
- `market`, `coaches` e `availability` seguem sem contrato publico dedicado no BFF atual;
- `/clubs` ainda conflita semanticamente com `/teams`;
- `/audit` ja se declara interna e nao deve virar superficie publica;
- `/landing` existe, mas ainda e placeholder puro.

Riscos imediatos de regressao:
- working tree ja esta sujo em muitos arquivos fora deste escopo;
- varios textos publicos misturam `clubes` e `times`;
- criar pagina nova sem contrato poderia simular dominio que o produto ainda nao entrega.

## 2. Linha-mestra de design e UX

Direcao sistemica desta execucao:
- spacing: blocos com ritmo largo em heroes e paines principais; listas densas com padding menor, mas sempre com respiro visual claro;
- hierarchy: `hero -> leitura operacional -> filtros locais -> lista/modulo -> saidas contextuais`;
- grid: duas familias principais:
  - superficies executivas em `hero + painel lateral`;
  - superficies analiticas em `conteudo principal + painel de leitura/ajuda`;
- hero/header patterns:
  - rotas canonicas continuam usando `PlatformShellFrame` + hero proprio da feature;
  - rotas secundarias ou honestas usam hero deliberado, nunca placeholder cru;
- cards:
  - mesma familia visual do app atual: borda suave, fundo branco/leite, sombra curta, tags compactas, CTA claro;
- tabelas/listas:
  - foco em legibilidade e progressao de leitura, nao em densidade maxima;
  - toda lista precisa deixar claro contexto, acao primaria e fallback;
- filtros e navegacao contextual:
  - filtros globais continuam sendo o eixo principal;
  - filtros locais so entram quando refinam a leitura da propria superficie;
- hover/active/focus:
  - reforco leve de cor e elevacao;
  - press feedback rapido, sem animacao longa;
- loading/empty/error/fallback:
  - usar superficies do proprio produto;
  - nada de `TODO`, `placeholder` ou copy genérica em rota publica;
- motion:
  - manter transicoes curtas e discretas;
  - evitar animacao decorativa em navegacao frequente.

## 3. Politica sistemica de rotas

### 3.1 Canonicas

Canonicas publicas do produto:
- `/`
- `/competitions`
- `/competitions/[competitionKey]`
- `/competitions/[competitionKey]/seasons/[seasonLabel]`
- `/matches`
- `/matches/[matchId]`
- `/teams`
- `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]`
- `/players`
- `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]`
- `/rankings/[rankingType]`

Regra:
- estas rotas definem a experiencia final;
- CTAs publicas devem sempre apontar para elas ou para uma entrada claramente declarada como secundaria.

### 3.2 Compatibilidade e legado

Aliases/compatibilidade:
- `/teams/[teamId]`
- `/players/[playerId]`
- `/clubs`
- `/clubs/[clubId]`
- `/competition/[competitionId]`

Regra:
- manter por deep link/compatibilidade;
- nao promover essas rotas como destino principal de experiencia;
- copy publica nao deve sugerir que elas sao a area canonica.

### 3.3 Secundarias publicas ou semipublicas

Secundarias atuais:
- `/head-to-head`
- `/market`
- `/coaches`
- `/coaches/[coachId]`
- `/landing`

Regra:
- quando houver base segura, entregar superficie funcional;
- quando nao houver base suficiente, entregar pagina honesta, deliberada e explicitamente limitada;
- nao mascarar falta de contrato como modulo "quase pronto".

### 3.4 Internas

Interna:
- `/audit`

Regra:
- fora da descoberta publica principal;
- pode existir, mas deve deixar claro seu papel interno.

### 3.5 Naming publico

Convencao canônica desta rodada:
- area/lista/rota publica: `Times`
- detalhe contextual: `Perfil de time`
- `Clubes` fica restrito a referencias historicas/compatibilidade onde isso evitar ruptura, nunca como destino principal de navegacao
- `Rankings` continua sem hub proprio; a descoberta publica aponta para um ranking canônico (`player-goals`) sem fingir pagina `/rankings`

## 4. Ordem dos blocos

| Ordem | Bloco | Superficie | Status | Diagnostico curto |
| --- | --- | --- | --- | --- |
| 1 | Superficie transversal | shell, naming, aliases, rotas publicas vs legado | `COMPLETO` | ambiguidade `clubs` vs `teams` e rotas secundarias sem framing coerente |
| 2 | Home executiva | `/` | `COMPLETO` | boa base; faltavam alinhamento de CTAs, naming e preservacao de contexto |
| 3 | Nucleo vivo preservado | competicoes, season hub, matches, teams, players, rankings | `COMPLETO` | base permaneceu verde; houve apenas normalizacao pontual e conexoes melhores |
| 4 | Head-to-head | `/head-to-head` | `COMPLETO` | rota publica saiu de placeholder e virou V1 funcional no recorte atual |
| 5 | Market | `/market` | `COMPLETO` | sem contrato de transfers; superficie ficou honesta e deliberada |
| 6 | Coaches | `/coaches` e `/coaches/[coachId]` | `COMPLETO` | indice criado; detalhe deixou de ser placeholder cru |
| 7 | Availability | `team profile > squad` | `COMPLETO` | lacuna tratada no lugar correto; rota global nao criada |
| 8 | Landing | `/landing` | `COMPLETO` | rota editorial separada da home operacional |
| 9 | Interno e fechamento | `/audit`, legado final, validacao | `COMPLETO` | `audit` mantido como interno e validacao estatica ficou verde |

## 5. Registro por bloco

### Bloco 1 - Superficie transversal

Status:
- `COMPLETO`

Antes:
- shell e home ainda expunham naming publico misto entre `clubs` e `teams`;
- `/clubs` redirecionava para `/competitions`, quebrando promessa semantica da CTA;
- `head-to-head`, `market` e `coaches` existiam na shell como superficies sem framing consistente.

Mudancas aplicadas:
- label publica do shell normalizada para `Times`;
- placeholder de busca do shell alinhado para `times`;
- `/clubs` passou a redirecionar para `/teams`, preservando compatibilidade sem confundir a area principal;
- shell state atualizado para:
  - marcar `/clubs/[clubId]` explicitamente como compatibilidade;
  - tratar `head-to-head` como comparativo real;
  - tratar `market` e `coaches` como superficies publicas em preparacao, nao como erro genérico;
  - manter `/audit` como interna.

Arquivos alterados:
- `frontend/src/app/(platform)/PlatformShell.tsx`
- `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- `frontend/src/app/(platform)/clubs/page.tsx`

Validacao objetiva:
- inspecao de rotas, shell e aliases apos mudanca;
- busca por `href="/clubs"` e copy publica residual sem ocorrencias fora do framing legado intencional.

Riscos residuais:
- `/clubs/[clubId]` continua existindo por deep link e compatibilidade, de forma intencional.

### Bloco 2 - Home executiva

Status:
- `COMPLETO`

Antes:
- home ja era forte como entrada executiva, mas ainda prometia `Clubes`;
- links de rodape para superficies secundarias perdiam o contexto ativo;
- H2H existia na home como promessa, mas sem encadeamento completo com o resto do produto.

Mudancas aplicadas:
- quick link `Clubes` virou `Times` apontando para a rota canonica;
- quick link `Head-to-head` passou a preservar filtros globais;
- rodape passou a preservar contexto para `matches`, `players`, `teams`, `market` e `coaches`;
- links editoriais e institucionais reorganizados para separar home operacional e landing.

Arquivos alterados:
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`

Validacao objetiva:
- leitura do before/after da home com URLs geradas via `buildMatchesPath`, `buildPlayersPath`, `buildTeamsPath`, `buildHeadToHeadPath` e `buildFilterQueryString`.

Riscos residuais:
- `/landing` permanece editorial e não preserva query de contexto por decisão de papel da superfície.

### Bloco 3 - Nucleo vivo preservado

Status:
- `COMPLETO`

Antes:
- competicoes, season hub, players, teams, match center e rankings ja estavam estruturados;
- havia copy publica residual usando `clube` em superficies canônicas;
- faltavam algumas conexoes sistêmicas entre detalhe de time, match center e H2H.

Mudancas aplicadas:
- normalizacao de copy publica de `clube` para `time` nas superfícies canônicas tocadas;
- match center ganhou entrada direta para `head-to-head`;
- perfil de time ganhou CTA para iniciar comparativo com o time já selecionado;
- rankings mantiveram family route sem criar hub extra e com busca alinhada ao tipo de entidade;
- comparativo de jogadores foi preservado como superficie real sem rota propria, em linha com o mapa.

Arquivos alterados:
- `frontend/src/features/matches/components/MatchCenterHeader.tsx`
- `frontend/src/features/players/components/PlayerHistorySection.tsx`
- `frontend/src/features/players/components/PlayerOverviewSection.tsx`
- `frontend/src/features/rankings/components/RankingTable.tsx`
- `frontend/src/features/teams/components/TeamMatchesSection.tsx`
- `frontend/src/features/teams/components/TeamOverviewSection.tsx`
- `frontend/src/features/teams/components/TeamProfileContent.tsx`
- `frontend/src/features/teams/components/TeamsPageContent.tsx`
- `frontend/src/features/teams/components/TeamStatsSection.tsx`
- `frontend/src/app/(platform)/players/page.tsx`
- `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]/page.tsx`

Validacao objetiva:
- revisao de links gerados a partir de match center, listagens e perfis;
- `pnpm typecheck`;
- `pnpm lint`.

Riscos residuais:
- comparativo de jogadores continua sem deep link dedicado, por alinhamento ao mapa atual.

### Bloco 4 - Head-to-head

Status:
- `COMPLETO`

Antes:
- rota publica existia como placeholder;
- o mapa apontava `head-to-head` como lacuna forte;
- nao havia descoberta direta a partir de match center e perfil de time.

Mudancas aplicadas:
- criada superficie real em `/head-to-head`;
- comparativo usa apenas base segura:
  - lista de times no contexto canônico;
  - partidas do Time A via contrato existente;
  - filtro local para confrontos contra o Time B;
- a pagina agora entrega:
  - selecao de Time A e Time B;
  - KPIs do confronto;
  - saldo de gols;
  - sequencia recente;
  - lista de confrontos com links para match center e perfis canonicos;
  - empty/error/loading/contexto obrigatorio honestos;
- a implementacao foi movida para a camada `app` para respeitar as regras de fronteira entre features.

Arquivos alterados:
- `frontend/src/app/(platform)/head-to-head/page.tsx`
- `frontend/src/app/(platform)/head-to-head/HeadToHeadPageContent.tsx`
- `frontend/src/features/matches/components/MatchCenterHeader.tsx`
- `frontend/src/features/teams/components/TeamProfileContent.tsx`
- `frontend/src/shared/utils/context-routing.ts`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- validacao manual do fluxo:
  - sem contexto => bloqueio honesto;
  - com contexto => seletores de times;
  - Time A = Time B => alerta;
  - sem confrontos => empty state;
  - com confrontos => links para match center e perfis.

Riscos residuais:
- V1 depende de `GET /api/v1/matches` por `teamId`, entao a leitura continua limitada ao calendário público já exposto;
- não há estatística dedicada de H2H nem série histórica fora do recorte atual.

### Bloco 5 - Market

Status:
- `COMPLETO`

Antes:
- `/market` era placeholder genérico;
- docs e arquitetura sustentavam o domínio, mas o BFF público não expõe transferências.

Mudancas aplicadas:
- `/market` virou uma superfície pública honesta e deliberada;
- hero, KPIs e painéis explicam o estado real do domínio;
- a página organiza a descoberta por `players`, `teams` e `matches` sem simular feed de mercado;
- o enquadramento deixou de parecer abandono e passou a comunicar limitação real de contrato.

Arquivos alterados:
- `frontend/src/app/(platform)/market/page.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- conferência dos links de continuidade com query passthrough preservada.

Riscos residuais:
- a página continua limitada pela ausência objetiva de endpoint público de transferências.

### Bloco 6 - Coaches

Status:
- `COMPLETO`

Antes:
- havia detalhe `/coaches/[coachId]`, mas sem índice `/coaches`;
- o detalhe era placeholder cru;
- home já apontava para `/coaches`, gerando quebra de descoberta.

Mudancas aplicadas:
- criada a rota índice `/coaches`;
- índice passou a enquadrar o domínio sem inventar catálogo, busca ou rankings;
- detalhe `/coaches/[coachId]` virou superfície de compatibilidade deliberada, com identificação do `coachId` e saídas seguras;
- descoberta pública ficou coerente com a ausência de contrato.

Arquivos alterados:
- `frontend/src/app/(platform)/coaches/page.tsx`
- `frontend/src/app/(platform)/coaches/[coachId]/page.tsx`
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
- `frontend/src/shared/components/navigation/usePlatformShellState.ts`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- leitura de navegação:
  - home -> `/coaches`;
  - `/coaches` -> times/partidas/competições;
  - deep link `/coaches/[coachId]` sem 404 e sem fingir perfil completo.

Riscos residuais:
- ainda não existe busca pública, listagem real ou perfil técnico com payload dedicado.

### Bloco 7 - Availability

Status:
- `COMPLETO`

Antes:
- o mapa apontava `availability/sidelined` como lacuna forte dentro de `team profile > squad`;
- o frontend atual não tinha shape nem payload para isso;
- não havia motivo suficiente para criar rota global.

Mudancas aplicadas:
- `TeamSquadSection` passou a explicitar que availability é lacuna do próprio squad;
- o produto agora deixa claro que, por enquanto, a leitura pública cobre participação, minutos e última aparição;
- nenhuma rota global foi criada.

Arquivos alterados:
- `frontend/src/features/teams/components/TeamSquadSection.tsx`

Validacao objetiva:
- inspeção do squad após ajuste;
- `pnpm typecheck`;
- `pnpm lint`.

Riscos residuais:
- a lacuna permanece real até existir payload público de afastados/sidelined.

### Bloco 8 - Landing

Status:
- `COMPLETO`

Antes:
- `/landing` era placeholder puro;
- não havia separação clara entre narrativa institucional e home executiva.

Mudancas aplicadas:
- criada landing editorial com identidade coerente com o produto;
- a página explicita sua diferença para a home executiva;
- links principais apontam só para o núcleo vivo do produto;
- legado e secundárias não são promovidos como destino institucional.

Arquivos alterados:
- `frontend/src/app/(marketing)/landing/page.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- revisão estrutural para garantir separação de papel entre landing e home.

Riscos residuais:
- landing é editorial/estática; não usa o payload dinâmico da home por decisão de papel e baixo risco.

### Bloco 9 - Interno e fechamento

Status:
- `COMPLETO`

Antes:
- `/audit` já era tratada como não pública;
- havia risco de a rodada acabar sem validação objetiva consolidada.

Mudancas aplicadas:
- `/audit` foi mantida como rota interna e fora da descoberta pública principal;
- fechamento final consolidado com validação estática e registro de decisões de normalização;
- implementação de H2H reposicionada para `app` após erro objetivo de fronteira entre features.

Arquivos alterados:
- `frontend/src/app/(platform)/audit/page.tsx`
- `docs/FRONTEND_REFINEMENT_EXECUTION_LOG.md`

Validacao objetiva:
- `pnpm typecheck`
- `pnpm lint`
- checagem de ausência de referência pública residual a `/clubs` fora do framing legado.

Riscos residuais:
- smoke/e2e não foram executados nesta rodada;
- o worktree já estava amplamente sujo antes da execução, então o log registra apenas este recorte.

## 6. Blockers

Blockers reais encontrados ate agora:
- nenhum

## 7. Correcao posterior por validacao manual

### Bloco 10 - Integracao de descoberta publica

Status:
- `COMPLETO`

Antes:
- validacao manual do usuario confirmou que as rotas secundarias novas estavam tecnicamente no ar, mas sem descoberta publica forte;
- `head-to-head`, `market`, `coaches` e `landing` ficavam dependentes de footer, deep link ou entradas pontuais;
- a IA primaria continuava concentrada no nucleo vivo sem deixar clara a existencia das superficies secundarias publicas.

Mudancas aplicadas:
- criados helpers canonicos para `market` e `coaches` com preservacao de filtros;
- adicionada trilha global de exploracao complementar no shell para expor `head-to-head`, `market`, `coaches` e `landing` em todas as areas da plataforma;
- adicionada secao visivel na home executiva para tirar essas superficies do rodape e enquadra-las como modulos secundarios reais.

Arquivos alterados:
- `frontend/src/shared/utils/context-routing.ts`
- `frontend/src/app/(platform)/PlatformShell.tsx`
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual do runtime em `3001`:
  - home com secao visivel `Superficies publicas secundarias`;
  - shell com rail `Exploracao complementar` em pagina interna (`/matches`);
- antes da correcao, descoberta dessas superficies dependia de footer/deep link; depois da correcao, elas ficaram expostas globalmente no shell e explicitadas na home.

Riscos residuais:
- esta correcao fecha descoberta e IA publica; nao substitui o refinamento pagina por pagina do restante do nucleo vivo.

### Bloco 11 - Competition hub

Status:
- `COMPLETO`

Antes:
- hub correto semanticamente, mas visualmente curto, pouco hierarquizado e com distribuicao fraca entre temporadas e saídas;
- leitura da página parecia mais um bloco administrativo do que um hub forte de produto.

Mudancas aplicadas:
- hero reformulado com identidade da competicao, papel claro da superficie e leitura lateral de escopo;
- lista de temporadas reforçada com card prioritario para a temporada atual;
- coluna de entradas canonicas refeita para calendario, tabela e rankings da temporada ativa;
- narrativa visual reforçada para deixar explícito que a profundidade real está na temporada.

Arquivos alterados:
- `frontend/src/features/competitions/components/CompetitionHubContent.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual em `artifacts/core-visual-final-20260330/competition-hub.png`.

Riscos residuais:
- a qualidade final do hub ainda depende da riqueza estrutural disponível por temporada.

### Bloco 12 - Season hub

Status:
- `COMPLETO`

Antes:
- season hub já era funcional, mas o topo ainda parecia uma variação discreta de outras páginas;
- o papel central da temporada no produto não estava forte o bastante na composição inicial.

Mudancas aplicadas:
- hero redesenhado para explicitar aba ativa, formato competitivo e função da temporada como âncora canônica;
- trilha de saídas canônicas refeita para partidas, rankings, times e jogadores;
- painel lateral passou a comunicar recorte ativo e função da superfície com mais clareza.

Arquivos alterados:
- `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual em `artifacts/core-visual-after-20260330/season-hub.png`.

Riscos residuais:
- abas internas densas seguem dependentes da qualidade dos payloads estruturais da temporada.

### Bloco 13 - Matches

Status:
- `COMPLETO`

Antes:
- a página já tinha base visual boa, mas o fluxo entre calendário e outras áreas da temporada ainda ficava disperso;
- a composição acima da lista era funcional, sem hierarquia forte entre leitura atual e próximos passos.

Mudancas aplicadas:
- integração visual sistêmica com o novo frame contextual;
- adicionadas saídas claras para temporada, times, jogadores e rankings dentro do fluxo da lista de partidas;
- reforçada a leitura de produto entre agenda, contexto e profundidade analítica.

Arquivos alterados:
- `frontend/src/app/(platform)/matches/page.tsx`
- `frontend/src/shared/components/navigation/PlatformShellFrame.tsx`
- `frontend/src/shared/components/profile/ProfileRouteCard.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual em `artifacts/core-visual-after-20260330/matches.png`.

Riscos residuais:
- cards de navegação complementar ficam abaixo do primeiro viewport em telas mais baixas; o ganho principal no topo veio do frame contextual.

### Bloco 14 - Teams

Status:
- `COMPLETO`

Antes:
- a página de times já possuía hero e lista consistentes, mas ainda com pouca amarração visual para o resto do núcleo vivo;
- faltava um bloco explícito de progressão para partidas, jogadores, rankings e temporada.

Mudancas aplicadas:
- adicionada faixa de saídas canônicas e comparativas;
- reforçada a coerência visual com o restante do núcleo vivo via frame contextual remodelado;
- mantida a estrutura de lista, mas com mais clareza de fluxo entre descoberta e aprofundamento.

Arquivos alterados:
- `frontend/src/features/teams/components/TeamsPageContent.tsx`
- `frontend/src/shared/components/navigation/PlatformShellFrame.tsx`
- `frontend/src/shared/components/profile/ProfileRouteCard.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual em `artifacts/core-visual-after-20260330/teams.png`.

Riscos residuais:
- a lista continua muito dependente da qualidade do payload resumido por time.

### Bloco 15 - Players

Status:
- `COMPLETO`

Antes:
- a página já tinha hero robusto e tabela forte, mas o encadeamento com temporada, rankings, times e partidas ainda não estava evidente o bastante;
- o topo visível do produto dependia excessivamente do frame antigo.

Mudancas aplicadas:
- frame contextual remodelado elevando hierarquia e orientação;
- adicionadas saídas explícitas para temporada, rankings, times e partidas;
- mantida a tabela forte e o comparativo, com contexto mais claro logo acima da área de filtro.

Arquivos alterados:
- `frontend/src/app/(platform)/players/page.tsx`
- `frontend/src/shared/components/navigation/PlatformShellFrame.tsx`
- `frontend/src/shared/components/profile/ProfileRouteCard.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual em `artifacts/core-visual-final-20260330/players.png`.

Riscos residuais:
- o hero próprio da página continua abaixo do frame e do filtro global; a melhoria principal de topo ficou concentrada na camada contextual.

### Bloco 16 - Rankings

Status:
- `COMPLETO`

Antes:
- a página já tinha boa densidade analítica, mas ainda repetia demais a estrutura anterior e comunicava pouco a progressão para outras áreas do produto;
- faltava reforço visual no topo da experiência.

Mudancas aplicadas:
- frame contextual remodelado com recorte ativo e orientação;
- adicionadas saídas visíveis para temporada, jogadores, times e partidas;
- a família de rankings ficou mais claramente integrada ao restante do núcleo vivo.

Arquivos alterados:
- `frontend/src/features/rankings/components/RankingTable.tsx`
- `frontend/src/shared/components/navigation/PlatformShellFrame.tsx`
- `frontend/src/shared/components/profile/ProfileRouteCard.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual em `artifacts/core-visual-final-20260330/rankings.png`.

Riscos residuais:
- a tabela de ranking segue visualmente forte, mas ainda há espaço para uma wave futura específica em densidade tabular e microinterações.

### Bloco 17 - Global Filter Bar

Status:
- `COMPLETO`

Antes:
- o filtro global compartilhado misturava hero, resumo, ajuda e campos na mesma caixa, com peso visual alto e pouca hierarquia de uso;
- o recorte ativo aparecia como uma sequência de pills sem estrutura clara entre contexto principal e leitura temporal;
- o layout criava muito ruído para um componente que se repete em várias páginas do núcleo vivo.

Mudancas aplicadas:
- refeito o `GlobalFilterBar` do zero no plano visual, preservando a mesma store e a mesma sincronização com a URL;
- o componente passou a separar claramente `Escopo` e `Leitura`, com campos agrupados por papel em vez de uma única malha indiferenciada;
- substituído o hero pesado por um resumo compacto do recorte ativo com chips mais leves e ação de reset mais discreta;
- janela temporal reorganizada em cards próprios para `Últimas partidas` e `Período`, com estado ativo explícito e leitura mais previsível;
- corrigido o rótulo de status temporal para não indicar `Últimas partidas` quando o estado real é neutro ou quando a leitura está em `Rodada fixa`.

Arquivos alterados:
- `frontend/src/shared/components/filters/GlobalFilterBar.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- captura visual do runtime em:
  - `artifacts/filter-redesign-20260330/players-filter.png`
  - `artifacts/filter-redesign-20260330/matches-filter.png`

Riscos residuais:
- o componente continua propositalmente completo em desktop; se houver uma wave futura só de densidade, o próximo alvo natural é reduzir ainda mais a altura da coluna de leitura em larguras médias;
- a validação visual foi feita em páginas do núcleo vivo com dados reais do mock/runtime local, sem percorrer todas as combinações possíveis de filtros.

### Bloco 18 - Matches Pagination

Status:
- `COMPLETO`

Antes:
- a rota `/matches` usava a coleção completa retornada pelo hook e despejava todos os cards na lista;
- o bloco de calendário não tinha paginação local, então a página podia renderizar dezenas ou centenas de partidas de uma vez;
- a linguagem da interface ainda tratava o total do recorte como se tudo estivesse visível ao mesmo tempo.

Mudancas aplicadas:
- limitada a lista de partidas para `30` cards por página;
- adicionada navegação paginada dentro do próprio bloco da lista, com `Anterior`, `Próxima` e páginas numéricas;
- implementado reset para página `1` sempre que busca, status, ordenação ou filtros globais mudam;
- ajustada a linguagem dos KPIs para diferenciar `jogos no recorte` do subconjunto exibido na página atual.

Arquivos alterados:
- `frontend/src/app/(platform)/matches/page.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- validação runtime em `http://127.0.0.1:3001/matches` registrou:
  - `cardsPage1: 30`
  - `hasNextButton: true`
  - `cardsPage2: 30`
  - `pageTwoActive: "page"`
- artefatos:
  - `artifacts/matches-pagination-20260330/runtime-check.json`
  - `artifacts/matches-pagination-20260330/matches-page-1.png`
  - `artifacts/matches-pagination-20260330/matches-page-2.png`

Riscos residuais:
- a consulta continua carregando o conjunto completo e a paginação é aplicada na camada de interface; isso resolve leitura e renderização da página, mas não reduz tráfego de rede;
- se a próxima wave priorizar performance de dados, o passo seguinte é migrar essa paginação para consumo paginado do endpoint já existente.

### Bloco 19 - Archive Summary Naming

Status:
- `COMPLETO`

Antes:
- alguns pontos da home e do shell misturavam nomenclaturas de acervo, usando `Ligas` para um catálogo que inclui ligas e copas;
- o total de `archiveSummary.seasons` aparecia apenas como `Temporadas`, o que abria margem para leitura errada como se fosse total de competições.

Mudancas aplicadas:
- normalizado `Ligas` para `Competições` no shell;
- normalizado `Temporadas` para `Temporadas / edições` nos pontos onde o dado de acervo aparece como métrica resumida;
- ajustada a frase-resumo da home para `competições` e `temporadas/edições`, alinhando a linguagem ao contrato real.

Arquivos alterados:
- `frontend/src/app/(platform)/PlatformShell.tsx`
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- confirmação de base canônica:
  - `archiveSummary.seasons` vem do `/api/v1/home`;
  - `SUPPORTED_COMPETITIONS` no frontend canônico contém `11` competições.

Riscos residuais:
- o número `52` continua dependente do payload do `/api/v1/home`; a mudança aqui corrige a semântica da UI, não o cálculo do backend.

### Bloco 20 - Canonical Coverage Summary

Status:
- `COMPLETO`

Antes:
- a home e o shell ainda comunicavam cobertura a partir do número bruto de `archiveSummary.seasons`;
- isso era semanticamente ambíguo e podia sugerir dezenas de temporadas de uma única competição, em vez do recorte canônico público realmente suportado pelo produto.

Mudancas aplicadas:
- a comunicação de cobertura passou a usar a base canônica do frontend, não o total bruto de temporadas indexadas no payload;
- `Competições` agora reflete `SUPPORTED_COMPETITIONS.length`;
- `Temporadas` agora reflete a contagem única de buckets de temporada suportados (`2024`, `2023`, `2022`, `2021`, `2020`);
- a frase-resumo da home foi normalizada para `11 competições em 5 temporadas`.

Arquivos alterados:
- `frontend/src/config/seasons.registry.ts`
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
- `frontend/src/app/(platform)/PlatformShell.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- confirmação de base canônica:
  - `SUPPORTED_COMPETITIONS.length = 11`
  - `SUPPORTED_SEASON_COVERAGE_COUNT = 5`

Riscos residuais:
- o backend continua retornando o total bruto em `archiveSummary.seasons`; a UI agora deliberadamente não usa esse número para comunicar cobertura pública.

### Bloco 21 - Closed Seasons Copy

Status:
- `COMPLETO`

Antes:
- mesmo após sair do número bruto do payload, a UI ainda dizia apenas `5 temporadas`, o que seguia ambíguo sobre o tipo de cobertura;
- a leitura correta do produto é `últimas 5 temporadas fechadas` por competição.

Mudancas aplicadas:
- home e shell passaram a comunicar `Temporadas fechadas`;
- a métrica da home agora explicita `últimas 5 por competição`;
- a frase principal da home foi normalizada para `11 competições com as últimas 5 temporadas fechadas`.

Arquivos alterados:
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
- `frontend/src/app/(platform)/PlatformShell.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- pontos ajustados:
  - `HomeExecutivePage.tsx:652`
  - `HomeExecutivePage.tsx:654`
  - `HomeExecutivePage.tsx:700`
  - `PlatformShell.tsx:438`

Riscos residuais:
- esta rodada corrige a comunicação pública; ela não revisa o registry detalhado de anos/labels por calendário.

### Bloco 22 - Season Calendar Load + Team Filter

Status:
- `COMPLETO`

Antes:
- o calendário da temporada no season hub abria a coleção inteira de partidas, chegando a renderizar `380` jogos de uma vez no Brasileirão;
- o filtro global não oferecia recorte por clube;
- a visão inicial carregava mais dados do que o necessário para a leitura primária da temporada.

Mudancas aplicadas:
- o calendário do season hub passou a buscar/renderizar apenas `10` partidas por padrão, equivalente à leitura de uma rodada;
- adicionado filtro global de `Clube` no `GlobalFilterBar`, alimentado pela lista real de times da competição/temporada;
- ao selecionar um clube, o calendário da temporada troca para a temporada inteira desse time e limpa `rodada` e `janela` para evitar combinação ambígua;
- o hook `useMatchesList` passou a aceitar `teamId` vindo do estado global do filtro;
- a linguagem do calendário foi ajustada para diferenciar visão padrão (`Última rodada da temporada`) de visão filtrada (`Partidas do clube na temporada`).

Arquivos alterados:
- `frontend/src/shared/types/filters.types.ts`
- `frontend/src/shared/stores/globalFilters.store.ts`
- `frontend/src/shared/hooks/useGlobalFilters.ts`
- `frontend/src/shared/components/filters/GlobalFilterBar.tsx`
- `frontend/src/features/matches/hooks/useMatchesList.ts`
- `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`

Validacao objetiva:
- `pnpm typecheck`;
- `pnpm lint`;
- validação runtime em `http://127.0.0.1:3001/competitions/brasileirao_a/seasons/2025`:
  - `defaultMatchLinks: 10`
  - `hasTeamSelect: true`
  - `selectedTeamLabel: "Atlético Mineiro"`
  - `filteredMatchLinks: 38`
- evidência de rede:
  - lista padrão: `/api/v1/matches?competitionId=71&seasonId=2025&page=1&pageSize=10...`
  - clube filtrado: `/api/v1/matches?competitionId=71&seasonId=2025&teamId=3427&page=1&pageSize=100...`
- artefatos:
  - `artifacts/season-calendar-team-filter-20260330/runtime-check.json`
  - `artifacts/season-calendar-team-filter-20260330/season-calendar.png`

Riscos residuais:
- o filtro global de clube foi conectado ao domínio de partidas; outras superfícies que não consomem `teamId` ainda não foram estendidas nesta rodada;
- o calendário padrão agora limita a `10` em qualquer season hub, o que é correto para leitura inicial e evita carga excessiva, mas não tenta inferir tamanhos diferentes por formato de competição.

### Bloco 23 - Canonical Season Catalog Cleanup

Status:
- `COMPLETO`

Antes:
- o catálogo anual global ainda expunha `2020`, apesar de o backend retornar `0` partidas para essa temporada nas competições anuais suportadas;
- a remoção de `2020` apenas da lista estática não seria suficiente, porque a resolução dinâmica de rota ainda aceitaria `seasonId=2020`;
- `supercopa_do_brasil` precisava permanecer listada como exceção, mesmo sem dados fechados no momento.

Mudancas aplicadas:
- removida a temporada anual `2020` de `SUPPORTED_SEASONS`, eliminando a menção global no filtro sem competição;
- adicionados `supportedSeasonQueryIds` explícitos para as competições anuais canônicas com dado fechado:
  - `brasileirao_a`
  - `brasileirao_b`
  - `libertadores`
  - `copa_do_brasil`
- preservada `supercopa_do_brasil` como exceção com `supportedSeasonQueryIds: ["2025"]`;
- atualizado o teste de roteamento para refletir o catálogo anual canônico e garantir que `2020` não resolva mais como temporada válida.

Arquivos alterados:
- `frontend/src/config/competitions.registry.ts`
- `frontend/src/config/seasons.registry.ts`
- `frontend/src/shared/utils/context-routing.test.ts`

Validacao objetiva:
- backend consultado com `pageSize=1`:
  - `brasileirao_a 2020 => totalCount 0`
  - `brasileirao_b 2020 => totalCount 0`
  - `libertadores 2020 => totalCount 0`
  - `copa_do_brasil 2020 => totalCount 0`
  - `supercopa_do_brasil 2025 => totalCount 0` mantida como exceção de catálogo
- `pnpm typecheck`
- `pnpm lint`
- `pnpm test -- --run src/shared/utils/context-routing.test.ts`

Riscos residuais:
- o ajuste foi intencionalmente restrito ao catálogo canônico do frontend; ele não altera disponibilidade real no backend nem faz auditoria completa de outras combinações fora do catálogo suportado.
