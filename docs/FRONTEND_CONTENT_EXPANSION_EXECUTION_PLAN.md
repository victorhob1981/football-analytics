# Frontend Content Expansion Execution Plan

Data da analise: 2026-03-27

Escopo desta rodada:
- planejamento tecnico-operacional;
- nenhuma implementacao de frontend ou BFF;
- nenhuma alteracao de codigo de produto;
- consolidacao do direcionamento de `docs/analise de conteudo pouco aproveitado.md` com base no estado real do repositorio.

Fontes principais usadas nesta analise:
- `docs/analise de conteudo pouco aproveitado.md`
- `docs/FRONTEND_IMPLEMENTATION_STATUS.md`
- `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- `docs/FRONTEND_ARCHITECTURE.md`
- `docs/FRONTEND_REFERENCE_INVENTORY.md`
- rotas e componentes reais em `frontend/src/app`, `frontend/src/features` e `api/src/routers`

# 1. Diagnostico do estado atual

## 1.1 Mapa das superficies atuais

### Competition hub
- Evidencia:
  - `frontend/src/features/competitions/components/CompetitionHubContent.tsx:100` comunica explicitamente que a tela serve para entrar em temporadas e ir para calendario, tabela e rankings.
  - `frontend/src/features/competitions/components/CompetitionHubContent.tsx:132` e `:192` reforcam a tela como distribuidor de temporadas, nao como superficie analitica profunda.
- Leitura estrutural:
  - esta superficie ja esta correta como hub de distribuicao;
  - nao deve absorver os conteudos do `.md` que pedem leitura analitica profunda.

### Season hub
- Evidencia:
  - `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx:68` define apenas tres abas canonicas: `calendar`, `standings`, `rankings`.
  - `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx:1077` resume a tela como acompanhamento de partidas, tabela e rankings da temporada.
- Leitura estrutural:
  - esta e a superficie canonica da temporada;
  - deve continuar sendo o ponto de entrada para calendario, standings e rankings;
  - nao deve virar deposito de H2H, mercado, coaches ou availability global.

### Match center
- Evidencia:
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:47` define `summary`, `timeline`, `lineups`, `team-stats`, `player-stats`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:667`, `:680`, `:693` e `:702` ja renderizam essas abas.
  - `api/src/routers/matches.py:430` expoe `GET /api/v1/matches/{matchId}`.
  - `api/src/routers/matches.py:445-448` ja aceitam `includeTimeline`, `includeLineups`, `includeTeamStats`, `includePlayerStats`.
- Leitura estrutural:
  - o match center ja e a superficie canonica para timeline, lineups e stats por partida;
  - o ganho futuro aqui e densidade de produto, nao nova arquitetura de rota.

### Team profile
- Evidencia:
  - `frontend/src/features/teams/components/TeamProfileContent.tsx:41` define `overview`, `journey`, `squad`, `matches`, `stats`.
  - `frontend/src/features/teams/components/TeamProfileContent.tsx:325` renderiza a aba `squad`.
  - `frontend/src/features/teams/components/TeamSquadSection.tsx` ja entrega base de elenco com minutos, aparicoes, titularidade e ultima aparicao.
  - `api/src/routers/teams.py:451` expoe `GET /api/v1/teams/{teamId}`.
  - `api/src/routers/teams.py:467-468` ja aceitam `includeSquad` e `includeStats`.
- Leitura estrutural:
  - o team profile tem estrutura pronta para absorver melhor cobertura de elenco;
  - availability/sidelined pertence naturalmente aqui primeiro, dentro de `squad`.

### Player profile
- Evidencia:
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx:44` define `overview`, `history`, `matches`, `stats`.
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx:399` e `:412` ja renderizam `history` e `stats`.
  - `api/src/routers/players.py:479` expoe `GET /api/v1/players/{playerId}`.
  - `api/src/routers/players.py:495-496` ja aceitam `includeHistory` e `includeStats`.
- Leitura estrutural:
  - player season statistics deve ser aprofundado aqui;
  - nao ha sinal de que uma pagina nova de stats de jogador seja necessaria.

### Rankings
- Evidencia:
  - `frontend/src/app/(platform)/rankings/[rankingType]/page.tsx` ja existe como rota dedicada.
  - `frontend/src/config/ranking.registry.ts:10` publica rankings reais para jogadores e times.
  - `api/src/routers/rankings.py:26` centraliza o contrato em `RANKING_CONFIG`.
  - `api/src/routers/rankings.py:41-44` marca `player-pass-accuracy` como `unsupported`.
- Leitura estrutural:
  - a superficie certa ja existe;
  - o gargalo nao e navegacao; e breadth de metricas e aderencia entre frontend e BFF.

### Home executiva
- Evidencia:
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx:568` ja possui bloco de curadoria editorial.
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx:575-577` consome `editorialHighlights`.
  - `api/src/routers/home.py:309` define `candidate_contexts`.
  - `api/src/routers/home.py:373` limita a curadoria a `limit 2`.
- Leitura estrutural:
  - a home ja tem o shape certo para curadoria;
  - o problema atual e profundidade e variedade, nao ausencia de superficie.

### Superficies dedicadas rasas ou placeholder
- Evidencia:
  - `frontend/src/app/(platform)/market/page.tsx:18` diz que a area de mercado ainda nao entrou na experiencia publica.
  - `frontend/src/app/(platform)/market/page.tsx:37` exibe `Mercado indisponivel`.
  - `frontend/src/app/(platform)/head-to-head/page.tsx:37` exibe `Comparativo indisponivel`.
  - `frontend/src/app/(platform)/coaches/[coachId]/page.tsx:39` exibe `Perfil de tecnico indisponivel`.
  - `frontend/src/app/(marketing)/landing/page.tsx:4-5` ainda e placeholder/TODO.
- Leitura estrutural:
  - estas sao as unicas superficies do `.md` que realmente pedem criacao ou fechamento de paginas dedicadas.

### Navegacao shell
- Evidencia:
  - `frontend/src/shared/components/navigation/usePlatformShellState.ts:533` reconhece `/market`.
  - `frontend/src/shared/components/navigation/usePlatformShellState.ts:548` reconhece `/head-to-head`.
  - `frontend/src/shared/components/navigation/usePlatformShellState.ts:568` reconhece `/coaches/`.
- Leitura estrutural:
  - o shell ja considera essas areas como destinos reais;
  - ainda falta o indice `/coaches`.

## 1.2 O que ja existe de relevante e deve ser reaproveitado

- Match center, team profile, player profile, rankings e home executiva ja possuem rotas, hooks, services e tipos reais.
- A camada de dados do frontend para esses dominios ja esta organizada por feature:
  - `frontend/src/features/matches/*`
  - `frontend/src/features/teams/*`
  - `frontend/src/features/players/*`
  - `frontend/src/features/rankings/*`
  - `frontend/src/features/home/*`
- O BFF atual ja sustenta:
  - match center via `api/src/routers/matches.py`
  - team profile via `api/src/routers/teams.py`
  - player profile via `api/src/routers/players.py`
  - rankings via `api/src/routers/rankings.py`
  - home via `api/src/routers/home.py`

## 1.3 O que esta raso, ausente ou subaproveitado

- `market`, `head-to-head`, `coaches/[coachId]` e `landing` existem como rota, mas ainda nao como produto.
- Nao existe rota `frontend/src/app/(platform)/coaches/page.tsx`.
- Nao existe rota global de `availability`; o unico encaixe coerente hoje e a aba `squad` do team profile.
- A home editorial existe, mas a composicao ainda e curta e controlada por selecao fixa no backend.
- Rankings possuem rota e registry, mas varias metricas disponiveis no acervo ainda nao estao fechadas ponta a ponta.

## 1.4 O que ja suporta expansao sem reabrir arquitetura

- Match center: suporte forte para aprofundar tabs existentes sem mexer em roteamento.
- Team profile: suporte forte para aprofundar `squad` e `stats`.
- Player profile: suporte forte para aprofundar `history` e `stats`.
- Rankings: suporte medio para ampliar metricas, com dependencia de BFF.
- Home: suporte medio para ampliar curadoria, com dependencia de composicao no BFF.

## 1.5 O que esta estruturalmente inadequado

- Divergencia entre documentacao antiga e app vivo:
  - `docs/FRONTEND_ARCHITECTURE.md:69`, `:72-74` ainda falam em `/h2h` e `/more/*`.
  - `docs/FRONTEND_REFERENCE_INVENTORY.md:29-32` tambem mantem esse namespace antigo.
  - o frontend vivo usa `/head-to-head`, `/market` e `/coaches/[coachId]`.
- Search global ainda nao cobre tecnico:
  - `api/src/routers/search.py:20` e `:22` limitam `SearchType` a `competition`, `team`, `player`, `match`.
- Nao ha routers dedicados hoje para:
  - head-to-head
  - market/transfers
  - coaches
  - availability
  Evidencia indireta: `api/src/routers` hoje contem apenas `competition_hub.py`, `health.py`, `home.py`, `insights.py`, `matches.py`, `players.py`, `rankings.py`, `search.py`, `standings.py`, `teams.py`.

# 2. Matriz por dominio/conteudo

## 2.1 Mercado / Transfers
- Dominio/conteudo: mercado e transferencias de jogadores
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` define `Mercado / Transfers` como nova pagina dedicada em `/market`.
  - `frontend/src/app/(platform)/market/page.tsx:18` e `:37` mostram rota existente em estado placeholder.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:485-507` registra `player_transfers` como dado disponivel hoje, com consumo em consolidacao e modulo de mercado ainda nao maduro no frontend.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:720` afirma explicitamente que transfers dependem de contrato BFF.
- Destino recomendado: nova pagina
- Rota alvo: `/market`
- Componentes/hooks/types/queries relacionados hoje:
  - pagina atual `frontend/src/app/(platform)/market/page.tsx`
  - nenhum modulo de feature dedicado encontrado em `frontend/src/features`
- Necessidade de contrato/BFF: sim, alta
- Grau de confianca: alto
- Justificativa objetiva:
  - o dado existe;
  - o dominio tem pergunta propria e compartilhavel;
  - nao ha superficie canonica existente que absorva o tema sem fragmentar outra pagina.

## 2.2 Head-to-Head dedicado
- Dominio/conteudo: comparativo historico entre dois times
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` define H2H como nova pagina dedicada em `/head-to-head`.
  - `frontend/src/app/(platform)/head-to-head/page.tsx:37` mostra placeholder.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:313-331` classifica `Head-to-Head` como dado disponivel hoje e bom para modulo dedicado.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:713` resume o dominio como pronto para modulo dedicado.
- Destino recomendado: nova pagina
- Rota alvo: `/head-to-head`
- Componentes/hooks/types/queries relacionados hoje:
  - pagina atual `frontend/src/app/(platform)/head-to-head/page.tsx`
  - link de entrada na home em `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx:498`
  - nenhum modulo de feature dedicado encontrado em `frontend/src/features`
- Necessidade de contrato/BFF: sim, alta
- Grau de confianca: alto
- Justificativa objetiva:
  - H2H e um dominio comparativo autonomo;
  - a pergunta principal nao pertence naturalmente ao season hub nem ao match center;
  - ja existe rota dedicada no app e dado confirmado no acervo.

## 2.3 Tecnicos / Coaches
- Dominio/conteudo: perfis e historico de tecnicos
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` define `Tecnicos / Coaches` como nova pagina dedicada com entrada para `/coaches/[coachId]`.
  - `frontend/src/app/(platform)/coaches/[coachId]/page.tsx:39` esta em placeholder.
  - nao existe `frontend/src/app/(platform)/coaches/page.tsx`.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:438-455` registra `team_coaches` como dado disponivel hoje com consumo em consolidacao.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:718` marca coaches como dominio pronto para evolucao de produto.
  - `api/src/routers/search.py:20-22` mostra que search global ainda nao cobre coach.
- Destino recomendado: nova pagina
- Rota alvo: `/coaches` com detalhe em `/coaches/[coachId]`
- Componentes/hooks/types/queries relacionados hoje:
  - pagina atual `frontend/src/app/(platform)/coaches/[coachId]/page.tsx`
  - shell reconhece `/coaches/` em `frontend/src/shared/components/navigation/usePlatformShellState.ts:568`
  - nenhum modulo de feature dedicado encontrado em `frontend/src/features`
- Necessidade de contrato/BFF: sim, alta
- Grau de confianca: alto para o dominio; medio para o desenho exato do detalhe
- Justificativa objetiva:
  - tecnico e dominio proprio do produto;
  - falta inclusive a rota indice;
  - integrar isso como bloco solto em team page geraria navegacao fragmentada e impediria exploracao portfolio-wide.

## 2.4 Landing editorial / institucional
- Dominio/conteudo: apresentacao institucional/editorial do produto
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` define `/landing` como pagina dedicada.
  - `frontend/src/app/(marketing)/landing/page.tsx:4-5` ainda e placeholder.
  - `docs/FRONTEND_IMPLEMENTATION_STATUS.md:177` distingue explicitamente home executiva de home editorial/institucional.
- Destino recomendado: nova pagina
- Rota alvo: `/landing`
- Componentes/hooks/types/queries relacionados hoje:
  - pagina atual `frontend/src/app/(marketing)/landing/page.tsx`
  - nenhum modulo dedicado de marketing alem do layout base
- Necessidade de contrato/BFF: baixa a media
- Grau de confianca: alto
- Justificativa objetiva:
  - landing e por natureza uma superficie separada do app analitico;
  - mantem a home executiva focada em produto.

## 2.5 Disponibilidade / Sidelined
- Dominio/conteudo: indisponiveis e ausencias do elenco
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]?tab=squad`.
  - `frontend/src/features/teams/components/TeamProfileContent.tsx:41` ja possui aba `squad`.
  - `frontend/src/features/teams/components/TeamSquadSection.tsx` ja concentra leitura de elenco.
  - `frontend/src/features/teams/types/teams.types.ts` nao possui tipo `availability`.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:461-479` registra `team_sidelined` como dado disponivel hoje, com consumo em consolidacao.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:719` classifica sidelined como dado de alto valor para contexto de lineup.
  - `docs/FRONTEND_ARCHITECTURE.md:299` e `docs/FRONTEND_REFERENCE_INVENTORY.md:32` ja apontavam availability primeiro como secao do squad, depois tela global.
- Destino recomendado: pagina existente -> secao interna da aba `squad`
- Rota alvo: `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]?tab=squad`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/features/teams/components/TeamProfileContent.tsx`
  - `frontend/src/features/teams/components/TeamSquadSection.tsx`
  - `frontend/src/features/teams/hooks/useTeamProfile.ts`
  - `frontend/src/features/teams/services/teams.service.ts`
  - `frontend/src/features/teams/types/teams.types.ts`
  - `api/src/routers/teams.py`
- Necessidade de contrato/BFF: sim, media a alta
- Grau de confianca: alto
- Justificativa objetiva:
  - availability sem contexto de elenco perde valor;
  - o time profile ja e a superficie canonica;
  - abrir tela global agora aumentaria fragmentacao antes de fechar o caso primario.

## 2.6 Match Statistics de time
- Dominio/conteudo: estatisticas comparativas por partida no nivel do time
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `/matches/[matchId]?tab=team-stats`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:47` ja define a aba `team-stats`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:702` ja a renderiza.
  - `api/src/routers/matches.py:445-448` ja aceita `includeTeamStats`.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:331-332` registram match statistics como dado disponivel hoje e com espaco para UX mais rica.
- Destino recomendado: pagina existente -> tab interna
- Rota alvo: `/matches/[matchId]?tab=team-stats`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx`
  - `frontend/src/features/matches/components/MatchTeamStatsPlaceholder.tsx`
  - `frontend/src/features/matches/hooks/useMatchCenter.ts`
  - `frontend/src/features/matches/services/matches.service.ts`
  - `frontend/src/features/matches/types/matches.types.ts`
- Necessidade de contrato/BFF: baixa para V1 de densificacao; media se a UX pedir agregacoes novas
- Grau de confianca: alto
- Justificativa objetiva:
  - a pergunta e inerentemente contextual a uma partida;
  - outra rota so duplicaria o match center.

## 2.7 Lineups
- Dominio/conteudo: escalacoes e formacoes por partida
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `/matches/[matchId]?tab=lineups`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:47` inclui `lineups`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:680` renderiza essa aba.
  - `api/src/routers/matches.py:445-448` ja aceita `includeLineups`.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:184` confirma `raw.fixture_lineups` no banco.
- Destino recomendado: pagina existente -> tab interna
- Rota alvo: `/matches/[matchId]?tab=lineups`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/features/matches/components/MatchLineupsPlaceholder.tsx`
  - `frontend/src/features/matches/hooks/useMatchCenter.ts`
  - `frontend/src/features/matches/types/matches.types.ts`
- Necessidade de contrato/BFF: baixa
- Grau de confianca: alto
- Justificativa objetiva:
  - lineup so faz sentido no contexto do jogo;
  - o contrato ja existe.

## 2.8 Match Events / Timeline
- Dominio/conteudo: cronologia do jogo
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `/matches/[matchId]?tab=timeline`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:47` inclui `timeline`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:667` renderiza a aba.
  - `api/src/routers/matches.py:445-448` ja aceita `includeTimeline`.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:388-405` e `:132` confirmam `match_events` disponivel hoje.
- Destino recomendado: pagina existente -> tab interna
- Rota alvo: `/matches/[matchId]?tab=timeline`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/features/matches/components/MatchTimelinePlaceholder.tsx`
  - `frontend/src/features/matches/hooks/useMatchCenter.ts`
  - `frontend/src/features/matches/types/matches.types.ts`
- Necessidade de contrato/BFF: baixa
- Grau de confianca: alto
- Justificativa objetiva:
  - timeline e leitura de partida, nao dominio proprio.

## 2.9 Fixture Player Statistics
- Dominio/conteudo: desempenho individual por partida
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `/matches/[matchId]?tab=player-stats`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:47` inclui `player-stats`.
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx:693` renderiza essa aba.
  - `api/src/routers/matches.py:445-448` ja aceita `includePlayerStats`.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:413-430` e `:133` confirmam `fixture_player_statistics` disponivel hoje.
- Destino recomendado: pagina existente -> tab interna
- Rota alvo: `/matches/[matchId]?tab=player-stats`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/features/matches/components/MatchPlayerStatsPlaceholder.tsx`
  - `frontend/src/features/matches/hooks/useMatchCenter.ts`
  - `frontend/src/features/matches/types/matches.types.ts`
- Necessidade de contrato/BFF: baixa
- Grau de confianca: alto
- Justificativa objetiva:
  - o dado responde pergunta de jogo a jogo;
  - o match center ja e a tela certa.

## 2.10 Player Season Statistics
- Dominio/conteudo: estatisticas acumuladas do jogador na temporada/contexto
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]?tab=stats`.
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx:44` inclui `stats`.
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx:412` renderiza `PlayerStatsSection`.
  - `api/src/routers/players.py:495-496` ja aceita `includeStats`.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:363-380` e `:131` confirmam `player_season_statistics` disponivel hoje.
- Destino recomendado: pagina existente -> tab interna
- Rota alvo: `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]?tab=stats`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`
  - `frontend/src/features/players/components/PlayerStatsSection.tsx`
  - `frontend/src/features/players/hooks/usePlayerProfile.ts`
  - `frontend/src/features/players/services/players.service.ts`
  - `frontend/src/features/players/types/players.types.ts`
- Necessidade de contrato/BFF: baixa para aprofundar leitura atual; media se entrar comparativo novo
- Grau de confianca: alto
- Justificativa objetiva:
  - o player profile ja e a superficie canonica;
  - a necessidade aqui e enriquecer interpretacao e composicao.

## 2.11 Rankings por metricas
- Dominio/conteudo: rankings portfolio-wide por metricas alem do conjunto atual
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda usar `/rankings/[rankingType]`.
  - `frontend/src/app/(platform)/rankings/[rankingType]/page.tsx` ja existe.
  - `frontend/src/config/ranking.registry.ts:10` ja publica uma lista inicial de rankings.
  - `frontend/src/config/metrics.registry.ts:53`, `:63`, `:83`, `:104`, `:115` mostram metricas extras no frontend.
  - `api/src/routers/rankings.py:26` concentra suporte real no backend.
  - `api/src/routers/rankings.py:41-44` marca `player-pass-accuracy` como `unsupported`.
- Destino recomendado: pagina existente
- Rota alvo: `/rankings/[rankingType]`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/app/(platform)/rankings/[rankingType]/page.tsx`
  - `frontend/src/features/rankings/components/RankingTable.tsx`
  - `frontend/src/features/rankings/hooks/useRankingTable.ts`
  - `frontend/src/features/rankings/services/rankings.service.ts`
  - `frontend/src/features/rankings/types/rankings.types.ts`
  - `frontend/src/config/ranking.registry.ts`
  - `frontend/src/config/metrics.registry.ts`
- Necessidade de contrato/BFF: sim, media a alta
- Grau de confianca: alto
- Justificativa objetiva:
  - a tela ja existe e ja e canonicamente correta;
  - o que falta e cobertura real de rankingType, nao nova navegacao.

## 2.12 Curadoria da home
- Dominio/conteudo: curadoria editorial de entrada no produto
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `/`.
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx:568-577` ja possui bloco de curadoria.
  - `frontend/src/features/home/services/home.service.ts` ja consome `/api/v1/home`.
  - `api/src/routers/home.py:309`, `:373`, `:424` mostram que a home ja retorna `editorialHighlights`, mas com curadoria curta.
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md:721` marca home portfolio executiva como disponivel com caveat.
- Destino recomendado: pagina existente
- Rota alvo: `/`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
  - `frontend/src/features/home/hooks/useHomePage.ts`
  - `frontend/src/features/home/services/home.service.ts`
  - `frontend/src/features/home/types/home.types.ts`
  - `api/src/routers/home.py`
- Necessidade de contrato/BFF: media
- Grau de confianca: alto
- Justificativa objetiva:
  - a home ja e a superficie de entrada;
  - criar outra pagina editorial quebraria o fluxo do app principal.

## 2.13 Cobertura de elenco no perfil de time
- Dominio/conteudo: quem mais jogou, recorrencia, distribuicao de minutos e profundidade do elenco
- Evidencia encontrada:
  - `docs/analise de conteudo pouco aproveitado.md` manda agregar em `?tab=squad`.
  - `frontend/src/features/teams/components/TeamProfileContent.tsx:41` inclui `squad`.
  - `frontend/src/features/teams/components/TeamSquadSection.tsx` ja mostra parte desta leitura.
  - `api/src/routers/teams.py:467-468` ja aceita `includeSquad`.
- Destino recomendado: pagina existente -> reforco da aba `squad`
- Rota alvo: `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]?tab=squad`
- Componentes/hooks/types/queries relacionados hoje:
  - `frontend/src/features/teams/components/TeamProfileContent.tsx`
  - `frontend/src/features/teams/components/TeamSquadSection.tsx`
  - `frontend/src/features/teams/hooks/useTeamProfile.ts`
  - `frontend/src/features/teams/types/teams.types.ts`
- Necessidade de contrato/BFF: baixa a media
- Grau de confianca: alto
- Justificativa objetiva:
  - a aba ja existe e ja faz esse trabalho parcialmente;
  - o ganho e aprofundar, nao mover de lugar.

# 3. Plano de execucao em blocos

## Bloco 1 - Densificar o match center sem abrir rotas novas
- Objetivo:
  - transformar `timeline`, `lineups`, `team-stats` e `player-stats` em leitura mais forte dentro da superficie ja canonica.
- Escopo exato:
  - melhorar composicao, resumos, hierarquia de informacao e links entre abas do match center;
  - nao alterar a arquitetura geral de matches nem abrir pagina nova.
- Mudanca visual/composicao:
  - reforcar resumo do jogo com CTAs claros para cada tab;
  - tornar mais evidentes os estados de coverage e as perguntas que cada tab responde;
  - substituir o comportamento de "aba existe mas parece secundaria" por "aba responde uma pergunta clara".
- Mudanca de rota/navegacao:
  - nenhuma rota nova;
  - apenas reforco de navegacao interna por `tab` em `/matches/[matchId]`.
- Rotas afetadas:
  - `/matches/[matchId]`
- Arquivos/componentes mais provaveis:
  - `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx`
  - `frontend/src/features/matches/components/MatchCenterHeader.tsx`
  - `frontend/src/features/matches/components/MatchTimelinePlaceholder.tsx`
  - `frontend/src/features/matches/components/MatchLineupsPlaceholder.tsx`
  - `frontend/src/features/matches/components/MatchTeamStatsPlaceholder.tsx`
  - `frontend/src/features/matches/components/MatchPlayerStatsPlaceholder.tsx`
- Hooks/queries/types:
  - `frontend/src/features/matches/hooks/useMatchCenter.ts`
  - `frontend/src/features/matches/services/matches.service.ts`
  - `frontend/src/features/matches/types/matches.types.ts`
- Contratos/BFF:
  - reaproveitar `GET /api/v1/matches/{matchId}` atual;
  - nenhum endpoint novo obrigatorio para a primeira entrega deste bloco.
- Entregaveis exatos:
  - tabs com leitura mais explicita;
  - resumo de partida apontando para tabs certas;
  - melhor conexao entre match center e perfis de time/jogador;
  - coverage mais visivel por secao.
- Criterios de pronto:
  - cada uma das quatro tabs responde claramente a pergunta definida no `.md`;
  - nao existe pagina secundaria criada para conteudo que ja cabe no match center;
  - nenhum novo contrato e necessario para a V1.
- Riscos:
  - nomenclatura atual de componentes como `Placeholder` pode confundir o escopo tecnico;
  - risco de excesso de informacao se a composicao crescer sem hierarquia.
- Fora de escopo:
  - nova pagina de estatisticas;
  - H2H;
  - integracao com dados adicionais fora do payload atual.
- Por que esse bloco vem nessa ordem:
  - maior ganho visivel com menor risco;
  - contrato e rota ja estao verdes.

## Bloco 2 - Fechar a aba squad do team profile
- Objetivo:
  - fazer a aba `squad` responder profundidade de elenco e indisponibilidades sem abrir modulo paralelo.
- Escopo exato:
  - fortalecer leitura de elenco;
  - incorporar availability/sidelined como secao interna da aba `squad`.
- Mudanca visual/composicao:
  - organizar elenco por uso/recorrencia;
  - destacar titulares recorrentes, banco recorrente e distribuicao de minutos;
  - adicionar secao de indisponiveis com motivo e recencia quando houver contrato.
- Mudanca de rota/navegacao:
  - nenhuma rota nova;
  - permanencia em `/teams/[teamId]?tab=squad` no contexto canonico da competicao/temporada.
- Rotas afetadas:
  - `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]?tab=squad`
- Arquivos/componentes mais provaveis:
  - `frontend/src/features/teams/components/TeamProfileContent.tsx`
  - `frontend/src/features/teams/components/TeamSquadSection.tsx`
  - novo `frontend/src/features/teams/components/TeamAvailabilitySection.tsx`
- Hooks/queries/types:
  - `frontend/src/features/teams/hooks/useTeamProfile.ts`
  - `frontend/src/features/teams/services/teams.service.ts`
  - `frontend/src/features/teams/types/teams.types.ts`
- Contratos/BFF:
  - extensao do `GET /api/v1/teams/{teamId}` atual para incluir availability;
  - preferir novo bloco opcional no mesmo payload, seguindo o padrao de `includeSquad/includeStats`.
- Entregaveis exatos:
  - aba `squad` com leitura real de profundidade;
  - coverage explicita para availability;
  - indisponiveis visiveis no contexto do elenco.
- Criterios de pronto:
  - o usuario consegue responder quem mais jogou, onde ha carencia e quem esta fora sem sair do team profile;
  - nao existe tela global de availability neste bloco.
- Riscos:
  - a base atual de `team_sidelined` no inventario e pequena (`docs/INVENTARIO_DADOS_DO_PROJETO.md:135`), o que exige UX resiliente para vazios;
  - necessidade de alterar contrato BFF sem quebrar consumo atual.
- Fora de escopo:
  - rota global `/availability`;
  - comparativo portfolio-wide de ausencias.
- Por que esse bloco vem nessa ordem:
  - usa uma superficie central ja consolidada;
  - entrega valor alto com extensao localizada de contrato.

## Bloco 3 - Densificar stats e historico do player profile
- Objetivo:
  - transformar `stats` e `history` em leitura mais explicita de temporada e contexto.
- Escopo exato:
  - aprofundar apresentacao das estatisticas sazonais e ligacao com historico recente;
  - nao criar pagina nova de stats de jogador.
- Mudanca visual/composicao:
  - organizar blocos de destaque por perfil/temporada;
  - conectar stats com historico e partidas recentes;
  - reforcar links para rankings e time.
- Mudanca de rota/navegacao:
  - nenhuma rota nova;
  - manter `tab=history` e `tab=stats`.
- Rotas afetadas:
  - `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]?tab=history`
  - `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]?tab=stats`
- Arquivos/componentes mais provaveis:
  - `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`
  - `frontend/src/features/players/components/PlayerStatsSection.tsx`
  - `frontend/src/features/players/components/PlayerHistorySection.tsx`
  - `frontend/src/features/players/components/PlayerMatchesSection.tsx`
- Hooks/queries/types:
  - `frontend/src/features/players/hooks/usePlayerProfile.ts`
  - `frontend/src/features/players/services/players.service.ts`
  - `frontend/src/features/players/types/players.types.ts`
- Contratos/BFF:
  - reaproveitar `GET /api/v1/players/{playerId}` atual com `includeHistory/includeStats`;
  - extensoes pequenas de payload podem ser necessarias, mas nao sao pre-condicao para a V1 do bloco.
- Entregaveis exatos:
  - leitura mais forte de temporada;
  - relacao mais clara entre desempenho agregado e contexto;
  - melhor conexao com rankings e match center.
- Criterios de pronto:
  - a aba `stats` responde "como foi a temporada deste jogador";
  - a aba `history` responde "em quais contextos ele ja apareceu e com qual recencia".
- Riscos:
  - sobreposicao com rankings se a tela virar apenas outra tabela;
  - excesso de detalhe sem hierarquia.
- Fora de escopo:
  - nova pagina dedicada de comparacao de jogadores;
  - novos dominos fora do player profile.
- Por que esse bloco vem nessa ordem:
  - contrato ja existe;
  - reaproveita o que esta mais maduro no frontend atual.

## Bloco 4 - Expandir breadth real de rankings
- Objetivo:
  - ampliar `/rankings/[rankingType]` com metricas que ja existem no ecossistema do projeto.
- Escopo exato:
  - fechar alinhamento entre `ranking.registry.ts`, `metrics.registry.ts` e `api/src/routers/rankings.py`;
  - reforcar entradas para rankings a partir do season hub e perfis.
- Mudanca visual/composicao:
  - melhorar descoberta de metricas relacionadas;
  - tornar a pagina de ranking mais navegavel entre tipos semelhantes.
- Mudanca de rota/navegacao:
  - nenhuma rota nova obrigatoria;
  - ampliar links existentes a partir do season hub, player profile e home.
- Rotas afetadas:
  - `/rankings/[rankingType]`
  - `/competitions/[competitionKey]/seasons/[seasonLabel]?tab=rankings`
- Arquivos/componentes mais provaveis:
  - `frontend/src/config/ranking.registry.ts`
  - `frontend/src/config/metrics.registry.ts`
  - `frontend/src/features/rankings/components/RankingTable.tsx`
  - `frontend/src/features/rankings/hooks/useRankingTable.ts`
  - `frontend/src/features/rankings/services/rankings.service.ts`
  - `frontend/src/features/rankings/types/rankings.types.ts`
  - `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`
- Hooks/queries/types:
  - `useRankingTable`
  - `rankings.service`
  - `rankings.types`
- Contratos/BFF:
  - ajuste real em `api/src/routers/rankings.py`;
  - algumas metricas hoje expostas no frontend nao estao realmente suportadas no BFF.
- Entregaveis exatos:
  - conjunto mais amplo e coerente de rankingType;
  - eliminacao de rotas ou labels publicas sem suporte real;
  - entradas melhores a partir de season hub e perfis.
- Criterios de pronto:
  - cada rankingType publico tem suporte real no backend;
  - os rankings adicionais refletem metricas que ja existem no acervo ou no mart.
- Riscos:
  - tentar publicar metricas so porque aparecem em `metrics.registry.ts` sem fechar o BFF;
  - criar navegacao para rankingType ainda `unsupported`.
- Fora de escopo:
  - rankings de coach;
  - reformulacao completa do conceito de ranking.
- Por que esse bloco vem nessa ordem:
  - aumenta densidade do produto sem abrir novos modulos;
  - depende de BFF, mas em dominio ja maduro.

## Bloco 5 - Fechar o modulo dedicado de head-to-head
- Objetivo:
  - transformar `/head-to-head` de placeholder em modulo analitico real.
- Escopo exato:
  - criar experiencia dedicada de comparativo historico entre dois times;
  - integrar entradas vindas de home, match center e team profile.
- Mudanca visual/composicao:
  - tela focada em saldo historico, ultimos confrontos e recortes relevantes;
  - nao virar um segundo match center.
- Mudanca de rota/navegacao:
  - manter `/head-to-head` como rota propria;
  - definir URL state para time A, time B e contexto de competicao/temporada.
- Rotas afetadas:
  - `/head-to-head`
  - links de entrada vindos de `/`, `/matches/[matchId]` e team profile
- Arquivos/componentes mais provaveis:
  - `frontend/src/app/(platform)/head-to-head/page.tsx`
  - novo modulo em `frontend/src/features/head-to-head/`
  - extensoes pontuais em `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- Hooks/queries/types:
  - novos hooks/types/services dedicados a H2H
- Contratos/BFF:
  - router dedicado novo no BFF;
  - nao ha evidencias de suporte H2H hoje em `api/src/routers`.
- Entregaveis exatos:
  - pagina funcional para comparar dois times;
  - leitura de vantagem historica e recencia;
  - links claros de volta para as partidas do confronto.
- Criterios de pronto:
  - `/head-to-head` deixa de ser placeholder;
  - a tela responde as perguntas do `.md` sem desviar o usuario para telas auxiliares.
- Riscos:
  - sem contrato dedicado, o frontend vira composicao artificial de dados;
  - URL state mal definido pode dificultar compartilhamento.
- Fora de escopo:
  - confrontos multiplos;
  - estatistica evento a evento entre dois times alem do necessario para a V1.
- Por que esse bloco vem nessa ordem:
  - e o primeiro dominio realmente novo mais proximo do nucleo analitico.

## Bloco 6 - Densificar a home executiva
- Objetivo:
  - ampliar a capacidade da home de funcionar como launcher editorial do produto.
- Escopo exato:
  - aumentar densidade de curadoria, entradas para modulos e variedade de contexto;
  - manter a home como superficie de produto, nao institucional.
- Mudanca visual/composicao:
  - mais prateleiras ou blocos editoriais com criterio claro;
  - melhor uso de `editorialHighlights` e entradas para rankings/H2H/temporadas.
- Mudanca de rota/navegacao:
  - nenhuma rota nova;
  - reforco de deep links a partir da home.
- Rotas afetadas:
  - `/`
- Arquivos/componentes mais provaveis:
  - `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
  - `frontend/src/features/home/hooks/useHomePage.ts`
  - `frontend/src/features/home/services/home.service.ts`
  - `frontend/src/features/home/types/home.types.ts`
- Hooks/queries/types:
  - `useHomePage`
  - `home.service`
  - `home.types`
- Contratos/BFF:
  - extensao real em `api/src/routers/home.py` para enriquecer a curadoria;
  - o contrato atual ja existe, mas hoje e curto e fixo.
- Entregaveis exatos:
  - home com mais contexto e melhores portas de entrada;
  - curadoria menos estreita que o `limit 2` atual.
- Criterios de pronto:
  - a home passa a apresentar mais de um tipo de entrada relevante no acervo;
  - a curadoria nao depende de dois itens fixos apenas.
- Riscos:
  - transformar a home em agregado desorganizado;
  - misturar home executiva com landing institucional.
- Fora de escopo:
  - redesign de marketing;
  - substituir `/landing`.
- Por que esse bloco vem nessa ordem:
  - fica melhor depois que match, team, player e rankings estiverem mais densos;
  - assim a home passa a apontar para superficies mais fortes.

## Bloco 7 - Fechar o modulo dedicado de market
- Objetivo:
  - transformar `/market` em dominio de produto real para transferencias.
- Escopo exato:
  - criar feed/lista exploravel de transferencias;
  - conectar o modulo a perfis de jogador e time.
- Mudanca visual/composicao:
  - foco em movimentos, janela, origem, destino e trilha de carreira;
  - nao virar apenas tabela crua sem contexto.
- Mudanca de rota/navegacao:
  - manter `/market` como rota propria;
  - definir filtros basicos de URL para janela, time e direcao se o contrato permitir.
- Rotas afetadas:
  - `/market`
- Arquivos/componentes mais provaveis:
  - `frontend/src/app/(platform)/market/page.tsx`
  - novo modulo em `frontend/src/features/market/`
- Hooks/queries/types:
  - novos hooks/types/services dedicados a transferencias
- Contratos/BFF:
  - router dedicado novo para market/transfers;
  - o inventario confirma dado, mas nao ha router hoje.
- Entregaveis exatos:
  - pagina funcional de mercado;
  - filtros minimos de leitura;
  - ligacao com player/team profile.
- Criterios de pronto:
  - usuario responde quem entrou, saiu, de onde veio e para onde foi;
  - a rota deixa de ser placeholder.
- Riscos:
  - abrir o modulo sem contrato BFF coerente e cair em UX pouco confiavel;
  - excesso de filtro para uma primeira entrega.
- Fora de escopo:
  - analytics financeiros;
  - comparativos complexos de mercado.
- Por que esse bloco vem nessa ordem:
  - depende mais de contrato novo do que H2H;
  - ainda e importante, mas mais periferico que match/team/player/rankings.

## Bloco 8 - Fechar o modulo dedicado de coaches
- Objetivo:
  - ativar coaches como dominio navegavel portfolio-wide.
- Escopo exato:
  - criar rota indice `/coaches`;
  - transformar `/coaches/[coachId]` em pagina real;
  - definir entradas contextuais a partir de time e, opcionalmente, search em etapa posterior.
- Mudanca visual/composicao:
  - indice de tecnicos e detalhe por tecnico;
  - foco em passagens por clube, periodos e contexto de desempenho.
- Mudanca de rota/navegacao:
  - nova rota `/coaches`;
  - expansao do shell para tratar rota indice;
  - search global para coach nao entra como requisito desta V1.
- Rotas afetadas:
  - `/coaches`
  - `/coaches/[coachId]`
- Arquivos/componentes mais provaveis:
  - novo `frontend/src/app/(platform)/coaches/page.tsx`
  - `frontend/src/app/(platform)/coaches/[coachId]/page.tsx`
  - novo modulo em `frontend/src/features/coaches/`
  - `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- Hooks/queries/types:
  - novos hooks/types/services dedicados a coaches
- Contratos/BFF:
  - router dedicado novo;
  - search global de coach e follow-up, nao pre-condicao.
- Entregaveis exatos:
  - pagina indice de coaches;
  - detalhe funcional de coach;
  - navegacao coerente no shell.
- Criterios de pronto:
  - o dominio deixa de existir apenas como placeholder de detalhe;
  - a navegacao faz sentido mesmo sem busca global por coach.
- Riscos:
  - abrir indice sem criterio de listagem;
  - acoplamento precoce ao search global.
- Fora de escopo:
  - search de coach;
  - rankings de coach.
- Por que esse bloco vem nessa ordem:
  - e um dominio novo valido, mas mais periferico e mais dependente de contrato do que os blocos anteriores.

## Bloco 9 - Fechar a landing editorial/institucional
- Objetivo:
  - transformar `/landing` em superficie institucional distinta da home executiva.
- Escopo exato:
  - apresentar o produto, cobertura e caminhos de entrada sem competir com a home do app.
- Mudanca visual/composicao:
  - experiencia de marketing/apresentacao;
  - foco em cobertura, proposta do produto e rotas de entrada.
- Mudanca de rota/navegacao:
  - nenhuma rota nova alem da ja existente `/landing`;
  - eventualmente links entre landing e app principal.
- Rotas afetadas:
  - `/landing`
- Arquivos/componentes mais provaveis:
  - `frontend/src/app/(marketing)/landing/page.tsx`
  - possiveis componentes novos sob o layout de marketing
- Hooks/queries/types:
  - baixos requisitos; pode iniciar com dados minimos
- Contratos/BFF:
  - baixa dependencia inicial;
  - pode reutilizar dados agregados ja expostos pela home ou contratos simples.
- Entregaveis exatos:
  - rota `/landing` deixa de ser placeholder;
  - narrativa institucional clara e separada do produto analitico.
- Criterios de pronto:
  - a landing explica o produto sem invadir a home executiva;
  - a pagina orienta o usuario sobre cobertura e entrada.
- Riscos:
  - puxar este bloco cedo demais e competir com backlog analitico central;
  - misturar decisao de marketing com necessidade de produto.
- Fora de escopo:
  - redesign do app principal;
  - reestruturacao da home executiva.
- Por que esse bloco vem nessa ordem:
  - menor impacto analitico direto;
  - fecha a camada institucional por ultimo.

# 4. Ordem final de execucao

Ordem recomendada:
1. Bloco 1 - Densificar o match center
2. Bloco 2 - Fechar a aba squad do team profile
3. Bloco 3 - Densificar stats e historico do player profile
4. Bloco 4 - Expandir breadth real de rankings
5. Bloco 5 - Fechar o modulo dedicado de head-to-head
6. Bloco 6 - Densificar a home executiva
7. Bloco 7 - Fechar o modulo dedicado de market
8. Bloco 8 - Fechar o modulo dedicado de coaches
9. Bloco 9 - Fechar a landing editorial/institucional

Justificativa executiva da ordem:
- primeiro entram superficies centrais ja verdes, porque nelas o ganho visivel e alto e o risco de regressao e baixo;
- depois entram expansoes que pedem algum ajuste real de contrato, mas ainda reaproveitam rotas e features maduras;
- os dominios novos so entram depois que o produto central esta mais denso;
- home vem depois do nucleo analitico porque ela deve apontar para superficies fortes, nao compensar superficies rasas;
- landing fica por ultimo porque nao aumenta a capacidade analitica do app principal.

# 5. Dependencias e blockers provaveis

## 5.1 Dependencias reais ja evidenciadas
- Nao ha router BFF dedicado hoje para H2H, market/transfers, coaches ou availability.
- `api/src/routers/search.py:20-22` nao cobre coach; isto bloqueia integracao com busca global, mas nao bloqueia a primeira versao de `/coaches`.
- `api/src/routers/rankings.py:41-44` mostra que pelo menos parte da expansao desejada de rankings nao fecha so no frontend.
- `frontend/src/features/teams/types/teams.types.ts` e `useTeamProfile.ts` nao possuem shape para availability hoje.
- `api/src/routers/home.py:309` e `:373` limitam a curadoria da home a um algoritmo curto e `limit 2`.

## 5.2 Blockers provaveis por dominio
- Match center:
  - nenhum blocker estrutural forte para a primeira wave;
  - maior risco e composicao ruim, nao contrato.
- Team squad + availability:
  - depende de extensao do contrato de `teams.py`;
  - exige types novos e coverage nova no frontend.
- Player stats:
  - nenhum blocker estrutural forte para a primeira wave;
  - pode demandar pequenas extensoes de payload, mas o shape base ja existe.
- Rankings:
  - bloqueado parcialmente por `RANKING_CONFIG` e suporte real no BFF.
- Head-to-head:
  - bloqueado por ausencia de router/contrato dedicado.
- Market:
  - bloqueado por ausencia de router/contrato dedicado.
- Coaches:
  - bloqueado por ausencia de router/contrato dedicado e por falta de rota indice no app;
  - busca global de coach e dependencia secundaria.
- Landing:
  - sem blocker tecnico forte; depende mais de prioridade.

## 5.3 Dependencias que ainda precisam de evidencia adicional antes da implementacao
- Qual shape de payload sera mais estavel para H2H, market e coaches no BFF.
- Se a home vai receber apenas mais highlights ou tambem novos agregados auxiliares.
- Se availability vai entrar no payload de team profile como secao opcional unica ou com query dedicada reaproveitada pelo mesmo hook.

# 6. Diff esperado da futura implementacao

Observacao:
- este diff esperado foi derivado do estado real do repositorio;
- itens com alta confianca sao aqueles em que a ausencia/presenca atual no codigo ja aponta o caminho;
- itens com media confianca dependem do desenho exato de contrato/BFF.

## 6.1 Arquivos provavelmente novos - alta confianca
- `frontend/src/app/(platform)/coaches/page.tsx`
  - motivo: a rota esta prevista no `.md` e hoje nao existe no app.
- `frontend/src/features/teams/components/TeamAvailabilitySection.tsx`
  - motivo: availability precisa entrar como secao do squad e hoje nao ha componente proprio para isso.
- `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`
  - motivo: documento de planejamento desta rodada.

## 6.2 Arquivos provavelmente novos - media confianca
- modulo novo em `frontend/src/features/head-to-head/`
  - motivo: nao ha nenhum arquivo de feature para H2H hoje e a rota atual e placeholder.
- modulo novo em `frontend/src/features/market/`
  - motivo: nao ha nenhum arquivo de feature para market/transfers hoje e a rota atual e placeholder.
- modulo novo em `frontend/src/features/coaches/`
  - motivo: nao ha nenhum arquivo de feature para coaches hoje e o detalhe atual e placeholder.
- possiveis subcomponentes novos em `frontend/src/features/home/`
  - motivo: a home ja existe, mas o aumento de densidade pode pedir composicao modular nova.
- routers novos em `api/src/routers/` para H2H, market/transfers e coaches
  - motivo: hoje nao existem routers dedicados para esses dominios.

## 6.3 Arquivos provavelmente alterados - extensao pequena e localizada
- `frontend/src/shared/components/navigation/usePlatformShellState.ts`
  - para incluir o indice `/coaches` e ajustar metadados de navegacao.
- `frontend/src/features/teams/hooks/useTeamProfile.ts`
  - para incluir novo bloco opcional de availability.
- `frontend/src/features/teams/types/teams.types.ts`
  - para adicionar shape de availability/sidelined.
- `frontend/src/features/players/types/players.types.ts`
  - para pequenos enriquecimentos de leitura em stats/history, se necessarios.
- `frontend/src/features/rankings/types/rankings.types.ts`
  - para refletir novos ranking types realmente suportados.
- `frontend/src/features/home/types/home.types.ts`
  - para refletir mudancas de curadoria, se o contrato evoluir.

## 6.4 Arquivos provavelmente alterados - expansao estrutural da superficie
- `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx`
- `frontend/src/features/matches/components/MatchCenterHeader.tsx`
- `frontend/src/features/matches/components/MatchTimelinePlaceholder.tsx`
- `frontend/src/features/matches/components/MatchLineupsPlaceholder.tsx`
- `frontend/src/features/matches/components/MatchTeamStatsPlaceholder.tsx`
- `frontend/src/features/matches/components/MatchPlayerStatsPlaceholder.tsx`
- `frontend/src/features/teams/components/TeamProfileContent.tsx`
- `frontend/src/features/teams/components/TeamSquadSection.tsx`
- `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`
- `frontend/src/features/players/components/PlayerStatsSection.tsx`
- `frontend/src/features/players/components/PlayerHistorySection.tsx`
- `frontend/src/features/rankings/components/RankingTable.tsx`
- `frontend/src/config/ranking.registry.ts`
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
- `frontend/src/app/(platform)/head-to-head/page.tsx`
- `frontend/src/app/(platform)/market/page.tsx`
- `frontend/src/app/(platform)/coaches/[coachId]/page.tsx`
- `frontend/src/app/(marketing)/landing/page.tsx`

## 6.5 Arquivos que podem exigir refactor localizado maior
- `frontend/src/app/(platform)/(home)/HomeExecutivePage.tsx`
  - motivo: a home ja concentra muitos blocos e a nova curadoria pode exigir redistribuicao interna.
- `frontend/src/app/(platform)/matches/[matchId]/MatchCenterContent.tsx`
  - motivo: a tela ja concentra varias tabs e pode precisar melhor separacao de responsabilidades.
- `frontend/src/features/matches/components/*Placeholder.tsx`
  - motivo: apesar de ja representarem superficies reais, o nome e a responsabilidade atual podem ficar desalinhados apos a densificacao.

## 6.6 Rotas novas
- `/coaches`
- endpoints BFF dedicados para H2H
- endpoints BFF dedicados para market/transfers
- endpoints BFF dedicados para coaches

## 6.7 Rotas existentes expandidas
- `/matches/[matchId]`
- `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]`
- `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]`
- `/rankings/[rankingType]`
- `/`
- `/head-to-head`
- `/market`
- `/coaches/[coachId]`
- `/landing`

## 6.8 Componentes compartilhados impactados
- `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- possivelmente `frontend/src/shared/components/feedback/EmptyState.tsx`
- possivelmente `frontend/src/shared/components/coverage/PartialDataBanner.tsx`
- possivelmente `frontend/src/shared/components/data-display/DataTable.tsx`

## 6.9 Hooks, queries, clients e adapters impactados
- Matches:
  - `frontend/src/features/matches/hooks/useMatchCenter.ts`
  - `frontend/src/features/matches/services/matches.service.ts`
  - `frontend/src/features/matches/queryKeys.ts`
  - `frontend/src/features/matches/types/matches.types.ts`
- Teams:
  - `frontend/src/features/teams/hooks/useTeamProfile.ts`
  - `frontend/src/features/teams/services/teams.service.ts`
  - `frontend/src/features/teams/queryKeys.ts`
  - `frontend/src/features/teams/types/teams.types.ts`
- Players:
  - `frontend/src/features/players/hooks/usePlayerProfile.ts`
  - `frontend/src/features/players/services/players.service.ts`
  - `frontend/src/features/players/queryKeys.ts`
  - `frontend/src/features/players/types/players.types.ts`
- Rankings:
  - `frontend/src/features/rankings/hooks/useRankingTable.ts`
  - `frontend/src/features/rankings/services/rankings.service.ts`
  - `frontend/src/features/rankings/queryKeys.ts`
  - `frontend/src/features/rankings/types/rankings.types.ts`
- Home:
  - `frontend/src/features/home/hooks/useHomePage.ts`
  - `frontend/src/features/home/services/home.service.ts`
  - `frontend/src/features/home/types/home.types.ts`
- Novos dominios:
  - modulo novo de H2H com hook/query/service/type proprios
  - modulo novo de market com hook/query/service/type proprios
  - modulo novo de coaches com hook/query/service/type proprios

## 6.10 Contratos/BFF impactados
- `api/src/routers/matches.py`
  - baixa probabilidade de mudanca estrutural; maior chance de pequenos enriquecimentos.
- `api/src/routers/teams.py`
  - alta probabilidade de extensao para availability dentro do team profile.
- `api/src/routers/players.py`
  - media probabilidade de pequenos enriquecimentos em stats/history.
- `api/src/routers/rankings.py`
  - alta probabilidade de extensao para ranking types adicionais e alinhamento com registry/frontend.
- `api/src/routers/home.py`
  - alta probabilidade de extensao para curadoria mais rica.
- novos routers dedicados para:
  - H2H
  - market/transfers
  - coaches

# 7. Arquivo gerado

Caminho:
- `docs/FRONTEND_CONTENT_EXPANSION_EXECUTION_PLAN.md`

Resumo do que o documento cobre:
- diagnostica o frontend atual com base em rotas, componentes, hooks, services, types e routers reais;
- separa claramente evidencia do repositorio de decisao recomendada;
- confirma quais conteudos do `.md` viram nova pagina e quais entram em superficies ja existentes;
- define um plano de execucao em blocos, com ordem recomendada, escopo, dependencias e limites;
- identifica dependencias reais de BFF, hooks, query state, tipos e navegacao;
- registra um diff esperado derivado do codigo atual, com grau de confianca implicito por categoria;
- deixa o trabalho preparado para implementacao posterior sem reabrir a arquitetura a cada wave.
