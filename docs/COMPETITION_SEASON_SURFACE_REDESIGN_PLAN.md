# Competition Season Surface Redesign Plan

## Objetivo

Planejar a reformulação da surface de temporada de competição como uma família de superfícies orientadas por tipo da edição:

- `LIGA`
- `COPA`
- `HÍBRIDA`

Premissas fixas:

- os 3 mockups do Stitch são guia visual e de hierarquia, não spec funcional;
- o produto aqui é acervo histórico de edições fechadas;
- não abrir backend/BFF sem gap real;
- manter shell global, rotas canônicas e coerência com a página de `competitions` já redesenhada;
- não criar 3 páginas isoladas.

## Diagnóstico curto

### Estado atual

- A surface atual de temporada está centralizada em um único arquivo grande: `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`.
- A página ainda é modelada como uma superfície universal com 3 abas genéricas: `calendar`, `standings`, `rankings`.
- A diferenciação estrutural por tipo já existe parcialmente, mas só dentro da aba de tabela, via `competition-structure`, `group-standings` e `ties`.
- O shell compartilhado também assume esse modelo universal. `frontend/src/shared/components/navigation/usePlatformShellState.ts` publica links de temporada como `Calendário`, `Tabela` e `Rankings`.

### Evidência objetiva

- `frontend/src/shared/utils/context-routing.ts` define `SEASON_HUB_TABS = ["calendar", "standings", "rankings"]`.
- `SeasonHubContent.tsx` decide o hero e o conteúdo principal pela aba, não pelo tipo real da edição.
- `SeasonHubContent.tsx` usa `shouldUseCompetitionStructure = competitionDefinition?.type !== "domestic_league"`, ou seja: a estrutura da edição hoje ainda é ligada ao tipo cadastral da competição, não ao formato real da temporada.
- `StructuredSeasonStandingsTab` já prova que o produto tem dado suficiente para alternar entre `league_table`, `group_table` e `knockout`, mas isso ainda está encaixado num shell genérico.
- O filtro global mostra labels internos que não podem sobreviver ao redesign, como `Contexto fixo` e `Travado na rota`, em `frontend/src/shared/components/filters/GlobalFilterBar.tsx`.

### Recorte correto do problema

- Problema principal: frontend e arquitetura de surface.
- Problema secundário: copy e hierarquia de leitura.
- Dados/BFF: há cobertura relevante já disponível; os gaps são pontuais e não bloqueiam um V1.
- Ambiente: nenhum blocker real identificado no ambiente para planejamento. O repositório está com worktree suja; a implementação futura deve ser cirúrgica.

## Escopo e não escopo

### Em escopo

- redefinir a surface de temporada como `competition season surface` orientada por tipo;
- trocar a hierarquia interna da página;
- criar shell base compartilhada e variantes por tipo;
- adaptar os mockups ao contexto histórico fechado;
- preservar e compatibilizar navegação canônica existente;
- listar gaps reais de contrato, sem inventar blocker.

### Fora de escopo

- criar 3 rotas independentes;
- redesenhar a página de `competitions` novamente;
- refatorar backend/BFF por antecipação;
- inventar módulos live season;
- criar regras semânticas locais de classificação continental/rebaixamento sem fonte confiável.

## Inventário atual

### Rotas relevantes

| Rota | Papel atual | Arquivo |
| --- | --- | --- |
| `/competitions` | catálogo redesenhado, entrada no domínio | `frontend/src/app/(platform)/competitions/page.tsx` |
| `/competitions/[competitionKey]` | hub da competição com escolha de temporada | `frontend/src/app/(platform)/competitions/[competitionKey]/page.tsx` |
| `/competitions/[competitionKey]/seasons/[seasonLabel]` | surface atual da temporada | `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/page.tsx` |
| `/competition/[competitionId]` | rota legada que redireciona para a canônica | `frontend/src/app/(platform)/competition/[competitionId]/page.tsx` |
| `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]` | perfil canônico de time dentro do contexto da temporada | `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]/page.tsx` |
| `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]` | perfil canônico de jogador dentro do contexto da temporada | `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]/page.tsx` |

### Arquivos frontend centrais

#### Surface atual

- `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`
- `frontend/src/features/competitions/components/SeasonCompetitionAnalyticsSection.tsx`
- `frontend/src/features/competitions/components/CompetitionHubContent.tsx`

#### Infra de contexto e navegação

- `frontend/src/shared/utils/context-routing.ts`
- `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- `frontend/src/shared/components/routing/CanonicalRouteContextSync.tsx`
- `frontend/src/shared/components/routing/CompetitionRouteContextSync.tsx`
- `frontend/src/shared/components/filters/GlobalFilterBar.tsx`
- `frontend/src/shared/stores/globalFilters.store.ts`

#### Hooks e contratos usados pela surface

- `frontend/src/features/competitions/hooks/useCompetitionStructure.ts`
- `frontend/src/features/competitions/hooks/useCompetitionAnalytics.ts`
- `frontend/src/features/competitions/hooks/useStageTies.ts`
- `frontend/src/features/competitions/hooks/useTeamJourneyHistory.ts`
- `frontend/src/features/standings/hooks/useStandingsTable.ts`
- `frontend/src/features/standings/hooks/useGroupStandingsTable.ts`
- `frontend/src/features/matches/hooks/useMatchesList.ts`
- `frontend/src/features/rankings/hooks/useRankingTable.ts`
- `frontend/src/features/competitions/services/competition-hub.service.ts`
- `frontend/src/features/competitions/types/competition-structure.types.ts`
- `frontend/src/features/standings/types/standings.types.ts`
- `frontend/src/features/matches/types/matches.types.ts`

### Navegação local e dependências já existentes

- Breadcrumb interno da temporada: catálogo -> competição -> temporada.
- `ProfileTabs` local hoje controla `calendar`, `standings`, `rankings`.
- `buildSeasonHubTabPath` é usado fora da página por `matches`, `players`, `teams`, `coaches`, `market` e pelo shell compartilhado.
- `CanonicalRouteContextSync` trava `competitionId` e `seasonId` no contexto canônico da rota.
- `GlobalFilterBar` preserva `roundId`, `venue`, `teamId` e janela temporal ao navegar entre áreas.

### Contratos/BFF usados hoje

| Endpoint / fonte | Dados disponíveis hoje | Uso atual | Valor para o redesign | Gap real |
| --- | --- | --- | --- | --- |
| `/api/v1/competition-structure` | escopo da edição, fases, grupos, transições, `formatFamily`, `seasonFormatCode` | hub da competição e aba de tabela estruturada | resolver variante, ordem de fases, transição estrutural | não expõe `surfaceType` pronto |
| `/api/v1/group-standings` | tabela de um grupo por fase, com rodadas | híbrida e grupos | resumo de grupos e detalhamento por grupo | visão de todos os grupos exige fan-out por grupo |
| `/api/v1/ties` | confrontos agregados por fase, vencedor, resolução, datas, próxima fase | mata-mata e bracket | bracket concluído, final, caminho do campeão | não existe payload único de bracket completo; hoje é fan-out por fase |
| `/api/v1/competition-analytics` | resumo da edição, analytics por fase, comparativo histórico | seção analítica | hero estável, comparativos, contagem de fases, volume de jogos | não traz campeão/finalista explícitos |
| `/api/v1/team-journey-history` | histórico completo de um time na competição, com `stageResult` por temporada | perfil de time | compor caminho do campeão ou do vice | não é season-scoped; precisa filtrar no frontend |
| `/api/v1/standings` | tabela, rodadas, `selectedRound`, `currentRound`, stage | ligas e league phase | campeão, vice, rodada final, classificação final | não traz metadado semântico de zona continental/rebaixamento |
| `/api/v1/matches` | lista com contexto de fase/grupo/chave, placar, status, estádio | calendário | finais, decisões concluídas, partidas marcantes por composição | não existe curadoria pronta de “partidas marcantes” |
| `resolveSeasonChampionArtwork` | asset visual local do campeão em algumas edições | hero lateral atual | identidade visual da edição fechada | cobertura parcial de mídia |

## Leitura crítica dos 3 mockups do Stitch

## LIGA

### O que é aproveitável

- tabela como centro da leitura;
- legenda visual de faixas da classificação;
- side rail mais enxuta que apoia a tabela, não disputa com ela;
- hero com densidade baixa e entrada rápida na informação principal.

### O que conflita com o produto real

- `Atualizado até a Rodada 28`;
- `Detalhes Rodada 28`;
- `9/10 partidas concluídas`;
- `Probabilidade de Título`;
- `Destaque da Rodada`.

### Adaptação correta

- falar de edição concluída;
- classificação final como módulo principal;
- campeão, vice, rodada final, volume da temporada, destaques finais;
- zona continental/rebaixamento só se houver semântica confiável; sem isso, não inventar rótulo.

## COPA

### O que é aproveitável

- bracket como protagonista;
- leitura de confronto por confronto;
- destaque do caminho para o título;
- side rail com histórico e contexto de competição.

### O que conflita com o produto real

- `Times vivos`;
- `Próximas Decisões`;
- confronto em destaque ainda em aberto;
- hero falando de `fase decisiva` em andamento.

### Adaptação correta

- `caminho do campeão` como eixo;
- chaveamento finalizado;
- confrontos concluídos, final, semifinal, quartas;
- o bloco de “próximas decisões” vira “confrontos marcantes da edição” ou “últimos confrontos da campanha”.

## HÍBRIDA

### O que é aproveitável

- explicitar coexistência de fase classificatória e mata-mata;
- resumo de grupos antes do bracket;
- bloco de transição entre estruturas;
- dois regimes de leitura coexistindo na mesma página.

### O que conflita com o produto real

- `Em andamento`;
- `Próxima fase`;
- `Próximas Partidas Decisivas`;
- `Destaques da Rodada`;
- `Última atualização: Hoje, 14:30`.

### Adaptação correta

- fase de grupos / league phase concluída;
- qualificados e síntese da fase classificatória;
- progressão concluída no mata-mata;
- bloco central de transição estrutural: “como a edição saiu da fase classificatória para a decisão”.

## Conclusão dos mockups

- Os mockups resolvem o problema de hierarquia.
- Eles não resolvem o domínio do produto.
- A implementação correta é extrair composição visual, densidade, ordem e protagonismo de módulos, mas reescrever copy, estados e semântica para edições históricas fechadas.

## Problemas reais a corrigir na surface atual

### Código / arquitetura

- Surface monolítica demais.
- Hero e navegação orientados por abas universais, não por tipo da edição.
- Regras de estrutura distribuídas e acopladas ao tipo cadastral da competição.
- Round filter hoje vaza semântica operacional para a narrativa principal da página.

### Dados / contratos

- Há dado suficiente para montar LIGA, COPA e HÍBRIDA em V1.
- Os gaps reais são:
  - zona continental/rebaixamento sem metadado semântico;
  - resumo de todos os grupos em híbrida com custo de múltiplas requests;
  - ausência de payload consolidado de hero da edição.

### Validação

- O E2E atual cobre fluxo crítico da página universal, não variantes por tipo.
- Falta critério objetivo por representative season de liga, copa e híbrida.

### Ambiente

- Nenhum blocker técnico de ambiente foi encontrado para este planejamento.

## Arquitetura de frontend proposta

## Princípio

Manter:

- a rota canônica atual;
- o shell global atual;
- o contexto global atual;
- os deep links canônicos para time/jogador.

Trocar:

- o modelo interno da surface;
- a hierarquia de conteúdo;
- a navegação local da própria edição.

## Proposta

### 1. Um orquestrador único da surface

Transformar `SeasonHubContent.tsx` em orquestrador fino e extrair a surface para uma arquitetura em camadas:

- `CompetitionSeasonSurface`
- `CompetitionSeasonSurfaceShell`
- `resolveCompetitionSeasonSurfaceType`
- variantes:
  - `LeagueSeasonSurface`
  - `CupSeasonSurface`
  - `HybridSeasonSurface`

### 2. Resolução de variante por edição, não por rota

Criar utilitário de resolução com fallback:

1. se `competition-structure` existir:
   - só tabela -> `LIGA`
   - só mata-mata -> `COPA`
   - tabela + mata-mata -> `HÍBRIDA`
2. se não existir:
   - `domestic_league` -> `LIGA`
   - `domestic_cup` / `international_cup` -> `COPA` conservadora

Isso evita prender a classificação da surface ao catálogo estático quando a edição real for híbrida.

### 3. Backward compatibility de rota

Não quebrar a rota atual nem os links já espalhados no produto.

Estratégia:

- manter `/competitions/[competitionKey]/seasons/[seasonLabel]`;
- manter leitura de `?tab=` de forma compatível;
- reinterpretar `tab` como deep link para seção ou estado interno da nova surface, em vez de manter a página dividida em três produtos internos;
- não forçar migração ampla no mesmo bloco.

### 4. Separar navegação de produto de navegação da edição

- Navegação do produto continua no shell e nos links canônicos para `matches`, `rankings`, `teams`, `players`.
- Navegação local da edição passa a ser sobre a leitura da temporada concluída:
  - visão geral;
  - classificação / chaveamento / fase classificatória;
  - decisão / destaques / confrontos;
  - sempre variando por tipo.

## Base compartilhada da nova surface

## A base compartilhada deve conter

- breadcrumb canônico;
- sincronização de contexto (`CanonicalRouteContextSync`);
- resolução de variante;
- shell visual alinhada ao redesign atual;
- hero/topo estável da edição;
- trilha curta de metadados:
  - competição
  - temporada
  - tipo da edição
  - cobertura
  - última atualização, quando fizer sentido
- rail de saídas do produto:
  - partidas
  - rankings
  - times
  - jogadores
- fallbacks de erro / vazio / cobertura parcial;
- compatibilidade com `roundId`, `stageId`, `groupId`, mas sem deixar isso sequestrar a narrativa principal da edição fechada.

## O que não fica na base

- classificação final específica de liga;
- bracket concluído de copa;
- resumo de grupos de híbrida;
- hero com copy específica de cada tipo.

## Definição por variante

## Variante LIGA

### Hero / topo

- título: nome da competição + temporada;
- kicker: `edição concluída`;
- bloco principal: campeão da temporada;
- KPIs prioritários:
  - campeão
  - vice
  - rodada final
  - total de clubes / jogos, se disponível
- apoio visual do campeão só como reforço, não como peça promocional.

### Navegação local

- `Visão geral`
- `Classificação final`
- `Partidas marcantes`
- `Destaques da edição`

Compatibilidade:

- `tab=standings` aponta para `Classificação final`
- `tab=calendar` aponta para `Partidas marcantes`
- `tab=rankings` aponta para `Destaques da edição`

### Blocos de conteúdo

1. hero da edição concluída
2. classificação final
3. faixa superior/inferior da tabela
4. rodada final ou recorte de encerramento
5. destaques finais de ranking
6. saídas para páginas irmãs

### Fallbacks

- sem `competition-structure`: continua viável com `standings`;
- sem mídia do campeão: card neutro;
- sem semântica de zona: não exibir rótulos inventados de continental/rebaixamento; usar apenas top/bottom slices com texto neutro.

## Variante COPA

### Hero / topo

- kicker: `edição concluída`;
- bloco principal: campeão + final;
- KPIs prioritários:
  - campeão
  - finalista
  - fases eliminatórias
  - confrontos resolvidos

### Navegação local

- `Visão geral`
- `Chaveamento`
- `Confrontos decisivos`
- `Destaques da edição`

Compatibilidade:

- `tab=standings` aponta para `Chaveamento`
- `tab=calendar` aponta para `Confrontos decisivos`
- `tab=rankings` aponta para `Destaques da edição`

### Blocos de conteúdo

1. hero da edição e da decisão
2. bracket concluído como peça central
3. final e semifinais
4. caminho do campeão
5. confrontos marcantes da edição
6. líderes da competição / histórico resumido

### Fallbacks

- sem bracket completo: mostrar fases eliminatórias em cards por estágio com `ties`;
- sem jornada do campeão: usar final + confrontos resolvidos;
- sem asset visual: hero neutro.

## Variante HÍBRIDA

### Hero / topo

- kicker: `edição concluída`;
- mensagem principal: a edição combinou fase classificatória e mata-mata;
- KPIs prioritários:
  - campeão
  - grupos ou league phase
  - fases eliminatórias
  - volume total de jogos

### Navegação local

- `Visão geral`
- `Fase classificatória`
- `Mata-mata`
- `Destaques da edição`

Compatibilidade:

- `tab=standings` aponta para a seção dominante da estrutura:
  - grupos / league phase se a leitura estrutural começar na fase classificatória;
  - ou bloco de estrutura da edição;
- `tab=calendar` aponta para confrontos decisivos;
- `tab=rankings` aponta para destaques.

### Blocos de conteúdo

1. hero da edição híbrida
2. resumo da fase classificatória
3. bloco de transição estrutural
4. bracket concluído
5. caminho do campeão
6. destaques finais da edição

### Fallbacks

- resumo de grupos em V1 pode ser:
  - grupo selecionado por padrão + seletor;
  - ou cards dos grupos via fan-out controlado;
- se o fan-out de grupos ficar caro, não tratar como blocker; entregar resumo focal e evoluir depois;
- sem jornada do campeão: usar vencedor da final + bracket finalizado.

## Proposta de navegação local por tipo

## Decisão

A season surface deixa de ser “3 abas universais” e vira “1 visão editorial da edição + seções internas”.

## Modelo recomendado

### Navegação local interna

- usar `ProfileTabs` ou componente equivalente para seções da edição;
- labels por tipo, não genéricas;
- não duplicar o shell compartilhado do produto.

### Navegação externa ao produto

Continuar por links/saídas para:

- `/matches`
- `/rankings/...`
- `/teams`
- `/players`

Isso evita que a season surface continue funcionando como um mini-aplicativo universal dentro da rota de temporada.

## Hero/topo por tipo no contexto de competição fechada

| Tipo | Hero deve comunicar | Hero não pode comunicar |
| --- | --- | --- |
| LIGA | campeão, classificação final, rodada final, temporada encerrada | temporada em andamento, projeção, rodada atual ao vivo |
| COPA | decisão concluída, caminho do campeão, confrontos resolvidos | times vivos, próximas decisões, mata-mata em curso |
| HÍBRIDA | fase classificatória concluída + mata-mata concluído | transição ao vivo, próxima fase, cobertura parcial em tempo real como eixo narrativo |

## Fallbacks recomendados

### Estrutura ausente

- `LIGA`: renderizar versão standings-led baseada em `standings`.
- `COPA`: renderizar resumo da edição + confrontos disponíveis, sem prometer bracket completo.
- `HÍBRIDA`: rebaixar para overview conservador com aviso claro de estrutura parcial.

### Cobertura parcial

- manter `CoverageBadge` / `PartialDataBanner`;
- copy deve falar de `cobertura parcial da edição`, não de fase “em andamento”.

### Dados insuficientes para storytelling

- não inventar narrativa;
- substituir por texto factual:
  - `Campeão indisponível`
  - `Confrontos da fase indisponíveis`
  - `Resumo estrutural indisponível`

### Filtros globais

- `competitionId` e `seasonId` continuam travados pela rota canônica;
- `roundId`, `teamId`, `venue` e janela temporal continuam existindo, mas devem afetar módulos táticos, não redefinir o hero da edição concluída.

## Textos e labels internos que precisam sair

Com evidência já encontrada no frontend:

- `Hub da competicao`
- `Saída canônica`
- `Contexto fixo`
- `Travado na rota`
- `Em leitura`
- `Calendário em foco`
- `Leitura principal da edição`
- `Descoberta`
- `sem desviar para uma tela cenográfica`
- `presos ao contexto certo desta edição`

Diretriz:

- substituir por linguagem de produto final;
- evitar qualquer copy que exponha mecânica interna do sistema.

## O que pode ser feito só no frontend

- resolver variante da edição a partir de `competition-structure`;
- trocar hero, navegação local e hierarquia de módulos;
- compor campeão/vice/final a partir de `standings`, `ties`, `analytics` e `team-journey-history`;
- reutilizar `SeasonCompetitionAnalyticsSection` onde fizer sentido;
- reinterpretar `tab=` para manter backward compatibility;
- limpar copy e estados vazios;
- manter saídas para páginas irmãs sem mudar BFF.

## O que só depende de ajuste de contrato se o produto insistir

| Tema | Situação | Observação |
| --- | --- | --- |
| faixas semânticas de classificação continental / rebaixamento | gap real | `standings` não entrega zona semântica; não inventar |
| resumo de todos os grupos na híbrida em first paint | gap incremental | hoje exige múltiplas requests em `group-standings` |
| payload único de hero da edição | incremental | hoje dá para compor no frontend |
| curadoria real de “partidas marcantes” | incremental | `matches` entrega matéria-prima, não curadoria pronta |
| `surfaceType` pronto no BFF | incremental | o frontend já consegue derivar via estrutura |

## Ordem exata de execução em blocos

## Bloco 0. Grounding e freeze do plano

### Objetivo

- confirmar estado atual;
- mapear rotas, componentes, contratos e dados;
- fechar a estratégia antes de codar.

### Arquivos

- `docs/COMPETITION_SEASON_SURFACE_REDESIGN_PLAN.md`

### Status

- concluído neste passo.

## Bloco 1. Base compartilhada

### Objetivo

- criar a shell única da `competition season surface`;
- extrair o orquestrador da página atual;
- resolver `competition season surface type`;
- compatibilizar deep links e shell.

### Tarefas

1. Criar utilitário de resolução de variante por edição.
2. Extrair a surface para componentes menores.
3. Manter a rota atual e compatibilidade de `tab=`.
4. Separar navegação local da edição das saídas de produto.
5. Congelar o hero como resumo da edição fechada.

### Arquivos provavelmente afetados

- `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`
- `frontend/src/features/competitions/utils/competition-season-surface.ts` ou equivalente novo
- `frontend/src/features/competitions/components/season-surface/*` novos
- `frontend/src/shared/utils/context-routing.ts`
- `frontend/src/shared/components/navigation/usePlatformShellState.ts`

## Bloco 2. Variante LIGA

### Objetivo

- implementar primeiro a variante mais estável e próxima do modelo atual;
- trocar a página de `calendar/standings/rankings` por uma edição concluída centrada em classificação final.

### Tarefas

1. Construir hero de edição concluída.
2. Promover classificação final a bloco principal.
3. Rebaixar calendário para bloco auxiliar de partidas marcantes.
4. Integrar rankings como apoio, não como aba principal.
5. Remover copy live season.

### Arquivos provavelmente afetados

- `frontend/src/features/competitions/components/season-surface/variants/LeagueSeasonSurface.tsx`
- `frontend/src/features/competitions/components/season-surface/blocks/*`
- `frontend/src/features/standings/hooks/useStandingsTable.ts` se precisar de ajuste leve de consumo
- `frontend/src/features/competitions/components/SeasonCompetitionAnalyticsSection.tsx` se parte da leitura for reaproveitada

## Bloco 3. Variante COPA

### Objetivo

- tornar o bracket o centro da surface;
- usar confrontos concluídos e decisão da edição como narrativa principal.

### Tarefas

1. Hero com campeão e final.
2. Bracket concluído como bloco principal.
3. Cards de confrontos decisivos por fase.
4. Caminho do campeão por composição com `team-journey-history`, se viável.
5. Reescrever side rail e destaques para contexto histórico.

### Arquivos provavelmente afetados

- `frontend/src/features/competitions/components/season-surface/variants/CupSeasonSurface.tsx`
- `frontend/src/features/competitions/components/season-surface/blocks/*`
- `frontend/src/features/competitions/hooks/useStageTies.ts` apenas se exigir refinamento de consumo
- `frontend/src/features/competitions/hooks/useTeamJourneyHistory.ts` apenas se exigir uso season-scoped no frontend

## Bloco 4. Variante HÍBRIDA

### Objetivo

- materializar fase classificatória + mata-mata na mesma superfície;
- deixar clara a transição estrutural da edição.

### Tarefas

1. Hero híbrido e resumo estrutural.
2. Resumo de grupos / league phase.
3. Bloco de transição para o mata-mata.
4. Bracket concluído.
5. Caminho do campeão e destaques finais.

### Arquivos provavelmente afetados

- `frontend/src/features/competitions/components/season-surface/variants/HybridSeasonSurface.tsx`
- `frontend/src/features/competitions/components/season-surface/blocks/*`
- `frontend/src/features/standings/hooks/useGroupStandingsTable.ts`
- `frontend/src/features/competitions/services/competition-hub.service.ts` apenas se houver necessidade de um agregador futuro, não para o primeiro corte

## Bloco 5. Limpeza final

### Objetivo

- limpar copy;
- consolidar fallbacks;
- ajustar navegação e testes;
- fechar consistência visual.

### Tarefas

1. Remover labels internos feios.
2. Revisar estados vazios/erro/cobertura parcial.
3. Garantir coerência com shell global.
4. Atualizar testes de navegação e surface.
5. Validar representative seasons.

### Arquivos provavelmente afetados

- `frontend/src/shared/components/filters/GlobalFilterBar.tsx`
- `frontend/src/shared/components/navigation/usePlatformShellState.ts`
- `frontend/e2e/competition-season-core.spec.ts`
- novo spec visual ou expansão de um spec existente para liga/copa/híbrida

## Riscos reais

### 1. Quebra de contrato de navegação interna

Risco:

- muitos pontos do produto já chamam `buildSeasonHubTabPath`.

Mitigação:

- manter rota e `tab=` compatíveis no primeiro corte;
- migrar sem quebrar deep links.

### 2. Hero ficar refém de `roundId`

Risco:

- o filtro de rodada continuar mudando o significado principal da página.

Mitigação:

- hero sempre representa a edição concluída;
- `roundId` afeta blocos táticos, não a narrativa principal.

### 3. Híbrida exigir requests demais para grupos

Risco:

- fan-out de `group-standings` por grupo aumentar custo de render.

Mitigação:

- V1 com resumo focal, lazy load ou seleção de grupo;
- só abrir agregação no BFF se houver evidência de necessidade.

### 4. Inventar semântica que o dado não suporta

Risco:

- labels de zona continental/rebaixamento ou storytelling de “partidas marcantes” sem fonte confiável.

Mitigação:

- texto factual primeiro;
- só promover o que o contrato realmente sustenta.

### 5. Refactor amplo demais no arquivo atual

Risco:

- `SeasonHubContent.tsx` é grande e concentra muita coisa.

Mitigação:

- transformar em orquestrador fino;
- extrair blocos pequenos;
- preservar comportamento compatível por blocos.

## Critérios objetivos de pronto

Uma implementação baseada neste plano só deve ser considerada pronta quando:

1. A route canônica `/competitions/[competitionKey]/seasons/[seasonLabel]` continuar funcionando sem regressão de contexto.
2. A surface resolver corretamente `LIGA`, `COPA` e `HÍBRIDA` para pelo menos 1 edição representativa de cada tipo.
3. O hero falar sempre de edição concluída, nunca de competição ao vivo.
4. A leitura principal mudar por tipo:
   - liga -> classificação final
   - copa -> bracket
   - híbrida -> fase classificatória + mata-mata
5. Os links para `matches`, `rankings`, `teams` e `players` continuarem preservando o contexto da edição.
6. Labels internos feios deixarem de aparecer na season surface.
7. Fallbacks de estrutura, mídia e cobertura parcial estiverem definidos e testados.
8. Não houver dependência obrigatória de endpoint novo para o primeiro corte.

## Representative seasons para validação

- `LIGA`: `primeira_liga 2024/2025` ou `premier_league 2024/2025`
- `COPA`: `copa_do_brasil 2024`
- `HÍBRIDA`: `champions_league 2024/2025` ou `libertadores 2025`

## Próximo passo seguro

Executar o `Bloco 1` sem tocar em backend:

- extrair a shell compartilhada;
- criar o resolvedor de variante;
- manter compatibilidade da rota atual;
- deixar `SeasonHubContent.tsx` apenas como ponto de entrada e composição.
