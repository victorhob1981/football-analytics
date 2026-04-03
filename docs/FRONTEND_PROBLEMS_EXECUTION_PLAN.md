# FRONTEND_PROBLEMS_EXECUTION_PLAN

# 1. Objetivo do plano

Este documento transforma a análise inicial de `docs/FRONTEND_PROBLEMS.md` em um plano operacional por blocos de execução.

O objetivo não é assumir causa raiz antes da hora. Cada bloco parte de investigação objetiva no comportamento real, no frontend, no BFF, nos contratos e, quando aplicável, nos dados. A correção definitiva só entra depois dessa confirmação.

Como evidência de contexto, o repositório já contém:
- rotas reais para `home`, `competitions`, `season hub`, `teams`, `players`, `rankings` e `matches` em `frontend/src/app/(platform)`;
- contratos BFF documentados para `search`, `teams`, `players`, `rankings` e `matches` em `docs/BFF_API_CONTRACT.md`;
- status declarando várias dessas superfícies como `COMPLETO` em `docs/FRONTEND_IMPLEMENTATION_STATUS.md`.
- a varredura adicional encontrou o mesmo padrão crítico de linguagem interna, excesso de framing estrutural e banners de coverage em `PlatformShell.tsx`, `GlobalFilterBar.tsx`, `usePlatformShellState.ts`, `GlobalSearchOverlay.tsx`, `TeamsPageContent.tsx`, `PlayersPage`, `TeamProfileContent.tsx`, `PlayerProfileContent.tsx`, `MatchesPage` e `MatchCenterContent.tsx`;
- rotas auxiliares e legadas como `market`, `head-to-head`, `audit` e `coaches` hoje entram no produto como superfícies de aviso sobre escopo/materialização, em vez de uma experiência final de produto.

Por isso, a prioridade do plano é reconciliar o que foi observado manualmente com o que hoje está documentado como pronto.

# 2. Critérios de agrupamento

Os blocos foram organizados por cinco critérios:

- por tela quando os problemas estão concentrados em uma mesma superfície e compartilham contexto de execução;
- por cadeia técnica quando a mesma causa provável afeta mais de uma tela, como `search + resolver curto` ou contrato de identidade do jogador;
- por família de domínio quando listas e perfis compartilham o mesmo padrão de problema, como `teams + players + matches`;
- por dependência entre camadas para reduzir ida e volta entre frontend, BFF, resolver e dados;
- por tipo de correção apenas quando isso muda a ordem ótima de execução, priorizando falha funcional e contrato antes de polimento visual.

A lógica de agrupamento evita:
- tratar cada problema como ticket isolado quando a correção natural é da tela inteira;
- misturar módulos secundários não citados no diagnóstico inicial;
- abrir frentes novas fora de `home`, `competitions`, `season hub`, `search`, `teams`, `players`, `rankings` e `matches`.

# 3. Estratégia de execução

A estratégia recomendada para este conjunto específico de problemas é:

- começar pelas falhas que quebram confiança no produto e dependem de cadeia técnica compartilhada: `search`, resolução canônica de entidades e estabilidade do `match center`;
- fechar em seguida o gap de dados/contrato de identidade do jogador, porque ele contamina percepção de qualidade e bloqueia ajustes úteis em superfícies de descoberta;
- atacar depois a tela de `rankings`, resolvendo no mesmo bloco a usabilidade do filtro, a composição dos cards e a semântica de coverage daquela superfície;
- corrigir na sequência as superfícies de descoberta e perfil de `teams`, `players` e `matches`, porque a varredura mostrou muito framing estrutural, metadado excessivo e inconsistência de linguagem justamente nos fluxos centrais já considerados prontos;
- corrigir em bloco as superfícies de descoberta de competição e temporada, porque elas repetem o mesmo padrão de texto arquitetural, baixa densidade informacional e affordance ruim;
- deixar a `home` depois dessas telas, para reposicioná-la com CTAs e conteúdo apontando para superfícies já coerentes;
- encerrar com uma passada transversal de shell global, estados de fallback e rotas legadas, concentrando o que for realmente compartilhado e evitando reabrir blocos já fechados.

# 4. Blocos de execução

## Bloco 1 — Busca global e resolução canônica de entidades

### Escopo
- overlay de busca global;
- rotas curtas `/teams/[teamId]` e `/players/[playerId]`;
- resolução de contexto canônico para `team` e `player`;
- mensagens exibidas no fluxo de busca e no fluxo de resolução de rota quando a navegação falha.

### Tipo principal
- funcional;
- navegação/roteamento/resolver;
- dados/contrato.

### Problemas cobertos
- busca não encontra `Flamengo`;
- overlay de busca continua usando linguagem de navegabilidade/cobertura como copy de produto;
- perfil do Flamengo não abre corretamente;
- mensagens expostas ao usuário são técnicas demais quando o contexto de `team` ou `player` não resolve;
- possível divergência entre cobertura documentada da busca/resolver e comportamento real observado.

### Hipóteses iniciais
- `defaultContext` do time existe no dado bruto, mas não chega corretamente no BFF ou é filtrado antes de virar resultado navegável;
- o endpoint de `search` está descartando entidades relevantes por regra de navegabilidade canônica;
- `TeamRouteResolver` e `PlayerRouteResolver` não tratam corretamente `defaultContext` ausente, inválido ou inconsistente com o registry de competições/safras suportadas;
- o overlay da busca e os fallbacks dos resolvers estão expondo linguagem de implementação em vez de linguagem de produto.

### Dependências
- BFF `GET /api/v1/search`;
- BFF `GET /api/v1/teams/{teamId}/contexts`;
- BFF `GET /api/v1/players/{playerId}/contexts`;
- registry de competição/temporada suportada no frontend;
- `TeamRouteResolver`, `PlayerRouteResolver` e navegação derivada do overlay.

### Ordem interna recomendada
- reproduzir o caso do Flamengo na busca e na rota curta;
- comparar resposta real de `search`, `teams/{teamId}/contexts` e `players/{playerId}/contexts`;
- corrigir a cadeia de seleção de contexto canônico e o fallback dos resolvers;
- revisar a mensagem final exibida ao usuário no overlay e nos deep links de `team` e `player`.

### Risco de regressão
- alto, porque mexe em navegação primária, busca global e resolução de contexto compartilhada.

### Critério de conclusão
- `Flamengo` aparece quando pesquisado em cenário compatível com a cobertura atual;
- as rotas curtas de `team` e `player` resolvem para a rota canônica correta ou entregam fallback de produto aceitável;
- o fluxo deixa de expor termos internos de navegabilidade, contexto canônico ou BFF no caso de falha.

### Observações
- não expandir o bloco para `coaches`, `head-to-head` ou módulos não citados; o foco aqui é `search + resolvers de entidades`.

## Bloco 2 — Match center: estabilidade, tags e enquadramento do detalhe

### Escopo
- rota `/matches/[matchId]`;
- composição de tags/metadados da shell do `match center`;
- renderização inicial do detalhe da partida;
- hero, abas, alertas e blocos de resumo do `match center`.

### Tipo principal
- funcional;
- UX/conteúdo.

### Problemas cobertos
- erro visível ao abrir detalhe de partida com chave React duplicada (`Rodada 38`);
- risco de renderização inconsistente no cabeçalho/shell da página;
- detalhe da partida ainda destaca disponibilidade estrutural e cobertura antes do conteúdo do jogo;
- mistura de rótulos PT/EN e descrições muito técnicas nas abas e blocos do `match center`.

### Hipóteses iniciais
- a tela gera `key` a partir de label visível e não de identificador único;
- a mesma informação de rodada entra duas vezes no array de tags, uma via `roundId` e outra via `timeWindowLabel`;
- o BFF pode estar entregando campos semanticamente duplicados e a shell não está deduplicando;
- a superfície herdou copy de transição/implementação e componentes de coverage com peso visual acima do necessário.

### Dependências
- frontend do `match center`;
- `usePlatformShellState` / `PlatformShellFrame`, se a duplicidade vier das `scopeTags`;
- metadados recebidos do contrato de `matches/{matchId}` apenas se a duplicidade vier do payload;
- independente do restante do plano na maior parte do fluxo.

### Ordem interna recomendada
- reproduzir com a mesma partida que gerou o erro;
- identificar se a duplicidade nasce no payload ou no mapeamento de `scopeTags`;
- corrigir unicidade e deduplicação;
- revisar hero, alertas, nomes de abas e descrições do detalhe para priorizar a leitura do jogo;
- validar a mesma rota com pelo menos mais uma partida.

### Risco de regressão
- médio-alto, porque afeta uma tela central e pode tocar shell compartilhada.

### Critério de conclusão
- o `match center` abre sem erro visível ou warning funcional equivalente;
- tags e metadados continuam corretos e sem duplicação perceptível;
- o detalhe da partida passa a falar do jogo antes de falar da estrutura da tela.

## Bloco 3 — Contrato de identidade de jogadores

### Escopo
- listagem de jogadores;
- campo `nationality` ao longo da cadeia provider -> transformação -> BFF -> frontend;
- superfícies já existentes que dependem da identidade básica do jogador.

### Tipo principal
- dados/contrato.

### Problemas cobertos
- todos os jogadores observados aparecem com `Nacionalidade não informada`;
- perda de valor informacional em cards/listas de descoberta que deveriam carregar identidade básica do atleta.

### Hipóteses iniciais
- a fonte entrega nacionalidade e o campo se perde em ingestão, modelagem ou transformação;
- o dado existe no warehouse, mas não está sendo exposto no contrato usado pelo frontend;
- o frontend está aplicando fallback padrão mesmo quando o contrato traz valor;
- a cobertura real do campo é parcial e o tratamento atual mascara esse recorte.

### Dependências
- provider/dados brutos, se a investigação mostrar ausência já na origem;
- modelagem intermediária/mart, se o campo se perder antes do BFF;
- contratos `GET /api/v1/players` e consumidores de lista/perfil;
- frontend apenas na etapa final de renderização/fallback.

### Ordem interna recomendada
- confirmar se a nacionalidade existe na origem e em qual camada ela some;
- corrigir persistência/transformação/contrato na primeira camada defeituosa;
- ajustar o frontend para diferenciar ausência real de ausência causada por contrato;
- validar a listagem de jogadores com amostra concreta de atletas.

### Risco de regressão
- médio, porque atravessa dados e BFF, mas o domínio é semântico e bem delimitado.

### Critério de conclusão
- jogadores com nacionalidade disponível passam a exibir o valor correto;
- o fallback só aparece quando a ausência for real e comprovada;
- o contrato final da lista de jogadores fica coerente com o que a UI precisa renderizar.

### Observações
- este bloco deve fechar antes do ajuste de densidade dos cards de `rankings` se a solução escolhida depender de metadados do jogador.

## Bloco 4 — Rankings: filtro, cards e coverage

### Escopo
- rota `/rankings/[rankingType]`;
- controle de busca/filtro da tela;
- cards superiores de jogadores;
- semântica de coverage exibida nessa superfície.

### Tipo principal
- funcional;
- UX/conteúdo;
- visual/layout.

### Problemas cobertos
- digitar no filtro/busca causa refresh e reposiciona a página;
- cards dos jogadores estão visualmente quebrados e mal aproveitados;
- banner de coverage aparece como alerta sem motivo claro, inclusive com leitura contraditória;
- hero, painéis laterais e alertas ainda supervalorizam cobertura, recorte e semântica de registry em vez da leaderboard;
- a tela mistura linguagem de produto com linguagem operacional/cache, diluindo o foco do ranking.

### Hipóteses iniciais
- o input sincroniza com rota/query de forma agressiva e força navegação ou rerender completo;
- a composição atual do card não usa bem o espaço e possivelmente depende de dados hoje ausentes ou mal posicionados;
- a regra que decide mostrar coverage está acoplada a um componente compartilhado ou a um estado semânticamente mal classificado;
- a página foi estruturada demais em torno de `metric registry`, `coverageWarning` e recorte técnico, e de menos em torno da hierarquia da leaderboard.

### Dependências
- frontend da tela de `rankings`;
- `metrics.registry` / `ranking.registry`, se a copy operacional vier dessas definições;
- componentes compartilhados de filtro e feedback, se a investigação confirmar reutilização;
- depende do Bloco 3 apenas se a composição final do card usar `nationality` ou outro metadado já documentado.

### Ordem interna recomendada
- estabilizar a interação do filtro e impedir reset de scroll;
- reduzir a carga de metadado técnico no hero, painéis laterais e alertas da tela;
- confirmar quais dados realmente estão disponíveis para enriquecer o card sem inventar contrato novo;
- redesenhar a composição do card no mesmo bloco;
- ajustar a regra de coverage dessa tela e só extrair mudança para componente compartilhado se houver reuso real.

### Risco de regressão
- médio, porque combina interação, estado de rota e componentes visuais compartilhados.

### Critério de conclusão
- o filtro não reposiciona a página a cada tecla;
- os cards ficam legíveis, densos e com informação útil;
- coverage só aparece quando houver limitação relevante para o usuário;
- a leaderboard volta a ser o centro visual e semântico da página.

## Bloco 5 — Descoberta e perfis de teams, players e matches

### Escopo
- `/teams`, `/players` e `/matches`;
- `team profile` e `player profile`;
- seções internas `overview`, `squad` e `history` dessas superfícies.

### Tipo principal
- UX/conteúdo;
- visual/layout;
- navegação.

### Problemas cobertos
- listas e perfis ainda falam demais de rota canônica, resolver curto, contrato/BFF e recorte estrutural;
- painéis como `Recorte ativo`, `Saídas estruturais` e `Navegação e contexto` consomem espaço nobre com pouco valor de futebol;
- heroes e seções usam termos mistos ou internos como `Teams`, `Team profile`, `History`, `Squad`, `Stitch`, `fact_fixture_lineups`, `payload` e `projeto sustenta`;
- pills de coverage e tags de contexto aparecem cedo demais em listas e perfis, antes do conteúdo principal;
- a lista de partidas expõe metadado operacional como `matchId`;
- há inconsistência de linguagem e hierarquia entre listas e perfis de um mesmo domínio.

### Hipóteses iniciais
- `ProfileShell`, `ProfilePanel`, `ProfileTabs` e componentes correlatos foram reutilizados sem adaptação suficiente por domínio;
- copy de migração e honestidade técnica vazou da implementação para a camada de produto;
- parte dos painéis laterais foi criada para explicar navegação/contexto quando o fluxo ainda estava em construção e nunca foi reduzida;
- alguns caveats pertencem à documentação interna, não à interface final.

### Dependências
- frontend;
- componentes compartilhados de `profile`, `coverage` e `feedback`;
- depende do Bloco 1 apenas no que tocar feedback compartilhado de deep link;
- depende do Bloco 3 se a revisão de `players` passar a priorizar metadados hoje incompletos, como nacionalidade.

### Ordem interna recomendada
- limpar primeiro as listas de `players`, `teams` e `matches`, porque são entradas de descoberta;
- corrigir depois heroes, sidebars e abas de `team profile` e `player profile`;
- revisar por último `overview`, `squad` e `history`, removendo caveats de tabela/provider e metadados operacionais que não deveriam ser protagonistas.

### Risco de regressão
- médio, porque toca muitas superfícies, mas a maior parte do trabalho é de UI e hierarquia.

### Critério de conclusão
- listas e perfis passam a priorizar entidade, contexto esportivo e ação útil;
- termos internos de rota, resolver, provider, tabela ou fase de implementação deixam de aparecer como copy principal;
- metadados operacionais como `matchId` e painéis de contexto estrutural deixam de dominar a leitura.

## Bloco 6 — Descoberta de competições, competition hub e season hub

### Escopo
- `/competitions`;
- `/competitions/[competitionKey]`;
- `/competitions/[competitionKey]/seasons/[seasonLabel]`.

### Tipo principal
- UX/conteúdo;
- visual/layout;
- navegação/affordance.

### Problemas cobertos
- tela de competições excessivamente explicativa e com affordance ruim;
- `competition hub` com hero grande, linguagem arquitetural e blocos de baixo valor;
- `season hub` com muito texto interno e pouco conteúdo útil;
- áreas nobres ocupadas por escopo técnico, chips ambíguos e cards repetitivos;
- área branca clicável sem indicação clara na listagem de competições;
- mistura de rótulos PT/EN e painéis de ajuda que falam de registry, app paralelo, fluxo canônico e reaproveitamento de endpoint;
- `season hub` usa cartões e descrições para explicar a própria navegação em vez de aprofundar a temporada.

### Hipóteses iniciais
- as três telas compartilham a mesma lógica de framing e o mesmo excesso de metadado visual;
- o produto está explicando a estrutura do app em vez do campeonato/temporada;
- a affordance ruim nasce do mesmo card/container usado na descoberta de competições;
- a maior parte do problema é de priorização visual e conteúdo, não de ausência de endpoint;
- a superfície herdou copy de implementação para justificar `calendar`, `standings` e `rankings` como arquitetura, não como valor de produto.

### Dependências
- frontend e componentes compartilhados de hero/lista/tabs;
- dependência baixa de BFF, salvo se a investigação provar que falta dado mínimo para substituir texto filler;
- independente de módulos secundários.

### Ordem interna recomendada
- ajustar primeiro `/competitions`, porque ela é a porta de entrada do fluxo;
- corrigir depois o `competition hub` para reposicionar a narrativa do campeonato;
- fechar no `season hub`, já usando a nova hierarquia informacional como base e reduzindo os cards de ajuda estrutural.

### Risco de regressão
- médio, porque o bloco toca três superfícies relacionadas e provavelmente reutiliza componentes.

### Critério de conclusão
- cada tela explica o contexto esportivo com clareza e sem texto de arquitetura;
- a listagem de competições deixa claro o que é clicável;
- `competition hub` e `season hub` passam a privilegiar informação de produto e navegação útil;
- `calendar`, `standings` e `rankings` deixam de ser apresentados como prova de arquitetura.

### Observações
- não reabrir `standings`, `calendar` ou outras abas por iniciativa própria se o problema não emergir da investigação do próprio `season hub`.

## Bloco 7 — Home executiva e entrada do produto

### Escopo
- rota inicial em `frontend/src/app/(platform)/(home)`;
- topo da `home`;
- blocos principais da primeira dobra e sua hierarquia.

### Tipo principal
- UX/conteúdo;
- visual/layout.

### Problemas cobertos
- conteúdo fraco e pouco relevante na abertura;
- mensagem abstrata e cards com utilidade baixa;
- duplicação e excesso de contexto no topo;
- ruído visual logo na entrada do produto;
- repetição excessiva do framing `executivo`, `canônico` e `estrutural` em quase todos os painéis;
- pills de coverage e blocos de orientação continuam competindo com o conteúdo que deveria vender o produto.

### Hipóteses iniciais
- a `home` está tentando enquadrar o produto em excesso e entregar valor em falta;
- existem blocos que duplicam função de descoberta já resolvida em outras telas;
- o topo recebeu mais texto de framing do que dados ou CTA realmente úteis;
- a home herdou o mesmo padrão de painéis executivos/contextuais usado em outras superfícies e não filtrou o que realmente precisava aparecer na entrada.

### Dependências
- frontend;
- depende dos Blocos 5 e 6 para que os principais destinos da `home` já estejam coerentes;
- pode consumir o resultado do Bloco 8 para linguagem final, mas não precisa esperar por ele para reorganizar a hierarquia.

### Ordem interna recomendada
- auditar a primeira dobra e definir o que o usuário precisa entender nos primeiros segundos;
- eliminar duplicação conceitual no topo;
- promover dados, CTAs e sinais de descoberta realmente úteis;
- validar a nova abertura em desktop e mobile.

### Risco de regressão
- médio-baixo, porque o bloco é concentrado e não depende de contrato novo.

### Critério de conclusão
- a `home` comunica valor de produto rapidamente;
- o topo deixa de competir consigo mesmo;
- a primeira dobra passa a abrir com conteúdo útil e próximos passos claros;
- a abertura deixa de explicar arquitetura e passa a vender exploração real do produto.

## Bloco 8 — Shell global, estados globais e rotas legadas

### Escopo
- header global;
- `PlatformShellFrame` e `usePlatformShellState`;
- `GlobalFilterBar`, mensagens e banners compartilhados;
- overlay de busca no que for texto/semântica visual;
- `loading`, `error`, `not-found` e `PlatformStateSurface`;
- rotas auxiliares e legadas relevantes (`market`, `head-to-head`, `audit`, `coaches`);
- padrões repetidos de chips, labels, metadados e hierarquia visual.

### Tipo principal
- transversal;
- UX/conteúdo;
- visual/layout;
- navegação.

### Problemas cobertos
- navegação superior quebrando em duas linhas;
- linguagem técnica/interna exposta ao usuário;
- poluição visual e excesso de metainformação em várias telas;
- hierarquia visual destacando banners, labels e estados secundários acima do conteúdo útil;
- overlay de busca com linguagem de QA/operação;
- `GlobalFilterBar` e `PlatformShellFrame` falam de shell, contexto travado, filtros estruturais, helper text e modos internos;
- `usePlatformShellState` pode duplicar tags de contexto como `Rodada N`, além de produzir helper text demais;
- `GlobalError` expõe `error.message` e `digest` para o usuário;
- `loading`, `error`, `not-found` e rotas auxiliares/legadas têm cara de superfície provisória do produto, com textos como `fora do escopo`, `não materializado` e `rota legada`.

### Hipóteses iniciais
- a shell global e componentes de feedback ainda carregam vocabulário de engenharia;
- metadados compartilhados estão recebendo peso visual maior do que deveriam;
- o header não foi reequilibrado depois da convergência de navegação principal;
- alguns problemas locais observados em busca/rankings/resolvers são sintomas do mesmo componente compartilhado;
- os estados globais e rotas auxiliares foram desenhados para transparência técnica e reentrada segura, não para percepção final de produto.

### Dependências
- componentes compartilhados de shell, feedback e navegação;
- páginas globais e rotas auxiliares/legadas;
- deve reaproveitar o que já for confirmado nos Blocos 1 e 2 para não duplicar investigação;
- idealmente executado depois dos blocos de tela para evitar retrabalho de copy e hierarquia.

### Ordem interna recomendada
- corrigir layout do header e comportamento responsivo da navegação principal;
- revisar `PlatformShellFrame`, `GlobalFilterBar` e `scopeTags`, incluindo deduplicação semântica de contexto;
- definir blacklist de termos internos e substituir por linguagem de produto nos componentes compartilhados;
- revisar `loading/error/not-found` e impedir exposição de erro cru/digest;
- revisar `market`, `head-to-head`, `audit` e `coaches` para que deixem de parecer telas públicas provisórias;
- fazer a passada final de peso visual e affordance nos elementos realmente compartilhados.

### Risco de regressão
- médio-alto, porque toca componentes usados em várias telas e superfícies globais.

### Critério de conclusão
- a navegação principal fica estável nos breakpoints alvo;
- o app deixa de expor termos internos nas superfícies principais;
- banners, labels e metadados compartilhados perdem protagonismo indevido;
- estados globais e rotas auxiliares deixam de expor erro cru, digest ou copy de escopo técnico.

### Observações
- este bloco não deve reabrir páginas já fechadas, exceto quando a correção vier de um componente compartilhado comprovadamente reutilizado;
- rotas auxiliares e legadas devem ser tratadas como reentrada segura ou desvio controlado, não como abertura de nova frente de produto.

# 5. Blocos transversais

Os blocos transversais explícitos deste plano são:

- Bloco 1, porque une `search` e resolvers de entidade na mesma cadeia de contexto canônico. Aqui o acoplamento é intencional e reduz retrabalho entre BFF, routing e shell.
- Bloco 3, porque o contrato de identidade do jogador atravessa mais de uma superfície de descoberta e deve ser resolvido uma vez na origem certa.
- Bloco 8, porque concentra linguagem de produto, shell, feedback, estados globais e hierarquia compartilhada.

Regra para evitar duplicação:
- se um problema continuar local a uma tela, ele fica no bloco da tela;
- só sobe para bloco transversal quando a investigação provar reuso real do componente, da regra ou do contrato.

# 6. Ordem final recomendada

1. Bloco 1 — Busca global e resolução canônica de entidades.
Justificativa: é a quebra mais sensível de confiança e compartilha a mesma cadeia entre busca, deep link e contexto canônico.

2. Bloco 2 — Match center: estabilidade, tags e enquadramento do detalhe.
Justificativa: erro visível de runtime em fluxo central, com evidência de acoplamento à shell e alto impacto percebido.

3. Bloco 3 — Contrato de identidade de jogadores.
Justificativa: fecha um gap real de dado/contrato antes de redesenhar superfícies que dependem dessa informação.

4. Bloco 4 — Rankings: filtro, cards e coverage.
Justificativa: consolida em uma única passada a usabilidade, a composição visual e a semântica da tela, já com o contrato de jogador estabilizado se necessário.

5. Bloco 5 — Descoberta e perfis de teams, players e matches.
Justificativa: a varredura mostrou que listas e perfis desses domínios repetem o mesmo excesso de framing estrutural e metadado operacional.

6. Bloco 6 — Descoberta de competições, competition hub e season hub.
Justificativa: são três superfícies da mesma família com o mesmo problema estrutural de conteúdo, hierarquia e affordance.

7. Bloco 7 — Home executiva e entrada do produto.
Justificativa: a `home` deve ser reposicionada depois que as principais superfícies de destino estiverem coerentes.

8. Bloco 8 — Shell global, estados globais e rotas legadas.
Justificativa: passada final de padronização em componentes compartilhados e de limpeza de estados/rotas provisórias, sem reabrir investigação de causa raiz que já deveria estar resolvida.

# 7. Próximo uso correto do plano

O uso correto deste plano é:

- investigar um bloco por vez com evidência objetiva;
- confirmar se o problema é de frontend, BFF, dados, contrato, resolver ou combinação real dessas camadas;
- executar correção mínima e cirúrgica dentro do bloco;
- validar o bloco antes de seguir para o próximo, incluindo regressão nos pontos compartilhados que ele tocar.

Se durante a investigação aparecer uma causa diferente da hipótese inicial, o bloco deve ser reclassificado, não forçado.
