# Plano de execução — reconstrução de Jogadores e Clubes

> Documento operacional para execução por IA.
>
> Este plano é a fonte de verdade da tarefa. O agente executor deve lê-lo por
> completo antes de alterar arquivos. Decisões marcadas como fechadas não devem
> ser reabertas durante a implementação.

---

## 1. Objetivo

Reconstruir integralmente as páginas de jogadores e clubes para que funcionem
como superfícies de descoberta do acervo publicado pela plataforma, e não como
rankings implícitos ou dashboards densos.

A entrega inclui, no mesmo escopo:

- catálogo de jogadores;
- perfil global e perfil contextual de jogador;
- catálogo de clubes;
- perfil global e perfil contextual de clube;
- comparação já existente;
- filtros globais e locais;
- rotas canônicas e aliases antigos;
- contratos de dados, API e cobertura necessários;
- responsividade, acessibilidade, testes, integração e deploy.

Não existe divisão entre “primeira versão” e “versão completa”. A implementação
é organizada em gates apenas para impedir conflito entre agentes e retrabalho.
Todos os itens deste documento pertencem à mesma entrega.

---

## 2. Como usar este documento

Há dois papéis de execução com responsabilidades diferentes.

| Papel | Nome neste documento | Responsabilidade |
|---|---|---|
| Modelo/subagentes de menor custo | **Modelo F — Fundação** | Dados, dbt, relevância, API, tipos, hooks, rotas, aliases, linguagem não visual, testes de contrato e integração |
| Modelo de maior capacidade/custo | **Modelo V — Visual** | Arquitetura de informação das telas, componentes, hierarquia, composição, estilo, responsividade, acessibilidade e iteração visual |

O orquestrador deve informar explicitamente ao agente qual papel ele está
executando. Um agente não deve assumir os dois papéis na mesma passagem.

### Instrução de entrada por papel

- Se o papel ativo for **Modelo F**, executar as seções 11.1 a 11.9, validar o
  Gate F e produzir o pacote da seção 14. Encerrar o handoff sem iniciar o
  redesign visual.
- Se o papel ativo for **Modelo V**, confirmar que o pacote da seção 14 existe,
  executar as seções 12.1 a 12.7 e validar o Gate V. Não alterar fundação.
- Se o papel ativo for **Orquestrador/QA**, garantir a sequência
  `Modelo F → Gate F → Modelo V → Gate V → integração → aprovação → deploy`.
- Não executar Modelo F e Modelo V simultaneamente no mesmo worktree. Se houver
  paralelismo, usar branches/worktrees separados e integrar somente nos gates.

### Regra principal de coordenação

O Modelo V só começa a implementar depois que o Modelo F concluir o **Gate F**
e entregar o pacote de handoff definido na seção 14.

O Modelo V não deve investigar ou modificar backend para “destravar” uma tela.
Se o contrato congelado for insuficiente, ele deve registrar um
`CONTRACT_GAP`, explicar o campo ou comportamento ausente e devolver o problema
ao Modelo F.

O Modelo F não deve decidir o layout final nem fazer redesign visual. Ele deve
entregar dados e integrações previsíveis para que o Modelo V possa concentrar
seu orçamento na qualidade da interface.

---

## 3. Estado inicial que deve ser reconfirmado

Na elaboração deste plano, o repositório estava em:

- branch: `master`;
- commit de referência: `5e96234 feat: restore rankings and isolate Power BI`;
- páginas existentes de jogadores e times já funcionais, porém visual e
  semanticamente inadequadas;
- arquivos grandes não rastreados e alheios ao redesign presentes no worktree.

Antes de executar, o primeiro agente deve reconfirmar `git status`, branch e
HEAD. Não deve apagar, mover, adicionar ao commit ou sobrescrever arquivos não
rastreados que já estavam no workspace.

### Restrições herdadas do produto

- Rankings continuam em rotas próprias.
- Power BI continua em seção própria.
- Não substituir rankings por páginas de jogadores ou clubes.
- Preservar Copa de 2026, IDs canônicos e correções recentes de entidades.
- Não fazer revert amplo.
- Não alterar ingestão ou banco fora do que for necessário para os contratos
  definidos neste plano.

---

## 4. Decisões de produto fechadas

| Tema | Decisão fechada |
|---|---|
| Função de `/players` | Descobrir nomes e explorar carreiras |
| Função de `/clubs` | Descobrir clubes e explorar suas trajetórias |
| Escopo inicial | Todo o acervo publicado, sem filtro ativo |
| Aplicação de filtros | Os filtros reduzem o acervo; não existe recorte silencioso inicial |
| Paginação | “Todo o acervo” significa todo o universo consultável, não renderizar milhares de itens de uma vez |
| Ordenação padrão | Relevância documental determinística |
| Significado de relevância | Carreiras e trajetórias mais documentadas, não melhores ou maiores da história |
| Explicação da relevância | Implícita; não mostrar score, posição nem “por que aparece” em cada item |
| Destaque individual inicial | Proibido destacar automaticamente um jogador ou clube como principal |
| Mensagem inicial | Escala e proposta do acervo da plataforma |
| Limite histórico | Informar que o acervo publicado não representa tudo que existe no futebol |
| Perfil de jogador | Clubes e trajetória antes da densidade estatística |
| Perfil de clube | Identidade, origem e conquistas antes da densidade estatística |
| Comparação | Continua existindo, com prioridade visual secundária |
| Linguagem pública | “Clubes” quando a entidade for clube |
| Rota pública | `/clubs` é canônica |
| Rotas antigas | `/teams` permanece como camada de compatibilidade e resolução de tipo; nunca redireciona seleção para `/clubs` |
| Estilo | Linguagem própria da plataforma; não imitar uma referência externa |
| Método de entrega | Reconstrução completa com iterações visuais até aprovação |

Essas decisões não devem ser convertidas em novas perguntas durante a execução.

---

## 5. Diagnóstico do estado atual

### 5.1 Acertos estruturais que devem ser preservados

- Filtros globais já transportam competição, temporada, rodada, fase, mando e
  intervalo temporal.
- Existem rotas globais e contextualizadas para perfis.
- Jogadores já possuem lista, perfil, histórico, partidas, tendências e
  comparação.
- Clubes já possuem lista, perfil, jornada, elenco, partidas, estatísticas e
  uma estrutura inicial de conquistas.
- React Query, serviços, query keys e tipos já separam parte da integração.
- O BFF do Next.js funciona como proxy genérico; não há motivo conhecido para
  criar outro proxy específico.
- Rankings e Power BI já estão separados no commit de referência.

### 5.2 Problemas que a entrega deve resolver

1. `/players` abre ordenado por gols e transforma o primeiro resultado em
   “Destaque da lista”.
2. `/teams` abre ordenado por pontos e transforma a agregação global do acervo
   em uma classificação aparente.
3. As duas listas misturam catálogo, ranking, comparação e dashboard no mesmo
   primeiro viewport.
4. O perfil do jogador concentra métricas demais antes de contar a carreira.
5. O perfil do clube não possui uma identidade histórica suficientemente
   estruturada.
6. A listagem de jogadores não expõe cobertura com o mesmo cuidado das demais
   superfícies.
7. O bloco de insights de jogador exibe falha de endpoint não implementado.
8. `/clubs` redireciona incorretamente para `/competitions`, enquanto `/teams`
   continua sendo a rota principal e também pode receber IDs de seleções.
9. O domínio interno `team` pode representar clubes, seleções e outras equipes;
   a interface não pode chamar todas essas entidades de clube.
10. O preview de conquistas de clubes é local, restrito e não serve como fonte
    final do produto.

---

## 6. Arquitetura de execução e propriedade de arquivos

### 6.1 Modelo F — Fundação

Pode alterar:

- `platform/dbt/models/marts/analytics/`;
- testes e schemas dbt diretamente relacionados;
- migration necessária para os modelos de serving;
- `api/src/routers/players.py`;
- `api/src/routers/teams.py`;
- `api/src/routers/home.py`;
- busca global apenas se for necessário expor `teamType`;
- testes de API relacionados;
- `frontend/src/features/players/types/`;
- `frontend/src/features/players/services/`;
- `frontend/src/features/players/hooks/`;
- `frontend/src/features/players/queryKeys.ts`;
- `frontend/src/features/teams/types/`;
- `frontend/src/features/teams/services/`;
- `frontend/src/features/teams/hooks/`;
- `frontend/src/features/teams/queryKeys.ts`;
- rotas `clubs`, `teams` e rotas contextuais correspondentes;
- utilitários de roteamento, shell state, busca e filtros globais quando a
  alteração for exclusivamente semântica ou de navegação;
- testes não visuais do frontend.

Não pode:

- redesenhar páginas ou componentes visuais;
- escolher layout, cores, tipografia ou animações;
- criar painéis apenas porque os dados existem;
- alterar rankings ou Power BI, exceto para reparar regressão introduzida por
  esta execução.

### 6.2 Modelo V — Visual

Pode alterar:

- `frontend/src/app/(platform)/players/page.tsx`;
- `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`;
- componentes visuais em `frontend/src/features/players/components/`;
- `frontend/src/features/teams/components/TeamsPageContent.tsx`;
- `frontend/src/features/teams/components/TeamProfileContent.tsx`;
- `frontend/src/features/teams/components/TeamAggregateProfileContent.tsx`;
- demais componentes visuais em `frontend/src/features/teams/components/`;
- estilos locais necessários;
- novos componentes visuais específicos de jogadores e clubes, somente quando
  houver reuso real ou separação clara de responsabilidade.

Pode inspecionar, mas não alterar:

- contratos TypeScript congelados pelo Modelo F;
- services, hooks e query keys;
- API, dbt e migrations;
- regras de relevância;
- aliases e builders de rota;
- shell global, salvo autorização expressa do orquestrador para um ajuste
  estritamente visual.

### 6.3 Arquivos com risco de conflito

Os componentes de página atuais misturam estado e visual. Para evitar edições
concorrentes:

1. O Modelo F conclui tipos, hooks, services e rotas primeiro.
2. O Modelo F não altera `PlayersPage`, `TeamsPageContent` ou os conteúdos de
   perfil apenas para trocar o layout.
3. O Modelo V recebe os hooks já compatíveis e configura nas páginas o estado
   inicial `sortBy: "relevance"`.
4. Depois do início do Modelo V, nenhum subagente modifica os arquivos visuais
   permitidos sem coordenar o handoff.

---

## 7. Mapa final de rotas

| Função | Rota canônica | Comportamento |
|---|---|---|
| Catálogo de jogadores | `/players` | Renderiza o catálogo global ou filtrado |
| Perfil global de jogador | `/players/[playerId]` | Resolve o melhor contexto sem mudar a identidade global |
| Perfil contextual de jogador | `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]` | Exibe o perfil no contexto explícito |
| Catálogo de clubes | `/clubs` | Renderiza o catálogo de clubes |
| Perfil global de clube | `/clubs/[clubId]` | Resolve o perfil agregado/canônico |
| Perfil contextual de clube | `/competitions/[competitionKey]/seasons/[seasonLabel]/clubs/[clubId]` | Exibe o clube no contexto explícito |
| Alias de catálogo | `/teams` | Redirect permanente para `/clubs`, preservando query string |
| Resolver legado de perfil | `/teams/[teamId]` | Resolve `teamType`: clube vai para `/clubs/[teamId]`; seleção vai para sua rota de seleção quando houver; tipo desconhecido permanece em superfície neutra de compatibilidade |
| Resolver contextual legado | `.../teams/[teamId]` | Clube vai para a rota contextual `.../clubs/[teamId]`; seleção ou entidade não-clube permanece na rota neutra/contextual compatível |

### Regras de nomenclatura

- Navegação, catálogo e perfis de entidade usam “Clubes”.
- Seleções nacionais usam “Seleções”.
- Quando o tipo da entidade não for conhecido e o contexto puder misturar
  clubes e seleções, usar “Equipes” até a classificação ser resolvida.
- `/clubs/[clubId]` deve validar que a entidade é `club`. Uma seleção acessada
  nessa rota deve ser redirecionada à superfície correta quando houver
  mapeamento seguro, ou resultar em estado não encontrado; nunca deve renderizar
  um perfil rotulado como clube.
- Links de carreira de jogador dependem de `teamType`: clube usa `/clubs`,
  seleção usa a rota de seleção disponível e tipo desconhecido não recebe link
  de clube.
- Não fazer substituição textual global de “time”. Em partidas, “time da casa”,
  “mandante”, “visitante” ou “equipes” podem continuar semanticamente corretos.
- Os endpoints internos `/api/v1/teams` e o diretório `features/teams` permanecem
  com seus nomes atuais.

---

## 8. Contrato de relevância documental

### 8.1 Regras gerais

- `relevance` deve ser um valor válido de `sortBy` para jogadores e times.
- A ordenação padrão é `sortBy=relevance&sortDirection=desc`.
- Não retornar nem exibir `relevanceScore`, posição ou justificativa.
- A ordem deve ser estável entre requisições idênticas.
- A relevância deve ser calculada no servidor sobre todo o universo filtrado,
  nunca no cliente sobre a página atual.
- Ao aplicar competição, temporada, fase, rodada, mando ou período, os sinais
  documentais devem ser recalculados dentro desse recorte.
- Ordenações explícitas por gols, assistências, nome, pontos ou outras métricas
  continuam disponíveis, mas deixam de ser o padrão.

### 8.2 Jogadores

Ordenação lexicográfica padrão:

1. `season_count DESC`;
2. `competition_count DESC`;
3. `team_count DESC`;
4. `matches_played DESC`;
5. `minutes_played DESC`;
6. `career_end_at DESC NULLS LAST`;
7. `player_id ASC`.

Essa ordem representa profundidade documental. Gols, assistências e rating não
participam da relevância padrão.

### 8.3 Clubes

Ordenação lexicográfica padrão:

1. `season_count DESC`;
2. `competition_count DESC`;
3. `matches_played DESC`;
4. `first_match_at ASC NULLS LAST`;
5. `last_match_at DESC NULLS LAST`;
6. `team_id ASC`.

Pontos, saldo, vitórias, posição e quantidade de títulos não participam da
relevância padrão. A cobertura atual de títulos não é uniforme o suficiente
para influenciar a descoberta.

### 8.4 Relevância não é ranking

- O frontend pode chamar a ordenação de “Relevância”.
- O frontend não deve numerar os resultados.
- O frontend não deve usar frases como “mais relevante”, “líder”, “melhor” ou
  “maior do acervo”.
- Em escopo global, `position` e `totalTeams` não devem aparecer na interface.
- Posição esportiva só é válida quando houver contexto de classificação
  explícito, como competição e temporada.

---

## 9. Contratos de dados alvo

Os nomes abaixo são o contrato alvo entre os dois modelos. O Modelo F pode
preservar campos antigos para compatibilidade, mas não deve renomear os campos
novos sem atualizar este documento e comunicar o handoff.

### 9.1 Escopo de catálogo

```ts
interface CatalogScope {
  kind: "archive" | "filtered";
  label: string;
  isExhaustive: false;
  updatedAt?: string | null;
}
```

`isExhaustive: false` significa que o endpoint representa o acervo publicado,
não toda a história do futebol. Isso é diferente de `meta.coverage`, que mede a
disponibilidade dentro das fontes publicadas.

### 9.2 Resumo global do acervo

Expandir o contrato já existente de `/api/v1/home`:

```ts
interface HomeArchiveSummary {
  competitions: number;
  seasons: number;
  matches: number;
  players: number;
  clubs: number;
}
```

Não criar um endpoint novo apenas para esses contadores.

### 9.3 Lista de jogadores

Adicionar ao item atual:

```ts
interface PlayerListItem {
  // campos atuais preservados
  competitionCount: number;
  seasonCount: number;
  careerStartAt?: string | null;
  careerEndAt?: string | null;
}

interface PlayersListData {
  items: PlayerListItem[];
  scope: CatalogScope;
}
```

O total do resultado atual continua em `meta.pagination.totalCount`.

### 9.4 Carreira do jogador

```ts
type CareerTeamType = "club" | "national_team" | "representative" | "other" | "unknown";

interface PlayerCareerTeam {
  teamId: string;
  teamName: string;
  teamType: CareerTeamType;
  competitionCount: number;
  seasonCount: number;
  matchesPlayed: number;
  minutesPlayed: number;
  goals: number;
  assists: number;
  firstMatchAt?: string | null;
  lastMatchAt?: string | null;
}

interface PlayerCareerSummary {
  teamCount: number;
  clubCount: number;
  nationalTeamCount: number;
  competitionCount: number;
  seasonCount: number;
  firstMatchAt?: string | null;
  lastMatchAt?: string | null;
  teams: PlayerCareerTeam[];
}
```

Adicionar `career: PlayerCareerSummary` ao perfil atual. Manter `history`,
`recentMatches`, `stats` e `sectionCoverage` para as leituras detalhadas.

### 9.5 Lista de clubes

Adicionar ao item atual:

```ts
type TeamType = "club" | "national_team" | "representative" | "other" | "unknown";

interface TeamListItem {
  // campos atuais preservados
  teamType: TeamType;
  countryOrTerritory?: string | null;
  competitionCount: number;
  seasonCount: number;
  firstMatchAt?: string | null;
  lastMatchAt?: string | null;
  stadiumName?: string | null;
}

interface TeamsListData {
  items: TeamListItem[];
  scope: CatalogScope;
}
```

`/clubs` deve solicitar `entityType=club`. Nenhuma seleção conhecida pode ser
emitida nesse resultado.

### 9.6 Perfil de clube

```ts
interface TeamIdentity {
  teamType: TeamType;
  officialName: string;
  countryOrTerritory?: string | null;
  city?: string | null;
  foundedYear?: number | null;
  stadiumName?: string | null;
  stadiumCapacity?: number | null;
}

interface TeamArchiveSummary {
  competitionCount: number;
  seasonCount: number;
  matchesPlayed: number;
  firstMatchAt?: string | null;
  lastMatchAt?: string | null;
}

interface ClubHonor {
  competitionName: string;
  competitionKey?: string | null;
  scope: "mundial" | "continental" | "nacional" | "estadual" | "other";
  seasonLabel?: string | null;
  year?: number | null;
  sourceName: string;
  sourceUrl?: string | null;
  confidence: "high" | "medium" | "low";
}

interface ClubHonors {
  criterionLabel: string;
  total: number;
  items: ClubHonor[];
  coverage: CoverageState;
}
```

Adicionar ao perfil genérico atual:

- `identity: TeamIdentity`;
- `archive: TeamArchiveSummary`;
- `honors?: ClubHonors | null`;
- cobertura de `identity` e `honors` em `sectionCoverage`.

A rota `/clubs/[clubId]` só monta esse perfil quando
`identity.teamType === "club"`. O endpoint interno pode continuar atendendo
outros tipos de equipe para compatibilidade e para superfícies de seleções.

### 9.7 Semântica de cobertura

- `scope.isExhaustive` responde se o acervo representa toda a história real:
  sempre `false` nesta entrega.
- `meta.coverage` responde se a consulta foi atendida dentro do acervo
  publicado.
- `sectionCoverage.identity` responde quais campos de identidade estão
  disponíveis.
- `sectionCoverage.honors` responde a disponibilidade das conquistas
  documentadas.
- O frontend não pode converter cobertura parcial em afirmação totalizante.

---

## 10. Dados mínimos por tela

| Tela | Dados bloqueadores | Dados condicionais | Não deve depender de |
|---|---|---|---|
| `/players` | `archiveSummary`, paginação, `scope`, nome, IDs, `seasonCount`, `competitionCount`, `teamCount`, jogos e intervalo da carreira | posição, nacionalidade, imagens, gols, assistências, rating | insights, prêmios individuais, score público de relevância |
| Perfil de jogador | identidade, `career`, histórico e cobertura | imagem, nacionalidade, partidas recentes, stats, Copa | endpoint de insights não implementado |
| `/clubs` | `archiveSummary.clubs`, paginação, `scope`, `teamType=club`, nome, temporadas, competições, partidas e intervalo | país, estádio, escudo | posição global, pontos globais, títulos como fator de relevância |
| Perfil de clube | identidade mínima com `teamType=club`, `archive`, jornada e cobertura | cidade, fundação, estádio, conquistas, elenco, stats | preview hardcoded de apenas um clube como fonte final |

### Estados obrigatórios em todas as telas

- carregamento inicial;
- atualização silenciosa após filtro;
- erro com retry;
- resultado vazio por filtro;
- perfil inexistente;
- dado parcial;
- imagem ausente;
- rota antiga redirecionada;
- acesso direto sem navegação prévia.

---

## 11. Execução do Modelo F — Fundação

O Modelo F pode distribuir os blocos abaixo entre subagentes, desde que dois
agentes não editem o mesmo arquivo simultaneamente.

### F0 — Preflight e baseline

1. Ler este documento por completo.
2. Reconfirmar branch, HEAD e worktree.
3. Inventariar consumidores de:
   - `GET /api/v1/players`;
   - `GET /api/v1/players/{id}`;
   - `GET /api/v1/teams`;
   - `GET /api/v1/teams/{id}`;
   - builders de `/teams`;
   - tipos de listas e perfis.
4. Registrar testes existentes e comandos oficiais de build/deploy.
5. Não editar nada visual neste bloco.

**Saída:** lista de consumidores e riscos de compatibilidade.

### F1 — Serving e relevância

1. Expandir `player_serving_summary` com os sinais da seção 8.2.
2. Expandir `team_serving_summary` com os sinais da seção 8.3.
3. Adicionar índices apenas para os padrões de ordenação realmente usados.
4. Adicionar testes dbt de:
   - unicidade por entidade;
   - contagens não negativas;
   - datas coerentes;
   - `first <= last`;
   - não nulidade dos campos bloqueadores.
5. Implementar a mesma semântica para consultas filtradas que não usam os
   summaries globais.

**Aceite:** a mesma consulta repetida retorna a mesma ordem, e o recorte muda os
sinais documentais de forma coerente.

### F2 — Classificação de clubes

1. Reutilizar `control.team_identity.team_type` e o crosswalk canônico.
2. Auditar quantos registros estão classificados como `club`,
   `national_team`, `representative`, `other` e desconhecidos.
3. Verificar se o bootstrap marcou entidades como clube sem evidência posterior.
4. Resolver classificações somente com evidência existente ou verificável.
5. Não alterar IDs canônicos nem reescrever fatos.
6. Garantir que toda entidade retornada para `entityType=club` esteja
   explicitamente classificada como `club`.
7. Garantir que nenhuma seleção conhecida apareça em `/clubs`.

Não existe meta percentual arbitrária. O critério de correção é: todo item
emitido como clube precisa ter classificação explícita; desconhecidos devem ser
reportados, não adivinhados.

### F3 — Identidade e conquistas de clubes

1. Reusar primeiro:
   - `control.team_identity`;
   - `raw.tm_clubs`;
   - país da competição doméstica quando confiável;
   - estádios documentados;
   - `api/data/team_honors_seed.csv`;
   - campeões determináveis a partir do acervo publicado.
2. Preservar fonte e confiança de cada conquista.
3. Descrever conquistas como documentadas no acervo.
4. Não inferir fundação, cidade ou título sem fonte.
5. Campos sem fonte ficam nulos; isso não bloqueia o perfil.
6. Substituir o loader local/hardcoded de preview por uma fonte canônica
   consumida pela API, mantendo compatibilidade apenas durante a migração.
7. Não usar quantidade de títulos na relevância padrão.

**Aceite:** um clube com conquistas verificadas recebe `honors`; um clube sem
cobertura recebe `honors: null` ou cobertura vazia sem erro de interface.

### F4 — API de jogadores

1. Adicionar `relevance` ao enum de ordenação.
2. Tornar `relevance DESC` o padrão do endpoint.
3. Implementar a ordem global no serving summary.
4. Implementar a ordem recalculada no caminho filtrado.
5. Adicionar `scope` e coverage à lista.
6. Adicionar os campos documentais aos itens.
7. Adicionar `career` ao perfil, agrupando passagens por equipe e distinguindo
   clubes de seleções.
8. Preservar filtros e ordenações anteriores.
9. Preservar perfis locais da Copa e perfis sem histórico estatístico.
10. Não implementar um endpoint de insights para sustentar o layout antigo.

### F5 — API de clubes

1. Adicionar `relevance` ao enum de ordenação.
2. Tornar `relevance DESC` o padrão do catálogo.
3. Adicionar `entityType`, aceitando ao menos `club`.
4. Aplicar classificação antes da paginação e do `totalCount`.
5. Adicionar `scope`, coverage e campos documentais.
6. Não calcular posição global por pontos quando `sortBy=relevance`.
7. Manter ordenações esportivas explícitas para consumidores compatíveis.
8. Adicionar `identity`, `archive`, `honors` e coberturas ao perfil.
9. Preservar jornada, elenco, partidas e estatísticas atuais.
10. Manter o perfil interno capaz de serializar seleções e outros tipos sem
    rotulá-los como clube.

### F6 — Resumo do acervo

1. Adicionar `clubs` a `HomeArchiveSummary`.
2. Calcular clubes com a mesma classificação usada por `entityType=club`.
3. Não contar seleções como clubes.
4. Preservar competições, temporadas, partidas e jogadores atuais.
5. Reusar o endpoint e hook existentes.

### F7 — Tipos, services e hooks

1. Atualizar os contratos TypeScript da seção 9.
2. Serializar `sortBy=relevance` nos services.
3. Preservar query keys sensíveis a todos os filtros.
4. Expor `scope`, pagination e coverage nos hooks.
5. Não calcular relevância, carreira ou agrupamentos históricos no cliente.
6. Não criar dependência nova para essa adaptação.

### F8 — Rotas e linguagem

1. Tornar `/clubs` a página canônica de catálogo.
2. Criar a rota contextual de clube.
3. Transformar `/teams` em redirect permanente de catálogo para `/clubs`.
4. Manter `/teams/[teamId]` como resolver compatível: clube redireciona para
   `/clubs`, seleção redireciona para sua superfície quando houver mapeamento e
   tipo desconhecido usa uma superfície neutra.
5. Aplicar a mesma resolução de tipo às rotas contextuais antigas.
6. Preservar query string e parâmetros contextuais.
7. Atualizar builders, links de comparação, filtros e shell state.
8. Atualizar a busca global para diferenciar clube e seleção quando o tipo
   estiver disponível.
9. Aplicar as regras de nomenclatura da seção 7.
10. Não alterar o design do shell.

### F9 — Testes de contrato

Cobertura mínima:

- lista global de jogadores usa relevância;
- lista filtrada recalcula relevância;
- paginação de jogadores é estável;
- lista de clubes exclui seleções;
- total de clubes usa a mesma classificação do catálogo;
- lista filtrada de clubes recalcula relevância;
- posição por pontos não é apresentada como posição global de relevância;
- perfil de jogador serializa `career`;
- perfil de clube serializa identidade, arquivo e conquistas condicionais;
- perfis incompletos continuam retornando 200 com coverage correto;
- rotas canônicas funcionam por acesso direto;
- aliases preservam query string e resolvem o tipo da entidade corretamente;
- uma seleção nunca é redirecionada nem renderizada como `/clubs/[id]`;
- busca e comparação geram links canônicos.

### Gate F — Fundação pronta

O Gate F só está concluído quando:

- dbt e/ou migrations necessárias estão aplicáveis e testadas;
- testes de API direcionados estão verdes;
- tipos, services e hooks compilam;
- contratos da seção 9 estão implementados;
- `/clubs` e aliases estão funcionais;
- o pacote de handoff da seção 14 foi produzido;
- nenhuma tela visual foi redesenhada pelo Modelo F.

---

## 12. Execução do Modelo V — Visual

O Modelo V deve começar lendo:

1. este documento;
2. o pacote de handoff do Modelo F;
3. as páginas atuais de jogadores e clubes;
4. as superfícies já aprovadas da plataforma, especialmente shell, home,
   competições, rankings e perfis relacionados.

Ele deve usar os dados reais do ambiente local. Fixtures servem para cobrir
estados raros, não para substituir a integração.

### V0 — Auditoria visual curta

Antes de editar:

1. abrir as quatro superfícies em desktop e mobile;
2. registrar screenshots do estado anterior;
3. identificar tokens, tipografia, espaçamento, containers, imagens e
   interações já usados pela plataforma;
4. confirmar que o Gate F está realmente integrado;
5. reportar `CONTRACT_GAP` se algum dado bloqueador da seção 10 estiver ausente.

Não produzir um novo plano conceitual. As decisões de produto já estão
fechadas; esta auditoria serve para orientar a execução visual.

### V1 — Catálogo de jogadores

Objetivo de cinco segundos: o usuário entende que está diante de um grande
acervo de jogadores e pode buscar nomes ou explorar carreiras.

Obrigatório:

- mostrar escala do acervo e resultado do recorte atual;
- mensagem curta de que o acervo não é exaustivo;
- busca por nome com alta prioridade;
- filtros globais e locais acessíveis sem dominar o primeiro viewport;
- resultados em relevância documental por padrão;
- nenhum jogador como hero ou “destaque da lista”;
- nenhuma numeração de ranking;
- poucos fatos por item, escolhidos por utilidade de descoberta;
- ordenações métricas disponíveis de forma secundária;
- comparação preservada como ação secundária;
- paginação real;
- loading, atualização, erro e vazio coerentes.

O Modelo V pode escolher lista, cartões ou estrutura híbrida. Não deve manter a
tabela atual apenas por ser o caminho mais curto.

### V2 — Perfil do jogador

Hierarquia de conteúdo obrigatória:

1. identidade;
2. clubes da carreira;
3. seleções, quando houver, sem chamá-las de clube;
4. trajetória por temporadas e competições;
5. síntese de partidas, gols e assistências;
6. partidas recentes;
7. estatísticas e tendências;
8. comparação.

Regras:

- clubes devem ser a principal leitura estrutural;
- não repetir oito KPIs no hero;
- distinguir total de carreira documentada e recorte ativo;
- prêmios individuais inexistentes não geram placeholder;
- remover a chamada e o bloco visual de insights não implementados;
- perfis de Copa e perfis parciais devem parecer estados válidos do produto;
- cobertura parcial deve ser informativa e discreta, sem banners repetidos em
  todas as seções.

### V3 — Catálogo de clubes

Objetivo de cinco segundos: o usuário entende quantos clubes estão documentados
e pode explorar suas trajetórias.

Obrigatório:

- usar “Clubes”;
- usar o total classificado de clubes;
- busca e filtros;
- relevância documental por padrão;
- nenhum clube como destaque automático;
- nenhuma posição ou pontuação global;
- identidade e presença histórica antes de desempenho agregado;
- não apresentar seleções;
- comparação/head-to-head preservada por link ou ação secundária;
- estados de loading, atualização, erro e vazio.

### V4 — Perfil do clube

Hierarquia de conteúdo obrigatória:

1. nome, escudo e identidade;
2. origem disponível e verificada;
3. conquistas documentadas;
4. trajetória por competições e temporadas;
5. elenco;
6. partidas;
7. estatísticas.

Regras:

- não inventar cidade, fundação ou estádio;
- omitir campos nulos sem deixar buracos visuais;
- conquistas devem deixar claro o critério “documentadas no acervo”;
- ausência de conquistas no contrato não significa “zero títulos na história”;
- o perfil global e o contextual devem compartilhar identidade, mas deixar o
  recorte atual explícito;
- reduzir mosaico de métricas no primeiro viewport.

### V5 — Comparação

- Reusar o mecanismo existente de comparação de jogadores.
- Reusar o head-to-head/comparação existente para clubes.
- Não construir um terceiro motor de comparação.
- O seletor ou ação de comparar não pode competir com busca e descoberta.
- Links de saída devem usar rotas canônicas.

### V6 — Responsividade e acessibilidade

Validar no mínimo:

- 390 px;
- 768 px;
- 1440 px.

Obrigatório:

- ordem de leitura correta;
- foco visível;
- operação por teclado;
- rótulos acessíveis para filtros e comparação;
- contraste adequado;
- nomes longos sem quebrar layout;
- imagens com fallback;
- sem overflow horizontal;
- skeletons com geometria próxima do conteúdo final;
- `prefers-reduced-motion` respeitado se houver animação.

### V7 — Loop visual

1. Capturar as quatro páginas nas três larguras.
2. Comparar hierarquia, densidade e consistência.
3. Corrigir problemas visuais encontrados.
4. Repetir até a experiência estar coesa.
5. Apresentar ao usuário para aprovação.
6. Aplicar ajustes solicitados e repetir a validação.

O Modelo V não encerra após a primeira renderização funcional.

### Gate V — Visual pronto

O Gate V só está concluído quando:

- as quatro superfícies foram reconstruídas;
- desktop e mobile foram inspecionados visualmente;
- todos os estados obrigatórios foram contemplados;
- não há `CONTRACT_GAP` pendente;
- typecheck e build passam;
- screenshots de evidência foram produzidos;
- o usuário aprovou a direção visual.

---

## 13. Integração final, QA e deploy

Depois do Gate V, o Modelo F ou um agente de QA de menor custo executa a
validação integrada. Ele não redesenha a interface; defeitos visuais voltam ao
Modelo V.

### 13.1 Testes técnicos

Executar os comandos oficiais encontrados no repositório. A cobertura mínima
inclui:

- testes dbt direcionados a `player_serving_summary` e
  `team_serving_summary`;
- testes de API de jogadores, clubes/times, home e busca;
- testes de contrato de rotas;
- testes frontend existentes relacionados;
- `pnpm typecheck`;
- `pnpm build`;
- validação local das páginas reais via navegador.

Não inventar um novo framework de testes. Reusar pytest, testes Node e
ferramentas já instaladas.

### 13.2 Cenários funcionais obrigatórios

1. `/players` sem filtros.
2. `/players` com busca.
3. `/players` com competição e temporada.
4. Perfil global de jogador com histórico completo.
5. Perfil de jogador com cobertura parcial/Copa.
6. Comparação de jogadores.
7. `/clubs` sem filtros.
8. `/clubs` com busca.
9. `/clubs` com competição e temporada.
10. Perfil global de clube com identidade/conquistas.
11. Perfil de clube sem identidade completa ou conquistas.
12. Perfil contextual de clube.
13. Head-to-head/comparação de clubes.
14. Resolver legado de `/teams` preservando query e distinguindo clube de
    seleção.
15. Busca global gerando links canônicos.

### 13.3 Regressões obrigatórias

Validar que continuam funcionais:

- `/rankings`;
- páginas individuais de rankings;
- `/power-bi`;
- `/copa-do-mundo/rankings`;
- rotas e dados da Copa de 2026;
- IDs canônicos e assets de jogadores/clubes.

### 13.4 Deploy

Somente depois de aprovação visual:

1. revisar diff e arquivos não rastreados;
2. separar commits por responsabilidade quando isso melhorar rastreabilidade;
3. executar CI pelo fluxo oficial;
4. aguardar CI verde;
5. executar deploy pelo mecanismo já configurado no repositório;
6. invalidar/recriar cache apenas se o fluxo oficial exigir;
7. validar em produção as rotas canônicas, aliases, filtros, perfis,
   comparação, rankings, Power BI e Copa de 2026.

---

## 14. Pacote de handoff entre os modelos

Antes de chamar o Modelo V, o Modelo F deve entregar ao orquestrador:

### 14.1 Estado do contrato

- campos adicionados por endpoint;
- exemplos reais de resposta global e filtrada;
- exemplos de perfil completo e parcial;
- definição final de `CatalogScope`;
- semântica final de coverage;
- enum final de ordenação;
- confirmação de `entityType=club`;
- contagem auditada por `teamType`;
- lista de campos condicionais que podem ser nulos.

### 14.2 Estado técnico

- commit ou diff da fundação;
- arquivos alterados;
- migrations/dbt executados;
- testes executados e resultados;
- endpoints locais validados;
- qualquer limitação real de dados;
- zero alterações visuais intencionais.

### 14.3 Fixtures de referência

Disponibilizar respostas representativas, obtidas da API ou registradas em
testes, para:

- lista global de jogadores;
- lista filtrada de jogadores;
- jogador com carreira rica;
- jogador com cobertura parcial;
- lista global de clubes;
- lista filtrada de clubes;
- clube com identidade e conquistas;
- clube com dados mínimos.

Não adicionar Storybook ou mock server apenas para o handoff. Reusar fixtures
de testes ou respostas locais serializadas quando necessário.

### 14.4 Formato de `CONTRACT_GAP`

Se o Modelo V encontrar uma lacuna, deve responder ao orquestrador neste
formato:

```text
CONTRACT_GAP
Tela: <rota>
Componente: <componente>
Dado/comportamento ausente: <descrição objetiva>
Por que bloqueia: <impacto visual ou funcional>
Contrato esperado: <campo, estado ou endpoint>
Fallback seguro disponível: <sim/não e qual>
```

O Modelo V não deve criar cálculo histórico complexo no cliente como fallback.

---

## 15. Critérios finais de aceite

### Produto

- A primeira leitura comunica a dimensão e a proposta do acervo.
- A plataforma não sugere que o primeiro jogador ou clube é o maior da
  história.
- O escopo global e os filtros são compreensíveis.
- O limite do acervo publicado está explícito sem dominar a tela.
- Jogadores são descobertos por carreiras documentadas.
- Clubes são descobertos por trajetórias documentadas.
- A comparação continua acessível.

### Dados

- Relevância é determinística e recalculada por recorte.
- Clubes e seleções não são misturados semanticamente.
- Conquistas e identidade possuem proveniência e coverage.
- Ausência de dado nunca é convertida em afirmação histórica falsa.
- Totais do acervo e do resultado filtrado são coerentes.

### Frontend

- Nenhum hero automático de jogador ou clube.
- Nenhuma posição global por gols, pontos ou saldo.
- Perfil de jogador começa pela carreira e pelos clubes.
- Perfil de clube começa por identidade, origem disponível e conquistas.
- O erro de insights não implementados desapareceu.
- Loading, atualização, vazio, erro e parcial estão resolvidos.
- Desktop, tablet e mobile foram validados.
- Acessibilidade básica foi validada.

### Engenharia

- Rotas canônicas e resolvers de compatibilidade funcionam sem chamar seleção
  de clube.
- API antiga continua compatível onde necessário.
- Testes, typecheck e build estão verdes.
- Rankings, Power BI e Copa de 2026 não regrediram.
- O diff não inclui arquivos alheios à tarefa.
- Produção foi validada após o deploy.

---

## 16. Condições de parada e escalonamento

O agente deve parar e reportar ao orquestrador quando:

- uma decisão necessária contradizer uma decisão fechada deste documento;
- a implementação exigir alterar IDs canônicos ou reescrever fatos;
- não houver fonte verificável para um dado apresentado como histórico;
- o worktree tiver mudança concorrente no mesmo arquivo que não possa ser
  preservada;
- o Modelo V encontrar `CONTRACT_GAP` sem fallback seguro;
- uma regressão em rankings, Power BI ou Copa de 2026 exigir expansão material
  de escopo.

Não são condições de parada:

- um campo opcional de identidade ser nulo;
- um clube não possuir conquistas documentadas;
- um perfil possuir coverage parcial;
- a necessidade de iterar visualmente após a primeira composição.

---

## 17. Relatório final obrigatório

Ao concluir, o agente coordenador deve entregar:

- resumo do resultado de produto;
- commits de fundação e visual;
- mapa final de rotas;
- contratos adicionados ou alterados;
- regra final de relevância;
- cobertura observada de clubes, seleções, identidade e conquistas;
- arquivos alterados por camada;
- testes e comandos executados;
- evidências visuais antes/depois;
- resultado do CI;
- mecanismo e resultado do deploy;
- URLs públicas validadas;
- limitações reais de dados que permaneceram;
- confirmação de ausência de regressão em rankings, Power BI e Copa de 2026.
