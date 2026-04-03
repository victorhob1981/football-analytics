# COMPETITION_MODEL_EVOLUTION_PLAN

Data de referencia: `2026-03-25`  
Projeto: `football-analytics`

## 1. Objetivo

Consolidar um plano de evolucao do projeto para suportar competicoes em tres familias sem destruir o que ja esta verde para ligas:

- `league`: tabela global e acumulada;
- `knockout`: eliminatoria pura;
- `hybrid`: fases com formatos diferentes na mesma competicao-temporada.

O foco deste plano e semantico e estrutural. A meta nao e redesenhar o produto inteiro agora, e sim corrigir o eixo de modelagem para que o projeto suporte:

- copas eliminatorias puras, como `copa_do_brasil`;
- copas hibridas, como `libertadores`;
- mudancas de formato por temporada, como `champions_league`;
- competicoes FIFA futuras ou novas no portfolio, como `fifa_world_cup`, `fifa_club_world_cup` e `fifa_intercontinental_cup`.

---

## 2. Diagnostico objetivo do estado atual

### 2.1 Evidencia objetiva no repositorio

O repositorio ja saiu do modelo mono-liga em alguns pontos, mas ainda nao fechou a semantica de copa:

- `frontend/src/config/competitions.registry.ts`
  - ja distingue `domestic_league`, `domestic_cup` e `international_cup`;
  - isso resolve taxonomia de portfolio, mas nao resolve formato da temporada nem regras de progressao.
- `dbt/models/staging/stg_competition_stages.sql`
  - ja existe ingestao de `stage`;
  - hoje o stage entra como metadado estrutural, sem `stage_format`.
- `dbt/models/staging/stg_competition_rounds.sql`
  - ja existe ingestao de `round`;
  - round continua sem contexto de grupo ou confronto.
- `dbt/models/staging/stg_standings_snapshots.sql`
  - standings ja chegam com `league_id`, `season_id`, `stage_id`, `round_id`, `team_id`;
  - nao existe `group_id`.
- `dbt/models/marts/core/dim_stage.sql`
  - `dim_stage` ja existe;
  - hoje nao materializa formato da fase, nem regra de progressao, nem relacao explicita com fase seguinte.
- `dbt/models/marts/core/dim_round.sql`
  - `dim_round` ja existe com `round_key` sequencial por temporada;
  - isso ajuda ordenacao, mas nao resolve grupo nem mata-mata.
- `dbt/models/marts/core/fact_standings_snapshots.sql`
  - o grain atual ja inclui `stage_id` e `round_id`;
  - ainda nao inclui `group_id`, entao nao representa varias tabelas paralelas na mesma fase.
- `dbt/models/marts/analytics/standings_evolution.sql`
  - reconstrui classificacao acumulada sobre todos os jogos de `competition + season`;
  - isso faz sentido para liga e para fase de tabela unica;
  - nao faz sentido para copa eliminatoria nem para grupos paralelos sem scoping adicional.
- `api/src/routers/standings.py`
  - escolhe um `stage` e retorna uma tabela;
  - nao existe contrato para varios grupos simultaneos;
  - nao existe contrato para confrontos eliminatorios.
- `docs/INVENTARIO_DADOS_DO_PROJETO.md`
  - evidencia estrutural ja materializada no acervo:
    - `10` competicoes;
    - `50` escopos competicao-temporada;
    - `165` stages;
    - `1367` rounds.

### 2.2 Delimitacao correta do problema

Separacao do problema por camada:

- `codigo/modelagem`
  - o problema central esta aqui;
  - o projeto ja entende `competition`, `stage` e `round`, mas ainda nao modela `group`, `tie` e `season format variant`.
- `dados/provider`
  - existe risco real de providers nao entregarem `tie` ou `group` de forma completa;
  - o plano precisa prever inferencia auditavel quando o provider nao trouxer a entidade pronta.
- `validacao`
  - hoje faltam contratos e testes para:
    - standings por grupo;
    - resultado agregado de confronto;
    - determinacao de classificado por fase;
    - variacao de regra por temporada.
- `ambiente`
  - nao e o problema principal desta frente;
  - esta tarefa e de planejamento e contrato, nao de operacao local.

### 2.3 O que nao deve ser feito

- nao destruir o caminho atual de ligas;
- nao tentar enfiar semantica de confronto dentro de `round`;
- nao renomear tudo de `league_*` para `competition_*` de uma vez;
- nao abrir escopo para todos os casos raros antes de fechar `copa_do_brasil`, `libertadores` e `champions_league`.

---

## 3. Decisao de modelagem: separar taxonomia, formato e regra

O erro conceitual atual e usar um unico eixo para responder perguntas diferentes.

O projeto precisa separar pelo menos quatro eixos:

### 3.1 Taxonomia de portfolio

Este eixo ja existe no frontend e pode continuar:

- `domestic_league`
- `domestic_cup`
- `international_cup`

Esse eixo serve para navegacao, catalogo e narrativa de produto. Ele nao deve carregar regra de competicao.

### 3.2 Formato estrutural da competicao-temporada

Novo eixo obrigatorio em nivel de `competition + season`:

- `league`
- `knockout`
- `hybrid`

Regra importante: o formato estrutural precisa viver em `competition-season`, nao apenas em `competition`.

### 3.3 Formato estrutural da fase

Novo eixo obrigatorio em nivel de `stage`:

- `league_table`
- `group_table`
- `knockout`
- `qualification_knockout`
- `placement_match`

Regra importante: a UI, a API e os marts devem reagir ao `stage_format`, nao ao nome da competicao.

### 3.4 Escopo dos participantes

Novo eixo para nao vazar suposicao de clube para competicoes futuras:

- `club`
- `national_team`

Isso nao exige redesenhar o projeto inteiro agora, mas precisa entrar no contrato conceitual desde ja.

---

## 4. Modelo alvo refinado

### 4.1 Principios

- `competition` continua como container canonico;
- `match` continua como unidade atomica;
- comportamento esportivo passa a ser definido por `competition-season` e `stage`;
- entidades novas entram de forma aditiva e nullable onde possivel.

### 4.2 Entidades que ja existem e devem ser preservadas

- `competition`
- `season`
- `stage`
- `round`
- `match`
- `standings_snapshot`

### 4.3 Entidades novas ou ampliadas

#### A. `competition_season_config`

Nova camada de controle, pequena e explicita, para versionar regra sem hardcode no codigo.

Chave minima:

- `competition_key`
- `season_label`

Campos minimos:

- `format_family` = `league | knockout | hybrid`
- `season_format_code`
- `participant_scope` = `club | national_team`
- `group_ranking_rule_code`
- `tie_rule_code`
- `notes`

Essa tabela resolve o principal problema da Champions: a mesma competicao com formatos diferentes por temporada.

#### B. `stage` ampliado

`dim_stage` precisa incorporar:

- `stage_format`
- `stage_code`
- `sort_order`
- `advances_count`
- `next_stage_code` ou referencia equivalente
- `elimination_mode` quando aplicavel

Nao precisa virar uma super-entidade agora. O minimo necessario e permitir que o sistema saiba como cada fase deve ser tratada.

#### C. `group`

Nova entidade obrigatoria para fases com grupos.

Campos minimos:

- `group_id`
- `competition_key`
- `season_label`
- `stage_id`
- `group_name`
- `group_order`

#### D. `tie`

Nova entidade central para fases eliminatorias.

Campos minimos:

- `tie_id`
- `competition_key`
- `season_label`
- `stage_id`
- `home_side_team_id`
- `away_side_team_id`
- `tie_order`
- `winner_team_id`
- `resolution_type`
- `is_inferred`

`resolution_type` deve distinguir pelo menos:

- `aggregate`
- `extra_time`
- `penalties`
- `administrative`

#### E. `match` ampliado

`fact_matches` deve receber apenas campos aditivos e nullable:

- `stage_id` ou `stage_sk` coerente com a camada final;
- `group_id` quando aplicavel;
- `tie_id` quando aplicavel;
- `leg_number` quando aplicavel.

Isso preserva compatibilidade com ligas e evita criar uma segunda tabela de partidas.

#### F. `standings` por escopo correto

`fact_standings_snapshots` pode continuar existindo, mas precisa ser extendida com:

- `group_id` / `group_sk` quando a tabela for de grupo;
- grain efetivo:
  - liga: `competition + season + stage + round + team`
  - grupo: `competition + season + stage + group + round + team`

#### G. Fatos novos

Novos fatos recomendados:

- `fact_tie_results`
- `fact_group_standings`
- `fact_stage_progression`

Recomendacao de baixo risco:

- nao forcar `standings_evolution` atual a virar mart universal;
- manter `standings_evolution` focado em liga e tabela unica;
- criar mart novo para grupo quando a necessidade estiver fechada.

---

## 5. Champions League: tratamento especifico para mudanca de formato

### 5.1 Problema

`champions_league` nao pode ser tratada por uma unica regra fixa em nivel de competicao.

O acervo ja contem temporadas anteriores e futuras sob o mesmo `competition_key`, mas com estruturas diferentes:

- temporadas anteriores: fase de grupos + mata-mata;
- temporadas mais recentes: primeira fase em tabela unica + mata-mata.

Se o projeto guardar o formato apenas em `competition`, ele erra necessariamente em pelo menos uma parte do historico.

### 5.2 Decisao recomendada

Preservar um unico `competition_key=champions_league`, mas versionar o formato por temporada.

Exemplo de codigos de formato:

- `ucl_group_knockout_v1`
- `ucl_league_table_knockout_v1`

Regras:

- o `competition_key` continua estavel para catalogo, URLs e identidade do produto;
- o comportamento do pipeline e do produto vem de `season_format_code`;
- o primeiro stage das temporadas antigas usa `stage_format=group_table`;
- o primeiro stage das temporadas novas usa `stage_format=league_table`;
- os stages eliminatorios continuam reutilizando o mesmo modelo de `tie`.

Decisao importante:

- para Champions, a exibicao ao usuario continua como um unico produto;
- no banco, a diferenciacao acontece por `season_format_code` e pelas regras da temporada;
- diferente do caso `fifa_club_world_cup` vs `fifa_intercontinental_cup`, aqui nao ha motivo para separar a competicao em dois catalogos publicos distintos;
- aqui o problema e de `regulation versioning`, nao de `product lineage split`.

### 5.3 Consequencia pratica

Nao pode existir branch do tipo:

- "se competicao = champions, entao ...".

O branch correto e:

- "se `stage_format = group_table`, retorna grupos";
- "se `stage_format = league_table`, retorna tabela unica";
- "se `stage_format = knockout`, retorna confrontos".

### 5.4 Validacao especifica que deve existir

Para Champions:

- `2023_24` deve materializar varios grupos e nenhum stage inicial de tabela unica;
- `2024_25` deve materializar um stage inicial de tabela unica e nenhum `group_id` nesse stage;
- os stages eliminatorios das duas temporadas devem convergir para o mesmo contrato de confronto.

Esse ponto deve entrar explicitamente nos testes e na documentacao de regra.

### 5.5 Regra de exibicao para o usuario

A exibicao recomendada no produto e:

- manter uma unica identidade publica `champions_league`;
- mostrar a temporada normalmente no catalogo;
- adicionar metadado visual de formato quando isso ajudar o entendimento historico.

Exemplos de badge:

- `2023/24 · fase de grupos + mata-mata`
- `2024/25 · league phase + mata-mata`

Isso permite:

- continuidade editorial para o usuario;
- correção semantica no banco;
- queries e componentes dirigidos por `stage_format` e `season_format_code`.

### 5.6 Diferenca em relacao ao caso FIFA

Resumo da regra:

- `champions_league`
  - um produto publico;
  - uma competicao canonica;
  - multiplas versoes de regulamento por temporada.
- `fifa_club_world_cup` vs `fifa_intercontinental_cup`
  - produtos publicos diferentes;
  - identidades canonicas diferentes;
  - sem fusao historica automatica entre eles.

---

## 6. Copas futuras: Mundial de Clubes e Copa do Mundo de selecoes

O plano nao deve abrir implementacao completa agora, mas precisa evitar retrabalho obvio.

### 6.1 Mundial de Clubes

O modelo acima ja cobre o caso desde que:

- `participant_scope = club`;
- a estrutura seja definida por `competition-season` e `stage`;
- fases de grupos, tabela unica ou mata-mata sejam tratadas via `stage_format`.

### 6.2 Copa do Mundo de selecoes

O ponto que precisa entrar desde ja no contrato conceitual e:

- `participant_scope = national_team`.

O erro a evitar e deixar o ecossistema inteiro implicito como "clube".

Nao e necessario resolver todo o dominio de selecoes neste plano, mas e necessario garantir que a modelagem de competicao nao fique acoplada a clube.

### 6.3 Regra geral de extensibilidade

O sistema deve perguntar:

- qual e o formato desta temporada?
- qual e o formato desta fase?
- quem sao os participantes deste recorte?

Ele nao deve perguntar:

- esta competicao e igual a Libertadores?
- esta competicao parece liga ou copa pelo nome?

### 6.4 Decisao fixa de catalogo para produtos FIFA

Recomendacao de catalogo publico:

- `fifa_world_cup`
- `fifa_club_world_cup`
- `fifa_intercontinental_cup`

Essas tres competicoes devem existir como identidades publicas distintas no catalogo.

Decisoes semanticas obrigatorias:

- `fifa_world_cup` e competicao de selecoes, nao de clubes;
- `fifa_club_world_cup` e o produto quadrianual grande de clubes iniciado em `2025`;
- `fifa_intercontinental_cup` e o produto anual de clubes iniciado em `2024`;
- o antigo torneio anual de clubes da FIFA nao deve entrar automaticamente em `fifa_club_world_cup`;
- a antiga `Intercontinental Cup` pre-FIFA reconhecida historicamente como titulo mundial tambem nao deve entrar automaticamente em `fifa_intercontinental_cup`.

Recomendacao de modelagem interna:

- usar `public_competition_key` para a identidade exibida ao cliente;
- usar `season_format_code` para variacao de regulamento por temporada;
- usar um campo opcional como `semantic_lineage_key` apenas se um dia for necessario separar lineagens historicas internamente sem quebrar a exibicao.

Decisao especifica para `fifa_intercontinental_cup`:

- manter uma identidade publica unica por enquanto;
- nao abrir duas competicoes visiveis ao cliente neste momento;
- se houver mudanca material de formato no futuro, separar internamente por `season_format_code` ou `semantic_lineage_key`, preservando uma experiencia de catalogo unica quando isso continuar semanticamente defensavel.

### 6.5 Lineagens historicas que nao podem ser misturadas

Para evitar erro de catalogo, ficam fora deste contrato semantico:

- `2000` e `2005-2023` do antigo `FIFA Club World Cup / FIFA Club World Championship`;
- `1960-2004` da antiga `Intercontinental Cup` UEFA x CONMEBOL.

Se essas lineagens forem ingeridas no futuro, a recomendacao e:

- criar identidade semantica separada;
- nao anexar automaticamente ao historico de `fifa_club_world_cup` ou `fifa_intercontinental_cup`;
- permitir, no maximo, agrupamento editorial na camada de produto, nunca fusao cega no contrato canonico.

### 6.6 Politica de janela de ingestao para competicoes FIFA

Recomendacao:

- `fifa_world_cup`
  - rolling window por ultimas `5` edicoes completas ou disponiveis no provider;
  - na pratica, este e um recorte de longo prazo porque a competicao e quadrianual.
- `fifa_club_world_cup`
  - iniciar em `2025`;
  - nao tentar preencher uma janela de cinco edicoes usando o torneio anual anterior, porque isso quebra o contrato semantico.
- `fifa_intercontinental_cup`
  - usar rolling window anual de `5` edicoes a partir de `2024`;
  - no estado atual conhecido em `2026-03-25`, isso significa que as edicoes confirmadas sao `2024` e `2025`; as demais entram conforme passarem a existir.

### 6.7 Matriz de regras para competicoes FIFA

#### Matriz de catalogo

| public_competition_key | participant_scope | cadence | format_scope | ingest_policy | observacao |
|---|---|---|---|---|---|
| `fifa_world_cup` | `national_team` | `quadrennial` | versionado por edicao | ultimas `5` edicoes disponiveis | onboarding mais seguro; semantica de stages mais estavel e cobertura de World Cup aparece no catalogo publico da SportMonks |
| `fifa_club_world_cup` | `club` | `quadrennial` | versionado por edicao | iniciar em `2025` e seguir edicoes futuras do mesmo produto | nao misturar com o torneio anual antigo da FIFA |
| `fifa_intercontinental_cup` | `club` | `annual` | versionado por temporada | rolling window anual de `5` edicoes a partir de `2024` | manter uma identidade publica unica enquanto o formato anual atualizado permanecer comparavel |

#### Matriz de formato e regra

| public_competition_key | season_scope | season_format_code | format_family | first_stage_format | has_group_stage | has_knockout_stage | group_ranking_rule_code | tie_rule_code | regulation_status | observacao |
|---|---|---|---|---|---|---|---|---|---|---|
| `fifa_world_cup` | `2010`, `2014`, `2018`, `2022` | `fwc_32_group_knockout_v1` | `hybrid` | `group_table` | `yes` | `yes` | `fifa_group_standard_32_v1` | `single_leg_extra_time_penalties_v1` | `confirmed_family` | `8` grupos de `4`, top `2` avancam; este e o baseline historico mais estavel para onboarding |
| `fifa_world_cup` | `2026+` | `fwc_48_group_knockout_v2` | `hybrid` | `group_table` | `yes` | `yes` | `fifa_group_standard_48_best_thirds_v2_pending_regs` | `single_leg_extra_time_penalties_v1` | `format_confirmed_detailed_regs_pending` | `12` grupos de `4`, top `2` + `8` melhores terceiros; confirmar regulamento detalhado antes de implementar ranking final e selecao de terceiros |
| `fifa_club_world_cup` | `2025` | `fcwc_32_group_knockout_v1` | `hybrid` | `group_table` | `yes` | `yes` | `fifa_group_standard_32_v1` | `single_leg_extra_time_penalties_v1` | `confirmed_family` | novo produto FIFA; `8` grupos de `4`, oitavas em diante em jogo unico, sem terceiro lugar |
| `fifa_club_world_cup` | `2029+` | `fcwc_32_group_knockout_v1_pending_reconfirmation` | `hybrid` | `group_table` | `yes` | `yes` | `fifa_group_standard_32_v1` | `single_leg_extra_time_penalties_v1` | `planned_reconfirm_before_activation` | manter a familia de `2025` so apos reconfirmacao oficial de cada nova edicao |
| `fifa_intercontinental_cup` | `2024-2025` | `fic_annual_champions_knockout_v1` | `knockout` | `qualification_knockout` | `no` | `yes` | `not_applicable` | `single_leg_extra_time_penalties_v1_pending_tournament_regs` | `confirmed_family` | torneio anual de `5` partidas, com entrada escalonada de campeoes continentais; UEFA entra diretamente na final |
| `fifa_intercontinental_cup` | `2026+` | `fic_annual_champions_knockout_v1_pending_reconfirmation` | `knockout` | `qualification_knockout` | `no` | `yes` | `not_applicable` | `single_leg_extra_time_penalties_v1_pending_tournament_regs` | `planned_reconfirm_before_activation` | manter uma identidade publica unica enquanto o formato anual continuar equivalente ao introduzido em `2024` |

### 6.8 Prioridade recomendada de onboarding FIFA

Ordem recomendada:

1. `fifa_world_cup`
2. `fifa_club_world_cup`
3. `fifa_intercontinental_cup`

Justificativa tecnica:

- `fifa_world_cup` tem semantica mais estavel, menos ambiguidade conceitual e melhor perfil para validar `participant_scope=national_team`;
- `fifa_club_world_cup` deve entrar como produto proprio e limpo a partir de `2025`, sem herdar legado anual;
- `fifa_intercontinental_cup` exige mais cuidado de catalogo porque o risco de mistura historica e maior do que a complexidade estrutural do torneio em si.

### 6.9 Fontes externas que sustentam este recorte FIFA

Fontes oficiais FIFA e provider usadas como referencia para este bloco:

- FIFA World Cup 2026 format:
  - `https://www.fifa.com/en/articles/article-fifa-world-cup-2026-mexico-canada-usa-new-format-tournament-football-soccer`
- FIFA Club World Cup 2025 new 32-team quadrennial format:
  - `https://www.fifa.com/en/articles/fifa-club-world-cup-2025-dates-format-and-qualifiers`
  - `https://inside.fifa.com/media-releases/fifa-council-confirms-key-details-for-fifa-club-world-cup-2025-tm`
- FIFA Intercontinental Cup annual club tournament from 2024:
  - `https://www.fifa.com/en/tournaments/mens/intercontinentalcup/2024/articles/bureau-council-update`
- SportMonks coverage/support for World Cup domain:
  - `https://www.sportmonks.com/football-api/`
  - `https://www.sportmonks.com/football-api/world-cup-api/`
  - `https://www.sportmonks.com/football-api/coverage/`

Ponto de incerteza assumido explicitamente:

- para `fifa_world_cup 2026+` e futuras edicoes de `fifa_club_world_cup` / `fifa_intercontinental_cup`, o formato macro esta fechado nas fontes acima, mas o regulamento detalhado de desempate, progressao e resolver operacional de cada edicao deve ser revalidado antes de codificar regra definitiva em pipeline.

---

## 7. Impactos por camada

### 7.1 Ingestao

Necessidades:

- preservar o pipeline atual de matches;
- extrair ou inferir `group`, `tie` e `leg_number`;
- versionar regra por `competition + season`.

Quando o provider nao entregar `tie` pronto:

- inferir `tie` em camada intermediaria;
- registrar `is_inferred=true`;
- manter chave auditavel baseada em `competition-season-stage-pair`.

### 7.2 Staging e normalizacao

Necessidades:

- scoping correto de standings por grupo;
- relacao explicita `match -> tie`;
- derivacao de agregado por confronto;
- derivacao de vencedor conforme regra configurada.

### 7.3 Warehouse / marts

Necessidades:

- extensao aditiva em `dim_stage`, `fact_matches` e `fact_standings_snapshots`;
- novos marts para confronto, grupo e progressao;
- manter marts de liga estaveis enquanto a camada de copa amadurece.

### 7.4 API / BFF

Endpoints novos ou expandidos:

- `competition structure`
- `group standings`
- `ties by stage`
- `team progression`

Campos de filtro que precisam entrar:

- `stageId`
- `groupId`
- `tieId` quando o fluxo for eliminatorio

`roundId` continua valido, mas deixa de ser o eixo principal para copas.

### 7.5 Frontend / produto

Diretriz:

- ligas continuam com fluxo atual;
- copas passam a abrir pelo hub estrutural da competicao;
- fase de grupos mostra grupos e tabelas;
- fase eliminatoria mostra confrontos e depois bracket.

---

## 8. Ordem de execucao recomendada

### Bloco 0 - Contrato semantico e matriz de regras

Entregar primeiro:

- lista de competicoes-alvo desta frente;
- `season_format_code` por competicao-temporada;
- `tie_rule_code` e `group_ranking_rule_code`;
- matriz especifica da Champions por temporada.
- matriz especifica das competicoes FIFA por temporada/edicao.

Sem isso, a implementacao vira hardcode disperso.

### Bloco 1 - Extensao aditiva do modelo base

Entregar:

- `competition_season_config`;
- ampliacao de `dim_stage`;
- campos opcionais em `fact_matches`;
- `group_id` em standings/fatos aplicaveis;
- estrutura raw/staging minima para `group` e `tie`.

Objetivo:

- criar lugar correto para armazenar sem quebrar liga.

### Bloco 2 - Piloto knockout puro

Caso recomendado:

- `copa_do_brasil`.

Entregar:

- `tie` funcional;
- `fact_tie_results`;
- determinacao de vencedor;
- progressao basica entre fases.

Objetivo:

- provar o modelo eliminatorio puro com menor complexidade.

### Bloco 3 - Piloto de grupos

Caso recomendado:

- `libertadores` fase de grupos.

Entregar:

- `group`;
- `fact_group_standings`;
- standings com scoping por grupo;
- filtro `stageId + groupId`.

Objetivo:

- provar multiplas tabelas paralelas na mesma competicao.

### Bloco 4 - Hibrido completo

Caso recomendado:

- `libertadores` completa.

Entregar:

- transicao grupo -> mata-mata;
- `fact_stage_progression`;
- hubs e APIs estruturais minimos.

Objetivo:

- fechar o primeiro torneio hibrido ponta a ponta.

### Bloco 5 - Champions por versao de temporada

Entregar:

- matriz de formato antiga vs nova;
- materializacao correta por `season_format_code`;
- garantias de que a UI reage por `stage_format`, nao por nome da competicao.

Objetivo:

- impedir regressao historica e preparar extensibilidade real.

### Bloco 6 - Produto e analytics avancados

Entregar depois que a base estiver correta:

- bracket visual;
- trajetoria por time;
- filtros analiticos por fase;
- refinamentos de jogador/time por stage.

### Bloco 7 - Casos complexos

Somente depois:

- repescagem entre competicoes;
- regras administrativas retroativas;
- criterios de desempate raros e multi-nivel;
- formatos exoticos futuros.

---

## 9. Validacao objetiva por etapa

### 9.1 Regras minimas de nao-regressao

Toda etapa precisa provar:

- ligas continuam materializando sem precisar de `group_id` ou `tie_id`;
- rerun idempotente continua verde;
- queries antigas de liga continuam retornando o mesmo resultado quando filtradas no mesmo escopo.

### 9.2 Validacoes novas obrigatorias

Para `tie`:

- um confronto completo tem exatamente um vencedor oficial;
- gols agregados batem com a soma dos legs;
- `resolution_type` e coerente com o estado final;
- confrontos inferidos ficam marcados como inferidos.

Para `group standings`:

- nao existe mistura de times de grupos diferentes no mesmo snapshot;
- grain unico por `competition-season-stage-group-round-team`;
- posicoes de grupo batem com a regra configurada.

Para `progression`:

- time eliminado nao aparece na fase seguinte;
- time classificado aparece exatamente uma vez na fase esperada.

Para Champions:

- temporadas antigas e novas materializam estruturas diferentes sob o mesmo `competition_key`;
- o primeiro stage da nova fase de tabela unica nao gera grupos ficticios.

---

## 10. Decisoes de compatibilidade

Para reduzir risco:

- manter `fact_matches` como fato canonico de partida;
- manter nomenclaturas legadas como `league_summary` enquanto o comportamento semantico e estabilizado;
- evitar rename amplo antes da camada nova estar validada;
- preferir expansao aditiva a substituicao.

Decisao importante:

- `league_summary` e nomenclatura legada;
- o escopo semantico real do projeto deve continuar migrando para `competition-season`;
- a troca de nome, se acontecer, deve ser etapa posterior e separada da habilitacao de copas.

---

## 11. Proximo passo seguro

O proximo passo correto nao e abrir frontend nem bracket.

O proximo passo seguro e produzir a matriz de regras e formatos por `competition-season`, pelo menos para:

- `copa_do_brasil`
- `libertadores`
- `champions_league`
- `fifa_world_cup`
- `fifa_club_world_cup`
- `fifa_intercontinental_cup`

Campos minimos dessa matriz:

- `competition_key`
- `season_label`
- `format_family`
- `season_format_code`
- `participant_scope`
- `first_stage_format`
- `has_group_stage`
- `has_knockout_stage`
- `tie_rule_code`
- `group_ranking_rule_code`

Sem essa matriz, o restante da implementacao tende a espalhar regra por if/else e reabrir retrabalho.
