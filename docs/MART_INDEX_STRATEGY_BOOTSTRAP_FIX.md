# Mart Index Strategy Bootstrap Fix

Data de referencia: 2026-04-01  
Escopo desta rodada: corrigir o blocker de bootstrap causado por `20260331214500_wave1_hot_read_indexes.sql` sem reabrir `raw.match_events`.

## 1. Problema encontrado

A migration [`20260331214500_wave1_hot_read_indexes.sql`](C:/Users/Vitinho/Desktop/Projetos/football-analytics/db/migrations/20260331214500_wave1_hot_read_indexes.sql) criava indices diretamente em:

- `mart.fact_fixture_player_stats`
- `mart.fact_fixture_lineups`
- `mart.player_match_summary`

Essas relacoes nao fazem parte do schema base criado por migrations.  
Elas nascem depois, via dbt/materializacao.

Consequencia no replay limpo:

- `dbmate up` chegava nessa migration antes de existir qualquer uma dessas tabelas;
- o bootstrap quebrava com `relation does not exist`.

## 2. Por que a migration original era inadequada

Ela era inadequada por tres motivos estruturais:

1. colocava concern de superficie derivada do dbt dentro de foundation SQL tradicional;
2. nao sobrevivia corretamente ao ciclo de `full_refresh`, porque tabelas recriadas pelo dbt perdem indices criados fora do proprio ciclo de materializacao;
3. quebrava a reprodutibilidade do bootstrap, ao assumir que relacoes `mart` ja existiam antes do `dbt run`.

`IF EXISTS` nao seria correcao correta:

- faria o bootstrap passar, mas mascararia o problema arquitetural;
- continuaria deixando os indices fora do ponto correto de manutencao;
- continuaria permitindo `full_refresh` recriar as tabelas sem garantia de reconstituicao declarativa dos indices.

## 3. Estratégia escolhida

Estrategia adotada:

1. neutralizar a migration historica de indices do `mart` para que ela deixe de participar do bootstrap de schema base;
2. mover a definicao desses indices para os modelos dbt correspondentes, usando configuracao nativa `indexes`.

Justificativa tecnica:

- no dbt Postgres do projeto (`dbt-core 1.8.8`, `dbt-postgres 1.8.2`), o adapter chama `create_indexes(target_relation)` nas materializacoes `table` e `incremental`;
- isso significa que os indices sao recriados no momento correto:
  - criacao inicial da tabela
  - `full_refresh`
  - recriacao da relacao alvo
- o schema bootstrap volta a depender apenas de foundation real;
- o lifecycle dos indices passa a acompanhar o lifecycle das tabelas derivadas.

## 4. Arquivos alterados

- [`20260331214500_wave1_hot_read_indexes.sql`](C:/Users/Vitinho/Desktop/Projetos/football-analytics/db/migrations/20260331214500_wave1_hot_read_indexes.sql)
- [`fact_fixture_player_stats.sql`](C:/Users/Vitinho/Desktop/Projetos/football-analytics/dbt/models/marts/core/fact_fixture_player_stats.sql)
- [`fact_fixture_lineups.sql`](C:/Users/Vitinho/Desktop/Projetos/football-analytics/dbt/models/marts/core/fact_fixture_lineups.sql)
- [`player_match_summary.sql`](C:/Users/Vitinho/Desktop/Projetos/football-analytics/dbt/models/marts/analytics/player_match_summary.sql)

## 5. Impacto em bootstrap limpo

Depois da correcao:

- `20260331214500_wave1_hot_read_indexes.sql` deixou de bloquear o replay de schema;
- o bootstrap global passou com sucesso por esse ponto;
- o novo blocker real apareceu depois, em outra frente:
  - versoes duplicadas de migration `20260331214500_*` no controle do `dbmate`.

Leitura correta:

- o blocker do `mart` foi resolvido;
- o bootstrap agora avanca alem dele;
- o proximo blocker ja nao e `relation does not exist` nas tabelas derivadas.

## 6. Impacto em dbt/materialização

Os indices quentes ficam agora declarados nos proprios modelos:

- `fact_fixture_player_stats`:
  - `(match_id, team_id)`
- `fact_fixture_lineups`:
  - `(match_id, team_id)`
- `player_match_summary`:
  - `(player_id, match_date desc, match_id desc)`

Validacao objetiva desta rodada:

- `dbt parse` executou com sucesso;
- o `manifest.json` do dbt passou a expor `config.indexes` nesses tres modelos.

## 7. Riscos remanescentes

1. Ambientes antigos continuarao com os indices herdados da migration historica ja aplicada ate o proximo ciclo de recriacao/materializacao; isso nao quebra compatibilidade.
2. Os nomes fisicos dos indices podem passar a ser gerados pelo dbt em vez dos nomes manuais da migration historica; funcionalmente isso e aceitavel para o objetivo atual.
3. O bootstrap global do repo ainda nao esta 100% concluido por um blocker novo e distinto: duplicidade de versao `20260331214500` em duas migrations diferentes.

## 8. Próximo passo seguro

Tratar o novo blocker real do bootstrap global:

- duas migrations compartilham a mesma versao `20260331214500`:
  - `20260331214500_coaches_identity.sql`
  - `20260331214500_wave1_hot_read_indexes.sql`

Isso precisa ser reconciliado de forma segura no historico de migrations do `dbmate` antes de declarar o bootstrap global completamente verde.
