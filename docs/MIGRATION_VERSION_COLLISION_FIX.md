# Migration Version Collision Fix

Data de referencia: 2026-04-01  
Escopo desta rodada: resolver a colisao de versao `20260331214500` no historico de migrations do `dbmate`, sem reabrir `raw.match_events`.

## 1. Problema encontrado

O repositório tinha duas migrations com o mesmo prefixo de versao:

- `20260331214500_coaches_identity.sql`
- `20260331214500_wave1_hot_read_indexes.sql`

Isso quebrava o bootstrap limpo com:

- `duplicate key value violates unique constraint "schema_migrations_pkey"`

O erro era estrutural:

- `dbmate` registra apenas a versao em `schema_migrations`;
- duas migrations diferentes nao podem compartilhar a mesma versao;
- no replay limpo, a segunda tentava inserir a mesma chave primaria na tabela de controle.

## 2. Por que a colisao era perigosa

Ela era perigosa por tres motivos:

1. deixava o historico do repositório ambiguo;
2. impedia bootstrap limpo reproduzivel;
3. criava risco de diagnostico errado em ambientes existentes, porque uma unica linha em `schema_migrations` nao consegue representar duas migrations distintas.

## 3. Diagnóstico sobre impacto em ambientes existentes

Evidencia do ambiente atual:

- `schema_migrations` contem `20260331214500` uma unica vez;
- `raw.coaches` existe;
- `raw.idx_coaches_name` existe;
- a nova versao `20260401103000` ainda nao existe em `schema_migrations` do ambiente atual.

Leitura correta:

- a versao `20260331214500` provavelmente foi historicamente ocupada por `coaches_identity`;
- `wave1_hot_read_indexes` ficou sem identidade propria confiavel no historico;
- isso e coerente com a ordem lexicografica observada no replay limpo, em que `coaches_identity` entra antes de `wave1_hot_read_indexes`.

Risco de apenas renomear `coaches_identity`:

- quebraria o mapeamento historico mais provavel entre versao aplicada e efeito real observado;
- deixaria a linha `20260331214500` em ambientes existentes sem corresponder ao artefato mais substancial daquele ponto;
- obrigaria ambientes existentes a aplicar de novo uma migration de foundation que ja deixou efeito real no banco.

## 4. Estratégia escolhida

Estratégia adotada:

- preservar `20260331214500_coaches_identity.sql` intacta na sua versao historica;
- mover `wave1_hot_read_indexes` para uma nova versao unica:
  - `20260401103000_wave1_hot_read_indexes.sql`

Por que essa e a abordagem mais segura:

- preserva a migration que provavelmente ocupou historicamente a versao duplicada;
- da identidade propria ao artefato que estava colidindo;
- mantém integridade de `schema_migrations` em ambientes existentes;
- em ambientes ja provisionados, a nova migration entra apenas como `no-op`, portanto sem risco operacional.

## 5. Arquivos alterados

- [`20260401103000_wave1_hot_read_indexes.sql`](C:/Users/Vitinho/Desktop/Projetos/football-analytics/db/migrations/20260401103000_wave1_hot_read_indexes.sql)
- [`DB_TUNING_EXECUTION_LOG.md`](C:/Users/Vitinho/Desktop/Projetos/football-analytics/docs/DB_TUNING_EXECUTION_LOG.md)

Observacao:

- `20260331214500_coaches_identity.sql` foi preservada.
- a antiga colisao deixou de existir no repositório.

## 6. Validação de bootstrap

Validacao executavel realizada:

- banco temporario limpo: `football_dw_bootstrap_version_fix`
- comando: `dbmate --url postgres://.../football_dw_bootstrap_version_fix?sslmode=disable --migrations-dir /db/migrations up`

Resultado:

- o replay passou por:
  - `20260331214500_coaches_identity.sql`
  - `20260331230000_sudamericana_intercontinental_seed.sql`
  - `20260401101500_match_events_canonical_assertion.sql`
  - `20260401103000_wave1_hot_read_indexes.sql`
- o bootstrap global concluiu com sucesso:
  - `Writing: ./db/schema.sql`

Verificacao adicional:

- nao ha mais duplicidade de prefixo de versao no diretório `db/migrations`.

## 7. Riscos remanescentes

1. Ambientes existentes passarao a ver uma nova versao pendente (`20260401103000`), mas ela e `no-op`, entao o risco operacional e baixo.
2. A interpretacao historica correta depende de aceitar que `coaches_identity` foi a ocupante real da versao antiga, o que e a leitura mais forte disponivel pelas evidencias observadas.

## 8. Próximo passo seguro

Com a colisao de versao resolvida e o bootstrap limpo concluido, o proximo passo seguro e:

- consolidar a trilha de bootstrap do repositório como verde;
- so depois retomar frentes estruturais posteriores do plano, sem reabrir `raw.match_events` desnecessariamente.
