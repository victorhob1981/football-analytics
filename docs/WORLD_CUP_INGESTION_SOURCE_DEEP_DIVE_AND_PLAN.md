# Copa do Mundo — aprofundamento de fontes e planejamento de ingestão

# Parte 1 — Aprofundamento técnico das fontes

---

## 1.1 StatsBomb Open Data

**Cobertura por edição**

A análise anterior estava estreita demais. O `competitions.json` público do StatsBomb Open Data hoje expõe, para a Copa masculina, as edições `1958`, `1962`, `1970`, `1974`, `1986`, `1990`, `2018` e `2022`. Mas isso não significa cobertura homogênea. O desenho correto é por tiers de profundidade e completude:

- **Tier A — cobertura completa + profundidade máxima:** `2022` com `64` jogos, `64` arquivos de eventos, `64` arquivos de lineups e `64` arquivos de StatsBomb 360. Esta é a única edição masculina aberta com cobertura integral e camada 360.
- **Tier B — cobertura completa + profundidade alta sem 360:** `2018` com `64` jogos, `64` arquivos de eventos e `64` arquivos de lineups. É arquiteturalmente compatível com 2022 para ingestão de eventos e lineups, mas sem 360.
- **Tier C — cobertura histórica amostral, não completa por edição:** `1958` (`2` jogos), `1962` (`1`), `1970` (`6`), `1974` (`6`), `1986` (`3`) e `1990` (`1`). Para esses jogos existem eventos e lineups, mas não há completude de torneio. Isso serve para enriquecimento pontual de partidas históricas, não para ingestão edition-wide.
- **Fora do escopo deste plano:** o open data também expõe Women's World Cup `2019` e `2023`. Isso é relevante para futura expansão feminina, mas não altera o desenho atual da Copa masculina.

O erro do texto anterior não era subestimar a qualidade do StatsBomb. Era tratar "presença no open data" como sinônimo de "cobertura de edição". Para a Copa masculina, isso só é verdadeiro em `2018` e `2022`.

**Tipo de dado disponível**

O repositório entrega três camadas bem distintas: (a) metadata de competição/jogo em JSON estruturado; (b) event data por jogo, com uma linha por evento e atributos aninhados por tipo de ação; (c) lineup data por jogo, com titulares e banco. Em 2022 existe ainda a camada StatsBomb 360, com freeze frames espaciais associados a eventos.

**Granularidade**

É a mais alta disponível gratuitamente para futebol. Cada ação no campo é um evento com timestamp, coordenadas no sistema StatsBomb (`0–120 x 0–80`), atributos táticos aninhados e relacionamentos entre eventos (`related_events`). Isso vale para 2018 e 2022. Nas edições históricas amostrais, a granularidade por partida continua alta, mas a completude por edição não existe.

**Formato dos arquivos**

JSON puro, estruturado hierarquicamente: `competitions.json` como índice global; arquivos de matches organizados por `competition_id/season_id.json`; eventos e lineups por `match_id.json` em suas respectivas pastas. O JSON é profundamente aninhado e não deve ser flattenado no bronze.

**Dificuldade prática de ingestão**

Alta. O JSON aninhado exige normalização não trivial para produzir tabelas relacionais úteis. O risco principal não é "ler o arquivo". É escolher cedo demais um schema tabular e perder semântica de eventos. A abordagem correta é:

- bronze preservando o JSON bruto;
- silver normalizando eventos em contrato explícito;
- derivação de agregados somente depois.

Existe `statsbombpy` (Python) e `StatsBombR` que aceleram a exploração, mas isso não muda a necessidade de um contrato próprio no pipeline.

**Implicações no desenho de ingestão**

O desenho correto muda por tier:

- `2022` e `2018` podem compartilhar o mesmo pipeline estrutural desde o início;
- `2022` não pode ser o único caso mental do pipeline, porque 360 existe só ali;
- as edições históricas amostrais não devem entrar no catálogo como "edições com cobertura StatsBomb"; devem entrar como `sampled_match_enrichment`;
- qualquer inventário de cobertura precisa diferenciar `FULL_TOURNAMENT`, `PARTIAL_MATCH_SAMPLE` e `NO_COVERAGE`.

Se isso não for explicitado, o pipeline tende a assumir que "há StatsBomb para 1970" significa que "1970 está coberto". Não está.

**Versionamento e reprodutibilidade**

Excelente. O repositório é um git público. O fluxo de IDs é determinístico: `competitions.json` -> `season_id` -> `match_id` -> arquivo de eventos/lineups/360. Um snapshot por commit resolve versionamento e permite replay reprodutível.

**Consistência e confiabilidade**

Alta para o que está publicado. O StatsBomb é forte como fonte de evento e lineup. O ponto fraco não é confiabilidade dos jogos presentes. É o risco de usar cobertura parcial como se fosse cobertura de edição.

**Lacunas previsíveis**

As estatísticas agregadas que seu projeto usa (`match_statistics` no sentido de posse, passes, finalizações por time) não existem como tabela pronta no open data. Elas precisam ser derivadas dos eventos. O gap de `player_season_statistics` também continua real: o StatsBomb entrega performance por jogo, não uma visão sazonal comparável à de ligas. Transferências, sidelined e outras entidades de contexto seguem fora de escopo.

**Aderência aos domínios canônicos**

| Domínio seu | StatsBomb | Obs |
|---|---|---|
| `fixtures` | ✅ direto | completo para 2018/2022; parcial nas edições amostrais |
| `match_events` | ✅ nativo | é o produto central deles |
| `fixture_lineups` | ✅ nativo | completo para 2018/2022; disponível nos jogos amostrais |
| `match_statistics` | ⚠️ derivável | só onde houver eventos |
| `fixture_player_statistics` | ⚠️ derivável | só onde houver eventos |
| `competition_stages` | ⚠️ parcial | existe metadata de fase por jogo, não uma entidade estrutural forte |
| `standings_snapshots` | ❌ ausente | não existe |
| `player_season_statistics` | ❌ não aplicável direto | exige modelagem própria |
| `team_coaches` | ⚠️ fraco | não é um domínio canônico forte no open data |
| `player_transfers`, `team_sidelined` | ❌ fora de escopo | |

**O que resolve muito bem:** `match_events` e `fixture_lineups` de `2022` e `2018`.

**O que resolve parcialmente:** `fixtures` e enriquecimento pontual de jogos históricos específicos.

**Não deve ser usado para:** backbone estrutural de edições históricas, standings, histórico de técnicos e qualquer completude artificial fora de `2018` e `2022`.

**Papel arquitetural ideal:** fonte primária para profundidade de eventos e lineups em `2022` e `2018`; enriquecimento pontual por jogo em alguns recortes históricos; nunca backbone de completude por edição fora desses dois torneios.

**Riscos técnicos como fonte primária:** (a) normalização errada do JSON aninhado; (b) suposição falsa de cobertura edition-wide para os anos históricos amostrais; (c) acoplamento desnecessário do pipeline a 360 como se fosse universal.

---

## 1.2 Fjelstul World Cup Database

**Cobertura por edição**

Cobre todos os `22` torneios masculinos de `1930` a `2022` e todos os `8` femininos de `1991` a `2019`. O banco contém `27` datasets com mais de `1,58` milhão de data points. Continua sendo a melhor base gratuita para backbone estrutural e histórico de Copa.

**Tipo de dado disponível**

Cobre: partidas, times, jogadores, técnicos, árbitros, escalações, gols, pênaltis em shootout, cartões, substituições, classificações e prêmios. A estrutura é relacional por design.

**Granularidade**

Média-alta. É muito mais estruturado do que um CSV de resultados, mas continua abaixo do StatsBomb em profundidade de evento. O valor dele está em competição, identidade e contexto histórico, não em telemetria do jogo.

**Formato dos arquivos**

Disponível em `.RData`, `.csv`, `.json` e `SQLite`, com codebook em PDF e CSV (`datasets.csv` e `variables.csv`). Para ingestão em Python/SQL, o pacote CSV + codebook continua sendo o caminho mais simples e mais auditável.

**Dificuldade prática de ingestão**

Baixa a média. Os CSVs são flat e bem documentados. A dificuldade real está em reconciliação de entidades e em entender os limites históricos por dataset.

**Versionamento e reprodutibilidade**

Bom. É um repositório estático, versionável por commit e com licença explícita `CC-BY-SA 4.0`. Para portfolio, isso é operacionalmente viável, mas tem implicação concreta de atribuição e share-alike quando você redistribui o dado ou derivados diretos.

**Consistência e confiabilidade**

Alta para o que cobre. O codebook é forte, a estrutura é coerente e o autor explicita a origem dos dados e as limitações históricas. Isso dá um nível de rastreabilidade bem melhor do que Kaggle/openfootball.

**Lacunas previsíveis**

Estatísticas técnicas por time por jogo não existem. `fixture_player_statistics` no sentido moderno também não existe. Além disso, o próprio dataset explicita recortes históricos importantes:

- `player_appearances` existe só desde `1970`;
- `bookings` existe só desde `1970`;
- `substitutions` existe só desde `1970`.

Isso importa diretamente para lineups e match events históricos. Antes de `1970`, você tem elenco e contexto, mas não o mesmo nível de match participation e eventos discretos.

**Aderência aos domínios canônicos**

| Domínio seu | Fjelstul | Obs |
|---|---|---|
| `fixtures` | ✅ completo | `matches` tem data, placar, fase, estádio, cidade, árbitro |
| `competition_stages` | ✅ direto | `tournament_stages` e `groups` |
| `standings_snapshots` | ✅ forte | `group_standings`; `tournament_standings` ajuda em fechamento de edição |
| `fixture_lineups` | ⚠️ parcial | forte de `1970+`; pré-1970 não tem o mesmo nível de match participation |
| `match_events` | ⚠️ parcial | gols em todo o histórico; bookings/substitutions só de `1970+` |
| `team_coaches` | ✅ | `manager_appointments` e `manager_appearances` |
| `fixture_player_statistics` | ❌ ausente | não há estatísticas individuais por jogo além de gol/cartão |
| `match_statistics` | ❌ ausente | sem posse, passes, finalizações por time |
| `player_season_statistics` | ❌ não aplicável | |
| `player_transfers`, `team_sidelined` | ❌ fora de escopo | |

**O que resolve muito bem:** backbone estrutural da competição, fixtures ricos, groups/stages/standings, identidade de times/jogadores/técnicos e eventos discretos declarados.

**O que resolve parcialmente:** lineups históricos e match events discretos, especialmente quando você atravessa o corte pré-1970.

**Não deve ser usado para:** estatística técnica moderna por jogo ou por jogador.

**Papel arquitetural ideal:** fonte primária para estrutura histórica e cobertura de edição. Também é a melhor âncora para `fixtures`, `competition_stages`, `groups`, `standings_snapshots` e `team_coaches`.

**Riscos técnicos como fonte primária:** o principal risco continua sendo entity matching, não parsing.

---

## 1.3 Datasets Kaggle mencionados

Os datasets Kaggle continuam sendo um espectro amplo de qualidade, escopo, licença e rastreabilidade. O ajuste aqui não é promover Kaggle. É enquadrá-lo corretamente como enrichment pontual ou validação cruzada, nunca como backbone uniforme.

### `die9origephit/fifa-world-cup-2022-complete-dataset`

**Cobertura:** apenas Copa 2022 (64 jogos).

**Tipo de dado:** estatísticas de partida incluindo posse, gols, tentativas, passes e ações defensivas por equipe para cada jogo. Esse é o dataset com melhor mapeamento direto para o seu `match_statistics` em formato tabular — flat, por jogo, por time.

**Granularidade:** nível de partida, por time. Sem granularidade de jogador.

**Formato:** CSV flat. Baixo esforço de ingestão.

**Dificuldade de ingestão:** baixa. É tabular, documentado o suficiente, sem aninhamento.

**Versionamento e reprodutibilidade:** médio-baixo. Datasets Kaggle dependem de conta, têm versões mas sem o controle de git público que você tem no GitHub. A reprodutibilidade depende de versão de dataset específica ser mantida. Para portfolio, isso é um risco real de longo prazo.

**Consistência:** não verificada independentemente. É contribuição de usuário, não de empresa ou pesquisador com credencial verificável. A fonte original dos dados provavelmente é a própria FIFA/Sofascore/FBref, mas a cadeia não é declarada explicitamente.

**Lacunas:** sem histórico, sem dados de jogador, sem eventos, sem lineup detalhado.

**Papel arquitetural:** enrichment pontual ou check externo para `match_statistics` de `2022`, nunca fonte primária canônica.

---

### `piterfm/fifa-football-world-cup` e `jahaidulislam/fifa-world-cup-1930-2022-all-match-dataset`

**Cobertura:** 1930-2022, todos os jogos.

**Tipo de dado:** resultados por jogo — times, placar, data, fase. Alguns incluem gols marcadores e minutos.

**Granularidade:** jogo. Sem jogador, sem estatística técnica.

**Formato:** CSV flat.

**Dificuldade:** muito baixa para o que entrega.

**Consistência:** variável. São agregações de contribuição comunitária. Bons para fixtures básicos, arriscados para dados derivados.

**Aderência:** cobre `fixtures` básico (placar, times, data, fase) para todo o histórico. Nada além disso de forma confiável.

**Valor marginal:** baixo se o Fjelstul já está no pipeline.

**Papel arquitetural:** fallback muito distante para `fixtures` básicos, não necessário como parte do desenho principal.

---

### `swaptr/fifa-world-cup-2022-player-data` e `swaptr/fifa-world-cup-2022-match-data`

**Cobertura:** 2022 apenas.

**Tipo de dado:** estatísticas individuais de jogadores e estatísticas de partida para os 64 jogos.

**Granularidade:** por jogador por jogo para o dataset de player data; por time por jogo para match data.

**Formato:** CSV.

**Fonte de dados declarada:** provavelmente raspagem de FIFA.com ou fonte similar — não declarado explicitamente.

**Papel arquitetural:** só fazem sentido como atalho se você decidir conscientemente não derivar player stats do StatsBomb na primeira onda. Fora isso, são redundantes.

**Decisão recomendada:** permanecem fora do caminho principal.

---

## 1.4 openfootball/worldcup e openfootball/worldcup.more

**Cobertura por edição**

`worldcup` e `worldcup.more` cobrem `1930-2022` com fixtures, placares, marcadores de gols e, no `worldcup.more`, lineups, pênaltis e cartões vermelhos em Football.TXT.

**Tipo de dado**

É dado aberto, legível, comunitário e útil para resgate. Não compete com Fjelstul em estrutura relacional nem com StatsBomb em profundidade.

**Granularidade**

Média-baixa. Suficiente para fixture backbone e alguns eventos discretos, insuficiente para estatística técnica e para governar o modelo estrutural principal.

**Formato dos arquivos**

Football.TXT no repositório principal e possibilidade de conversão para JSON. O parsing é factível, mas o ganho estrutural sobre Fjelstul é baixo.

**Dificuldade prática de ingestão**

Média. Menor do que o StatsBomb, maior do que o Fjelstul, com menos retorno estrutural.

**Versionamento e reprodutibilidade**

Bom. É git público e `CC0`, o que reduz fortemente a fricção para snapshot local e redistribuição em portfolio.

**Consistência e confiabilidade**

Média. É um projeto comunitário estável e útil, mas não oferece o mesmo nível de codebook, curadoria e semântica relacional do Fjelstul.

**Lacunas previsíveis**

Sem estatística técnica, sem standings estruturados, sem coaches, sem contrato forte de completude histórica por domínio.

**Aderência aos domínios canônicos**

| Domínio seu | openfootball | Obs |
|---|---|---|
| `fixtures` | ✅ forte | ótimo fallback histórico |
| `match_events` | ⚠️ parcial | gols, shootouts, cartões vermelhos, algumas substituições |
| `fixture_lineups` | ⚠️ parcial | disponível em `worldcup.more`, com completude variável |
| `competition_stages` | ⚠️ implícito | rounds/grupos presentes, mas não modelados como Fjelstul |
| `standings_snapshots` | ❌ ausente | |
| `match_statistics` | ❌ ausente | |
| `fixture_player_statistics` | ❌ ausente | |
| `team_coaches` | ❌ ausente | |

**O que resolve muito bem:** fallback aberto e simples para `fixtures` e alguns eventos discretos.

**O que resolve parcialmente:** lineups e detalhes de partidas modernas.

**Não deve ser usado para:** governar o modelo estrutural principal quando Fjelstul está disponível.

**Papel arquitetural ideal:** fallback explícito, não fonte primária. Serve para resgate, verificação pontual ou cenário em que a licença `CC0` tenha valor operacional específico para redistribuição pública do snapshot.

**Riscos técnicos:** custo de normalização maior do que o valor marginal quando comparado ao Fjelstul.

---

## 1.5 Síntese crítica: o que vale preencher e o que não vale

**Domínios que valem preencher com cobertura parcial sem comprometer qualidade:**

`fixtures` — com backbone confiável já habilita calendário, match center e navegação.

`fixture_lineups` — desde que a cobertura parcial seja explicitada por edição e por fonte.

`match_events` no nível de gols + cartões + substituições — suficiente para timeline histórica discreta fora do recorte StatsBomb rico.

**Domínios que não vale popular de forma fraca só para dizer que existem:**

`match_statistics` para edições sem evento rico ou fonte tabular confiável.

`fixture_player_statistics` para edições sem base observacional forte.

`player_season_statistics` no mesmo modelo de liga.

**Onde derivar faz sentido:**

`match_statistics` de `2022` e `2018` a partir do StatsBomb.

`fixture_player_statistics` de `2022` e `2018` a partir do StatsBomb.

**Onde aceitar coverage gap documentado é a decisão correta:**

- estatística técnica para edições fora de `2018/2022`;
- lineups e eventos discretos antes de `1970` quando o Fjelstul não os observa no mesmo nível;
- qualquer edição histórica em que o StatsBomb só ofereça jogos amostrais.

Cobertura parcial documentada continua sendo a decisão correta. O que muda é que agora ela precisa ser expressa com recorte por domínio, por edição e por tipo de lacuna.

---

# Parte 2 — Planejamento de ingestão

---

## 2.1 Princípios arquiteturais

**P1 — Separação explícita de origem:** cada dado no bronze carrega `source` e `source_version`. Nunca mergear no bronze.

**P2 — IDs canônicos são seus, não da fonte:** StatsBomb, Fjelstul e openfootball usam identificadores próprios. O warehouse não deve herdar nenhum deles como identidade canônica.

**P3 — Cobertura declarada, não cobertura assumida:** cada edição precisa registrar, por domínio, se a cobertura é `FULL_TOURNAMENT`, `PARTIAL_MATCH_SAMPLE`, `PARTIAL_DOMAIN`, `PROVIDER_COVERAGE_GAP` ou `NOT_IN_SCOPE_YET`.

**P4 — Copa do Mundo não é liga:** a semântica de edição, fases e grupos não pode ser forçada num modelo league-first sem adaptação explícita.

**P5 — Derivação no silver, não no bronze:** agregados derivados do StatsBomb nascem depois do bronze cru.

**P6 — Merge conservador, não completista:** prioridade por domínio e divergência auditada. Nada de merge silencioso.

**P7 — Planejamento estrutural deve acomodar `2018` e `2022` desde o início:** mesmo que a execução inicial carregue só `2022`, o desenho não pode assumir 360 universal nem formato específico demais.

### 2.1.1 Bloqueador real vs decisão arquitetural recomendada

O texto anterior endurecia cedo demais alguns pontos. A classificação correta é esta:

| Classe | Itens | Observação |
|---|---|---|
| **Obrigatório antes da primeira ingestão** | definir `competition_key` e `edition_key`; definir `team_type = national_team`; definir política de snapshot/versionamento por fonte; definir bronze por fonte com metadados de proveniência | sem isso a ingestão nasce sem identidade estável nem reprodutibilidade |
| **Obrigatório antes da primeira publicação canônica em raw** | `provider_entity_map` de times e partidas para a edição publicada; prioridade por domínio; regra de coverage status; decisão explícita para `raw.wc_match_events` | sem isso o merge cross-source fica frágil |
| **Pode nascer simples e evoluir depois** | matching cross-source de jogadores; adaptação completa de `dim_season`/`season_type` no mart; auditoria detalhada por campo; validação cruzada automatizada com Kaggle | importante, mas não precisa bloquear bronze/silver nem o primeiro raw focado em `2022` |
| **Refinamento arquitetural desejável** | modelo canônico comum de eventos cross-provider; workflow de revisão manual assistida; automação de fallback openfootball | melhora governança, mas não é pré-requisito para começar |

Ponto importante: `dim_season` com semântica de torneio é **recomendado antes da integração ampla ao mart/BFF**, mas não é bloqueador real para começar bronze e silver da Copa.

---

## 2.2 Matriz fonte × domínio

| Domínio canônico | Fonte primária | Fonte secundária | Edições cobertas | Qualidade esperada |
|---|---|---|---|---|
| `competition_leagues` | manual/seed | — | todas (1930-2022) | alta — dado de setup |
| `competition_seasons` | manual/seed | — | todas | alta |
| `competition_stages` | Fjelstul | openfootball | todas | alta para estrutura; validar formatos não modernos por edição |
| `competition_rounds` | Fjelstul | openfootball | todas | alta |
| `standings_snapshots` | Fjelstul | — | grupos + fechamento de edição | alta |
| `fixtures` | Fjelstul | StatsBomb (`2018/2022`), openfootball fallback | todas (1930-2022) | alta |
| `match_statistics` | StatsBomb (derivado) | Kaggle pontual (`2022`) | `2018`, `2022` | alta onde houver evento rico |
| `fixture_lineups` | StatsBomb (`2018/2022`) | Fjelstul (`1970+`), openfootball fallback | `2018/2022` completo; histórico parcial | alta em `2018/2022`, média fora disso |
| `match_events` | StatsBomb (`2018/2022`) | Fjelstul histórico; openfootball fallback | rico em `2018/2022`; discreto no histórico | alta em `2018/2022`, parcial fora disso |
| `fixture_player_statistics` | StatsBomb (derivado, 2018/22) | — | 2018, 2022 | alta; gap documentado para resto |
| `player_season_statistics` | derivado específico de torneio | — | 2018, 2022 | média |
| `team_coaches` | Fjelstul | — | todas | alta |
| `head_to_head_fixtures` | derivado de `fixtures` | — | todas | alta (se fixtures completo) |
| `player_transfers` | ❌ sem fonte | — | — | gap declarado |
| `team_sidelined` | ❌ sem fonte | — | — | gap declarado |

Nota importante: os jogos históricos amostrais do StatsBomb não entram aqui como "edições cobertas". Entram como enrichment pontual por partida.

---

## 2.3 Estratégia de canonicalização, IDs e proveniência

Esta parte já estava conceitualmente boa, mas ainda genérica demais. Abaixo está o nível de precisão que o plano precisa.

### 2.3.1 Competição e edição

- `competition_key` recomendado: `fifa_world_cup_mens`
- `edition_key` recomendado: `fifa_world_cup_mens__2022`, `fifa_world_cup_mens__2018`
- `season_label` continua sendo o ano puro (`2022`, `2018`), nunca `2022/23`

Isso evita colisão com outras competições e já deixa o caminho aberto para futura expansão feminina (`fifa_world_cup_womens__2023`) sem quebrar o padrão.

### 2.3.2 Seleção nacional como tipo de team

Seleção não é clube. O modelo precisa tratar isso explicitamente:

- `team_type = national_team`
- `team_scope = international`
- não reutilizar IDs de clube já existentes no projeto

A chave natural não deve ser o nome bruto. O caminho seguro é:

- `team_id` interno canônico como surrogate key;
- `team_key` natural com prefixo de domínio, por exemplo `national_team__ARG`, `national_team__BRA`;
- para casos historicamente ambíguos ou politicamente distintos, parear código e rótulo histórico, por exemplo `national_team__DEU__west_germany`, `national_team__YUG`, `national_team__SCG`, `national_team__SUN`, `national_team__DDR`, `national_team__CSK`.

Regra prática: não colapsar sucessores e predecessores nacionais por equivalência moderna. `West Germany`, `Germany`, `Yugoslavia`, `Serbia and Montenegro` e `Serbia` não devem ser mergeados automaticamente.

### 2.3.3 Convenção para stage e group

Os nomes de fase variam entre fontes e entre eras. A canonicalização precisa separar rótulo bruto de chave canônica.

Recomendação:

- `stage_key` canônico: `group_stage_1`, `group_stage_2`, `round_of_16`, `quarter_final`, `semi_final`, `third_place`, `final`, `playoff`, `replay`
- `stage_label_source`: o nome original da fonte
- `group_key`: `<edition_key>__<stage_key>__<group_code>`

Exemplos:

- `fifa_world_cup_mens__2022__group_stage_1__A`
- `fifa_world_cup_mens__1974__group_stage_2__B`

Isso é importante porque uma convenção ingênua de `Group A/B/C...` quebra em torneios com múltiplas group stages e em formatos antigos.

### 2.3.4 Provider map

`provider_entity_map` deixa de ser opcional no momento em que duas fontes alimentam a mesma edição publicada.

Obrigatório para:

- `team`
- `match`

Necessário depois, quando fizer merge cross-source por jogador:

- `player`
- eventualmente `coach`

Sem `provider_entity_map`, qualquer reconciliação vira join por nome, o que é tecnicamente fraco e auditavelmente ruim.

### 2.3.5 Matching de jogador entre fontes

O desenho correto aqui é por nível de confiança, não por "bateu nome = merge".

**Alta confiança**

- mapping explícito já homologado no `provider_entity_map`; ou
- `normalized_name + date_of_birth + national_team` coincidem sem conflito

**Média confiança**

- `normalized_name + national_team + edition` coincidem;
- posição e/ou camisa reforçam a hipótese;
- não há outro candidato plausível

**Baixa confiança**

- match por nome apenas;
- transliteração ambígua;
- múltiplos candidatos plausíveis

**Regra operacional**

- apenas `alta confiança` entra em merge automático para identidade canônica;
- `média confiança` pode gerar candidato, mas não deve consolidar identidade canônica sem revisão;
- `baixa confiança` vai para revisão manual obrigatória.

Isso evita o pior erro possível: poluir `dim_player` com merges irreversíveis por nome.

### 2.3.6 Proveniência

Para qualquer domínio com sobreposição entre fontes, armazenar:

- `source_name`
- `source_entity_id`
- `source_version`
- `coverage_tier`
- `resolution_rule_used`

Proveniência por campo é desejável, mas não é bloqueador para a primeira onda.

---

## 2.4 Desenho Bronze / Silver / Raw

**Bronze por fonte, não por domínio:**

```
bronze.statsbomb_wc_competitions
bronze.statsbomb_wc_matches
bronze.statsbomb_wc_events
bronze.statsbomb_wc_lineups
bronze.statsbomb_wc_three_sixty
bronze.fjelstul_wc_matches
bronze.fjelstul_wc_squads
bronze.fjelstul_wc_player_appearances
bronze.fjelstul_wc_goals
bronze.fjelstul_wc_bookings
bronze.fjelstul_wc_substitutions
bronze.fjelstul_wc_manager_appointments
bronze.fjelstul_wc_tournament_stages
bronze.fjelstul_wc_groups
bronze.fjelstul_wc_group_standings
bronze.openfootball_wc_matches
```

Cada tabela bronze mantém o schema da fonte o mais próximo possível do original, com colunas de metadados adicionadas pelo pipeline (`ingested_at`, `source_file`, `source_version`).

**Silver: normalização e derivação, ainda separada por origem quando necessário:**

```
silver.wc_fixtures
silver.wc_match_events
silver.wc_lineups
silver.wc_match_stats
silver.wc_player_match_stats (derivado de StatsBomb events para 2018/22)
silver.wc_group_standings
silver.wc_stages
silver.wc_groups
silver.wc_coaches
silver.wc_entity_map
silver.wc_coverage_manifest
silver.wc_source_divergences
```

### 2.4.1 Decisão sobre `raw.wc_match_events`

Aqui o texto anterior deixava ambiguidade demais. A decisão recomendada é clara:

**`raw.wc_match_events` deve nascer como tabela raw permanente, específica da Copa, e não como tabela temporária descartável.**

Motivo:

- o schema observacional do StatsBomb é semanticamente mais rico e diferente do `raw.match_events` atual;
- forçar compatibilidade imediata gera perda de semântica ou quebra do contrato existente;
- chamar de "temporária" incentiva uma dívida sem owner e sem critério de saída.

O trade-off é explícito:

| Opção | Veredito | Trade-off |
|---|---|---|
| `raw.wc_match_events` como staging temporário | **NO-GO** | tende a virar pseudo-permanente sem contrato claro |
| `raw.wc_match_events` como raw permanente e específica de domínio | **GO** | cria uma tabela adicional, mas preserva linhagem e reduz regressão |
| convergir já para `raw.match_events` atual | **NO-GO agora** | alto risco de degradar o dado da Copa ou quebrar o dado já verde |

Decisão final:

- `raw.wc_match_events` é permanente **na arquitetura atual**;
- ela só deve ser descontinuada se existir depois um contrato canônico comum de eventos, lossless, versionado e validado;
- até esse ponto, a convergência é downstream, não pré-requisito.

Para os demais domínios da Copa, a integração nas mesmas tabelas `raw.*` do projeto continua sendo aceitável, desde que a semântica seja realmente compatível.

---

## 2.5 Regras de merge, fallback e prioridade entre fontes

**Por domínio:**

`fixtures`: Fjelstul é autoritativo para completude de edição. StatsBomb valida e enriquece `2018/2022`. openfootball entra apenas como fallback de resgate, não como desempate primário.

`match_events` ricos (`2018/2022`): StatsBomb é autoritativo.

`match_events` históricos discretos: Fjelstul é primário; openfootball é fallback.

`fixture_lineups` (`2018/2022`): StatsBomb é autoritativo.

`fixture_lineups` históricos: Fjelstul é primário onde houver `player_appearances`; pré-1970 o gap deve ser assumido, não mascarado.

`match_statistics`: StatsBomb derivado é preferido. Kaggle é check externo ou enrichment pontual, não origem canônica.

`group_standings`, `groups`, `stages`, `coaches`: Fjelstul é a fonte governante.

**Regra explícita de não-merge silencioso:**

- divergência em `home_team`, `away_team`, `final_score`, `winner`, `stage_key` ou `edition_key` bloqueia publicação canônica do jogo até auditoria;
- divergência em minuto de evento é auditada e reportada, mas não autoriza merge híbrido por conveniência;
- ausência esperada por cobertura não é tratada como erro de pipeline.

---

## 2.6 Recorte inicial recomendado

O texto anterior recomendava "Fase 0 só 2022". Como desenho operacional isso continua correto. Como desenho estrutural, ele precisa ser ajustado.

**Recomendação revisada:**

- **executar ingestão efetiva só de `2022` na Fase 0;**
- **desenhar o pipeline, as chaves e o inventário já compatíveis com `2022 + 2018` desde o início.**

Essa combinação é melhor do que qualquer extremo:

- carregar `2022` e `2018` já na primeira onda amplia blast radius sem ganho imediato;
- desenhar só para `2022` força premissas frágeis, principalmente em torno de 360, coverage tiers e futura replicação do pipeline.

### Fase 0 — Execução inicial

Escopo operacional carregado: **apenas Copa `2022`**.

Escopo estrutural já contemplado no desenho: **Copa `2022` e `2018`**.

**Domínios a implementar na Fase 0:**

1. `competition_leagues` + `competition_seasons` para `2022` e `2018` como seeds de torneio
2. `competition_stages` + `groups` + `competition_rounds` para `2022`
3. `fixtures` para `2022`
4. `fixture_lineups` para `2022`
5. `match_events` para `2022`
6. `standings_snapshots` para `2022`
7. `team_coaches` para `2022`

**O que já deve nascer pronto para `2018` mesmo sem carga operacional:**

- parametrização por `edition_key`
- regra de coverage tier sem 360
- `provider_entity_map` e manifests sem suposição de edição única
- `raw.wc_match_events` sem dependência de atributos exclusivos de 2022

**Fase 0 excluído intencionalmente:**

- derivação de `match_statistics`
- derivação de `fixture_player_statistics`
- integração ampla ao mart/BFF compartilhado
- histórico pré-2018

---

## 2.7 Expansões futuras

**Fase 1 — Derivação de estatísticas para 2022**

Derivar `match_statistics` e `fixture_player_statistics` do StatsBomb de `2022`. Kaggle entra apenas como conferência externa pontual, não como verdade de sistema.

**Fase 2 — Ativação operacional de 2018**

Reutilizar a mesma arquitetura desenhada na Fase 0. A intenção é que o delta seja operacional, não estrutural.

**Fase 3 — Cobertura histórica**

Fjelstul como backbone; openfootball como fallback; StatsBomb histórico apenas como enriquecimento de partidas específicas, nunca como claim de completude de edição.

**Fase 4 — Convergência analítica**

Só aqui vale decidir se a Copa entra em marts compartilhados, marts dedicados ou um contrato canônico comum de eventos.

---

## 2.8 Validação operacional

Esta seção estava faltando. Sem ela, o documento ainda era bom em arquitetura, mas insuficiente como base de execução.

### 2.8.1 Checks obrigatórios por fase

| Fase | Check objetivo | Regra |
|---|---|---|
| snapshot | cada fonte local snapshotada com URL de origem, commit/version, data de acesso e checksum | **bloqueador** se faltar |
| bronze fixtures | `2022 = 64` fixtures esperados; `2018 = 64` quando ativada | **bloqueador** para publicar edição |
| bronze stages/groups | `2022/2018` devem resultar em `8` grupos, `32` linhas finais de group standings e stages canônicos `group_stage_1`, `round_of_16`, `quarter_final`, `semi_final`, `third_place`, `final` | **bloqueador** |
| bronze StatsBomb coverage | `2022`: `64` events + `64` lineups + `64` 360; `2018`: `64` events + `64` lineups + `0` 360 esperado | **bloqueador** para claim de cobertura StatsBomb |
| bronze StatsBomb histórico amostral | `1958 = 2`, `1962 = 1`, `1970 = 6`, `1974 = 6`, `1986 = 3`, `1990 = 1` | não é bloqueador global; é check de integridade do sample se esse enrichment for usado |
| silver merge cross-source | pareamento Fjelstul ↔ StatsBomb de partidas em `2022` e `2018` deve ser `64/64` | **bloqueador** para edição completa |
| silver structural divergence | mismatch em `home_team`, `away_team`, `final_score`, `winner`, `stage_key` deve ser `0` | **bloqueador** |
| raw canonical publish | `team` e `match` com `provider_entity_map` resolvido em `100%` | **bloqueador** |
| rerun idempotente | mesma snapshot, mesma edição, nenhum delta material não explicado em reprocessamento | **bloqueador** |

### 2.8.2 Checks específicos de lineups e events

`fixture_lineups`

- `2022` e `2018`: cobertura esperada `64/64` no StatsBomb
- Fjelstul histórico: validar cobertura de `player_appearances` apenas de `1970+`
- pré-1970: ausência de appearance-level lineup é `PROVIDER_COVERAGE_GAP`, não falha

`match_events`

- `2022` e `2018`: StatsBomb governa
- histórico Fjelstul: gols devem existir; bookings/substitutions só são cobrados de `1970+`
- histórico StatsBomb amostral: cobrar somente os jogos que o próprio repositório publica

### 2.8.3 Divergência entre fontes

Para o mesmo jogo:

- resultado final divergente: bloqueia publicação
- times divergentes: bloqueia publicação
- fase divergente após canonicalização: bloqueia publicação
- minuto de gol divergente: audita e reporta, sem merge híbrido automático

### 2.8.4 Entity matching

`team`

- `100%` obrigatório antes de publicar qualquer edição

`match`

- `100%` obrigatório quando duas fontes alimentam a mesma edição

`player`

- reportar taxa de `alta`, `média`, `baixa` confiança e fila manual;
- somente `alta` confiança auto-publica identidade canônica;
- `média` e `baixa` não bloqueiam bronze/silver, mas bloqueiam merge canônico de jogador se estiverem pendentes.

### 2.8.5 Gaps esperados por domínio

Estes gaps devem ser previstos e registrados:

- `match_statistics` e `fixture_player_statistics` fora de `2018/2022`
- lineups e eventos discretos pré-1970 no Fjelstul
- qualquer edição histórica coberta pelo StatsBomb apenas via sample
- `player_transfers` e `team_sidelined` em todo o escopo Copa

Regra final: falta de dado esperada precisa virar status explícito. `NULL` silencioso não é status.

---

## 2.9 Licença, atribuição e reprodutibilidade

Não precisa virar parecer jurídico. Mas precisa virar regra operacional.

| Fonte | Impacto prático para portfolio com snapshots locais | Recomendação objetiva |
|---|---|---|
| StatsBomb Open Data | excelente para snapshot por commit; termos do repositório pedem atribuição a StatsBomb e uso da marca/logo quando você publica análise baseada no dado; não trate como se fosse CC0 genérico | pode usar como backbone de eventos/lineups; snapshotar por commit; manter README/termos junto; não relicenciar |
| Fjelstul | licença explícita `CC-BY-SA 4.0`; atribuição e indicação de modificações são obrigatórias; share-alike importa se você redistribuir o dataset ou derivados diretos | seguro para portfolio; incluir arquivo de atribuição; se publicar snapshot/derivado, assumir compliance de share-alike |
| Kaggle | licença e proveniência variam por dataset; versão e disponibilidade dependem da página do autor; rastreabilidade é inferior | usar só como enrichment/validação pontual; registrar URL, versão/data, licença declarada e checksum; não tornar canônico se a licença ou a origem estiverem frágeis |
| openfootball | `CC0`; fricção mínima para snapshot e redistribuição; qualidade estrutural menor | melhor fallback do ponto de vista legal; não promover acima de Fjelstul por qualidade |

### 2.9.1 Regra mínima de reprodutibilidade

Cada snapshot local precisa carregar:

- `source_name`
- URL de origem
- data exata de acesso
- commit hash ou versão, quando existir
- checksum do artefato
- nota curta de licença/atribuição

### 2.9.2 Recomendação prática por fonte

StatsBomb

- preferir snapshot por commit do repositório;
- manter referência explícita ao upstream no repositório local;
- usar em portfolio, mas sem tratar "está no GitHub" como autorização automática para relicenciar ou remover atribuição.

Fjelstul

- manter arquivo de atribuição no snapshot;
- documentar qualquer transformação local relevante;
- se o repositório do portfolio for público e carregar dados derivados diretamente desta base, assumir desde já que a obrigação de atribuição/share-alike precisa ser respeitada.

Kaggle

- não consumir live;
- versionar snapshot local só se a licença específica do dataset permitir o uso/re-distribuição que você pretende;
- se a licença estiver ambígua, usar apenas localmente e fora do caminho canônico.

openfootball

- ótimo fallback para snapshot público;
- baixo risco legal relativo;
- continua sendo fallback técnico, não fonte governante.

---

## 2.10 Riscos e trade-offs

**R1 — Entity matching de jogador (alto risco):** continua sendo o principal risco semântico.

**R2 — Forçar convergência prematura de evento (alto risco):** tentar encaixar StatsBomb no `raw.match_events` atual cedo demais é o caminho mais curto para perda de semântica.

**R3 — Semântica de season no mart (médio risco):** não bloqueia bronze/silver, mas bloqueia integração limpa ao mart/BFF se continuar league-first.

**R4 — Cobertura assimétrica por edição (médio risco de produto):** `2022` e `2018` terão profundidade bem maior do que o histórico.

**R5 — Kaggle como dependência estrutural (médio risco):** aumenta fragilidade de licença, proveniência e reprodutibilidade.

**R6 — Obrigações de atribuição/share-alike no Fjelstul (baixo risco técnico, médio risco operacional):** não quebra pipeline, mas precisa ser tratado desde o início se o portfolio for público.

---

## 2.11 GO / NO-GO do que vale implementar agora

| Decisão | GO / NO-GO | Justificativa |
|---|---|---|
| Ingerir Copa `2022` com Fjelstul + StatsBomb | **GO** | melhor ROI e menor ambiguidade operacional |
| Desenhar o pipeline já compatível com `2022 + 2018` | **GO obrigatório de desenho** | evita retrabalho estrutural e suposições 2022-only |
| Carregar `2018` junto na primeira onda operacional | **NO-GO** | amplia blast radius sem necessidade |
| Derivar `match_statistics` de `2022` do StatsBomb | **GO fase 1** | rastreável e consistente |
| Derivar `fixture_player_statistics` de `2022` do StatsBomb | **GO fase 1** | mesmo raciocínio |
| Tratar `raw.wc_match_events` como permanente até existir canônico comum | **GO** | menor risco de regressão |
| Usar openfootball como fallback explícito | **GO condicionado** | útil para resgate e licença simples |
| Usar openfootball como fonte primária | **NO-GO** | Fjelstul é superior como backbone estrutural |
| Usar Kaggle como fonte primária canônica | **NO-GO** | confiabilidade/licença/proveniência mais fracas |
| Popular `match_statistics` fora de `2018/2022` só para "ter cobertura" | **NO-GO** | completude artificial |
| Versionar snapshots locais sem manifesto de origem/licença | **NO-GO** | destrói rastreabilidade |

---

# Parte 3 — Conclusão: recomendação prática de caminho inicial

O caminho mais limpo continua sendo começar por `2022`, mas agora com duas correções importantes:

1. `2022` é a **primeira execução**, não o **único caso estrutural**.
2. `raw.wc_match_events` é uma **decisão arquitetural explícita e permanente no estado atual**, não uma "ponte temporária" vaga.

**Passo 1:** semear `competition_key` e `edition_key` para `2022` e `2018`, com semântica de torneio.

**Passo 2:** snapshot local do Fjelstul com manifesto de licença/atribuição/versionamento.

**Passo 3:** modelar `team_type = national_team` e fechar `provider_entity_map` de times e partidas para `2022`.

**Passo 4:** carregar backbone estrutural de `2022` via Fjelstul: `fixtures`, `stages`, `groups`, `group_standings`, `team_coaches`.

**Passo 5:** snapshot local do StatsBomb por commit e ingestão de `events`, `lineups` e `three-sixty` de `2022`.

**Passo 6:** publicar `raw.wc_match_events` como tabela raw própria e derivar no silver o que for necessário.

**Passo 7:** aplicar os checks operacionais da seção `2.8` antes de declarar a edição verde.

**Passo 8:** só depois ativar a carga operacional de `2018`, que já deve caber sem redesenho.

## Veredito prático

**Pronto para virar base de execução.**

O documento, com os ajustes acima, já tem recorte, prioridades, decisões arquiteturais e critérios operacionais suficientes para iniciar implementação sem depender de mais uma rodada de pesquisa. O que ainda falta depois daqui não é outro plano conceitual. É o artefato de execução: manifests, DDL/contratos e queries de validação.
