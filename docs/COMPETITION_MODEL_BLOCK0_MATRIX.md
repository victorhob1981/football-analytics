# COMPETITION_MODEL_BLOCK0_MATRIX

Data de referencia: `2026-03-25`  
Status: `BLOCO 0`  
Fonte mandataria: `docs/ROADMAP_OPERACIONAL_COMPETITION_MODEL.md`

Este arquivo materializa os entregaveis operacionais do `Bloco 0` sem avancar para schema fisico, ingestao, API ou frontend.

## 1. Baseline confirmado com evidencia objetiva

| Area | Evidencia objetiva | Estado atual confirmado | Implicacao para o roadmap |
|---|---|---|---|
| Estrutura de fase | `dbt/models/staging/stg_competition_stages.sql`, `dbt/models/marts/core/dim_stage.sql` | existe `stage_id`, `stage_name` e `sort_order`; nao existe `stage_format` nem contrato de progressao | `stage` existe, mas ainda nao expressa o papel semantico da fase |
| Standings | `dbt/models/staging/stg_standings_snapshots.sql`, `dbt/models/marts/core/fact_standings_snapshots.sql` | existe recorte por `league_id`, `season_id`, `stage_id`, `round_id`, `team_id`; nao existe `group_id` | o projeto ainda nao representa tabelas paralelas por grupo |
| Partidas | `dbt/models/staging/stg_matches.sql` | `fixtures` ja carregam `stage_id` e `round_id`; nao carregam `tie_id`, `group_id` nem `leg_number` | o modelo atual nao fecha confrontos eliminatorios nem legs |
| API de standings | `api/src/routers/standings.py` | o contrato escolhe um unico `stage` e retorna uma unica tabela | o BFF atual ainda e orientado a liga/tabela unica |
| Acervo atual | `docs/INVENTARIO_DADOS_DO_PROJETO.md`, `docs/DATA_COVERAGE_AUDIT_20260320.md`, `docs/MART_FINAL_COVERAGE_AUDIT.md` | portfolio ativo contem `copa_do_brasil`, `libertadores` e `champions_league` nas temporadas usadas neste bloco | ha base suficiente para fechar o contrato semantico e fixar temporadas-prova |

Leitura operacional:

- o problema desta frente e primariamente de `codigo/modelagem` e `validacao`, nao de ambiente;
- ligas ja sao baseline verde e nao devem ser reabertas no `Bloco 0`;
- qualquer expansao para copa precisa nascer aditiva sobre `stage`, `match` e `standings` ja existentes.

## 2. Escopo fechado do Bloco 0

Entra neste bloco:

- contrato semantico minimo do dominio;
- boundaries entre configuracao de edicao, fase, progressao, produto e excecao;
- matriz por `competition + season` para os pilotos centrais;
- fixacao das temporadas-prova dos pilotos;
- matriz de extensao posterior para identidades FIFA;
- registro dos casos de inferencia permitidos e dos blockers que exigem parada.

Nao entra neste bloco:

- schema fisico definitivo;
- parser/provider implementation;
- marts, APIs ou componentes de frontend;
- rename amplo de `league_*` para `competition_*`;
- edge cases posteriores fora dos pilotos centrais.

## 3. Matriz de responsabilidades por boundary

| Boundary | Deve possuir | Nao deve possuir |
|---|---|---|
| `competition_season_config` | identidade da edicao, `format_family`, `season_format_code`, `participant_scope`, capacidades macro da edicao | semantica detalhada de cada `stage`, grafo completo de progressao, regras de navegacao, excecoes historicas |
| Configuracao de `stage` | `stage_format`, papel estrutural da fase, ordenacao estrutural, elegibilidade para standings/bracket, modos de grupo/leg/eliminacao | identidade global da competicao, links de progressao completos, politica de UI |
| Configuracao de progressao | origem, destino, tipo de classificacao/eliminacao e slots estruturais | tipo do `stage`, regra interna de ranking da tabela, defaults de produto |
| Configuracao de produto | tabs default, superfices prioritarias, navegacao entre standings/groups/bracket/calendar | regra esportiva, decisao de classificado, semantica de confronto |
| Excecoes regulamentares | desvios pontuais versionados e auditaveis que nao cabem no contrato base | regra normal da competicao, configuracao principal da edicao, definicao principal de fase |

Regra de recusa deste bloco:

- se um mesmo artefato precisar decidir identidade da edicao, tipo de fase, progressao e navegacao, o boundary esta errado e o bloco nao fecha.

## 4. Temporadas-prova fixadas

| competition_key | Acervo disponivel confirmado | Temporadas-prova do piloto | Justificativa objetiva |
|---|---|---|---|
| `copa_do_brasil` | `2021-2025` | `2024`, `2025` | as temporadas estao no acervo atual e evitam os `PROVIDER_COVERAGE_GAP` documentados para `2021-2023` em `lineups` e `fixture_player_statistics`; o piloto estrutural continua na mesma familia `knockout` |
| `libertadores` | `2021-2025` | `2024`, `2025` | as temporadas estao no acervo atual e representam o caso hibrido recente usado pelo produto; ha caveats de cobertura em lineups, mas isso nao invalida o contrato estrutural de grupos + mata-mata |
| `champions_league` | `2020/21-2024/25` | `2023/24`, `2024/25` | exigencia explicita do roadmap; o par cobre a fronteira entre `group_table` e `league_table` dentro do mesmo `competition_key` |

## 5. Catalogo de `season_format_code`

| season_format_code | Macro significado |
|---|---|
| `cdb_knockout_progressive_entry_v1` | edicao de mata-mata puro, com entradas em fases diferentes e detalhes de leg resolvidos por configuracao de `stage`, nao pela identidade da edicao |
| `lib_qualification_group_knockout_v1` | edicao hibrida com fases preliminares eliminatorias, fase de grupos e mata-mata final |
| `ucl_group_knockout_v1` | edicoes da Champions com fase inicial em grupos e fases finais eliminatorias |
| `ucl_league_table_knockout_v1` | edicoes da Champions com primeira fase em tabela unica e mata-mata final |
| `fwc_32_group_knockout_v1` | World Cup de `32` selecoes com grupos e mata-mata |
| `fwc_48_group_knockout_v2` | World Cup de `48` selecoes com grupos e classificacao de melhores terceiros; regulamento fino ainda precisa revalidacao antes de codificar detalhe operacional |
| `fcwc_32_group_knockout_v1` | novo Club World Cup quadrienal com `32` clubes, grupos e mata-mata |
| `fcwc_32_group_knockout_v1_pending_reconfirmation` | mesma familia de `2025`, mas sem ativacao automatica antes de reconfirmacao oficial de nova edicao |
| `fic_annual_champions_knockout_v1` | Intercontinental Cup anual em mata-mata com entradas escalonadas por fase |
| `fic_annual_champions_knockout_v1_pending_reconfirmation` | mesma familia anual, dependente de reconfirmacao antes de cada nova edicao futura |

## 6. Matriz operacional dos pilotos centrais

| competition_key | season_label | format_family | season_format_code | participant_scope | Familias obrigatorias de `stage` | `standings_context` esperado | `bracket_context` esperado | Observacao de boundary |
|---|---|---|---|---|---|---|---|---|
| `copa_do_brasil` | `2021` | `knockout` | `cdb_knockout_progressive_entry_v1` | `club` | `knockout` | `not_applicable` | obrigatorio em toda a edicao | diferencas de leg por fase pertencem ao `stage`, nao a `competition season` |
| `copa_do_brasil` | `2022` | `knockout` | `cdb_knockout_progressive_entry_v1` | `club` | `knockout` | `not_applicable` | obrigatorio em toda a edicao | manter o mesmo contrato macro enquanto a familia estrutural nao mudar |
| `copa_do_brasil` | `2023` | `knockout` | `cdb_knockout_progressive_entry_v1` | `club` | `knockout` | `not_applicable` | obrigatorio em toda a edicao | provider coverage gap em lineups/fps nao muda a familia estrutural da edicao |
| `copa_do_brasil` | `2024` | `knockout` | `cdb_knockout_progressive_entry_v1` | `club` | `knockout` | `not_applicable` | obrigatorio em toda a edicao | temporada-prova do piloto puro de mata-mata |
| `copa_do_brasil` | `2025` | `knockout` | `cdb_knockout_progressive_entry_v1` | `club` | `knockout` | `not_applicable` | obrigatorio em toda a edicao | temporada-prova do piloto puro de mata-mata |
| `libertadores` | `2021` | `hybrid` | `lib_qualification_group_knockout_v1` | `club` | `qualification_knockout`, `group_table`, `knockout` | `grouped_table` apenas na fase de grupos | obrigatorio nas fases preliminares e eliminatorias | grupos e mata-mata coexistem na mesma edicao; nao achatar em uma unica tabela |
| `libertadores` | `2022` | `hybrid` | `lib_qualification_group_knockout_v1` | `club` | `qualification_knockout`, `group_table`, `knockout` | `grouped_table` apenas na fase de grupos | obrigatorio nas fases preliminares e eliminatorias | progressao entre grupos e mata-mata pertence a boundary propria |
| `libertadores` | `2023` | `hybrid` | `lib_qualification_group_knockout_v1` | `club` | `qualification_knockout`, `group_table`, `knockout` | `grouped_table` apenas na fase de grupos | obrigatorio nas fases preliminares e eliminatorias | manter contrato unico para a familia atual da competicao |
| `libertadores` | `2024` | `hybrid` | `lib_qualification_group_knockout_v1` | `club` | `qualification_knockout`, `group_table`, `knockout` | `grouped_table` apenas na fase de grupos | obrigatorio nas fases preliminares e eliminatorias | temporada-prova para grupos e para o caso hibrido completo |
| `libertadores` | `2025` | `hybrid` | `lib_qualification_group_knockout_v1` | `club` | `qualification_knockout`, `group_table`, `knockout` | `grouped_table` apenas na fase de grupos | obrigatorio nas fases preliminares e eliminatorias | temporada-prova para grupos e para o caso hibrido completo |
| `champions_league` | `2020/21` | `hybrid` | `ucl_group_knockout_v1` | `club` | `group_table`, `knockout` | `grouped_table` no primeiro stage | obrigatorio nas fases eliminatorias | temporada historica ainda na familia de grupos + mata-mata |
| `champions_league` | `2021/22` | `hybrid` | `ucl_group_knockout_v1` | `club` | `group_table`, `knockout` | `grouped_table` no primeiro stage | obrigatorio nas fases eliminatorias | o comportamento continua dirigido por `season_format_code`, nao por nome |
| `champions_league` | `2022/23` | `hybrid` | `ucl_group_knockout_v1` | `club` | `group_table`, `knockout` | `grouped_table` no primeiro stage | obrigatorio nas fases eliminatorias | manter compatibilidade historica da competicao publica |
| `champions_league` | `2023/24` | `hybrid` | `ucl_group_knockout_v1` | `club` | `group_table`, `knockout` | `grouped_table` no primeiro stage | obrigatorio nas fases eliminatorias | temporada-prova pre-mudanca de formato |
| `champions_league` | `2024/25` | `hybrid` | `ucl_league_table_knockout_v1` | `club` | `league_table`, `knockout` | `single_table` no primeiro stage | obrigatorio nas fases eliminatorias | temporada-prova pos-mudanca; nao gerar grupos ficticios |

## 7. Matriz especifica da Champions por temporada

| season_label | season_format_code | Primeiro `stage_format` obrigatorio | Gera `group`? | Regra obrigatoria |
|---|---|---|---|---|
| `2020/21` | `ucl_group_knockout_v1` | `group_table` | sim | standings e grupos valem apenas na primeira fase; eliminatorias reutilizam o mesmo contrato de `tie` |
| `2021/22` | `ucl_group_knockout_v1` | `group_table` | sim | nenhum branch por nome da competicao e permitido |
| `2022/23` | `ucl_group_knockout_v1` | `group_table` | sim | grupos sao estrutura real, nao editorial |
| `2023/24` | `ucl_group_knockout_v1` | `group_table` | sim | temporada-prova obrigatoria do modelo antigo |
| `2024/25` | `ucl_league_table_knockout_v1` | `league_table` | nao | o primeiro stage e tabela unica; qualquer `group_id` nesse stage e bug estrutural |

## 8. Matriz de extensao posterior para identidades FIFA

| public_competition_key | season_scope | format_family | season_format_code | participant_scope | Primeiro `stage_format` | Tem grupos? | Tem mata-mata? | Status regulatorio |
|---|---|---|---|---|---|---|---|---|
| `fifa_world_cup` | `2010`, `2014`, `2018`, `2022` | `hybrid` | `fwc_32_group_knockout_v1` | `national_team` | `group_table` | sim | sim | `confirmed_family` |
| `fifa_world_cup` | `2026+` | `hybrid` | `fwc_48_group_knockout_v2` | `national_team` | `group_table` | sim | sim | `format_confirmed_detailed_regs_pending` |
| `fifa_club_world_cup` | `2025` | `hybrid` | `fcwc_32_group_knockout_v1` | `club` | `group_table` | sim | sim | `confirmed_family` |
| `fifa_club_world_cup` | `2029+` | `hybrid` | `fcwc_32_group_knockout_v1_pending_reconfirmation` | `club` | `group_table` | sim | sim | `planned_reconfirm_before_activation` |
| `fifa_intercontinental_cup` | `2024-2025` | `knockout` | `fic_annual_champions_knockout_v1` | `club` | `qualification_knockout` | nao | sim | `confirmed_family` |
| `fifa_intercontinental_cup` | `2026+` | `knockout` | `fic_annual_champions_knockout_v1_pending_reconfirmation` | `club` | `qualification_knockout` | nao | sim | `planned_reconfirm_before_activation` |

## 9. Casos de inferencia permitidos e blockers obrigatorios

| Conceito | Suporte explicito hoje | Derivacao permitida nos blocos seguintes | Marcacao obrigatoria | Quando parar como blocker real |
|---|---|---|---|---|
| `stage_format` | nao existe coluna dedicada no staging/mart atual | backfill controlado a partir da matriz aprovada do `Bloco 0` + mapeamento canonico de `stage_name` por competicao-temporada | registrar origem do backfill em configuracao controlada; nao confundir com verdade nativa do provider | se um `stage` nao puder ser classificado sem ambiguidade estrutural |
| `group` | nao existe `group_id` exposto hoje em standings ou matches | extrair discriminante do grupo do payload bruto quando existir; se nao existir, so admitir chave sintetica quando a particao for deterministica e auditavel | `is_inferred=true` para qualquer grupo sintetico | se rows de grupos diferentes nao puderem ser separados de forma deterministica |
| `tie` | nao existe `tie_id` nem `leg_number` em `fixtures` | derivar por `competition + season + stage + pares competitivos`, com ordenacao dos legs por cronologia oficial quando houver mais de um jogo | `is_inferred=true` para `tie` e `leg` derivados | se um jogo puder pertencer a mais de um confronto plausivel |
| `progression` | nao existe entidade canonica de progressao hoje | derivar apenas quando origem, resultado competitivo e destino estrutural fecharem de forma univoca | `is_inferred=true` quando nao houver edge explicito do provider | se houver mais de um destino estrutural plausivel ou repescagem fora do escopo do piloto |

Regra dura:

- inferencia auditavel e admissivel;
- inferencia silenciosa e proibida;
- ausencia de discriminante estrutural minimo e `blocker de dado/contrato`, nao convite para workaround fragil.

## 10. Separacao de essencial vs posterior

Essencial para esta frente:

- `copa_do_brasil`, `libertadores`, `champions_league`;
- contratos de `competition season`, `stage`, `group`, `tie`, `leg`, `progression`;
- distincoes entre `league`, `knockout` e `hybrid`;
- compatibilidade retroativa com ligas por expansao aditiva.

Posterior:

- onboarding FIFA;
- analytics avancados dependentes de brackets e trajetoria por time;
- repescagens intercompeticoes;
- criterios raros de desempate;
- migracao ampla de nomenclatura `league_*` -> `competition_*`.

## 11. Checklist de fechamento do Bloco 0

| Criterio do roadmap | Fechamento neste artefato |
|---|---|
| contrato semantico minimo fechado | preservado no roadmap e operacionalizado por este arquivo |
| boundaries explicitos e sem sobreposicao | secao `3` |
| toda edicao dos pilotos essenciais com `season_format_code` definido | secao `6` |
| temporadas-prova fixadas | secao `4` |
| `champions_league` versionada por temporada | secao `7` |
| escopo essencial separado do posterior | secao `10` |
| casos de inferencia identificados | secao `9` |

Proximo passo seguro apos aprovacao deste arquivo:

- iniciar `Bloco 1A` e carregar `competition_season_config` exatamente com a matriz das secoes `5`, `6` e `8`, sem embutir regras de fase, progressao ou produto nessa entidade.
