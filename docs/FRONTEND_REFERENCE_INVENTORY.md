# Frontend Reference Inventory

Data de referencia: `2026-03-21`  
Escopo: inventario operacional do material em `reference design/`, tratado como fonte visual/estrutural para substituicao progressiva dentro de `frontend/`.

## 1. Regras deste inventario

- `reference design/` nao e app executavel.
- `frontend/` continua sendo o unico app executavel.
- HTML e assets do reference design nao devem ser copiados em big bang para producao.
- clube e jogador usam rotas canonicas contextualizadas por `competitionKey + seasonLabel + entityId`.
- `/teams/:teamId` e `/players/:playerId` ficam reservadas para resolver/redirect de contexto.

## 2. Inventario operacional

| Tela do reference design                     | Destino canonico                                                                                   | Contrato/BFF                                                                                 | Reaproveitamento do frontend atual                                                                    | Bloco recomendado                                |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `in_cio_destaques_do_dia_bloco_6_pt_br`      | `/home`                                                                                            | `GET /api/v1/insights` hoje; agregados de home ainda dependem de composicao BFF              | `frontend/src/app/(platform)/(home)/page.tsx`, `frontend/src/features/insights/**`, shell e coverage  | Depois do shell novo                             |
| `in_cio_vis_o_geral_executiva_bloco_11`      | `/home`                                                                                            | Precisa agregados portfolio-wide e contratos de competicoes/league summary                   | `features/insights`, `shared/components/data-display/**`, shell                                       | Fase final                                       |
| `season_hub_calendar_bloco_2`                | `/competitions/:competitionKey/seasons/:seasonLabel` aba `calendar`                                | `GET /api/v1/matches?competitionId&seasonId&roundId`                                         | `frontend/src/features/matches/**`, `shared/components/data-display/DataTable.tsx`, `GlobalFilterBar` | Depois do shell novo                             |
| `season_hub_standings_bloco_3`               | `/competitions/:competitionKey/seasons/:seasonLabel` aba `standings`                               | Precisa router/contrato de standings                                                         | `CoverageBadge`, `PartialDataBanner`, componentes de tabela/charts                                    | Depois de `calendar`                             |
| `season_hub_rankings_bloco_5_pt_br`          | `/competitions/:competitionKey/seasons/:seasonLabel` aba `rankings`                                | `GET /api/v1/rankings/{rankingType}?competitionId&seasonId`                                  | `frontend/src/features/rankings/**`, `RankingTable`, coverage infra                                   | Com o season hub                                 |
| `match_center_summary_bloco_2`               | `/matches/:fixtureId`                                                                              | `GET /api/v1/matches/{matchId}` ja existe com include flags                                  | `frontend/src/features/matches/**`, `MatchCenterContent`, coverage infra                              | Primeiro bloco visual com dado real              |
| `perfil_do_clube_vis_o_geral_bloco_8_pt_br`  | `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId` aba `overview`                  | Precisa `GET /api/v1/teams/{teamId}/contexts` + summary contextualizado                      | `shell`, `charts`, `StatCard`, `CoverageBadge`; pagina atual de clube e placeholder                   | Depois do BFF de teams                           |
| `perfil_do_clube_partidas_bloco_8_pt_br`     | `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId` aba `matches`                   | `GET /api/v1/matches?teamId&competitionId&seasonId` + summary de team                        | Padrao da lista de partidas, `DataTable`, cobertura por modulo                                        | Depois do `overview` de team                     |
| `perfil_do_clube_elenco_bloco_10_pt_br`      | `/competitions/:competitionKey/seasons/:seasonLabel/teams/:teamId` aba `squad`                     | Precisa `GET /api/v1/teams/{teamId}/squad?competitionId&seasonId` e `availability/sidelined` | `DataTable`, `EmptyState`, badges de coverage                                                         | Expansao de team                                 |
| `perfil_do_jogador_partidas_bloco_9_pt_br`   | `/competitions/:competitionKey/seasons/:seasonLabel/players/:playerId` abas `overview` e `matches` | `GET /api/v1/players/{playerId}?competitionId&seasonId` e/ou `.../matches`                   | `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`, `features/players/**`      | Logo apos teams/context resolver                 |
| `perfil_do_jogador_hist_rico_bloco_10_pt_br` | `/competitions/:competitionKey/seasons/:seasonLabel/players/:playerId` aba `history`               | Precisa `GET /api/v1/players/{playerId}/history` + resolver de contexto                      | `features/players/**`, charts e coverage infra                                                        | Expansao de player                               |
| `confronto_direto_bloco_10_pt_br`            | `/h2h?teamA=:id&teamB=:id&competitionKey=:competitionKey&seasonLabel=:seasonLabel`                 | Precisa contrato dedicado de H2H                                                             | `frontend/src/app/(platform)/head-to-head/page.tsx` e coverage infra; pagina atual e placeholder      | Depois de teams                                  |
| `mercado_transfer_ncias_bloco_11`            | `/more/market`                                                                                     | Precisa contrato dedicado de transferencias                                                  | Apenas shell, coverage e tabelas reutilizaveis                                                        | Ultimos blocos                                   |
| `treinadores_perfil_bloco_11_pt_br`          | `/more/coaches/:coachId`                                                                           | Precisa contrato dedicado de coaches                                                         | Apenas shell e componentes-base                                                                       | Ultimos blocos                                   |
| `disponibilidade_global_bloco_11`            | `/more/availability` ou secao na aba `squad` do club profile                                       | Precisa contrato dedicado de availability                                                    | `CoverageBadge`, `EmptyState`, `DataTable`                                                            | Primeiro como secao de clube, depois tela global |
| `busca_global_overlay_bloco_7_pt_br`         | Overlay global do shell, sem rota propria                                                          | Precisa `GET /api/v1/search` e payload com contexto canonico                                 | `frontend/src/app/(platform)/layout.tsx`, `GlobalFilterBar` como referencia de controles persistentes | Depois do shell novo                             |

## 3. Artefatos de apoio dentro de `reference design/`

- `stitch_home_football_analytics/emerald_pitch/DESIGN.md`: fonte principal de design system.
- `frontend_delivery_plan.md_refined.html`: referencia narrativa; nao substitui os docs versionados em `docs/`.
- `relat_rio_de_auditoria_final_football_analytics.html`: referencia complementar; nao substitui o inventario deste documento.

## 4. Proximo uso correto

- usar este inventario para abrir os proximos blocos sem migracao big bang.
- toda tela nova deve nascer no `frontend/` com base em rota canonica + contrato BFF explicito + reaproveitamento do legado ja verde.

## 5. Status do bloco de contexto

Implementado neste bloco:

- helpers de contexto e geracao de rotas canonicas no `frontend/`;
- rotas canonicas de player/team dentro de `/competitions/:competitionKey/seasons/:seasonLabel/...`;
- resolver funcional para `/players/:playerId` com redirect quando o contexto esta disponivel e fallback de compatibilidade quando nao esta;
- resolver funcional para `/teams/:teamId` com redirect quando o contexto esta disponivel e estado controlado quando nao esta;
- links internos de player atualizados para usar a nova camada de rotas;
- links de team do match center atualizados para a nova rota canonica/resolver.

Pendente para o proximo bloco:

- remover o fallback de compatibilidade de player quando o resolver ja conseguir fechar todo caso real so via `players/contexts`;
- expandir o team profile alem do contrato minimo atual (`summary` + `recentMatches`);
- alinhar season hub e team profile com contratos canonicos completos;
- abrir os proximos contratos de team (`matches`, `squad`, `availability`) sem migracao visual ampla.

## 6. Status do bloco BFF/contexto minimo

Implementado neste bloco:

- `GET /api/v1/players/{playerId}/contexts` no BFF;
- `GET /api/v1/teams/{teamId}/contexts` no BFF;
- `GET /api/v1/teams/{teamId}` como primeiro contrato real de club profile contextualizado;
- resolvers de `/players/:playerId` e `/teams/:teamId` agora consultam o BFF antes do fallback;
- a rota canonica de team deixou de ser placeholder total e passou a consumir o contrato minimo real;
- o redirect curto preserva filtros nao canonicos (`venue`, `lastN`, `dateRange`, `roundId`) ao migrar para a rota canonica.
