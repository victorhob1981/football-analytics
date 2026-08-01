# Mapa de performance de rotas — 2026-08-01

## Escopo e método

Foram medidas 30 rotas públicas representativas, todas respondendo `200` no deploy final em `https://football.victorhob.me`.

- `cold-client`: primeiro GET usando um novo `HttpClient`, conexão e TLS novos para cada rota.
- `warm`: mediana de três GETs usando a mesma conexão.
- As medidas são resposta HTTP do HTML, não o tempo total até todos os dados client-side aparecerem.
- Os tempos estão em milissegundos; `1.000 ms = 1 s`.
- Isso não equivale a reiniciar o processo para cada rota. Houve reinício controlado do frontend antes da matriz final; o processo da API não foi reiniciado.

## Matriz antes e depois

| Rota | Representante | Antes cold / warm | Depois cold / warm | Cache depois |
| --- | --- | ---: | ---: | --- |
| `/` | `/` | 1.387 / 201 | 238 / 30 | `s-maxage=31536000` |
| `/landing` | `/landing` | 369 / 103 | 117 / 91 | `s-maxage=31536000` |
| `/analises` | `/analises` | 773 / 239 | 299 / 96 | `no-store` |
| `/analytics` | `/analytics` | 290 / 94 | 171 / 59 | `no-store` |
| `/clubs` | `/clubs` | 63 / 24 | 91 / 140 | `s-maxage=31536000` |
| `/clubs/[clubId]` | `/clubs/3000000001540` | 142 / 108 | 1.512 / 96 | `no-store` |
| `/competition/[competitionId]` | `/competition/564` | 186 / 78 | 100 / 72 | `no-store` |
| `/competitions` | `/competitions` | 203 / 74 | 63 / 22 | `s-maxage=31536000` |
| `/competitions/[competitionKey]` | `/competitions/la_liga` | 697 / 144 | 140 / 138 | `no-store` |
| `/competitions/[competitionKey]/seasons/[seasonLabel]` | `/competitions/la_liga/seasons/2025_26` | 98 / 75 | 121 / 126 | `no-store` |
| `.../seasons/[seasonLabel]/clubs/[clubId]` | `/competitions/la_liga/seasons/2025_26/clubs/3000000001540` | 100 / 90 | 213 / 126 | `no-store` |
| `.../seasons/[seasonLabel]/players/[playerId]` | `/competitions/la_liga/seasons/2025_26/players/37562171` | 98 / 96 | 123 / 104 | `no-store` |
| `.../seasons/[seasonLabel]/teams/[teamId]` | `/competitions/la_liga/seasons/2025_26/teams/3000000001540` | 179 / 63 | 151 / 111 | `no-store` |
| `/copa-do-mundo` | `/copa-do-mundo` | 66 / 20 | 228 / 45 | `s-maxage=31536000` |
| `/copa-do-mundo/[edition]` | `/copa-do-mundo/2022` | 79 / 83 | 396 / 258 | `no-store` |
| `/copa-do-mundo/finais` | `/copa-do-mundo/finais` | 56 / 22 | 207 / 71 | `s-maxage=31536000` |
| `/copa-do-mundo/rankings` | `/copa-do-mundo/rankings` | 59 / 25 | 90 / 27 | `s-maxage=31536000` |
| `/copa-do-mundo/selecoes` | `/copa-do-mundo/selecoes` | 52 / 22 | 75 / 21 | `s-maxage=31536000` |
| `/copa-do-mundo/selecoes/[selection]` | `/copa-do-mundo/selecoes/world-cup-brazil` | 89 / 49 | 105 / 100 | `no-store` |
| `/head-to-head` | `/head-to-head` | 57 / 26 | 73 / 18 | `s-maxage=31536000` |
| `/market` | `/market` | 55 / 24 | 74 / 20 | `s-maxage=31536000` |
| `/matches` | `/matches` | 65 / 23 | 73 / 26 | `s-maxage=31536000` |
| `/matches/[matchId]` | `/matches/4000000497724` | 86 / 83 | 175 / 115 | `no-store` |
| `/players` | `/players` | 63 / 18 | 73 / 22 | `s-maxage=31536000` |
| `/players/[playerId]` | `/players/37562171` | 88 / 78 | 120 / 94 | `no-store` |
| `/power-bi` | `/power-bi` | 74 / 79 | 122 / 59 | `no-store` |
| `/rankings` | `/rankings` | 280 / 105 | 103 / 24 | `s-maxage=31536000` |
| `/rankings/[rankingType]` | `/rankings/player-goals` | 92 / 83 | 107 / 83 | `no-store` |
| `/teams` | `/teams` | 79 / 94 | 132 / 125 | `no-store` |
| `/teams/[teamId]` | `/teams/3000000001540` | 81 / 76 | 119 / 47 | `no-store` |

As diferenças de rotas que não foram alteradas devem ser lidas como observação de rede/cache, não como ganho ou regressão causal. A mudança objetiva da matriz foi `/rankings`: o build final passou de `ƒ Dynamic` para `○ Static` e a resposta passou a ter cache público.

## Gargalos de dados e correções

| Superfície | Evidência anterior | Estado final |
| --- | --- | --- |
| Perfil de jogador padrão | BFF com histórico, partidas e estatísticas completos observado em ~6,5 s | O overview pede só resumo + partidas recentes; `6039 B`, 719 ms cold-client / 18 ms warm na amostra medida |
| Perfil de clube padrão | BFF completo observado em ~28,4 s | O overview pede resumo; `1144 B`, 53 ms cold-client / 29 ms warm na amostra medida |
| Aba de elenco | `mart.fact_fixture_lineups` fazia `Parallel Seq Scan` em 747.908 linhas e a aba expirava em 15 s | Índices `(team_id, match_id)` e `(team_id, fixture_id)` aplicados com `CONCURRENTLY`; plano passou a `Index Scan`, BFF da aba respondeu `200` em ~0,98 s na primeira medição |
| Hub `/rankings` | `force-dynamic`, `revalidate=0` e uso server-side de `searchParams` | Catálogo estático; links client-side preservam os filtros atuais dentro de `Suspense` |

A migration dos índices está em [`20260801173000_team_profile_lineup_indexes.sql`](../../db/migrations/20260801173000_team_profile_lineup_indexes.sql).

## Validação prática

- `/rankings` exibiu os oito cards esperados.
- `/rankings?competitionId=2&seasonId=2024` preservou `competitionId` e `seasonId` nos links dos cards.
- Perfil de jogador exibiu identidade, carreira documentada, resumo e partidas; as abas adiadas mostraram `Abrir`, não zero artificial.
- A aba de histórico do jogador carregou cinco contextos com dados.
- Perfil contextual de clube exibiu resumo; a aba de elenco deixou de expirar e carregou o estado vazio documentado do clube testado.
- `/api/health` e `/bff/health` responderam `ok`; o console do navegador não registrou erros.

## Riscos residuais

- A matriz não representa cold start de processo por rota; fazer isso para todas as rotas exigiria reiniciar produção repetidamente e introduziria indisponibilidade desnecessária.
- A rota `/competitions/la_liga/seasons/2025_26` continua entre as mais pesadas no navegador por carregar uma superfície client-side grande, embora o HTML medido seja rápido.
- O build local no Windows continua falhando apenas na cópia de symlinks para `.next/standalone` (`EPERM`); o build Linux usado no deploy passou integralmente.
