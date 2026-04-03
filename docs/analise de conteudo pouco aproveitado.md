# Análise de Conteúdo Pouco Aproveitado

Critério desta reorganização: seguir o padrão real do frontend atual. Conteúdo vira página própria quando já é um domínio separado do produto; quando não é, entra na superfície canônica onde o usuário já navega hoje.

## Nova página dedicada

| Nome | Página / destino | Quais perguntas responderia |
|---|---|---|
| Mercado / Transfers | `/market` | Quem entrou e saiu, de onde veio, para onde foi, em qual janela, como está a trilha de carreira do jogador |
| Head-to-Head dedicado | `/head-to-head` | Qual time leva vantagem no confronto, como foi o histórico recente, qual recorte por mando e período muda a leitura |
| Técnicos / Coaches | `/coaches` com entrada para `/coaches/[coachId]` | Quem comandava o time em cada fase, quando houve troca, como o desempenho muda por ciclo de trabalho |
| Landing editorial / institucional | `/landing` | O que o produto cobre, quais campeonatos e temporadas estão disponíveis, por onde começar a navegação |

## Agregar em página existente

| Nome | Página existente | Quais perguntas responderia |
|---|---|---|
| Disponibilidade / Sidelined | `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]?tab=squad` | Quem está fora, por qual motivo, desde quando, como as ausências afetam elenco e escalação |
| Match Statistics de time | `/matches/[matchId]?tab=team-stats` | Quem controlou posse, passe e volume ofensivo no jogo, onde cada time levou vantagem, qual foi o perfil da partida |
| Lineups | `/matches/[matchId]?tab=lineups` | Quem começou jogando, qual formação foi usada, quem ficou no banco, quais escolhas de escalação mudaram a leitura do jogo |
| Match Events / Timeline | `/matches/[matchId]?tab=timeline` | Quando o jogo virou, quem decidiu, quais padrões de gols, cartões e substituições apareceram |
| Fixture Player Statistics | `/matches/[matchId]?tab=player-stats` | Quem foi melhor em cada partida, como foi a atuação individual jogo a jogo, quem sustentou impacto no match center |
| Player Season Statistics | `/competitions/[competitionKey]/seasons/[seasonLabel]/players/[playerId]?tab=stats` | Como foi a temporada do jogador, em que métricas ele se destaca, onde cresceu ou caiu no recorte atual |
| Rankings por métricas | `/rankings/[rankingType]` | Quem lidera passes, defesas, cartões, minutos e outras métricas além de gols e assistências |
| Curadoria da home | `/` | Quais histórias do acervo merecem destaque, quais recortes têm mais profundidade, quem são os nomes e temporadas que merecem entrada editorial |
| Cobertura de elenco no perfil de time | `/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]?tab=squad` | Quem mais jogou, quem virou peça recorrente, quais posições concentram minutos, onde faltam profundidade e contexto no elenco |
