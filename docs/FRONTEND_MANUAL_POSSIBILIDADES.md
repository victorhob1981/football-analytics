# Frontend Manual de Possibilidades (Estado Atual + Proximo Ciclo)

Data de referencia: `2026-03-20`  
Projeto: `football-analytics`

## 1) Objetivo deste manual

Este manual e uma base funcional para planejar o frontend sem retrabalho.

Ele assume, ao mesmo tempo:

- estado real atual (raw forte, waves fechadas, caveats classificados);
- estado alvo de curto prazo (consolidacao da camada de consumo em escala portfolio).

Nao e um documento de "feature wish list".  
Tambem nao e um documento conservador demais preso ao que ja esta pronto em poucos endpoints.

---

## 2) Premissas arquiteturais (detalhadas)

## 2.1 Premissas obrigatorias

- Waves `4`, `5`, `6` e `7` estao fechadas.
- O escopo de produto e multi-competicao e multi-temporada.
- O frontend nao deve consultar banco diretamente.
- A camada de consumo oficial e BFF/API interna.
- `raw` e a camada factual mais robusta no estado atual.
- `mart` e a camada preferencial de consumo, mas ainda em consolidacao para portfolio completo.

## 2.2 O que significa "raw forte + mart em consolidacao"

**Na pratica para o frontend:**

- as telas podem e devem ser desenhadas para escopo portfolio agora;
- a estrategia de dados precisa ser `mart-first` com fallback controlado quando necessario;
- coverage-state vira parte da arquitetura de tela, nao detalhe de erro.

**Na pratica para BFF:**

- normalizar contratos para o frontend;
- esconder heterogeneidade de origem (`mart` vs `raw`);
- devolver metadados de cobertura por resposta e por secao.

## 2.3 Papel da BFF (camada anti-corruption)

Funcoes obrigatorias da BFF:

1. uniformizar naming, tipos e filtros globais;
2. compor fontes de dados quando um modulo depender de mais de um dominio;
3. expor coverage-state com reason code;
4. aplicar fallback sem quebrar experiencia da UI;
5. separar erro tecnico de indisponibilidade por coverage.

## 2.4 Estrategia de fallback recomendada

- priorizar `mart` quando objeto estiver consolidado para o recorte;
- usar `raw` quando:
  - mart estiver desatualizada para o escopo;
  - modulo ainda nao tiver modelagem final;
  - dado de detalhe depender de dominio ainda nao agregado.
- marcar explicitamente no `meta.source` qual camada respondeu.

---

## 3) Arquitetura de navegacao (IA/UX)

## 3.1 Fluxos principais de navegacao

Fluxos base obrigatorios:

1. `Competicao -> Temporada -> Fase/Rodada -> Partida`
2. `Competicao -> Clubes -> Clube -> Temporada -> Partidas`
3. `Competicao -> Jogadores -> Jogador -> Temporada -> Historico`
4. `Competicao -> Head-to-Head`
5. `Competicao -> Rankings`

## 3.2 Mapa de rotas conceituais (exemplo)

- `/portfolio`
- `/competitions`
- `/competitions/{competitionKey}`
- `/competitions/{competitionKey}/seasons/{seasonLabel}`
- `/competitions/{competitionKey}/seasons/{seasonLabel}/standings`
- `/competitions/{competitionKey}/seasons/{seasonLabel}/rounds/{roundKey}`
- `/matches`
- `/matches/{fixtureId}`
- `/teams/{teamId}`
- `/competitions/{competitionKey}/seasons/{seasonLabel}/teams/{teamId}`
- `/players`
- `/players/{playerId}`
- `/competitions/{competitionKey}/seasons/{seasonLabel}/players/{playerId}`
- `/head-to-head?teamA={id}&teamB={id}`
- `/rankings`
- `/market`
- `/coaches`
- `/availability`

Regra:

- `/teams/{teamId}` e `/players/{playerId}` funcionam apenas como resolvers de contexto.
- telas canonicas de clube/jogador exigem `competitionKey + seasonLabel + entityId` na rota.

## 3.3 Organizacao visual recomendada

- shell global com filtros persistentes:
  - competicao
  - temporada
  - fase/rodada (quando aplicavel)
  - janela temporal
- conteudo por modulos independentes em cada tela;
- coverage-state por modulo, nao apenas por pagina inteira;
- trilha de navegacao (breadcrumb) sempre visivel para contexto.

---

## 4) Catalogo funcional por modulo (expandido)

## 4.1 Home Portfolio

**Objetivo**  
Visao executiva do portfolio com leitura rapida de competicao/temporada.

**Dados necessarios**

- fixtures
- standings
- resumos por competicao
- destaques de jogadores/clubes

**Fonte dominante**

- curto prazo: composicao BFF (`mart` + `raw`)
- alvo: `mart` consolidada portfolio-wide

**Maturidade atual**

- **Em consolidacao**

**Caveats**

- depende de rematerializacao da mart para homogeneidade.

**UI possivel agora**

- cards de volume (jogos, gols, media);
- rankings curtos;
- links rapidos para competicao/temporada.

**UI para fase seguinte**

- feed de insights;
- comparativos cross-competition mais robustos.

## 4.2 Pagina de Competicao

**Objetivo**  
Hub da competicao com navegacao para temporada, standings, partidas e ranking.

**Dados necessarios**

- estrutura de competicao
- catalogo de temporadas
- standings
- fixtures

**Fonte dominante**

- BFF sobre raw estruturado + mart quando disponivel

**Maturidade atual**

- **Disponivel hoje**

**Caveats**

- campos de contexto podem ser parciais por recorte.

**UI possivel agora**

- seletor de temporada;
- cards de resumo;
- atalhos para standings, calendario e rankings.

## 4.3 Pagina de Temporada

**Objetivo**  
Visao concentrada da competicao naquele recorte temporal.

**Dados necessarios**

- fixtures da temporada
- standings por rodada
- agregados de clube/jogador

**Fonte dominante**

- `raw` + marts de apoio

**Maturidade atual**

- **Disponivel hoje** (com partes em consolidacao)

**UI possivel agora**

- resumo da temporada;
- trend por rodada;
- destaques de artilharia/assistencia.

## 4.4 Standings

**Objetivo**  
Tabela oficial e evolucao de classificacao.

**Dados necessarios**

- standings snapshots
- round metadata

**Fonte dominante**

- `raw.standings_snapshots` + marts de evolucao

**Maturidade atual**

- **Disponivel hoje**

**UI possivel agora**

- tabela completa;
- mini-graficos de posicao/pontos por time;
- comparador de times na mesma tabela.

## 4.5 Calendario / Lista de Partidas

**Objetivo**  
Navegar fixtures e abrir match center.

**Dados necessarios**

- fixtures
- status da fixture
- metadados de rodada/fase

**Fonte dominante**

- `raw.fixtures` + fact de matches quando disponivel

**Maturidade atual**

- **Disponivel hoje**

**Caveats**

- metadados secundarios (weather/attendance/referee) podem faltar.

**UI possivel agora**

- filtros por status e periodo;
- agrupamento por rodada/data;
- quick summary de placar.

## 4.6 Match Center

**Objetivo**  
Detalhamento completo da partida em secoes independentes.

**Dados necessarios**

- fixture base
- team stats
- lineups
- player stats
- events timeline

**Fonte dominante**

- composicao BFF entre raw e mart

**Maturidade atual**

- **Disponivel com caveat**

**Caveats**

- coverage por secao varia por fixture/escopo.

**UI possivel agora**

- abas ou secoes:
  - resumo
  - timeline
  - lineups
  - player stats
  - comparativo de times
- badge de coverage por secao.

## 4.7 Timeline de Eventos

**Objetivo**  
Contar a historia da partida minuto a minuto.

**Dados necessarios**

- match_events por fixture

**Fonte dominante**

- `raw.match_events` / `mart.fact_match_events`

**Maturidade atual**

- **Disponivel com caveat leve**

**Caveats**

- residual pequeno de provider coverage em escopos especificos.

**UI possivel agora**

- lista cronologica;
- filtros por tipo de evento;
- highlights de gol/cartao/substituicao.

## 4.8 Lineups

**Objetivo**  
Visualizar titulares, banco e participacao.

**Dados necessarios**

- fixture_lineups
- metadado de time/jogador

**Fonte dominante**

- `raw.fixture_lineups` / `mart.fact_fixture_lineups`

**Maturidade atual**

- **Disponivel com caveat**

**Caveats**

- coverage parcial em recortes especificos;
- alguns registros sem `player_id`.

**UI possivel agora**

- grid de titulares por time;
- banco de reservas;
- indicador de ausencia de dados por slot.

## 4.9 Player Stats da Partida

**Objetivo**  
Comparar desempenho individual no jogo.

**Dados necessarios**

- fixture_player_statistics
- contexto de jogador/time

**Fonte dominante**

- `raw.fixture_player_statistics` / `mart.fact_fixture_player_stats`

**Maturidade atual**

- **Disponivel com caveat**

**Caveats**

- gaps pontuais por payload de provider.

**UI possivel agora**

- tabela ordenavel por metrica;
- filtros por time/posicao;
- comparador lado a lado de atletas.

## 4.10 Perfil de Clube

**Objetivo**  
Consolidar desempenho do clube por temporada e recortes recentes.

**Contexto minimo obrigatorio**

- `competitionKey`
- `seasonLabel`
- `teamId`

**Dados necessarios**

- fixtures
- standings
- match statistics
- lineups/jogadores

**Fonte dominante**

- composicao BFF

**Maturidade atual**

- **Em consolidacao**

**UI possivel agora**

- forma recente;
- historico de resultados;
- cards ofensivo/defensivo;
- jogadores em destaque.

## 4.11 Perfil de Jogador

**Objetivo**  
Historico de performance por jogo e por temporada.

**Contexto minimo obrigatorio**

- `competitionKey`
- `seasonLabel`
- `playerId`

**Dados necessarios**

- fixture_player_statistics
- player_season_statistics

**Fonte dominante**

- raw + marts de resumo

**Maturidade atual**

- **Disponivel com caveat**

**UI possivel agora**

- resumo sazonal;
- serie de ultimos jogos;
- radar simples de metricas-chave;
- comparador entre jogadores.

## 4.12 Rankings

**Objetivo**  
Central unica para liderancas de jogadores e times.

**Dados necessarios**

- agregados de jogador
- agregados de clube
- standings e metricas de jogo

**Fonte dominante**

- `mart` (quando consolidada) + composicao BFF

**Maturidade atual**

- **Em consolidacao** (ja com entregas iniciais possiveis)

**UI possivel agora**

- ranking de jogadores por gols/assistencias/rating;
- ranking de times por posse/passe;
- filtros por competicao/temporada.

## 4.13 Head-to-Head

**Objetivo**  
Historico e dominancia de confronto direto.

**Dados necessarios**

- h2h fixtures
- resumo por par de times

**Fonte dominante**

- raw + resumo mart

**Maturidade atual**

- **Disponivel hoje**

**UI possivel agora**

- placar historico agregado;
- ultimos confrontos;
- tendencia recente do duelo.

## 4.14 Mercado (Transfers)

**Objetivo**  
Acompanhar movimentacoes de jogadores.

**Dados necessarios**

- player_transfers

**Fonte dominante**

- raw com composicao BFF

**Maturidade atual**

- **Em consolidacao**

**UI possivel agora**

- feed de transferencias;
- filtros por clube, periodo e tipo;
- perfil de jogador com historico de mercado.

## 4.15 Tecnicos

**Objetivo**  
Mostrar historico e impacto de tecnicos.

**Dados necessarios**

- team_coaches
- resumos de performance (quando disponiveis)

**Fonte dominante**

- raw + marts de resumo

**Maturidade atual**

- **Em consolidacao**

**UI possivel agora**

- timeline de passagem por clubes;
- cards de desempenho agregado.

## 4.16 Disponibilidade / Sidelined

**Objetivo**  
Contextualizar ausencias do elenco.

**Dados necessarios**

- team_sidelined

**Fonte dominante**

- raw + composicao BFF

**Maturidade atual**

- **Em consolidacao**

**UI possivel agora**

- lista de indisponiveis por clube;
- filtros por categoria (lesao/suspensao);
- impacto potencial no pre-jogo.

---

## 5) Estados de cobertura da UI (expandido)

## 5.1 Estados padrao

- `complete`
- `partial`
- `unavailable`
- `unknown`

## 5.2 Comportamento esperado por estado

| Estado | Comportamento visual | Acoes de UX |
|---|---|---|
| `complete` | render normal | sem banner |
| `partial` | render modulo + badge de cobertura parcial | mostrar tooltip com motivo e escopo |
| `unavailable` | empty-state contextualizado | CTA para trocar recorte/temporada |
| `unknown` | fallback conservador | nao afirmar completude |

## 5.3 Mensagens recomendadas

`partial`:

- "Parte dos dados deste recorte nao foi disponibilizada pelo provider."
- "Este modulo esta parcialmente disponivel para a temporada selecionada."

`unavailable`:

- "Nao ha dados disponiveis para este modulo neste recorte."
- "Tente outra temporada ou competicao."

`unknown`:

- "Cobertura ainda nao confirmada para este recorte."

## 5.4 Exemplos por modulo

- Match center timeline:
  - `partial`: renderizar eventos disponiveis + badge.
- Lineups:
  - `partial`: mostrar titulares disponiveis e slots sem identificacao.
- Player stats:
  - `unavailable`: exibir tabela vazia com explicacao de coverage.
- Rankings:
  - `unknown`: ocultar ordenacao "global" ate confirmar base comparavel.

---

## 6) Contratos conceituais de API/BFF

## 6.1 Contrato base de resposta

```json
{
  "data": {},
  "meta": {
    "scope": {
      "competitionKey": "brasileirao_a",
      "seasonLabel": "2024",
      "stageId": null,
      "roundId": null
    },
    "coverage": {
      "status": "complete|partial|unavailable|unknown",
      "reasonCode": "PROVIDER_COVERAGE_GAP|PIPELINE_LIMIT|NOT_APPLICABLE|UNKNOWN",
      "coveragePct": 100
    },
    "source": {
      "primaryLayer": "mart|raw",
      "fallbackLayer": "raw|null",
      "generatedAt": "2026-03-20T00:00:00Z"
    }
  }
}
```

## 6.2 Filtros globais obrigatorios

- `competitionKey`
- `seasonLabel`
- `stageId` (quando aplicavel)
- `roundId` (quando aplicavel)
- `dateFrom` e `dateTo` ou `lastN`

## 6.3 Filtros contextuais por modulo

- match list: `status`, `teamId`, `venueId`
- ranking: `metric`, `minMinutes`, `position`, `window`
- player profile: `competitionKey`, `seasonLabel` (obrigatorios), `window`
- club profile: `competitionKey`, `seasonLabel` (obrigatorios), `homeAway`

## 6.4 Endpoints conceituais (imediatos)

- `GET /v1/competitions`
- `GET /v1/competitions/{competitionKey}/seasons`
- `GET /v1/standings`
- `GET /v1/matches`
- `GET /v1/matches/{fixtureId}`
- `GET /v1/matches/{fixtureId}/events`
- `GET /v1/matches/{fixtureId}/lineups`
- `GET /v1/matches/{fixtureId}/player-stats`
- `GET /v1/players`
- `GET /v1/players/{playerId}` com `competitionId` + `seasonId` no consumo canonico
- `GET /v1/rankings`
- `GET /v1/head-to-head`

## 6.5 Endpoints conceituais (alvo curto prazo)

- `GET /v1/teams/{teamId}/contexts`
- `GET /v1/teams/{teamId}` com `competitionId` + `seasonId`
- `GET /v1/teams/{teamId}/season-summary`
- `GET /v1/players/{playerId}/contexts`
- `GET /v1/market/transfers`
- `GET /v1/coaches`
- `GET /v1/coaches/{coachId}`
- `GET /v1/availability/sidelined`
- `GET /v1/coverage/report`

---

## 7) Entregaveis por fase (detalhado)

## 7.1 MVP funcional (fase 1)

Escopo:

- shell de navegacao portfolio;
- filtros globais;
- standings;
- lista de partidas;
- match center basico (resumo + timeline + lineups + player stats);
- rankings iniciais.

Dependencias:

- contratos BFF minimos;
- coverage-state padrao.

## 7.2 Expansao 1 (fase 2)

Escopo:

- perfil de clube completo;
- perfil de jogador completo;
- head-to-head dedicado;
- ranking com recortes temporais;
- badges de cobertura refinados por modulo.

Dependencias:

- evolucao de composicao BFF;
- consolidacao parcial de marts de resumo.

## 7.3 Expansao 2 (fase 3)

Escopo:

- home executiva portfolio robusta;
- comparativos avancados clube x clube e jogador x jogador;
- feed de insights automatizados;
- modulos de mercado, tecnicos e disponibilidade com UX consistente.

Dependencias:

- rematerializacao dirigida de `mart`;
- contratos de modulo secundarios estabilizados.

## 7.4 Backlog estrutural

- mart 100% portfolio-ready sem fallback frequente;
- cobertura uniforme em dominios profundos;
- camada de metrica derivada mais rica no mart/BFF;
- observabilidade de coverage por endpoint em tempo real.

---

## 8) Maximo de possibilidades de frontend sustentadas hoje ou no curto prazo

## 8.1 Widgets e cards que ja fazem sentido planejar

- cards de volume por competicao/temporada;
- cards de forma recente por clube;
- cards de tendencia de gols pro/contra;
- cards de lideres de ranking por metrica;
- tabela de standings com mini-trend;
- calendario por rodada com status;
- timeline minuto a minuto;
- comparador de times no match center;
- tabela de player stats com ordenacao multi-coluna;
- perfil de jogador com historico de jogos;
- comparador de jogadores por janela temporal;
- head-to-head summary e ultimos confrontos;
- feed de transferencias;
- painel de indisponiveis por clube;
- painel de cobertura de dados por modulo.

## 8.2 Modulos com alto valor e baixo risco de retrabalho

- navegacao portfolio + filtros globais;
- standings + fixtures + match center;
- perfis de jogador/clube em versao incremental;
- ranking center com coverage-state.

## 8.3 Modulos com alto valor e dependencia de consolidacao

- home executiva cross-competition;
- insights automaticos em larga escala;
- mercado/tecnicos/disponibilidade com camadas analiticas completas.

---

## 9) Riscos e guardrails para nao gerar retrabalho

1. Nao acoplar UX a um unico campeonato/temporada.
2. Nao tratar caveat de provider como erro de plataforma.
3. Nao depender de mart "perfeita" para iniciar frontend.
4. Nao esconder coverage-state do usuario final.
5. Nao assumir uniformidade de metadados secundarios em todas as competicoes.

Guardrail de implementacao:

- cada modulo precisa declarar:
  - fonte primaria de dado,
  - fallback permitido,
  - coverage-state esperado,
  - criterio de "ready" para sair de consolidacao.

---

## 10) O que ainda depende de validacao futura para fechar 100%

- cronograma final de rematerializacao portfolio da `mart`;
- normalizacao final da metadata semantica em `fixture_player_statistics`;
- contratos BFF finais para mercado/tecnicos/disponibilidade;
- evolucao do painel de cobertura em nivel de endpoint/modulo.

---

## 11) Conclusao operacional para planejamento frontend

A direcao correta e:

1. planejar frontend portfolio-wide agora;
2. entregar em fases, com cobertura explicita;
3. evoluir a camada de consumo em paralelo;
4. evitar qualquer arquitetura que suponha mono-competicao ou mart plenamente consolidada desde o dia 1.

Esse caminho permite acelerar produto sem perder consistencia tecnica.

---

## 12) Referencias usadas

- `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- `docs/DATA_COVERAGE_AUDIT_20260320.md`
- `docs/MART_FINAL_COVERAGE_AUDIT.md`
- `docs/MART_FRONTEND_BFF_CONTRACTS.md`
- `docs/WAVES_4_TO_7_FINAL_CLOSURE.md`
- `docs/FRONTEND_ARCHITECTURE.md`
- `docs/FRONTEND_DELIVERY_PLAN.md`
