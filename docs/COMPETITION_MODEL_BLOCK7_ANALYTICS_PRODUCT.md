# COMPETITION MODEL — BLOCO 7 ANALYTICS E PRODUTO

**Referência:** 2026-03-25  
**Status:** Executável sem FIFA

## Decisão de dependência

O `Bloco 7` foi executado sem reabrir o eixo FIFA porque sua dependência material recai sobre os pilotos centrais já fechados:

- `copa_do_brasil`
- `libertadores`
- `champions_league`

O bloco consome:

- `competition_structure`
- `fact_tie_results`
- `fact_stage_progression`
- `fact_matches`
- contratos de BFF já validados no `Bloco 5A`
- superfícies de frontend já validadas no `Bloco 5B`

## Entregas do bloco

### 1. Analytics por fase

- filtros estruturais por `stageId` e `stageFormat` nos contratos analíticos;
- rankings segmentáveis por fase;
- leitura explícita do formato esportivo de cada fase.

### 2. Comparativo histórico por competição

- comparação entre temporadas da mesma competição;
- preservação da identidade da competição mesmo quando o formato da edição muda;
- suporte direto ao corte estrutural da `champions_league` entre famílias de temporada.

### 3. Bracket visual completo

- visualização das fases eliminatórias em ordem estrutural;
- consumo via APIs aprovadas, sem query ad hoc no frontend;
- zero branch por `competition_key` para renderização estrutural.

### 4. Jornada histórica por time

- histórico multi-temporada dentro da mesma competição;
- combinação de progressão, confrontos e campanha por fase;
- preservação de leitura para `knockout` puro, `group_table` e `hybrid`.

## Boundaries preservados

- FIFA continua fora do caminho crítico desta execução;
- ligas permanecem sem mudança de comportamento estrutural;
- a nova superfície usa estrutura e configuração, não nome da competição;
- não houve rename amplo nem substituição destrutiva.
