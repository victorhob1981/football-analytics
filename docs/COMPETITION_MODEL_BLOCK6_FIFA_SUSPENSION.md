# COMPETITION MODEL — BLOCO 6 FIFA SUSPENSO

**Referência:** 2026-03-25  
**Status:** Suspenso por dependência externa  
**Escopo impactado:** `Bloco 6A`, `Bloco 6B`, `Bloco 6C`

## Decisão operacional

O eixo FIFA fica **fora do caminho crítico atual** desta frente.

Esta suspensão não reabre os blocos já validados (`1A` a `5B`) e não altera o fechamento estrutural já obtido para:

- `copa_do_brasil`
- `libertadores`
- `champions_league`

## Causa raiz formal

O bloqueio do eixo FIFA é de **dado/contrato operacional com provider**.

No estado atual do ambiente:

- `competition_season_config` já suporta identidades FIFA;
- o provider operacional desta stack não oferece cobertura consultável suficiente para `fifa_world_cup` e `fifa_club_world_cup`;
- não existe alternativa segura já contratada nesta stack para preencher essa lacuna sem criar workaround frágil.

## Consequência de execução

- `Bloco 6A`, `6B` e `6C` permanecem **suspensos**;
- o roadmap executável atual pode avançar apenas em blocos que **não dependam materialmente** de dados FIFA;
- nenhum branch estrutural deve ser introduzido para “simular” FIFA sem cobertura real.

## Regra de retomada

O eixo FIFA só volta para execução quando a pré-condição externa abaixo estiver satisfeita:

- existe cobertura consultável real, estável e validável no provider operacional para as competições FIFA previstas no roadmap.

Na retomada, as validações mínimas continuam sendo:

- `fifa_world_cup` sem times de clube;
- `fifa_club_world_cup` sem temporadas anteriores a `2025`;
- `fifa_intercontinental_cup` sem temporadas anteriores a `2024`;
- reutilização dos modelos já validados de grupo, tie e progressão;
- zero regressão em ligas e nos pilotos centrais.

## Relação com o Bloco 7

A suspensão do eixo FIFA **não autoriza atalho estrutural**.

Ela apenas formaliza que qualquer continuação após `5B` depende de prova objetiva de que o próximo bloco consome somente:

- a fundação já validada;
- os pilotos centrais já fechados;
- dados efetivamente disponíveis no acervo atual.
