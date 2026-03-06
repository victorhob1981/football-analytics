# Analise de Compatibilidade: Blueprint x Manual Funcional

> Data: 2026-02-22  
> Escopo: validar compatibilidade entre `docs/FRONTEND_ARCHITECTURE_BLUEPRINT.md` e `docs/FRONTEND_MANUAL_POSSIBILIDADES.md`, corrigindo divergencias de especificacao.

---

## 1. Veredicto

Compatibilidade estrutural: **alta**.  
As divergencias identificadas eram de **especificacao operacional** (filtros temporais por pagina e contrato de insights), nao de stack ou organizacao de pastas.

Depois dos ajustes aplicados neste ciclo, os dois documentos ficaram alinhados no que e critico para implementacao.

---

## 2. Metodo de Validacao

Criticos usados na comparacao:

1. Time intelligence (recortes globais, regras de precedencia e comportamento por rota).
2. Contrato de insights (nomes de campos, severidade e semantica visual).
3. Compatibilidade de padroes arquiteturais (feature-based, stores globais, registry e coverage state).
4. Risco de ambiguidades que geram implementacao inconsistente.

---

## 3. Achados Principais (antes dos ajustes)

| ID | Divergencia | Impacto |
|---|---|---|
| C-01 | Manual definia 5 recortes temporais (inclui mes), blueprint documentava 4 | Alto |
| C-02 | Blueprint nao definia regra formal para filtros `enabled/partial/disabled` por pagina | Alto |
| C-03 | Contrato de insights com nomes diferentes entre docs (`severidade` vs `severity`, etc.) | Alto |
| C-04 | Ordenacao/hierarquia visual de severidade no `InsightFeed` nao estava obrigatoria | Medio |

---

## 4. Ajustes Aplicados

## 4.1 Blueprint (`docs/FRONTEND_ARCHITECTURE_BLUEPRINT.md`)

- Secao 6.1: `GlobalFilters Store` atualizado para incluir `monthKey` (`YYYY-MM`) no estado global.
- Secao 6.1: regras de precedencia formalizadas para `roundId`, `monthKey`, `lastN` e `dateRange` (mutuamente exclusivos na dimensao temporal).
- Secao 6.2: introduzido `pageFilterConfig` por rota com estados `enabled | partial | disabled`.
- Secao 6.2: regras de UX explicitas para cada estado de filtro.
- Secao 6.4: recortes temporais atualizados para 5 modos (temporada, rodada, mes, lastN, intervalo customizado).
- Secao 6.5: matriz de suporte por pagina vinculada ao `pageFilterConfig`.
- Secao 9.1: contrato canonico de insights reforcado (`insight_id`, `severity`, `explanation`, `evidences`, `reference_period`, `data_source`).
- Secao 9.1: regra de compatibilidade para aliases em PT-BR documentada (normalizacao na BFF).
- Secao 9.2: ordenacao obrigatoria no feed (`critical -> warning -> info`) e semantica visual por severidade.

## 4.2 Manual (`docs/FRONTEND_MANUAL_POSSIBILIDADES.md`)

- Secao 15.2: contrato de insights alinhado com nomes canonicos usados no blueprint.
- Secao 15.2: nota explicita adicionada sobre aliases documentais em PT-BR e normalizacao na API.

---

## 5. Compatibilidade Atual por Tema

| Tema | Status | Observacao |
|---|---|---|
| Stack e arquitetura base | Compativel | Sem conflito entre docs |
| Estrutura por features | Compativel | Manual e blueprint apontam para modularizacao coerente |
| Filtros globais e recortes temporais | Compativel | Ajustado para 5 recortes e suporte por rota |
| Comparativos e rankings | Compativel | Registry/comparison store continuam aderentes |
| Camada de insights | Compativel | Contrato e severidade agora alinhados |
| Coverage e estados de UI | Compativel | Manual e blueprint convergem em `loading/success/partial/empty/error` |

---

## 6. Risco Residual (fora de documentacao)

Os documentos estao alinhados, mas ainda existe trabalho de implementacao no codigo:

1. `monthKey` ainda precisa ser incorporado em `shared/types`, store, hooks e query keys.
2. `pageFilterConfig` precisa ser implementado no shell/layout para controlar `GlobalFilterBar` por rota.
3. O pipeline de insights precisa garantir ordenacao por severidade no backend ou no adaptador frontend.

---

## 7. Conclusao

Nao havia incompatibilidade arquitetural real entre os documentos.  
O problema era especificacao incompleta/inconsistente em pontos operacionais.  
Com os ajustes aplicados, blueprint e manual ficam prontos para orientar implementacao sem ambiguidade nesses temas.
