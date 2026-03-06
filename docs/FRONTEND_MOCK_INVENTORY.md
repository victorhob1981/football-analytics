# Inventario de Mocks e Placeholders no Frontend

Data da auditoria: 2026-02-25  
Escopo: `frontend/src` e `frontend/e2e`  
Metodo: 2 varreduras (busca textual ampla + segunda busca de validacao com termos alternativos e revisao manual dos arquivos criticos)

## Resumo

Foram encontrados mocks/placeholders em telas de producao, rotas ainda nao implementadas, paginas "Em breve", trechos parcialmente ilustrativos e mocks de teste E2E.

## 1) Mocks de dados em telas de producao

1. Arquivo: `frontend/src/app/(platform)/clubs/[clubId]/page.tsx`  
Tipo: Mock de perfil/insights de clube  
Linhas-chave: `68`, `230`, `436`, `462`  
Evidencias: `getMockProfile(...)`, `const insights = [...]`, uso direto do perfil mock e aviso visual `Dados ilustrativos`.

2. Arquivo: `frontend/src/app/(platform)/clubs/compare/page.tsx`  
Tipo: Mock de comparacao de clubes + H2H hardcoded  
Linhas-chave: `49`, `100-101`, `278`, `366-367`, `380`  
Evidencias: `MOCK_CLUBS`, `getClub(...)` com fallback mock, objeto `h2h` fixo e aviso `Dados ilustrativos`.

3. Arquivo: `frontend/src/app/(platform)/coaches/[coachId]/page.tsx`  
Tipo: Mock de perfil de tecnico + ranking  
Linhas-chave: `60`, `87-88`, `148`, `157`, `231`  
Evidencias: `MOCK_DATA`, `getMockCoach(...)`, `RANKING_DATA` fixo.

4. Arquivo: `frontend/src/app/(platform)/audit/page.tsx`  
Tipo: Dashboard com blocos de dados estaticos/mock  
Linhas-chave: `19`, `56`, `119`, `130`, `173`  
Evidencias: colecoes fixas `MODULES`, `SYNC_STATE`, `METRIC_ISSUES`, `RAW_COVERAGE`.

## 2) Placeholders de rota (nao implementado)

1. Arquivo: `frontend/src/app/(marketing)/landing/page.tsx`  
Linhas-chave: `4-5`  
Evidencias: texto de rota + `TODO: Placeholder da area (marketing).`

2. Arquivo: `frontend/src/app/(platform)/competition/[competitionId]/page.tsx`  
Linhas-chave: `10-11`  
Evidencias: texto de rota + `TODO: Placeholder Competition.`

## 3) Paginas "Em breve" (placeholder funcional)

1. Arquivo: `frontend/src/app/(platform)/coaches/page.tsx`  
Evidencia: usa `ComingSoonPage`.

2. Arquivo: `frontend/src/app/(platform)/market/page.tsx`  
Evidencia: usa `ComingSoonPage`.

3. Arquivo: `frontend/src/app/(platform)/head-to-head/page.tsx`  
Evidencia: usa `ComingSoonPage`.

4. Arquivo: `frontend/src/app/(platform)/players/compare/page.tsx`  
Evidencia: usa `ComingSoonPage`.

5. Arquivo: `frontend/src/shared/components/feedback/ComingSoonPage.tsx`  
Linhas-chave: `11`, `25`  
Evidencias: componente base de "Em breve".

## 4) Mock/placeholder parcial dentro de telas reais

1. Arquivo: `frontend/src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx`  
Tipo: baseline ilustrativo + secoes placeholder  
Linhas-chave: `107-111`, `396`, `409`, `416`, `423`  
Evidencias: valores fixos de media da liga (`liga: 4/3/20/75/65`), texto `estimativa ilustrativa` e blocos marcados como placeholder (lineups, eventos, transferencias).

2. Arquivo: `frontend/src/app/(platform)/players/page.tsx`  
Tipo: funcionalidade desabilitada temporariamente  
Linha-chave: `283`  
Evidencia: tooltip `Filtro por posição em breve`.

## 5) Mocks de teste (E2E)

1. Arquivo: `frontend/e2e/fixtures/mock-api.ts`  
Linhas-chave: `16`, `31`, `166-197`  
Evidencias: intercepta `**/api/v1/**` e responde payloads mock (`route.fulfill`, `sample_value` etc.).

2. Arquivo: `frontend/e2e/rankings-stability.spec.ts`  
Linhas-chave: `3`, `7`, `24`  
Evidencias: usa `installApiMocks(...)` e contem assert de placeholder (`RankingMetricSelector: placeholder`).

3. Arquivo: `frontend/e2e/global-filters-players.spec.ts`  
Linhas-chave: `3`, `7`  
Evidencia: usa `installApiMocks(...)`.

## 6) TODOs de placeholder tecnico (nao sao mock de dados, mas indicam lacuna)

1. Arquivo: `frontend/src/app/(platform)/error.tsx`  
Linha-chave: `12`  
Evidencia: `TODO: integrar com tracking central de erro.`

2. Arquivo: `frontend/src/shared/components/feedback/GlobalErrorBoundary.tsx`  
Linha-chave: `35`  
Evidencia: `TODO: fallback global de erro.`

## 7) Segunda varredura (recheck)

1. A segunda varredura confirmou os mesmos pontos centrais da primeira.
2. O principal ponto "menos obvio" confirmado foi o baseline hardcoded em `PlayerProfileContent.tsx` (`liga: ...`) para o radar.
3. Nao foram identificados novos arquivos de mock fora desta lista.
