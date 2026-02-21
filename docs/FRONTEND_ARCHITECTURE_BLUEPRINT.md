# Football Analytics Platform — Frontend Architecture Blueprint

> Versão: 1.0 | Data: 2026-02-21  
> Propósito: Blueprint completo de arquitetura frontend, pronto para implementação.  
> Escopo: decisões de stack, estrutura, padrões arquiteturais, camadas, e evolução do produto.

---

## Índice

1. Stack Recomendada e Justificativas
2. Estrutura de Pastas Profissional
3. Padrões Arquiteturais
4. Camadas da Aplicação
5. Organização de Rankings e Comparativos
6. Estratégias de Filtros Globais e Time Intelligence
7. Reutilização de Tabelas e Gráficos
8. Paginação, Ordenação, Loading e Error States
9. Camada de Insights
10. Estratégia de Cache e Data Freshness
11. Como Evitar Caos Estrutural no Crescimento
12. Convenções e Contratos de Qualidade

---

## 1. Stack Recomendada e Justificativas

### 1.1 Framework principal: **Next.js 15 (App Router)**

Justificativa: este produto não é uma SPA isolada. É uma plataforma analítica com múltiplas páginas de domínio, perfis públicos, SEO potencial para relatórios exportados, e necessidade de streaming de dados pesados. O App Router permite RSC (React Server Components), streaming via Suspense, layouts aninhados nativos, e cache granular no nível do fetch — tudo isso sem custo de configuração de infraestrutura adicional. Vite + React puro exigiria implementar manualmente o que o Next.js já entrega.

### 1.2 Linguagem: **TypeScript (strict mode)**

Justificativa: a plataforma tem mais de 80 métricas mapeadas, múltiplos domínios (clube, jogador, técnico, partida, competição), e contratos de API tipados. Sem TypeScript em modo strict, o crescimento vira caos de runtime. Tipagem forte é parte da arquitetura, não opcional.

### 1.3 Gerenciamento de estado global: **Zustand**

Justificativa: os filtros globais (liga, temporada, rodada, lastN, intervalo) precisam de estado persistente, reativo, e compartilhado entre múltiplas features. Zustand é minimal, sem boilerplate de Redux, testável, e compatível com SSR. Context API não escala para múltiplos slices com derivações. Redux é overhead desnecessário para este tipo de estado.

### 1.4 Cache e data fetching: **TanStack Query v5 (React Query)**

Justificativa: a plataforma tem TTLs diferentes por endpoint (5 min para insights, mais longo para dimensões), necessidade de invalidação seletiva, revalidação em background, e múltiplas chamadas paralelas por página. TanStack Query resolve isso com `staleTime`, `gcTime`, `queryKey` compostos, e `invalidateQueries` cirúrgico. Não há alternativa madura equivalente.

### 1.5 Estilização: **Tailwind CSS + CSS Modules onde necessário**

Justificativa: Tailwind para componentes genéricos e utilitários. CSS Modules para componentes de domínio com variantes complexas (ex: sparklines, heatmaps, badges de insight). Evita colisões de classe em escala.

### 1.6 Gráficos: **Recharts (primário) + D3 (casos avançados)**

Justificativa: Recharts cobre 90% dos casos (linhas, barras, radar, área). D3 é reservado para visualizações personalizadas sem abstração viável, como timeline de partida (match events) e heatmaps de posicionamento. Misturar as duas é aceitável quando os domínios de uso estão separados.

### 1.7 Tabelas: **TanStack Table v8**

Justificativa: rankings, comparativos e listagens são o núcleo do produto. TanStack Table é headless (UI sob controle total), suporta virtualização, ordenação, agrupamento e paginação nativa. É a única opção madura para tabelas analíticas complexas em React.

### 1.8 Formulários e filtros: **React Hook Form + Zod**

Justificativa: os painéis de filtro são formulários controlados com validação de tipos (ex: rodada não pode ser maior que o total da temporada). Zod garante que o schema de filtros é o mesmo validado no cliente e inferido no TypeScript.

### 1.9 Testes: **Vitest + Testing Library + Playwright**

Justificativa: Vitest para unit/integration (hooks, transformers, stores). Testing Library para componentes de UI. Playwright para E2E nos fluxos críticos (filtro global → renderização de ranking, comparativo). Cypress seria mais pesado sem ganho real para este perfil.

---

## 2. Estrutura de Pastas Profissional

A estrutura segue **Feature-based architecture com Domain isolation**. Cada domínio é autossuficiente. Shared não conhece os domínios. Os domínios não se importam entre si diretamente — comunicam via contratos de tipos e serviços compartilhados.

```
src/
│
├── app/                          ← Next.js App Router: rotas, layouts, loading, error
│   ├── (marketing)/              ← route group para páginas públicas/landing
│   ├── (platform)/               ← route group protegido: todo o produto analítico
│   │   ├── layout.tsx            ← Layout raiz da plataforma (nav, filtro global)
│   │   ├── page.tsx              ← Home executiva
│   │   ├── competition/
│   │   │   └── [competitionId]/
│   │   ├── matches/
│   │   │   ├── page.tsx          ← lista de partidas
│   │   │   └── [matchId]/        ← match center
│   │   ├── clubs/
│   │   │   ├── page.tsx
│   │   │   └── [clubId]/
│   │   ├── players/
│   │   │   ├── page.tsx
│   │   │   └── [playerId]/
│   │   ├── coaches/
│   │   │   └── [coachId]/
│   │   ├── rankings/
│   │   │   └── [rankingType]/
│   │   ├── head-to-head/
│   │   ├── market/
│   │   └── audit/
│   │
│   └── api/                      ← Route handlers Next.js (proxy para BFF se necessário)
│
├── features/                     ← Domínios de produto
│   ├── home/
│   │   ├── components/
│   │   ├── hooks/
│   │   └── types/
│   ├── competition/
│   │   ├── components/
│   │   ├── hooks/
│   │   ├── services/
│   │   └── types/
│   ├── matches/
│   │   ├── components/
│   │   │   ├── MatchCard/
│   │   │   ├── MatchCenter/
│   │   │   ├── MatchTimeline/
│   │   │   ├── MatchLineup/
│   │   │   └── MatchStatsPanel/
│   │   ├── hooks/
│   │   ├── services/
│   │   └── types/
│   ├── clubs/
│   │   ├── components/
│   │   │   ├── ClubHeader/
│   │   │   ├── ClubPerformanceChart/
│   │   │   ├── ClubRoster/
│   │   │   └── ClubStatsGrid/
│   │   ├── hooks/
│   │   ├── services/
│   │   └── types/
│   ├── players/
│   │   ├── components/
│   │   ├── hooks/
│   │   ├── services/
│   │   └── types/
│   ├── coaches/
│   ├── rankings/
│   │   ├── components/
│   │   │   ├── RankingTable/
│   │   │   ├── RankingFilters/
│   │   │   └── RankingMetricSelector/
│   │   ├── hooks/
│   │   ├── registry/             ← ranking registry (ver seção 5)
│   │   ├── services/
│   │   └── types/
│   ├── head-to-head/
│   │   ├── components/
│   │   │   ├── H2HSelector/
│   │   │   ├── H2HSummaryCards/
│   │   │   ├── H2HHistoryTable/
│   │   │   └── H2HDominanceChart/
│   │   ├── hooks/
│   │   ├── services/
│   │   └── types/
│   ├── market/
│   ├── insights/
│   │   ├── components/
│   │   │   ├── InsightCard/
│   │   │   ├── InsightFeed/
│   │   │   └── InsightBadge/
│   │   ├── hooks/
│   │   ├── services/
│   │   └── types/
│   └── audit/
│
├── shared/                       ← Componentes, hooks e utilitários transversais
│   ├── components/
│   │   ├── ui/                   ← Primitivos de UI (Button, Badge, Tooltip, etc.)
│   │   ├── data-display/         ← DataTable, StatCard, MetricBadge, Sparkline
│   │   ├── charts/               ← LineChart, BarChart, RadarChart, Heatmap wrappers
│   │   ├── filters/              ← GlobalFilterBar, FilterChip, DateRangePicker
│   │   ├── comparison/           ← ComparisonLayout, ComparisonColumn, DeltaIndicator
│   │   ├── coverage/             ← CoverageBadge, PartialDataBanner, EmptyState
│   │   ├── feedback/             ← LoadingSkeleton, ErrorBoundary, NoDataState
│   │   └── layout/               ← PageHeader, SectionDivider, StickyHeader
│   ├── hooks/
│   │   ├── useGlobalFilters.ts
│   │   ├── useQueryWithCoverage.ts
│   │   ├── usePaginatedTable.ts
│   │   ├── useSortableTable.ts
│   │   ├── useComparison.ts
│   │   ├── useTimeRange.ts
│   │   └── useInsights.ts
│   ├── services/
│   │   ├── api-client.ts         ← fetch wrapper base com interceptors
│   │   └── query-client.ts       ← configuração global TanStack Query
│   ├── stores/
│   │   ├── globalFilters.store.ts
│   │   ├── comparison.store.ts
│   │   └── ui.store.ts
│   ├── types/
│   │   ├── filters.types.ts
│   │   ├── insight.types.ts
│   │   ├── coverage.types.ts
│   │   ├── pagination.types.ts
│   │   └── api-response.types.ts
│   └── utils/
│       ├── formatters.ts         ← moeda, percentual, número, data
│       ├── normalizers.ts        ← z-score, per-90, transformações
│       ├── coverage.utils.ts
│       └── ranking.utils.ts
│
├── lib/                          ← Instâncias singleton e configurações externas
│   ├── query-client.ts
│   └── analytics.ts
│
└── config/
    ├── routes.ts                 ← constantes de rotas
    ├── metrics.registry.ts       ← registro central de métricas disponíveis
    └── ranking.registry.ts       ← registro central de tipos de ranking
```

---

## 3. Padrões Arquiteturais

### 3.1 Feature-based Architecture

Cada feature encapsula seus próprios componentes, hooks, services e types. Nenhuma feature importa diretamente de outra feature. A comunicação entre features acontece via shared layer (stores, types, hooks transversais) ou via URL/state de rota. Isso garante que a feature de Rankings pode evoluir independentemente da feature de Clubes.

**Regra de ouro:** se um componente ou hook só faz sentido dentro de uma feature, ele mora lá. Se faz sentido em duas ou mais features, ele vai para shared.

### 3.2 Não é Atomic Design puro

Atomic Design seria overhead de nomenclatura para este produto. Usamos uma variante pragmática:

- **Primitivos** (shared/components/ui): Button, Badge, Tooltip, Tabs — sem lógica de domínio.
- **Compostos** (shared/components/data-display, charts, filters): StatCard, DataTable, LineChart — lógica de apresentação, sem acoplamento a domínio.
- **Seções de domínio** (features/X/components): ClubPerformanceChart, RankingTable — lógica de domínio, podem chamar hooks de dados.
- **Páginas** (app/): orquestram seções, gerenciam layouts, definem Suspense boundaries.

### 3.3 Container vs Presentation

Utilizamos a separação de forma implícita via hooks, não via container components explícitos:

- Componentes de apresentação recebem dados via props e não chamam serviços.
- Hooks de feature encapsulam a chamada de dados, transformação e estados derivados.
- Componentes de página usam os hooks e passam dados para os componentes de apresentação.

Essa separação é mais ergonômica no ecossistema React moderno do que o padrão Container/Presentational clássico.

### 3.4 Estado Global vs Local

| Tipo de estado | Onde vive |
|---|---|
| Filtros globais (liga, temporada, rodada, lastN, intervalo) | Zustand store global |
| Entidades selecionadas para comparativo | Zustand store de comparação |
| Estado de UI (sidebars, modais abertos) | Zustand store de UI ou useState local |
| Dados remotos (respostas de API) | TanStack Query cache |
| Estado de formulário (filtros locais, forms) | React Hook Form local |
| Estado de tabela (sort, página, colunas visíveis) | useState/useReducer local no componente ou hook de tabela |

**Princípio:** o menor escopo possível. Estado sobe de nível apenas quando comprovadamente necessário.

### 3.5 React Server Components (RSC)

Layouts estáticos, shells de página, e dados de dimensões estáveis (lista de competições, lista de times) podem ser RSC — sem JavaScript no cliente, melhora de performance. Componentes interativos (filtros, gráficos, tabelas ordenáveis) são Client Components marcados explicitamente com `"use client"`. A fronteira RSC/Client é uma decisão arquitetural por componente, não por página inteira.

---

## 4. Camadas da Aplicação

```
┌──────────────────────────────────────────────────────┐
│                    UI Layer                          │
│  Componentes de apresentação, layouts, páginas       │
│  Responsabilidade: renderização, interação           │
└──────────────────┬───────────────────────────────────┘
                   │ props / hooks
┌──────────────────▼───────────────────────────────────┐
│                  Domain Layer                        │
│  Hooks de feature, transformações, derivações        │
│  Responsabilidade: lógica de negócio frontend        │
│  Ex: calcular forma recente, montar config de ranking│
└──────────────────┬───────────────────────────────────┘
                   │ query functions / mutations
┌──────────────────▼───────────────────────────────────┐
│                  Data Layer                          │
│  TanStack Query: cache, revalidação, estados         │
│  Responsabilidade: sincronização com servidor        │
│  Query keys compostos por domínio + filtros ativos   │
└──────────────────┬───────────────────────────────────┘
                   │ fetch
┌──────────────────▼───────────────────────────────────┐
│                 Services Layer                       │
│  Funções puras de chamada HTTP ao BFF                │
│  Responsabilidade: contrato com API, serialização    │
│  Sem lógica de negócio, sem estado                   │
└──────────────────┬───────────────────────────────────┘
                   │ HTTP
┌──────────────────▼───────────────────────────────────┐
│                  BFF / API Interna                   │
│  (fora do escopo do frontend)                        │
└──────────────────────────────────────────────────────┘
```

### 4.1 Types Layer (transversal a todas as camadas)

Os tipos vivem em dois lugares:

- `shared/types/`: contratos de API genéricos, filtros, paginação, insights, cobertura.
- `features/X/types/`: entidades de domínio específicas (ClubProfile, PlayerSeasonSummary, H2HSummary).

Os types de domínio derivam dos contratos de API (BFF response shapes). São inferidos via Zod schemas sempre que possível, garantindo validação em runtime e tipagem estática simultâneas.

---

## 5. Organização de Rankings e Comparativos

### 5.1 Ranking Registry Pattern

O problema: existem dezenas de tipos de ranking (gols, assistências, rating, por90, disciplina, etc.) e cada um tem configurações diferentes (métrica, label, formato, fonte, cobertura mínima). Sem um padrão central, cada ranking seria um componente novo com lógica duplicada.

**Solução: Ranking Registry**

Existe um arquivo central `config/ranking.registry.ts` que define um array/map de `RankingDefinition`. Cada definição contém:

- `id`: identificador único (ex: `goals-per-90-players`)
- `label`: nome legível (ex: "Gols por 90 min — Jogadores")
- `entity`: `player | club | coach`
- `metricKey`: chave da métrica no payload da API
- `metricLabel`: label da coluna
- `format`: `number | percentage | rating`
- `normalizable`: boolean
- `minSampleFilter`: threshold mínimo de minutos/partidas
- `coverageWarning`: string descrevendo limitação conhecida (se houver)
- `endpoint`: endpoint BFF correspondente
- `defaultSort`: `asc | desc`
- `availableFilters`: quais filtros globais se aplicam

O componente `RankingTable` é genérico e recebe um `RankingDefinition` como prop. Ele não sabe "qual ranking está mostrando" — apenas renderiza conforme o contrato. Isso permite adicionar um novo ranking sem escrever um novo componente.

A página `/rankings/[rankingType]` lê o `rankingType` da URL, busca a definição no registry, e passa para o componente genérico. Adicionar um novo ranking = adicionar uma entrada no registry.

### 5.2 Comparativo — Padrão Genérico vs Específico

O comparativo tem dois modos:

**Modo genérico (ComparisonLayout):** side-by-side de qualquer entidade do mesmo tipo. Existe um componente `ComparisonLayout` em `shared/components/comparison/` que aceita N colunas de entidade e renderiza métricas em linhas. É agnóstico a domínio. Usado para clube vs clube e jogador vs jogador nos comparativos rápidos.

**Modo específico (H2H):** head-to-head entre clubes tem histórico, resultado de jogos, dominância. Não é apenas "colunas lado a lado de métricas". Este modo tem seus próprios componentes em `features/head-to-head/` e usa os dados específicos do endpoint `mart.head_to_head_summary`.

**Comparison Store:** um Zustand store `comparison.store.ts` guarda:
- `entityType`: `player | club`
- `selectedIds`: array de IDs (max 2 para tabela, max 4 para radar)
- `activeMetrics`: quais métricas estão ativas no comparativo

Qualquer página pode "adicionar ao comparativo" enviando uma entidade para o store. Quando o store tem 2 ou mais entidades, um floating panel ou modal de comparativo fica disponível globalmente.

**DeltaIndicator:** componente compartilhado que recebe dois valores e renderiza a diferença absoluta e relativa com formatação visual (positivo/negativo/neutro). Usado em todo comparativo.

---

## 6. Estratégia de Filtros Globais e Time Intelligence

### 6.1 GlobalFilters Store

O store Zustand `globalFilters.store.ts` mantém:

```
{
  competitionId: string
  seasonId: string
  roundId: string | null
  dateRangeStart: string | null
  dateRangeEnd: string | null
  lastN: number | null
  venue: 'home' | 'away' | 'all'
}
```

Regras de precedência: `lastN` e `dateRange` são mutuamente exclusivos. A UI desabilita um quando o outro está ativo. Zustand action `setTimeRange` resolve isso internamente.

### 6.2 GlobalFilterBar

Componente fixo no layout raiz da plataforma (header ou sidebar superior). É um Client Component que lê e escreve no store. É o único lugar onde os filtros globais são alterados globalmente.

Cada página pode ter filtros locais adicionais (ex: posição de jogador no ranking de jogadores). Esses filtros locais não vão para o store global — ficam em estado local da feature.

### 6.3 Query Keys e Reatividade

Todos os query keys do TanStack Query incluem o estado dos filtros globais como parte da chave. Isso garante que qualquer mudança no filtro global invalida automaticamente todos os queries dependentes e dispara revalidação.

Padrão de query key:

```
['entity-type', 'action', { ...globalFilters, ...localFilters }]
```

Exemplo:
```
['players', 'ranking', 'goals-per-90', { competitionId: '1', seasonId: '2024', lastN: 5 }]
```

### 6.4 Time Intelligence — Recortes Dinâmicos

A plataforma suporta quatro recortes temporais:

- **Temporada completa**: sem filtro de tempo adicional.
- **Últimos N jogos (lastN)**: parâmetro inteiro enviado ao BFF.
- **Intervalo de datas**: dateRangeStart + dateRangeEnd.
- **Por rodada**: roundId específico.

O hook `useTimeRange` abstrai qual recorte está ativo e retorna os parâmetros corretos para passar ao service. Componentes de UI não interpretam o recorte — apenas chamam `useTimeRange()` e passam o resultado para o serviço.

---

## 7. Reutilização de Tabelas e Gráficos

### 7.1 DataTable (shared)

Componente wrapper sobre TanStack Table. Aceita:

- `columns`: definição de colunas com tipos (número, percentual, badge, link, sparkline).
- `data`: array tipado.
- `sortable`: boolean.
- `paginated`: boolean.
- `loading`: boolean.
- `coverage`: objeto de cobertura parcial por coluna.
- `emptyState`: configuração customizável por domínio.

A DataTable não sabe nada sobre rankings, clubes ou jogadores. É infraestrutura pura de apresentação de dados tabulares.

### 7.2 Chart Wrappers (shared)

Cada tipo de gráfico tem um wrapper em `shared/components/charts/`:

- `LineChart`: recebe series, xKey, yKey, domínio, e handles de tooltip.
- `BarChart`: horizontal ou vertical, com suporte a grupos.
- `RadarChart`: para comparativo multidimensional de índices analíticos.
- `SparklineChart`: miniatura inline para uso em células de tabela.
- `HeatmapChart`: para distribuições mensais ou por rodada.

Esses wrappers encapsulam a configuração de Recharts e expõem uma API limpa. Componentes de domínio nunca usam Recharts diretamente — usam apenas os wrappers.

---

## 8. Paginação, Ordenação, Loading e Error States

### 8.1 Paginação

Duas estratégias, definidas por contexto:

- **Server-side pagination**: rankings longos, listagens de partidas. O query key inclui `{ page, pageSize }`. A DataTable recebe `totalCount` e `onPageChange` externos.
- **Client-side pagination**: tabelas com volume controlado (ex: lineup de 11 jogadores, H2H de últimos 20 jogos). TanStack Table gerencia internamente.

O hook `usePaginatedTable` encapsula a lógica de qual estratégia usar e expõe interface uniforme para a DataTable.

### 8.2 Ordenação

- Rankings têm sort server-side (o BFF retorna já ordenado).
- Tabelas comparativas têm sort client-side.

O hook `useSortableTable` mantém o estado de sort e decide se dispara nova query (server-side) ou apenas reordena localmente (client-side).

### 8.3 Loading States

Hierarquia de loading, do mais global ao mais granular:

1. **Page-level Suspense**: Next.js `loading.tsx` por rota — esqueleto da página inteira.
2. **Section-level Skeleton**: cada seção (StatsGrid, RankingTable, Chart) tem seu próprio skeleton com dimensões realistas.
3. **Cell-level loading**: células individuais de tabela com cobertura parcial mostram shimmer específico.

**Regra:** nunca usar spinner global para dados que chegam em paralelo. Cada seção tem autonomia de loading.

### 8.4 Partial Data State

Cobertura parcial é tratada como cidadão de primeira classe, não como edge case. O componente `CoverageBadge` indica o nível de confiança de uma métrica. O componente `PartialDataBanner` aparece no topo de seções quando a cobertura está abaixo do threshold.

O tipo `CoverageState` existe em `shared/types/coverage.types.ts` e flui da API até os componentes. A BFF retorna cobertura junto com os dados — o frontend não calcula cobertura, apenas a exibe.

Exemplos de cobertura que precisam de tratamento visual:
- `raw.match_statistics` com cobertura de 68.77% de jogadores com stats.
- Campos de `attendance`, `weather`, `referee` com cobertura parcial — fallback "não informado".
- Lineups com 49 slots sem `player_id` — slot renderizado sem link de perfil.
- `mart.team_performance_monthly` vazio — seção inteira em estado "em breve".

### 8.5 Error Boundaries

Estrutura em camadas:

- **Global ErrorBoundary**: envolve toda a plataforma. Captura erros críticos de runtime.
- **Route-level**: Next.js `error.tsx` por rota.
- **Section-level**: cada seção crítica (match center, ranking, comparativo) tem seu próprio ErrorBoundary que exibe fallback inline sem derrubar a página inteira.

O componente de erro recebe a action de retry e tenta revalidar o query afetado, não a página toda.

---

## 9. Camada de Insights

### 9.1 Tipo Insight

O contrato do insight vem tipado da BFF conforme especificado no documento funcional:

```
InsightObject {
  insight_id: string
  severity: 'info' | 'warning' | 'critical'
  explanation: string
  evidences: Record<string, number>
  reference_period: string
  data_source: string[]
}
```

### 9.2 Componentes de Insight

- `InsightCard`: card autossuficiente com ícone de severidade, explicação, evidências numéricas, e período.
- `InsightFeed`: lista vertical de InsightCards com agrupamento por severidade ou domínio.
- `InsightBadge`: badge inline usado em tabelas, perfis de clube/jogador — indica que há insights para aquela entidade sem mostrar o conteúdo.

### 9.3 Posicionamento de Insights na UI

- **Home executiva**: `InsightFeed` de destaque da rodada/mês.
- **Perfil de clube**: seção dedicada de insights logo abaixo do header do clube.
- **Perfil de jogador**: idem.
- **Rankings**: `InsightBadge` em linhas com outliers positivos/negativos.
- **Match center**: insights específicos da partida (se disponíveis).

### 9.4 Hook useInsights

Hook em `shared/hooks/useInsights.ts` que aceita `{ entityType, entityId, filters }` e retorna a lista de insights para aquele contexto, com loading e error states. Features individuais consomem este hook sem precisar conhecer o endpoint.

---

## 10. Estratégia de Cache e Data Freshness

| Tipo de dado | TTL recomendado | Estratégia |
|---|---|---|
| Dimensões (times, jogadores, competições) | 30 min | staleTime alto, background refetch |
| Standings, tabelas de classificação | 5 min | revalidate on focus |
| Rankings de temporada | 10 min | staleTime 10 min |
| Insights (Home, Rankings) | 5 min | staleTime 5 min |
| Insights de perfil (clube/jogador) | 5 min | staleTime 5 min |
| Dados de partida em andamento | 30 seg | polling ativo se match ao vivo |
| Dados históricos de partidas encerradas | 60 min | staleTime alto, sem refetch automático |
| Auditoria de cobertura | 5 min | invalidação manual pós-ingestão |
| Head-to-head histórico | 30 min | staleTime alto |

**Query key strategy**: cada combinação de [endpoint + globalFilters + localFilters] gera uma entrada de cache distinta. Mudança em qualquer filtro invalida apenas os queries que incluem aquele filtro na chave — não invalida o cache inteiro.

**Prefetching**: na navegação de lista para detalhe (ex: lista de clubes → perfil do clube), o hover sobre um item pode disparar prefetch do perfil. Isso é implementado via `queryClient.prefetchQuery` no evento `onMouseEnter` do link.

---

## 11. Como Evitar Caos Estrutural no Crescimento

### 11.1 Regras de dependência (enforced via ESLint)

- Features não importam de outras features.
- Shared não importa de features.
- Services não importam de componentes.
- Stores não importam de componentes.

Essas regras são enforçadas via plugin ESLint `eslint-plugin-boundaries` ou `eslint-plugin-import`. Violações quebram o CI.

### 11.2 Registro central de métricas

`config/metrics.registry.ts` é a fonte de verdade de quais métricas existem, seus labels, formatos, cobertura conhecida e fonte de dados. Quando uma nova métrica é adicionada ao BFF, ela primeiro entra no registry — então os rankings, tabelas e comparativos a consomem. Isso evita métricas hardcoded em múltiplos lugares.

### 11.3 Covenants de feature

Cada feature tem um `index.ts` (barrel export) que define sua API pública. Outros módulos importam apenas desse barrel — nunca de arquivos internos da feature. Isso cria um contrato explícito do que é público vs privado dentro de cada domínio.

### 11.4 Componentes Genéricos antes de Específicos

Antes de criar `ClubRankingTable`, verificar se `DataTable` genérica + configuração de colunas resolve o problema. A criação de um componente específico só é justificada quando o comportamento é genuinamente único e não parametrizável.

### 11.5 Evolução de novas páginas

Cada nova página do produto (ex: Mercado de transferências) segue o mesmo protocolo:

1. Criar entry em `app/(platform)/market/`.
2. Criar feature em `features/market/` com componentes, hooks, services, types.
3. Registrar métricas e rankings relevantes nos registries centrais.
4. Reutilizar DataTable, charts e InsightFeed do shared.
5. Conectar aos filtros globais via `useGlobalFilters`.

Nenhum código de infraestrutura precisa ser reescrito para uma nova página. Apenas domínio novo.

---

## 12. Convenções e Contratos de Qualidade

### 12.1 Nomenclatura

- Componentes: PascalCase.
- Hooks: camelCase prefixado com `use`.
- Services: camelCase, sufixo `Service` (ex: `clubService`).
- Types: PascalCase, sufixo do contexto (ex: `ClubProfile`, `PlayerSeasonSummary`).
- Stores: camelCase, sufixo `Store` (ex: `globalFiltersStore`).
- Query keys: arrays de strings/objetos, definidos como constantes em `queryKeys.ts` por feature.

### 12.2 Padrão de Query Keys

Cada feature tem um arquivo `queryKeys.ts` que exporta funções geradoras de query key:

```
clubs.all → ['clubs']
clubs.list(filters) → ['clubs', 'list', filters]
clubs.detail(id) → ['clubs', 'detail', id]
clubs.stats(id, filters) → ['clubs', 'stats', id, filters]
```

Isso permite invalidação cirúrgica: `invalidateQueries(clubs.all)` invalida tudo de clubes. `invalidateQueries(clubs.detail(id))` invalida apenas aquele clube.

### 12.3 Padrão de Service

Cada service é uma coleção de funções puras que recebem parâmetros tipados e retornam promises do tipo de resposta da API. Sem estado, sem side effects além do fetch. Services são testáveis com mock de fetch.

### 12.4 Documentação mínima obrigatória

- Cada RankingDefinition no registry: campo `description` e `coverageWarning`.
- Cada componente compartilhado: JSDoc no arquivo com propósito e props principais.
- Cada hook de feature: comentário explicando o que encapsula e quando usar.

### 12.5 Checklist pré-implementação de nova feature

- [ ] Tipos de domínio definidos em `features/X/types/`
- [ ] Métricas registradas em `config/metrics.registry.ts`
- [ ] Service criado com funções tipadas
- [ ] Query keys definidos em `features/X/queryKeys.ts`
- [ ] Hook de dados criado consumindo o service via TanStack Query
- [ ] Componentes de apresentação criados sem chamadas diretas à API
- [ ] Coverage state mapeado e tratado na UI
- [ ] Loading skeleton dimensionado para o layout real
- [ ] Error boundary posicionado corretamente
- [ ] Filtros globais conectados via `useGlobalFilters`

---

## Apêndice: Mapa de Domínios vs Fontes de Dados Mart

| Domínio / Feature | Mart principal | Mart auxiliar | Raw (quando necessário) |
|---|---|---|---|
| Home executiva | standings_evolution, team_monthly_stats | player_match_summary | — |
| Competição | fact_matches, fact_standings_snapshots | league_summary | — |
| Match Center | fact_matches, fact_match_events | fact_fixture_lineups, fact_fixture_player_stats | match_statistics (stats de time) |
| Clubes | team_monthly_stats, fact_matches | player_season_summary | match_statistics |
| Jogadores | player_season_summary, player_90_metrics | player_match_summary | fixture_player_statistics (JSON raw) |
| Rankings | player_90_metrics, player_season_summary, team_monthly_stats | — | — |
| Head-to-Head | head_to_head_summary | fact_matches | head_to_head_fixtures |
| Técnicos | coach_performance_summary | dim_coach | team_coaches |
| Mercado | — | — | player_transfers |
| Auditoria | fact_matches, fact_match_events | fact_fixture_lineups | provider_sync_state |
| Insights | (todos os marts) | — | match_statistics (para alertas disciplinares) |
