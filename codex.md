# codex.md - Regras Operacionais Frontend (Blueprint)

## Stack obrigatoria
- Next.js 15 com App Router.
- TypeScript em modo strict.
- Estrutura com `src/`.
- Package manager: `pnpm`.

## Convencoes de pasta e naming
- Rotas e layouts em `src/app/`.
- Route groups: `src/app/(marketing)` e `src/app/(platform)`.
- Dominios em `src/features/`.
- Itens transversais em `src/shared/`.
- Singletons/config externas em `src/lib/`.
- Registries/constantes em `src/config/`.
- Componentes: PascalCase.
- Hooks: `useXxx`.
- Stores: `xxxStore`.

## Regras de boundaries
- `features/*` nao importam diretamente de outras features.
- `shared/*` nao importa de `features/*`.
- `services` nao importam de componentes.
- `stores` nao importam de componentes.

## Definition of Done por etapa
- `pnpm run lint` passando.
- `pnpm run build` passando.
- `pnpm run typecheck` (quando existir) passando.

## Guideline de estados obrigatorios de UI
- Toda tela/modulo deve prever estados: `loading`, `empty`, `error`, `partial`.
- `partial` deve explicitar cobertura parcial de dados.
- Fallback textual para campos nao informados.

## Guideline de query keys
- Toda feature deve ter seu `queryKeys.ts` local.
- Padrao obrigatorio: `['domain', 'action', ...params]`.
- Reutilizar helper compartilhado em `src/shared/utils/queryKeys.ts`.

## Testes E2E (Playwright)
- Rodar no diretório `frontend/`.
- Antes da primeira execução (ou após upgrade de versão), instalar browsers:
  - `pnpm exec playwright install chromium`
- Executar suíte E2E:
  - `pnpm run test:e2e`
