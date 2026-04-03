# FRONTEND_RELEASE_READINESS

Data de atualização: 2026-03-22  
Escopo: readiness mínima de entrega/demo fora do código de produto.

## 1. Gate operacional

Comando único a partir da raiz do repositório:

```powershell
python tools/frontend_release_gate.py
```

O gate mínimo executa, nesta ordem:
1. `pnpm validate:release`
2. `pnpm build`

Atalho `make`:

```powershell
make frontend-release
```

Resumo da execução:
- gravado em `artifacts/frontend_release_gate_<timestamp_utc>/summary.txt`
- status final `PASS` ou `FAIL`
- duração por etapa e total

## 2. Gate completo antes de demo/review final

Quando a intenção for demo, handoff ou revisão final de release:

```powershell
python tools/frontend_release_gate.py --mode full
```

O modo `full` executa:
1. `pnpm validate:release`
2. `pnpm build`
3. `pnpm test:regression`

Atalho `make`:

```powershell
make frontend-release-full
```

## 3. Bloqueante vs nao bloqueante

Bloqueante:
- `frontend_release_gate.py` falhar em qualquer etapa
- `pnpm lint` falhar
- `pnpm typecheck` falhar
- `pnpm test:smoke` falhar
- `pnpm build` falhar

Nao bloqueante neste escopo:
- auditorias visuais fora do modo `full`
- deploy automatizado ainda inexistente
- backend real nao estar disponivel para E2E, porque o smoke/regressao do frontend usa interceptacao local de `/api/*`

## 4. Checklist curto de demo/release

1. Rodar `python tools/frontend_release_gate.py --mode full`.
2. Confirmar `PASS` no resumo e guardar o diretório `artifacts/frontend_release_gate_<timestamp_utc>/`.
3. Se a demo usar backend real, validar `frontend/.env.local` com `NEXT_PUBLIC_BFF_BASE_URL` correto.
4. Subir o app localmente com `pnpm dev` em `frontend/` e abrir a home executiva.
5. Validar manualmente os caminhos principais de demo:
   - home executiva
   - `competitions -> season hub`
   - busca global
   - `teams`
   - `players`
   - `matches -> match center`
6. Se houver falha, tratar como bloqueante antes de demo/entrega.

## 5. Automação mínima no repositório

Automação criada neste escopo:
- `tools/frontend_release_gate.py`: gate local reproduzível
- `.github/workflows/frontend-release.yml`: reproduz o gate completo em PR, `main` e execução manual

Fora de escopo neste momento:
- pipeline de deploy
- promotion entre ambientes
- checklist operacional do backend/dados
