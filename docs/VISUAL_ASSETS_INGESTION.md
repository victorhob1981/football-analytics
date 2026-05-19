# Ingestao Local de Assets Visuais

Escopo implementado:

- `competitions`: logos de competicoes ja presentes no banco
- `clubs`: escudos/logos de clubes ja presentes no banco
- `players`: fotos de jogadores ja presentes no banco

Regras da rotina:

- usa apenas entidades que ja existem no Postgres local;
- nao faz scraping;
- reaproveita URLs de asset ja materializadas no banco antes de chamar a API;
- usa batch/filter na SportMonks para reduzir requests quando precisa buscar URLs faltantes;
- nao baixa arquivo que ja exista no cache local;
- gera manifesto por categoria e resumo consolidado.

Estratégia por categoria:

1. `competitions`
- origem primaria: `raw.competition_leagues.payload->>'image_path'`
- fallback: `GET /leagues/{id}` na SportMonks

2. `clubs`
- origem primaria: `raw.standings_snapshots.payload->'participant'->>'image_path'`
- fallback em lote: `GET /teams?filters=teamIds:...`

3. `players`
- origem primaria no banco atual: inexistente
- fallback em lote: `GET /players?filters=playerIds:...`

Cache local:

- `data/visual_assets/competitions/`
- `data/visual_assets/clubs/`
- `data/visual_assets/players/`
- `data/visual_assets/manifests/`

Arquivos gerados:

- `data/visual_assets/manifests/competitions.json`
- `data/visual_assets/manifests/clubs.json`
- `data/visual_assets/manifests/players.json`
- `data/visual_assets/manifests/summary.json`

Execucao PowerShell:

```powershell
python tools/sync_visual_assets.py --dry-run
python tools/sync_visual_assets.py
```

Comandos uteis:

```powershell
python tools/sync_visual_assets.py --categories competitions clubs
python tools/sync_visual_assets.py --api-batch-size 50 --download-workers 12
```

Observacoes:

- o cache fica em `data/`, que ja e ignorado pelo Git neste repositorio;
- reruns sao incrementais: arquivos existentes sao preservados e pulados;
- o resumo consolidado mede requests de API e requests de download do run executado.
