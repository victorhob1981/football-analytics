# Champion Visual Assets

## Escopo

`tools/sync_champion_assets.py` resolve o campeao de cada edicao a partir do Postgres local e tenta baixar uma imagem aberta de celebracao/trofeu para `data/visual_assets/champions/`.

O pipeline atual:

- resolve campeoes por `mart.fact_standings_snapshots` para ligas;
- resolve campeoes por `mart.fact_tie_results` para mata-matas/hibridos;
- usa apenas fontes abertas sem scraping arbitrario:
  - Wikimedia Commons
  - Openverse
- baixa automaticamente so candidatos acima do `min_confidence_score` e com sinal de time + competicao + contexto de celebracao;
- grava manifesto em `data/visual_assets/manifests/champions.json`.

## Contrato de nomeacao

Cada imagem campea segue o formato:

```text
<competition_key>__<season_key>__champion__<team_slug>__trophy-lift__v01.<ext>
```

Exemplos:

```text
brasileirao_a__2025__champion__flamengo__trophy-lift__v01.jpg
champions_league__2024-2025__champion__real-madrid__trophy-lift__v01.jpg
```

## Uso

PowerShell:

```powershell
python tools/sync_champion_assets.py --dry-run
python tools/sync_champion_assets.py --search-only
python tools/sync_champion_assets.py
python tools/sync_champion_assets.py --competition-keys libertadores champions_league
```

## Limites conhecidos

- nem toda edicao tera imagem aberta suficiente para auto-download;
- candidatos abaixo do score minimo ficam no manifesto como `no_candidate` ou `candidate_only`;
- o contrato fica travado por codigo e testes, nao por nome livre manual.
