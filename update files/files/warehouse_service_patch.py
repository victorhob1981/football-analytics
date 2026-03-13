"""
PATCH — warehouse_service.py
Correção da condição redundante em _enrich_with_season_identity.

============================================================
BUG 3 — _enrich_with_season_identity, linha ~919
Condição dupla é no-op: `"season_label" in df` sempre retorna
True quando `"season_label" in df.columns` é True em pandas.
============================================================

ANTES:
    df = _merge_competition_mapping(df, _competition_mapping_df(conn))
    if "season_label" in df.columns and "season_label" in df:
        fill_mask = df["season_label"].isna()
        if fill_mask.any():
            df.loc[fill_mask, "season_label"] = df.loc[fill_mask].apply(
                lambda row: derive_season_label(
                    season=row.get("season_id"),
                    season_name=row.get("season_name"),
                    start_date=row.get("starting_at"),
                    end_date=row.get("ending_at"),
                ),
                axis=1,
            )
    return _apply_common_provenance(df, run_id=run_id)

DEPOIS:
    df = _merge_competition_mapping(df, _competition_mapping_df(conn))
    if "season_label" in df.columns:
        fill_mask = df["season_label"].isna()
        if fill_mask.any():
            df.loc[fill_mask, "season_label"] = df.loc[fill_mask].apply(
                lambda row: derive_season_label(
                    season=row.get("season_id"),
                    season_name=row.get("season_name"),
                    start_date=row.get("starting_at"),
                    end_date=row.get("ending_at"),
                ),
                axis=1,
            )
    return _apply_common_provenance(df, run_id=run_id)

JUSTIFICATIVA:
  Em pandas, `"coluna" in df` e `"coluna" in df.columns` são
  equivalentes — ambos verificam a existência da coluna. A condição
  dupla cria falsa sensação de proteção adicional sem efeito real.
  Mais importante: a lógica já está corretamente protegida pelo
  `fill_mask.any()` interno, então a segunda condição externa era
  desnecessária em qualquer caso.
"""
