"""
PATCH — sportmonks.py
Duas correções cirúrgicas. Aplicar com str_replace ou diff.

============================================================
BUG 1 — _season_row_match_score, linha ~208
Comparação incorreta de namespace: season (ano) vs season_id (ID interno).
============================================================

ANTES:
    @classmethod
    def _season_row_match_score(
        cls,
        season_row: dict[str, Any],
        *,
        season: int | None = None,
        season_label: str | None = None,
        provider_season_id: int | None = None,
    ) -> int:
        identity = cls._season_identity(season_row)
        if provider_season_id is not None and identity["season_id"] == provider_season_id:
            return 400
        if season is not None and identity["season_id"] == season:
            return 300                                          # <-- REMOVER ESTE BLOCO
        if season_label and identity["season_label"] == season_label:
            return 250
        if season is not None and identity["season_year"] == season:
            return 200
        if season is not None and identity["season_label"] == str(season):
            return 180
        return 0

DEPOIS:
    @classmethod
    def _season_row_match_score(
        cls,
        season_row: dict[str, Any],
        *,
        season: int | None = None,
        season_label: str | None = None,
        provider_season_id: int | None = None,
    ) -> int:
        identity = cls._season_identity(season_row)
        if provider_season_id is not None and identity["season_id"] == provider_season_id:
            return 400
        if season_label and identity["season_label"] == season_label:
            return 250
        if season is not None and identity["season_year"] == season:
            return 200
        if season is not None and identity["season_label"] == str(season):
            return 180
        return 0

JUSTIFICATIVA:
  `season` é sempre um ano (ex.: 2024).
  `identity["season_id"]` é o ID interno do SportMonks (ex.: 21671).
  São namespaces completamente diferentes. A verificação score=300
  nunca produziria um match legítimo e criaria um falso positivo
  catastrófico se um season_id numérico coincidisse com um ano.
  O score=400 (provider_season_id explícito) já cobre o caso em
  que o caller quer fixar um ID de provider específico.


============================================================
BUG 2 — get_team_sidelined, linha ~1024
Heurística fraca de filtro de temporada com startswith.
============================================================

ANTES:
    def get_team_sidelined(
        self,
        *,
        team_id: int,
        season: int | None = None,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/teams/{team_id}"
        payload, headers = self._request(endpoint=endpoint, params={"include": "sidelined"})
        team_data = payload.get("data") or {}
        sidelined_rows = team_data.get("sidelined") or []
        response_rows = []
        for sidelined in sidelined_rows:
            start_date = str(sidelined.get("start_date") or "")
            season_id_raw = sidelined.get("season_id")
            if season is not None:
                if str(season_id_raw) != str(season) and not start_date.startswith(str(season)):
                    continue

DEPOIS:
    def get_team_sidelined(
        self,
        *,
        team_id: int,
        season: int | None = None,
        provider_season_id: int | None = None,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        endpoint = f"/teams/{team_id}"
        payload, headers = self._request(endpoint=endpoint, params={"include": "sidelined"})
        team_data = payload.get("data") or {}
        sidelined_rows = team_data.get("sidelined") or []
        response_rows = []
        for sidelined in sidelined_rows:
            start_date = str(sidelined.get("start_date") or "")
            season_id_raw = self._as_int(sidelined.get("season_id"))
            if provider_season_id is not None:
                # Filtro preciso: usa o ID interno do provider quando disponível.
                if season_id_raw != provider_season_id:
                    continue
            elif season is not None:
                # Filtro por data: cobre temporadas de calendário (season)
                # e temporadas cross-year que terminam em season+1.
                # Exemplo: season=2021 cobre starts em "2021-xx" e "2020-xx"
                # (início da temporada europeia 2020/21 que é o "season=2021").
                season_years = {str(season), str(season - 1)}
                if not any(start_date.startswith(y) for y in season_years):
                    continue

JUSTIFICATIVA:
  A heurística anterior `start_date.startswith(str(season))` excluía
  registros de temporadas europeias cross-year (ex.: 2020/21 começa
  em agosto de 2020; ao buscar season=2021 o filtro eliminava esses
  registros). Isso é o mesmo tipo de bug documentado no prompt como
  "2021 → resolvido como 2020". Além disso, `str(season_id_raw) != str(season)`
  é sempre True porque season_id e season são namespaces diferentes,
  tornando o primeiro guard um no-op.
  A correção prioriza provider_season_id quando disponível (o caller
  de ingestion já tem esse dado via runtime) e melhora o fallback por
  data para cobrir ambos os anos possíveis de início.

NOTA DE CHAMADOR:
  ingestion_service.py — onde chama get_team_sidelined — deve passar
  provider_season_id=runtime.get("provider_season_id") para aproveitar
  o filtro preciso. O parâmetro é opcional; a mudança é backward-compatible.
"""
