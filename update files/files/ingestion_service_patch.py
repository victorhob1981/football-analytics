"""
PATCH — ingestion_service.py
Correção do contador de requests em ingest_fixtures_raw.

============================================================
PROBLEMA — requests_used conta janelas, não requisições reais
============================================================

O loop atual:

    for date_from, date_to in windows:
        payload, headers = provider.get_fixtures(...)
        requests_used += 1   # ERRADO: get_fixtures pagina internamente

provider.get_fixtures chama _paginate_fixtures_between que pode fazer
N requisições ao SportMonks dependendo do volume de fixtures no período.
O log reporta requests=3 (número de janelas) quando o custo real foi ≥ 15.
Isso compromete o monitoramento do orçamento diário de 40k requisições.

============================================================
SOLUÇÃO: retornar page_count de _paginate_fixtures_between
============================================================

Em sportmonks.py — modificar _paginate_fixtures_between para retornar
o número de páginas consumidas:

ANTES:
    def _paginate_fixtures_between(
        self,
        *,
        date_from: str,
        date_to: str,
    ) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, Any]]:
        page = 1
        rows: list[dict[str, Any]] = []
        last_headers: dict[str, str] = {}
        last_meta: dict[str, Any] = {}

        while True:
            payload, headers = self._request(...)
            ...
            if not pagination.get("has_more"):
                break
            page += 1

        return rows, last_headers, last_meta

DEPOIS:
    def _paginate_fixtures_between(
        self,
        *,
        date_from: str,
        date_to: str,
    ) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, Any], int]:
        page = 1
        rows: list[dict[str, Any]] = []
        last_headers: dict[str, str] = {}
        last_meta: dict[str, Any] = {}

        while True:
            payload, headers = self._request(...)
            ...
            if not pagination.get("has_more"):
                break
            page += 1

        return rows, last_headers, last_meta, page  # retorna page count

Em sportmonks.py — atualizar get_fixtures para receber o page_count:

ANTES:
    def get_fixtures(self, ...) -> tuple[dict[str, Any], dict[str, str]]:
        rows, headers, provider_meta = self._paginate_fixtures_between(...)

DEPOIS:
    def get_fixtures(self, ...) -> tuple[dict[str, Any], dict[str, str], int]:
        rows, headers, provider_meta, pages_used = self._paginate_fixtures_between(...)
        ...
        return payload, headers, pages_used

Em ingestion_service.py — usar o page_count:

ANTES:
    for date_from, date_to in windows:
        ...
        payload, headers = provider.get_fixtures(**provider_kwargs)
        requests_used += 1

DEPOIS:
    for date_from, date_to in windows:
        ...
        payload, headers, pages_used = provider.get_fixtures(**provider_kwargs)
        requests_used += pages_used   # conta requisições reais de paginação

NOTA: A mudança de assinatura de get_fixtures de 2-tuple para 3-tuple é
breaking change para qualquer outro chamador. Verificar se há outros
pontos de uso de provider.get_fixtures no codebase antes de aplicar.
Se houver, considerar retornar um NamedTuple ou dataclass para não
quebrar chamadores existentes.
"""
