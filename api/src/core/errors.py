from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class AppError(Exception):
    message: str
    code: str
    status: int
    details: Any | None = None

    def __str__(self) -> str:
        return self.message


def error_payload(message: str, code: str, status: int, details: Any | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "message": message,
        "code": code,
        "status": status,
    }
    if details is not None:
        payload["details"] = details
    return payload
