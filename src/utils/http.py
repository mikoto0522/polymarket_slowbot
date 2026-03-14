from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_HEADERS = {
    "User-Agent": "curl/8.0",
    "Accept": "application/json",
}


def get_json(url: str, params: dict[str, Any] | None = None, timeout: int = 30) -> Any:
    query = f"?{urlencode(params)}" if params else ""
    req = Request(url + query, headers=DEFAULT_HEADERS)
    with urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))
