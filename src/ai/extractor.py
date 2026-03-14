from __future__ import annotations

import json
import re
from typing import Any

from .contract import validate_ai_output
from ..utils.time import iso_utc

SYSTEM_PROMPT = """
You are a prediction-market research analyst.
Return ONLY one JSON object and no extra text.
Required keys:
market_relevance, resolution_relevance, source_quality, novelty, direction, confidence,
event_type, directly_affects_resolution, summary, why, entities, time_sensitivity.
""".strip()

FEW_SHOT_USER = """
Title: SEC releases new ETF approval statement
Body: The SEC published an official statement confirming spot ETF approval.
Source tier: 3
""".strip()

FEW_SHOT_ASSISTANT = """
{
  "market_relevance": 0.88,
  "resolution_relevance": 0.91,
  "source_quality": 0.95,
  "novelty": 0.70,
  "direction": "positive",
  "confidence": 0.84,
  "event_type": "announcement",
  "directly_affects_resolution": true,
  "summary": "Official approval statement with immediate market impact.",
  "why": "The regulator directly confirmed a key event outcome.",
  "entities": ["SEC", "ETF"],
  "time_sensitivity": "high"
}
""".strip()


def _extract_json_object(raw_text: str) -> str:
    raw_text = raw_text.strip()
    if raw_text.startswith("{") and raw_text.endswith("}"):
        return raw_text
    match = re.search(r"\{[\s\S]*\}", raw_text)
    if match:
        return match.group(0)
    raise ValueError("No JSON object found in model response")


class AIExtractor:
    def __init__(self, config: dict[str, Any]) -> None:
        ai_cfg = config["ai"]
        self.enabled = bool(ai_cfg.get("enabled", True))
        self.model = ai_cfg.get("model", "gpt-4.1-mini")
        self.api_key = ai_cfg.get("api_key", "")
        self.base_url = ai_cfg.get("base_url", "https://api.openai.com/v1")
        self.max_retries = int(ai_cfg.get("max_retries", 2))
        self.deterministic_mode = bool(ai_cfg.get("deterministic_mode", True))
        self._openai_client = self._build_openai_client()

    def _build_openai_client(self) -> Any:
        if not self.enabled or not self.api_key:
            return None
        try:
            from openai import OpenAI  # type: ignore

            return OpenAI(api_key=self.api_key, base_url=self.base_url)
        except Exception:
            return None

    def _openai_extract(self, document: dict[str, Any]) -> tuple[dict[str, Any], str]:
        assert self._openai_client is not None
        user_prompt = (
            f"Title: {document['title']}\n"
            f"Body: {document['body']}\n"
            f"Source tier: {document['source_tier']}\n"
            "Return strict JSON."
        )

        last_error: Exception | None = None
        for _ in range(self.max_retries + 1):
            try:
                raw_text = self._request_openai_text(user_prompt=user_prompt)
                payload = json.loads(_extract_json_object(raw_text))
                validate_ai_output(payload)
                return payload, raw_text
            except Exception as exc:
                last_error = exc
                continue
        raise RuntimeError(f"AI extraction failed after retries: {last_error}")

    def _request_openai_text(self, *, user_prompt: str) -> str:
        assert self._openai_client is not None

        # Try Responses API first.
        try:
            request_payload: dict[str, Any] = {
                "model": self.model,
                "input": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": FEW_SHOT_USER},
                    {"role": "assistant", "content": FEW_SHOT_ASSISTANT},
                    {"role": "user", "content": user_prompt},
                ],
            }
            if self.deterministic_mode:
                request_payload["temperature"] = 0
            response = self._openai_client.responses.create(**request_payload)
            return getattr(response, "output_text", "") or json.dumps(
                response.model_dump(), ensure_ascii=False
            )
        except Exception:
            pass

        # Fallback for OpenAI-compatible proxy providers.
        chat_payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": FEW_SHOT_USER},
                {"role": "assistant", "content": FEW_SHOT_ASSISTANT},
                {"role": "user", "content": user_prompt},
            ],
        }
        if self.deterministic_mode:
            chat_payload["temperature"] = 0
        completion = self._openai_client.chat.completions.create(**chat_payload)
        content = completion.choices[0].message.content
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            # Some providers may return rich content blocks.
            text_parts = [item.get("text", "") for item in content if isinstance(item, dict)]
            return "\n".join(part for part in text_parts if part).strip()
        return json.dumps(completion.model_dump(), ensure_ascii=False)

    @staticmethod
    def _rule_based_extract(document: dict[str, Any]) -> tuple[dict[str, Any], str]:
        text = f"{document['title']} {document['body']}".lower()
        source_tier = int(document.get("source_tier", 1))
        source_quality = min(1.0, max(0.0, source_tier / 3.0))

        high_signal_keywords = [
            "official",
            "confirmed",
            "approval",
            "sec",
            "court",
            "federal",
            "announced",
            "wins",
            "declares",
            "result",
        ]
        rumor_keywords = ["rumor", "unconfirmed", "sources said", "alleged"]
        negative_keywords = ["ban", "reject", "denied", "lawsuit", "sanction", "delay"]

        signal_hits = sum(1 for keyword in high_signal_keywords if keyword in text)
        rumor_hits = sum(1 for keyword in rumor_keywords if keyword in text)
        negative_hits = sum(1 for keyword in negative_keywords if keyword in text)

        market_relevance = min(1.0, 0.35 + signal_hits * 0.08)
        resolution_relevance = min(1.0, 0.30 + signal_hits * 0.10 - rumor_hits * 0.05)
        novelty = min(1.0, 0.45 + 0.05 * max(0, signal_hits - 1))
        confidence = min(1.0, 0.40 + source_quality * 0.40 + signal_hits * 0.04)

        if rumor_hits > 0 and signal_hits == 0:
            direction = "unknown"
            event_type = "rumor"
            directly = False
        else:
            if negative_hits > 0:
                direction = "negative"
            elif signal_hits > 0:
                direction = "positive"
            else:
                direction = "neutral"
            event_type = "announcement" if signal_hits > 0 else "other"
            directly = signal_hits >= 2

        entities = []
        for token in re.findall(r"\b[A-Z][A-Za-z0-9]{2,}\b", document["title"]):
            if token not in entities:
                entities.append(token)

        summary = document["title"].strip()
        if not summary:
            summary = "No title summary available."

        payload = {
            "market_relevance": float(max(0.0, market_relevance)),
            "resolution_relevance": float(max(0.0, resolution_relevance)),
            "source_quality": float(source_quality),
            "novelty": float(max(0.0, novelty)),
            "direction": direction,
            "confidence": float(max(0.0, confidence)),
            "event_type": event_type,
            "directly_affects_resolution": directly,
            "summary": summary[:300],
            "why": "Rule-based deterministic analysis for stable and reproducible output.",
            "entities": entities[:10],
            "time_sensitivity": "medium" if directly else "low",
        }
        return payload, json.dumps(payload, ensure_ascii=False)

    def analyze(self, document: dict[str, Any]) -> dict[str, Any]:
        if self._openai_client is not None:
            payload, raw_response = self._openai_extract(document)
            model_name = self.model
        else:
            payload, raw_response = self._rule_based_extract(document)
            validate_ai_output(payload)
            model_name = "rule_based_deterministic"

        payload = dict(payload)
        payload["raw_ai_response"] = raw_response
        payload["analysis_model"] = model_name
        payload["analyzed_at"] = iso_utc()
        payload["analysis_json"] = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return payload
