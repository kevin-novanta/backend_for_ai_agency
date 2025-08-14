from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any

from workflows.followup_engine.utils import logger

class LLMUnavailable(Exception):
    pass

class LLMClient:
    """Tiny wrapper that prefers the new OpenAI SDK (>=1.0), falls back to legacy if needed.
    Reads API key from /Users/kevinnovanta/backend_for_ai_agency/Creds/gpt_key.json
    """
    def __init__(self):
        self.api_key = None
        gpt_key_path = Path("/Users/kevinnovanta/backend_for_ai_agency/Creds/gpt_key.json")
        if gpt_key_path.exists():
            try:
                with open(gpt_key_path, "r") as f:
                    data = json.load(f)
                    self.api_key = data.get("OPENAI_API_KEY") or data.get("api_key")
            except Exception as e:
                logger.warn(f"Failed to read {gpt_key_path}: {e}")

        self._new_client = None
        self._legacy_openai = None

        # Try new SDK first
        try:
            from openai import OpenAI  # type: ignore
            if self.api_key:
                self._new_client = OpenAI(api_key=self.api_key)
            else:
                logger.warn(f"⚠️ No OpenAI API key found at {gpt_key_path}")
        except Exception:
            self._new_client = None

        # Fallback: legacy SDK
        if self._new_client is None:
            try:
                import openai  # type: ignore
                if self.api_key:
                    openai.api_key = self.api_key
                self._legacy_openai = openai
            except Exception:
                self._legacy_openai = None

    def available(self) -> bool:
        return bool(self.api_key and (self._new_client or self._legacy_openai))

    def generate_email(self, *, system: str, prompt: str, temperature: float = 0.3, max_tokens: int = 220) -> Dict[str, str]:
        if not self.available():
            raise LLMUnavailable("No LLM provider available or API key not set.")

        text = None
        # New SDK path
        if self._new_client is not None:
            try:
                resp = self._new_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                text = resp.choices[0].message.content.strip()
            except Exception as e:
                logger.warn(f"New SDK call failed: {e}")

        # Legacy fallback
        if text is None and self._legacy_openai is not None:
            try:
                resp = self._legacy_openai.ChatCompletion.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                text = resp["choices"][0]["message"]["content"].strip()
            except Exception as e:
                logger.warn(f"Legacy SDK call failed: {e}")

        if not text:
            raise LLMUnavailable("LLM call failed on both new and legacy SDKs.")

        import json as _json
        subject = ""
        body = ""
        try:
            data = _json.loads(text)
            subject = (data.get("subject") or "").strip()
            body = (data.get("body_one_paragraph") or data.get("body") or "").strip()
        except Exception:
            body = text
        return {"subject": subject, "body_one_paragraph": " ".join(body.split())}


def render_llm_email(template_name: str, lead: Dict[str, Any], *, fallback_subject: str = "", llm_opts: Dict[str, Any] | None = None, context: Dict[str, Any] | None = None) -> Dict[str, str]:
    llm_opts = llm_opts or {}
    context = context or {}

    company = (lead.get("Company Name") or lead.get("Company") or "your team").strip()
    desc = (lead.get("Custom 2") or "").strip()
    first_name = (lead.get("First Name") or lead.get("FirstName") or "there").strip()

    opener_summary = context.get("opener_summary", "Short follow-up on my earlier note.")
    thread_summary = context.get("thread_summary", "No prior replies from the prospect yet.")

    system = (
        "You are an expert B2B SDR writing concise, friendly follow-up emails for workflow automation.\n"
        "Rules:\n- One paragraph, 3–6 sentences.\n- Reference the opener gently; don't repeat it.\n- Personalize with company and the short description if relevant.\n- Avoid links.\n- Keep language concrete and simple.\n- Output strict JSON with keys: subject, body_one_paragraph."
    )

    temperature = float(llm_opts.get("temperature", 0.3))
    max_tokens = int(llm_opts.get("max_tokens", 220))
    style = llm_opts.get("style", "concise, friendly, expert")
    constraints = llm_opts.get("constraints", {"one_paragraph": True, "csv_safe": True, "avoid_links": True})

    prompt = (
        f"Prospect: {first_name} at {company}.\n"
        f"Company description: {desc or 'n/a'}.\n"
        f"Opener summary: {opener_summary}.\n"
        f"Thread summary: {thread_summary}.\n"
        f"Style: {style}. Constraints: {constraints}.\n"
        "Task: Write a follow-up that advances the conversation with a low-friction CTA (e.g., 'Want a 60-sec loom?').\n"
        "Return JSON {\"subject\": \"...\", \"body_one_paragraph\": \"...\"}."
    )

    client = LLMClient()
    if not client.available():
        subj = fallback_subject or f"A quick win for {company}"
        body = (
            f"Hey {first_name}, following up on my earlier note — based on what {company} does"
            f"{(': ' + desc) if desc else ''}, I can share a 60-sec loom showing the exact workflow."
            " If now isn't ideal, happy to circle back later or close the loop."
        )
        return {"subject": subj, "body_one_paragraph": " ".join(body.split())}

    return client.generate_email(system=system, prompt=prompt, temperature=temperature, max_tokens=max_tokens)
