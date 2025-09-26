import logging
from pydantic import BaseModel
from typing import AsyncIterator, Dict, Any, List, Optional
from ai.llm_provider import complete, astream, get_llm

logger = logging.getLogger("ollama_client")

class AIRequest(BaseModel):
    prompt: str
    max_tokens: int = 500
    temperature: float = 0.7
    stream: bool = False

# Backwards-compatible client API
class OllamaClient:
    def __init__(self, base_url: str = "http://localhost:11434", model: Optional[str] = None):
        # base_url/model kept for compatibility; unused with Groq
        self.base_url = base_url.rstrip("/")
        self.model = model

    async def check_model_availability(self) -> bool:
        try:
            llm = get_llm()  # will raise if env not set
            _ = llm  # touch
            return True
        except Exception as e:
            logger.error(f"LLM not available: {e}")
            return False

    async def ensure_model_pulled(self) -> bool:
        # Groq is hosted; nothing to pull. Return availability.
        return await self.check_model_availability()

    # --- chat-like stream (kept for compatibility) ---
    async def stream_chat(self, model: str, messages: list, options: dict = None) -> AsyncIterator[str]:
        # Concatenate messages into a single prompt (simple compatibility mode)
        content = "\n".join([m.get("content", "") for m in messages])
        async for chunk in astream(content):
            yield chunk

    # --- text completion (streaming/non-streaming) ---
    async def generate_completion(self, ai_request: AIRequest) -> AsyncIterator[str]:
        if ai_request.stream:
            async for chunk in astream(ai_request.prompt):
                yield chunk
        else:
            text = await complete(ai_request.prompt)
            yield text

    # --- helpers used by routes (kept names) ---
    async def summarize_text(self) -> str:
        # Your summarize path in ai_routes creates its own context; we just run a tailored prompt
        prompt = (
            "You are a manufacturing efficiency analyst. "
            "Summarize the provided flagged-employee + production context into 5-8 crisp bullet points "
            "with actionable steps and KPIs. Keep it factual and concise."
        )
        return await complete(prompt)

    async def suggest_operations(self, context: str, query: str) -> List[Any]:
        prompt = (
            "Based on the garment manufacturing context and user query, "
            "return a JSON array of operations with fields: id, label, confidence (0..1). "
            f"\nContext:\n{context}\nQuery:\n{query}\nJSON:"
        )
        text = await complete(prompt)
        # Soft-parse JSON; if invalid, return a minimal fallback
        import json, re
        m = re.search(r"\[.*\]", text, re.DOTALL)
        if not m:
            return [{"id": "general", "label": "General Operation", "confidence": 0.6}]
        try:
            arr = json.loads(m.group(0))
            return arr if isinstance(arr, list) else [{"id": "general", "label": "General Operation", "confidence": 0.6}]
        except Exception:
            return [{"id": "general", "label": "General Operation", "confidence": 0.6}]

# Single shared instance (kept name for imports)
ollama_client = OllamaClient()
