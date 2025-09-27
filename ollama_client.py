import logging
from pydantic import BaseModel
from typing import AsyncIterator, Any, List, Optional
from llm_provider import complete, astream, get_llm

logger = logging.getLogger("ollama_client")

class AIRequest(BaseModel):
    prompt: str
    max_tokens: int = 500
    temperature: float = 0.7
    stream: bool = False

# Backwards-compatible client API, now backed by Groq via LlamaIndex
class OllamaClient:
    def __init__(self, base_url: str = "http://localhost:11434", model: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.model = model

    async def check_model_availability(self) -> bool:
        try:
            _ = get_llm()
            return True
        except Exception as e:
            logger.error(f"LLM not available: {e}")
            return False

    async def ensure_model_pulled(self) -> bool:
        return await self.check_model_availability()

    async def stream_chat(self, model: str, messages: list, options: dict = None) -> AsyncIterator[str]:
        content = "\n".join([m.get("content", "") for m in messages])
        async for chunk in astream(content):
            yield chunk

    async def generate_completion(self, ai_request: AIRequest) -> AsyncIterator[str]:
        if ai_request.stream:
            async for chunk in astream(ai_request.prompt):
                yield chunk
        else:
            text = await complete(ai_request.prompt)
            yield text

    async def summarize_text(self) -> str:
        prompt = (
            "You are a manufacturing efficiency analyst. "
            "Summarize the flagged-employee + production context into 5-8 crisp bullet points "
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
        import json, re
        m = re.search(r"\[.*\]", text, re.DOTALL)
        if not m:
            return [{"id": "general", "label": "General Operation", "confidence": 0.6}]
        try:
            arr = json.loads(m.group(0))
            return arr if isinstance(arr, list) else [{"id": "general", "label": "General Operation", "confidence": 0.8}]
        except Exception:
            return [{"id": "general", "label": "General Operation", "confidence": 0.6}]

# Single shared instance (kept name for imports)
ollama_client = OllamaClient()
