import os
from typing import Optional, AsyncIterator
from llama_index.llms.openai import OpenAI as LlamaIndexOpenAI
from llama_index.core import Settings

# Centralized LLM creation (Groq via OpenAI-compatible endpoint)
# Model: Llama 4 Scout 17B/16E Instruct on Groq (preview in docs)  :contentReference[oaicite:10]{index=10}

GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
AI_MODEL = os.getenv("AI_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
AI_TEMPERATURE = float(os.getenv("AI_TEMPERATURE", "0.7"))
AI_MAX_TOKENS = int(os.getenv("AI_MAX_LENGTH", "1024"))

def get_llm():
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set in environment.")
    llm = LlamaIndexOpenAI(
        api_key=GROQ_API_KEY,
        base_url=GROQ_BASE_URL,       # Groq’s OpenAI-compatible endpoint  :contentReference[oaicite:11]{index=11}
        model=AI_MODEL,
        temperature=AI_TEMPERATURE,
        max_tokens=AI_MAX_TOKENS,
        timeout=60,
    )
    # Optionally set as global default for LlamaIndex
    Settings.llm = llm
    return llm

async def complete(prompt: str) -> str:
    llm = get_llm()
    resp = await llm.acomplete(prompt)
    return str(resp)

async def astream(prompt: str) -> AsyncIterator[str]:
    llm = get_llm()
    async for event in llm.astream_complete(prompt):
        # event.delta is the streamed text chunk
        if hasattr(event, "delta") and event.delta:
            yield event.delta
