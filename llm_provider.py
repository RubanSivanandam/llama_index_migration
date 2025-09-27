import os
from typing import AsyncIterator
from llama_index.llms.groq import Groq
from llama_index.core import Settings

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
AI_MODEL = os.getenv("AI_MODEL", "llama-3.1-8b-instant")  # fallback
AI_TEMPERATURE = float(os.getenv("AI_TEMPERATURE", "0.7"))
AI_MAX_TOKENS = int(os.getenv("AI_MAX_LENGTH", "1024"))

def get_llm():
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set in environment.")
    llm = Groq(
        api_key=GROQ_API_KEY,
        model=AI_MODEL,
        temperature=AI_TEMPERATURE,
        max_tokens=AI_MAX_TOKENS,
    )
    Settings.llm = llm
    return llm

async def complete(prompt: str) -> str:
    llm = get_llm()
    resp = await llm.acomplete(prompt)
    return str(resp)

async def astream(prompt: str) -> AsyncIterator[str]:
    llm = get_llm()
    stream = await llm.astream_complete(prompt)
    async for event in stream:
        if hasattr(event, "delta") and event.delta:
            yield event.delta
