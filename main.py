#!/usr/bin/env python3
"""
Fabric Pulse AI - Unified Backend (Fixed)
✅ All /api/rtms/* and /api/ai/* endpoints available
✅ Aliases added for /api/ai/rtms/* so frontend won't 404
✅ CORS wide open for dev
"""

import logging
from ai_routes import router as ai_router
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Import config + routers
from config import config
from fabric_pulse_ai_main import (
    cache_status,
    generate_hourly_report,
    generate_pdf_report_api,
    predict_efficiency,
    refresh_ai_cache,
    rtms_engine,
    ai_summarize,
    ai_suggest_operations,
    ai_completion,
    get_service_status,
    get_unit_codes,
    get_floor_names,
    get_line_names,
    get_operations,
    analyze_production_data,
    get_operator_efficiencies,
    health_check,
    test_whatsapp_alerts,
    ultra_advanced_ai_chatbot,
)

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.service.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="Fabric Pulse AI - Unified Monitoring System",
    description="Real-time production monitoring with AI insights and WhatsApp alerts",
    version="5.0.1",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# Include AI routes
app.include_router(ai_router, prefix="/api/ai")

# CORS configuration (wide open for dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔗 Attach RTMS endpoints (original paths)
app.post("/api/ai/summarize")(ai_summarize)
app.post("/api/ai/suggest_ops")(ai_suggest_operations)
app.post("/api/ai/completion")(ai_completion)
app.post("/api/ai/predict_efficiency")(predict_efficiency)
app.post("/api/ai/refresh_cache")(refresh_ai_cache)
app.post("/api/rtms/test_whatsapp_alerts")(test_whatsapp_alerts)
app.get("/api/ai/refresh_status")(cache_status)
app.get("/api/status")(get_service_status)
app.get("/api/rtms/filters/units")(get_unit_codes)
app.get("/api/rtms/filters/floors")(get_floor_names)
app.get("/api/rtms/filters/lines")(get_line_names)
app.get("/api/rtms/filters/operations")(get_operations)
app.get("/api/rtms/analyze")(analyze_production_data)
app.get("/api/rtms/efficiency")(get_operator_efficiencies)
app.get("/api/ai/generate_hourly_report")(generate_hourly_report)
app.get("/api/reports/hourly_pdf")(generate_pdf_report_api)
app.get("/health")(health_check)

# 🔗 Aliases so frontend calls like /api/ai/rtms/... work too
app.get("/api/ai/rtms/overview")(get_service_status)
app.get("/api/ai/rtms/operators")(get_operator_efficiencies)
app.get("/api/ai/rtms/lines")(get_line_names)
app.post("/api/ai/predict_efficiency")(predict_efficiency)
app.post("/api/ai/ultra_chatbot")(ultra_advanced_ai_chatbot)  # New chatbot endpoint

if __name__ == "__main__":
    logger.info("🚀 Starting Unified Fabric Pulse AI Backend (with aliases)...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )
