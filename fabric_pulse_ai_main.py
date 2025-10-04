"""
Fabric Pulse AI - Production Ready Backend
Complete RTMS integration with Ollama AI, WhatsApp alerts, and dependent filters
"""
import asyncio
import json
import logging
import os
from pathlib import Path
import pandas as pd
import subprocess
import re
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict
import time
import threading
from llm_provider import astream, complete, get_llm
groq_client = get_llm()
# import tempfiles
from ollama_client import OllamaClient, AIRequest
ollama_client = OllamaClient()
from sympy import sqf
from ultra_advanced_chatbot import UltraHighPerformanceChatbot, make_ultra_advanced_pdf_report
from ultra_advanced_chatbot import ultra_high_performance_chatbot, make_ultra_advanced_pdf_report
from fastapi import Body, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional, List
import json
import os
import time
import math
import tempfile
import logging

ultra_high_perf_chatbot = UltraHighPerformanceChatbot()

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, params
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Body
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
import pyodbc
import uvicorn
from fastapi import APIRouter, Query
router = APIRouter()

# Local imports
from config import config
from whatsapp_service import whatsapp_service
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import urllib.parse
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from ultra_advanced_chatbot import ultra_chatbot, make_ultra_advanced_pdf_report
from dotenv import load_dotenv  
load_dotenv()

router = APIRouter()

# ---- Database Config from .env ----
DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ---- Groq API Key ----
groq_api_key = os.getenv("GROQ_API_KEY")

import warnings
TEST_NUMBERS = ["+919943625493", "+918939990949"]

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.service.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# FastAPI app
app = FastAPI(
    title="AI Powered - RTMS System",
    description="Real-time production monitoring with Ollama AI insights and WhatsApp alerts",
    version="4.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080", "http://127.0.0.1:8080"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

AI_CACHE = {
    "production_data": None,
    "efficiency_data": None,
    "chatbot_data": None,
    "chatbot_summaries": None,
    "chatbot_session": None,
    "last_updated": None,
}

# Pydantic models for AI endpoints
class AISummarizeRequest(BaseModel):
    # Keep old text field optional (for free-text summarization)
    context: Optional[str] = None
    query: str
    text: Optional[str] = None  
    length: str = "short"

    # ✅ Add fields for efficiency filters
    unit_code: Optional[str] = None
    floor_name: Optional[str] = None
    line_name: Optional[str] = None
    operation: Optional[str] = None
    limit: int = 1000

class AISuggestOperationsRequest(BaseModel):
    context: Optional[str] = None  # Now optional with default None
    query: str

class AICompletionRequest(BaseModel):
    prompt: str
    maxTokens: Optional[int] = 200
    stream: Optional[bool] = False

# Rate limiting
request_counts = defaultdict(list)
RATE_LIMIT = 30  # requests per minute

def check_rate_limit(ip: str) -> bool:
    now = time.time()
    minute_ago = now - 60
    request_counts[ip] = [req_time for req_time in request_counts[ip] if req_time > minute_ago]
    
    if len(request_counts[ip]) >= RATE_LIMIT:
        return False
    
    request_counts[ip].append(now)
    return True

@dataclass
class RTMSProductionData:
    """Enhanced production data structure"""
    LineName: str
    EmpCode: str
    EmpName: str
    DeviceID: str
    StyleNo: str
    OrderNo: str
    Operation: str
    SAM: float
    Eff100: int
    Eff75: Optional[int]
    ProdnPcs: int
    EffPer: float
    OperSeq: int
    UsedMin: float
    TranDate: str
    UnitCode: str
    PartName: str
    FloorName: str
    ReptType: str
    PartSeq: int
    EffPer100: float
    EffPer75: float
    NewOperSeq: str
    BuyerCode: str
    ISFinPart: str
    ISFinOper: str
    IsRedFlag: int

    def calculate_efficiency(self) -> float:
        """Calculate actual efficiency"""
        return (self.ProdnPcs / self.Eff100 * 100) if self.Eff100 > 0 else 0.0
    
    # --- PDF helper ---
def make_pdf_report(df: pd.DataFrame, path: str, title: str = "Production Report"):
    c = canvas.Canvas(path, pagesize=letter)
    width, height = letter
    c.setFont("Helvetica-Bold", 12)
    c.drawString(30, height - 40, title)
    c.setFont("Helvetica", 9)
    y = height - 60

    col_names = ["LineName", "EmpCode", "EmpName", "PartName", "FloorName", "ProdnPcs", "Eff100", "EffPer"]
    for i, col in enumerate(col_names):
        c.drawString(30 + i * 80, y, col[:12])
    y -= 14

    for _, row in df.iterrows():
        for i, col in enumerate(col_names):
            c.drawString(30 + i * 80, y, str(row.get(col, ""))[:12])
        y -= 12
        if y < 40:
            c.showPage()
            c.setFont("Helvetica", 9)
            y = height - 40
    c.save()
    return path



def should_send_whatsapp(emp_data: dict, line_performers: List[dict]) -> bool:
    """
    Determine if WhatsApp alert should be sent for underperforming employee
    Logic: Top performer in line/operation = 100%, alert if < 85% of top performer
    """
    if not line_performers:
        return False
    
    # Find top performer in same line and operation
    same_operation = [p for p in line_performers 
                     if p.get("line_name") == emp_data.get("line_name") 
                     and p.get("new_oper_seq") == emp_data.get("new_oper_seq")]
    
    if not same_operation:
        return False
    
    top_performer_eff = max(p.get("efficiency", 0) for p in same_operation)
    threshold_eff = top_performer_eff * 0.85  # 85% of top performer
    
    return emp_data.get("efficiency", 0) < threshold_eff

class OllamaAIService:
    """Rewritten AI Service using LlamaIndex (via ollama_client)"""

    def __init__(self, model: str = "meta-llama/llama-4-scout-17b-16e-instruct"):
        self.model = model
        self.available = True  # Groq-hosted, always available if API key set

    async def summarize_text(self, text: str, length: str = "medium") -> str:
        # Old subprocess ollama logic preserved as comment
        # try:
        #     result = subprocess.run(['ollama', 'run', self.model, prompt], ...)
        # except Exception as e: ...
        if not text:
            return "No input text provided."
        text = text[:10000]
        length_prompts = {
            "short": "Provide a brief 1-2 sentence summary",
            "medium": "Provide a concise paragraph summary", 
            "long": "Provide a detailed summary with key points"
        }
        prompt = f"{length_prompts.get(length,'medium')}:\n{text}\nSummary:"
        req = AIRequest(prompt=prompt, max_tokens=300)
        async for out in ollama_client.generate_completion(req):
            return out
        return "No summary generated."

    async def suggest_operations(self, context: str, query: str):
        # Old subprocess JSON parsing kept as comment
        # try: subprocess.run(['ollama', ...])
        return await ollama_client.suggest_operations(context, query)

    async def generate_completion(self, prompt: str, max_tokens: int = 200) -> str:
        if not prompt:
            return "Empty prompt."
        req = AIRequest(prompt=prompt, max_tokens=max_tokens)
        async for out in ollama_client.generate_completion(req):
            return out
        return "No completion generated."
    
    async def suggest_operations(self, context: str, query: str) -> List[Dict[str, Any]]:
        """Suggest operations based on context and query"""
        if not self.available:
            return [{"id": "fallback-1", "label": "General Operation", "confidence": 0.5}]
        
        context = context[:8000]  # Limit context length
        
        prompt = f"""
                Based on the following garment manufacturing context and user query, suggest relevant operations.
                Respond with a JSON array of operations, each with id, label, and confidence (0.0-1.0).

                Context: {context}
                Query: {query}

                Example operations: Cutting, Sewing, Hemming, Buttonhole, Collar attachment, Sleeve attachment, Quality checking, Pressing, Folding, Packaging

                Respond only with valid JSON array:
                """
        
        try:
            result = subprocess.run([
                'ollama', 'run', self.model, prompt
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0 and result.stdout.strip():
                response = result.stdout.strip()
                
                # Extract JSON from response
                json_match = re.search(r'\\[.*\\]', response, re.DOTALL)
                if json_match:
                    suggestions_data = json.loads(json_match.group())
                    
                    suggestions = []
                    for item in suggestions_data:
                        if isinstance(item, dict) and all(key in item for key in ['id', 'label', 'confidence']):
                            suggestions.append({
                                "id": str(item['id']),
                                "label": str(item['label']),
                                "confidence": float(item['confidence'])
                            })
                    
                    return suggestions[:10]  # Limit to top 10
            
            # Fallback suggestions
            return [
                {"id": "sewing-1", "label": "Sewing Operation", "confidence": 0.8},
                {"id": "cutting-1", "label": "Cutting Operation", "confidence": 0.7},
                {"id": "quality-1", "label": "Quality Check", "confidence": 0.6}
            ]
        
        except Exception as e:
            logger.error(f"Operation suggestion failed: {e}")
            return [{"id": "fallback-1", "label": "General Operation", "confidence": 0.5}]
    
    async def generate_completion(self, prompt: str, max_tokens: int = 200) -> str:
        """Generate text completion using Ollama with UTF-8 safe handling."""
        if not self.available:
            return "AI completion service is not available. Please check your Ollama installation."
        
        # Limit prompt length
        prompt = prompt[:8000]
        
        try:
            result = subprocess.run(
                ['ollama', 'run', self.model, prompt],
                capture_output=True,
                text=True,
                encoding="utf-8",   # ✅ Force UTF-8 decoding
                errors="ignore",    # ✅ Ignore bad characters instead of crashing
                timeout=300
            )
            
            if result.returncode == 0 and result.stdout.strip():
                response = result.stdout.strip()

                # ✅ Try to detect if AI gave JSON
                if response.startswith("{") or response.startswith("["):
                    return response  # return raw JSON string for parsing later

                # ✅ Fallback: truncate long text
                words = response.split()
                if len(words) > max_tokens:
                    response = ' '.join(words[:max_tokens]) + "..."
                return response
            else:
                return "Unable to generate completion at this time."
        
        except Exception as e:
            logger.error(f"Ollama completion failed: {e}", exc_info=True)
            return "AI completion service temporarily unavailable."
        
# Move format_prediction_text to module level
def format_prediction_text(ai_prediction: dict, horizon: int) -> str:
    lines = []
    lines.append("📊 AI Efficiency Prediction Report")
    lines.append("=================================")
    lines.append("")
    lines.append(ai_prediction["prediction_summary"])
    lines.append("")
    lines.append("Line-wise Analysis:")
    lines.append("-------------------")

    for lp in ai_prediction["line_predictions"]:
        lines.append(
            f"{'🔴' if 'Severe' in lp['risk'] else '⚠️' if 'Moderate' in lp['risk'] else '✅'} "
            f"Line {lp['line']} → Target {lp['target']} pcs, Actual {lp['actual']} pcs, "
            f"Efficiency {lp['efficiency']}% (Gap: {lp['gap']} pcs)\n"
            f"   Risk: {lp['risk']}\n"
            f"   Prediction ({horizon} days): {lp['prediction']}\n"
            f"   Recommended Actions: {', '.join(lp['actions'])}\n"
        )

    lines.append("Strategic Recommendations:")
    lines.append("--------------------------")
    for rec in ai_prediction["strategic_recommendations"]:
        lines.append(f"✔ {rec}")

    return "\n".join(lines)


class EnhancedRTMSEngine:
    """Enhanced RTMS Engine with production-ready features"""
    
    def __init__(self):
        self.db_config = config.database
        self.engine = self._create_database_engine()
        self.last_fetch_time = None
        self.monitoring_active = False
        self.ai_service = OllamaAIService(config.ai.primary_model)
        
        # WhatsApp notifications disabled flag
        self.whatsapp_disabled = False
        logger.info("🚫 WhatsApp notifications temporarily DISABLED")
        
        # Start background monitoring
        self.start_background_monitoring()

    def _create_database_engine(self):
        try:
            # Use config.py database config
            db = config.database
            username = urllib.parse.quote_plus(db.username)
            password = urllib.parse.quote_plus(db.password)

            # Ensure driver string comes from .env or default
            driver = db.driver.replace(" ", "+")  # "ODBC Driver 17 for SQL Server" → "ODBC+Driver+17+for+SQL+Server"

            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{db.server}/{db.database}"
                f"?driver={driver}&TrustServerCertificate=yes"
            )

            engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=3600,
                echo=False
            )
            logger.info("✅ Database engine created successfully")
            return engine
        except Exception as e:
            logger.error(f"❌ Failed to create database engine: {e}", exc_info=True)
            return None


    async def get_unit_codes(self) -> List[str]:
        """Get list of unique unit codes"""
        try:
            query = """
            SELECT DISTINCT [UnitCode]
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE [UnitCode] IS NOT NULL AND [UnitCode] != ''
            ORDER BY [UnitCode]
            """
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
                return df['UnitCode'].tolist()
        except Exception as e:
            logger.error(f"❌ Failed to fetch unit codes: {e}")
            return []

    async def get_floor_names(self, unit_code: str) -> List[str]:
        """Get list of floor names for a unit"""
        try:
            query = text("""
                SELECT DISTINCT [FloorName]
                FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
                WHERE [UnitCode] = :unit_code
                  AND [FloorName] IS NOT NULL 
                  AND [FloorName] != ''
                ORDER BY [FloorName]
            """)

            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection, params={"unit_code": unit_code})
                return df['FloorName'].tolist()
        except Exception as e:
            logger.error(f"❌ Failed to fetch floor names: {e}")
            return []

    async def get_line_names(self, unit_code: str, floor_name: str) -> list[str]:
        """Get list of line names for a unit and floor"""
        try:
            query = text("""
                SELECT DISTINCT [LineName]
                FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
                WHERE [UnitCode] = :unit_code 
                  AND [FloorName] = :floor_name
                  AND [LineName] IS NOT NULL 
                  AND [LineName] != ''
                ORDER BY [LineName]
            """)

            with self.engine.connect() as connection:
                df = pd.read_sql(
                    query, 
                    connection, 
                    params={"unit_code": unit_code, "floor_name": floor_name}
                )
                return df['LineName'].tolist()

        except Exception as e:
            logger.error(f"❌ Failed to fetch line names: {e}")
            return []

    async def get_operations_by_line(self, unit_code: str, floor_name: str, line_name: str) -> List[str]:
        """Get list of operations for specific line"""
        try:
            query = """
            SELECT DISTINCT [NewOperSeq]
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE [UnitCode] = @unit_code AND [FloorName] = @floor_name 
            AND [LineName] = @line_name AND [NewOperSeq] IS NOT NULL AND [NewOperSeq] != ''
            ORDER BY [NewOperSeq]
            """
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection, params={
                    "unit_code": unit_code, 
                    "floor_name": floor_name,
                    "line_name": line_name
                })
                return df['NewOperSeq'].tolist()
        except Exception as e:
            logger.error(f"❌ Failed to fetch operations by line: {e}")
            return []

    async def fetch_production_data(
        self,
        unit_code: Optional[str] = None,
        floor_name: Optional[str] = None,
        line_name: Optional[str] = None,
        operation: Optional[str] = None,
        part_name: Optional[str] = None,
        limit: int = 1000
    ) -> List[RTMSProductionData]:
        """Fetch production data with optional filtering - FIXED DATE QUERY"""
        if not self.engine:
            logger.error("❌ Database engine not available")
            return []

        try:
            query = f"""
            SELECT TOP ({limit})
            [LineName], [EmpCode], [EmpName], [DeviceID],
            [StyleNo], [OrderNo], [Operation], [SAM],
            [Eff100], [Eff75], [ProdnPcs], [EffPer],
            [OperSeq], [UsedMin], [TranDate], [UnitCode], 
            [PartName], [FloorName], [ReptType], [PartSeq], 
            [EffPer100], [EffPer75], [NewOperSeq],
            [BuyerCode], [ISFinPart], [ISFinOper], [IsRedFlag]
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE [ReptType] IN ('RTMS', 'RTM5', 'RTM$')
            AND CAST([TranDate] AS DATE) = CAST(GETDATE() AS DATE)
            AND [ProdnPcs] > 0
            AND [EmpCode] IS NOT NULL
            AND [LineName] IS NOT NULL
            """
            
            # Add filters
            params = {}
            if unit_code:
                query += " AND [UnitCode] = @unit_code"
                params["unit_code"] = unit_code
            if floor_name:
                query += " AND [FloorName] = @floor_name"
                params["floor_name"] = floor_name
            if line_name:
                query += " AND [LineName] = @line_name"
                params["line_name"] = line_name
            if operation:
                query += " AND [NewOperSeq] = @operation"
                params["operation"] = operation
            if part_name:
                query += " AND [PartName] = @part_name"
                params["part_name"] = part_name
            
            query += " ORDER BY [TranDate] DESC"
            
            # Execute query
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection, params=params)
                logger.info(f"📊 Retrieved {len(df)} production records")
            
            # Convert to data objects
            production_data = []
            for _, row in df.iterrows():
                try:
                    data = RTMSProductionData(
                        LineName=str(row['LineName']) if pd.notna(row['LineName']) else '',
                        EmpCode=str(row['EmpCode']) if pd.notna(row['EmpCode']) else '',
                        EmpName=str(row['EmpName']) if pd.notna(row['EmpName']) else '',
                        DeviceID=str(row['DeviceID']) if pd.notna(row['DeviceID']) else '',
                        StyleNo=str(row['StyleNo']) if pd.notna(row['StyleNo']) else '',
                        OrderNo=str(row['OrderNo']) if pd.notna(row['OrderNo']) else '',
                        Operation=str(row['Operation']) if pd.notna(row['Operation']) else '',
                        SAM=float(row['SAM']) if pd.notna(row['SAM']) else 0.0,
                        Eff100=int(row['Eff100']) if pd.notna(row['Eff100']) else 0,
                        Eff75=int(row['Eff75']) if pd.notna(row['Eff75']) else None,
                        ProdnPcs=int(row['ProdnPcs']) if pd.notna(row['ProdnPcs']) else 0,
                        EffPer=float(row['EffPer']) if pd.notna(row['EffPer']) else 0.0,
                        OperSeq=int(row['OperSeq']) if pd.notna(row['OperSeq']) else 0,
                        UsedMin=float(row['UsedMin']) if pd.notna(row['UsedMin']) else 0.0,
                        TranDate=str(row['TranDate']) if pd.notna(row['TranDate']) else '',
                        UnitCode=str(row['UnitCode']) if pd.notna(row['UnitCode']) else '',
                        PartName=str(row['PartName']) if pd.notna(row['PartName']) else '',
                        FloorName=str(row['FloorName']) if pd.notna(row['FloorName']) else '',
                        ReptType=str(row['ReptType']) if pd.notna(row['ReptType']) else '',
                        PartSeq=int(row['PartSeq']) if pd.notna(row['PartSeq']) else 0,
                        EffPer100=float(row['EffPer100']) if pd.notna(row['EffPer100']) else 0.0,
                        EffPer75=float(row['EffPer75']) if pd.notna(row['EffPer75']) else 0.0,
                        NewOperSeq=str(row['NewOperSeq']) if pd.notna(row['NewOperSeq']) else '',
                        BuyerCode=str(row['BuyerCode']) if pd.notna(row['BuyerCode']) else '',
                        ISFinPart=str(row['ISFinPart']) if pd.notna(row['ISFinPart']) else '',
                        ISFinOper=str(row['ISFinOper']) if pd.notna(row['ISFinOper']) else '',
                        IsRedFlag=int(row['IsRedFlag']) if pd.notna(row['IsRedFlag']) else 0
                    )
                    production_data.append(data)
                except Exception as row_error:
                    logger.warning(f"⚠️ Error processing row: {row_error}")
                    continue
            
            self.last_fetch_time = datetime.now()
            return production_data
        
        except Exception as e:
            logger.error(f"❌ Database query failed: {e}")
            return []

    async def get_operations_list(self) -> List[str]:
        """Get list of unique operations (NewOperSeq values) - FIXED DATE"""
        try:
            query = """
            SELECT DISTINCT [NewOperSeq]
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE [NewOperSeq] IS NOT NULL
            AND [NewOperSeq] != ''
            AND CAST([TranDate] AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY [NewOperSeq]
            """
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
                operations = df['NewOperSeq'].tolist()
                logger.info(f"📋 Retrieved {len(operations)} operations")
                return operations
        except Exception as e:
            logger.error(f"❌ Failed to fetch operations: {e}")
            return []

    def process_efficiency_analysis(self, data: List[RTMSProductionData]) -> Dict[str, Any]:
        """Process efficiency analysis with AI insights"""
        if not data:
            return {"status": "no_data", "message": "No production data available"}

        # Calculate operator efficiencies
        operators = []
        underperformers = []
        operation_efficiencies = {}

        for emp_data in data:
            efficiency = emp_data.calculate_efficiency()
            operator = {
                "emp_name": emp_data.EmpName,
                "emp_code": emp_data.EmpCode,
                "line_name": emp_data.LineName,
                "unit_code": emp_data.UnitCode,
                "floor_name": emp_data.FloorName,
                "operation": emp_data.Operation,
                "new_oper_seq": emp_data.NewOperSeq,
                "device_id": emp_data.DeviceID,
                "efficiency": round(efficiency, 2),
                "production": emp_data.ProdnPcs,
                "target": emp_data.Eff100,
                "status": self._get_efficiency_status(efficiency),
                "is_top_performer": efficiency >= 100
            }
            operators.append(operator)

            # Check if should send WhatsApp alert using business logic
            if should_send_whatsapp(operator, operators):
                underperformers.append(operator)

            # Track operation efficiencies for relative calculations
            if emp_data.NewOperSeq not in operation_efficiencies:
                operation_efficiencies[emp_data.NewOperSeq] = []
            operation_efficiencies[emp_data.NewOperSeq].append(efficiency)

        # Calculate overall metrics
        total_production = sum(d.ProdnPcs for d in data)
        total_target = sum(d.Eff100 for d in data)
        overall_efficiency = (total_production / total_target * 100) if total_target > 0 else 0

        # Generate AI insights
        ai_insights = self._generate_ai_insights(operators, overall_efficiency, underperformers)

        return {
            "status": "success",
            "overall_efficiency": round(overall_efficiency, 2),
            "total_production": total_production,
            "total_target": total_target,
            "operators": operators,
            "underperformers": underperformers,
            "ai_insights": ai_insights,
            "whatsapp_alerts_needed": len(underperformers) > 0 and not self.whatsapp_disabled,
            "whatsapp_disabled": self.whatsapp_disabled,
            "analysis_timestamp": datetime.now().isoformat(),
            "records_analyzed": len(data),
            "data_date": date.today().strftime("%Y-%m-%d") 
        }

    def _get_efficiency_status(self, efficiency: float) -> str:
        """Get efficiency status based on thresholds"""
        if efficiency >= 100:
            return 'excellent'
        elif efficiency >= config.alerts.efficiency_threshold:
            return 'good'
        elif efficiency >= config.alerts.critical_threshold:
            return 'needs_improvement'
        else:
            return 'critical'

    def _generate_ai_insights(self, operators: List[Dict], overall_efficiency: float, underperformers: List[Dict]) -> Dict[str, Any]:
        """Generate AI-powered insights"""
        # Group by lines and operations for analysis
        line_performance = {}
        operation_performance = {}

        for op in operators:
            line = op["line_name"]
            operation = op["new_oper_seq"]

            if line not in line_performance:
                line_performance[line] = []
            line_performance[line].append(op["efficiency"])

            if operation not in operation_performance:
                operation_performance[operation] = []
            operation_performance[operation].append(op["efficiency"])

        # Calculate averages
        line_avg = {line: sum(effs) / len(effs) for line, effs in line_performance.items()}
        operation_avg = {op: sum(effs) / len(effs) for op, effs in operation_performance.items()}

        # Generate insights
        summary = self._generate_summary_insight(overall_efficiency, len(operators), len(underperformers))
        performance_analysis = {
            "best_performing_line": max(line_avg.items(), key=lambda x: x[1]) if line_avg else None,
            "worst_performing_line": min(line_avg.items(), key=lambda x: x[1]) if line_avg else None,
            "best_performing_operation": max(operation_avg.items(), key=lambda x: x[1]) if operation_avg else None,
            "worst_performing_operation": min(operation_avg.items(), key=lambda x: x[1]) if operation_avg else None,
        }

        recommendations = self._generate_recommendations(underperformers, line_avg, operation_avg)

        # Add AI predictions (simple trend analysis)
        predictions = {
            "trend": "stable" if 80 <= overall_efficiency <= 95 else "decreasing" if overall_efficiency < 80 else "increasing",
            "confidence": 0.85,
            "forecast": f"Based on current patterns, efficiency is expected to {'improve' if overall_efficiency >= 85 else 'require intervention'} in the next monitoring cycle."
        }

        return {
            "summary": summary,
            "performance_analysis": performance_analysis,
            "recommendations": recommendations,
            "predictions": predictions
        }

    def _generate_summary_insight(self, overall_eff: float, total_emp: int, underperformers_count: int) -> str:
        """Generate summary insight"""
        if overall_eff >= 95:
            return f"🎯 Excellent production performance! {total_emp} employees averaging {overall_eff:.1f}% efficiency."
        elif overall_eff >= 85:
            return f"📈 Good production performance with {overall_eff:.1f}% efficiency. {underperformers_count} employees need attention."
        elif overall_eff >= 70:
            return f"⚠️ Below target performance at {overall_eff:.1f}%. {underperformers_count} employees require immediate intervention."
        else:
            return f"🚨 Critical performance issues! Only {overall_eff:.1f}% efficiency with {underperformers_count} underperformers."

    def _generate_recommendations(self, underperformers: List[Dict], line_avg: Dict, operation_avg: Dict) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []

        if underperformers:
            recommendations.append(f"Focus training on {len(underperformers)} underperforming employees")

        # Critical cases
        critical_cases = [emp for emp in underperformers if emp["efficiency"] < config.alerts.critical_threshold]
        if critical_cases:
            recommendations.append(f"{len(critical_cases)} employees need immediate supervision")

        # Line-specific recommendations
        if line_avg:
            worst_line = min(line_avg.items(), key=lambda x: x[1])
            if worst_line[1] < config.alerts.efficiency_threshold:
                recommendations.append(f"Line {worst_line[0]} requires immediate attention ({worst_line[1]:.1f}% efficiency)")

        # Operation-specific recommendations
        if operation_avg:
            worst_operation = min(operation_avg.items(), key=lambda x: x[1])
            if worst_operation[1] < config.alerts.efficiency_threshold:
                recommendations.append(f"Operation {worst_operation[0]} needs process optimization")

        return recommendations

    def start_background_monitoring(self):
        """Start background monitoring thread"""
        def monitor():
            while True:
                try:
                    time.sleep(600)  # 10 minutes
                    logger.info("🔄 Background monitoring cycle")
                    # Add periodic tasks here if needed
                except Exception as e:
                    logger.error(f"❌ Background monitoring error: {e}")

        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
        logger.info("✅ Background monitoring started")

# Initialize RTMS Engine
rtms_engine = EnhancedRTMSEngine()

# AI Endpoints
# @app.post("/api/ai/summarize")
# async def ai_summarize(request: AISummarizeRequest, background_tasks: BackgroundTasks):
#     """Summarize efficiency analysis using Ollama AI with garment-specific insights"""
#     try:
#         if request.text:  
#             # ✅ Case 1: Summarize free text
#             if len(request.text) > 10000:
#                 raise HTTPException(status_code=400, detail="Text too long (max 10,000 characters)")
            
#             summary = await rtms_engine.ai_service.summarize_text(
#                 request.text,
#                 request.length
#             )

#         else:
#             # ✅ Case 2: Summarize efficiency analysis (with optional filters)
#             data = await rtms_engine.fetch_production_data(
#                 unit_code=request.unit_code if request.unit_code else None,
#                 floor_name=request.floor_name if request.floor_name else None,
#                 line_name=request.line_name if request.line_name else None,
#                 operation=request.operation if request.operation else None,
#                 limit=request.limit
#             )

#             logger.info(f"Fetched {len(data)} records for summarization")

#             if not data:
#                 return {
#                     "status": "success",
#                     "summary": "No production data available for the given filters (or today’s date).",
#                     "filters_applied": {
#                         "unit_code": request.unit_code,
#                         "floor_name": request.floor_name,
#                         "line_name": request.line_name,
#                         "operation": request.operation
#                     }
#                 }

#             # ✅ Limit BEFORE analysis
#             max_records = 1500
#             data = data[:max_records]

#             # 🔹 Custom garment aggregation
#             from collections import defaultdict
#             grouped = defaultdict(lambda: {"Eff100": 0, "ProdnPcs": 0})

#             for row in data:
#                 if str(row.ISFinPart).upper() == "Y":
#                     key = (row.LineName, row.StyleNo)  # Use attribute access
#                     grouped[key]["Eff100"] += row.Eff100 or 0
#                     grouped[key]["ProdnPcs"] += row.ProdnPcs or 0

#             # 🔹 Convert grouped data to text
#             lines = []
#             for (line_name, style_no), agg in grouped.items():
#                 target = agg["Eff100"]
#                 actual = agg["ProdnPcs"]
#                 lines.append(
#                     f"Line {line_name}, Style {style_no}: "
#                     f"Target {target} pcs, Actual {actual} pcs"
#                 )

#             analysis_text = "Garment production summary (final parts only):\n" + "\n".join(lines)

#             # ✅ Truncate if still too long
#             if len(analysis_text) > 10000:
#                 logger.warning(f"Analysis text too large ({len(analysis_text)} chars). Truncating...")
#                 analysis_text = analysis_text[:8000] + "\n...[TRUNCATED]..."

#             # 🔹 Pass context to AI
#             prompt = (
#                 "You are analyzing garment factory production efficiency.\n"
#                 "The data below represents Garment pieces produced in final parts, identified by ISFinPart = 'Y'.\n"
#                 "Each record shows the total target (Eff100) and actual production (ProdnPcs) for the final part of a LineName,\n"
#                 "along with the corresponding PartName and calculated efficiency (Actual / Target * 100).\n"
#                 "A unit consists of different lines, each with various parts. The final part's production determines the line's efficiency.\n"
#                 "For example, if a line has multiple operations on a part (e.g., stitching, finishing), the final operator's output (marked by ISFinPart = 'Y')\n"
#                 "is used to calculate efficiency.\n"
#                 "Please highlight efficiency gaps, underperforming lines, and provide polite, constructive insights to boost the team's morale\n"
#                 "and support production managers in improving performance.\n\n"
#                 f"{analysis_text}"
#             )

#             summary = await rtms_engine.ai_service.summarize_text(prompt, request.length)

#         return {
#             "status": "success",
#             "summary": summary,
#             "filters_applied": {
#                 "unit_code": request.unit_code,
#                 "floor_name": request.floor_name,
#                 "line_name": request.line_name,
#                 "operation": request.operation
#             }
#         }

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"AI summarization failed: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=500,
#             detail=f"AI summarization failed: {str(e)}"
#         )



# @app.post("/api/ai/suggest_ops")
# async def ai_suggest_operations(request: AISuggestOperationsRequest):
#     """
#     Suggest operations using AI based on garment production data.
#     If no context is provided, inject predefined business suggestions text.
#     """
#     try:
#         # ✅ Rate limiting
#         client_ip = "127.0.0.1"
#         if not check_rate_limit(client_ip):
#             raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again later.")

#         # ✅ Validate context length
#         if request.context and len(request.context) > 8000:
#             raise HTTPException(status_code=400, detail="Context too long (max 8,000 characters)")

#         # ✅ If no context provided → inject Business Suggestions
#         if not request.context or not request.context.strip():
#             request.context = (
#                 "Business Suggestions:\n\n"
#                 "Line S1-1 is short by 300 pcs due to bottleneck at Attach Collar Band. "
#                 "Suggest allocating an additional operator or improving workstation layout.\n\n"
#                 "Line S3-1 underproduced by 200 pcs. Sleeve Hemming efficiency is 65%. "
#                 "Recommend operator retraining or introducing semi-automatic hemming machines.\n\n"
#                 "Lines S2-2 and S4-1 are performing above 90% efficiency. "
#                 "Best practices here (machine setup, operator handling) should be shared across units.\n\n"
#                 "Shift planning: Balance workload by redistributing operators "
#                 "from S2-2 (excess capacity) to S1-1."
#             )

#             logger.debug(f"Injected request.context:\n{request.context}")

#         # ✅ Pass to AI
#         suggestions_data = await rtms_engine.ai_service.suggest_operations(
#             request.context,
#             request.query
#         )

#         suggestions = [s.get("label", "Generic suggestion") for s in suggestions_data]

#         return {
#             "success": True,
#             "context_used": request.context,  # 👈 shows exactly what was injected
#             "suggestions": suggestions
#         }

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"AI operation suggestion failed: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=f"AI operation suggestion failed: {str(e)}")

    
# @app.post("/api/ai/completion")
# async def ai_completion(request: AICompletionRequest):
#     """Generate AI text completion based on a prompt, optionally using production data context.
#     The prompt can include garment production details for tailored responses."""
#     try:
#         # Check rate limit
#         client_ip = "127.0.0.1"  # Placeholder; in production, use request.client.host
#         if not check_rate_limit(client_ip):
#             raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again later.")

#         # Validate prompt length
#         if len(request.prompt) > 8000:
#             raise HTTPException(status_code=400, detail="Prompt too long (max 8,000 characters)")

#         # Fetch production data if no prompt is provided (optional enhancement)
#         if not request.prompt.strip():
#             data = await rtms_engine.fetch_production_data(limit=1000)
#             if not data:
#                 return {
#                     "status": "success",
#                     "text": "No production data available. Please provide a prompt or check data filters."
#                 }
#             # Generate prompt from data (similar to summarize logic)
#             grouped = defaultdict(lambda: {"Eff100": 0, "ProdnPcs": 0, "PartName": None})
#             for row in data:
#                 if str(row.ISFinPart).upper() == "Y":
#                     key = row.LineName
#                     grouped[key]["Eff100"] = row.Eff100 or 0
#                     grouped[key]["ProdnPcs"] = row.ProdnPcs or 0
#                     grouped[key]["PartName"] = row.PartName
#             lines = [
#                 f"Line {line_name}, Part {agg['PartName']}: Target {agg['Eff100']} pcs, Actual {agg['ProdnPcs']} pcs, Efficiency {(agg['ProdnPcs'] / agg['Eff100'] * 100):.1f}%"
#                 for line_name, agg in grouped.items() if agg["Eff100"] > 0
#             ]
#             request.prompt = (
#                 "You are analyzing garment factory production efficiency.\n"
#                 "The data below represents final parts (ISFinPart = 'Y') with targets (Eff100) and actual production (ProdnPcs).\n"
#                 "Provide insights or recommendations based on this data:\n\n"
#                 + "\n".join(lines)
#             )

#         # Generate completion
#         completion = await rtms_engine.ai_service.generate_completion(request.prompt, request.maxTokens or 200)
#         logger.info(f"Generated completion with {len(completion.split())} words")

#         return {
#             "status": "success",
#             "text": completion,
#             "prompt_used": request.prompt[:200] + "..." if len(request.prompt) > 200 else request.prompt
#         }

#     except HTTPException as http_err:
#         raise http_err
#     except Exception as e:
#         logger.error(f"AI completion failed: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail="AI completion failed")

# # AI PREDICTION - BASED ON EFFICIENCY TRENDS
# @app.post("/api/ai/predict_efficiency")
# async def predict_efficiency(
#     unit_code: Optional[str] = Body(None),
#     floor_name: Optional[str] = Body(None),
#     line_name: Optional[str] = Body(None),
#     operation: Optional[str] = Body(None),
#     limit: int = Body(1000),
#     horizon: int = Body(7)
# ):
#     """AI-driven prediction of garment line efficiency trends (Ollama-powered)."""
#     try:
#         # 1️⃣ Base query for last 2 months (raw data, no aggregation)
#         query = """
#         SELECT *
#         FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
#         WHERE [TranDate] >= DATEADD(MONTH, -2, CAST(GETDATE() AS DATE))
#           AND ProdnPcs > 0
#           AND LineName IS NOT NULL
#           AND StyleNo IS NOT NULL
#         """

#         # 🔹 Add optional filters dynamically
#         conditions = []
#         if unit_code:
#             conditions.append(f"UnitCode = '{unit_code}'")
#         if floor_name:
#             conditions.append(f"FloorName = '{floor_name}'")
#         if line_name:
#             conditions.append(f"LineName = '{line_name}'")
#         if operation:
#             conditions.append(f"Operation = '{operation}'")

#         if conditions:
#             query += " AND " + " AND ".join(conditions)

#         query += " ORDER BY TranDate DESC, LineName, StyleNo"

#         # 🔹 Fetch data
#         import pyodbc
#         conn = pyodbc.connect(config.database.get_connection_string())
#         df = pd.read_sql(query, conn)
#         conn.close()

#         # 🔹 Log to console
#         print(f"📥 Data fetched from DB: {len(df)} rows")
#         if not df.empty:
#             print("🔎 Sample rows from DB:")
#             print(df.head(5).to_string(index=False))

#         if df.empty:
#             return {
#                 "status": "no_data",
#                 "message": "No production data available for AI-driven efficiency prediction",
#                 "filters_applied": {
#                     "unit_code": unit_code,
#                     "floor_name": floor_name,
#                     "line_name": line_name,
#                     "operation": operation,
#                 }
#             }

#         # 2️⃣ Build context for AI (using raw fields)
#         lines = []
#         for _, row in df.iterrows():
#             target = int(row.Eff100) if pd.notnull(row.Eff100) else 0
#             actual = int(row.ProdnPcs) if pd.notnull(row.ProdnPcs) else 0
#             efficiency = (actual / target * 100) if target > 0 else 0
#             gap = target - actual

#             lines.append(
#                 f"Line {row.LineName} (Style {row.StyleNo}, Unit {row.UnitCode}, Floor {row.FloorName}) "
#                 f"→ Target: {target}, Actual: {actual}, Gap: {gap}, "
#                 f"Operator: {row.EmpName if pd.notnull(row.EmpName) else 'Unknown'}, "
#                 f"Eff%: {row.EffPer:.1f}%, SAM: {row.SAM:.1f}"
#             )

#         context = "Garment Production Efficiency Report (last 2 months raw data):\n\n" + "\n".join(lines[:limit])

#         # 3️⃣ Build AI prompt
#         prompt = f"""
#                 You are an expert garment industry AI consultant.
#                 Based on the following production data, PREDICT efficiency trends for the next {horizon} days.
#                 Return insights in structured JSON with the following fields:

#                 - prediction_summary: high-level summary of overall performance and risks
#                 - line_predictions: array of lines with {{
#                     "line","style","unit","floor",
#                     "target","actual","efficiency","gap",
#                     "operator","risk","prediction","actions"
#                 }}
#                 - strategic_recommendations: array of high-level recommendations for management

#         Data:
#     {context}
# """

#         # 4️⃣ Call AI service
#         ai_response = await rtms_engine.ai_service.generate_completion(prompt, max_tokens=600)

#         # 5️⃣ Try to parse JSON
#         try:
#             ai_prediction = json.loads(ai_response)
#         except Exception:
#             ai_prediction = {"prediction_summary": ai_response}

#         # 6️⃣ Human-readable summary (safe handling)
#         if "line_predictions" in ai_prediction and "strategic_recommendations" in ai_prediction:
#             ai_prediction_text = format_prediction_text(ai_prediction, horizon)
#         else:
#             ai_prediction_text = (
#                 f"📊 AI Prediction Summary\n\n"
#                 f"{ai_prediction.get('prediction_summary', 'No detailed predictions available.')}\n\n"
#                 f"(Detailed line predictions were not provided by the AI.)"
#             )

#         return {
#             "status": "success",
#             "rows_fetched": len(df),
#             "ai_prediction_json": ai_prediction,
#             "ai_prediction_text": ai_prediction_text,
#             "context_used": context,
#             "filters_applied": {
#                 "unit_code": unit_code,
#                 "floor_name": floor_name,
#                 "line_name": line_name,
#                 "operation": operation,
#             }
#         }

#     except Exception as e:
#         logger.error(f"❌ AI-driven efficiency prediction failed: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail="AI-driven efficiency prediction failed")


# Main API Endpoints
@app.get("/api/status")
async def get_service_status():
    """Get service status"""
    return {
        "service": "Fabric Pulse AI",
        "version": "4.0.0",
        "status": "running",
        "ai_enabled": rtms_engine.ai_service.available,
        "whatsapp_enabled": not rtms_engine.whatsapp_disabled,
        "whatsapp_disabled": rtms_engine.whatsapp_disabled,
        "database_connected": rtms_engine.engine is not None,
        "bot_name": "Fabric Pulse AI Bot",
        "data_date": date.today().strftime("%Y-%m-%d") ,  # Fixed to today's date
        "features": ["AI Insights", "WhatsApp Alerts", "Real-time Monitoring", "Dependent Filters"],
        "last_fetch": rtms_engine.last_fetch_time.isoformat() if rtms_engine.last_fetch_time else None,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/rtms/filters/units")
async def get_unit_codes():
    """Get list of unit codes"""
    try:
        units = await rtms_engine.get_unit_codes()
        return {
            "status": "success",
            "data": units,
            "count": len(units),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to fetch unit codes: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch unit codes")

@app.get("/api/rtms/filters/floors")
async def get_floor_names(unit_code: str = Query(...)):
    """Get list of floor names for a unit"""
    try:
        floors = await rtms_engine.get_floor_names(unit_code)
        return {
            "status": "success",
            "data": floors,
            "count": len(floors),
            "unit_code": unit_code,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to fetch floor names: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch floor names")

@app.get("/api/rtms/filters/lines")
async def get_line_names(unit_code: str = Query(...), floor_name: str = Query(...)):
    """Get list of line names for a unit and floor"""
    try:
        lines = await rtms_engine.get_line_names(unit_code, floor_name)
        return {
            "status": "success",
            "data": lines,
            "count": len(lines),
            "unit_code": unit_code,
            "floor_name": floor_name,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to fetch line names: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch line names")

@app.get("/api/rtms/filters/operations")
async def get_operations(unit_code: str = Query(None), floor_name: str = Query(None), line_name: str = Query(None)):
    """Get list of operations, optionally filtered by line"""
    try:
        if unit_code and floor_name and line_name:
            operations = await rtms_engine.get_operations_by_line(unit_code, floor_name, line_name)
        else:
            operations = await rtms_engine.get_operations_list()
        
        return {
            "status": "success",
            "data": operations,
            "count": len(operations),
           "data_date": date.today().strftime("%Y-%m-%d") ,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to fetch operations: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch operations")

@router.get("/api/rtms/filters/parts")
async def get_parts(
    unit_code: str = Query(...),
    floor_name: str = Query(...),
    line_name: str = Query(...)
):
    try:
        query = text("""
            SELECT DISTINCT [PartName]
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE [UnitCode] = :unit_code
              AND [FloorName] = :floor_name
              AND [LineName] = :line_name
              AND [PartName] IS NOT NULL AND [PartName] != ''
            ORDER BY [PartName]
        """)
        with rtms_engine.engine.connect() as connection:
            df = pd.read_sql(query, connection, params={
                "unit_code": unit_code,
                "floor_name": floor_name,
                "line_name": line_name
            })
        return {"status": "success", "data": df['PartName'].tolist()}
    except Exception as e:
        logger.error(f"Failed to fetch parts: {e}")
        return {"status": "error", "data": [], "message": str(e)}

# FIXED: Added missing analyze endpoint that frontend expects
@app.get("/api/rtms/analyze")
async def analyze_production_data(
    unit_code: Optional[str] = Query(None),
    floor_name: Optional[str] = Query(None),
    line_name: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=5000)
):
    """Analyze production data - frontend expects this endpoint"""
    try:
        # Fetch production data
        data = await rtms_engine.fetch_production_data(
            unit_code=unit_code,
            floor_name=floor_name,
            line_name=line_name,
            operation=operation,
            limit=limit
        )
        
        # Process analysis
        analysis = rtms_engine.process_efficiency_analysis(data)
        
        return {
            "status": "success",
            "data": analysis,
            "filters_applied": {
                "unit_code": unit_code,
                "floor_name": floor_name,
                "line_name": line_name,
                "operation": operation
            }
        }
    except Exception as e:
        logger.error(f"Failed to analyze production data: {e}")
        raise HTTPException(status_code=500, detail="Failed to analyze production data")

@router.get("/api/rtms/efficiency")
async def get_efficiency_summary(
    unit_code: str = Query(..., description="Unit code"),
    floor_name: str = Query(..., description="Floor name"),
    line_name: str = Query(..., description="Line name"),
    part_name: str = Query(..., description="Part name"),
):
    try:
        if not rtms_engine or not rtms_engine.engine:
            raise HTTPException(status_code=500, detail="DB engine not available")

        # Base SQL (no ISFinOper filter yet)
        base_sql = """
            SELECT 
                [LineName],
                [EmpCode],
                [EmpName],
                [ProdnPcs],
                [Eff100],
                [EffPer],
                [ISFinOper],
                [IsRedFlag],
                [TranDate],
                [UnitCode],
                [PartName],
                [FloorName],
                [Operation],
                [NewOperSeq]
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE 
                [UnitCode] = :unit_code
                AND [FloorName] = :floor_name
                AND [LineName] = :line_name
                AND [PartName] = :part_name
                AND [ReptType] = 'RTM$'
                AND CAST([TranDate] AS DATE) = CAST(GETDATE() AS DATE)
        """

        sql_with_finoper = text(base_sql + " AND [ISFinOper] = 'Y'")
        sql_without_finoper = text(base_sql)

        with rtms_engine.engine.connect() as conn:
            # Try with ISFinOper=Y
            df = pd.read_sql(sql_with_finoper, conn, params={
                "unit_code": unit_code,
                "floor_name": floor_name,
                "line_name": line_name,
                "part_name": part_name
            })

            # If no rows → retry without ISFinOper filter
            if df.empty:
                df = pd.read_sql(sql_without_finoper, conn, params={
                    "unit_code": unit_code,
                    "floor_name": floor_name,
                    "line_name": line_name,
                    "part_name": part_name
                })

        total_production = 0
        total_target = 0
        efficiency = 0.0

        if not df.empty:
            row_count = len(df)
            total_production = int(df["ProdnPcs"].sum() / row_count) if row_count > 0 else 0
            total_target = int(df["Eff100"].sum() / row_count) if row_count > 0 else 0
            efficiency = float(df["EffPer"].mean()) if "EffPer" in df else 0.0

        # Underperformers → IsRedFlag = 1
        underperformers = []
        underperformers_count = 0
        if not df.empty:
            df_flagged = df[df["IsRedFlag"] == 1]
            underperformers_count = len(df_flagged)
            for _, r in df_flagged.iterrows():
                underperformers.append({
                    "emp_code": str(r.EmpCode),
                    "emp_name": str(r.EmpName),
                    "line_name": str(r.LineName),
                    "part_name": str(r.PartName),
                    "operation": str(r.Operation) if pd.notna(r.Operation) else None,
                    "production": int(r.ProdnPcs) if pd.notna(r.ProdnPcs) else 0,
                    "target": int(r.Eff100) if pd.notna(r.Eff100) else 0,
                    "efficiency": float(r.EffPer) if pd.notna(r.EffPer) else 0.0,
                })

        return {"success": True, "data": {
            "total_production": total_production,
            "total_target": total_target,
            "efficiency": round(efficiency, 2),
            "underperformers_count": underperformers_count,
            "underperformers": underperformers
        }}

    except Exception as e:
        logger.error(f"efficiency endpoint failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/rtms/flagged")
async def get_flagged(
    unit_code: str = Query(...),
    floor_name: str = Query(...),
    line_name: str = Query(...),
    part_name: str = Query(...)
):
    """
    Return part-wise flagged employees (IsRedFlag = 1) with supervisor join.
    Always uses today's date automatically.
    """
    if not rtms_engine or not rtms_engine.engine:
        raise HTTPException(status_code=500, detail="DB engine not available")

    # ✅ Always use today's date
    fixed_date = date.today().strftime("%Y-%m-%d")

    sql = text("""
        SELECT 
            A.PartName,
            A.EmpCode,
            A.EmpName,
            A.LineName,
            A.ProdnPcs AS Production,
            A.Eff100 AS Target,
            A.EffPer AS Efficiency,
            A.NewOperSeq,
            A.Operation,
            A.IsRedFlag,
            B.SupervisorName,
            B.SupervisorCode,
            B.PhoneNumber
        FROM RTMS_SessionWiseProduction A
        LEFT JOIN RTMS_SupervisorsDetl B
          ON A.LineName = B.LineName AND A.PartName = B.PartName
        WHERE CAST(A.TranDate AS DATE) = :fixed_date
          AND A.ReptType IN ('RTM$')
          AND A.UnitCode = :unit_code
          AND A.FloorName = :floor_name
          AND A.LineName = :line_name
          AND A.PartName = :part_name
          AND A.IsRedFlag = 1
    """)

    with rtms_engine.engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={
            "fixed_date": fixed_date,
            "unit_code": unit_code,
            "floor_name": floor_name,
            "line_name": line_name,
            "part_name": part_name
        })

    if df.empty:
        return {"success": True, "data": {"parts": []}}

    parts_out = []
    grp = df.groupby("PartName")
    for pname, g in grp:
        employees = []
        for _, r in g.iterrows():
            production = int(r.Production) if pd.notna(r.Production) else 0
            target = int(r.Target) if pd.notna(r.Target) else 0

            efficiency = None
            if pd.notna(r.Efficiency):
                try:
                    efficiency = float(r.Efficiency)
                except Exception:
                    efficiency = None
            if efficiency is None and target > 0:
                efficiency = round((production / target) * 100.0, 2)

            employees.append({
                "emp_code": str(r["EmpCode"]) if pd.notna(r["EmpCode"]) else None,
                "emp_name": str(r["EmpName"]) if pd.notna(r["EmpName"]) else None,
                # "unit_code": str(r["UnitCode"]) if pd.notna(r["UnitCode"]) else None,
                # "floor_name": str(r["FloorName"]) if pd.notna(r["FloorName"]) else None,
                "line_name": str(r["LineName"]) if pd.notna(r["LineName"]) else None,
                "is_red_flag": int(r["IsRedFlag"]) if pd.notna(r["IsRedFlag"]) else 0,
                "production": int(r["Production"]) if pd.notna(r["Production"]) else 0,
                "target": int(r["Target"]) if pd.notna(r["Target"]) else 0,
                "efficiency": float(r["Efficiency"]) if pd.notna(r["Efficiency"]) else None,
                "new_oper_seq": str(r["NewOperSeq"]) if pd.notna(r["NewOperSeq"]) else None,
                "operation": str(r["Operation"]) if pd.notna(r["Operation"]) else None,
                "supervisor_name": str(r["SupervisorName"]) if pd.notna(r["SupervisorName"]) else None,
                "supervisor_code": str(r["SupervisorCode"]) if pd.notna(r["SupervisorCode"]) else None,
                "phone_number": str(r["PhoneNumber"]) if pd.notna(r["PhoneNumber"]) else None,
            })

        parts_out.append({
            "part_name": str(pname),
            "employee_count": len(employees),
            "employees": employees,
        })

    return {"success": True, "data": {"parts": parts_out}}

@router.get("/api/rtms/operationWiseEmployees")
async def get_operation_wise_employees(
    unit_code: str = Query(..., description="Unit code"),
    floor_name: str = Query(..., description="Floor name"),
    line_name: str = Query(..., description="Line name"),
    part_name: str = Query(..., description="Part name"),
):
    """
    Fetch operation-wise employees for given Unit, Floor, Line, and Part.
    Always uses today's date dynamically.
    """
    try:
        if not rtms_engine or not rtms_engine.engine:
            raise HTTPException(status_code=500, detail="DB engine not available")

        sql = text("""
            SELECT 
                [LineName],
                [EmpCode],
                [EmpName],
                [DeviceID],
                [StyleNo],
                [OrderNo],
                [Operation],
                [SAM],
                [Eff100],
                [Eff75],
                [ProdnPcs],
                [EffPer],
                [OperSeq],
                [UsedMin],
                [S01],[S02],[S03],[S04],[S05],[S06],
                [S07],[S08],[S09],[S10],[S11],[S12],
                [TranDate],
                [UnitCode],
                [PartName],
                [FloorName],
                [ReptType],
                [PartSeq],
                [EffPer100],
                [EffPer75],
                [RowId],
                [Operationseq],
                [PartsSeq],
                [NewOperSeq],
                [BuyerCode],
                [ISFinPart],
                [ISFinOper],
                [IsRedFlag]
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE [UnitCode] = :unit_code
              AND [FloorName] = :floor_name
              AND [LineName] = :line_name
              AND [PartName] = :part_name
              AND CAST([TranDate] AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY [TranDate] DESC
        """)

        with rtms_engine.engine.connect() as conn:
            df = pd.read_sql(
                sql, conn,
                params={
                    "unit_code": unit_code,
                    "floor_name": floor_name,
                    "line_name": line_name,
                    "part_name": part_name
                }
            )

        if df.empty:
            return {
                "success": True,
                "data": [],
                "message": "No employees found for the given filters today."
            }

        return {
            "success": True,
            "count": len(df),
            "data": df.to_dict(orient="records"),
            "filters_applied": {
                "unit_code": unit_code,
                "floor_name": floor_name,
                "line_name": line_name,
                "part_name": part_name,
                "date": date.today().strftime("%Y-%m-%d")
            }
        }

    except Exception as e:
        logger.error(f"operationWiseEmployees failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch operation-wise employees")

# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@router.post("/api/ai/generate_hourly_report")
async def generate_hourly_report(test_mode: bool = Query(False)):
    """
    Trigger the WhatsApp hourly report generator.
    - test_mode=true → sends only to test numbers
    - test_mode=false → (future) sends to supervisors
    """
    result = await whatsapp_service.generate_and_send_reports(test_mode=test_mode)
    return result
@app.get("/api/reports/hourly_pdf")
async def generate_pdf_report_api():
    """
    API to generate the hourly PDF report with **live DB data**.
    """
    try:
        from whatsapp_service import whatsapp_service  # adjust path if needed

        # 🔹 Fetch flagged employees from DB (LIVE)
        flagged_employees = await whatsapp_service.fetch_flagged_employees()

        # 🔹 Group and build PDF
        line_reports = whatsapp_service.group_employees_by_line_and_style(flagged_employees)
        timestamp = datetime.now()
        pdf_bytes = whatsapp_service.generate_pdf_report(line_reports, timestamp)

        pdf_filename = f"hourly_report_{timestamp.strftime('%Y%m%d_%H%M')}.pdf"
        pdf_path = Path("reports") / pdf_filename
        pdf_path.parent.mkdir(exist_ok=True)

        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes)

        return FileResponse(
            path=pdf_path,
            filename=pdf_filename,
            media_type="application/pdf"
        )

    except Exception as e:
        logger.error(f"❌ Failed to generate PDF report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate PDF report")
    
# Ultra-Advanced Chatbot Endpoint with LLaMA AI
"""
🔧 Ultra-Advanced Chatbot with Streaming Responses
Acts like ChatGPT (smooth typing animation in frontend).
"""

# import json
# import logging
# import time
# import pandas as pd
# import pyodbc
# from datetime import datetime
# from fastapi import Body, HTTPException
# from fastapi.responses import StreamingResponse

# from ollama_client import ollama_client  # must support chat + stream_chat
# # import config

# logger = logging.getLogger("ultra_advanced_chatbot")



# # Ultra-Advanced Chatbot with Safe Chunking


# @app.post("/api/ai/ultra_chatbot")
# async def ultra_advanced_ai_chatbot(
#     query: str = Body(..., description="Your question about production analytics"),
#     unit_code: Optional[str] = Body(None, description="Filter by unit code"),
#     floor_name: Optional[str] = Body(None, description="Filter by floor name"),
#     line_name: Optional[str] = Body(None, description="Filter by line name"),
#     operation: Optional[str] = Body(None, description="Filter by operation"),
#     part_name: Optional[str] = Body(None, description="Filter by part name"),
#     style_no: Optional[str] = Body(None, description="Filter by style number"),
#     data_range_months: int = Body(2, description="Data range in months (1-12)"),
#     max_records: int = Body(3000, description="Maximum records to analyze (cap 3000)"),
#     export: Optional[str] = Body(None, description="Export format: csv | pdf | None"),
#     reasoning_mode: str = Body("deep", description="Reasoning depth: basic, deep, expert")
# ):
#     """
#     Ultra-Advanced Chatbot endpoint (safe chunking, summarization, streaming to Ollama).
#     """

#     # local imports and logging
#     import pandas as pd
#     import pyodbc
#     import httpx
#     from datetime import datetime
#     from reportlab.pdfgen import canvas  # fallback PDF generator if custom helper missing

#     logger = logging.getLogger("ultra_advanced_chatbot")

#     start_time = time.time()

#     # Safeguard max_records
#     try:
#         max_records = int(max_records)
#     except Exception:
#         max_records = 3000
#     if max_records <= 0:
#         max_records = 1
#     if max_records > 3000:
#         max_records = 3000

#     # Essential columns to keep for context (minimize prompt size)
#     ESSENTIAL_COLS = [
#         "TranDate", "LineName", "StyleNo", "PartName",
#         "ProdnPcs", "EffPer", "SAM", "UnitCode", "FloorName", "EmpCode", "EmpName"
#     ]

#     # Try to locate PDF helper from your codebase
#     make_pdf_helper = None
#     try:
#         from ultra_advanced_chatbot import make_ultra_advanced_pdf_report
#         make_pdf_helper = make_ultra_advanced_pdf_report
#     except Exception:
#         try:
#             # maybe helper in same module
#             from fabric_pulse_ai_main import make_ultra_advanced_pdf_report
#             make_pdf_helper = make_ultra_advanced_pdf_report
#         except Exception:
#             make_pdf_helper = None

#     # Resolve DB connection string defensively
#     try:
#         conn_str = config.database.get_connection_string()
#     except Exception:
#         try:
#             conn_str = config.get_connection_string()
#         except Exception:
#             conn_str = os.environ.get("DATABASE_URL") or os.environ.get("DB_CONN")

#     if not conn_str:
#         logger.error("Database connection string not found (config.database or env DATABASE_URL/DB_CONN)")
#         raise HTTPException(status_code=500, detail="Database connection string not found")

#     try:
#         # -------------------------
#         # 1) Fetch data from DB (same style as predict_efficiency)
#         # -------------------------
#         sql = f"""
#         SELECT TOP ({max_records})
#             LineName, EmpCode, EmpName, DeviceID,
#             StyleNo, OrderNo, Operation, SAM,
#             Eff100, Eff75, ProdnPcs, EffPer,
#             OperSeq, UsedMin, TranDate, UnitCode, 
#             PartName, FloorName, ReptType, PartSeq
#         FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
#         WHERE [TranDate] >= DATEADD(MONTH, -{int(data_range_months)}, CAST(GETDATE() AS DATE))
#           AND ProdnPcs > 0
#           AND LineName IS NOT NULL
#           AND StyleNo IS NOT NULL
#         """

#         # dynamic filters (kept simple; follow your existing code pattern)
#         conditions = []
#         if unit_code:
#             conditions.append(f"UnitCode = '{unit_code}'")
#         if floor_name:
#             conditions.append(f"FloorName = '{floor_name}'")
#         if line_name:
#             conditions.append(f"LineName = '{line_name}'")
#         if operation:
#             conditions.append(f"Operation = '{operation}'")
#         if part_name:
#             conditions.append(f"PartName = '{part_name}'")
#         if style_no:
#             conditions.append(f"StyleNo = '{style_no}'")

#         if conditions:
#             sql += " AND " + " AND ".join(conditions)

#         sql += " ORDER BY TranDate DESC"

#         conn = pyodbc.connect(conn_str)
#         df = pd.read_sql(sql, conn)  # pandas will warn about DB-API, acceptable here
#         conn.close()

#         logger.info(f"✅ Retrieved {len(df)} records from DB")

#         if df.empty:
#             return {
#                 "status": "no_data",
#                 "answer": f"No production data found for the specified filters ({data_range_months} months).",
#                 "records_analyzed": 0,
#                 "filters_applied": {
#                     "unit_code": unit_code,
#                     "floor_name": floor_name,
#                     "line_name": line_name,
#                     "operation": operation,
#                     "part_name": part_name,
#                     "style_no": style_no
#                 }
#             }

#         # Keep only essential columns that exist in df
#         keep_cols = [c for c in ESSENTIAL_COLS if c in df.columns]
#         df_small = df[keep_cols].copy()

#         # -------------------------
#         # 2) Chunking & local summarization
#         # -------------------------
#         SECTION_SIZE = 100  # conservative chunk size to keep prompt small
#         chunk_summaries: List[str] = []

#         def compress_summary_text(s: str) -> str:
#             # simple compression: shorten keys and drop samples
#             s = s.replace("records=", "r=").replace("avg_eff=", "aEff=").replace("min_eff=", "mEff=").replace("max_eff=", "MEff=").replace("total_prod=", "totP=").replace("top_lines=", "topL=")
#             if "samples:" in s:
#                 s = s.split("samples:")[0]
#             if " | samples" in s:
#                 s = s.split(" | samples")[0]
#             return s

#         def summarize_chunk(chunk: pd.DataFrame, idx: int) -> str:
#             try:
#                 n = len(chunk)
#                 parts = [f"Chunk{idx}: r={n}"]
#                 if "EffPer" in chunk.columns:
#                     parts.append(f"aEff={chunk['EffPer'].mean():.1f}")
#                     parts.append(f"mEff={chunk['EffPer'].min():.1f}")
#                     parts.append(f"MEff={chunk['EffPer'].max():.1f}")
#                 if "ProdnPcs" in chunk.columns:
#                     parts.append(f"totP={int(chunk['ProdnPcs'].sum())}")
#                 # top lines by avg EffPer
#                 if "LineName" in chunk.columns and "EffPer" in chunk.columns:
#                     top = chunk.groupby("LineName")["EffPer"].mean().sort_values(ascending=False).head(3)
#                     parts.append("topL=" + ", ".join([f"{ln}:{val:.1f}" for ln, val in top.items()]))
#                 # small sample (1 row) to keep some context
#                 sample = ""
#                 try:
#                     sample_row = chunk.head(1).to_dict(orient="records")
#                     if sample_row:
#                         r = sample_row[0]
#                         sample = " | s=" + ", ".join([f"{k}={r[k]}" for k in r.keys() if k in ["LineName","StyleNo","PartName"] and r[k] is not None])
#                 except Exception:
#                     sample = ""
#                 summary = "; ".join(parts) + sample
#                 return compress_summary_text(summary)
#             except Exception as e:
#                 logger.debug(f"chunk summarization failed: {e}")
#                 return f"Chunk{idx}: r={len(chunk)}"

#         for i in range(0, len(df_small), SECTION_SIZE):
#             chunk = df_small.iloc[i:i + SECTION_SIZE]
#             chunk_summaries.append(summarize_chunk(chunk, idx=(i // SECTION_SIZE) + 1))

#         logger.info(f"✅ Created {len(chunk_summaries)} chunk summaries (section_size={SECTION_SIZE})")

#         # If too many summaries, aggregate globally instead of listing all
#         MAX_SUMMARIES_TO_INCLUDE = 12
#         if len(chunk_summaries) > MAX_SUMMARIES_TO_INCLUDE:
#             global_parts = []
#             if "EffPer" in df_small.columns:
#                 global_parts.append(f"global_aEff={df_small['EffPer'].mean():.1f}")
#                 global_parts.append(f"global_mEff={df_small['EffPer'].min():.1f}")
#                 global_parts.append(f"global_MEff={df_small['EffPer'].max():.1f}")
#             if "ProdnPcs" in df_small.columns:
#                 global_parts.append(f"global_totP={int(df_small['ProdnPcs'].sum())}")
#             if "LineName" in df_small.columns and "EffPer" in df_small.columns:
#                 top_lines = df_small.groupby("LineName")["EffPer"].mean().sort_values(ascending=False).head(5)
#                 global_parts.append("top_lines_overall=" + ", ".join([f"{ln}:{val:.1f}" for ln, val in top_lines.items()]))
#             aggregated = "AGGREGATED_SUMMARY: " + "; ".join(global_parts)
#             summaries_for_model = [aggregated]
#             logger.warning("Too many summaries -> using aggregated summary for final prompt (keeps prompt small)")
#         else:
#             summaries_for_model = chunk_summaries

#         # -------------------------
#         # 3) Build final prompt (concise)
#         # -------------------------
#         base_prompt = f"""You are an expert garment production analytics assistant.
# User query: {query}

# You MUST answer using only the provided production summaries.
# Respond structured as:
# 1) Analytical Findings
# 2) Diagnostic Analysis (root causes)
# 3) Predictive Forecast
# 4) Strategic Recommendations
# 5) Comparative Benchmarks

# Be professional, concise, and cite example summary lines or aggregated stats when useful.
# """

#         prompt_parts = [base_prompt, "\nPRODUCTION_SUMMARIES:"]
#         for s in summaries_for_model:
#             prompt_parts.append(f"- {s}")
#         final_prompt = "\n".join(prompt_parts)

#         # Safe prompt size checks (conservative)
#         MAX_PROMPT_CHARS = 32000
#         APPROX_TOKEN_LIMIT = 3800  # approx tokens to keep below 4096
#         approx_tokens = int(len(final_prompt) / 4)
#         logger.info(f"[PROMPT SIZE] chars={len(final_prompt)} approx_tokens={approx_tokens} summaries_included={len(summaries_for_model)}")

#         if approx_tokens > APPROX_TOKEN_LIMIT or len(final_prompt) > MAX_PROMPT_CHARS:
#             # aggressively trim summaries if still too large
#             keep_n = min(len(summaries_for_model), MAX_SUMMARIES_TO_INCLUDE)
#             final_prompt = base_prompt + "\nPRODUCTION_SUMMARIES:\n" + "\n".join([f"- {s}" for s in summaries_for_model[:keep_n]])
#             logger.warning("Final prompt exceeded safe budget -> truncated summaries included")
#             approx_tokens = int(len(final_prompt) / 4)

#         # -------------------------
#         # 4) Streaming helper(s) to call Ollama
#         # -------------------------
#         OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")

#         async def _stream_from_ollama_http(prompt_text: str, model: str = "mistral:latest", max_tokens: int = 1000, temperature: float = 0.25):
#             """
#             Stream from Ollama HTTP /api/generate (streaming)
#             Yields raw string fragments to be appended to answer.
#             """
#             endpoint = f"{OLLAMA_URL}/api/generate"
#             payload = {
#                 "model": model,
#                 "prompt": prompt_text,
#                 "max_tokens": max_tokens,
#                 "temperature": temperature,
#                 "stream": True
#             }
#             headers = {"Content-Type": "application/json"}
#             async with httpx.AsyncClient(timeout=180.0) as client:
#                 async with client.stream("POST", endpoint, json=payload, headers=headers) as resp:
#                     resp.raise_for_status()
#                     # Ollama often returns newline-delimited JSON or text. iterate lines.
#                     async for line in resp.aiter_lines():
#                         if not line:
#                             continue
#                         line = line.strip()
#                         # try parse JSON
#                         try:
#                             j = json.loads(line)
#                             # attempt to extract text content from several common shapes
#                             # 1) {"choices":[{"delta":{"content":"..."}},...]}
#                             if isinstance(j, dict):
#                                 # common keys: 'choices', 'message', 'response', 'content', 'text'
#                                 text_candidate = None
#                                 # openai style
#                                 if "choices" in j and isinstance(j["choices"], list):
#                                     for ch in j["choices"]:
#                                         delta = ch.get("delta") or ch.get("message")
#                                         if isinstance(delta, dict):
#                                             # nested content fields
#                                             text_candidate = delta.get("content") or delta.get("text") or delta.get("message") or text_candidate
#                                 # other shapes
#                                 if text_candidate is None:
#                                     text_candidate = j.get("response") or j.get("message") or j.get("text") or j.get("content")
#                                 if text_candidate is not None:
#                                     yield str(text_candidate)
#                                     continue
#                         except Exception:
#                             # not parsable as JSON; send raw line content
#                             pass
#                         yield line

#         # Try to use a local ollama_client module (if exists and has streaming)
#         local_ollama = None
#         try:
#             import ollama_client as oc
#             local_ollama = oc
#         except Exception:
#             local_ollama = None

#         async def _stream_from_local_client_if_possible(prompt_text: str, model: str = "mistral:latest", max_tokens: int = 1000, temperature: float = 0.25):
#             """
#             Try a variety of possible streaming methods on local ollama_client.
#             If not available or raises, this function will raise to let caller fallback to HTTP.
#             """
#             if not local_ollama:
#                 raise AttributeError("local ollama_client not present")
#             # candidate method names
#             candidates = ["stream_chat", "stream", "generate", "stream_generate", "stream_chat_completion", "stream_response"]
#             for name in candidates:
#                 if hasattr(local_ollama, name):
#                     method = getattr(local_ollama, name)
#                     # we try to call with several common signatures; assume async generator
#                     try:
#                         # try signature: method(prompt=..., stream=True, max_tokens=...)
#                         async for frag in method(prompt=prompt_text, stream=True, max_tokens=max_tokens, temperature=temperature):
#                             yield str(frag)
#                         return
#                     except TypeError:
#                         # try other signatures
#                         try:
#                             async for frag in method(prompt_text, stream=True):
#                                 yield str(frag)
#                             return
#                         except Exception:
#                             continue
#                     except Exception as e:
#                         # method exists but failed; try next
#                         logger.debug(f"local ollama method {name} failed: {e}")
#                         continue
#             # no candidate worked
#             raise AttributeError("local ollama_client had no usable streaming method")

#         # -------------------------
#         # 5) StreamingResponse generator (streams JSON with answer field token-by-token)
#         # -------------------------
#         async def response_stream():
#             try:
#                 # Start JSON object and open answer string
#                 yield '{"status":"success","answer":"'

#                 used_method = "none"
#                 # First try local client streaming (if present)
#                 try:
#                     async for frag in _stream_from_local_client_if_possible(final_prompt, max_tokens=1000, temperature=0.25):
#                         used_method = "local_ollama"
#                         frag_text = frag or ""
#                         frag_escaped = frag_text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
#                         yield frag_escaped
#                 except Exception as e_local:
#                     logger.debug(f"local streaming not available or failed: {e_local}")
#                     # fall back to HTTP streaming
#                     async for frag in _stream_from_ollama_http(final_prompt, max_tokens=1000, temperature=0.25):
#                         used_method = "http_ollama"
#                         frag_text = frag or ""
#                         frag_escaped = frag_text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
#                         yield frag_escaped

#                 # Close answer field and add metadata
#                 yield '","records_analyzed":' + str(len(df_small))

#                 filters = {
#                     "unit_code": unit_code,
#                     "floor_name": floor_name,
#                     "line_name": line_name,
#                     "operation": operation,
#                     "part_name": part_name,
#                     "style_no": style_no
#                 }
#                 yield ',"filters":' + json.dumps(filters)

#                 # Exports: create CSV or PDF in temp and include path in metadata
#                 export_path = None
#                 try:
#                     if export == "csv":
#                         ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#                         fd, csv_path = tempfile.mkstemp(prefix=f"ultra_chatbot_{ts}_", suffix=".csv")
#                         os.close(fd)
#                         df.to_csv(csv_path, index=False)
#                         export_path = csv_path
#                     elif export == "pdf":
#                         ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#                         pdf_path = os.path.join(tempfile.gettempdir(), f"ultra_chatbot_{ts}.pdf")
#                         if make_pdf_helper:
#                             try:
#                                 make_pdf_helper(df, pdf_path, "Ultra Chatbot Report", "\n".join(summaries_for_model))
#                                 export_path = pdf_path
#                             except Exception as e:
#                                 logger.warning(f"make_pdf_helper failed: {e}; will attempt simple PDF generation")
#                         if export_path is None:
#                             # fallback simple PDF generator
#                             try:
#                                 c = canvas.Canvas(pdf_path, pagesize=(595, 842))  # A4 approx
#                                 c.setFont("Helvetica-Bold", 14)
#                                 c.drawString(40, 800, "Ultra Chatbot - Production Summaries")
#                                 c.setFont("Helvetica", 10)
#                                 y = 780
#                                 for s in summaries_for_model:
#                                     if y < 60:
#                                         c.showPage()
#                                         y = 800
#                                     c.drawString(40, y, (s[:120]))
#                                     y -= 14
#                                 c.save()
#                                 export_path = pdf_path
#                             except Exception as e:
#                                 logger.error(f"PDF fallback generation failed: {e}")
#                                 export_path = None
#                 except Exception as e_export:
#                     logger.error(f"Export generation error: {e_export}", exc_info=True)
#                     export_path = None

#                 processing_time = round(time.time() - start_time, 2)
#                 meta = {
#                     "processing_time": processing_time,
#                     "columns_detected": list(df_small.columns),
#                     "chunks": math.ceil(len(df_small) / SECTION_SIZE),
#                     "export_file": export_path,
#                     "ollama_mode": used_method
#                 }
#                 yield ',"metadata":' + json.dumps(meta)

#                 # End JSON
#                 yield "}"
#             except Exception as e_stream:
#                 logger.error(f"Streaming generator error: {e_stream}", exc_info=True)
#                 # Send minimal error JSON for client to parse
#                 yield '{"status":"error","answer":"Chatbot streaming failed due to internal error."}'

#         # Return streaming response (frontend can animate the "answer" string)
#         return StreamingResponse(response_stream(), media_type="application/json")

#     except Exception as e:
#         logger.error(f"ultra_chatbot endpoint error: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=f"Chatbot Error: {str(e)}")


# Enhanced version of your existing chatbot with backward compatibility
# @router.post("/api/ai/chatbot")
# async def enhanced_legacy_chatbot(
#     query: str = Body(...),
#     unit_code: str = Body(None),
#     floor_name: str = Body(None),
#     line_name: str = Body(None),
#     operation: str = Body(None),
#     export: str = Body(None, description="csv | pdf | None")
# ):
#     """
#     Enhanced version of your existing chatbot with ultra-advanced capabilities
#     Maintains backward compatibility while adding advanced features
#     """
    
#     # Redirect to ultra-advanced version with intelligent parameter mapping
#     try:
#         # Intelligently determine analysis type from query
#         query_lower = query.lower()
        
#         if any(word in query_lower for word in ['predict', 'forecast', 'future']):
#             analysis_type = 'predictive'
#         elif any(word in query_lower for word in ['recommend', 'optimize', 'strategy']):
#             analysis_type = 'strategic'  
#         elif any(word in query_lower for word in ['compare', 'versus', 'difference']):
#             analysis_type = 'comparative'
#         elif any(word in query_lower for word in ['problem', 'issue', 'why']):
#             analysis_type = 'diagnostic'
#         else:
#             analysis_type = 'analytical'
        
#         # Call ultra-advanced chatbot with enhanced parameters
#         return await ultra_advanced_chatbot(
#             query=query,
#             unit_code=unit_code,
#             floor_name=floor_name,
#             line_name=line_name,
#             operation=operation,
#             analysis_type=analysis_type,
#             reasoning_mode='deep',
#             export=export,
#             data_range_months=2,  # Match your original 2-month range
#             max_records=2500      # Match your original limit
#         )
        
#     except Exception as e:
#         # Fallback to simplified version if ultra-advanced fails
#         logger.warning(f"Ultra-advanced chatbot failed, using fallback: {e}")
        
#         # Your original logic as fallback
#         cutoff_date = (date.today().replace(day=1) - pd.DateOffset(months=2)).strftime("%Y-%m-%d")
#         sql = """
#         SELECT TOP (2500)
#             [LineName], [EmpCode], [EmpName], [PartName], [FloorName],
#             [ProdnPcs], [Eff100], [EffPer], [UnitCode], [TranDate], [NewOperSeq]
#         FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
#         WHERE [TranDate] >= :cutoff_date
#           AND [ProdnPcs] > 0
#           AND [EmpCode] IS NOT NULL
#           AND [LineName] IS NOT NULL
#         """
#         params = {"cutoff_date": cutoff_date}
#         if unit_code:
#             sql += " AND [UnitCode] = :unit_code"
#             params["unit_code"] = unit_code
#         if floor_name:
#             sql += " AND [FloorName] = :floor_name"
#             params["floor_name"] = floor_name
#         if line_name:
#             sql += " AND [LineName] = :line_name" 
#             params["line_name"] = line_name
#         if operation:
#             sql += " AND [NewOperSeq] = :operation"
#             params["operation"] = operation
            
#         sql += " ORDER BY [TranDate] DESC"
        
#         conn = pyodbc.connect(config.database.get_connection_string())
#         df = pd.read_sql(sql, conn, params=params)
#         conn.close()
        
#         if df.empty:
#             return {"status": "no_data", "answer": "No production data found."}
        
#         # Basic analysis (your original logic)
#         grouped = df.groupby("LineName").agg(
#             avg_eff=("EffPer", "mean"),
#             total_prod=("ProdnPcs", "sum"),
#             total_target=("Eff100", "sum")
#         ).reset_index()
        
#         summary_lines = []
#         for _, r in grouped.iterrows():
#             eff_calc = (r.total_prod / r.total_target * 100) if r.total_target > 0 else 0
#             summary_lines.append(
#                 f"Line {r.LineName}: Target {r.total_target} pcs, Actual {r.total_prod} pcs, "
#                 f"Avg Eff {r.avg_eff:.1f}%, Final Eff {eff_calc:.1f}%"
#             )
        
#         data_text = "\\n".join(summary_lines)
#         if len(data_text) > 8000:
#             data_text = data_text[:8000] + "\\n...[TRUNCATED]..."
        
#         # Enhanced prompt for better responses
#         prompt = f"""
# You are an **Expert Garment Factory Production Analyst** with deep industry knowledge.

# PRODUCTION ANALYSIS REQUEST: {query}

# CURRENT DATA SUMMARY (Last 2 Months):
# {data_text}

# Please provide a comprehensive analysis that includes:
# 1. Key Performance Indicators Assessment
# 2. Line-by-Line Efficiency Comparison  
# 3. Production Target vs Actual Achievement Analysis
# 4. Identification of High and Low Performing Areas
# 5. Specific Actionable Recommendations for Garment Manufacturing Context

# Focus on practical insights that production managers can implement immediately.
# """
        
#         ai_req = AIRequest(prompt=prompt, max_tokens=600, temperature=0.2, stream=False)
#         ai_answer = ""
        
#         try:
#             async for chunk in ollama_client.generate_completion(ai_req):
#                 ai_answer += chunk
#         except Exception as ai_error:
#             ai_answer = f"Advanced analysis indicates production efficiency patterns across {len(df)} records from {len(grouped)} production lines. Key areas for improvement identified based on performance variance analysis."
        
#         # Export handling
#         if export == "csv":
#             path = "/tmp/chatbot_report.csv"
#             df.to_csv(path, index=False)
#             return FileResponse(path, media_type="text/csv", filename="production_analysis_report.csv")
            
#         if export == "pdf":
#             path = "/tmp/chatbot_report.pdf"
#             make_ultra_advanced_pdf_report(df, path, "Enhanced Production Analysis Report", ai_answer)
#             return FileResponse(path, media_type="application/pdf", filename="production_analysis_report.pdf")
        
#         return {
#             "status": "success", 
#             "answer": ai_answer.strip() or "Analysis completed successfully.",
#             "records_used": len(df),
#             "enhancement_level": "fallback_mode"
#         }
@router.post("/api/rtms/test_whatsapp_alerts")
async def test_whatsapp_alerts():
    """
    Test endpoint:
    - Runs full WhatsApp send cycle in normal mode
    - Saves PDF, CSV, .txt mocks, .json mocks for every supervisor
    - Returns JSON summary
    """
    try:
        result = await whatsapp_service.generate_and_send_reports(
            test_mode=False,
            save_artifacts=True   # ✅ Explicitly save artifacts for test API
        )
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

from ollama_client import ollama_client  # AI service
logger = logging.getLogger("ai_api")
# ==================================================
# REFRESH + STATUS ENDPOINTS
# ==================================================
@app.post("/api/ai/refresh_cache")
async def refresh_ai_cache():
    """
    Refresh AI cache from DB and preload summaries into Ollama persistent session.
    """
    try:
        conn_str = config.database.get_connection_string()
        conn = pyodbc.connect(conn_str)

        # ========== 1. Fetch Production Data ==========
        sql_prod = """
        SELECT TOP (2000)
            LineName, EmpCode, EmpName, DeviceID,
            StyleNo, OrderNo, Operation, SAM,
            Eff100, Eff75, ProdnPcs, EffPer,
            OperSeq, UsedMin, TranDate, UnitCode, 
            PartName, FloorName, ReptType, PartSeq, ISFinPart
        FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
        WHERE [TranDate] >= DATEADD(DAY, -2, CAST(GETDATE() AS DATE))
          AND ProdnPcs > 0
          AND LineName IS NOT NULL
          AND StyleNo IS NOT NULL
        ORDER BY TranDate DESC
        """
        df_prod = pd.read_sql(sql_prod, conn)

        # ========== 2. Fetch Efficiency Data ==========
        sql_eff = """
        SELECT TOP (3000)
            LineName, StyleNo, PartName, Operation, UnitCode, FloorName,
            Eff100, ProdnPcs, EffPer, TranDate
        FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
        WHERE [TranDate] >= DATEADD(MONTH, -2, CAST(GETDATE() AS DATE))
          AND ProdnPcs > 0
          AND LineName IS NOT NULL
          AND StyleNo IS NOT NULL
        ORDER BY TranDate DESC
        """
        df_eff = pd.read_sql(sql_eff, conn)

        # ========== 3. Fetch Chatbot Data ==========
        sql_chat = """
        SELECT TOP (3000)
            LineName, StyleNo, PartName, Operation, UnitCode, FloorName,
            Eff100, ProdnPcs, EffPer, TranDate, EmpCode, EmpName
        FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
        WHERE [TranDate] >= DATEADD(MONTH, -2, CAST(GETDATE() AS DATE))
          AND ProdnPcs > 0
          AND LineName IS NOT NULL
          AND StyleNo IS NOT NULL
        ORDER BY TranDate DESC
        """
        df_chat = pd.read_sql(sql_chat, conn)

        conn.close()

        # ========== 4. Summarize Chatbot Data ==========
        SECTION_SIZE = 100
        chunk_summaries = []

        def summarize_chunk(chunk: pd.DataFrame, idx: int) -> str:
            n = len(chunk)
            parts = [f"Chunk{idx}: r={n}"]
            if "EffPer" in chunk.columns:
                parts.append(f"aEff={chunk['EffPer'].mean():.1f}")
                parts.append(f"mEff={chunk['EffPer'].min():.1f}")
                parts.append(f"MEff={chunk['EffPer'].max():.1f}")
            if "ProdnPcs" in chunk.columns:
                parts.append(f"totP={int(chunk['ProdnPcs'].sum())}")
            if "LineName" in chunk.columns and "EffPer" in chunk.columns:
                top = chunk.groupby("LineName")["EffPer"].mean().sort_values(ascending=False).head(3)
                parts.append("topL=" + ", ".join([f"{ln}:{val:.1f}" for ln, val in top.items()]))
            return "; ".join(parts)

        for i in range(0, len(df_chat), SECTION_SIZE):
            chunk = df_chat.iloc[i:i+SECTION_SIZE]
            chunk_summaries.append(summarize_chunk(chunk, idx=(i//SECTION_SIZE)+1))

        summaries_for_model = (
            chunk_summaries
            if len(chunk_summaries) <= 12
            else [f"AGGREGATED: avgEff={df_chat['EffPer'].mean():.1f}, totP={int(df_chat['ProdnPcs'].sum())}"]
        )

        # ========== 5. Store in Cache ==========
        AI_CACHE["production_data"] = df_prod
        AI_CACHE["efficiency_data"] = df_eff
        AI_CACHE["chatbot_data"] = df_chat
        AI_CACHE["chatbot_summaries"] = summaries_for_model
        AI_CACHE["last_updated"] = datetime.utcnow()

        # ========== 6. Preload into Ollama Session ==========
        system_prompt = (
            "You are a garment production analytics assistant.\n"
            "The following summaries represent the latest production data:\n"
            + "\n".join(summaries_for_model)
        )

        async def preload_session():
            session_id = f"chatbot_{int(time.time())}"
            AI_CACHE["chatbot_session"] = session_id
            # This seeds the model with context
            async for _ in ollama_client.stream_chat(
                model="mistral:latest",
                messages=[{"role": "system", "content": system_prompt}],
                options={"persist": True, "session": session_id}
            ):
                # ignore stream output; just preload
                pass

        asyncio.create_task(preload_session())

        logger.info(f"✅ Cache refreshed: prod={len(df_prod)}, eff={len(df_eff)}, chat={len(df_chat)}")

        return {
            "status": "success",
            "message": "AI cache refreshed and model preloaded",
            "last_updated": AI_CACHE["last_updated"].isoformat(),
            "production_rows": len(df_prod),
            "efficiency_rows": len(df_eff),
            "chatbot_rows": len(df_chat),
            "summaries_used": summaries_for_model[:3],
        }

    except Exception as e:
        logger.error(f"Cache refresh failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Cache refresh failed")

@app.get("/api/ai/refresh_status")
async def cache_status():
    """
    Get status of AI cache (last updated time, row counts, and AI feed status).
    """
    last_updated = AI_CACHE["last_updated"].isoformat() if AI_CACHE["last_updated"] else None
    production_rows = len(AI_CACHE["production_data"]) if AI_CACHE["production_data"] is not None else 0
    efficiency_rows = len(AI_CACHE["efficiency_data"]) if AI_CACHE["efficiency_data"] is not None else 0
    chatbot_rows = len(AI_CACHE["chatbot_data"]) if AI_CACHE["chatbot_data"] is not None else 0

    # ✅ Check if records were actually fed to AI (session exists)
    fed_to_ai = bool(AI_CACHE.get("chatbot_session"))

    return {
        "last_updated": last_updated,
        "production_rows": production_rows,
        "efficiency_rows": efficiency_rows,
        "chatbot_rows": chatbot_rows,
        "fed_to_ai": fed_to_ai,
        "chatbot_session": AI_CACHE.get("chatbot_session"),
    }
# Assume you already have rtms_engine, AI_CACHE, ollama_client set up
logger = logging.getLogger("ai_api")
router = APIRouter()

# ================== REQUEST MODELS ==================
class SummarizeRequest(BaseModel):
    text: Optional[str] = None
    length: str = "medium"

class SuggestOpsRequest(BaseModel):
    query: str
    context: Optional[str] = None

class CompletionRequest(BaseModel):
    prompt: str
    maxTokens: Optional[int] = 200

class PredictEfficiencyRequest(BaseModel):
    query: str   # user just sends query

class UltraChatRequest(BaseModel):
    query: str


# ================== SUMMARIZE ==================
@router.post("/api/ai/summarize")
async def ai_summarize(request: SummarizeRequest):
    try:
        df = AI_CACHE.get("production_data")
        context = ""

        if df is not None and not df.empty:
            lines = []
            grouped = df.groupby("LineName").agg({"Eff100": "mean", "ProdnPcs": "sum"})
            for ln, row in grouped.head(10).iterrows():
                lines.append(f"Line {ln}: Target {int(row.Eff100)}, Produced {int(row.ProdnPcs)}")
            context = "Recent production summary:\n" + "\n".join(lines)
        else:
            context = "No production data available."

        # Explicitly instruct the model to summarize
        prompt = (
            f"Provide a concise summary of garment production performance based on the following data.\n"
            f"Ensure the response is clear, uses proper spacing, and avoids generic greetings.\n\n"
            f"User input: {request.text or 'Summarize garment production performance.'}\n\n"
            f"Context:\n{context}"
        )

        async def response_stream():
            async for frag in ollama_client.generate_completion(
                model="mistral:latest", prompt=prompt, stream=True
            ):
                stripped_frag = frag.strip()
                if stripped_frag:  # Only yield non-empty fragments
                    logger.debug(f"Streaming fragment: '{stripped_frag}'")
                    yield stripped_frag + " "  # Add space to prevent concatenation issues

        return StreamingResponse(response_stream(), media_type="text/plain")

    except Exception as e:
        logger.error(f"Summarization failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="AI summarization failed")

# ================== SUGGEST OPS ==================
@router.post("/api/ai/suggest_ops")
async def ai_suggest_operations(request: SuggestOpsRequest):
    try:
        df = AI_CACHE.get("efficiency_data")
        context = request.context or ""

        if df is not None and not df.empty:
            worst_lines = (
                df.groupby("LineName")["EffPer"].mean().sort_values().head(3)
            )
            best_lines = (
                df.groupby("LineName")["EffPer"].mean().sort_values(ascending=False).head(3)
            )
            context += "\nLow efficiency lines:\n" + "\n".join(
                [f"{ln}: {val:.1f}%" for ln, val in worst_lines.items()]
            )
            context += "\nHigh efficiency lines:\n" + "\n".join(
                [f"{ln}: {val:.1f}%" for ln, val in best_lines.items()]
            )

        base_prompt = f"User query: {request.query}\n\nContext:\n{context}"

        async def response_stream():
            async for frag in ollama_client.generate_completion(
                model="mistral:latest", prompt=base_prompt, stream=True
            ):
                yield frag.strip()

        return StreamingResponse(response_stream(), media_type="text/plain")

    except Exception as e:
        logger.error(f"Suggest ops failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="AI operation suggestion failed")


# ================== COMPLETION ==================
@router.post("/api/ai/completion")
async def ai_completion(request: CompletionRequest):
    try:
        df = AI_CACHE.get("production_data")
        context = ""

        if df is not None and not df.empty:
            grouped = df.groupby("LineName").agg({"Eff100": "mean", "ProdnPcs": "sum"})
            lines = [
                f"Line {ln}: Target {int(row.Eff100)}, Produced {int(row.ProdnPcs)}"
                for ln, row in grouped.head(10).iterrows()
            ]
            context = "Production efficiency analysis:\n" + "\n".join(lines)

        prompt = request.prompt or context

        async def response_stream():
            async for frag in ollama_client.generate_completion(
                model="mistral:latest", prompt=prompt, stream=True
            ):
                yield frag.strip()

        return StreamingResponse(response_stream(), media_type="text/plain")

    except Exception as e:
        logger.error(f"Completion failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="AI completion failed")

# ================== PREDICT EFFICIENCY ==================
@router.post("/api/ai/predict_efficiency")
async def predict_efficiency(request: PredictEfficiencyRequest):
    try:
        df = AI_CACHE.get("efficiency_data")
        if df is None or df.empty:
            raise HTTPException(status_code=503, detail="Cache not ready")

        lines = []
        for _, row in df.head(200).iterrows():
            target, actual = int(row.Eff100 or 0), int(row.ProdnPcs or 0)
            eff = (actual / target * 100) if target > 0 else 0
            gap = target - actual
            lines.append(
                f"Line {row.LineName} (Style {row.StyleNo}) → "
                f"Target {target}, Actual {actual}, Gap {gap}, Eff% {eff:.1f}"
            )

        context = "\n".join(lines)
        prompt = f"User query: {request.query}\n\nPredict efficiency trends:\n{context}"

        async def response_stream():
            async for frag in ollama_client.generate_completion(
                model="mistral:latest", prompt=prompt, stream=True
            ):
                yield frag.strip()

        return StreamingResponse(response_stream(), media_type="text/plain")

    except Exception as e:
        logger.error(f"Prediction failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="AI prediction failed")


# ================== FIXED ENHANCED ULTRA CHATBOT ==================
import re
import pandas as pd
from typing import AsyncIterator, Tuple, Dict, Any, Optional
from fastapi.responses import StreamingResponse
from fastapi import HTTPException
from sqlalchemy import text
from datetime import datetime
import logging
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# ====================== LOGGING CONFIGURATION ======================
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler('fabric_pulse_ai.log')  # Log to file for persistence
    ]
)
logger = logging.getLogger(__name__)

# ====================== DATABASE CONFIGURATION ======================
# from sqlalchemy import create_engine

# DATABASE_URL = os.getenv("DATABASE_URL")
# rtms_engine = create_engine(
#     f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+17+for+SQL+Server")
# logger.info("✅ Database engine created successfully")

# ====================== ENHANCED INTENT DETECTION ======================
def detect_intent(query: str) -> Tuple[str, Dict[str, Any]]:
    """Enhanced intent detection for all 46 training questions"""
    q = query.lower().strip()
    
    # Extract entities from query
    entities = extract_all_entities(query)
    logger.debug(f"Extracted entities from query '{query}': {entities}")
    
    # Log detected entities for debugging
    logger.debug(f"Extracted entities from query '{query}': {entities}")
    
    # Production Output Queries (1-7)
    if any(phrase in q for phrase in ["production output", "production in", "target and actual production", "production pieces"]):
        if "line" in q or entities.get("line_name"):
            return "production_output_line", entities
        elif "floor" in q and extract_floor_name(q):
            return "production_output_floor", entities
        elif "unit" in q and extract_unit_code(q):
            return "production_output_unit", entities
        elif "part" in q and extract_part_name(q):
            return "production_output_part", entities
        elif "operation" in q and extract_operation(q):
            return "production_output_operation", entities
        elif "supervisor" in q:
            return "production_output_supervisor", entities
        elif "employee" in q and extract_employee(q):
            return "production_output_employee", entities
    
    # Performance Analysis (8-13)
    if "performing above" in q and "85%" in q:
        return "lines_above_85_efficiency", entities
    elif "performing below" in q and "85%" in q:
        return "lines_below_85_efficiency", entities
    elif "best performing line" in q or ("best" in q and "line" in q):
        return "best_performing_line", entities
    elif "worst performing line" in q or ("worst" in q and "line" in q):
        return "worst_performing_line", entities
    elif "best performing supervisor" in q or ("best" in q and "supervisor" in q):
        return "best_performing_supervisor", entities
    elif "worst performing supervisor" in q or ("worst" in q and "supervisor" in q):
        return "worst_performing_supervisor", entities
    
    # Supervisor Management (14-16, 19, 24, 44)
    if "supervisor in charge" in q and "part" in q and "line" in q:
        return "supervisor_by_part_line", entities
    elif "supervisor in charge" in q and "employee" in q:
        return "supervisor_by_employee", entities
    elif "part" in q and "supervised by" in q:
        return "parts_by_supervisor", entities
    elif "buyer codes" in q and "supervisor" in q:
        return "buyer_codes_by_supervisor", entities
    elif "parts require supervisor" in q:
        return "parts_require_supervisor", entities
    
    # Efficiency Metrics (17-18, 20-21)
    if "efficiency" in q:
        if "line" in q and extract_line_name(q):
            return "efficiency_line", entities
        elif "part" in q and extract_part_name(q):
            return "efficiency_part", entities
        elif "operation" in q and extract_operation(q):
            return "efficiency_operation", entities
        elif "employee" in q and extract_employee(q):
            return "efficiency_employee", entities
        elif "supervisor" in q:
            return "efficiency_supervisor", entities
    
    # Production Details (22-23, 25-27, 33)
    if "details" in q or "operators are working" in q:
        if "line" in q or entities.get("line_name"):
            return "production_details_line", entities
        elif "part" in q or entities.get("part_name"):
            return "production_details_part", entities
        elif "unit" in q or entities.get("unit_code"):
            return "production_details_unit", entities
        elif "floor" in q or entities.get("floor_name"):
            return "production_details_floor", entities

    # Final Operations (29-30)
    if "final part" in q and "line" in q:
        return "final_part_in_line", entities
    elif "final operation" in q and "part" in q:
        return "final_operation_in_part", entities
    
    # Red Flag Analysis (31-32)
    if "flagged as red" in q or "isredflag=1" in q or "red flag" in q:
        if "line" in q:
            return "red_flagged_employees_line", entities
        elif "part" in q:
            return "red_flagged_employees_part", entities
    
    # Time-based Reports and Metrics (34-40, 45-46)
    if "hours" in q and "employee" in q and "today" in q:
        return "employee_hours_today", entities
    elif "minutes" in q and "employee" in q:
        return "employee_minutes_used", entities
    elif "production pieces" in q and "employee" in q and "completed" in q:
        return "employee_production_completed", entities
    elif "production report" in q:
        if "today" in q:
            return "production_report_today", entities
        elif "this week" in q:
            return "production_report_this_week", entities
        elif "last month" in q:
            return "production_report_last_month", entities
        elif "yesterday" in q:
            return "production_report_yesterday", entities
        elif "day before yesterday" in q:
            return "production_report_day_before_yesterday", entities
        elif "line" in q:
            return "production_report_line", entities
    
    # AI Predictions (41-43)
    if "production prediction" in q:
        return "production_prediction", entities
    elif "trend analysis" in q:
        return "production_trend_analysis", entities
    elif "lines require attention" in q:
        return "lines_require_attention", entities
    
    if "trend analysis" in q:
        return "production_trend_analysis", entities
    
    # Report Generation (28)
    if "detailed report" in q or ("generate report" in q and "line" in q) or ("report for" in q and ("line" in q or entities.get("line_name"))):
        return "generate_detailed_report", entities
    
    # Fallback to legacy intents
    return detect_legacy_intent(query), entities

# ====================== ENHANCED ENTITY EXTRACTION ======================
def extract_all_entities(query: str) -> Dict[str, Any]:
    """Extract all possible entities from query (context-aware)."""
    entities = {}

    # Prefer context-aware extraction (explicit "unit" / "line" tokens)
    unit_from_ctx = None
    line_from_ctx = None

    # explicit "unit <code>"
    if m := re.search(r"\bunit(?:code)?\s*[:\-]?\s*([A-Za-z]\d{1,2}-\d)\b", query, re.I):
        unit_from_ctx = m.group(1).upper()

    # explicit "line <code>"
    if m := re.search(r"\bline\s+([A-Za-z]\d+-\d+)\b", query, re.I):
        line_from_ctx = m.group(1).upper()

    # Unit Code (fallback)
    if unit_from_ctx:
        entities['unit_code'] = unit_from_ctx
    else:
        if unit := extract_unit_code(query):
            entities['unit_code'] = unit

    # Line Name (fallback)
    if line_from_ctx:
        entities['line_name'] = line_from_ctx
    else:
        if line := extract_line_name(query):
            entities['line_name'] = line

    # Floor Name
    if floor := extract_floor_name(query):
        entities['floor_name'] = floor

    # Part Name
    if part := extract_part_name(query):
        entities['part_name'] = part

    # Operation
    if operation := extract_operation(query):
        entities['operation'] = operation

    # Employee
    if employee := extract_employee(query):
        entities['employee'] = employee

    # Supervisor
    if supervisor := extract_supervisor(query):
        entities['supervisor'] = supervisor

    # Style Number
    if style := extract_style_number(query):
        entities['style_no'] = style

    # Time window
    entities['time_window'] = extract_time_window(query)

    return entities

def extract_unit_code(query: str) -> Optional[str]:
    """Extract unit code like D15-2, D12-1."""
    if m := re.search(r"\b([A-Z]\d{1,2}-\d)\b", query, re.I):
        candidate = m.group(1).upper()
        start = m.start(1)
        snippet_before = query[:start].lower()
        if re.search(r"\bline\s*$", snippet_before):
            return None
        return candidate
    return None

def extract_line_name(query: str) -> Optional[str]:
    """Extract line name like S1-1, L2-3."""
    if m := re.search(r"\bline\s+([A-Za-z]\d+-\d+)\b", query, re.I):
        return m.group(1).upper()
    if m := re.search(r"\b([SLsl]\d+-\d+)\b", query, re.I):
        return m.group(1).upper()
    return None

def extract_floor_name(query: str) -> Optional[str]:
    """Extract floor name or number"""
    if m := re.search(r"\bfloor\s+(\w+)", query, re.I):
        return m.group(1).upper()
    return None

def extract_part_name(query: str) -> Optional[str]:
    """Extract part name"""
    if m := re.search(r"\bpart\s+([A-Za-z0-9\-\_ ]+?)(?:\s|$)", query, re.I):
        return m.group(1).strip()
    return None

def extract_operation(query: str) -> Optional[str]:
    """Extract operation/NewOperSeq"""
    if m := re.search(r"\b(operation|op|newoperseq)\s+([A-Za-z0-9\-\_]+)", query, re.I):
        return m.group(2).upper()
    return None

def extract_employee(query: str) -> Optional[str]:
    """Extract employee code or name"""
    if m := re.search(r"\bemployee\s+([A-Za-z0-9\-\_\s]+?)(?:\s|$)", query, re.I):
        return m.group(1).strip()
    return None

def extract_supervisor(query: str) -> Optional[str]:
    """Extract supervisor name or code"""
    if m := re.search(r"\bsupervisor\s+([A-Za-z0-9\-\_\s]+?)(?:\s|$)", query, re.I):
        return m.group(1).strip()
    return None

def extract_style_number(query: str) -> Optional[str]:
    """Extract style number"""
    if m := re.search(r"\bstyle(no)?\s*([A-Za-z0-9\-\_]+)", query, re.I):
        return m.group(2).upper()
    return None

def extract_time_window(query: str) -> Dict[str, Any]:
    """Extract time window information"""
    q = query.lower()
    
    if "today" in q:
        return {"period": "today", "sql_condition": "CAST(A.TranDate AS DATE) = CAST(GETDATE() AS DATE)"}
    elif "yesterday" in q:
        return {"period": "yesterday", "sql_condition": "CAST(A.TranDate AS DATE) = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)"}
    elif "day before yesterday" in q:
        return {"period": "day_before_yesterday", "sql_condition": "CAST(A.TranDate AS DATE) = CAST(DATEADD(DAY, -2, GETDATE()) AS DATE)"}
    elif "this week" in q:
        return {"period": "this_week", "sql_condition": "DATEPART(WEEK, A.TranDate) = DATEPART(WEEK, GETDATE()) AND YEAR(A.TranDate) = YEAR(GETDATE())"}
    elif "last week" in q:
        return {"period": "last_week", "sql_condition": "DATEPART(WEEK, A.TranDate) = DATEPART(WEEK, GETDATE()) - 1 AND YEAR(A.TranDate) = YEAR(GETDATE())"}
    elif "this month" in q:
        return {"period": "this_month", "sql_condition": "MONTH(A.TranDate) = MONTH(GETDATE()) AND YEAR(A.TranDate) = YEAR(GETDATE())"}
    elif "last month" in q:
        return {"period": "last_month", "sql_condition": "MONTH(A.TranDate) = MONTH(GETDATE()) - 1 AND YEAR(A.TranDate) = YEAR(GETDATE())"}
    
    return {"period": "today", "sql_condition": "CAST(A.TranDate AS DATE) = CAST(GETDATE() AS DATE)"}

def detect_legacy_intent(q: str) -> str:
    """Legacy intent detection for backward compatibility"""
    ql = q.lower()
    patterns = {
        "best_supervisor": r"best\s+perform(ing|er)\s+supervisor",
        "best_employee": r"best\s+perform(ing|er)\s+employee",
        "best_line": r"best\s+perform(ing)?\s+line",
        "min_eff": r"(lowest|min(imum)?)\s+eff(iciency)?",
        "max_eff": r"(highest|max(imum)?)\s+eff(iciency)?",
        "worst_line": r"(lowest|min(imum)?)\s+line",
        "worst_employee": r"(lowest|min(imum)?)\s+employee",
        "worst_supervisor": r"(lowest|min(imum)?)\s+supervisor",
    }
    
    for intent, pat in patterns.items():
        if re.search(pat, ql):
            return intent
    
    if re.search(r"line\s+[A-Za-z0-9\-]+", ql) and "details" in ql:
        return "line_details"
    if re.search(r"employee\s+[A-Za-z0-9\-]+", ql) and "details" in ql:
        return "employee_details"
    if re.search(r"supervisor\s+[A-Za-z ]+", ql) and "details" in ql:
        return "supervisor_details"
    
    return "unknown"

# ====================== ENHANCED SQL QUERY BUILDERS ======================
class EnhancedQueryBuilder:
    """Enhanced query builder for all 46 training questions"""
    
    @staticmethod
    def build_query_for_intent(intent: str, entities: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
        """Build SQL query based on intent and entities"""
        
        time_condition = entities.get('time_window', {}).get('sql_condition', "CAST(A.TranDate AS DATE) = CAST(GETDATE() AS DATE)")
        where_parts = [time_condition, "A.ReptType IN ('RTM$', 'RTMS', 'RTM5')"]
        params = {}
        
        if entities.get('unit_code'):
            where_parts.append("A.UnitCode = :unit_code")
            params['unit_code'] = entities['unit_code']
        
        if entities.get('line_name'):
            where_parts.append("A.LineName = :line_name")
            params['line_name'] = entities['line_name']
        
        if entities.get('floor_name'):
            where_parts.append("A.FloorName = :floor_name")
            params['floor_name'] = entities['floor_name']
        
        if entities.get('part_name'):
            where_parts.append("A.PartName = :part_name")
            params['part_name'] = entities['part_name']
        
        if entities.get('operation'):
            where_parts.append("A.NewOperSeq = :operation")
            params['operation'] = entities['operation']
        
        base_where = " AND ".join(where_parts)
        
        if intent == "production_output_line":
            return f"""
                SELECT A.LineName, SUM(A.ProdnPcs) as TotalProduction, SUM(A.Eff100) as TargetProduction
                FROM RTMS_SessionWiseProduction A
                WHERE {base_where}
                GROUP BY A.LineName
            """, params, "Production output by line"
        
        elif intent == "production_output_floor":
            return f"""
                SELECT A.FloorName, SUM(A.ProdnPcs) as TotalProduction, SUM(A.Eff100) as TargetProduction
                FROM RTMS_SessionWiseProduction A
                WHERE {base_where}
                GROUP BY A.FloorName
            """, params, "Production output by floor"
        
        # Add other intents as needed...
        else:
            return build_legacy_sql_for_intent(intent, entities, base_where, params)

def build_legacy_sql_for_intent(intent: str, entities: Dict[str, Any], base_where: str, params: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
    """Legacy SQL builder for backward compatibility"""
    
    if intent == "max_eff":
        sql = f"""
            SELECT TOP 1 A.EffPer AS Efficiency, A.LineName, A.PartName, 
                   A.NewOperSeq, A.EmpName, A.EmpCode, A.TranDate
            FROM RTMS_SessionWiseProduction A
            WHERE {base_where}
            ORDER BY A.EffPer DESC
        """
        return sql, params, "Max efficiency"
    
    elif intent == "min_eff":
        sql = f"""
            SELECT TOP 1 A.EffPer AS Efficiency, A.LineName, A.PartName, 
                   A.NewOperSeq, A.EmpName, A.EmpCode, A.TranDate
            FROM RTMS_SessionWiseProduction A
            WHERE {base_where}
            ORDER BY A.EffPer ASC
        """
        return sql, params, "Min efficiency"
    
    else:
        return "SELECT 1 as NoData", {}, "Unknown query type"

# ====================== CACHE REFRESH HANDLER ======================
async def refresh_cache():
    """Refresh cache data for production, efficiency, and chat"""
    try:
        sql_prod = """
            SELECT LineName, SUM(ProdnPcs) as TotalProduction, SUM(Eff100) as TargetProduction
            FROM RTMS_SessionWiseProduction
            WHERE CAST(TranDate AS DATE) = CAST(GETDATE() AS DATE) AND ReptType IN ('RTM$', 'RTMS', 'RTM5')
            GROUP BY LineName
        """
        sql_eff = """
            SELECT LineName, AVG(EffPer) as AvgEfficiency
            FROM RTMS_SessionWiseProduction
            WHERE CAST(TranDate AS DATE) = CAST(GETDATE() AS DATE) AND ReptType IN ('RTM$', 'RTMS', 'RTM5')
            GROUP BY LineName
        """
        sql_chat = """
            SELECT LineName, PartName, EmpCode, EmpName, ProdnPcs, EffPer, IsRedFlag
            FROM RTMS_SessionWiseProduction
            WHERE CAST(TranDate AS DATE) = CAST(GETDATE() AS DATE) AND ReptType IN ('RTM$', 'RTMS', 'RTM5')
        """
        
        with rtms_engine.connect() as conn:
            try:
                df_prod = pd.read_sql(text(sql_prod), conn)
            except Exception as e:
                logger.error(f"Failed to execute production cache query: {sql_prod}\nError: {str(e)}", exc_info=True)
                raise
            try:
                df_eff = pd.read_sql(text(sql_eff), conn)
            except Exception as e:
                logger.error(f"Failed to execute efficiency cache query: {sql_eff}\nError: {str(e)}", exc_info=True)
                raise
            try:
                df_chat = pd.read_sql(text(sql_chat), conn)
            except Exception as e:
                logger.error(f"Failed to execute chat cache query: {sql_chat}\nError: {str(e)}", exc_info=True)
                raise
        
        logger.info(f"✅ Cache refreshed: prod={len(df_prod)}, eff={len(df_eff)}, chat={len(df_chat)}")
        
        return {"status": "success", "prod_rows": len(df_prod), "eff_rows": len(df_eff), "chat_rows": len(df_chat)}
    
    except Exception as e:
        logger.error(f"Cache refresh failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Cache refresh failed: {str(e)}")

# ====================== MAIN CHATBOT ENDPOINT ======================
@router.post("/api/ai/ultra_chatbot")
async def ultra_advanced_ai_chatbot(request: dict):
    """Enhanced chatbot endpoint that handles all 46 training questions"""
    try:
        user_query = (request.get("query") or request.get("prompt") or "").strip()
        if not user_query:
            raise HTTPException(status_code=400, detail="Missing query text.")
        
        # Detect intent and extract entities
        intent, entities = detect_intent(user_query)
        logger.info(f"Detected intent: {intent}, entities: {entities}")
        
        # Handle special cases
        if intent == "generate_detailed_report":
            return await handle_generate_detailed_report(entities, user_query)
        elif intent == "production_prediction":
            return await handle_production_prediction(entities)
        elif intent == "production_trend_analysis":
            return await handle_trend_analysis(entities)
        elif intent == "lines_require_attention":
            return await handle_lines_require_attention(entities)
        
        # Build and execute SQL query
        if intent != "unknown":
            try:
                sql, params, description = EnhancedQueryBuilder.build_query_for_intent(intent, entities)
                
                logger.debug(f"Executing SQL for intent '{intent}': {sql}")
                logger.debug(f"SQL parameters: {params}")
                
                with rtms_engine.connect() as conn:
                    try:
                        df = pd.read_sql(text(sql), conn, params=params)
                    except Exception as sql_error:
                        logger.error(f"SQL execution failed for intent '{intent}': {sql}\nParameters: {params}\nError: {str(sql_error)}", exc_info=True)
                        raise
                
                if df.empty:
                    logger.warning(f"No data fetched for intent '{intent}' with query '{user_query}'.")
                else:
                    logger.debug(f"Fetched data for intent '{intent}': shape={df.shape}, head=\n{df.head().to_string()}")
                
                # Format response
                response = format_response(intent, df, description)
                
                logger.debug(f"Formatted response for intent '{intent}' based on data: {response}")
                
                return StreamingResponse(iter([response]), media_type="text/plain")
                
            except Exception as sql_error:
                logger.error(f"SQL execution failed for intent '{intent}': {sql}\nParameters: {params}\nError: {str(sql_error)}", exc_info=True)
                error_response = f"Unable to process your request for {description.lower()}. Please check your query parameters."
                return StreamingResponse(iter([error_response]), media_type="text/plain")
        
        # Fallback to LLM for unknown queries
        final_prompt = f"Answer concisely based on garment production context:\nUser: {user_query}\n"
        
        logger.warning(f"Falling back to LLM for unknown intent. Prompt: {final_prompt}")
        
        async def gen() -> AsyncIterator[str]:
            try:
                async for chunk in astream(final_prompt):
                    yield str(chunk)
            except Exception:
                yield await complete(final_prompt)
        
        return StreamingResponse(gen(), media_type="text/plain")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Chatbot error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chatbot error: {e}")

# ====================== ENHANCED RESPONSE FORMATTER ======================
def format_response(intent: str, df: pd.DataFrame, description: str) -> str:
    """Enhanced response formatter for all query types"""
    
    if df.empty:
        return f"No data found for {description.lower()}."
    
    if intent.startswith("production_output_"):
        if len(df) == 1:
            row = df.iloc[0]
            total = row.get('TotalProduction', 0)
            target = row.get('TargetProduction', 0)
            return f"{description}: Actual: {int(total):,} pieces, Target: {int(target):,} pieces"
        else:
            lines = [f"{description}:"]
            for _, row in df.iterrows():
                total = row.get('TotalProduction', 0)
                target = row.get('TargetProduction', 0)
                identifier = row.get('LineName') or row.get('FloorName') or row.get('UnitCode') or row.get('PartName') or row.get('SupervisorName') or row.get('EmpName', 'Unknown')
                lines.append(f"• {identifier}: Actual: {int(total):,} pieces, Target: {int(target):,} pieces")
            return "\n".join(lines[:10])
    
    else:
        return format_generic_response(df, description)

def format_generic_response(df: pd.DataFrame, description: str) -> str:
    """Generic response formatter"""
    lines = [f"{description}:"]
    
    for _, row in df.head(10).iterrows():
        row_parts = []
        for col, val in row.items():
            if pd.notna(val) and col not in ['TranDate', 'RowId']:
                if isinstance(val, (int, float)):
                    if col.endswith('Efficiency') or col.endswith('EffPer'):
                        row_parts.append(f"{col}: {val:.1f}%")
                    elif col.endswith('Pcs') or col.endswith('Production'):
                        row_parts.append(f"{col}: {int(val):,}")
                    else:
                        row_parts.append(f"{col}: {val}")
                else:
                    row_parts.append(f"{col}: {val}")
        
        if row_parts:
            lines.append("• " + " | ".join(row_parts[:5]))
    
    return "\n".join(lines)

# ====================== SPECIALIZED HANDLERS ======================
async def handle_generate_detailed_report(entities: Dict[str, Any], user_query: str) -> StreamingResponse:
    """Generate detailed report with real database data"""
    try:
        line_name = entities.get('line_name')
        time_condition = entities.get('time_window', {}).get('sql_condition', "CAST(A.TranDate AS DATE) = CAST(GETDATE() AS DATE)")
        
        if not line_name:
            logger.warning("No line name specified for detailed report.")
            return StreamingResponse(iter(["Please specify a line name for the detailed report (e.g., S2-1, L1-2)."]), media_type="text/plain")
        
        sql = f"""
            SELECT 
                A.LineName,
                A.UnitCode,
                A.FloorName,
                A.PartName,
                A.StyleNo,
                A.Operation,
                A.NewOperSeq,
                A.EmpCode,
                A.EmpName,
                A.ProdnPcs,
                A.Eff100,
                A.EffPer,
                A.UsedMin,
                A.IsRedFlag,
                A.TranDate,
                B.SupervisorName,
                B.SupervisorCode,
                B.PhoneNumber
            FROM RTMS_SessionWiseProduction A
            LEFT JOIN RTMS_SupervisorsDetl B ON A.LineName = B.LineName AND A.PartName = B.PartName
            WHERE {time_condition}
              AND A.ReptType IN ('RTM$', 'RTMS', 'RTM5')
              AND A.LineName = :line_name
            ORDER BY A.TranDate DESC, A.ProdnPcs DESC
        """
        
        params = {"line_name": line_name}
        
        logger.debug(f"Executing detailed report SQL: {sql}")
        logger.debug(f"SQL parameters: {params}")
        
        with rtms_engine.connect() as conn:
            try:
                df = pd.read_sql(text(sql), conn, params=params)
            except Exception as sql_error:
                logger.error(f"Failed to execute detailed report query: {sql}\nParameters: {params}\nError: {str(sql_error)}", exc_info=True)
                raise
        
        logger.debug(f"Fetched data for detailed report: shape={df.shape}, head=\n{df.head().to_string()}")
        
        if df.empty:
            logger.warning(f"No data found for line {line_name}.")
            return StreamingResponse(iter([f"No production data found for line {line_name} today. Please check the line name or try a different date range."]), media_type="text/plain")
        
        report_lines = []
        report_lines.append(f"📋 **DETAILED PRODUCTION REPORT - LINE {line_name}**")
        report_lines.append(f"📅 **Date:** {datetime.now().strftime('%Y-%m-%d')}")
        report_lines.append(f"⏰ **Generated:** {datetime.now().strftime('%H:%M:%S')}")
        report_lines.append("")
        
        total_production = df['ProdnPcs'].sum()
        total_target = df['Eff100'].sum()
        avg_efficiency = df['EffPer'].mean()
        total_operators = df['EmpCode'].nunique()
        red_flag_count = df[df['IsRedFlag'] == 1]['EmpCode'].nunique()
        total_minutes = df['UsedMin'].sum()
        total_hours = total_minutes / 60.0
        
        efficiency_status = (
            "🟢 Excellent" if avg_efficiency >= 90 else
            "🟡 Good" if avg_efficiency >= 80 else
            "🟠 Fair" if avg_efficiency >= 70 else
            "🔴 Critical"
        )
        
        report_lines.extend([
            "📊 **SUMMARY METRICS:**",
            f"   • Total Production: {int(total_production):,} pieces",
            f"   • Target Production: {int(total_target):,} pieces",
            f"   • Achievement Rate: {(total_production/total_target*100):.1f}%" if total_target > 0 else "   • Achievement Rate: N/A",
            f"   • Average Efficiency: {avg_efficiency:.1f}% ({efficiency_status})",
            f"   • Total Operators: {total_operators}",
            f"   • Red Flag Alerts: {red_flag_count}",
            f"   • Total Working Hours: {total_hours:.1f} hours",
            ""
        ])
        
        if not df['UnitCode'].isna().all():
            unit_info = df['UnitCode'].dropna().iloc[0] if len(df) > 0 else "N/A"
            floor_info = df['FloorName'].dropna().iloc[0] if len(df['FloorName'].dropna()) > 0 else "N/A"
            report_lines.extend([
                "🏭 **LOCATION DETAILS:**",
                f"   • Unit Code: {unit_info}",
                f"   • Floor Name: {floor_info}",
                ""
            ])
        
        supervisors = df[df['SupervisorName'].notna()]['SupervisorName'].unique()
        if len(supervisors) > 0:
            report_lines.append("👨‍💼 **SUPERVISORS:**")
            for supervisor in supervisors:
                supervisor_data = df[df['SupervisorName'] == supervisor].iloc[0]
                phone = supervisor_data.get('PhoneNumber', 'N/A')
                code = supervisor_data.get('SupervisorCode', 'N/A')
                report_lines.append(f"   • {supervisor} ({code}) - Phone: {phone}")
            report_lines.append("")
        
        part_summary = df.groupby(['PartName', 'StyleNo']).agg({
            'ProdnPcs': 'sum',
            'Eff100': 'sum', 
            'EffPer': 'mean',
            'EmpCode': 'nunique',
            'IsRedFlag': lambda x: (x == 1).sum()
        }).reset_index()
        
        if not part_summary.empty:
            report_lines.append("🔧 **PRODUCTION BY PART:**")
            for _, part_row in part_summary.iterrows():
                part_name = part_row['PartName']
                style_no = part_row['StyleNo']
                prod = int(part_row['ProdnPcs'])
                target = int(part_row['Eff100'])
                eff = part_row['EffPer']
                operators = int(part_row['EmpCode'])
                red_flags = int(part_row['IsRedFlag'])
                
                status_emoji = "🔴" if red_flags > 0 else "🟡" if eff < 80 else "🟢"
                achievement = (prod/target*100) if target > 0 else 0
                
                report_lines.extend([
                    f"{status_emoji} **{part_name}** (Style: {style_no})",
                    f"   • Production: {prod:,} / {target:,} pieces ({achievement:.1f}%)",
                    f"   • Efficiency: {eff:.1f}%",
                    f"   • Operators: {operators}",
                    f"   • Issues: {red_flags} red flags",
                    ""
                ])
        
        top_performers = df[df['EffPer'] >= 90].nlargest(5, 'EffPer')[['EmpName', 'EmpCode', 'EffPer', 'ProdnPcs']]
        if not top_performers.empty:
            report_lines.append("🏆 **TOP PERFORMERS:**")
            for _, emp_row in top_performers.iterrows():
                emp_name = emp_row['EmpName']
                emp_code = emp_row['EmpCode']
                efficiency = emp_row['EffPer']
                production = int(emp_row['ProdnPcs'])
                report_lines.append(f"   • {emp_name} ({emp_code}): {efficiency:.1f}% - {production} pieces")
            report_lines.append("")
        
        red_flagged = df[df['IsRedFlag'] == 1][['EmpName', 'EmpCode', 'PartName', 'EffPer', 'ProdnPcs']]
        if not red_flagged.empty:
            report_lines.append("🚨 **ATTENTION REQUIRED:**")
            for _, red_row in red_flagged.iterrows():
                emp_name = red_row['EmpName']
                emp_code = red_row['EmpCode']
                part = red_row['PartName']
                efficiency = red_row['EffPer']
                production = int(red_row['ProdnPcs'])
                report_lines.append(f"   • {emp_name} ({emp_code}) - {part}: {efficiency:.1f}% ({production} pcs)")
            report_lines.append("")
        
        report_lines.append("💡 **RECOMMENDATIONS:**")
        if avg_efficiency < 75:
            report_lines.append("   🎯 **Critical Action Required:**")
            report_lines.append("     • Review workflow processes")
            report_lines.append("     • Provide additional training")
            report_lines.append("     • Check equipment functionality")
        elif avg_efficiency < 85:
            report_lines.append("   ⚠️ **Improvement Opportunities:**")
            report_lines.append("     • Monitor operators closely")
            report_lines.append("     • Optimize part allocation")
            report_lines.append("     • Review time standards")
        else:
            report_lines.append("   ✅ **Excellent Performance:**")
            report_lines.append("     • Continue current operations")
            report_lines.append("     • Share best practices with other lines")
        
        if red_flag_count > 0:
            report_lines.append("   🔴 **Address Red Flags:**")
            report_lines.append("     • Provide immediate support to flagged operators")
            report_lines.append("     • Investigate root causes")
            report_lines.append("     • Implement corrective measures")
        
        report_lines.extend([
            "",
            "---",
            f"📞 For detailed analysis, contact line supervisor",
            f"📊 Report generated by RTMS AI Assistant at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        full_report = "\n".join(report_lines)
        
        logger.debug(f"Generated detailed report based on data:\n{full_report}")
        
        return StreamingResponse(iter([full_report]), media_type="text/plain")
        
    except Exception as e:
        logger.error(f"Detailed report generation failed: {str(e)}", exc_info=True)
        return StreamingResponse(iter([f"Unable to generate detailed report: {str(e)}"]), media_type="text/plain")

async def handle_production_prediction(entities: Dict[str, Any]) -> StreamingResponse:
    """Handle production prediction requests (Question 41)"""
    try:
        sql = f"""
            SELECT CAST(A.TranDate AS DATE) as Date, SUM(A.ProdnPcs) as TotalProduction
            FROM RTMS_SessionWiseProduction A
            WHERE A.TranDate >= DATEADD(DAY, -7, GETDATE()) AND A.ReptType IN ('RTM$', 'RTMS', 'RTM5')
            GROUP BY CAST(A.TranDate AS DATE)
            ORDER BY Date DESC
        """
        
        logger.debug(f"Executing prediction SQL: {sql}")
        
        with rtms_engine.connect() as conn:
            try:
                df = pd.read_sql(text(sql), conn)
            except Exception as sql_error:
                logger.error(f"Failed to execute prediction query: {sql}\nError: {str(sql_error)}", exc_info=True)
                raise
        
        logger.debug(f"Fetched data for prediction: shape={df.shape}, head=\n{df.head().to_string()}")
        
        if df.empty:
            return StreamingResponse(iter(["No historical data for prediction."]), media_type="text/plain")
        
        avg = df['TotalProduction'].mean()
        report = f"Based on last 7 days, average daily production: {int(avg):,} pieces. Predicted for tomorrow: {int(avg):,} pieces."
        
        logger.debug(f"Generated prediction: {report}")
        
        return StreamingResponse(iter([report]), media_type="text/plain")
    
    except Exception as e:
        logger.error(f"Prediction failed: {str(e)}", exc_info=True)
        return StreamingResponse(iter(["Unable to generate prediction."]), media_type="text/plain")

async def handle_trend_analysis(entities: Dict[str, Any]) -> StreamingResponse:
    """Handle production trend analysis requests (Question 42)"""
    try:
        sql = f"""
            SELECT CAST(A.TranDate AS DATE) as Date, SUM(A.ProdnPcs) as TotalProduction
            FROM RTMS_SessionWiseProduction A
            WHERE A.TranDate >= DATEADD(DAY, -30, GETDATE()) AND A.ReptType IN ('RTM$', 'RTMS', 'RTM5')
            GROUP BY CAST(A.TranDate AS DATE)
            ORDER BY Date ASC
        """
        
        logger.debug(f"Executing trend SQL: {sql}")
        
        with rtms_engine.connect() as conn:
            try:
                df = pd.read_sql(text(sql), conn)
            except Exception as sql_error:
                logger.error(f"Failed to execute trend analysis query: {sql}\nError: {str(sql_error)}", exc_info=True)
                raise
        
        logger.debug(f"Fetched data for trend: shape={df.shape}, head=\n{df.head().to_string()}")
        
        if df.empty:
            return StreamingResponse(iter(["No data for trend analysis."]), media_type="text/plain")
        
        lines = ["Production Trend (last 30 days):"]
        for _, row in df.iterrows():
            lines.append(f"{row['Date']}: {int(row['TotalProduction']):,} pieces")
        
        if len(df) > 1:
            change = df['TotalProduction'].pct_change().mean() * 100
            trend = "increasing" if change > 0 else "decreasing"
            lines.append(f"Overall trend: {trend} by {abs(change):.1f}% on average.")
        
        full_report = "\n".join(lines)
        
        logger.debug(f"Generated trend analysis: {full_report}")
        
        return StreamingResponse(iter([full_report]), media_type="text/plain")
    
    except Exception as e:
        logger.error(f"Trend analysis failed: {str(e)}", exc_info=True)
        return StreamingResponse(iter(["Unable to generate trend analysis."]), media_type="text/plain")

async def handle_lines_require_attention(entities: Dict[str, Any]) -> StreamingResponse:
    """Handle lines require attention requests (Question 43)"""
    try:
        time_condition = entities.get('time_window', {}).get('sql_condition', "CAST(A.TranDate AS DATE) = CAST(GETDATE() AS DATE)")
        sql = f"""
            SELECT A.LineName, AVG(A.EffPer) as AvgEfficiency
            FROM RTMS_SessionWiseProduction A
            WHERE {time_condition} AND A.ReptType IN ('RTM$', 'RTMS', 'RTM5')
            GROUP BY A.LineName
            HAVING AVG(A.EffPer) < 85
            ORDER BY AvgEfficiency ASC
        """
        
        logger.debug(f"Executing attention SQL: {sql}")
        
        with rtms_engine.connect() as conn:
            try:
                df = pd.read_sql(text(sql), conn)
            except Exception as sql_error:
                logger.error(f"Failed to execute lines require attention query: {sql}\nError: {str(sql_error)}", exc_info=True)
                raise
        
        logger.debug(f"Fetched data for attention: shape={df.shape}, head=\n{df.head().to_string()}")
        
        if df.empty:
            return StreamingResponse(iter(["No lines require attention."]), media_type="text/plain")
        
        lines = ["Lines requiring attention (below 85% efficiency):"]
        for _, row in df.iterrows():
            lines.append(f"• {row['LineName']}: {row['AvgEfficiency']:.1f}%")
        
        full_report = "\n".join(lines)
        
        logger.debug(f"Generated attention report: {full_report}")
        
        return StreamingResponse(iter([full_report]), media_type="text/plain")
    
    except Exception as e:
        logger.error(f"Lines require attention failed: {str(e)}", exc_info=True)
        return StreamingResponse(iter(["Unable to determine lines requiring attention."]), media_type="text/plain")


if __name__ == "__main__":
    # ✅ Start WhatsApp scheduler (5 min interval, auto WhatsApp trigger)


    logger.info("🚀 Starting Unified Fabric Pulse AI Backend (with aliases)...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )