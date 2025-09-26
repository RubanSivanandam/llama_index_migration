"""
AI API Routes for Fabric Pulse AI
Enhanced with Professional Reporting Integration
Combines original functionality with new reporting endpoints
"""

import asyncio
import logging
import time
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import os
from collections import defaultdict
import pyodbc
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from config import config
from ollama_client import ollama_client, AIRequest


# Import the WhatsApp service instance
from whatsapp_service import whatsapp_service

logger = logging.getLogger(__name__)

# Configuration
RATE_LIMIT_REQUESTS = 60  # requests per hour per IP
MAX_INPUT_LENGTH = 10000

# In-memory rate limiting
rate_limit_store: Dict[str, Dict] = defaultdict(lambda: {"count": 0, "reset_time": datetime.now()})

# Function to initialize router (to avoid circular import)
def get_router():
    router = APIRouter(prefix="/api/ai", tags=["AI"])
    
    # Database connection
    def get_db_connection():
        """Establish database connection using config"""
        try:
            conn = pyodbc.connect(config.database.get_connection_string())
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise HTTPException(status_code=500, detail="Database connection failed")

    # Request/Response Models
    class SummarizeRequestModel(BaseModel):
        text: str = Field(..., max_length=MAX_INPUT_LENGTH, description="Text to summarize")
        length: str = Field("medium", pattern="^(short|medium|long)$", description="Summary length")

    class SummarizeResponse(BaseModel):
        summary: str
        original_length: int
        processing_time: float

    class OperationSuggestionModel(BaseModel):
        id: str
        label: str
        confidence: float

    class SuggestOperationsRequest(BaseModel):
        context: str = Field(..., max_length=MAX_INPUT_LENGTH)
        query: str = Field(..., max_length=500)

    class SuggestOperationsResponse(BaseModel):
        suggestions: list[OperationSuggestionModel]
        processing_time: float

    class CompletionRequest(BaseModel):
        prompt: str = Field(..., max_length=MAX_INPUT_LENGTH)
        max_tokens: int = Field(500, ge=1, le=2000)
        temperature: float = Field(0.7, ge=0.0, le=2.0)
        stream: bool = Field(False)

    class ProductionOverview(BaseModel):
        total_operators: int
        avg_efficiency: float
        total_production: int
        lines_on_target: int
        alerts_generated: int

    class OperatorData(BaseModel):
        employee_code: str
        employee_name: str
        efficiency: float
        production: int
        target_production: int
        unit_code: str
        line_name: str
        operation: str
        new_oper_seq: str
        floor_name: str

    class LineData(BaseModel):
        line_name: str
        unit_code: str
        total_production: int
        target_production: int
        avg_efficiency: float
        operator_count: int

    # Rate limiting dependency
    async def rate_limit_check(request: Request) -> bool:
        client_ip = request.client.host
        now = datetime.now()
        
        client_data = rate_limit_store[client_ip]
        
        # Reset counter if hour has passed
        if now >= client_data["reset_time"]:
            client_data["count"] = 0
            client_data["reset_time"] = now + timedelta(hours=1)
        
        # Check rate limit
        if client_data["count"] >= RATE_LIMIT_REQUESTS:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Max {RATE_LIMIT_REQUESTS} requests per hour."
            )
        
        client_data["count"] += 1
        return True

    @router.get("/health")
    async def health_check(rate_limited: bool = Depends(rate_limit_check)):
        """Check AI service health"""
        try:
            model_available = await ollama_client.check_model_availability()
            return {
                "status": "healthy" if model_available else "model_unavailable",
                "model": ollama_client.model,
                "base_url": ollama_client.base_url,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    @router.post("/summarize", response_model=SummarizeResponse)
    async def summarize_text(
        request: SummarizeRequestModel,
        rate_limited: bool = Depends(rate_limit_check)
    ):
        """Summarize text with optional flagged employee data using local Ollama model"""
        start_time = time.time()
        
        try:
            # Fetch flagged employees for context
            flagged_employees = await whatsapp_service.fetch_flagged_employees()
            
            # Create context text from flagged employees
            if flagged_employees:
                context_lines = [f"Flagged Employee Analysis for {datetime.now().strftime('%Y-%m-%d %H:%M')}:"]
                
                # Group by line for better organization
                line_groups = {}
                for emp in flagged_employees:
                    line_key = f"{emp.unit_code}_{emp.line_name}"
                    if line_key not in line_groups:
                        line_groups[line_key] = []
                    line_groups[line_key].append(emp)
                
                for line_key, employees in line_groups.items():
                    line_name = employees[0].line_name
                    unit_code = employees[0].unit_code
                    context_lines.append(f"\\nLine: {line_name} (Unit: {unit_code})")
                    
                    for emp in employees:
                        context_lines.append(
                            f"- {emp.emp_name} ({emp.emp_code}): {emp.efficiency_per:.1f}% efficiency, "
                            f"Production: {emp.production_pcs}/{emp.eff100}, "
                            f"Operation: {emp.new_oper_seq}, Style: {emp.style_no}"
                        )
                
                context_text = "\\n".join(context_lines)
            else:
                context_text = "No flagged employees found for today - all production lines performing above target."
            
            # Combine with user-provided text
            if request.text:
                full_text = f"{context_text}\\n\\nAdditional Context: {request.text}"
            else:
                full_text = context_text
            
            # Ensure model is available
            if not await ollama_client.ensure_model_pulled():
                # Provide fallback summary
                fallback_summary = f"Found {len(flagged_employees)} employees below efficiency targets across {len(set(emp.line_name for emp in flagged_employees))} production lines. Focus areas include efficiency improvement and operational support."
                processing_time = time.time() - start_time
                
                return SummarizeResponse(
                    summary=fallback_summary,
                    original_length=len(full_text),
                    processing_time=processing_time
                )
            
            # Create summarize request
            # summarize_req = SummarizeRequest(
            #     text=full_text,
            #     length=request.length
            # )
            
            # Generate AI summary
            summary = await ollama_client.summarize_text()
            processing_time = time.time() - start_time
            
            logger.info(f"Flagged employee summarization completed in {processing_time:.2f}s")
            
            return SummarizeResponse(
                summary=summary,
                original_length=len(full_text),
                processing_time=processing_time
            )
            
        except Exception as e:
            logger.error(f"Summarization failed: {e}")
            raise HTTPException(status_code=500, detail=f"Summarization failed: {str(e)}")

    @router.post("/suggest_ops", response_model=SuggestOperationsResponse)
    async def suggest_operations(
        request: SuggestOperationsRequest,
        rate_limited: bool = Depends(rate_limit_check)
    ):
        """Suggest operations with flagged employee context"""
        start_time = time.time()
        
        try:
            # Fetch flagged employees for context
            flagged_employees = await whatsapp_service.fetch_flagged_employees()
            
            # Create enhanced context
            flagged_context = f"Current flagged employees context: {len(flagged_employees)} employees below target. "
            
            if flagged_employees:
                operations = set()
                lines = set()
                avg_efficiency = sum(emp.efficiency_per for emp in flagged_employees) / len(flagged_employees)
                
                for emp in flagged_employees:
                    operations.add(emp.new_oper_seq)
                    lines.add(emp.line_name)
                
                flagged_context += f"Problem operations: {', '.join(operations)}. "
                flagged_context += f"Affected lines: {', '.join(lines)}. "
                flagged_context += f"Average efficiency: {avg_efficiency:.1f}%. "
            
            enhanced_context = f"{flagged_context} {request.context}"
            
            if not await ollama_client.ensure_model_pulled():
                # Fallback suggestions
                fallback_suggestions = [
                    OperationSuggestionModel(
                        id="training-support",
                        label="Provide Additional Training Support",
                        confidence=0.9
                    ),
                    OperationSuggestionModel(
                        id="equipment-check",
                        label="Check Equipment and Tools",
                        confidence=0.8
                    ),
                    OperationSuggestionModel(
                        id="workflow-optimization",
                        label="Optimize Workflow Process",
                        confidence=0.7
                    ),
                    OperationSuggestionModel(
                        id="quality-coaching",
                        label="Provide Quality and Speed Coaching",
                        confidence=0.8
                    )
                ]
                
                processing_time = time.time() - start_time
                return SuggestOperationsResponse(
                    suggestions=fallback_suggestions,
                    processing_time=processing_time
                )
            
            # Generate AI-powered suggestions
            suggestions = await ollama_client.suggest_operations(
                context=enhanced_context,
                query=request.query
            )
            
            processing_time = time.time() - start_time
            
            suggestion_models = [
                OperationSuggestionModel(
                    id=s.id,
                    label=s.label,
                    confidence=s.confidence
                ) for s in suggestions
            ]
            
            logger.info(f"Enhanced operation suggestions completed in {processing_time:.2f}s")
            
            return SuggestOperationsResponse(
                suggestions=suggestion_models,
                processing_time=processing_time
            )
            
        except Exception as e:
            logger.error(f"Operation suggestions failed: {e}")
            raise HTTPException(status_code=500, detail=f"Operation suggestions failed: {str(e)}")

    @router.post("/completion")
    async def text_completion(
        request: CompletionRequest,
        rate_limited: bool = Depends(rate_limit_check)
    ):
        """Generate text completion with optional streaming"""
        try:
            if not await ollama_client.ensure_model_pulled():
                raise HTTPException(
                    status_code=503,
                    detail="AI model not available. Please ensure Ollama is running."
                )
            
            ai_request = AIRequest(
                prompt=request.prompt,
                max_tokens=request.max_tokens,
                temperature=request.temperature,
                stream=request.stream
            )
            
            if request.stream:
                # Streaming response
                async def generate():
                    try:
                        async for chunk in ollama_client.generate_completion(ai_request):
                            if chunk:
                                yield f"data: {chunk}\n\n"
                        yield "data: [DONE]\n\n"
                    except Exception as e:
                        yield f"data: Error: {str(e)}\n\n"
                
                return StreamingResponse(
                    generate(),
                    media_type="text/plain",
                    headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
                )
            else:
                # Non-streaming response
                result = ""
                async for chunk in ollama_client.generate_completion(ai_request):
                    result += chunk
                
                return {"completion": result, "prompt": request.prompt}
                
        except Exception as e:
            logger.error(f"Text completion failed: {e}")
            raise HTTPException(status_code=500, detail=f"Text completion failed: {str(e)}")

    @router.get("/rtms/overview", response_model=ProductionOverview)
    async def get_production_overview(rate_limited: bool = Depends(rate_limit_check)):
        """Fetch production overview statistics"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                COUNT(DISTINCT EmpCode) as total_operators,
                AVG(EffPer) as avg_efficiency,
                SUM(ProdnPcs) as total_production,
                SUM(CASE WHEN ProdnPcs >= Eff100 THEN 1 ELSE 0 END) as lines_on_target,
                COUNT(CASE WHEN EffPer < ? THEN 1 END) as alerts_generated
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE CAST(TranDate AS DATE) = CAST(GETDATE() AS DATE)
            """
            
            cursor.execute(query, (config.alerts.critical_threshold,))
            result = cursor.fetchone()
            
            response = ProductionOverview(
                total_operators=result[0] or 0,
                avg_efficiency=float(result[1] or 0),
                total_production=result[2] or 0,
                lines_on_target=result[3] or 0,
                alerts_generated=result[4] or 0
            )
            
            cursor.close()
            conn.close()
            
            logger.info(f"Fetched production overview: {response.dict()}")
            return response
            
        except Exception as e:
            logger.error(f"Failed to fetch production overview: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch production overview: {str(e)}")

    @router.get("/rtms/operators", response_model=List[OperatorData])
    async def get_operator_data(rate_limited: bool = Depends(rate_limit_check)):
        """Fetch operator-specific production data"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                pr.EmpCode,
                e.EmpName,
                pr.EffPer,
                pr.ProdnPcs,
                pr.Eff100,
                pr.UnitCode,
                pr.LineName,
                pr.Operation,
                pr.NewOperSeq,
                pr.FloorName
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction] pr
            LEFT JOIN [ITR_PRO_IND].[dbo].[Employees] e ON pr.EmpCode = e.EmpCode
            WHERE CAST(pr.TranDate AS DATE) = CAST(GETDATE() AS DATE)
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            response = [
                OperatorData(
                    employee_code=row[0] or '',
                    employee_name=row[1] or 'UNKNOWN',
                    efficiency=float(row[2]) if row[2] is not None else 0.0,
                    production=int(row[3]) if row[3] is not None else 0,
                    target_production=int(row[4]) if row[4] is not None else 0,
                    unit_code=row[5] or '',
                    line_name=row[6] or '',
                    operation=row[7] or '',
                    new_oper_seq=row[8] or 'UNKNOWN',
                    floor_name=row[9] or 'FLOOR-UNKNOWN'
                ) for row in rows
            ]
            
            cursor.close()
            conn.close()
            
            logger.info(f"Fetched {len(response)} operator records")
            return response
            
        except Exception as e:
            logger.error(f"Failed to fetch operator data: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch operator data: {str(e)}")

    @router.get("/rtms/lines", response_model=List[LineData])
    async def get_line_data(rate_limited: bool = Depends(rate_limit_check)):
        """Fetch line-specific production data"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                LineName,
                UnitCode,
                SUM(ProdnPcs) as total_production,
                SUM(Eff100) as target_production,
                AVG(EffPer) as avg_efficiency,
                COUNT(DISTINCT EmpCode) as operator_count
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE CAST(TranDate AS DATE) = CAST(GETDATE() AS DATE)
            GROUP BY LineName, UnitCode
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            response = [
                LineData(
                    line_name=row[0] or '',
                    unit_code=row[1] or '',
                    total_production=int(row[2]) if row[2] is not None else 0,
                    target_production=int(row[3]) if row[3] is not None else 0,
                    avg_efficiency=float(row[4]) if row[4] is not None else 0.0,
                    operator_count=int(row[5]) if row[5] is not None else 0
                ) for row in rows
            ]
            
            cursor.close()
            conn.close()
            
            logger.info(f"Fetched {len(response)} line records")
            return response
            
        except Exception as e:
            logger.error(f"Failed to fetch line data: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch line data: {str(e)}")

    @router.post("/api/ai/predict_efficiency")
    async def predict_production_efficiency(
        rate_limited: bool = Depends(rate_limit_check)
    ):
        """
        Predict production efficiencies grouped by line and style number
        Uses the same data feed pattern as summarize API
        """
        start_time = time.time()
        try:
            # Fetch production data from database
            conn = get_db_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                LineName,
                StyleNo,
                UnitCode,
                FloorName,
                COUNT(DISTINCT EmpCode) as operator_count,
                AVG(EffPer) as avg_efficiency,
                SUM(ProdnPcs) as total_production,
                SUM(Eff100) as total_target,
                AVG(SAM) as avg_sam
            FROM [ITR_PRO_IND].[dbo].[RTMS_SessionWiseProduction]
            WHERE CAST(TranDate AS DATE) = CAST(GETDATE() AS DATE)
                AND ProdnPcs > 0
                AND LineName IS NOT NULL
                AND StyleNo IS NOT NULL
            GROUP BY LineName, StyleNo, UnitCode, FloorName
            ORDER BY LineName, StyleNo
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Prepare data for prediction analysis
            prediction_data = []
            for row in rows:
                line_data = {
                    "line_name": row[0] or '',
                    "style_no": row[1] or '',
                    "unit_code": row[2] or '',
                    "floor_name": row[3] or '',
                    "operator_count": int(row[4]) if row[4] is not None else 0,
                    "avg_efficiency": float(row[5]) if row[5] is not None else 0.0,
                    "total_production": int(row[6]) if row[6] is not None else 0,
                    "total_target": int(row[7]) if row[7] is not None else 0,
                    "avg_sam": float(row[8]) if row[8] is not None else 0.0
                }
                line_data["efficiency_ratio"] = line_data["avg_efficiency"] / 100.0 if line_data["avg_efficiency"] > 0 else 0.0
                line_data["production_ratio"] = (line_data["total_production"] / line_data["total_target"]) if line_data["total_target"] > 0 else 0.0
                prediction_data.append(line_data)
            
            cursor.close()
            conn.close()
            
            # Generate predictions with enhanced logic
            predictions = []
            for data in prediction_data:
                base_efficiency = data['avg_efficiency']
                
                # Factor in production ratio and operator dynamics
                production_factor = min(1.1, data['production_ratio'] + 0.1)
                operator_factor = max(0.95, 1.0 - (data['operator_count'] * 0.002))
                sam_factor = max(0.98, 1.0 - (data['avg_sam'] * 0.001)) if data['avg_sam'] > 0 else 1.0
                
                predicted_efficiency = min(100.0, base_efficiency * production_factor * operator_factor * sam_factor)
                
                trend = "stable"
                if predicted_efficiency > base_efficiency + 2:
                    trend = "improving"
                elif predicted_efficiency < base_efficiency - 2:
                    trend = "declining"
                
                predictions.append({
                    **data,
                    "predicted_efficiency": round(predicted_efficiency, 1),
                    "efficiency_trend": trend,
                    "confidence": 0.80,
                    "improvement_potential": round(predicted_efficiency - base_efficiency, 1)
                })
            
            processing_time = time.time() - start_time
            
            # Group predictions by line
            grouped_predictions = {}
            for pred in predictions:
                line_name = pred['line_name']
                if line_name not in grouped_predictions:
                    grouped_predictions[line_name] = {
                        "line_name": line_name,
                        "unit_code": pred['unit_code'],
                        "floor_name": pred['floor_name'],
                        "styles": [],
                        "line_summary": {
                            "total_operators": 0,
                            "avg_current_efficiency": 0,
                            "avg_predicted_efficiency": 0,
                            "total_production": 0
                        }
                    }
                
                grouped_predictions[line_name]['styles'].append({
                    "style_no": pred['style_no'],
                    "current_efficiency": pred['avg_efficiency'],
                    "predicted_efficiency": pred['predicted_efficiency'],
                    "efficiency_trend": pred['efficiency_trend'],
                    "improvement_potential": pred['improvement_potential'],
                    "operator_count": pred['operator_count'],
                    "total_production": pred['total_production'],
                    "confidence": pred['confidence']
                })
                
                # Update line summary
                summary = grouped_predictions[line_name]['line_summary']
                summary['total_operators'] += pred['operator_count']
                summary['total_production'] += pred['total_production']
            
            # Calculate line-level averages
            for line_data in grouped_predictions.values():
                styles = line_data['styles']
                if styles:
                    line_data['line_summary']['avg_current_efficiency'] = round(
                        sum(s['current_efficiency'] for s in styles) / len(styles), 1
                    )
                    line_data['line_summary']['avg_predicted_efficiency'] = round(
                        sum(s['predicted_efficiency'] for s in styles) / len(styles), 1
                    )
            
            logger.info(f"Efficiency prediction completed in {processing_time:.2f}s for {len(predictions)} line-style combinations")
            
            return {
                "status": "success",
                "predictions_by_line": grouped_predictions,
                "summary": {
                    "total_lines": len(grouped_predictions),
                    "total_style_combinations": len(predictions),
                    "processing_time": processing_time,
                    "prediction_date": datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Production efficiency prediction failed: {e}")
            raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

    @router.post("/generate_report")
    async def generate_hourly_report(
        test_mode: bool = False,
        rate_limited: bool = Depends(rate_limit_check)
    ):
        """
        Generate and send professional hourly production report to supervisors
        Groups by LineName → StyleNo with branded PDF format
        """
        try:
            result = await whatsapp_service.generate_and_send_reports(test_mode=test_mode)
            return result
            
        except Exception as e:
            logger.error(f"❌ Report generation failed: {e}")
            raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")

    @router.get("/report_status")
    async def get_report_status(rate_limited: bool = Depends(rate_limit_check)):
        """Get status of the enhanced reporting system"""
        return whatsapp_service.get_status()

    @router.post("/start_scheduler")
    async def start_report_scheduler(rate_limited: bool = Depends(rate_limit_check)):
        """Start the hourly report scheduler - generates reports every hour at :00"""
        try:
            whatsapp_service.start_hourly_scheduler()
            return {
                "status": "success",
                "message": "Hourly report scheduler started - reports will be generated every hour at :00 minutes",
                "scheduler_running": True,
                "test_numbers": whatsapp_service.test_numbers
            }
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to start scheduler: {str(e)}")

    @router.post("/stop_scheduler")
    async def stop_report_scheduler(rate_limited: bool = Depends(rate_limit_check)):
        """Stop the hourly report scheduler"""
        try:
            whatsapp_service.stop_hourly_scheduler()
            return {
                "status": "success",
                "message": "Hourly report scheduler stopped",
                "scheduler_running": False
            }
        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to stop scheduler: {str(e)}")

    @router.get("/fetch_flagged_employees")
    async def fetch_flagged_employees_endpoint(rate_limited: bool = Depends(rate_limit_check)):
        """Fetch current flagged employees for debugging/monitoring"""
        try:
            flagged_employees = await whatsapp_service.fetch_flagged_employees()
            
            # Convert to serializable format
            employees_data = []
            for emp in flagged_employees:
                employees_data.append({
                    "emp_name": emp.emp_name,
                    "emp_code": emp.emp_code,
                    "unit_code": emp.unit_code,
                    "floor_name": emp.floor_name,
                    "line_name": emp.line_name,
                    "style_no": emp.style_no,
                    "part_name": emp.part_name,
                    "operation": emp.operation,
                    "new_oper_seq": emp.new_oper_seq,
                    "production_pcs": emp.production_pcs,
                    "eff100": emp.eff100,
                    "efficiency_per": emp.efficiency_per,
                    "is_red_flag": emp.is_red_flag
                })
            
            return {
                "status": "success",
                "flagged_employees": employees_data,
                "total_count": len(employees_data),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch flagged employees: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch flagged employees: {str(e)}")

    logger.info("✅ Enhanced AI routes with professional reporting loaded successfully")
    return router

# Expose the router initialization function
router = get_router()