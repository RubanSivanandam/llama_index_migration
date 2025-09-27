import logging
import json
import io
import asyncio
import time
import schedule
import threading
import os
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from sqlalchemy import text, create_engine
from urllib.parse import quote_plus

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

try:
    from twilio.rest import Client
    from twilio.base.exceptions import TwilioException
except Exception:
    Client = None
    TwilioException = Exception

# local config
from config import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(ch)

DEFAULT_TEST_NUMBERS = ["+919943625493", "+918939990949", "+919894070745"]
TEMPLATE_SID = "HX059c8f6500786c9f43eda250ef7178e1"  # Twilio template SID


@dataclass
class SupervisorRow:
    supervisor_name: str
    phone_number: str
    unit_code: str
    floor_name: str
    line_name: str
    part_name: str
    prodn_pcs: int
    target_pcs: int
    achv_percent: float


def _make_db2_engine_from_env() -> Optional[any]:
    try:
        db2_server = os.getenv("DB1_SERVER")
        db2_database = os.getenv("DB1_DATABASE")
        db2_username = os.getenv("DB1_USERNAME")
        db2_password = os.getenv("DB1_PASSWORD")
        driver = "ODBC Driver 17 for SQL Server"
        conn_str = (
            f"DRIVER={{{driver}}};SERVER={db2_server};DATABASE={db2_database};"
            f"UID={db2_username};PWD={db2_password};TrustServerCertificate=yes;"
        )
        eng = create_engine("mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str), pool_pre_ping=True)
        return eng
    except Exception as e:
        logger.error(f"❌ Failed creating DB2 engine: {e}")
        return None


class ProductionReadyWhatsAppService:
    def __init__(self):
        self.config = config
        self.reports_dir = Path("reports")
        self.reports_dir.mkdir(exist_ok=True)
        self.mock_dir = self.reports_dir / "mock_messages"
        self.mock_dir.mkdir(exist_ok=True)
        self.temporarily_disabled = False
        self.test_numbers = DEFAULT_TEST_NUMBERS.copy()

        self.twilio_client = None
        try:
            if Client and hasattr(self.config, "twilio") and self.config.twilio.is_configured():
                self.twilio_client = Client(self.config.twilio.account_sid, self.config.twilio.auth_token)
                logger.info("✅ Twilio client initialized")
            else:
                logger.info("⚠️ Twilio not configured or client not available - using mock send")
        except Exception as e:
            logger.warning(f"⚠️ Failed to init Twilio client: {e}")
            self.twilio_client = None

        self.db2_engine = _make_db2_engine_from_env()

    # ----------------------------------------------------------------------
    # Stored Procedure (DB1)
    # ----------------------------------------------------------------------
    def execute_stored_proc(self):
        """Run sync stored procedure before querying DB1"""
        try:
            from fabric_pulse_ai_main import rtms_engine
            if not rtms_engine or not rtms_engine.engine:
                logger.error("❌ DB1 engine not available for stored procedure.")
                return

            query = text("EXEC dbo.usp_Sync_RTMS_SessionWiseProduction @TranDate = :tran_date")
            today = date.today()

            with rtms_engine.engine.begin() as conn:
                conn.execute(query, {"tran_date": today})

            logger.info("✅ Stored procedure executed successfully on DB1.")
        except Exception as e:
            logger.error(f"❌ execute_stored_proc failed: {e}", exc_info=True)

    # ----------------------------------------------------------------------
    # Inline Query (DB1)
    # ----------------------------------------------------------------------
    async def _query_part_efficiencies(self) -> List[SupervisorRow]:
        try:
            from fabric_pulse_ai_main import rtms_engine
            if not rtms_engine or not rtms_engine.engine:
                logger.error("❌ DB1 engine not available for production query")
                return []

            sql = """
            ;WITH OperationDetails AS (
                SELECT
                    A.ReptType,
                    A.UnitCode,
                    A.TranDate,
                    A.FloorName,            
                    A.LineName,
                    B.SupervisorName,
                    B.SupervisorCode,
                    B.PhoneNumber,
                    A.PartName,
                    A.PartSeq,
                    SUM(A.PRODNPCS) AS ProdPcs,
                    COUNT(*) AS NoofOprs
                FROM RTMS_SessionWiseProduction A
                JOIN RTMS_SupervisorsDetl B
                    ON A.LineName = B.LineName
                AND A.PartName = B.PartName
                WHERE
                    A.UnitCode = 'D15-2'
                    AND A.TranDate = CAST(GETDATE() AS DATE)
                    AND A.ReptType = 'RTM$'
                    AND A.ISFinPart = 'Y'
                GROUP BY
                    A.ReptType,
                    A.UnitCode,
                    A.TranDate,
                    A.FloorName,            
                    A.LineName,
                    B.SupervisorName,
                    B.SupervisorCode,
                    B.PhoneNumber,
                    A.PartName,
                    A.PartSeq
            ),
            OperationSummary AS (
                SELECT
                    ReptType,
                    TranDate,
                    LineName,
                    PartSeq,
                    PartName,
                    Operation,
                    SUM(ProdnPcs) AS OperProd,
                    COUNT(DISTINCT EmpCode) AS NoofOperators,
                    ISFinPart
                FROM dbo.RTMS_SessionWiseProduction
                WHERE TranDate = CAST(GETDATE() AS DATE)
                AND ReptType = 'RTM$'
                GROUP BY ReptType, TranDate, LineName, PartSeq, PartName, Operation, ISFinPart
            ),
            MaxProdPerPart AS (
                SELECT
                    ReptType,
                    TranDate,
                    LineName,
                    PartSeq,
                    PartName,
                    ISFinPart,
                    MAX(OperProd) AS MaxProd
                FROM OperationSummary
                GROUP BY ReptType, TranDate, LineName, PartSeq, PartName, ISFinPart
            ),
            LowPerformers AS (
                SELECT
                    a.ReptType,
                    a.TranDate,
                    a.LineName,
                    a.PartSeq,
                    a.PartName,
                    a.Operation,
                    a.OperProd,
                    b.MaxProd,
                    a.NoofOperators,
                    ROUND(b.MaxProd * 0.85, 0) AS TargetPcs,
                    ROUND((a.OperProd * 100.0) / b.MaxProd, 2) AS AchvPercent,
                    a.ISFinPart
                FROM OperationSummary a
                JOIN MaxProdPerPart b
                    ON a.ReptType = b.ReptType
                AND a.TranDate = b.TranDate
                AND a.LineName = b.LineName
                AND a.PartSeq = b.PartSeq
                AND a.PartName = b.PartName
                WHERE a.OperProd < b.MaxProd * 0.85
                AND a.ISFinPart = 'Y'
            ),
            SummaryTable AS (
                SELECT
                    ReptType,
                    TranDate,
                    LineName,
                    PartSeq,
                    PartName,
                    MAX(TargetPcs) AS TargetPcs,
                    MAX(AchvPercent) AS AchvPercent
                FROM LowPerformers
                GROUP BY ReptType, TranDate, LineName, PartSeq, PartName
            )
            SELECT
                OD.*,
                ST.TargetPcs,
                ST.AchvPercent
            FROM OperationDetails OD
            JOIN SummaryTable ST
                ON OD.TranDate = ST.TranDate
            AND OD.LineName = ST.LineName
            AND OD.PartName = ST.PartName
            AND OD.ReptType = ST.ReptType
            ORDER BY OD.LineName, OD.PartSeq;
            """
            with rtms_engine.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)

            rows: List[SupervisorRow] = []
            for _, r in df.iterrows():
                phone = str(r["PhoneNumber"] or "").strip()
                if phone and not phone.startswith("+"):
                    phone = f"+91{phone}"
                rows.append(
                    SupervisorRow(
                        supervisor_name=str(r["SupervisorName"] or "Unknown Supervisor"),
                        phone_number=phone,
                        unit_code=str(r["UnitCode"] or ""),
                        floor_name=str(r["FloorName"] or ""),
                        line_name=str(r["LineName"] or ""),
                        part_name=str(r["PartName"] or ""),
                        prodn_pcs=int(r["ProdPcs"] or 0),
                        target_pcs=int(r["TargetPcs"] or 0),
                        achv_percent=float(r["AchvPercent"] or 0.0),
                    )
                )
            logger.info(f"📊 _query_part_efficiencies fetched {len(rows)} rows")
            return rows
        except Exception as e:
            logger.error(f"❌ _query_part_efficiencies failed: {e}", exc_info=True)
            return []

    # ----------------------------------------------------------------------
    # DB2 Helpers
    # ----------------------------------------------------------------------
    def _db2_query(self, sql: str) -> pd.DataFrame:
        if self.db2_engine is None:
            raise RuntimeError("❌ DB2 engine not configured.")
        with self.db2_engine.connect() as conn:
            return pd.read_sql(text(sql), conn)

    def get_session_code(self) -> Optional[str]:
        try:
            q = """
            SELECT TOP (1) SessionCode
            FROM [ITR_CON].[dbo].[ITR_CON_SessionMasterNew]
            WHERE Unitcode = 'D15-2' AND LineCode = 'All'
              AND CAST((CONVERT(varchar(10), GETDATE(), 111) + ' ' + ToTime) AS datetime) <= GETDATE()
            ORDER BY CAST(ToTime AS time) DESC
            """
            df = self._db2_query(q)
            if df.empty:
                logger.info("ℹ️ No session code found")
                return None
            code = str(df.iloc[0]["SessionCode"])
            logger.info(f"ℹ️ Retrieved session_code={code}")
            return code
        except Exception as e:
            logger.error(f"❌ get_session_code failed: {e}", exc_info=True)
            return None

    # ----------------------------------------------------------------------
    # Message formatting
    # ----------------------------------------------------------------------
    def _format_supervisor_message(self, r: SupervisorRow, session_code: Optional[str]) -> str:
        display_part = "Assembly" if "assembly tops" in r.part_name.lower() else r.part_name
        session_line = f"Upto the session {session_code}" if session_code else "Upto the session"
        return (
            f"Supervisor: {r.supervisor_name}\n"
            f"Part: {display_part} | Location: {r.unit_code} → {r.floor_name or 'FLOOR-?'} → {r.line_name}\n"
            f"Produced: {r.prodn_pcs} pcs / Target: {r.target_pcs} pcs\n"
            f"Efficiency: {round(r.achv_percent, 1)}%\n"
            f"{session_line}\n"
            "Please review the details above!"
        )

    # ----------------------------------------------------------------------
    # Send WhatsApp
    # ----------------------------------------------------------------------
    async def send_whatsapp_report(
        self,
        phone_number: str,
        message: str,
        row: Optional[SupervisorRow] = None,
        session_code: Optional[str] = None,
        save_artifacts: bool = False
    ) -> Dict[str, Any]:
        try:
            logger.info(f"📨 Preparing WhatsApp to {phone_number}")
            if not phone_number.startswith("+"):
                phone_number = f"+91{phone_number}"

            if self.temporarily_disabled or self.twilio_client is None or not (
                hasattr(self.config, "twilio") and self.config.twilio.is_configured()
            ):
                logger.warning(f"⚠️ WhatsApp send mocked for {phone_number}")
                return {"status": "mocked", "to": phone_number, "body": message}

            if row is None:
                raise ValueError("row must be provided when sending template message")

            from_whatsapp = self.config.twilio.whatsapp_number
            to_addr = f"whatsapp:{phone_number}"
            from_addr = from_whatsapp if str(from_whatsapp).startswith("whatsapp:") else f"whatsapp:{from_whatsapp}"

            logger.info(f"➡️ Sending WhatsApp to {phone_number} via Twilio...")
            msg = self.twilio_client.messages.create(
                from_=from_addr,
                to=to_addr,
                content_sid=TEMPLATE_SID,
                content_variables=json.dumps({
                    "1": row.supervisor_name,
                    "2": row.part_name,
                    "3": row.unit_code,
                    "4": row.floor_name or "FLOOR-?",
                    "5": row.line_name,
                    "6": str(row.prodn_pcs),
                    "7": str(row.target_pcs),
                    "8": str(round(row.achv_percent, 1)),
                    "9": session_code or ""
                })
            )
            logger.info(f"✅ WhatsApp sent to {phone_number}, SID={getattr(msg,'sid',None)}")
            return {"status": "sent", "sid": getattr(msg, 'sid', None)}
        except Exception as e:
            logger.error(f"❌ send_whatsapp_report failed: {e}", exc_info=True)
            return {"status": "error", "reason": str(e)}

    # ----------------------------------------------------------------------
    # Scheduler
    # ----------------------------------------------------------------------
    def start_scheduler(self):
        """Start background scheduler to send WhatsApp every hour"""

        def job():
            try:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"[Scheduler] 🚀 Triggered WhatsApp cycle at {now}")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.run_report_cycle())
                loop.close()
                logger.info("[Scheduler] ✅ WhatsApp cycle completed")
            except Exception as e:
                logger.error(f"❌ Scheduler job failed: {e}", exc_info=True)

        schedule.every().hour.do(job)
        # every 1 minute
        # schedule.every(1).minutes.do(job)

        def run_schedule():
            logger.info("✅ WhatsApp scheduler started (every hour)")
            while True:
                schedule.run_pending()
                time.sleep(1)

        threading.Thread(target=run_schedule, daemon=True).start()

    # ----------------------------------------------------------------------
    # Run Report Cycle
    # ----------------------------------------------------------------------
    async def run_report_cycle(self):
        logger.info("🚀 run_report_cycle started")
        self.execute_stored_proc()
        rows = await self._query_part_efficiencies()
        logger.info(f"📊 Processing {len(rows)} rows")
        session_code = self.get_session_code()

        for r in rows:
            msg = self._format_supervisor_message(r, session_code)
            logger.info(f"📝 Prepared message for Supervisor={r.supervisor_name}, Phone={r.phone_number}")

            if r.phone_number:
                logger.info(f"📤 Sending to supervisor {r.phone_number}")
                await self.send_whatsapp_report(r.phone_number, msg, row=r, session_code=session_code)

            for test_num in self.test_numbers:
                logger.info(f"📤 Sending duplicate to test number {test_num}")
                await self.send_whatsapp_report(test_num, msg, row=r, session_code=session_code)

        logger.info("🏁 run_report_cycle completed")


# Export singleton
whatsapp_service = ProductionReadyWhatsAppService()
