"""
Enhanced Configuration Management for Fabric Pulse AI
Secure credential management with environment variables
"""

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    server: str
    database: str
    username: str
    password: str
    driver: str = 'ODBC Driver 17 for SQL Server'
    timeout: int = 30
    
    def get_connection_string(self) -> str:
        """Build SQL Server connection string"""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"CONNECTION TIMEOUT={self.timeout};"
        )

@dataclass
class TwilioConfig:
    """Twilio WhatsApp configuration"""
    account_sid: str
    auth_token: str
    whatsapp_number: str
    content_sid: str
    alert_phone_number: str
    bot_name: str = "RTMS BOT"
    
    def is_configured(self) -> bool:
        """Check if Twilio is properly configured"""
        return all([
            self.account_sid and self.account_sid != 'your_account_sid',
            self.auth_token and self.auth_token != 'your_auth_token_here',
            self.whatsapp_number,
            self.content_sid,
            self.alert_phone_number
        ])

@dataclass
class AIConfig:
    """AI model configuration"""
    primary_model: str
    use_gpu: bool
    max_length: int
    temperature: float
    cache_dir: Optional[str]

@dataclass
class AlertConfig:
    """Alert system configuration"""
    efficiency_threshold: float
    critical_threshold: float
    max_alerts_per_hour: int
    alert_interval_minutes: int

@dataclass
class ServiceConfig:
    """Service configuration"""
    host: str
    port: int
    log_level: str
    workers: int
    monitoring_interval: int

class FabricPulseConfig:
    """Main configuration class with secure credential management"""
    
    def __init__(self):
        # Database configuration
        self.database = DatabaseConfig(
            server=os.getenv('DB_SERVER', '172.16.9.240'),
            database=os.getenv('DB_DATABASE', 'ITR_PRO_IND'),
            username=os.getenv('DB_USERNAME', 'sa'),
            password=os.getenv('DB_PASSWORD', 'Passw0rd')
        )
        
        # Twilio configuration
        self.twilio = TwilioConfig(
            account_sid=os.getenv('TWILIO_ACCOUNT_SID', ''),
            auth_token=os.getenv('TWILIO_AUTH_TOKEN', ''),
            whatsapp_number=os.getenv('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886'),
            content_sid=os.getenv('TWILIO_CONTENT_SID', ''),
            alert_phone_number=os.getenv('ALERT_PHONE_NUMBER', 'whatsapp:+919943625493')
        )
        
        # AI configuration
        self.ai = AIConfig(
            primary_model=os.getenv('AI_MODEL', 'mistral:latest'),
            use_gpu=os.getenv('AI_USE_GPU', 'false').lower() == 'true',
            max_length=int(os.getenv('AI_MAX_LENGTH', '512')),
            temperature=float(os.getenv('AI_TEMPERATURE', '0.7')),
            cache_dir=os.getenv('AI_CACHE_DIR', './ai_cache')
        )
        
        # Alert configuration
        self.alerts = AlertConfig(
            efficiency_threshold=float(os.getenv('EFFICIENCY_THRESHOLD', '85.0')),
            critical_threshold=float(os.getenv('CRITICAL_THRESHOLD', '70.0')),
            max_alerts_per_hour=int(os.getenv('MAX_ALERTS_PER_HOUR', '20')),
            alert_interval_minutes=int(os.getenv('ALERT_INTERVAL_MINUTES', '5'))
        )
        
        # Service configuration
        self.service = ServiceConfig(
            host=os.getenv('SERVICE_HOST', '0.0.0.0'),
            port=int(os.getenv('SERVICE_PORT', '8000')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            workers=int(os.getenv('WORKERS', '4')),
            monitoring_interval=int(os.getenv('MONITORING_INTERVAL', '10'))
        )
    
    def validate_configuration(self) -> Dict[str, bool]:
        """Validate all configurations"""
        return {
            "database": bool(self.database.server and self.database.database),
            "twilio": self.twilio.is_configured(),
            "ai": bool(self.ai.primary_model),
            "service": bool(self.service.host and self.service.port)
        }

# Global configuration instance
config = FabricPulseConfig()
