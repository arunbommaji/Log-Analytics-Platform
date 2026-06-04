from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import uuid

class LogEntry(BaseModel):
    service: str
    level: str                              # DEBUG | INFO | WARN | ERROR | CRITICAL
    message: str
    host: Optional[str] = "unknown"
    trace_id: Optional[str] = None
    response_time_ms: Optional[int] = None
    status_code: Optional[int] = None
    environment: Optional[str] = "production"
    metadata: Optional[Dict[str, Any]] = {}

class LogEntryEnriched(LogEntry):
    log_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
