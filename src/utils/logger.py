import logging
import json
import sys
import numpy as np
from datetime import datetime
from typing import Any, Dict, Optional


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


class StructuredLogger:
    
    def __init__(self, name: str, level: str = "INFO"):
       
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []
        
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(self._get_formatter())
        self.logger.addHandler(handler)
        
    def _get_formatter(self) -> logging.Formatter:
        return JsonFormatter()
    
    def _log(self, level: str, message: str, extra: Optional[Dict[str, Any]] = None):
        
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "logger_name": self.logger.name
        }
        
        if extra:
            log_data.update(extra)
        
        getattr(self.logger, level.lower())(json.dumps(log_data, cls=NumpyEncoder))
    
    def debug(self, message: str, **kwargs):
        self._log("DEBUG", message, kwargs)
    
    def info(self, message: str, **kwargs):
        self._log("INFO", message, kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log("WARNING", message, kwargs)
    
    def error(self, message: str, **kwargs):
        self._log("ERROR", message, kwargs)
    
    def critical(self, message: str, **kwargs):
        self._log("CRITICAL", message, kwargs)
    
    def log_execution_start(self, job_name: str, **kwargs):
        self.info(
            f"Starting execution: {job_name}",
            job_name=job_name,
            event_type="execution_start",
            **kwargs
        )
    
    def log_execution_end(self, job_name: str, status: str, **kwargs):
        self.info(
            f"Completed execution: {job_name} with status: {status}",
            job_name=job_name,
            status=status,
            event_type="execution_end",
            **kwargs
        )
    
    def log_data_quality(self, metric_name: str, value: Any, **kwargs):
        self.info(
            f"Data quality metric: {metric_name}",
            metric_name=metric_name,
            metric_value=value,
            event_type="data_quality",
            **kwargs
        )
    
    def log_partition_write(self, table: str, partition: str, record_count: int):
        self.info(
            f"Wrote partition {partition} to {table}",
            table=table,
            partition=partition,
            record_count=record_count,
            event_type="partition_write"
        )


class JsonFormatter(logging.Formatter):
    
    def format(self, record: logging.LogRecord) -> str:
        try:
            # If the message is already JSON, return as-is
            message = json.loads(record.getMessage())
            return json.dumps(message, default=str)
        except json.JSONDecodeError:
            # If not JSON, create structured log entry
            log_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": record.levelname,
                "message": record.getMessage(),
                "logger_name": record.name,
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno
            }
            
            if record.exc_info:
                log_data["exception"] = self.formatException(record.exc_info)
            
            return json.dumps(log_data, default=str)


def get_logger(name: str, level: str = "INFO") -> StructuredLogger:
   
    return StructuredLogger(name, level)
