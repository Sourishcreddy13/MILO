"""
Code Quality Improvements for M.I.L.O

This file contains examples of improved patterns and fixes for common issues
"""

# 1. IMPROVED ERROR HANDLING
import logging
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from fastapi import HTTPException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MILOException(Exception):
    """Base exception for MILO application"""
    def __init__(self, message: str, error_code: str = "GENERAL_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

class DatabaseException(MILOException):
    """Database-related exceptions"""
    def __init__(self, message: str):
        super().__init__(message, "DATABASE_ERROR")

class AIServiceException(MILOException):
    """AI service-related exceptions"""
    def __init__(self, message: str):
        super().__init__(message, "AI_SERVICE_ERROR")

# 2. IMPROVED ASYNC PATTERNS
@asynccontextmanager
async def get_db_connection(pool):
    """Context manager for database connections"""
    conn = None
    try:
        conn = pool.get_connection()
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise DatabaseException(f"Database connection failed: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

# 3. PAGINATION FOR LARGE DATASETS
class PaginationParams:
    def __init__(self, page: int = 1, size: int = 100, max_size: int = 1000):
        self.page = max(1, page)
        self.size = min(max(1, size), max_size)
        self.offset = (self.page - 1) * self.size

def paginate_results(results: list, pagination: PaginationParams) -> Dict[str, Any]:
    """Paginate query results"""
    total = len(results)
    paginated_results = results[pagination.offset:pagination.offset + pagination.size]
    
    return {
        "data": paginated_results,
        "pagination": {
            "page": pagination.page,
            "size": pagination.size,
            "total": total,
            "pages": (total + pagination.size - 1) // pagination.size
        }
    }

# 4. IMPROVED SQL GENERATION VALIDATION
import re
from typing import List

class SQLValidator:
    """Validates generated SQL queries for safety"""
    
    ALLOWED_KEYWORDS = {
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER',
        'ON', 'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'AS',
        'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL',
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT'
    }
    
    DANGEROUS_KEYWORDS = {
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'TRUNCATE', 'ALTER',
        'CREATE', 'EXEC', 'EXECUTE', 'GRANT', 'REVOKE'
    }
    
    @classmethod
    def validate_sql(cls, sql: str) -> tuple[bool, str]:
        """Validate SQL query for safety"""
        sql_upper = sql.upper().strip()
        
        # Check for dangerous keywords
        words = re.findall(r'\b\w+\b', sql_upper)
        for word in words:
            if word in cls.DANGEROUS_KEYWORDS:
                return False, f"Dangerous keyword detected: {word}"
        
        # Must start with SELECT
        if not sql_upper.startswith('SELECT'):
            return False, "Query must start with SELECT"
        
        # Check for multiple statements (semicolon followed by more content)
        if ';' in sql and sql.strip().rstrip(';').count(';') > 0:
            return False, "Multiple statements not allowed"
        
        return True, "Valid"

# 5. IMPROVED CACHING STRATEGY
import json
import hashlib
from datetime import datetime, timedelta

class CacheManager:
    """Enhanced caching with expiration and versioning"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.default_ttl = 3600  # 1 hour
    
    def _generate_cache_key(self, prefix: str, *args) -> str:
        """Generate consistent cache key"""
        key_data = f"{prefix}:{':'.join(map(str, args))}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    async def get_cached_result(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached result with metadata"""
        try:
            cached = self.redis.get(key)
            if cached:
                data = json.loads(cached)
                if data.get('expires_at') and datetime.fromisoformat(data['expires_at']) > datetime.utcnow():
                    return data.get('result')
        except Exception as e:
            logger.warning(f"Cache get error: {e}")
        return None
    
    async def set_cached_result(self, key: str, result: Any, ttl: Optional[int] = None) -> None:
        """Set cached result with metadata"""
        try:
            expires_at = datetime.utcnow() + timedelta(seconds=ttl or self.default_ttl)
            cache_data = {
                'result': result,
                'cached_at': datetime.utcnow().isoformat(),
                'expires_at': expires_at.isoformat()
            }
            self.redis.setex(key, ttl or self.default_ttl, json.dumps(cache_data, default=str))
        except Exception as e:
            logger.warning(f"Cache set error: {e}")

# 6. IMPROVED CONFIGURATION MANAGEMENT
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    host: str
    port: int
    username: str
    password: str
    database: str
    pool_size: int = 10
    pool_name: str = "milo_pool"

@dataclass
class AIConfig:
    api_key: str
    model_name: str = "gemini-1.5-flash-latest"
    temperature: float = 0.1
    max_tokens: int = 4000

@dataclass
class AppConfig:
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: List[str] = None
    session_timeout: int = 86400
    max_request_size: int = 16777216  # 16MB
    
    def __post_init__(self):
        if self.cors_origins is None:
            self.cors_origins = ["http://localhost:3000"]

# Example usage of improved patterns:
"""
# In your main application:

# 1. Use proper error handling
try:
    result = await some_database_operation()
except DatabaseException as e:
    logger.error(f"Database error: {e.message}")
    raise HTTPException(status_code=500, detail=e.message)

# 2. Use context managers for connections
async with get_db_connection(db_pool) as conn:
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()

# 3. Validate SQL before execution
is_valid, message = SQLValidator.validate_sql(generated_sql)
if not is_valid:
    raise HTTPException(status_code=400, detail=f"Invalid SQL: {message}")

# 4. Use caching
cache_key = cache_manager._generate_cache_key("sql_result", user_id, sql_hash)
cached_result = await cache_manager.get_cached_result(cache_key)
if cached_result:
    return cached_result

# Execute query and cache result
result = execute_query(sql)
await cache_manager.set_cached_result(cache_key, result, ttl=1800)
"""