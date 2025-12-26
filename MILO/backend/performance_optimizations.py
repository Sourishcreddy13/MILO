"""
Performance Optimization Strategies for M.I.L.O

This file contains recommendations and implementations for improving performance
"""

import asyncio
import asyncpg
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import json

# 1. DATABASE CONNECTION POOLING IMPROVEMENT
class DatabasePool:
    """Enhanced database connection pool with async support"""
    
    def __init__(self, 
                 host: str, 
                 port: int, 
                 database: str, 
                 username: str, 
                 password: str,
                 min_connections: int = 5,
                 max_connections: int = 20):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': username,
            'password': password
        }
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.pool = None
    
    async def initialize(self):
        """Initialize the connection pool"""
        self.pool = await asyncpg.create_pool(
            **self.connection_params,
            min_size=self.min_connections,
            max_size=self.max_connections,
            command_timeout=60
        )
    
    async def execute_query(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute query with automatic connection management"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]

# 2. CACHING STRATEGY WITH REDIS
class SmartCache:
    """Intelligent caching system with automatic invalidation"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.cache_stats = {"hits": 0, "misses": 0}
    
    def _get_cache_key(self, prefix: str, *args) -> str:
        """Generate deterministic cache key"""
        key_string = f"{prefix}:{':'.join(map(str, args))}"
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]
    
    async def get_or_compute(self, 
                           cache_key: str, 
                           compute_func, 
                           ttl: int = 3600,
                           *args, **kwargs):
        """Get from cache or compute and store"""
        try:
            cached_data = await self.redis.get(cache_key)
            if cached_data:
                self.cache_stats["hits"] += 1
                return json.loads(cached_data)
        except Exception:
            pass
        
        # Cache miss - compute value
        self.cache_stats["misses"] += 1
        result = await compute_func(*args, **kwargs)
        
        try:
            await self.redis.setex(
                cache_key, 
                ttl, 
                json.dumps(result, default=str)
            )
        except Exception:
            pass  # Don't fail if cache storage fails
        
        return result

# 3. ASYNC BATCH PROCESSING
class BatchProcessor:
    """Process multiple requests in batches to improve throughput"""
    
    def __init__(self, batch_size: int = 10, timeout: float = 1.0):
        self.batch_size = batch_size
        self.timeout = timeout
        self.pending_requests = []
        self.batch_timer = None
    
    async def add_request(self, request_data: Dict[str, Any]) -> Any:
        """Add request to batch and return future result"""
        future = asyncio.Future()
        self.pending_requests.append({
            "data": request_data,
            "future": future
        })
        
        if len(self.pending_requests) >= self.batch_size:
            await self._process_batch()
        elif self.batch_timer is None:
            self.batch_timer = asyncio.create_task(self._wait_and_process())
        
        return await future
    
    async def _wait_and_process(self):
        """Wait for timeout then process batch"""
        await asyncio.sleep(self.timeout)
        await self._process_batch()
    
    async def _process_batch(self):
        """Process accumulated requests"""
        if not self.pending_requests:
            return
        
        requests = self.pending_requests.copy()
        self.pending_requests.clear()
        
        if self.batch_timer:
            self.batch_timer.cancel()
            self.batch_timer = None
        
        # Process batch (implement your batch logic here)
        results = await self._execute_batch([r["data"] for r in requests])
        
        # Return results to waiting futures
        for request, result in zip(requests, results):
            request["future"].set_result(result)

# 4. SQL QUERY OPTIMIZATION
class QueryOptimizer:
    """Optimize SQL queries for better performance"""
    
    @staticmethod
    def add_query_hints(query: str, table_info: Dict[str, Any]) -> str:
        """Add MySQL query hints based on table structure"""
        optimized = query
        
        # Add USE INDEX hints for common patterns
        if "WHERE" in query.upper() and "ORDER BY" in query.upper():
            # Suggest using indexes for ORDER BY columns
            optimized = query.replace("SELECT", "SELECT /*+ USE_INDEX_FOR_ORDER_BY */")
        
        return optimized
    
    @staticmethod
    def add_limit_if_missing(query: str, max_rows: int = 10000) -> str:
        """Add LIMIT clause if missing to prevent large result sets"""
        if "LIMIT" not in query.upper() and "SELECT" in query.upper():
            return f"{query} LIMIT {max_rows}"
        return query

# 5. RESPONSE COMPRESSION
import gzip
import json
from fastapi.responses import JSONResponse

class CompressedJSONResponse(JSONResponse):
    """JSON response with gzip compression for large payloads"""
    
    def render(self, content: Any) -> bytes:
        json_data = json.dumps(content, default=str).encode()
        
        # Compress if data is larger than 1KB
        if len(json_data) > 1024:
            return gzip.compress(json_data)
        
        return json_data

# 6. MEMORY-EFFICIENT DATA PROCESSING
class StreamingProcessor:
    """Process large datasets without loading everything into memory"""
    
    @staticmethod
    async def process_large_dataset(
        query_func, 
        process_func, 
        chunk_size: int = 1000
    ):
        """Process large dataset in chunks"""
        offset = 0
        results = []
        
        while True:
            chunk_query = f"({query_func.__call__()}) LIMIT {chunk_size} OFFSET {offset}"
            chunk_data = await query_func(chunk_query)
            
            if not chunk_data:
                break
            
            processed_chunk = await process_func(chunk_data)
            results.extend(processed_chunk)
            
            offset += chunk_size
            
            # Yield control to prevent blocking
            await asyncio.sleep(0)
        
        return results

# 7. CONCURRENT REQUEST HANDLING
class ConcurrentHandler:
    """Handle multiple requests concurrently with proper resource management"""
    
    def __init__(self, max_concurrent: int = 10):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_requests = {}
    
    async def handle_request(self, request_id: str, handler_func, *args, **kwargs):
        """Handle request with concurrency control"""
        async with self.semaphore:
            task = asyncio.create_task(handler_func(*args, **kwargs))
            self.active_requests[request_id] = task
            
            try:
                result = await task
                return result
            finally:
                self.active_requests.pop(request_id, None)
    
    async def cancel_request(self, request_id: str):
        """Cancel specific request"""
        task = self.active_requests.get(request_id)
        if task and not task.done():
            task.cancel()
            return True
        return False

# 8. PERFORMANCE MONITORING
class PerformanceMonitor:
    """Monitor and log performance metrics"""
    
    def __init__(self):
        self.metrics = {
            "request_count": 0,
            "avg_response_time": 0,
            "cache_hit_rate": 0,
            "active_connections": 0
        }
    
    async def track_request(self, handler_func, *args, **kwargs):
        """Track request performance"""
        start_time = datetime.now()
        
        try:
            result = await handler_func(*args, **kwargs)
            status = "success"
        except Exception as e:
            result = {"error": str(e)}
            status = "error"
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Update metrics
        self.metrics["request_count"] += 1
        self.metrics["avg_response_time"] = (
            (self.metrics["avg_response_time"] * (self.metrics["request_count"] - 1) + duration) 
            / self.metrics["request_count"]
        )
        
        # Log performance data
        print(f"Request completed in {duration:.3f}s - Status: {status}")
        
        return result

# USAGE EXAMPLES:

"""
# In your main application:

# 1. Use async database pool
db_pool = DatabasePool(host, port, database, username, password)
await db_pool.initialize()

# 2. Implement smart caching
cache = SmartCache(redis_client)
result = await cache.get_or_compute(
    cache_key="query_123", 
    compute_func=execute_query,
    ttl=1800,
    sql_query="SELECT * FROM users"
)

# 3. Use batch processing for similar requests
batch_processor = BatchProcessor(batch_size=5, timeout=0.5)
result = await batch_processor.add_request({"query": query, "user_id": user_id})

# 4. Monitor performance
monitor = PerformanceMonitor()
result = await monitor.track_request(your_handler_function, *args, **kwargs)

# 5. Handle concurrent requests
concurrent_handler = ConcurrentHandler(max_concurrent=20)
result = await concurrent_handler.handle_request(
    request_id=f"{user_id}:{session_id}",
    handler_func=run_sql_pipeline,
    question=question,
    user_id=user_id,
    session_id=session_id
)
"""

# Performance Configuration
PERFORMANCE_CONFIG = {
    "database": {
        "pool_min": 5,
        "pool_max": 20,
        "query_timeout": 30,
        "connection_timeout": 10
    },
    "cache": {
        "default_ttl": 3600,
        "max_memory": "256mb",
        "eviction_policy": "allkeys-lru"
    },
    "processing": {
        "max_concurrent_requests": 100,
        "chunk_size": 1000,
        "batch_size": 10,
        "batch_timeout": 1.0
    },
    "response": {
        "compression_threshold": 1024,
        "max_response_size": "10mb"
    }
}