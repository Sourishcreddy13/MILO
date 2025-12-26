"""
Enhanced Security Module for M.I.L.O
Addresses security vulnerabilities and adds proper protection
"""

import os
import time
import hashlib
import secrets
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import jwt
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from functools import wraps
from fastapi import HTTPException, Request
import re

load_dotenv()

class SecurityManager:
    _instance: Optional['SecurityManager'] = None
    KEY_ROTATION_HOURS = 24
    
    def __init__(self):
        self._secret_key = os.getenv("SECRET_KEY")
        if not self._secret_key:
            if os.getenv("ENVIRONMENT", "development") == "production":
                raise RuntimeError("SECRET_KEY must be set in production")
            print("WARNING: SECRET_KEY not set; using ephemeral key for development.")
            self._secret_key = Fernet.generate_key().decode()
        
        self._cipher_suite = Fernet(self._secret_key.encode())
        self._last_rotation = time.time()
        self._operation_count = 0
        self._last_minute = time.time()
        self._minute_ops = 0
        
        # JWT settings
        self._jwt_secret = os.getenv("JWT_SECRET", self._secret_key)
        self._jwt_algorithm = "HS256"
        self._jwt_expiry_hours = 24
        
    @classmethod
    def get_instance(cls) -> 'SecurityManager':
        if cls._instance is None:
            cls._instance = SecurityManager()
        return cls._instance
    
    def _check_rate_limit(self):
        """Enhanced rate limiting for crypto operations"""
        current = time.time()
        if current - self._last_minute >= 60:
            self._last_minute = current
            self._minute_ops = 0
        self._minute_ops += 1
        if self._minute_ops > 1000:  # 1000 ops per minute limit
            raise HTTPException(status_code=429, detail="Rate limit exceeded for crypto operations")
    
    def _check_rotation(self):
        """Check if key rotation is needed"""
        if time.time() - self._last_rotation > self.KEY_ROTATION_HOURS * 3600:
            if os.getenv("ENVIRONMENT") == "production":
                print("WARNING: Key rotation needed")
            else:
                self._secret_key = Fernet.generate_key().decode()
                self._cipher_suite = Fernet(self._secret_key.encode())
            self._last_rotation = time.time()

    def generate_jwt_token(self, user_id: str, session_id: str) -> str:
        """Generate JWT token for user session"""
        payload = {
            "user_id": user_id,
            "session_id": session_id,
            "exp": datetime.utcnow() + timedelta(hours=self._jwt_expiry_hours),
            "iat": datetime.utcnow(),
            "iss": "milo-api"
        }
        return jwt.encode(payload, self._jwt_secret, algorithm=self._jwt_algorithm)
    
    def verify_jwt_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, self._jwt_secret, algorithms=[self._jwt_algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")

# Input validation utilities
class InputValidator:
    @staticmethod
    def sanitize_sql_input(query: str) -> str:
        """Basic SQL input sanitization"""
        # Remove potentially dangerous SQL keywords
        dangerous_patterns = [
            r'\b(DROP|DELETE|TRUNCATE|ALTER|CREATE|INSERT|UPDATE)\b',
            r'--',  # SQL comments
            r'/\*.*?\*/',  # SQL block comments
            r';\s*$',  # Trailing semicolons
        ]
        
        sanitized = query
        for pattern in dangerous_patterns:
            sanitized = re.sub(pattern, '', sanitized, flags=re.IGNORECASE)
        
        return sanitized.strip()
    
    @staticmethod
    def validate_question_length(question: str, max_length: int = 1000) -> bool:
        """Validate question length"""
        return len(question.strip()) <= max_length
    
    @staticmethod
    def validate_session_id(session_id: str) -> bool:
        """Validate session ID format"""
        # Should be a valid UUID format
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, session_id, re.IGNORECASE))

# Rate limiting decorator
class RateLimiter:
    def __init__(self):
        self.requests = {}
    
    def limit_requests(self, max_requests: int = 60, window_seconds: int = 60):
        """Rate limiting decorator"""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
                
                if request:
                    client_ip = request.client.host
                    current_time = time.time()
                    
                    if client_ip not in self.requests:
                        self.requests[client_ip] = []
                    
                    # Clean old requests
                    self.requests[client_ip] = [
                        req_time for req_time in self.requests[client_ip]
                        if current_time - req_time < window_seconds
                    ]
                    
                    if len(self.requests[client_ip]) >= max_requests:
                        raise HTTPException(status_code=429, detail="Too many requests")
                    
                    self.requests[client_ip].append(current_time)
                
                return await func(*args, **kwargs)
            return wrapper
        return decorator

# Initialize instances
security_manager = SecurityManager.get_instance()
input_validator = InputValidator()
rate_limiter = RateLimiter()

# Enhanced encryption functions
def encrypt_text(text: str) -> str:
    """Encrypts a string and returns it as a URL-safe base64 encoded string."""
    if not text:
        return ""
    manager = SecurityManager.get_instance()
    manager._check_rate_limit()
    manager._check_rotation()
    encrypted_text = manager._cipher_suite.encrypt(text.encode())
    return encrypted_text.decode()

def decrypt_text(encrypted_text: str) -> str:
    """Decrypts a string and returns the original text."""
    if not encrypted_text:
        return ""
    manager = SecurityManager.get_instance()
    manager._check_rate_limit()
    manager._check_rotation()
    try:
        decrypted_text = manager._cipher_suite.decrypt(encrypted_text.encode())
        return decrypted_text.decode()
    except Exception as e:
        raise HTTPException(status_code=400, detail="Failed to decrypt data")

def hash_password(password: str) -> str:
    """Hash password with salt"""
    salt = secrets.token_hex(16)
    password_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{password_hash.hex()}"

def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash"""
    try:
        salt, password_hash = hashed.split(':')
        return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex() == password_hash
    except ValueError:
        return False