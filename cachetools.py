"""Mock implementation of cachetools for testing"""

class TTLCache:
    def __init__(self, maxsize=128, ttl=240):
        self.cache = {}
        self.maxsize = maxsize
        self.ttl = ttl

def cached(cache):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator