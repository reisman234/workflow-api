import time
from functools import wraps


def retry(max_retries, delay=1, _backoff=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for _ in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Error: {e}. Retrying...")
                    time.sleep(delay)
                    current_delay *= _backoff
            raise Exception(f"Method {func.__name__} failed after {max_retries} retries.")
        return wrapper
    return decorator
