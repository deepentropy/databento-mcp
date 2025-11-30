"""Retry logic module with exponential backoff for the Databento MCP server."""
import asyncio
import functools
import logging
import random
from typing import Callable, TypeVar

import httpx


logger = logging.getLogger(__name__)


# HTTP status codes that indicate transient errors
TRANSIENT_STATUS_CODES = frozenset([429, 502, 503, 504])

# Exceptions that indicate transient errors worth retrying
TRANSIENT_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    TimeoutError,
    ConnectionError,
    ConnectionResetError,
)

# Type variable for generic return type
T = TypeVar("T")


class RetryError(Exception):
    """Exception raised when all retry attempts have been exhausted."""
    
    def __init__(self, message: str, last_exception: Exception | None = None):
        super().__init__(message)
        self.last_exception = last_exception


def is_transient_error(exception: Exception) -> bool:
    """
    Check if an exception indicates a transient error that should be retried.
    
    Args:
        exception: The exception to check
        
    Returns:
        True if the error is transient and should be retried
    """
    # Check if it's a known transient exception type
    if isinstance(exception, TRANSIENT_EXCEPTIONS):
        return True
    
    # Check for HTTP status code errors
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code in TRANSIENT_STATUS_CODES
    
    # Check for rate limit errors embedded in other exceptions
    error_str = str(exception).lower()
    if "429" in error_str or "rate limit" in error_str:
        return True
    if "502" in error_str or "503" in error_str or "504" in error_str:
        return True
    if "timeout" in error_str or "timed out" in error_str:
        return True
    if "connection" in error_str and ("reset" in error_str or "refused" in error_str):
        return True
    
    return False


def is_rate_limit_error(exception: Exception) -> bool:
    """
    Check if an exception specifically indicates a rate limit error.
    
    Args:
        exception: The exception to check
        
    Returns:
        True if the error is a rate limit (429) error
    """
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code == 429
    
    error_str = str(exception).lower()
    return "429" in error_str or "rate limit" in error_str


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> float:
    """
    Calculate the backoff delay for a retry attempt using exponential backoff.
    
    Args:
        attempt: The current attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        jitter: Whether to add random jitter to prevent thundering herd
        
    Returns:
        Delay in seconds before the next retry
    """
    # Exponential backoff: base_delay * 2^attempt
    delay = base_delay * (2 ** attempt)
    
    # Cap at max_delay
    delay = min(delay, max_delay)
    
    # Add jitter (randomize between 0.5x and 1.5x the delay)
    if jitter:
        delay = delay * (0.5 + random.random())
    
    return delay


def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on: Callable[[Exception], bool] | None = None,
):
    """
    Decorator that adds retry logic with exponential backoff to async functions.
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds between retries (default: 1.0)
        max_delay: Maximum delay in seconds (default: 60.0)
        retry_on: Optional function to determine if an exception should trigger a retry.
                  Defaults to is_transient_error if not provided.
                  
    Returns:
        Decorated function with retry logic
        
    Example:
        @with_retry(max_retries=3, base_delay=1.0)
        async def fetch_data():
            return await api_call()
    """
    should_retry = retry_on or is_transient_error
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_exception: Exception | None = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry this error
                    if not should_retry(e):
                        logger.debug(
                            f"Non-retryable error in {func.__name__}: {type(e).__name__}: {e}"
                        )
                        raise
                    
                    # Check if we have retries left
                    if attempt >= max_retries:
                        logger.warning(
                            f"All {max_retries} retries exhausted for {func.__name__}: "
                            f"{type(e).__name__}: {e}"
                        )
                        raise RetryError(
                            f"All {max_retries} retry attempts exhausted for {func.__name__}",
                            last_exception=e,
                        ) from e
                    
                    # Calculate backoff delay
                    delay = calculate_backoff(attempt, base_delay, max_delay)
                    
                    # Log the retry
                    is_rate_limit = is_rate_limit_error(e)
                    error_type = "Rate limit" if is_rate_limit else "Transient error"
                    
                    logger.info(
                        f"{error_type} in {func.__name__} (attempt {attempt + 1}/{max_retries + 1}): "
                        f"{type(e).__name__}: {e}. Retrying in {delay:.2f}s..."
                    )
                    
                    # Wait before retrying
                    await asyncio.sleep(delay)
            
            # This should never be reached, but just in case
            raise RetryError(
                f"Unexpected retry exhaustion for {func.__name__}",
                last_exception=last_exception,
            )
        
        return wrapper
    
    return decorator


async def retry_async(
    func: Callable[..., T],
    *args,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on: Callable[[Exception], bool] | None = None,
    **kwargs,
) -> T:
    """
    Execute an async function with retry logic.
    
    This is a function-based alternative to the decorator for one-off retries.
    
    Args:
        func: The async function to call
        *args: Positional arguments to pass to func
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds between retries (default: 1.0)
        max_delay: Maximum delay in seconds (default: 60.0)
        retry_on: Optional function to determine if an exception should trigger a retry
        **kwargs: Keyword arguments to pass to func
        
    Returns:
        The result of the function call
        
    Raises:
        RetryError: If all retry attempts are exhausted
    """
    should_retry = retry_on or is_transient_error
    last_exception: Exception | None = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            
            if not should_retry(e):
                raise
            
            if attempt >= max_retries:
                raise RetryError(
                    f"All {max_retries} retry attempts exhausted",
                    last_exception=e,
                ) from e
            
            delay = calculate_backoff(attempt, base_delay, max_delay)
            
            logger.info(
                f"Retry attempt {attempt + 1}/{max_retries + 1} after error: "
                f"{type(e).__name__}: {e}. Waiting {delay:.2f}s..."
            )
            
            await asyncio.sleep(delay)
    
    raise RetryError(
        "Unexpected retry exhaustion",
        last_exception=last_exception,
    )
