"""Tests for the retry module."""
import pytest
import httpx
from retry import (
    RetryError,
    is_transient_error,
    is_rate_limit_error,
    calculate_backoff,
    with_retry,
    retry_async,
)


class TestIsTransientError:
    """Tests for is_transient_error function."""

    def test_connection_error_is_transient(self):
        """Test that connection errors are transient."""
        error = httpx.ConnectError("Connection refused")
        assert is_transient_error(error) is True

    def test_timeout_error_is_transient(self):
        """Test that timeout errors are transient."""
        error = httpx.ReadTimeout("Read timed out")
        assert is_transient_error(error) is True

    def test_429_status_is_transient(self):
        """Test that HTTP 429 is transient."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(429, request=request)
        error = httpx.HTTPStatusError("Rate limited", request=request, response=response)
        assert is_transient_error(error) is True

    def test_502_status_is_transient(self):
        """Test that HTTP 502 is transient."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(502, request=request)
        error = httpx.HTTPStatusError("Bad gateway", request=request, response=response)
        assert is_transient_error(error) is True

    def test_503_status_is_transient(self):
        """Test that HTTP 503 is transient."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(503, request=request)
        error = httpx.HTTPStatusError("Service unavailable", request=request, response=response)
        assert is_transient_error(error) is True

    def test_504_status_is_transient(self):
        """Test that HTTP 504 is transient."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(504, request=request)
        error = httpx.HTTPStatusError("Gateway timeout", request=request, response=response)
        assert is_transient_error(error) is True

    def test_400_status_is_not_transient(self):
        """Test that HTTP 400 is not transient."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(400, request=request)
        error = httpx.HTTPStatusError("Bad request", request=request, response=response)
        assert is_transient_error(error) is False

    def test_404_status_is_not_transient(self):
        """Test that HTTP 404 is not transient."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(404, request=request)
        error = httpx.HTTPStatusError("Not found", request=request, response=response)
        assert is_transient_error(error) is False

    def test_value_error_is_not_transient(self):
        """Test that ValueError is not transient."""
        error = ValueError("Invalid argument")
        assert is_transient_error(error) is False

    def test_error_message_with_429(self):
        """Test that error messages containing 429 are detected."""
        error = Exception("API returned 429 rate limit exceeded")
        assert is_transient_error(error) is True

    def test_error_message_with_timeout(self):
        """Test that error messages containing timeout are detected."""
        error = Exception("Request timed out after 30 seconds")
        assert is_transient_error(error) is True


class TestIsRateLimitError:
    """Tests for is_rate_limit_error function."""

    def test_429_is_rate_limit(self):
        """Test that HTTP 429 is a rate limit error."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(429, request=request)
        error = httpx.HTTPStatusError("Rate limited", request=request, response=response)
        assert is_rate_limit_error(error) is True

    def test_503_is_not_rate_limit(self):
        """Test that HTTP 503 is not a rate limit error."""
        request = httpx.Request("GET", "http://example.com")
        response = httpx.Response(503, request=request)
        error = httpx.HTTPStatusError("Service unavailable", request=request, response=response)
        assert is_rate_limit_error(error) is False

    def test_error_message_with_rate_limit(self):
        """Test that error messages containing rate limit are detected."""
        error = Exception("Rate limit exceeded")
        assert is_rate_limit_error(error) is True


class TestCalculateBackoff:
    """Tests for calculate_backoff function."""

    def test_first_attempt_backoff(self):
        """Test backoff for first attempt."""
        delay = calculate_backoff(0, base_delay=1.0, jitter=False)
        assert delay == 1.0

    def test_second_attempt_backoff(self):
        """Test backoff for second attempt."""
        delay = calculate_backoff(1, base_delay=1.0, jitter=False)
        assert delay == 2.0

    def test_third_attempt_backoff(self):
        """Test backoff for third attempt."""
        delay = calculate_backoff(2, base_delay=1.0, jitter=False)
        assert delay == 4.0

    def test_backoff_respects_max_delay(self):
        """Test that backoff respects max_delay."""
        delay = calculate_backoff(10, base_delay=1.0, max_delay=60.0, jitter=False)
        assert delay == 60.0

    def test_backoff_with_jitter_in_range(self):
        """Test that backoff with jitter is within expected range."""
        # With jitter, delay should be between 0.5x and 1.5x the base
        for _ in range(100):  # Run multiple times due to randomness
            delay = calculate_backoff(0, base_delay=1.0, jitter=True)
            assert 0.5 <= delay <= 1.5


class TestWithRetryDecorator:
    """Tests for with_retry decorator."""

    @pytest.mark.asyncio
    async def test_successful_call_no_retry(self):
        """Test successful call doesn't retry."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        async def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await successful_func()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_transient_error_retries(self):
        """Test transient errors trigger retries."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        async def failing_then_success():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ConnectError("Connection refused")
            return "success"

        result = await failing_then_success()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_non_transient_error_no_retry(self):
        """Test non-transient errors don't retry."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        async def non_transient_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("Invalid argument")

        with pytest.raises(ValueError, match="Invalid argument"):
            await non_transient_error()
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self):
        """Test RetryError is raised when all retries are exhausted."""
        call_count = 0

        @with_retry(max_retries=2, base_delay=0.01)
        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise httpx.ConnectError("Connection refused")

        with pytest.raises(RetryError, match="retry attempts exhausted"):
            await always_fails()
        assert call_count == 3  # Initial call + 2 retries


class TestRetryAsync:
    """Tests for retry_async function."""

    @pytest.mark.asyncio
    async def test_successful_call(self):
        """Test successful call with retry_async."""
        async def successful_func():
            return "success"

        result = await retry_async(successful_func, max_retries=3, base_delay=0.01)
        assert result == "success"

    @pytest.mark.asyncio
    async def test_retry_on_transient_error(self):
        """Test retry_async retries on transient errors."""
        call_count = 0

        async def failing_then_success():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.ReadTimeout("Timeout")
            return "success"

        result = await retry_async(failing_then_success, max_retries=3, base_delay=0.01)
        assert result == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_retry_async_with_args(self):
        """Test retry_async passes arguments correctly."""
        async def add(a, b):
            return a + b

        result = await retry_async(add, 1, 2, max_retries=3, base_delay=0.01)
        assert result == 3

    @pytest.mark.asyncio
    async def test_retry_async_with_kwargs(self):
        """Test retry_async passes keyword arguments correctly."""
        async def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        result = await retry_async(
            greet, "World", greeting="Hi", max_retries=3, base_delay=0.01
        )
        assert result == "Hi, World!"
