import re
from urllib.parse import urlparse

_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")
_ALLOWED_PARAMS = {"country", "limit", "page", "parameter"}


def validate_country(country: str) -> str:
    if not _COUNTRY_RE.match(country):
        raise ValueError(f"Invalid country code: {country!r}. Must be ISO 3166-1 alpha-2 (e.g. 'DK').")
    return country


def validate_limit(limit: int) -> int:
    if not isinstance(limit, int) or limit < 1 or limit > 10000:
        raise ValueError(f"Invalid limit: {limit!r}. Must be an integer between 1 and 10000.")
    return limit


def enforce_https(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"Insecure URL scheme {parsed.scheme!r}. Only HTTPS is allowed.")
    return url


def build_safe_params(country: str, limit: int) -> dict:
    """
    Returns a dict of validated query parameters.
    Passing a dict to requests.get(params=...) ensures proper URL encoding
    and prevents any injection via raw string interpolation.
    """
    return {
        "country": validate_country(country),
        "limit": validate_limit(limit),
    }
