import re
from urllib.parse import urljoin, urlparse

EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def normalize_email(value):
    return (value or "").strip().lower()


def is_valid_email(value):
    return bool(EMAIL_PATTERN.match(value or ""))


def is_safe_redirect_target(target, host_url):
    if not target:
        return False
    ref_url = urlparse(host_url)
    test_url = urlparse(urljoin(host_url, target))
    return test_url.scheme in {"http", "https"} and ref_url.netloc == test_url.netloc
