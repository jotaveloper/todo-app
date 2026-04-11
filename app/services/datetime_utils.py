from datetime import date, datetime, timedelta


def to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "t", "true", "yes", "y"}


def to_iso_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    return text


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _parse_datetime_safe(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_date_safe(value):
    if isinstance(value, date):
        return value
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def relative_time_label(datetime_value):
    parsed = _parse_datetime_safe(datetime_value)
    if parsed is None:
        parsed_date = _parse_date_safe(datetime_value)
        if parsed_date is None:
            return "Sin registro"
        delta_days = (date.today() - parsed_date).days
        if delta_days <= 0:
            return "hoy"
        if delta_days == 1:
            return "hace 1 día"
        return f"hace {delta_days} días"

    if parsed.tzinfo is not None:
        now_dt = datetime.now(parsed.tzinfo)
    else:
        now_dt = datetime.now()
    delta_seconds = int((now_dt - parsed).total_seconds())
    if delta_seconds < 0:
        delta_seconds = 0
    if delta_seconds < 60:
        return "hace unos segundos"
    if delta_seconds < 3600:
        minutes = delta_seconds // 60
        return f"hace {minutes} min"
    if delta_seconds < 86400:
        hours = delta_seconds // 3600
        return f"hace {hours} hora{'s' if hours != 1 else ''}"
    days = delta_seconds // 86400
    return f"hace {days} día{'s' if days != 1 else ''}"


def parse_iso_date(value):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        date.fromisoformat(raw)
        return raw
    except ValueError:
        return None


def add_month(base_date):
    year = base_date.year
    month = base_date.month + 1
    if month > 12:
        year += 1
        month = 1
    day = min(base_date.day, 28)
    return date(year, month, day)


def next_due_date(current_due, recurrence):
    base = current_due if current_due else date.today()
    if recurrence == "daily":
        return base + timedelta(days=1)
    if recurrence == "weekly":
        return base + timedelta(days=7)
    if recurrence == "monthly":
        return add_month(base)
    return None

