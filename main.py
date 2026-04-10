import sqlite3
import calendar
from datetime import date, timedelta
from datetime import datetime
import json
import os
import re
from pathlib import Path
from functools import lru_cache
from urllib.parse import urljoin, urlparse

from flask import Flask, jsonify, make_response, redirect, render_template, request, url_for, flash
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from werkzeug.security import check_password_hash, generate_password_hash
from db import get_connection as get_postgres_connection
try:
    from authlib.integrations.flask_client import OAuth
    OAUTH_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover - fallback cuando dependencia no está instalada
    OAuth = None
    OAUTH_IMPORT_ERROR = str(exc)
try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:  # pragma: no cover - fallback cuando dependencia no está instalada
    load_dotenv = None
    find_dotenv = None

if load_dotenv is not None:
    dotenv_path = find_dotenv(usecwd=True) if find_dotenv is not None else ""
    if not dotenv_path:
        dotenv_path = str(Path(__file__).resolve().parent / ".env")
    load_dotenv(dotenv_path)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "todoapp-dev-secret-change-this")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_DISCOVERY_URL = os.getenv(
    "GOOGLE_DISCOVERY_URL",
    "https://accounts.google.com/.well-known/openid-configuration",
).strip()
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "tasks.db"
VALID_FILTERS = {"all", "pending", "completed"}
VALID_PRIORITIES = {"baja", "media", "alta"}
VALID_SORTS = {"created_desc", "created_asc", "priority", "due_date"}
VALID_QUICK_DATES = {"", "today", "week"}
VALID_RECURRENCES = {"", "daily", "weekly", "monthly"}
VALID_STATS_RANGES = {7, 30, 90}
VALID_NAVS = {
    "dashboard",
    "tasks",
    "calendar",
    "stats",
    "projects",
    "labels",
    "settings",
}
TASK_SELECT_FIELDS = (
    "id",
    "user_id",
    "title",
    "completed",
    "priority",
    "recurrence",
    "due_date",
    "category",
    "completed_at",
    "position",
    "notes",
    "project_id",
)

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message = "Inicia sesión para continuar."
login_manager.login_message_category = "error"

oauth_client = None
oauth = OAuth(app) if OAuth is not None else None


class User(UserMixin):
    def __init__(self, user_id, name, email):
        self.id = str(user_id)
        self.name = name
        self.email = email


@login_manager.user_loader
def load_user(user_id):
    try:
        normalized_id = int(user_id)
    except (TypeError, ValueError):
        return None
    user = pg_fetch_one_dict(
        "SELECT id, name, email FROM users WHERE id = %s",
        (normalized_id,),
    )
    if user is None:
        return None
    return User(user["id"], user["name"], user["email"])


def normalize_email(value):
    return (value or "").strip().lower()


def is_valid_email(value):
    return bool(EMAIL_PATTERN.match(value or ""))


def get_user_by_email(email):
    normalized = normalize_email(email)
    if not normalized:
        return None
    return pg_fetch_one_dict(
        "SELECT id, name, email, password_hash, auth_provider, google_id "
        "FROM users WHERE LOWER(email) = LOWER(%s)",
        (normalized,),
    )


def get_user_by_google_id(google_id):
    normalized = (google_id or "").strip()
    if not normalized:
        return None
    return pg_fetch_one_dict(
        "SELECT id, name, email, password_hash, auth_provider, google_id "
        "FROM users WHERE google_id = %s",
        (normalized,),
    )


def is_safe_redirect_target(target):
    if not target:
        return False
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in {"http", "https"} and ref_url.netloc == test_url.netloc


def current_user_id():
    if current_user.is_authenticated:
        try:
            return int(current_user.get_id())
        except (TypeError, ValueError):
            return None
    return None


def get_google_oauth_client():
    global oauth_client
    if oauth_client is not None:
        return oauth_client
    if oauth is None:
        return None

    client_id = (os.getenv("GOOGLE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    discovery_url = (
        (os.getenv("GOOGLE_DISCOVERY_URL") or "").strip()
        or "https://accounts.google.com/.well-known/openid-configuration"
    )
    if not client_id or not client_secret:
        return None

    oauth_client = oauth.register(
        name="google",
        server_metadata_url=discovery_url,
        client_id=client_id,
        client_secret=client_secret,
        client_kwargs={"scope": "openid email profile"},
    )
    return oauth_client


def is_google_login_enabled():
    return bool(
        (os.getenv("GOOGLE_CLIENT_ID") or "").strip() and
        (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    )


@app.context_processor
def inject_auth_context():
    return {"google_login_enabled": is_google_login_enabled()}


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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


def pg_fetch_all_dicts(query, params=()):
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description] if cur.description else []
    return [dict(zip(cols, row)) for row in rows]


def pg_fetch_one_dict(query, params=()):
    rows = pg_fetch_all_dicts(query, params)
    return rows[0] if rows else None


@lru_cache(maxsize=1)
def get_pg_task_columns():
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND table_name = 'tasks'"
                )
                return {row[0] for row in cur.fetchall()}
    except Exception:
        return {
            "id",
            "title",
            "completed",
            "priority",
            "recurrence",
            "due_date",
            "category",
            "completed_at",
            "position",
            "notes",
            "project_id",
        }


def pg_task_has(column_name):
    try:
        return column_name in get_pg_task_columns()
    except Exception:
        return False


def pg_task_select_clause():
    columns = get_pg_task_columns()
    parts = []
    for field in TASK_SELECT_FIELDS:
        if field in columns:
            parts.append(field)
        elif field in {"completed", "position"}:
            parts.append(f"0 AS {field}")
        else:
            parts.append(f"NULL AS {field}")
    return ", ".join(parts)


def pg_task_order_by(sort_value):
    has_position = pg_task_has("position")
    if sort_value == "created_asc":
        return "COALESCE(position, id) ASC, id ASC" if has_position else "id ASC"
    if sort_value == "priority":
        return (
            "CASE priority "
            "WHEN 'alta' THEN 0 "
            "WHEN 'media' THEN 1 "
            "WHEN 'baja' THEN 2 "
            "ELSE 3 END ASC, id DESC"
        )
    if sort_value == "due_date":
        return "due_date IS NULL ASC, due_date ASC, id DESC"
    return "COALESCE(position, id) DESC, id DESC" if has_position else "id DESC"


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


def log_event(action_type, task_title, details="", task_id=None, conn=None, user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return
    safe_title = (task_title or "").strip() or "(sin título)"
    safe_details = (details or "").strip()
    if conn is not None:
        conn.execute(
            "INSERT INTO activity_log (user_id, action_type, task_id, task_title, details, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (owner_id, action_type, task_id, safe_title, safe_details, now_iso()),
        )
    else:
        with get_connection() as new_conn:
            new_conn.execute(
            "INSERT INTO activity_log (user_id, action_type, task_id, task_title, details, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (owner_id, action_type, task_id, safe_title, safe_details, now_iso()),
            )


def get_activity_logs(limit=25):
    owner_id = current_user_id()
    if owner_id is None:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT action_type, task_title, details, created_at "
            "FROM activity_log WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (owner_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def build_export_payload():
    owner_id = current_user_id()
    if owner_id is None:
        raise ValueError("Usuario no autenticado.")
    required_tables = ["tasks", "subtasks", "categories", "activity_log"]
    optional_tables = ["streaks", "goals"]
    payload = {
        "version": 1,
        "exported_at": now_iso(),
        "tables": {},
    }
    with get_connection() as conn:
        task_rows = conn.execute(
            "SELECT * FROM tasks WHERE user_id = ? ORDER BY id ASC",
            (owner_id,),
        ).fetchall()
        payload["tables"]["tasks"] = [dict(row) for row in task_rows]
        owned_task_ids = [row["id"] for row in task_rows]
        if owned_task_ids:
            placeholders = ",".join(["?"] * len(owned_task_ids))
            note_rows = conn.execute(
                f"SELECT * FROM subtasks WHERE task_id IN ({placeholders}) ORDER BY id ASC",
                tuple(owned_task_ids),
            ).fetchall()
        else:
            note_rows = []
        payload["tables"]["subtasks"] = [dict(row) for row in note_rows]
        payload["tables"]["categories"] = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM categories WHERE user_id = ? ORDER BY id ASC",
                (owner_id,),
            ).fetchall()
        ]
        payload["tables"]["activity_log"] = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM activity_log WHERE user_id = ? ORDER BY id ASC",
                (owner_id,),
            ).fetchall()
        ]
        for table in optional_tables:
            if table_exists(conn, table):
                rows = conn.execute(f"SELECT * FROM {table} ORDER BY id ASC").fetchall()
                payload["tables"][table] = [dict(row) for row in rows]
    try:
        payload["tables"]["projects"] = pg_fetch_all_dicts(
            "SELECT id, name, COALESCE(description, '') AS description, "
            "COALESCE(color, '') AS color, "
            "TO_CHAR(due_date, 'YYYY-MM-DD') AS due_date, "
            "COALESCE(status, 'active') AS status, "
            "TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS') AS created_at "
            "FROM projects WHERE user_id = %s ORDER BY id ASC",
            (owner_id,),
        )
    except Exception:
        payload["tables"]["projects"] = []
    try:
        payload["tables"]["tags"] = pg_fetch_all_dicts(
            "SELECT id, name, COALESCE(color, '#22c55e') AS color "
            "FROM tags WHERE user_id = %s ORDER BY id ASC",
            (owner_id,),
        )
        payload["tables"]["task_tags"] = pg_fetch_all_dicts(
            "SELECT task_id, tag_id FROM task_tags "
            "WHERE task_id IN (SELECT id FROM tasks WHERE user_id = %s) "
            "ORDER BY task_id ASC, tag_id ASC",
            (owner_id,),
        )
    except Exception:
        payload["tables"]["tags"] = []
        payload["tables"]["task_tags"] = []
    return payload


def restore_from_payload(payload):
    owner_id = current_user_id()
    if owner_id is None:
        raise ValueError("Usuario no autenticado.")
    required_tables = ["tasks", "subtasks", "categories", "activity_log"]
    if not isinstance(payload, dict):
        raise ValueError("Formato inválido: se esperaba un objeto JSON.")
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("Formato inválido: falta 'tables'.")
    for name in required_tables:
        if name not in tables or not isinstance(tables[name], list):
            raise ValueError(f"Importación incompleta: falta la tabla '{name}'.")

    with get_connection() as conn:
        task_columns_rows = conn.execute("PRAGMA table_info(tasks)").fetchall()
        task_column_names = {column["name"] for column in task_columns_rows}
        owned_task_rows = conn.execute(
            "SELECT id FROM tasks WHERE user_id = ?",
            (owner_id,),
        ).fetchall()
        owned_task_ids = [row["id"] for row in owned_task_rows]
        if owned_task_ids:
            placeholders = ",".join(["?"] * len(owned_task_ids))
            conn.execute(
                f"DELETE FROM subtasks WHERE task_id IN ({placeholders})",
                tuple(owned_task_ids),
            )
        conn.execute("DELETE FROM activity_log WHERE user_id = ?", (owner_id,))
        conn.execute("DELETE FROM categories WHERE user_id = ?", (owner_id,))
        conn.execute("DELETE FROM tasks WHERE user_id = ?", (owner_id,))

        if "projects" in tables and isinstance(tables["projects"], list):
            with get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cur:
                    cur.execute("DELETE FROM projects WHERE user_id = %s", (owner_id,))
                    for row in tables["projects"]:
                        if not isinstance(row, dict):
                            continue
                        name = (row.get("name") or "").strip()
                        if not name:
                            continue
                        cur.execute(
                            "INSERT INTO projects (id, name, description, color, due_date, status, created_at, user_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s::timestamp, CURRENT_TIMESTAMP), %s) "
                            "ON CONFLICT (id) DO NOTHING",
                            (
                                row.get("id"),
                                name,
                                row.get("description") or "",
                                row.get("color") or "",
                                parse_iso_date(row.get("due_date") or ""),
                                row.get("status") if row.get("status") in {"active", "completed"} else "active",
                                row.get("created_at"),
                                owner_id,
                            ),
                        )
                pg_conn.commit()
        if "tags" in tables and isinstance(tables["tags"], list):
            with get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM task_tags WHERE task_id IN (SELECT id FROM tasks WHERE user_id = %s)",
                        (owner_id,),
                    )
                    cur.execute("DELETE FROM tags WHERE user_id = %s", (owner_id,))
                    for row in tables["tags"]:
                        if not isinstance(row, dict):
                            continue
                        name = (row.get("name") or "").strip()
                        if not name:
                            continue
                        cur.execute(
                            "INSERT INTO tags (id, name, color, user_id) VALUES (%s, %s, %s, %s) "
                            "ON CONFLICT (id) DO NOTHING",
                            (row.get("id"), name, row.get("color") or "#22c55e", owner_id),
                        )
                pg_conn.commit()

        for row in tables["tasks"]:
            if not isinstance(row, dict):
                raise ValueError("Fila inválida en tasks.")
            task_id = row.get("id")
            title = (row.get("title") or "").strip()
            if task_id is None or not title:
                raise ValueError("Registro de tasks inválido (id/title).")
            completed = 1 if row.get("completed") else 0
            priority = row.get("priority") if row.get("priority") in VALID_PRIORITIES else "media"
            recurrence = row.get("recurrence") if row.get("recurrence") in VALID_RECURRENCES else ""
            due_date = row.get("due_date")
            completed_at = row.get("completed_at")
            category = (row.get("category") or "").strip()
            notes = (row.get("notes") or "").strip()
            position = row.get("position")
            project_id = row.get("project_id")
            if not isinstance(project_id, int):
                project_id = None
            if not isinstance(position, int):
                position = int(task_id)
            columns = ["id", "title", "completed", "completed_at", "priority", "recurrence", "due_date", "category", "position", "notes"]
            values = [task_id, title, completed, completed_at, priority, recurrence, due_date, category, position, notes]
            if "user_id" in task_column_names:
                columns.append("user_id")
                values.append(owner_id)
            if "project_id" in task_column_names:
                columns.append("project_id")
                values.append(project_id)
            placeholders = ", ".join(["?"] * len(values))
            conn.execute(
                f"INSERT INTO tasks ({', '.join(columns)}) VALUES ({placeholders})",
                tuple(values),
            )

        for row in tables["subtasks"]:
            if not isinstance(row, dict):
                raise ValueError("Fila inválida en subtasks.")
            sid = row.get("id")
            task_id = row.get("task_id")
            title = (row.get("title") or "").strip()
            if sid is None or task_id is None or not title:
                raise ValueError("Registro de subtasks inválido (id/task_id/title).")
            completed = 1 if row.get("completed") else 0
            task_exists = conn.execute(
                "SELECT id FROM tasks WHERE id = ? AND user_id = ?", (task_id, owner_id)
            ).fetchone()
            if task_exists is None:
                raise ValueError("Nota con task_id inexistente.")
            conn.execute(
                "INSERT INTO subtasks (id, task_id, title, completed) VALUES (?, ?, ?, ?)",
                (sid, task_id, title, completed),
            )

        for row in tables["categories"]:
            if not isinstance(row, dict):
                raise ValueError("Fila inválida en categories.")
            cid = row.get("id")
            name = (row.get("name") or "").strip()
            if cid is None or not name:
                raise ValueError("Registro de categories inválido (id/name).")
            conn.execute(
                "INSERT OR IGNORE INTO categories (id, user_id, name) VALUES (?, ?, ?)",
                (cid, owner_id, name),
            )

        conn.execute(
            "INSERT OR IGNORE INTO categories (user_id, name) "
            "SELECT DISTINCT ?, category FROM tasks "
            "WHERE user_id = ? AND category IS NOT NULL AND TRIM(category) != ''",
            (owner_id, owner_id),
        )
        conn.execute(
            "UPDATE tasks SET position = id WHERE user_id = ? AND (position IS NULL OR position = 0)",
            (owner_id,),
        )

        for row in tables["activity_log"]:
            if not isinstance(row, dict):
                raise ValueError("Fila inválida en activity_log.")
            lid = row.get("id")
            action_type = (row.get("action_type") or "").strip()
            task_title = (row.get("task_title") or "").strip()
            created_at = (row.get("created_at") or "").strip()
            if lid is None or not action_type or not task_title or not created_at:
                raise ValueError("Registro de activity_log inválido.")
            conn.execute(
                "INSERT INTO activity_log (id, user_id, action_type, task_id, task_title, details, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    lid,
                    owner_id,
                    action_type,
                    row.get("task_id"),
                    task_title,
                    row.get("details") or "",
                    created_at,
                ),
            )

    if "task_tags" in tables and isinstance(tables["task_tags"], list):
        with get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cur:
                for row in tables["task_tags"]:
                    if not isinstance(row, dict):
                        continue
                    task_id = row.get("task_id")
                    tag_id = row.get("tag_id")
                    if not isinstance(task_id, int) or not isinstance(tag_id, int):
                        continue
                    task_owner = pg_fetch_one_dict(
                        "SELECT id FROM tasks WHERE id = %s AND user_id = %s",
                        (task_id, owner_id),
                    )
                    tag_owner = pg_fetch_one_dict(
                        "SELECT id FROM tags WHERE id = %s AND user_id = %s",
                        (tag_id, owner_id),
                    )
                    if task_owner is None or tag_owner is None:
                        continue
                    cur.execute(
                        "INSERT INTO task_tags (task_id, tag_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (task_id, tag_id),
                    )
            pg_conn.commit()


def build_task(row):
    due_date = to_iso_date(row.get("due_date"))
    is_overdue = False
    if due_date and not to_bool(row["completed"]):
        try:
            is_overdue = date.fromisoformat(due_date) < date.today()
        except ValueError:
            is_overdue = False

    return {
        "id": row["id"],
        "text": row["title"],
        "completed": to_bool(row["completed"]),
        "priority": row["priority"],
        "due_date": due_date,
        "overdue": is_overdue,
        "category": (row["category"] or "").strip(),
        "recurrence": row["recurrence"] if row["recurrence"] in VALID_RECURRENCES else "",
        "notes": (row.get("notes") or "").strip(),
        "project_id": row.get("project_id"),
    }


def get_filter_value():
    value = request.args.get("filter") or request.form.get("filter") or "all"
    return value if value in VALID_FILTERS else "all"


def get_search_value():
    return (request.args.get("q") or request.form.get("q") or "").strip()


def get_date_search_value():
    raw_value = (request.args.get("date_q") or request.form.get("date_q") or "").strip()
    if not raw_value:
        return ""
    return raw_value if parse_iso_date(raw_value) else ""


def get_sort_value():
    value = request.args.get("sort") or request.form.get("sort") or "created_desc"
    return value if value in VALID_SORTS else "created_desc"


def get_quick_date_value():
    value = (request.args.get("quick_date") or request.form.get("quick_date") or "").strip().lower()
    return value if value in VALID_QUICK_DATES else ""


def get_nav_value():
    value = request.args.get("nav") or request.form.get("nav") or "dashboard"
    return value if value in VALID_NAVS else "dashboard"


def get_calendar_month_value():
    value = (request.args.get("month") or request.form.get("month") or "").strip()
    if not value:
        return date.today().strftime("%Y-%m")
    try:
        parsed = datetime.strptime(value, "%Y-%m")
        return parsed.strftime("%Y-%m")
    except ValueError:
        return date.today().strftime("%Y-%m")


def get_stats_range_days():
    raw_value = request.args.get("stats_range") or request.form.get("stats_range")
    try:
        days = int(raw_value) if raw_value is not None else 30
    except (TypeError, ValueError):
        days = 30
    return days if days in VALID_STATS_RANGES else 30


def get_dashboard_from_value():
    raw = request.args.get("dashboard_from") or request.form.get("dashboard_from") or ""
    return parse_iso_date(raw) or ""


def get_dashboard_to_value():
    raw = request.args.get("dashboard_to") or request.form.get("dashboard_to") or ""
    return parse_iso_date(raw) or ""


def build_dashboard_due_filter(date_from="", date_to="", column="due_date"):
    parts = []
    params = []
    prefix = f"{column}"
    if date_from:
        parts.append(f"{prefix} IS NOT NULL AND {prefix} >= %s")
        params.append(date_from)
    if date_to:
        parts.append(f"{prefix} IS NOT NULL AND {prefix} <= %s")
        params.append(date_to)
    return parts, params


def get_calendar_view(user_id, month_value):
    first_day = datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    month_calendar = calendar.Calendar(firstweekday=0)
    grid_dates = list(month_calendar.itermonthdates(first_day.year, first_day.month))
    start_date = grid_dates[0].isoformat()
    end_date = grid_dates[-1].isoformat()
    today_iso = date.today().isoformat()

    rows = pg_fetch_all_dicts(
        f"SELECT {pg_task_select_clause()} "
        "FROM tasks "
        "WHERE user_id = %s "
        "AND due_date IS NOT NULL "
        "AND due_date >= %s AND due_date <= %s "
        "ORDER BY due_date ASC, id DESC",
        (user_id, start_date, end_date),
    )

    task_ids = [row["id"] for row in rows]
    notes_map = get_notes_map(task_ids)
    tasks_by_date = {}
    task_details = {}
    for row in rows:
        due_date = to_iso_date(row.get("due_date"))
        task_item = {
            "id": row["id"],
            "text": row["title"],
            "completed": to_bool(row["completed"]),
            "priority": row["priority"],
            "category": (row["category"] or "").strip(),
            "due_date": due_date,
        }
        if due_date:
            tasks_by_date.setdefault(due_date, []).append(task_item)
        task_details[row["id"]] = {
            "id": row["id"],
            "text": row["title"],
            "completed": to_bool(row["completed"]),
            "priority": row["priority"],
            "category": (row["category"] or "").strip(),
            "due_date": due_date,
            "notes": (row.get("notes") or "").strip(),
            "notes_items": notes_map.get(row["id"], []),
            "subtasks": notes_map.get(row["id"], []),  # Compatibilidad temporal
        }

    weeks = []
    for week_start in range(0, len(grid_dates), 7):
        week_days = []
        for day_obj in grid_dates[week_start:week_start + 7]:
            day_iso = day_obj.isoformat()
            week_days.append(
                {
                    "date": day_iso,
                    "day": day_obj.day,
                    "in_month": day_obj.month == first_day.month,
                    "is_today": day_iso == today_iso,
                    "tasks": tasks_by_date.get(day_iso, []),
                }
            )
        weeks.append(week_days)

    prev_month = (first_day.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    next_month = (first_day.replace(day=28) + timedelta(days=7)).replace(day=1).strftime("%Y-%m")
    month_labels = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
    ]

    return {
        "month": month_value,
        "month_label": f"{month_labels[first_day.month - 1]} {first_day.year}",
        "prev_month": prev_month,
        "next_month": next_month,
        "today_month": date.today().strftime("%Y-%m"),
        "weekdays": ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"],
        "weeks": weeks,
        "task_details": task_details,
    }


def get_order_by_clause(sort_value):
    if sort_value == "created_asc":
        return "position ASC, id ASC"
    if sort_value == "priority":
        return (
            "CASE priority "
            "WHEN 'alta' THEN 0 "
            "WHEN 'media' THEN 1 "
            "WHEN 'baja' THEN 2 "
            "ELSE 3 END ASC, id DESC"
        )
    if sort_value == "due_date":
        return "due_date IS NULL ASC, due_date ASC, id DESC"
    return "position DESC, id DESC"


def redirect_to_index(
    filter_value,
    search_query,
    sort_value,
    nav_value,
    date_search_query=None,
    quick_date_value=None,
    new_task_id=None,
    dashboard_from_value=None,
    dashboard_to_value=None,
    category_status=None,
    category_message=None,
    project_id_value=None,
    tag_id_value=None,
):
    safe_date_search = date_search_query if date_search_query is not None else get_date_search_value()
    safe_quick_date = quick_date_value if quick_date_value is not None else get_quick_date_value()
    safe_dashboard_from = (
        dashboard_from_value if dashboard_from_value is not None else get_dashboard_from_value()
    )
    safe_dashboard_to = (
        dashboard_to_value if dashboard_to_value is not None else get_dashboard_to_value()
    )
    args = {
        "filter": filter_value,
        "q": search_query,
        "date_q": safe_date_search,
        "quick_date": safe_quick_date,
        "sort": sort_value,
        "nav": nav_value,
        "new_task_id": new_task_id,
        "dashboard_from": safe_dashboard_from,
        "dashboard_to": safe_dashboard_to,
        "project_id": project_id_value if project_id_value is not None else get_selected_project_id(),
        "tag_id": tag_id_value if tag_id_value is not None else get_selected_tag_id(),
    }
    if category_status:
        args["category_status"] = category_status
    if category_message:
        args["category_message"] = category_message
    return redirect(url_for("index", **args))


def get_dashboard_stats(user_id, date_from="", date_to=""):
    today_iso = date.today().isoformat()
    where_parts, where_params = build_dashboard_due_filter(date_from, date_to, "due_date")
    where_parts = ["user_id = %s"] + where_parts
    where_params = [user_id] + where_params
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    total = pg_fetch_one_dict(
        f"SELECT COUNT(*) AS total FROM tasks {where_clause}",
        tuple(where_params),
    )["total"]

    completed_parts = list(where_parts) + ["completed = %s"]
    completed_params = list(where_params) + [True]
    completed = pg_fetch_one_dict(
        f"SELECT COUNT(*) AS total FROM tasks WHERE {' AND '.join(completed_parts)}",
        tuple(completed_params),
    )["total"]

    pending_parts = list(where_parts) + ["completed = %s"]
    pending_params = list(where_params) + [False]
    pending = pg_fetch_one_dict(
        f"SELECT COUNT(*) AS total FROM tasks WHERE {' AND '.join(pending_parts)}",
        tuple(pending_params),
    )["total"]

    overdue_parts = list(where_parts) + [
        "completed = %s",
        "due_date IS NOT NULL",
        "due_date < %s",
    ]
    overdue_params = list(where_params) + [False, today_iso]
    overdue = pg_fetch_one_dict(
        f"SELECT COUNT(*) AS total FROM tasks WHERE {' AND '.join(overdue_parts)}",
        tuple(overdue_params),
    )["total"]
    return {
        "total": total,
        "completed": completed,
        "pending": pending,
        "overdue": overdue,
    }


def get_productivity_metrics(user_id, date_from="", date_to=""):
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    today_iso = today.isoformat()
    week_start_iso = week_start.isoformat()
    where_parts, where_params = build_dashboard_due_filter(date_from, date_to, "due_date")
    where_parts = ["user_id = %s"] + where_parts
    where_params = [user_id] + where_params
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    totals = pg_fetch_one_dict(
        "SELECT "
        "COUNT(*) AS total, "
        "SUM(CASE WHEN completed = TRUE THEN 1 ELSE 0 END) AS completed, "
        "SUM(CASE WHEN completed = FALSE THEN 1 ELSE 0 END) AS pending, "
        "SUM(CASE WHEN completed = FALSE AND due_date IS NOT NULL "
        "AND due_date < %s THEN 1 ELSE 0 END) AS overdue "
        f"FROM tasks {where_clause}",
        tuple([today_iso] + where_params),
    )

    priority_rows = pg_fetch_all_dicts(
        f"SELECT priority, COUNT(*) AS total FROM tasks {where_clause} GROUP BY priority",
        tuple(where_params),
    )

    category_rows = pg_fetch_all_dicts(
        "SELECT "
        "CASE WHEN category IS NULL OR TRIM(category) = '' THEN '(Sin categoría)' "
        "ELSE category END AS category_name, "
        "COUNT(*) AS total "
        f"FROM tasks {where_clause} GROUP BY category_name ORDER BY total DESC, category_name ASC",
        tuple(where_params),
    )

    if date_from or date_to:
        note_task_rows = pg_fetch_all_dicts(
            "SELECT id FROM tasks "
            f"{where_clause} "
            "ORDER BY id ASC",
            tuple(where_params),
        )
        note_task_ids = [row["id"] for row in note_task_rows]
        if note_task_ids:
            placeholders = ",".join(["?"] * len(note_task_ids))
            with get_connection() as conn:
                notes_row = conn.execute(
                    "SELECT "
                    "SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) AS completed, "
                    "SUM(CASE WHEN completed = 0 THEN 1 ELSE 0 END) AS pending "
                    f"FROM subtasks WHERE task_id IN ({placeholders})",
                    tuple(note_task_ids),
                ).fetchone()
        else:
            notes_row = {"completed": 0, "pending": 0}
    else:
        note_task_rows = pg_fetch_all_dicts(
            "SELECT id FROM tasks WHERE user_id = %s ORDER BY id ASC",
            (user_id,),
        )
        note_task_ids = [row["id"] for row in note_task_rows]
        if note_task_ids:
            placeholders = ",".join(["?"] * len(note_task_ids))
            with get_connection() as conn:
                notes_row = conn.execute(
                    "SELECT "
                    "SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) AS completed, "
                    "SUM(CASE WHEN completed = 0 THEN 1 ELSE 0 END) AS pending "
                    f"FROM subtasks WHERE task_id IN ({placeholders})",
                    tuple(note_task_ids),
                ).fetchone()
        else:
            notes_row = {"completed": 0, "pending": 0}
    if pg_task_has("completed_at"):
        completed_today_parts = list(where_parts) + [
            "completed = TRUE",
            "completed_at = %s",
        ]
        completed_today_params = list(where_params) + [today_iso]
        completed_today = pg_fetch_one_dict(
            f"SELECT COUNT(*) AS total FROM tasks WHERE {' AND '.join(completed_today_parts)}",
            tuple(completed_today_params),
        )["total"]
        completed_week_parts = list(where_parts) + [
            "completed = TRUE",
            "completed_at IS NOT NULL",
            "completed_at >= %s",
        ]
        completed_week_params = list(where_params) + [week_start_iso]
        completed_week = pg_fetch_one_dict(
            f"SELECT COUNT(*) AS total FROM tasks WHERE {' AND '.join(completed_week_parts)}",
            tuple(completed_week_params),
        )["total"]
    else:
        completed_today = 0
        completed_week = 0

    total = totals["total"] or 0
    completed = totals["completed"] or 0
    completion_rate = round((completed / total) * 100, 1) if total else 0.0

    priority_map = {"alta": 0, "media": 0, "baja": 0}
    for row in priority_rows:
        if row["priority"] in priority_map:
            priority_map[row["priority"]] = row["total"]

    return {
        "total": total,
        "completed": completed,
        "pending": totals["pending"] or 0,
        "overdue": totals["overdue"] or 0,
        "priority": priority_map,
        "categories": [
            {"name": row["category_name"], "total": row["total"]}
            for row in category_rows
        ],
        "notes": {
            "completed": notes_row["completed"] or 0,
            "pending": notes_row["pending"] or 0,
        },
        "subtasks": {  # Compatibilidad temporal
            "completed": notes_row["completed"] or 0,
            "pending": notes_row["pending"] or 0,
        },
        "completed_today": completed_today,
        "completed_week": completed_week,
        "completion_rate": completion_rate,
    }


def get_dashboard_hub_data(user_id, date_from="", date_to=""):
    where_parts, where_params = build_dashboard_due_filter(date_from, date_to, "due_date")
    where_parts = ["user_id = %s"] + where_parts
    where_params = [user_id] + where_params
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    recent_rows = pg_fetch_all_dicts(
        f"SELECT {pg_task_select_clause()} "
        f"FROM tasks {where_clause} "
        "ORDER BY id DESC LIMIT 5",
        tuple(where_params),
    )
    recent_tasks = []
    for row in recent_rows:
        if to_bool(row["completed"]):
            status = "Completada"
            time_ref = row.get("completed_at") or row.get("due_date")
        else:
            status = "Pendiente"
            time_ref = row.get("due_date")
        recent_tasks.append(
            {
                "id": row["id"],
                "title": row["title"],
                "status": status,
                "priority": row["priority"] if row["priority"] in VALID_PRIORITIES else "media",
                "time_ago": relative_time_label(time_ref),
            }
        )

    with get_connection() as conn:
        last_completed_row = conn.execute(
            "SELECT created_at FROM activity_log "
            "WHERE action_type IN ('task_completed', 'task_recurrent_completed') "
            "AND user_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        if date_from or date_to:
            note_task_rows = pg_fetch_all_dicts(
                "SELECT id FROM tasks "
                f"{where_clause} "
                "ORDER BY id ASC",
                tuple(where_params),
            )
            note_task_ids = [row["id"] for row in note_task_rows]
            if note_task_ids:
                placeholders = ",".join(["?"] * len(note_task_ids))
                note_rows = conn.execute(
                    f"SELECT task_id, title, completed FROM subtasks "
                    f"WHERE task_id IN ({placeholders}) "
                    "ORDER BY id DESC LIMIT 4",
                    tuple(note_task_ids),
                ).fetchall()
            else:
                note_rows = []
        else:
            note_task_rows = pg_fetch_all_dicts(
                "SELECT id FROM tasks WHERE user_id = %s ORDER BY id ASC",
                (user_id,),
            )
            note_task_ids = [row["id"] for row in note_task_rows]
            if note_task_ids:
                placeholders = ",".join(["?"] * len(note_task_ids))
                note_rows = conn.execute(
                    f"SELECT task_id, title, completed FROM subtasks "
                    f"WHERE task_id IN ({placeholders}) "
                    "ORDER BY id DESC LIMIT 4",
                    tuple(note_task_ids),
                ).fetchall()
            else:
                note_rows = []

    last_completed_label = "Sin tareas completadas aún"
    if last_completed_row is not None:
        last_completed_label = f"Última tarea completada: {relative_time_label(last_completed_row['created_at'])}"

    task_ids = list({row["task_id"] for row in note_rows if row["task_id"] is not None})
    task_titles = {}
    if task_ids:
        placeholders = ",".join(["%s"] * len(task_ids))
        title_rows = pg_fetch_all_dicts(
            f"SELECT id, title FROM tasks WHERE user_id = %s AND id IN ({placeholders})",
            tuple([user_id] + task_ids),
        )
        task_titles = {row["id"]: row["title"] for row in title_rows}

    recent_notes = []
    for row in note_rows:
        recent_notes.append(
            {
                "task_title": task_titles.get(row["task_id"], "Tarea"),
                "text": row["title"],
                "completed": bool(row["completed"]),
            }
        )

    return {
        "last_completed_label": last_completed_label,
        "recent_tasks": recent_tasks,
        "recent_notes": recent_notes,
    }


def get_stats_view(user_id, range_days):
    today = date.today()
    start_day = today - timedelta(days=range_days - 1)
    start_iso = start_day.isoformat()
    labels = [
        (start_day + timedelta(days=index)).strftime("%d %b")
        for index in range(range_days)
    ]
    created_series = [0] * range_days
    completed_series = [0] * range_days
    weekday_completed = [0] * 7
    created_dates_by_task = {}
    weekday_labels = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]

    tasks = pg_fetch_all_dicts(
        f"SELECT {pg_task_select_clause()} FROM tasks WHERE user_id = %s",
        (user_id,),
    )
    task_ids_for_user = [row["id"] for row in tasks]
    with get_connection() as conn:
        if task_ids_for_user:
            placeholders = ",".join(["?"] * len(task_ids_for_user))
            notes_row = conn.execute(
                "SELECT "
                "SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) AS completed, "
                "SUM(CASE WHEN completed = 0 THEN 1 ELSE 0 END) AS pending "
                f"FROM subtasks WHERE task_id IN ({placeholders})",
                tuple(task_ids_for_user),
            ).fetchone()
        else:
            notes_row = {"completed": 0, "pending": 0}
        activity_rows = conn.execute(
            "SELECT action_type, task_id, created_at "
            "FROM activity_log "
            "WHERE action_type IN ('task_created', 'task_completed', 'task_recurrent_completed') "
            "AND created_at >= ? AND user_id = ?",
            (start_iso, user_id),
        ).fetchall()
        created_lookup_rows = conn.execute(
            "SELECT task_id, created_at FROM activity_log "
            "WHERE action_type = 'task_created' AND task_id IS NOT NULL AND user_id = ? "
            "ORDER BY id ASC"
            ,
            (user_id,),
        ).fetchall()
        completion_days_rows = conn.execute(
            "SELECT DISTINCT SUBSTR(created_at, 1, 10) AS day "
            "FROM activity_log "
            "WHERE action_type IN ('task_completed', 'task_recurrent_completed') "
            "AND user_id = ? "
            "ORDER BY day DESC",
            (user_id,),
        ).fetchall()

    for row in created_lookup_rows:
        task_id = row["task_id"]
        if task_id is None or task_id in created_dates_by_task:
            continue
        created_raw = (row["created_at"] or "")[:10]
        try:
            created_dates_by_task[task_id] = date.fromisoformat(created_raw)
        except ValueError:
            continue

    for row in activity_rows:
        raw_date = (row["created_at"] or "")[:10]
        try:
            parsed = date.fromisoformat(raw_date)
        except ValueError:
            continue
        index = (parsed - start_day).days
        if index < 0 or index >= range_days:
            continue
        if row["action_type"] == "task_created":
            created_series[index] += 1
        elif row["action_type"] in {"task_completed", "task_recurrent_completed"}:
            completed_series[index] += 1
            weekday_completed[parsed.weekday()] += 1

    total_tasks = len(tasks)
    completed_total = 0
    pending_total = 0
    in_progress_total = 0
    priority_counts = {"alta": 0, "media": 0, "baja": 0}
    category_counts = {}
    created_in_range = sum(created_series)
    completed_in_range = 0
    cycle_days = []

    for task in tasks:
        is_completed = to_bool(task["completed"])
        if is_completed:
            completed_total += 1
        else:
            pending_total += 1
            if (task["recurrence"] or "").strip():
                in_progress_total += 1

        priority = task["priority"] if task["priority"] in priority_counts else "media"
        priority_counts[priority] += 1

        category_name = (task["category"] or "").strip() or "(Sin categoría)"
        category_counts[category_name] = category_counts.get(category_name, 0) + 1

        completed_raw = (task["completed_at"] or "").strip()
        if completed_raw:
            completed_date = None
            try:
                completed_date = date.fromisoformat(completed_raw[:10])
                if completed_date >= start_day:
                    completed_in_range += 1
            except ValueError:
                pass

            created_date = created_dates_by_task.get(task["id"])
            if created_date is not None and completed_date is not None:
                delta_days = (completed_date - created_date).days
                if delta_days >= 0:
                    cycle_days.append(delta_days)

    completion_rate = round((completed_total / total_tasks) * 100, 1) if total_tasks else 0.0
    avg_cycle_days = round(sum(cycle_days) / len(cycle_days), 1) if cycle_days else None
    categories_sorted = sorted(
        [{"name": name, "total": total} for name, total in category_counts.items()],
        key=lambda item: (-item["total"], item["name"].lower()),
    )
    range_label = f"Últimos {range_days} días"

    streak = 0
    completion_days = set()
    for row in completion_days_rows:
        raw_day = (row["day"] or "").strip()
        try:
            completion_days.add(date.fromisoformat(raw_day))
        except ValueError:
            continue
    cursor_day = date.today()
    while cursor_day in completion_days:
        streak += 1
        cursor_day -= timedelta(days=1)

    best_weekday_idx = max(range(7), key=lambda index: weekday_completed[index]) if any(weekday_completed) else None
    best_weekday_label = weekday_labels[best_weekday_idx] if best_weekday_idx is not None else "Sin datos"
    best_weekday_value = weekday_completed[best_weekday_idx] if best_weekday_idx is not None else 0

    midpoint = max(1, range_days // 2)
    first_half = sum(completed_series[:midpoint])
    second_half = sum(completed_series[midpoint:])
    if first_half == 0 and second_half == 0:
        trend_label = "Sin variación"
        trend_delta = 0.0
    elif first_half == 0:
        trend_label = "Mejora fuerte"
        trend_delta = 100.0
    else:
        trend_delta = round(((second_half - first_half) / first_half) * 100, 1)
        if trend_delta > 0:
            trend_label = "Mejorando"
        elif trend_delta < 0:
            trend_label = "Bajó ritmo"
        else:
            trend_label = "Sin variación"

    return {
        "range_days": range_days,
        "range_label": range_label,
        "cards": {
            "total": total_tasks,
            "completed": completed_in_range,
            "created": created_in_range,
            "pending": pending_total,
            "in_progress": in_progress_total,
            "completion_rate": completion_rate,
            "avg_cycle_days": avg_cycle_days,
            "streak": streak,
        },
        "priority": priority_counts,
        "categories": categories_sorted,
        "notes": {
            "completed": notes_row["completed"] or 0,
            "pending": notes_row["pending"] or 0,
        },
        "subtasks": {  # Compatibilidad temporal
            "completed": notes_row["completed"] or 0,
            "pending": notes_row["pending"] or 0,
        },
        "charts": {
            "labels": labels,
            "completed_trend": completed_series,
            "created_vs_completed": {
                "created": created_series,
                "completed": completed_series,
            },
            "priority_labels": ["Alta", "Media", "Baja"],
            "priority_values": [
                priority_counts["alta"],
                priority_counts["media"],
                priority_counts["baja"],
            ],
            "weekly_labels": weekday_labels,
            "weekly_completed": weekday_completed,
        },
        "insights": {
            "best_day_label": best_weekday_label,
            "best_day_value": best_weekday_value,
            "trend_label": trend_label,
            "trend_delta": trend_delta,
            "trend_period_label": f"Comparado con los {midpoint} días previos",
        },
    }


def get_notes_map(task_ids):
    if not task_ids:
        return {}
    placeholders = ",".join(["?"] * len(task_ids))
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id, task_id, title, completed "
            f"FROM subtasks WHERE task_id IN ({placeholders}) "
            "ORDER BY id ASC",
            tuple(task_ids),
        ).fetchall()
    grouped = {task_id: [] for task_id in task_ids}
    for row in rows:
        grouped.setdefault(row["task_id"], []).append(
            {
                "id": row["id"],
                "text": row["title"],
                "completed": bool(row["completed"]),
            }
        )
    return grouped


def get_subtasks_map(task_ids):
    # Compatibilidad temporal con llamadas existentes durante la transición.
    return get_notes_map(task_ids)


def get_date_view(user_id, date_from="", date_to=""):
    today = date.today()
    grouped = {"overdue": [], "today": [], "upcoming": []}
    where_parts = ["user_id = %s", "completed = %s", "due_date IS NOT NULL"]
    where_params = [user_id, False]
    if date_from:
        where_parts.append("due_date >= %s")
        where_params.append(date_from)
    if date_to:
        where_parts.append("due_date <= %s")
        where_params.append(date_to)
    rows = pg_fetch_all_dicts(
        "SELECT id, title, due_date, priority, category "
        "FROM tasks "
        f"WHERE {' AND '.join(where_parts)} "
        "ORDER BY due_date ASC, id DESC",
        tuple(where_params),
    )

    for row in rows:
        try:
            due_date = to_iso_date(row.get("due_date"))
            due = date.fromisoformat(due_date) if due_date else None
        except ValueError:
            continue
        if due is None:
            continue

        item = {
            "id": row["id"],
            "text": row["title"],
            "due_date": due_date,
            "priority": row["priority"],
            "category": (row["category"] or "").strip(),
        }
        if due < today:
            grouped["overdue"].append(item)
        elif due == today:
            grouped["today"].append(item)
        else:
            grouped["upcoming"].append(item)
    return grouped


def get_reminders(user_id, days_ahead=3, date_from="", date_to=""):
    today = date.today()
    limit = today + timedelta(days=days_ahead)
    reminders = {"overdue": [], "upcoming": [], "days_ahead": days_ahead}
    where_parts = ["user_id = %s", "completed = %s", "due_date IS NOT NULL"]
    where_params = [user_id, False]
    if date_from:
        where_parts.append("due_date >= %s")
        where_params.append(date_from)
    if date_to:
        where_parts.append("due_date <= %s")
        where_params.append(date_to)

    rows = pg_fetch_all_dicts(
        "SELECT id, title, due_date FROM tasks "
        f"WHERE {' AND '.join(where_parts)} "
        "ORDER BY due_date ASC, id DESC",
        tuple(where_params),
    )

    for row in rows:
        try:
            due_date = to_iso_date(row.get("due_date"))
            due = date.fromisoformat(due_date) if due_date else None
        except ValueError:
            continue
        if due is None:
            continue
        item = {"id": row["id"], "text": row["title"], "due_date": due_date}
        if due < today:
            reminders["overdue"].append(item)
        elif today <= due <= limit:
            reminders["upcoming"].append(item)
    return reminders


def get_category_options():
    owner_id = current_user_id()
    if owner_id is None:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT name FROM categories WHERE user_id = ? ORDER BY name COLLATE NOCASE ASC",
            (owner_id,),
        ).fetchall()
    return [row["name"] for row in rows]


def get_priority_value():
    value = request.form.get("priority", "media").strip().lower()
    return value if value in VALID_PRIORITIES else "media"


def get_due_date_value():
    value = request.form.get("due_date", "").strip()
    if not value:
        return None
    try:
        date.fromisoformat(value)
        return value
    except ValueError:
        return None


def parse_iso_date(value):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        date.fromisoformat(raw)
        return raw
    except ValueError:
        return None


def get_recurrence_value():
    value = request.form.get("recurrence", "").strip().lower()
    return value if value in VALID_RECURRENCES else ""


def get_category_value():
    value = request.form.get("category", "").strip()
    return value


def get_project_value():
    raw_value = request.form.get("project_id", "").strip()
    if not raw_value:
        return None
    try:
        parsed = int(raw_value)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def normalize_project_id(project_id, user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return None
    if project_id is None:
        return None
    row = pg_fetch_one_dict(
        "SELECT id FROM projects WHERE id = %s AND user_id = %s",
        (project_id, owner_id),
    )
    return project_id if row is not None else None


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


def ensure_category_exists(name, user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return
    cleaned = (name or "").strip()
    if not cleaned:
        return
    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO categories (user_id, name) VALUES (?, ?)",
            (owner_id, cleaned),
        )


def ensure_projects_table():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    color TEXT DEFAULT '',
                    due_date DATE,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE tasks
                ADD COLUMN IF NOT EXISTS project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL
                """
            )
            cur.execute(
                """
                ALTER TABLE projects
                ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE CASCADE
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_projects_user_id ON projects(user_id)
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_user_name_unique
                ON projects(user_id, LOWER(name))
                """
            )
        conn.commit()


def ensure_users_table():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT,
                    auth_provider TEXT NOT NULL DEFAULT 'local',
                    google_id TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS name TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS email TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS password_hash TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS auth_provider TEXT NOT NULL DEFAULT 'local'
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS google_id TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_unique
                ON users (LOWER(email))
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS users_google_id_unique
                ON users (google_id)
                WHERE google_id IS NOT NULL
                """
            )
        conn.commit()


def ensure_tags_tables():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tags (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    color TEXT NOT NULL DEFAULT '#22c55e'
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE tags
                ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE CASCADE
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tags_user_id ON tags(user_id)
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tags_user_name_unique
                ON tags(user_id, LOWER(name))
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_tags (
                    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                    PRIMARY KEY (task_id, tag_id)
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE tasks
                ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE CASCADE
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)
                """
            )
        conn.commit()


def get_projects(user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return []
    rows = pg_fetch_all_dicts(
        "SELECT id, name, COALESCE(description, '') AS description, "
        "COALESCE(color, '') AS color, "
        "TO_CHAR(due_date, 'YYYY-MM-DD') AS due_date, "
        "COALESCE(status, 'active') AS status, "
        "TO_CHAR(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS') AS created_at "
        "FROM projects WHERE user_id = %s ORDER BY id DESC",
        (owner_id,),
    )
    return rows


def get_tags(user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return []
    rows = pg_fetch_all_dicts(
        "SELECT t.id, t.name, COALESCE(t.color, '#22c55e') AS color, "
        "COUNT(tk.id) AS task_count "
        "FROM tags t "
        "LEFT JOIN task_tags tt ON tt.tag_id = t.id "
        "LEFT JOIN tasks tk ON tk.id = tt.task_id "
        "AND tk.user_id = t.user_id "
        "WHERE t.user_id = %s "
        "GROUP BY t.id, t.name, t.color "
        "ORDER BY LOWER(t.name) ASC",
        (owner_id,),
    )
    return rows


def get_selected_tag_id():
    raw = request.args.get("tag_id") or request.form.get("tag_id") or ""
    raw = raw.strip()
    if not raw:
        return None
    try:
        parsed = int(raw)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def get_tag_ids_from_form():
    raw_values = request.form.getlist("tag_ids")
    normalized = []
    for value in raw_values:
        text = (value or "").strip()
        if not text:
            continue
        try:
            tag_id = int(text)
        except ValueError:
            continue
        if tag_id > 0 and tag_id not in normalized:
            normalized.append(tag_id)
    return normalized


def normalize_tag_ids(tag_ids, user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return []
    if not tag_ids:
        return []
    placeholders = ",".join(["%s"] * len(tag_ids))
    rows = pg_fetch_all_dicts(
        f"SELECT id FROM tags WHERE user_id = %s AND id IN ({placeholders})",
        tuple([owner_id] + tag_ids),
    )
    existing_ids = {row["id"] for row in rows}
    return [tag_id for tag_id in tag_ids if tag_id in existing_ids]


def replace_task_tags(task_id, tag_ids, user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return
    task_row = pg_fetch_one_dict(
        "SELECT id FROM tasks WHERE id = %s AND user_id = %s",
        (task_id, owner_id),
    )
    if task_row is None:
        return
    valid_tag_ids = normalize_tag_ids(tag_ids, user_id=owner_id)
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM task_tags WHERE task_id = %s", (task_id,))
            for tag_id in valid_tag_ids:
                cur.execute(
                    "INSERT INTO task_tags (task_id, tag_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (task_id, tag_id),
                )
        pg_conn.commit()


def get_task_tags_map(task_ids, user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return {}
    if not task_ids:
        return {}
    placeholders = ",".join(["%s"] * len(task_ids))
    rows = pg_fetch_all_dicts(
        "SELECT tt.task_id, t.id AS tag_id, t.name, COALESCE(t.color, '#22c55e') AS color "
        "FROM task_tags tt "
        "INNER JOIN tags t ON t.id = tt.tag_id "
        "INNER JOIN tasks tk ON tk.id = tt.task_id "
        f"WHERE tt.task_id IN ({placeholders}) AND tk.user_id = %s AND t.user_id = %s "
        "ORDER BY LOWER(t.name) ASC",
        tuple(task_ids + [owner_id, owner_id]),
    )
    grouped = {task_id: [] for task_id in task_ids}
    for row in rows:
        grouped.setdefault(row["task_id"], []).append(
            {"id": row["tag_id"], "name": row["name"], "color": row["color"]}
        )
    return grouped


def get_project_progress_map(user_id=None):
    owner_id = user_id if user_id is not None else current_user_id()
    if owner_id is None:
        return {}
    rows = pg_fetch_all_dicts(
        "SELECT p.id AS project_id, "
        "COUNT(t.id) AS total_tasks, "
        "SUM(CASE WHEN t.completed = TRUE THEN 1 ELSE 0 END) AS completed_tasks "
        "FROM projects p "
        "LEFT JOIN tasks t ON t.project_id = p.id AND t.user_id = p.user_id "
        "WHERE p.user_id = %s "
        "GROUP BY p.id",
        (owner_id,),
    )
    progress_map = {}
    for row in rows:
        total = row["total_tasks"] or 0
        completed = row["completed_tasks"] or 0
        percent = round((completed / total) * 100, 1) if total else 0.0
        progress_map[row["project_id"]] = {
            "total_tasks": total,
            "completed_tasks": completed,
            "progress_percent": percent,
        }
    return progress_map


def get_selected_project_id():
    raw = request.args.get("project_id") or request.form.get("project_id") or ""
    raw = raw.strip()
    if not raw:
        return None
    try:
        parsed = int(raw)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def get_default_user_id():
    row = pg_fetch_one_dict("SELECT id FROM users ORDER BY id ASC LIMIT 1")
    return row["id"] if row else None


def backfill_user_ownership(default_user_id):
    if default_user_id is None:
        return
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("UPDATE tasks SET user_id = %s WHERE user_id IS NULL", (default_user_id,))
            cur.execute("UPDATE projects SET user_id = %s WHERE user_id IS NULL", (default_user_id,))
            cur.execute("UPDATE tags SET user_id = %s WHERE user_id IS NULL", (default_user_id,))
        pg_conn.commit()


def init_db():
    ensure_users_table()
    ensure_projects_table()
    ensure_tags_tables()
    default_user_id = get_default_user_id()
    backfill_user_ownership(default_user_id)
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                completed_at TEXT,
                priority TEXT NOT NULL DEFAULT 'media',
                recurrence TEXT NOT NULL DEFAULT '',
                due_date TEXT,
                category TEXT NOT NULL DEFAULT '',
                position INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL DEFAULT 0,
                name TEXT NOT NULL,
                UNIQUE(user_id, name)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subtasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(task_id) REFERENCES tasks(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL DEFAULT 0,
                action_type TEXT NOT NULL,
                task_id INTEGER,
                task_title TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            """
        )
        columns = conn.execute("PRAGMA table_info(tasks)").fetchall()
        column_names = {column["name"] for column in columns}
        if "completed" not in column_names:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN completed INTEGER NOT NULL DEFAULT 0"
            )
        if "priority" not in column_names:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN priority TEXT NOT NULL DEFAULT 'media'"
            )
        if "recurrence" not in column_names:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN recurrence TEXT NOT NULL DEFAULT ''"
            )
        if "completed_at" not in column_names:
            conn.execute("ALTER TABLE tasks ADD COLUMN completed_at TEXT")
        if "due_date" not in column_names:
            conn.execute("ALTER TABLE tasks ADD COLUMN due_date TEXT")
        if "category" not in column_names:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN category TEXT NOT NULL DEFAULT ''"
            )
        if "position" not in column_names:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN position INTEGER NOT NULL DEFAULT 0"
            )
        if "notes" not in column_names:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN notes TEXT NOT NULL DEFAULT ''"
            )
        if "project_id" not in column_names:
            conn.execute("ALTER TABLE tasks ADD COLUMN project_id INTEGER")
        if "user_id" not in column_names:
            conn.execute("ALTER TABLE tasks ADD COLUMN user_id INTEGER")
        category_columns = conn.execute("PRAGMA table_info(categories)").fetchall()
        category_column_names = {column["name"] for column in category_columns}
        if "user_id" not in category_column_names:
            conn.execute("ALTER TABLE categories ADD COLUMN user_id INTEGER NOT NULL DEFAULT 0")
        activity_columns = conn.execute("PRAGMA table_info(activity_log)").fetchall()
        activity_column_names = {column["name"] for column in activity_columns}
        if "user_id" not in activity_column_names:
            conn.execute("ALTER TABLE activity_log ADD COLUMN user_id INTEGER NOT NULL DEFAULT 0")

        if default_user_id is not None:
            conn.execute("UPDATE tasks SET user_id = ? WHERE user_id IS NULL OR user_id = 0", (default_user_id,))
            conn.execute(
                "UPDATE categories SET user_id = ? WHERE user_id IS NULL OR user_id = 0",
                (default_user_id,),
            )
            conn.execute(
                "UPDATE activity_log SET user_id = ? WHERE user_id IS NULL OR user_id = 0",
                (default_user_id,),
            )

        conn.execute("UPDATE tasks SET position = id WHERE position IS NULL OR position = 0")
        conn.execute(
            "UPDATE tasks SET category = '' "
            "WHERE category IS NULL OR TRIM(category) = '' OR category = 'General'"
        )
        conn.execute(
            "UPDATE tasks SET recurrence = '' "
            "WHERE recurrence IS NULL OR recurrence NOT IN ('', 'daily', 'weekly', 'monthly')"
        )
        conn.execute(
            "UPDATE tasks SET completed_at = ? "
            "WHERE completed = 1 AND (completed_at IS NULL OR TRIM(completed_at) = '')",
            (date.today().isoformat(),),
        )
        conn.execute("UPDATE tasks SET completed_at = NULL WHERE completed = 0")
        conn.execute("DELETE FROM categories WHERE name = 'General'")
        if default_user_id is not None:
            conn.execute(
                "INSERT OR IGNORE INTO categories (user_id, name) "
                "SELECT DISTINCT ?, category FROM tasks "
                "WHERE category IS NOT NULL AND TRIM(category) != ''",
                (default_user_id,),
            )


@app.before_request
def require_login_for_app_routes():
    if request.endpoint is None:
        return None
    allowed_endpoints = {"static", "login", "register", "auth_google", "google_callback"}
    if request.endpoint in allowed_endpoints:
        return None
    if current_user.is_authenticated:
        return None
    return redirect(url_for("login", next=request.full_path if request.query_string else request.path))


@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        email = normalize_email(request.form.get("email"))
        password = request.form.get("password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        if not name:
            flash("El nombre es obligatorio.", "error")
        elif not is_valid_email(email):
            flash("Ingresa un email válido.", "error")
        elif not password:
            flash("La contraseña es obligatoria.", "error")
        elif password != confirm_password:
            flash("La confirmación de contraseña no coincide.", "error")
        elif get_user_by_email(email) is not None:
            flash("Ese email ya está registrado.", "error")
        else:
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO users (name, email, password_hash, auth_provider) "
                        "VALUES (%s, %s, %s, 'local') RETURNING id",
                        (name, email, generate_password_hash(password)),
                    )
                    inserted = cur.fetchone()
                    user_id = inserted[0] if inserted else None
                conn.commit()
            if user_id is not None:
                login_user(User(user_id, name, email))
                flash("Cuenta creada correctamente.", "ok")
                next_target = request.args.get("next") or request.form.get("next")
                if next_target and is_safe_redirect_target(next_target):
                    return redirect(next_target)
                return redirect(url_for("index"))
            flash("No se pudo crear la cuenta. Inténtalo nuevamente.", "error")

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        email = normalize_email(request.form.get("email"))
        password = request.form.get("password") or ""
        user_row = get_user_by_email(email)
        if user_row is None:
            flash("No existe una cuenta con ese email.", "error")
        elif user_row.get("auth_provider") != "local" and not user_row.get("password_hash"):
            flash("Esta cuenta requiere inicio con proveedor externo.", "error")
        elif not check_password_hash(user_row.get("password_hash") or "", password):
            flash("Contraseña incorrecta.", "error")
        else:
            login_user(User(user_row["id"], user_row["name"], user_row["email"]))
            flash("Inicio de sesión correcto.", "ok")
            next_target = request.args.get("next") or request.form.get("next")
            if next_target and is_safe_redirect_target(next_target):
                return redirect(next_target)
            return redirect(url_for("index"))

    return render_template("login.html")


@app.route("/auth/google")
@app.route("/login/google")
def auth_google():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    client = get_google_oauth_client()
    if client is None:
        if OAUTH_IMPORT_ERROR:
            flash("Google login no está disponible: falta dependencia OAuth.", "error")
        else:
            flash("Google login no está configurado en este entorno.", "error")
        return redirect(url_for("login"))
    redirect_uri = url_for("google_callback", _external=True)
    return client.authorize_redirect(redirect_uri)


@app.route("/auth/google/callback")
def google_callback():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    client = get_google_oauth_client()
    if client is None:
        if OAUTH_IMPORT_ERROR:
            flash("Google login no está disponible: falta dependencia OAuth.", "error")
        else:
            flash("Google login no está configurado en este entorno.", "error")
        return redirect(url_for("login"))

    try:
        token = client.authorize_access_token()
    except Exception:
        flash("No se pudo completar la autenticación con Google.", "error")
        return redirect(url_for("login"))

    userinfo = token.get("userinfo") if isinstance(token, dict) else None
    if not userinfo:
        try:
            userinfo = client.userinfo()
        except Exception:
            userinfo = None

    if not userinfo:
        flash("No fue posible obtener los datos del perfil de Google.", "error")
        return redirect(url_for("login"))

    email = normalize_email(userinfo.get("email"))
    name = (userinfo.get("name") or userinfo.get("given_name") or "").strip()
    google_id = (userinfo.get("sub") or "").strip()

    if not email or not is_valid_email(email):
        flash("Google no devolvió un email válido.", "error")
        return redirect(url_for("login"))
    if not google_id:
        flash("Google no devolvió un identificador de cuenta.", "error")
        return redirect(url_for("login"))
    if not name:
        name = email.split("@")[0]

    existing_by_google = get_user_by_google_id(google_id)
    existing_by_email = get_user_by_email(email)

    if existing_by_google is not None:
        login_user(User(existing_by_google["id"], existing_by_google["name"], existing_by_google["email"]))
        flash("Inicio de sesión con Google correcto.", "ok")
        return redirect(url_for("index"))

    if existing_by_email is not None:
        if existing_by_email.get("google_id") and existing_by_email.get("google_id") != google_id:
            flash("Este email ya está vinculado a otra cuenta de Google.", "error")
            return redirect(url_for("login"))
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users "
                    "SET name = COALESCE(NULLIF(name, ''), %s), google_id = %s "
                    "WHERE id = %s",
                    (name, google_id, existing_by_email["id"]),
                )
            conn.commit()
        merged_user = get_user_by_email(email)
        login_user(User(merged_user["id"], merged_user["name"], merged_user["email"]))
        flash("Cuenta vinculada con Google correctamente.", "ok")
        return redirect(url_for("index"))

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (name, email, password_hash, auth_provider, google_id) "
                "VALUES (%s, %s, %s, 'google', %s) RETURNING id",
                (name, email, None, google_id),
            )
            inserted = cur.fetchone()
            user_id = inserted[0] if inserted else None
        conn.commit()

    if user_id is None:
        flash("No se pudo crear la cuenta con Google.", "error")
        return redirect(url_for("login"))

    login_user(User(user_id, name, email))
    flash("Inicio de sesión con Google correcto.", "ok")
    return redirect(url_for("index"))


@app.route("/logout", methods=["POST", "GET"])
@login_required
def logout():
    logout_user()
    flash("Sesión cerrada correctamente.", "ok")
    return redirect(url_for("login"))


@app.route("/")
def index():
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    edit_id = request.args.get("edit", type=int)
    new_task_id = request.args.get("new_task_id", type=int)
    current_filter = get_filter_value()
    search_query = get_search_value()
    date_search_query = get_date_search_value()
    quick_date = get_quick_date_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    current_month = get_calendar_month_value()
    stats_range_days = get_stats_range_days()
    dashboard_from = get_dashboard_from_value()
    dashboard_to = get_dashboard_to_value()
    selected_project_id = normalize_project_id(get_selected_project_id(), user_id=owner_id)
    selected_tag_raw = get_selected_tag_id()
    selected_tag_id = None
    if selected_tag_raw is not None:
        valid_tags = normalize_tag_ids([selected_tag_raw], user_id=owner_id)
        selected_tag_id = valid_tags[0] if valid_tags else None
    import_status = request.args.get("import_status", "").strip()
    import_message = request.args.get("import_message", "").strip()
    category_status = request.args.get("category_status", "").strip()
    category_message = request.args.get("category_message", "").strip()
    order_clause = pg_task_order_by(current_sort)
    pg_where_parts = ["user_id = %s"]
    pg_params = [owner_id]
    if current_filter == "pending":
        pg_where_parts.append("completed = %s")
        pg_params.append(False)
    elif current_filter == "completed":
        pg_where_parts.append("completed = %s")
        pg_params.append(True)
    if search_query:
        pg_where_parts.append("LOWER(category) LIKE LOWER(%s)")
        pg_params.append(f"%{search_query}%")
    if date_search_query:
        pg_where_parts.append("due_date = %s")
        pg_params.append(date_search_query)
    if quick_date == "today":
        pg_where_parts.append("due_date = %s")
        pg_params.append(date.today().isoformat())
    elif quick_date == "week":
        week_end = (date.today() + timedelta(days=6)).isoformat()
        pg_where_parts.append("due_date IS NOT NULL AND due_date >= %s AND due_date <= %s")
        pg_params.extend([date.today().isoformat(), week_end])
    if selected_project_id is not None:
        pg_where_parts.append("project_id = %s")
        pg_params.append(selected_project_id)
    if selected_tag_id is not None:
        pg_where_parts.append(
            "id IN (SELECT task_id FROM task_tags WHERE tag_id = %s)"
        )
        pg_params.append(selected_tag_id)
    pg_where_clause = f"WHERE {' AND '.join(pg_where_parts)}" if pg_where_parts else ""
    rows = pg_fetch_all_dicts(
        f"SELECT {pg_task_select_clause()} FROM tasks {pg_where_clause} ORDER BY {order_clause}",
        tuple(pg_params),
    )
    tasks = [build_task(row) for row in rows]
    notes_map = get_notes_map([task["id"] for task in tasks])
    tags_map = get_task_tags_map([task["id"] for task in tasks], user_id=owner_id)
    for task in tasks:
        task["notes_items"] = notes_map.get(task["id"], [])
        task["subtasks"] = notes_map.get(task["id"], [])  # Compatibilidad temporal
        task["tags"] = tags_map.get(task["id"], [])
        task["tag_ids"] = [item["id"] for item in task["tags"]]
    dashboard = get_dashboard_stats(owner_id, dashboard_from, dashboard_to)
    metrics = get_productivity_metrics(owner_id, dashboard_from, dashboard_to)
    dashboard_hub = get_dashboard_hub_data(owner_id, dashboard_from, dashboard_to)
    date_view = get_date_view(owner_id, dashboard_from, dashboard_to)
    reminders = get_reminders(owner_id, date_from=dashboard_from, date_to=dashboard_to)
    activity_logs = get_activity_logs()
    category_options = get_category_options()
    project_options = get_projects(user_id=owner_id)
    tag_options = get_tags(user_id=owner_id)
    project_progress_map = get_project_progress_map(user_id=owner_id)
    for project in project_options:
        project.update(project_progress_map.get(project["id"], {
            "total_tasks": 0,
            "completed_tasks": 0,
            "progress_percent": 0.0,
        }))
    selected_project = None
    if selected_project_id is not None:
        selected_project = next((item for item in project_options if item["id"] == selected_project_id), None)

    project_tasks = []
    if selected_project is not None:
        project_rows = pg_fetch_all_dicts(
            f"SELECT {pg_task_select_clause()} "
            "FROM tasks WHERE project_id = %s AND user_id = %s "
            "ORDER BY "
            + pg_task_order_by(current_sort),
            (selected_project_id, owner_id),
        )
        project_tasks = [build_task(row) for row in project_rows]
        project_tags_map = get_task_tags_map([task["id"] for task in project_tasks], user_id=owner_id)
        for task in project_tasks:
            task["tags"] = project_tags_map.get(task["id"], [])
    calendar_view = get_calendar_view(owner_id, current_month)
    stats_view = get_stats_view(owner_id, stats_range_days)
    return render_template(
        "index.html",
        tasks=tasks,
        edit_id=edit_id,
        current_filter=current_filter,
        current_search=search_query,
        current_date_search=date_search_query,
        current_sort=current_sort,
        current_quick_date=quick_date,
        current_nav=current_nav,
        current_month=current_month,
        stats_range_days=stats_range_days,
        dashboard_from=dashboard_from,
        dashboard_to=dashboard_to,
        import_status=import_status,
        import_message=import_message,
        category_status=category_status,
        category_message=category_message,
        dashboard=dashboard,
        metrics=metrics,
        dashboard_hub=dashboard_hub,
        date_view=date_view,
        reminders=reminders,
        calendar_view=calendar_view,
        stats_view=stats_view,
        activity_logs=activity_logs,
        category_options=category_options,
        project_options=project_options,
        selected_project_id=selected_project_id,
        selected_tag_id=selected_tag_id,
        tag_options=tag_options,
        selected_project=selected_project,
        project_tasks=project_tasks,
        new_task_id=new_task_id,
    )


@app.route("/partial/tasks")
def partial_tasks():
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    edit_id = request.args.get("edit", type=int)
    current_filter = get_filter_value()
    search_query = get_search_value()
    date_search_query = get_date_search_value()
    quick_date = get_quick_date_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    new_task_id = request.args.get("new_task_id", type=int)
    dashboard_from = get_dashboard_from_value()
    dashboard_to = get_dashboard_to_value()
    selected_project_id = normalize_project_id(get_selected_project_id(), user_id=owner_id)
    selected_tag_raw = get_selected_tag_id()
    selected_tag_id = None
    if selected_tag_raw is not None:
        valid_tags = normalize_tag_ids([selected_tag_raw], user_id=owner_id)
        selected_tag_id = valid_tags[0] if valid_tags else None

    order_clause = pg_task_order_by(current_sort)
    pg_where_parts = ["user_id = %s"]
    pg_params = [owner_id]
    if current_filter == "pending":
        pg_where_parts.append("completed = %s")
        pg_params.append(False)
    elif current_filter == "completed":
        pg_where_parts.append("completed = %s")
        pg_params.append(True)
    if search_query:
        pg_where_parts.append("LOWER(category) LIKE LOWER(%s)")
        pg_params.append(f"%{search_query}%")
    if date_search_query:
        pg_where_parts.append("due_date = %s")
        pg_params.append(date_search_query)
    if quick_date == "today":
        pg_where_parts.append("due_date = %s")
        pg_params.append(date.today().isoformat())
    elif quick_date == "week":
        week_end = (date.today() + timedelta(days=6)).isoformat()
        pg_where_parts.append("due_date IS NOT NULL AND due_date >= %s AND due_date <= %s")
        pg_params.extend([date.today().isoformat(), week_end])
    if selected_project_id is not None:
        pg_where_parts.append("project_id = %s")
        pg_params.append(selected_project_id)
    if selected_tag_id is not None:
        pg_where_parts.append(
            "id IN (SELECT task_id FROM task_tags WHERE tag_id = %s)"
        )
        pg_params.append(selected_tag_id)
    pg_where_clause = f"WHERE {' AND '.join(pg_where_parts)}" if pg_where_parts else ""

    rows = pg_fetch_all_dicts(
        f"SELECT {pg_task_select_clause()} FROM tasks {pg_where_clause} ORDER BY {order_clause}",
        tuple(pg_params),
    )
    tasks = [build_task(row) for row in rows]
    notes_map = get_notes_map([task["id"] for task in tasks])
    tags_map = get_task_tags_map([task["id"] for task in tasks], user_id=owner_id)
    for task in tasks:
        task["notes_items"] = notes_map.get(task["id"], [])
        task["subtasks"] = notes_map.get(task["id"], [])
        task["tags"] = tags_map.get(task["id"], [])
        task["tag_ids"] = [item["id"] for item in task["tags"]]
    project_options = get_projects(user_id=owner_id)
    tag_options = get_tags(user_id=owner_id)

    return render_template(
        "_tasks_list.html",
        tasks=tasks,
        edit_id=edit_id,
        current_filter=current_filter,
        current_search=search_query,
        current_date_search=date_search_query,
        current_quick_date=quick_date,
        current_sort=current_sort,
        current_nav=current_nav,
        new_task_id=new_task_id,
        dashboard_from=dashboard_from,
        dashboard_to=dashboard_to,
        project_options=project_options,
        selected_project_id=selected_project_id,
        selected_tag_id=selected_tag_id,
        tag_options=tag_options,
    )


@app.route("/add", methods=["POST"])
def add_task():
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    text = request.form.get("text", "").strip()
    priority = get_priority_value()
    recurrence = get_recurrence_value()
    due_date = get_due_date_value()
    category = get_category_value()
    project_id = normalize_project_id(get_project_value(), user_id=owner_id)
    tag_ids = get_tag_ids_from_form()
    notes = request.form.get("notes", "").strip()
    pg_task_id = None
    if text:
        ensure_category_exists(category, user_id=owner_id)
        columns = ["title", "category", "priority", "due_date", "completed", "notes"]
        values = [text, category, priority, due_date, False, notes]
        if pg_task_has("user_id"):
            columns.append("user_id")
            values.append(owner_id)
        if pg_task_has("project_id"):
            columns.append("project_id")
            values.append(project_id)
        if pg_task_has("recurrence"):
            columns.append("recurrence")
            values.append(recurrence)
        if pg_task_has("position"):
            max_position_row = pg_fetch_one_dict(
                "SELECT COALESCE(MAX(position), 0) AS max_position FROM tasks WHERE user_id = %s",
                (owner_id,),
            )
            next_position = (max_position_row["max_position"] or 0) + 1
            columns.append("position")
            values.append(next_position)

        placeholders = ", ".join(["%s"] * len(values))
        columns_sql = ", ".join(columns)
        with get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO tasks ({columns_sql}) VALUES ({placeholders}) RETURNING id",
                    tuple(values),
                )
                inserted = cur.fetchone()
                pg_task_id = inserted[0] if inserted else None
            pg_conn.commit()
        if pg_task_id is not None:
            replace_task_tags(pg_task_id, tag_ids, user_id=owner_id)
        log_event("task_created", text, task_id=pg_task_id, user_id=owner_id)
    return redirect_to_index(current_filter, search_query, current_sort, current_nav, quick_date_value=get_quick_date_value(), new_task_id=pg_task_id if text else None)


@app.route("/reorder", methods=["POST"])
def reorder_tasks():
    owner_id = current_user_id()
    if owner_id is None:
        return jsonify({"ok": False, "error": "no autenticado"}), 401
    payload = request.get_json(silent=True) or {}
    ordered_ids = payload.get("ordered_ids")
    if not isinstance(ordered_ids, list):
        return jsonify({"ok": False, "error": "ordered_ids inválido"}), 400
    clean_ids = []
    for value in ordered_ids:
        if not isinstance(value, int):
            return jsonify({"ok": False, "error": "id inválido"}), 400
        clean_ids.append(value)
    if not clean_ids:
        return jsonify({"ok": False, "error": "sin ids"}), 400

    placeholders = ",".join(["%s"] * len(clean_ids))
    existing_rows = pg_fetch_all_dicts(
        f"SELECT id FROM tasks WHERE user_id = %s AND id IN ({placeholders})",
        tuple([owner_id] + clean_ids),
    )
    existing_ids = {row["id"] for row in existing_rows}
    for task_id in clean_ids:
        if task_id not in existing_ids:
            return jsonify({"ok": False, "error": "id no encontrado"}), 400

    if pg_task_has("position"):
        with get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cur:
                for index, task_id in enumerate(clean_ids, start=1):
                    cur.execute(
                        "UPDATE tasks SET position = %s WHERE id = %s AND user_id = %s",
                        (index, task_id, owner_id),
                    )
            pg_conn.commit()
    return jsonify({"ok": True})


@app.route("/calendar/move", methods=["POST"])
def move_calendar_task():
    owner_id = current_user_id()
    if owner_id is None:
        return jsonify({"ok": False, "error": "no autenticado"}), 401
    payload = request.get_json(silent=True) or {}
    task_id = payload.get("task_id")
    new_date_raw = payload.get("new_date")
    if not isinstance(task_id, int):
        return jsonify({"ok": False, "error": "task_id inválido"}), 400
    new_date = parse_iso_date(new_date_raw)
    if not new_date:
        return jsonify({"ok": False, "error": "new_date inválido"}), 400

    task = pg_fetch_one_dict(
        "SELECT id, title FROM tasks WHERE id = %s AND user_id = %s",
        (task_id, owner_id),
    )
    if task is None:
        return jsonify({"ok": False, "error": "tarea no encontrada"}), 404

    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET due_date = %s WHERE id = %s AND user_id = %s",
                (new_date, task_id, owner_id),
            )
        pg_conn.commit()

    return jsonify({"ok": True, "task_id": task_id, "new_date": new_date})


@app.route("/edit/<int:task_id>", methods=["POST"])
def edit_task(task_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    text = request.form.get("text", "").strip()
    priority = get_priority_value()
    recurrence = get_recurrence_value()
    due_date = get_due_date_value()
    category = get_category_value()
    project_id = normalize_project_id(get_project_value(), user_id=owner_id)
    tag_ids = get_tag_ids_from_form()
    notes = request.form.get("notes", "").strip()
    if text:
        ensure_category_exists(category, user_id=owner_id)
        current = pg_fetch_one_dict(
            f"SELECT {pg_task_select_clause()} FROM tasks WHERE id = %s AND user_id = %s",
            (task_id, owner_id),
        )
        if current is None:
            return redirect_to_index(current_filter, search_query, current_sort, current_nav)

        update_parts = ["title = %s", "priority = %s", "due_date = %s", "category = %s"]
        update_values = [text, priority, due_date, category]
        if pg_task_has("project_id"):
            update_parts.append("project_id = %s")
            update_values.append(project_id)
        if pg_task_has("notes"):
            update_parts.append("notes = %s")
            update_values.append(notes)
        if pg_task_has("recurrence"):
            update_parts.append("recurrence = %s")
            update_values.append(recurrence)
        update_values.extend([task_id, owner_id])
        with get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"UPDATE tasks SET {', '.join(update_parts)} WHERE id = %s AND user_id = %s",
                    tuple(update_values),
                )
            pg_conn.commit()
        replace_task_tags(task_id, tag_ids, user_id=owner_id)
        if current is not None:
            changes = []
            if current["title"] != text:
                changes.append("texto")
            if to_iso_date(current.get("due_date")) != due_date:
                changes.append("fecha")
            if current["priority"] != priority:
                changes.append("prioridad")
            if (current["category"] or "") != category:
                changes.append("categoría")
            if current.get("project_id") != project_id:
                changes.append("proyecto")
            if (current.get("notes") or "") != notes:
                changes.append("notas")
            if current["recurrence"] != recurrence:
                changes.append("recurrencia")
            detail_text = f"Cambios: {', '.join(changes)}" if changes else "Sin cambios relevantes"
            log_event("task_edited", text, detail_text, task_id=task_id, user_id=owner_id)
    return redirect_to_index(current_filter, search_query, current_sort, current_nav)


@app.route("/categories/add", methods=["POST"])
def add_category():
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    new_category = request.form.get("new_category", "").strip()
    if new_category:
        ensure_category_exists(new_category, user_id=owner_id)
    return redirect_to_index(current_filter, search_query, current_sort, current_nav)


@app.route("/categories/delete", methods=["POST"])
def delete_category():
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    category_name = (request.form.get("category_name") or "").strip()
    if not category_name:
        return redirect_to_index(
            current_filter,
            search_query,
            current_sort,
            current_nav,
            category_status="error",
            category_message="Selecciona una categoría válida.",
        )

    in_use_row = pg_fetch_one_dict(
        "SELECT COUNT(*) AS total FROM tasks WHERE category = %s AND user_id = %s",
        (category_name, owner_id),
    )
    in_use_total = in_use_row["total"] if in_use_row is not None else 0
    if in_use_total:
        return redirect_to_index(
            current_filter,
            search_query,
            current_sort,
            current_nav,
            category_status="error",
            category_message=(
                f"No se pudo eliminar '{category_name}' porque está en uso "
                f"en {in_use_total} tarea{'s' if in_use_total != 1 else ''}."
            ),
        )

    with get_connection() as conn:
        conn.execute(
            "DELETE FROM categories WHERE user_id = ? AND name = ?",
            (owner_id, category_name),
        )

    return redirect_to_index(
        current_filter,
        search_query,
        current_sort,
        current_nav,
        category_status="deleted",
        category_message=f"Categoría '{category_name}' eliminada.",
    )


@app.route("/export", methods=["GET"])
def export_data():
    payload = build_export_payload()
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    response = make_response(content)
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response.headers["Content-Disposition"] = (
        f'attachment; filename="todo-backup-{date.today().isoformat()}.json"'
    )
    return response


@app.route("/import", methods=["POST"])
def import_data():
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    file = request.files.get("import_file")
    if file is None or not file.filename:
        return redirect(
            url_for(
                "index",
                filter=current_filter,
                q=search_query,
                sort=current_sort,
                nav=current_nav,
                import_status="error",
                import_message="Selecciona un archivo JSON válido.",
            )
        )
    try:
        raw = file.read()
        payload = json.loads(raw.decode("utf-8"))
        restore_from_payload(payload)
    except Exception as exc:
        return redirect(
            url_for(
                "index",
                filter=current_filter,
                q=search_query,
                sort=current_sort,
                nav=current_nav,
                import_status="error",
                import_message=f"Importación fallida: {str(exc)}",
            )
        )
    return redirect(
        url_for(
            "index",
            filter=current_filter,
            q=search_query,
            sort=current_sort,
            nav=current_nav,
            import_status="ok",
            import_message="Importación completada correctamente.",
        )
    )


@app.route("/notes/add/<int:task_id>", methods=["POST"])
def add_note(task_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    text = request.form.get("note_text", "").strip() or request.form.get("subtask_text", "").strip()
    if text:
        pg_task = pg_fetch_one_dict(
            "SELECT id, title FROM tasks WHERE id = %s AND user_id = %s",
            (task_id, owner_id),
        )
        if pg_task is None:
            return redirect_to_index(current_filter, search_query, current_sort, current_nav)
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO subtasks (task_id, title) VALUES (?, ?)",
                (task_id, text),
            )
            log_event(
                "note_created",
                pg_task["title"],
                details=f"Nota: {text}",
                task_id=task_id,
                conn=conn,
                user_id=owner_id,
            )
    return redirect_to_index(current_filter, search_query, current_sort, current_nav)


@app.route("/notes/toggle/<int:note_id>", methods=["POST"])
def toggle_note(note_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT s.completed, s.title, s.task_id "
            "FROM subtasks s "
            "WHERE s.id = ?",
            (note_id,)
        ).fetchone()
        if row is not None:
            task_owner = pg_fetch_one_dict(
                "SELECT id, title FROM tasks WHERE id = %s AND user_id = %s",
                (row["task_id"], owner_id),
            )
            if task_owner is None:
                return redirect_to_index(current_filter, search_query, current_sort, current_nav)
            new_value = 0 if row["completed"] else 1
            conn.execute(
                "UPDATE subtasks SET completed = ? WHERE id = ?",
                (new_value, note_id),
            )
            if new_value == 1:
                log_event(
                    "note_completed",
                    task_owner["title"],
                    details=f"Nota: {row['title']}",
                    task_id=row["task_id"],
                    conn=conn,
                    user_id=owner_id,
                )
    return redirect_to_index(current_filter, search_query, current_sort, current_nav)


@app.route("/notes/delete/<int:note_id>", methods=["POST"])
def delete_note(note_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT s.title, s.task_id "
            "FROM subtasks s "
            "WHERE s.id = ?",
            (note_id,),
        ).fetchone()
        if row is not None:
            task_owner = pg_fetch_one_dict(
                "SELECT id, title FROM tasks WHERE id = %s AND user_id = %s",
                (row["task_id"], owner_id),
            )
            if task_owner is None:
                return redirect_to_index(current_filter, search_query, current_sort, current_nav)
        else:
            task_owner = None
        conn.execute("DELETE FROM subtasks WHERE id = ?", (note_id,))
    if row is not None:
        log_event(
            "note_deleted",
            task_owner["title"] if task_owner is not None else "(sin título)",
            details=f"Nota: {row['title']}",
            task_id=row["task_id"],
            user_id=owner_id,
        )
    return redirect_to_index(current_filter, search_query, current_sort, current_nav)


@app.route("/subtasks/add/<int:task_id>", methods=["POST"])
def add_subtask(task_id):
    # Compatibilidad temporal con rutas previas.
    return add_note(task_id)


@app.route("/subtasks/toggle/<int:subtask_id>", methods=["POST"])
def toggle_subtask(subtask_id):
    # Compatibilidad temporal con rutas previas.
    return toggle_note(subtask_id)


@app.route("/subtasks/delete/<int:subtask_id>", methods=["POST"])
def delete_subtask(subtask_id):
    # Compatibilidad temporal con rutas previas.
    return delete_note(subtask_id)


@app.route("/toggle/<int:task_id>", methods=["POST"])
def toggle_task(task_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    task = pg_fetch_one_dict(
        f"SELECT {pg_task_select_clause()} FROM tasks WHERE id = %s AND user_id = %s",
        (task_id, owner_id),
    )
    if task is not None:
        currently_completed = to_bool(task["completed"])
        recurrence = (task.get("recurrence") or "").strip().lower()
        recurrence = recurrence if recurrence in VALID_RECURRENCES else ""
        parsed_due = None
        task_due_date = to_iso_date(task.get("due_date"))
        if task_due_date:
            try:
                parsed_due = date.fromisoformat(task_due_date)
            except ValueError:
                parsed_due = None

        if recurrence and not currently_completed:
            next_due = next_due_date(parsed_due, recurrence)
            with get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cur:
                    if pg_task_has("completed_at"):
                        cur.execute(
                            "UPDATE tasks SET completed = %s, completed_at = %s, due_date = %s "
                            "WHERE id = %s AND user_id = %s",
                            (
                                False,
                                date.today().isoformat(),
                                next_due.isoformat() if next_due else None,
                                task_id,
                                owner_id,
                            ),
                        )
                    else:
                        cur.execute(
                            "UPDATE tasks SET completed = %s, due_date = %s "
                            "WHERE id = %s AND user_id = %s",
                            (False, next_due.isoformat() if next_due else None, task_id, owner_id),
                        )
                pg_conn.commit()
            with get_connection() as conn:
                conn.execute(
                    "UPDATE subtasks SET completed = 0 WHERE task_id = ?",
                    (task_id,),
                )
                log_event(
                    "task_recurrent_completed",
                    task["title"],
                    details=f"Siguiente fecha: {next_due.isoformat() if next_due else 'sin fecha'}",
                    task_id=task_id,
                    conn=conn,
                    user_id=owner_id,
                )
        else:
            new_value = not currently_completed
            completed_at = date.today().isoformat() if new_value else None
            with get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cur:
                    if pg_task_has("completed_at"):
                        cur.execute(
                            "UPDATE tasks SET completed = %s, completed_at = %s "
                            "WHERE id = %s AND user_id = %s",
                            (new_value, completed_at, task_id, owner_id),
                        )
                    else:
                        cur.execute(
                            "UPDATE tasks SET completed = %s WHERE id = %s AND user_id = %s",
                            (new_value, task_id, owner_id),
                        )
                pg_conn.commit()
            if new_value:
                log_event("task_completed", task["title"], task_id=task_id, user_id=owner_id)
    return redirect_to_index(current_filter, search_query, current_sort, current_nav)


@app.route("/delete/<int:task_id>", methods=["POST"])
def delete_task(task_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    task = pg_fetch_one_dict(
        "SELECT title FROM tasks WHERE id = %s AND user_id = %s",
        (task_id, owner_id),
    )
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM task_tags WHERE task_id = %s", (task_id,))
            cur.execute("DELETE FROM tasks WHERE id = %s AND user_id = %s", (task_id, owner_id))
        pg_conn.commit()
    with get_connection() as conn:
        conn.execute("DELETE FROM subtasks WHERE task_id = ?", (task_id,))
    if task is not None:
        log_event("task_deleted", task["title"], task_id=task_id, user_id=owner_id)
    return redirect_to_index(current_filter, search_query, current_sort, current_nav)


@app.route("/duplicate/<int:task_id>", methods=["POST"])
def duplicate_task(task_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    current_nav = get_nav_value()
    quick_date = get_quick_date_value()

    task = pg_fetch_one_dict(
        f"SELECT {pg_task_select_clause()} FROM tasks WHERE id = %s AND user_id = %s",
        (task_id, owner_id),
    )
    if task is None:
        return redirect_to_index(current_filter, search_query, current_sort, current_nav, quick_date_value=quick_date)

    title = (task.get("title") or "").strip()
    if not title:
        return redirect_to_index(current_filter, search_query, current_sort, current_nav, quick_date_value=quick_date)

    category = (task.get("category") or "").strip()
    priority = task.get("priority") if task.get("priority") in VALID_PRIORITIES else "media"
    due_date = to_iso_date(task.get("due_date"))
    recurrence = task.get("recurrence") if task.get("recurrence") in VALID_RECURRENCES else ""
    notes = (task.get("notes") or "").strip()
    project_id = normalize_project_id(task.get("project_id"), user_id=owner_id)
    source_tag_rows = pg_fetch_all_dicts(
        "SELECT tag_id FROM task_tags WHERE task_id = %s",
        (task_id,),
    )
    source_tag_ids = [row["tag_id"] for row in source_tag_rows]

    columns = ["title", "category", "priority", "due_date", "completed", "notes"]
    values = [title, category, priority, due_date, False, notes]
    if pg_task_has("project_id"):
        columns.append("project_id")
        values.append(project_id)
    if pg_task_has("user_id"):
        columns.append("user_id")
        values.append(owner_id)
    if pg_task_has("recurrence"):
        columns.append("recurrence")
        values.append(recurrence)
    if pg_task_has("completed_at"):
        columns.append("completed_at")
        values.append(None)
    if pg_task_has("position"):
        max_position_row = pg_fetch_one_dict(
            "SELECT COALESCE(MAX(position), 0) AS max_position FROM tasks WHERE user_id = %s",
            (owner_id,),
        )
        columns.append("position")
        values.append((max_position_row["max_position"] or 0) + 1)

    new_task_id = None
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            placeholders = ", ".join(["%s"] * len(values))
            cur.execute(
                f"INSERT INTO tasks ({', '.join(columns)}) VALUES ({placeholders}) RETURNING id",
                tuple(values),
            )
            inserted = cur.fetchone()
            new_task_id = inserted[0] if inserted else None
        pg_conn.commit()

    if new_task_id is not None:
        replace_task_tags(new_task_id, source_tag_ids, user_id=owner_id)
        with get_connection() as conn:
            note_rows = conn.execute(
                "SELECT title, completed FROM subtasks WHERE task_id = ? ORDER BY id ASC",
                (task_id,),
            ).fetchall()
            for row in note_rows:
                conn.execute(
                    "INSERT INTO subtasks (task_id, title, completed) VALUES (?, ?, ?)",
                    (new_task_id, row["title"], row["completed"]),
                )
        log_event(
            "task_duplicated",
            title,
            details=f"Tarea origen: #{task_id}",
            task_id=new_task_id,
            user_id=owner_id,
        )

    return redirect_to_index(
        current_filter,
        search_query,
        current_sort,
        current_nav,
        quick_date_value=quick_date,
        new_task_id=new_task_id,
    )


@app.route("/tags/add", methods=["POST"])
def add_tag():
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    name = (request.form.get("tag_name") or "").strip()
    color = (request.form.get("tag_color") or "").strip() or "#22c55e"
    if not name:
        return redirect_to_index(current_filter, search_query, current_sort, "labels")
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tags (name, color, user_id) VALUES (%s, %s, %s)",
                (name, color, owner_id),
            )
        pg_conn.commit()
    return redirect_to_index(current_filter, search_query, current_sort, "labels")


@app.route("/tags/edit/<int:tag_id>", methods=["POST"])
def edit_tag(tag_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    name = (request.form.get("tag_name") or "").strip()
    color = (request.form.get("tag_color") or "").strip() or "#22c55e"
    if not name:
        return redirect_to_index(current_filter, search_query, current_sort, "labels")
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE tags SET name = %s, color = %s WHERE id = %s AND user_id = %s",
                (name, color, tag_id, owner_id),
            )
        pg_conn.commit()
    return redirect_to_index(current_filter, search_query, current_sort, "labels")


@app.route("/tags/delete/<int:tag_id>", methods=["POST"])
def delete_tag(tag_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM task_tags "
                "WHERE tag_id = %s "
                "AND task_id IN (SELECT id FROM tasks WHERE user_id = %s)",
                (tag_id, owner_id),
            )
            cur.execute("DELETE FROM tags WHERE id = %s AND user_id = %s", (tag_id, owner_id))
        pg_conn.commit()
    return redirect_to_index(current_filter, search_query, current_sort, "labels")


@app.route("/projects/add", methods=["POST"])
def add_project():
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    name = (request.form.get("project_name") or "").strip()
    description = (request.form.get("project_description") or "").strip()
    color = (request.form.get("project_color") or "").strip()
    due_date_raw = (request.form.get("project_due_date") or "").strip()
    due_date = parse_iso_date(due_date_raw)
    if not name:
        return redirect_to_index(current_filter, search_query, current_sort, "projects")

    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO projects (name, description, color, due_date, status, user_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (name, description, color, due_date, "active", owner_id),
            )
        pg_conn.commit()
    return redirect_to_index(current_filter, search_query, current_sort, "projects")


@app.route("/projects/edit/<int:project_id>", methods=["POST"])
def edit_project(project_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    name = (request.form.get("project_name") or "").strip()
    description = (request.form.get("project_description") or "").strip()
    color = (request.form.get("project_color") or "").strip()
    due_date_raw = (request.form.get("project_due_date") or "").strip()
    due_date = parse_iso_date(due_date_raw)
    status = (request.form.get("project_status") or "active").strip().lower()
    if status not in {"active", "completed"}:
        status = "active"
    if not name:
        return redirect_to_index(current_filter, search_query, current_sort, "projects", new_task_id=None)

    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE projects SET name = %s, description = %s, color = %s, due_date = %s, status = %s "
                "WHERE id = %s AND user_id = %s",
                (name, description, color, due_date, status, project_id, owner_id),
            )
        pg_conn.commit()
    return redirect_to_index(
        current_filter,
        search_query,
        current_sort,
        "projects",
        dashboard_from_value=get_dashboard_from_value(),
        dashboard_to_value=get_dashboard_to_value(),
    )


@app.route("/projects/delete/<int:project_id>", methods=["POST"])
def delete_project(project_id):
    owner_id = current_user_id()
    if owner_id is None:
        return redirect(url_for("login"))
    current_filter = get_filter_value()
    search_query = get_search_value()
    current_sort = get_sort_value()
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM projects WHERE id = %s AND user_id = %s", (project_id, owner_id))
        pg_conn.commit()
    return redirect_to_index(current_filter, search_query, current_sort, "projects")


init_db()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)
