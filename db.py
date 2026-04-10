import os

import psycopg2
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


def get_connection():
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")

    required = {
        "PGHOST": host,
        "PGPORT": port,
        "PGDATABASE": dbname,
        "PGUSER": user,
        "PGPASSWORD": password,
    }
    missing = [key for key, value in required.items() if value is None or str(value).strip() == ""]
    if missing:
        raise RuntimeError(
            "Faltan variables de entorno de PostgreSQL: " + ", ".join(missing)
        )

    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
    )
