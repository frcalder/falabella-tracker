"""
Backup completo de Supabase a JSON.
Uso: python scripts/backup_db.py
Genera: backups/backup_YYYY-MM-DD.json
"""
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

TABLES = ["categorias", "presupuestos", "clasificaciones", "reglas_sugerencia", "movimientos"]
BACKUP_DIR = Path("backups")


def serialize(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def main():
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    cur = conn.cursor()

    backup = {}
    total = 0
    for table in TABLES:
        cur.execute(f"SELECT * FROM {table}")
        rows = [dict(r) for r in cur.fetchall()]
        backup[table] = rows
        total += len(rows)
        print(f"  {table}: {len(rows)} filas")

    conn.close()

    BACKUP_DIR.mkdir(exist_ok=True)
    filename = BACKUP_DIR / f"backup_{date.today().isoformat()}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(backup, f, default=serialize, ensure_ascii=False, indent=2)

    print(f"\nBackup guardado: {filename} ({filename.stat().st_size // 1024} KB, {total} filas)")


if __name__ == "__main__":
    main()
