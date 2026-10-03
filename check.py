import sqlite3
import json
from pathlib import Path

print("=" * 50)
print("ПРОВЕРКА ДАННЫХ")
print("=" * 50)

# --- SQLite ---
print("\n📦 ТРЕНДЫ В SQLite (trends.db)")
try:
    conn = sqlite3.connect("trends.db")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM trends")
    count = cur.fetchone()[0]
    print(f"  Всего записей: {count}")

    print("\n  Последние 5 записей:")
    for row in cur.execute("SELECT * FROM trends ORDER BY id DESC LIMIT 5"):
        print(f"    id={row[0]}  ts={row[1]:.3f}  src={row[2]}  obj={row[3]}  val={row[4]}  type={row[5]}")

    print("\n  Записей по источникам:")
    for row in cur.execute("SELECT source_id, COUNT(*) FROM trends GROUP BY source_id"):
        print(f"    Источник {row[0]}: {row[1]} записей")

    conn.close()
except Exception as e:
    print(f"  Ошибка: {e}")

# --- JSON ---
print("\n📄 JSON-ЖУРНАЛ (non_trends_journal/)")
json_dir = Path("non_trends_journal")
if json_dir.exists():
    files = sorted(json_dir.glob("non_trends_*.json"))
    print(f"  Файлов: {len(files)}")
    total = 0
    for f in files[:5]:
        with open(f, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        total += len(data)
        print(f"    {f.name}: {len(data)} записей")
        if data:
            print(f"      Пример: {data[0]}")
    if len(files) > 5:
        print(f"    ... и ещё {len(files) - 5} файлов")
    print(f"  Итого в JSON: {total} записей")
else:
    print("  Директория не найдена (возможно, все данные ушли в тренды)")

print("\n✅ Проверка завершена")