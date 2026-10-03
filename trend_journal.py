#!/usr/bin/env python3
"""
Система журналирования трендов.
- 4 источника × 100-600 значений/сек
- 1000 предопределённых скалярных объектов
- Тренды → SQLite (append-only "волжурнал")
- Не-тренды → JSON
- При старте: восстановление JSON → SQLite
- Лимит: 1000 записей НА КАЖДЫЙ источник (итого 4000)
"""

import asyncio
import aiosqlite
import aiofiles
import json
import random
import time
import signal
import shutil
from dataclasses import dataclass
from typing import Any, List, Dict, Optional
from collections import defaultdict
from pathlib import Path

# ==================== КОНФИГУРАЦИЯ ====================

NUM_OBJECTS = 1000
NUM_SOURCES = 4
DB_PATH = "trends.db"
JSON_DIR = Path("non_trends_journal")
QUEUE_MAXSIZE = 50000
FLUSH_INTERVAL = 1.0
JSON_MAX_SIZE = 10 * 1024 * 1024
MAX_RECORDS_PER_SOURCE = 1000  # ✅ ПО 1000 НА КАЖДЫЙ ИСТОЧНИК
CLEAN_START = True

# ==================== МОДЕЛИ ====================

@dataclass
class Tag:
    id: int
    name: str
    source_id: int
    data_type: str
    deadband: float = 0.0

@dataclass
class DataPoint:
    timestamp: float
    source_id: int
    object_id: int
    value: Any
    data_type: str

# ==================== РЕЕСТР ОБЪЕКТОВ ====================

class TagRegistry:
    def __init__(self):
        self.tags: List[Tag] = []
        types = ['int', 'float', 'bool', 'string']
        for i in range(NUM_OBJECTS):
            source_id = (i % NUM_SOURCES) + 1
            dtype = types[i % len(types)]
            deadband = 1.0 if dtype in ('int', 'float') else 0.0
            self.tags.append(Tag(
                id=i, name=f"Tag_{i:04d}",
                source_id=source_id, data_type=dtype, deadband=deadband
            ))
        self.by_source: Dict[int, List[Tag]] = defaultdict(list)
        for t in self.tags:
            self.by_source[t.source_id].append(t)

    def get_tag(self, object_id: int) -> Tag:
        return self.tags[object_id]

# ==================== ИСТОЧНИКИ ДАННЫХ ====================

class DataSource:
    def __init__(self, source_id: int, tags: List[Tag], queue: asyncio.Queue):
        self.source_id = source_id
        self.tags = tags
        self.queue = queue
        self._running = False
        self._tag_map = {t.id: t for t in tags}
        self._count = 0  # ✅ счётчик для этого источника

    def _generate(self, tag: Tag) -> Any:
        if tag.data_type == 'int':    return random.randint(-10000, 10000)
        if tag.data_type == 'float':  return round(random.uniform(-10000.0, 10000.0), 3)
        if tag.data_type == 'bool':   return random.choice([True, False])
        if tag.data_type == 'string': return f"val_{random.randint(0, 999)}"

    async def run(self):
        self._running = True
        target_rate = random.randint(100, 600)
        interval = 0.05
        batch_size = max(1, int(target_rate * interval))
        print(f"[Source {self.source_id}] rate≈{target_rate}/s, batch={batch_size}")

        while self._running:
            # ✅ Остановка если набрали 1000 для этого источника
            if self._count >= MAX_RECORDS_PER_SOURCE:
                print(f"[Source {self.source_id}] reached {MAX_RECORDS_PER_SOURCE}, stopping")
                self._running = False
                break

            batch = []
            for _ in range(batch_size):
                if self._count >= MAX_RECORDS_PER_SOURCE:
                    break
                p = DataPoint(
                    timestamp=time.time(),
                    source_id=self.source_id,
                    object_id=random.choice(self.tags).id,
                    value=None, data_type=None
                )
                tag = self._tag_map.get(p.object_id, self.tags[0])
                p.value = self._generate(tag)
                p.data_type = tag.data_type
                batch.append(p)
                self._count += 1

            if batch:
                await self.queue.put(batch)
            await asyncio.sleep(interval)

    def stop(self):
        self._running = False

# ==================== ДЕТЕКТОР ТРЕНДОВ ====================

class TrendDetector:
    def __init__(self):
        self.last: Dict[int, Any] = {}

    def is_trend(self, point: DataPoint, tag: Tag) -> bool:
        oid = point.object_id
        if oid not in self.last:
            self.last[oid] = point.value
            return True
        last, cur = self.last[oid], point.value
        if tag.data_type in ('int', 'float'):
            if abs(float(cur) - float(last)) > tag.deadband:
                self.last[oid] = cur
                return True
            return False
        if cur != last:
            self.last[oid] = cur
            return True
        return False

# ==================== SQLITE WRITER ====================

class SQLiteWriter:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db: Optional[aiosqlite.Connection] = None
        self._buffer: List[tuple] = []
        self._lock = asyncio.Lock()

    async def init(self):
        self.db = await aiosqlite.connect(self.db_path)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA synchronous=NORMAL")
        await self.db.execute("PRAGMA cache_size=-10000")
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS trends (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp  REAL    NOT NULL,
                source_id  INTEGER NOT NULL,
                object_id  INTEGER NOT NULL,
                value      TEXT,
                data_type  TEXT    NOT NULL
            )
        """)
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_ts  ON trends(timestamp)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_obj ON trends(object_id)")
        await self.db.commit()
        print(f"[SQLite] ready @ {self.db_path}")

    async def write(self, p: DataPoint):
        async with self._lock:
            self._buffer.append((p.timestamp, p.source_id, p.object_id,
                                 str(p.value), p.data_type))

    async def write_many(self, points: List[DataPoint]):
        async with self._lock:
            for p in points:
                self._buffer.append((p.timestamp, p.source_id, p.object_id,
                                     str(p.value), p.data_type))

    async def flush(self) -> int:
        async with self._lock:
            if not self._buffer:
                return 0
            buf, self._buffer = self._buffer, []
        await self.db.executemany(
            "INSERT INTO trends (timestamp, source_id, object_id, value, data_type) "
            "VALUES (?,?,?,?,?)", buf
        )
        await self.db.commit()
        return len(buf)

    async def periodic_flush(self, interval: float):
        while True:
            await asyncio.sleep(interval)
            try:
                await self.flush()
            except Exception as e:
                print(f"[SQLite] flush error: {e}")

    async def close(self):
        try:
            await self.flush()
        except Exception as e:
            print(f"[SQLite] final flush error: {e}")
        if self.db:
            await self.db.close()

# ==================== JSON WRITER ====================

class JSONWriter:
    def __init__(self, json_dir: Path):
        self.json_dir = json_dir
        self.json_dir.mkdir(exist_ok=True)
        self._file: Optional[Path] = None
        self._size = 0
        self._first = True
        self._lock = asyncio.Lock()

    def _new_file(self):
        ts = int(time.time() * 1000)
        self._file = self.json_dir / f"non_trends_{ts}.json"
        self._file.write_text("[\n", encoding='utf-8')
        self._size = 2
        self._first = True

    def _finalize(self):
        if self._file and self._file.exists():
            txt = self._file.read_text(encoding='utf-8').rstrip()
            if not txt.endswith(']'):
                self._file.write_text(txt + "\n]\n", encoding='utf-8')

    async def write(self, p: DataPoint):
        async with self._lock:
            if self._file is None or self._size > JSON_MAX_SIZE:
                if self._file is not None:
                    self._finalize()
                self._new_file()

            entry = {"ts": round(p.timestamp, 6), "src": p.source_id,
                     "obj": p.object_id, "val": p.value, "type": p.data_type}
            line = ("" if self._first else ",") + json.dumps(entry, ensure_ascii=False) + "\n"
            self._first = False

            async with aiofiles.open(self._file, 'a', encoding='utf-8') as f:
                await f.write(line)
            self._size += len(line.encode('utf-8'))

    def close(self):
        self._finalize()

# ==================== ВОССТАНОВЛЕНИЕ ПРИ СТАРТЕ ====================

class Recovery:
    @staticmethod
    async def recover(json_dir: Path, sqlite: SQLiteWriter) -> int:
        if not json_dir.exists():
            return 0
        files = sorted(json_dir.glob("non_trends_*.json"))
        print(f"[Recovery] found {len(files)} JSON file(s)")
        total = 0
        for f in files:
            try:
                async with aiofiles.open(f, 'r', encoding='utf-8') as fp:
                    data = json.loads(await fp.read())
                points = [DataPoint(e['ts'], e['src'], e['obj'], e['val'], e['type'])
                          for e in data]
                await sqlite.write_many(points)
                total += len(points)
                f.unlink()
                print(f"[Recovery] {f.name} → {len(points)} points")
            except Exception as e:
                print(f"[Recovery] error on {f}: {e}")
        await sqlite.flush()
        return total

# ==================== ГЛАВНЫЙ ЦИКЛ ====================

async def main():
    print("=" * 60)
    print(f"Trend Journaling System | objects={NUM_OBJECTS} sources={NUM_SOURCES}")
    print(f"MAX_RECORDS_PER_SOURCE = {MAX_RECORDS_PER_SOURCE}")
    print(f"TOTAL EXPECTED = {MAX_RECORDS_PER_SOURCE * NUM_SOURCES}")
    print("=" * 60)

    if CLEAN_START:
        if Path(DB_PATH).exists():
            Path(DB_PATH).unlink()
            print(f"[Cleanup] deleted {DB_PATH}")
        if JSON_DIR.exists():
            shutil.rmtree(JSON_DIR)
            print(f"[Cleanup] deleted {JSON_DIR}/")

    registry = TagRegistry()
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    sqlite = SQLiteWriter(DB_PATH)
    await sqlite.init()
    json_w = JSONWriter(JSON_DIR)

    recovered = await Recovery.recover(JSON_DIR, sqlite)
    if recovered:
        print(f"[Recovery] total restored: {recovered}")

    detector = TrendDetector()

    sources = [DataSource(i + 1, registry.by_source[i + 1], queue)
               for i in range(NUM_SOURCES)]
    src_tasks = [asyncio.create_task(s.run()) for s in sources]
    flush_task = asyncio.create_task(sqlite.periodic_flush(FLUSH_INTERVAL))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # ✅ Статистика по каждому источнику отдельно
    stats = {
        "total": {i: 0 for i in range(1, NUM_SOURCES + 1)},
        "trends": {i: 0 for i in range(1, NUM_SOURCES + 1)},
        "non": {i: 0 for i in range(1, NUM_SOURCES + 1)},
        "t0": time.time()
    }
    last_report = time.time()

    try:
        while not stop.is_set():
            # ✅ Проверяем, все ли источники завершены
            all_done = all(
                stats["total"][i] >= MAX_RECORDS_PER_SOURCE
                for i in range(1, NUM_SOURCES + 1)
            )
            if all_done:
                print(f"[Main] all sources reached {MAX_RECORDS_PER_SOURCE}, stopping")
                stop.set()
                break

            try:
                batch = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if time.time() - last_report > 5:
                    _report(stats)
                    last_report = time.time()
                continue

            for p in batch:
                sid = p.source_id
                # ✅ Пропускаем если этот источник уже набрал лимит
                if stats["total"][sid] >= MAX_RECORDS_PER_SOURCE:
                    continue

                tag = registry.get_tag(p.object_id)
                stats["total"][sid] += 1
                if detector.is_trend(p, tag):
                    await sqlite.write(p)
                    stats["trends"][sid] += 1
                else:
                    await json_w.write(p)
                    stats["non"][sid] += 1

            if time.time() - last_report > 5:
                _report(stats)
                last_report = time.time()

    finally:
        print("\n[Main] shutting down...")
        for s in sources:
            s.stop()
        for t in src_tasks:
            t.cancel()
        flush_task.cancel()

        # DRAIN очереди
        drained = 0
        while not queue.empty():
            try:
                batch = queue.get_nowait()
                for p in batch:
                    sid = p.source_id
                    if stats["total"][sid] >= MAX_RECORDS_PER_SOURCE:
                        continue
                    tag = registry.get_tag(p.object_id)
                    if detector.is_trend(p, tag):
                        await sqlite.write(p)
                    else:
                        await json_w.write(p)
                    drained += 1
            except asyncio.QueueEmpty:
                break
        print(f"[Main] drained {drained} points from queue")

        await sqlite.close()
        json_w.close()
        _report(stats)
        print("[Main] bye")

def _report(s):
    dt = time.time() - s["t0"]
    total_all = sum(s["total"].values())
    rate = total_all / dt if dt > 0 else 0
    print(f"[Stats] elapsed={dt:.1f}s  rate={rate:.0f}/s")
    for sid in sorted(s["total"].keys()):
        print(f"  Source {sid}: total={s['total'][sid]} "
              f"trends={s['trends'][sid]} non_trends={s['non'][sid]}")
    print(f"  TOTAL: {total_all}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass