#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_richer.py
Day1 升級：10 站點每秒模擬事件，含「產品/工單/班別/完工時間/能耗/振動/停機原因」等。
同步寫入 SQLite（data.db）與 CSV（data.csv），每 60 秒計算一次各站 p95，建立瓶頸標註 bottleneck_labels。
"""

import csv
import datetime as dt
import math
import os
import random
import sqlite3
import string
import time
from collections import defaultdict, deque
from statistics import quantiles

DB_PATH = "data.db"
CSV_PATH = "data.csv"

# -----------------------------
# 站別/班別/人員/分布參數
# -----------------------------
LINE_ID = "L01"
STATIONS = [f"ST{i:02d}" for i in range(1, 11)]
SEED = 42
random.seed(SEED)

# 平均 cycle（秒），瓶頸站（ST01/ST07）偏慢 & 重尾
BASE_CYCLE_MEAN = {
    "ST01": 9.5, "ST02": 7.5, "ST03": 6.8, "ST04": 6.3, "ST05": 5.9,
    "ST06": 6.1, "ST07": 8.3, "ST08": 5.1, "ST09": 4.8, "ST10": 4.5,
}
TAIL_HEAVINESS = {st: (1.6 if st in ("ST01", "ST07") else 1.25) for st in STATIONS}

# 班別（簡化：每 8 小時一班）
def current_shift(now: dt.datetime) -> str:
    h = now.hour
    if 6 <= h < 14:   return "A"
    if 14 <= h < 22:  return "B"
    return "C"

# 站別固定配置的操作員池
OPERATORS = {st: [f"{st}-OP{n:02d}" for n in range(1, 6)] for st in STATIONS}

# 狀態碼/名稱/日常機率
STATUS_DEF = [
    (0, "OK", 0.80),
    (1, "MINOR_STOP", 0.08),
    (2, "MAJOR_STOP", 0.03),
    (3, "MAINT", 0.03),
    (4, "QUALITY_HOLD", 0.06),
]

# 停機原因（對 MAJOR_STOP/MAINT/QUALITY_HOLD 使用）
DOWNTIME_REASONS = {
    1: ["微停-補料", "微停-校正", "微停-換治具"],
    2: ["故障-馬達", "故障-感測器", "故障-氣壓"],
    3: ["保養-刀具更換", "保養-點檢", "保養-清潔"],
    4: ["品質-抽檢", "品質-返工", "品質-待判"]
}

# 產品/工單/客戶
PRODUCTS = [
    ("SKU-A100", "Alpha Frame"),
    ("SKU-B250", "Beta Housing"),
    ("SKU-C900", "Carbon Plate")
]
CUSTOMERS = ["Acme", "Globex", "Initech", "Umbrella"]
PRIORITIES = ["Low", "Normal", "High", "Critical"]

# 到達/服務近似（影響 queue）
BASE_SERVICE_RATE = {st: 1.0 / max(2.0, BASE_CYCLE_MEAN[st]) for st in STATIONS}
ARRIVAL_RATE = 0.60
LABEL_WINDOW_SEC = 60

# -----------------------------
# 生成器們
# -----------------------------
class SerialGen:
    def __init__(self):
        self.sn_counter = defaultdict(int)
        self.wo_counter = 1000

    def next_sn(self, line_id: str, st: str) -> str:
        key = (line_id, st, dt.date.today().strftime("%Y%m%d"))
        self.sn_counter[key] += 1
        return f"{dt.date.today():%Y%m%d}-{line_id}-{st}-{self.sn_counter[key]:07d}"

    def lot_id(self) -> str:
        suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
        return f"LOT-{dt.date.today():%Y%m%d}-{suffix}"

    def work_order(self) -> str:
        self.wo_counter += 1
        return f"WO-{self.wo_counter}"

    def product(self):
        return random.choice(PRODUCTS)

    def customer(self):
        return random.choice(CUSTOMERS)

SER = SerialGen()

# 每站當前工單 & 批 & 產品 & 交期
CURRENT_ORDER = {
    st: {
        "work_order_id": SER.work_order(),
        "lot_id": SER.lot_id(),
        "sku": SER.product()[0],
        "product_name": SER.product()[1],
        "customer": SER.customer(),
        "due_date": (dt.date.today() + dt.timedelta(days=random.randint(1, 10))).isoformat(),
        "priority": random.choice(PRIORITIES),
        "qty_target": random.randint(200, 500),  # 本工單目標量
        "qty_done": 0
    } for st in STATIONS
}

# -----------------------------
# SQLite DDL（擴充分欄位）
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS machine_data (
        ts              TEXT,       -- 記錄時間（秒）
        start_ts        TEXT,       -- 開始加工時間（若有）
        end_ts          TEXT,       -- 完工時間（若有）
        duration_sec    REAL,       -- 對應該事件的時長（秒）
        line_id         TEXT,
        station_id      TEXT,
        machine_id      TEXT,
        shift           TEXT,
        operator_id     TEXT,

        work_order_id   TEXT,
        customer        TEXT,
        due_date        TEXT,
        priority        TEXT,

        product_name    TEXT,
        sku             TEXT,
        lot_id          TEXT,
        serial_number   TEXT,

        status_code     INTEGER,    -- 0 OK, 1 MINOR_STOP, 2 MAJOR_STOP, 3 MAINT, 4 QUALITY_HOLD
        status          TEXT,
        downtime_reason TEXT,

        cycle_time      REAL,       -- 若有產出則為 end-start，否則 NULL
        qty_in          INTEGER,
        qty_out         INTEGER,
        scrap_qty       INTEGER,

        good            INTEGER,    -- 1 良 / 0 不良
        defect_code     TEXT,

        queue_len       INTEGER,
        temp_c          REAL,       -- 機台溫度
        vibration_mm_s  REAL,       -- 振動速度（mm/s）
        energy_kwh      REAL        -- 能耗（kWh）
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ts ON machine_data(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_st ON machine_data(station_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wo ON machine_data(work_order_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sn ON machine_data(serial_number)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS bottleneck_labels(
        window_start    TEXT,
        station_id      TEXT,
        p95_cycle       REAL,
        is_bottleneck   INTEGER
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lbl_w ON bottleneck_labels(window_start)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lbl_s ON bottleneck_labels(station_id)")
    conn.commit()
    conn.close()

# -----------------------------
# CSV
# -----------------------------
def init_csv():
    new = not os.path.exists(CSV_PATH)
    f = open(CSV_PATH, "a", newline="", encoding="utf-8")
    w = csv.writer(f)
    if new:
        w.writerow([
            "ts","start_ts","end_ts","duration_sec",
            "line_id","station_id","machine_id","shift","operator_id",
            "work_order_id","customer","due_date","priority",
            "product_name","sku","lot_id","serial_number",
            "status_code","status","downtime_reason",
            "cycle_time","qty_in","qty_out","scrap_qty",
            "good","defect_code",
            "queue_len","temp_c","vibration_mm_s","energy_kwh"
        ])
    return f, w

# -----------------------------
# 抽樣工具
# -----------------------------
def sample_status():
    r = random.random(); acc = 0
    for code, name, p in STATUS_DEF:
        acc += p
        if r <= acc:
            return code, name
    return STATUS_DEF[-1][0], STATUS_DEF[-1][1]

def sample_cycle(st: str, status_code: int) -> float | None:
    """回傳 cycle_time（秒），停機/保養回傳 None"""
    if status_code in (2, 3):  # MAJOR_STOP/MAINT
        return None
    base = BASE_CYCLE_MEAN[st]
    tail = TAIL_HEAVINESS[st]
    mu = math.log(base) - 0.5
    sigma = 0.35 * tail
    ct = random.lognormvariate(mu, sigma)

    if status_code == 1:  # 微停
        ct *= random.uniform(1.1, 1.4)
    if status_code == 4:  # 品質暫停
        ct *= random.uniform(1.05, 1.2)
    return max(0.8, round(ct, 2))

DEFECTS = ["D001_Scratch","D002_Warp","D003_DimNG","D004_Color"]
def sample_quality(status_code: int):
    if status_code == 4:
        good = 0 if random.random() < 0.7 else 1
    else:
        ngp = 0.02 if status_code == 0 else 0.05
        good = 0 if random.random() < ngp else 1
    defect = random.choice(DEFECTS) if good == 0 else None
    return good, defect

# 佇列
_queue = {st: 0 for st in STATIONS}
def update_queue(st: str, produced: bool, status_code: int) -> int:
    q = _queue[st]
    if random.random() < ARRIVAL_RATE:
        q += 1
    if produced:  # 有產出 → 出隊
        q = max(0, q - 1)
    if status_code in (2,3):
        q += random.choice([0,1,1])
    elif status_code == 1:
        q += random.choice([0,0,1])
    _queue[st] = max(0, min(500, q))
    return _queue[st]

# 感測：能耗/溫度/振動（隨狀態變動）
def sample_sensors(status_code: int, base_cycle: float):
    temp = random.gauss(45, 4)      # °C
    vib  = abs(random.gauss(2.0, .7))  # mm/s
    energy = max(0.01, base_cycle/3600.0 * random.uniform(0.08, 0.15))  # 粗略：秒→kWh

    if status_code == 1:  # 微停：能耗/振動微升
        temp += random.uniform(0.5, 1.5)
        vib  += random.uniform(0.2, 0.6)
    if status_code in (2,3):  # 大停/保養：能耗下降、振動下降
        energy *= random.uniform(0.2, 0.6)
        vib    *= random.uniform(0.4, 0.8)
    if status_code == 4:  # 品質暫停：輕微變動
        temp += random.uniform(-0.3, 0.8)
    return round(temp,2), round(vib,2), round(energy,4)

# p95 buffer
_buffers = {st: deque(maxlen=6000) for st in STATIONS}
def p95(values):
    if not values: return None
    try:
        return round(quantiles(values, n=100)[94], 3)
    except Exception:
        vs = sorted(values)
        idx = max(0, min(len(vs)-1, int(math.ceil(0.95*len(vs))) - 1))
        return round(vs[idx], 3)

# -----------------------------
# 主循環
# -----------------------------
def main():
    init_db()
    fcsv, writer = init_csv()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    machine_id = {st: f"{st}-M01" for st in STATIONS}
    last_label_emit = int(time.time() // LABEL_WINDOW_SEC) * LABEL_WINDOW_SEC

    print("[ingest] richer model running. Ctrl+C to stop.")
    try:
        while True:
            now = dt.datetime.now().replace(microsecond=0)
            iso_now = now.isoformat()
            shift = current_shift(now)

            rows_db, rows_csv = [], []

            for st in STATIONS:
                # 站點當前工單
                wo = CURRENT_ORDER[st]
                # 操作員
                operator = random.choice(OPERATORS[st])

                # 是否有產出（簡化）
                base_p = min(0.95, BASE_SERVICE_RATE[st] * (1.0 + 0.02 * _queue[st]))
                code, status = sample_status()
                produced = (random.random() < base_p) and (code not in (2,3))

                # 取 cycle
                ct = sample_cycle(st, code)

                # 取感測
                temp, vib, energy = sample_sensors(code, BASE_CYCLE_MEAN[st])

                # 佇列更新
                qlen = update_queue(st, produced, code)

                # 數量欄位
                qty_in = 1 if produced else 0
                qty_out = 1 if (produced and ct is not None) else 0
                scrap   = 0

                start_ts = None
                end_ts   = None
                duration = None
                sn = None
                good = None
                defect = None
                downtime_reason = None

                if produced and ct is not None:
                    # 有產出：有 start/end、SN、品質
                    end_ts = now
                    start_ts = now - dt.timedelta(seconds=ct)
                    duration = ct
                    sn = SER.next_sn(LINE_ID, st)
                    good, defect = sample_quality(code)
                    if good == 0:
                        scrap = 1
                    wo["qty_done"] += 1

                    # 偶爾達成目標 → 滾動到下一張工單/批
                    if wo["qty_done"] >= wo["qty_target"]:
                        CURRENT_ORDER[st] = {
                            "work_order_id": SER.work_order(),
                            "lot_id": SER.lot_id(),
                            "sku": SER.product()[0],
                            "product_name": SER.product()[1],
                            "customer": SER.customer(),
                            "due_date": (dt.date.today() + dt.timedelta(days=random.randint(1, 10))).isoformat(),
                            "priority": random.choice(PRIORITIES),
                            "qty_target": random.randint(200, 500),
                            "qty_done": 0
                        }

                else:
                    # 停機/保養/品質暫停 → 記事件（估一個 duration，無 SN/無 cycle）
                    if code in (1,2,3,4):
                        rs = DOWNTIME_REASONS.get(code, [])
                        downtime_reason = random.choice(rs) if rs else None
                        # 隨機估個事件持續秒（微停短/大停長）
                        if code == 1: duration = round(random.uniform(5, 40), 1)
                        elif code == 2: duration = round(random.uniform(60, 900), 1)
                        elif code == 3: duration = round(random.uniform(120, 600), 1)
                        elif code == 4: duration = round(random.uniform(10, 120), 1)
                        start_ts = now - dt.timedelta(seconds=duration)
                        end_ts   = now

                row = (
                    iso_now,
                    start_ts.isoformat() if start_ts else None,
                    end_ts.isoformat() if end_ts else None,
                    duration,
                    LINE_ID, st, machine_id[st], shift, operator,

                    wo["work_order_id"], wo["customer"], wo["due_date"], wo["priority"],
                    wo["product_name"], wo["sku"], wo["lot_id"], sn,

                    code, status, downtime_reason,

                    ct if (produced and ct is not None) else None,
                    qty_in, qty_out, scrap,

                    good, defect,
                    qlen, temp, vib, energy
                )

                rows_db.append(row)
                rows_csv.append(row)

                # p95 buffer
                if produced and ct is not None:
                    _buffers[st].append(ct)

            # 批次寫入
            if rows_db:
                cur.executemany("""
                INSERT INTO machine_data(
                    ts,start_ts,end_ts,duration_sec,
                    line_id,station_id,machine_id,shift,operator_id,
                    work_order_id,customer,due_date,priority,
                    product_name,sku,lot_id,serial_number,
                    status_code,status,downtime_reason,
                    cycle_time,qty_in,qty_out,scrap_qty,
                    good,defect_code,
                    queue_len,temp_c,vibration_mm_s,energy_kwh
                ) VALUES (?,?,?,?, ?,?,?,?,?,
                          ?,?,?,?,
                          ?,?,?,?,
                          ?,?,?,
                          ?,?,?,?,
                          ?,?,
                          ?,?, ?,?)
                """, rows_db)
                conn.commit()
            if rows_csv:
                writer.writerows(rows_csv)

            # 每分鐘標註 p95 → 瓶頸
            now_ts = time.time()
            curr_win = int(now_ts // LABEL_WINDOW_SEC) * LABEL_WINDOW_SEC
            if curr_win > last_label_emit:
                pmap = {st: p95(list(_buffers[st])) for st in STATIONS}
                valid = [(st, v) for st, v in pmap.items() if v is not None]
                if valid:
                    valid.sort(key=lambda x: x[1], reverse=True)
                    top_st = valid[0][0]
                    window_start_iso = dt.datetime.fromtimestamp(curr_win - LABEL_WINDOW_SEC).replace(microsecond=0).isoformat()
                    lbl_rows = [(window_start_iso, st, pmap[st], 1 if st == top_st else 0) for st in STATIONS]
                    cur.executemany("""
                        INSERT INTO bottleneck_labels(window_start,station_id,p95_cycle,is_bottleneck)
                        VALUES (?,?,?,?)
                    """, lbl_rows)
                    conn.commit()
                    print(f"[label] {window_start_iso} bottleneck={top_st}")
                last_label_emit = curr_win

            # 對齊秒
            sleep_left = 1.0 - (time.time() - now_ts)
            if sleep_left > 0:
                time.sleep(sleep_left)

    except KeyboardInterrupt:
        print("\n[ingest] stopped.")
    finally:
        fcsv.close()
        conn.close()

if __name__ == "__main__":
    main()
