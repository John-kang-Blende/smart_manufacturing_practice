#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_bulk.py
快速大量造數：一次生成 N 筆模擬製造資料，寫入 SQLite (data.db) 與 CSV (data.csv)。

欄位涵蓋：時間/完成時間、產品/工單/客戶、班別/操作員、狀態碼/停機原因、
cycle_time、數量(入/出/報廢)、感測(溫度/振動/能耗) 等。

用法範例：
  python generate_bulk.py --rows 1000000 --stations 10 --db ./data/data.db --csv ./data/data.csv
  python generate_bulk.py --rows 200000 --start 2025-09-09T00:00:00 --hours 6
"""

import argparse
import csv
import datetime as dt
import os
import random
import sqlite3
import string
import time
from pathlib import Path

import numpy as np

# =========================
# 參數與常數
# =========================
PRODUCTS = [
    ("SKU-A100", "Alpha Frame"),
    ("SKU-B250", "Beta Housing"),
    ("SKU-C900", "Carbon Plate"),
]
CUSTOMERS = ["Acme", "Globex", "Initech", "Umbrella"]
PRIORITIES = ["Low", "Normal", "High", "Critical"]

STATUS_DEF = np.array([
    (0, "OK",           0.80),
    (1, "MINOR_STOP",   0.08),
    (2, "MAJOR_STOP",   0.03),
    (3, "MAINT",        0.03),
    (4, "QUALITY_HOLD", 0.06),
], dtype=object)

DOWNTIME_REASONS = {
    1: ["微停-補料", "微停-校正", "微停-換治具"],
    2: ["故障-馬達", "故障-感測器", "故障-氣壓"],
    3: ["保養-刀具更換", "保養-點檢", "保養-清潔"],
    4: ["品質-抽檢", "品質-返工", "品質-待判"]
}

BASE_CYCLE_MEAN = {
    # 可調站別均值（故意讓 ST01 / ST07 偏慢、重尾）
    "ST01": 9.5, "ST02": 7.5, "ST03": 6.8, "ST04": 6.3, "ST05": 5.9,
    "ST06": 6.1, "ST07": 8.3, "ST08": 5.1, "ST09": 4.8, "ST10": 4.5,
}

TAIL_HEAVINESS = {st: (1.6 if st in ("ST01", "ST07") else 1.25) for st in BASE_CYCLE_MEAN.keys()}

DEFECTS = ["D001_Scratch","D002_Warp","D003_DimNG","D004_Color"]

# =========================
# 小工具
# =========================
def ensure_dirs(path: str):
    p = Path(path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)

def current_shift_by_hour(h: int) -> str:
    if 6 <= h < 14:
        return "A"
    if 14 <= h < 22:
        return "B"
    return "C"

def rand_choice(arr, size):
    idx = np.random.randint(0, len(arr), size=size)
    return np.array(arr, dtype=object)[idx]

def random_string(k=3):
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choices(alphabet, k=k))

def prepare_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    # 加速 PRAGMA（建議僅在批量灌資料使用）
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")  # 約 200MB page cache（負值=KB）
    conn.commit()

    # 建表（不建索引，先灌資料，最後再建索引）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS machine_data (
        ts              TEXT,
        start_ts        TEXT,
        end_ts          TEXT,
        duration_sec    REAL,
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

        status_code     INTEGER,
        status          TEXT,
        downtime_reason TEXT,

        cycle_time      REAL,
        qty_in          INTEGER,
        qty_out         INTEGER,
        scrap_qty       INTEGER,

        good            INTEGER,
        defect_code     TEXT,

        queue_len       INTEGER,
        temp_c          REAL,
        vibration_mm_s  REAL,
        energy_kwh      REAL
    )
    """)
    conn.commit()

def create_indexes(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_md_ts ON machine_data(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_md_st ON machine_data(station_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_md_wo ON machine_data(work_order_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_md_sn ON machine_data(serial_number)")
    conn.commit()

# =========================
# 主造數邏輯（向量化）
# =========================
def generate_bulk(rows: int, stations: int, start_dt: dt.datetime, hours: float, line_id: str):
    """
    向量化產生 rows 筆資料，平均分布在 [start_dt, start_dt + hours) 期間。
    站別自動命名 ST01..STxx。欄位與 ingest_richer 對齊。
    """
    # ----- 基本欄位 -----
    end_dt = start_dt + dt.timedelta(hours=hours)
    # 均勻隨機時間戳
    delta_sec = (end_dt - start_dt).total_seconds()
    sec_offsets = np.random.rand(rows) * delta_sec
    ts = np.array([ (start_dt + dt.timedelta(seconds=float(s))).replace(microsecond=0).isoformat() for s in sec_offsets ], dtype=object)

    hours_arr = np.array([ int(ts_i[11:13]) for ts_i in ts ])  # 擷取小時做班別
    shift = np.array([ current_shift_by_hour(h) for h in hours_arr ], dtype=object)

    # 站別
    st_ids = [f"ST{i:02d}" for i in range(1, stations+1)]
    station_id = rand_choice(st_ids, rows)
    machine_id = np.char.add(station_id, "-M01")

    # 操作員（每站 10 人池）
    op_map = {st: np.array([f"{st}-OP{n:02d}" for n in range(1, 11)], dtype=object) for st in st_ids}
    operator_id = np.empty(rows, dtype=object)
    # 向量化指派操作員
    for st in st_ids:
        idx = np.where(station_id == st)[0]
        if idx.size > 0:
            operator_id[idx] = rand_choice(op_map[st], idx.size)

    # 產品/工單/客戶（隨機分配）
    skus = np.array([p[0] for p in PRODUCTS], dtype=object)
    pnames = np.array([p[1] for p in PRODUCTS], dtype=object)
    sku = rand_choice(skus, rows)
    product_name = np.empty(rows, dtype=object)
    for i in range(rows):
        # 對應 SKU→產品名（簡單映射）
        if sku[i] == "SKU-A100": product_name[i] = "Alpha Frame"
        elif sku[i] == "SKU-B250": product_name[i] = "Beta Housing"
        else: product_name[i] = "Carbon Plate"

    customer = rand_choice(CUSTOMERS, rows)
    priority = rand_choice(PRIORITIES, rows)
    # due_date：從 ts 日期起 1~10 天
    ts_dates = np.array([ dt.datetime.fromisoformat(t).date() for t in ts ])
    due_offsets = np.random.randint(1, 11, size=rows)
    due_date = np.array([ (d + dt.timedelta(days=int(off))).isoformat() for d, off in zip(ts_dates, due_offsets) ], dtype=object)
    # work_order_id / lot_id
    work_order_id = np.array([ f"WO-{100000 + i}" for i in np.random.randint(0, 1000000, size=rows) ], dtype=object)
    lot_id = np.array([ f"LOT-{d.strftime('%Y%m%d')}-{random_string(3)}" for d in ts_dates ], dtype=object)

    line_id_arr = np.full(rows, line_id, dtype=object)

    # ----- 狀態碼與 cycle_time -----
    # 先抽狀態碼
    weights = STATUS_DEF[:,2].astype(float)
    weights = weights / weights.sum()
    status_idx = np.random.choice(np.arange(len(STATUS_DEF)), size=rows, p=weights)
    status_code = STATUS_DEF[status_idx, 0].astype(int)
    status_name = STATUS_DEF[status_idx, 1]

    # 站別對應 base/tail
    base_mean = np.vectorize(lambda st: BASE_CYCLE_MEAN.get(st, 6.0))(station_id).astype(float)
    tail_h = np.vectorize(lambda st: TAIL_HEAVINESS.get(st, 1.25))(station_id).astype(float)

    # 對數常態參數（讓 median ~ base_mean）
    mu = np.log(base_mean) - 0.5
    sigma = 0.35 * tail_h
    # 先抽對數常態
    ct = np.random.lognormal(mean=mu, sigma=sigma, size=rows)
    # 狀態修飾
    # MINOR_STOP：1.1~1.4x
    mask_minor = (status_code == 1)
    ct[mask_minor] = ct[mask_minor] * np.random.uniform(1.1, 1.4, size=mask_minor.sum())
    # QUALITY_HOLD：1.05~1.2x
    mask_q = (status_code == 4)
    ct[mask_q] = ct[mask_q] * np.random.uniform(1.05, 1.2, size=mask_q.sum())
    # MAJOR_STOP/MAINT → 無產出
    mask_stop = np.isin(status_code, [2,3])
    ct[mask_stop] = np.nan
    # 下限
    ct = np.where(np.isnan(ct), np.nan, np.maximum(ct, 0.8))
    # 產出旗標（有 cycle 即視為產出）
    produced = ~np.isnan(ct)

    # 開始/結束/持續
    end_ts = ts
    # 有產出的才有 start_ts/duration
    duration_sec = np.where(produced, ct, np.nan)
    start_ts = np.array([ None ]*rows, dtype=object)
    for i in np.where(produced)[0]:
        t_end = dt.datetime.fromisoformat(end_ts[i])
        t_start = (t_end - dt.timedelta(seconds=float(duration_sec[i]))).replace(microsecond=0)
        start_ts[i] = t_start.isoformat()

    # 停機/保養/品質暫停 → downtime_reason & duration（若無產出）
    downtime_reason = np.array([ None ]*rows, dtype=object)
    for code, reasons in DOWNTIME_REASONS.items():
        mask = (status_code == code) & (~produced)
        if mask.any():
            downtime_reason[mask] = rand_choice(reasons, mask.sum())
            # 若沒有 duration，給估計秒數
            if code == 1:  # 微停
                duration_sec[mask] = np.random.uniform(5, 40, size=mask.sum())
            elif code == 2:
                duration_sec[mask] = np.random.uniform(60, 900, size=mask.sum())
            elif code == 3:
                duration_sec[mask] = np.random.uniform(120, 600, size=mask.sum())
            elif code == 4:
                duration_sec[mask] = np.random.uniform(10, 120, size=mask.sum())
            # start_ts（用 end_ts 回推）
            for i in np.where(mask)[0]:
                t_end = dt.datetime.fromisoformat(end_ts[i])
                t_start = (t_end - dt.timedelta(seconds=float(duration_sec[i]))).replace(microsecond=0)
                start_ts[i] = t_start.isoformat()

    # SN / 數量 / 品質
    serial_number = np.array([ None ]*rows, dtype=object)
    qty_in  = np.where(produced, 1, 0).astype(int)
    qty_out = np.where(produced, 1, 0).astype(int)
    scrap_qty = np.zeros(rows, dtype=int)
    good = np.array([ None ]*rows, dtype=object)
    defect_code = np.array([ None ]*rows, dtype=object)

    # 依站別產生 SN（格式：YYYYMMDD-Lxx-STxx-#######）
    # 為快速，簡化成亂數序號（如需嚴格連號可額外跑每站計數器，但速度會慢些）
    for i in np.where(produced)[0]:
        d = dt.datetime.fromisoformat(end_ts[i]).date().strftime("%Y%m%d")
        serial_number[i] = f"{d}-{line_id}-{station_id[i]}-{np.random.randint(1, 9999999):07d}"

    # 品質
    # OK: ~2% NG；其他（非停機）: ~5% NG；品質暫停：~70% NG
    good = np.where(produced, 1, None)
    ng_base = np.where(status_code == 0, 0.02, 0.05)
    ng_base = np.where(status_code == 4, 0.70, ng_base)
    randv = np.random.rand(rows)
    ng_mask = produced & (randv < ng_base)
    good = np.where(ng_mask, 0, good)
    scrap_qty[ng_mask] = 1
    # 缺陷碼
    if ng_mask.any():
        defect_code[ng_mask] = rand_choice(DEFECTS, ng_mask.sum())

    # 佇列（簡化：0~120 隨機，品質/停機稍高）
    queue_base = np.random.randint(0, 30, size=rows)
    queue_len = queue_base + (status_code == 1)*np.random.randint(0, 10, size=rows) \
                          + (np.isin(status_code, [2,3]))*np.random.randint(5, 40, size=rows)
    queue_len = np.minimum(queue_len, 120)

    # 感測
    temp_c = np.round(np.random.normal(45, 4, size=rows), 2)
    vibration = np.round(np.abs(np.random.normal(2.0, 0.7, size=rows)), 2)
    energy_kwh = np.round(np.where(produced, (ct/3600.0)*np.random.uniform(0.08, 0.15, size=rows),
                                   np.random.uniform(0.005, 0.02, size=rows)), 4)
    # 狀態影響
    temp_c += (status_code == 1)*np.random.uniform(0.5, 1.5, size=rows)
    vibration += (status_code == 1)*np.random.uniform(0.2, 0.6, size=rows)
    energy_kwh *= np.where(np.isin(status_code,[2,3]), np.random.uniform(0.2, 0.6, size=rows), 1.0)

    # start/end_ts 若 None 用 None，duration_sec 用 None
    start_ts = np.array(start_ts, dtype=object)
    end_ts   = np.array(end_ts, dtype=object)
    duration_sec = np.where(np.isnan(duration_sec), None, np.round(duration_sec, 2))

    # 機台/班別/… 組裝
    records = zip(
        ts, start_ts, end_ts, duration_sec,
        np.full(rows, line_id, dtype=object), station_id, machine_id, shift, operator_id,
        work_order_id, customer, due_date, priority,
        product_name, sku, lot_id, serial_number,
        status_code.astype(int), status_name, downtime_reason,
        np.where(produced, np.round(ct,2), None),
        qty_in, qty_out, scrap_qty,
        good, defect_code,
        queue_len.astype(int), np.round(temp_c,2), vibration, energy_kwh
    )
    return records

# =========================
# 寫入（CSV / SQLite）
# =========================
def write_csv(csv_path: str, rows_iter, batch_size=50000):
    new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
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
        batch = []
        for row in rows_iter:
            batch.append(row)
            if len(batch) >= batch_size:
                w.writerows(batch)
                batch.clear()
        if batch:
            w.writerows(batch)

def write_sqlite(conn: sqlite3.Connection, rows_iter, batch_size=50000):
    cur = conn.cursor()
    sql = """
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
    """
    batch = []
    n = 0
    for row in rows_iter:
        batch.append(row)
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            conn.commit()
            n += len(batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        n += len(batch)
    return n

# =========================
# CLI
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000, help="要生成的資料筆數")
    ap.add_argument("--stations", type=int, default=10, help="站別數量（ST01..）")
    ap.add_argument("--line", type=str, default="L01", help="產線ID")
    ap.add_argument("--start", type=str, default=None, help="起始時間 ISO8601（預設=現在）")
    ap.add_argument("--hours", type=float, default=8.0, help="資料分佈的時間範圍（小時）")
    ap.add_argument("--db", type=str,
                default=r"C:\GITHUB\smart_manufacturing_practice\data.db",
                help="SQLite 路徑（絕對）")
    ap.add_argument("--csv", type=str,
                default=r"C:\GITHUB\smart_manufacturing_practice\data.csv",
                help="CSV 路徑（絕對）")

    ap.add_argument("--batch", type=int, default=50_000, help="批次大小（寫入/輸出）")
    args = ap.parse_args()

    # 解析時間
    start_dt = dt.datetime.now().replace(microsecond=0) if args.start is None else dt.datetime.fromisoformat(args.start)

    # 準備路徑與 DB
    db_path  = ensure_dirs(args.db)
    csv_path = ensure_dirs(args.csv)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    print(f"[info] generating rows={args.rows:,}, stations={args.stations}, hours={args.hours}, start={start_dt.isoformat()}")
    t0 = time.time()
    rec_iter = generate_bulk(args.rows, args.stations, start_dt, args.hours, args.line)
    # 因為要寫 CSV 與 SQLite，rec_iter 需重複走兩次 → 轉 list（內存夠時）
    # 若 rows 極大（>5e6），可選擇只寫DB或只寫CSV，或拆兩次各自生成。
    data = list(rec_iter)
    t1 = time.time()
    print(f"[gen] generated in {t1-t0:.2f}s")

    # CSV
    print(f"[csv] writing to {csv_path} ...")
    write_csv(csv_path, iter(data), batch_size=args.batch)

    # SQLite
    print(f"[db] opening {db_path} ...")
    conn = sqlite3.connect(db_path, timeout=120)
    prepare_db(conn)
    print("[db] bulk inserting ...")
    n = write_sqlite(conn, iter(data), batch_size=args.batch)
    print(f"[db] inserted {n:,} rows. creating indexes ...")
    create_indexes(conn)
    conn.close()

    t2 = time.time()
    print(f"[done] total time {t2-t0:.2f}s | CSV: {csv_path} | DB: {db_path}")

if __name__ == "__main__":
    np.random.seed(42)
    random.seed(42)
    main()
