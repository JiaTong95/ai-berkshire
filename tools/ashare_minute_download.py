#!/usr/bin/env python3
"""免费 A 股分钟/日线下载器。

设计目标
--------
在不使用付费终端（Wind/Choice/Tushare 积分）的前提下，尽量拿到
可复现的近 5 年分钟级数据，并诚实记录每个源的真实深度。

实测结论（2026-08-29，以 600519 探测）：
- BaoStock ``frequency=5``：2021-08-30 ~ 2026-08-28，58128 根，覆盖 1211 个交易日。
  这是目前能稳定拿到的、跨度约 5 年的最细免费分钟线。
- BaoStock 无 1 分钟线。
- 东方财富 ``klt=1``：通常只返回最近 1 个交易日约 240 根。
- 东方财富 ``trends2 ndays=5``：最近约 5 个交易日 1 分钟线。
- 新浪 ``scale=5``：约 5000 根 5 分钟线（约 5 个月），无法回放到 5 年。
- 腾讯 ``mkline m1``：当日或最近若干根，不能回放 5 年。

因此本工具的默认策略：
1. 近 5 年 **5 分钟** + **日线**：BaoStock（主）+ 东方财富日线（交叉验证）
2. 最近可获得的 **1 分钟**：东方财富 kline / trends2
3. 所有输出带来源、时间戳、行数、首末 bar，写入 manifest

用法
----
    python3 tools/ashare_minute_download.py probe
    python3 tools/ashare_minute_download.py download --out data/ashare
    python3 tools/ashare_minute_download.py download --out data/ashare --skip-minute
    python3 tools/ashare_minute_download.py snapshot --out data/ashare
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode


START_DEFAULT = "2021-08-30"
END_DEFAULT = "2026-08-28"
TIMEOUT = 40

# 研究宇宙：市值前十 + 对照 + 红利低波相关 ETF/指数
UNIVERSE = [
    # 指数
    {"code": "000001", "bs": "sh.000001", "name": "上证指数", "kind": "index"},
    {"code": "399300", "bs": "sz.399300", "name": "沪深300", "kind": "index"},
    {"code": "000016", "bs": "sh.000016", "name": "上证50", "kind": "index"},
    {"code": "000922", "bs": "sh.000922", "name": "中证红利", "kind": "index"},
    # 市值前十（2026-08-28 东方财富总市值排序，周六无交易）
    {"code": "688825", "bs": "sh.688825", "name": "长鑫科技", "kind": "stock"},
    {"code": "601398", "bs": "sh.601398", "name": "工商银行", "kind": "stock"},
    {"code": "601939", "bs": "sh.601939", "name": "建设银行", "kind": "stock"},
    {"code": "601288", "bs": "sh.601288", "name": "农业银行", "kind": "stock"},
    {"code": "600941", "bs": "sh.600941", "name": "中国移动", "kind": "stock"},
    {"code": "601857", "bs": "sh.601857", "name": "中国石油", "kind": "stock"},
    {"code": "601988", "bs": "sh.601988", "name": "中国银行", "kind": "stock"},
    {"code": "300750", "bs": "sz.300750", "name": "宁德时代", "kind": "stock"},
    {"code": "600938", "bs": "sh.600938", "name": "中国海油", "kind": "stock"},
    {"code": "600519", "bs": "sh.600519", "name": "贵州茅台", "kind": "stock"},
    {"code": "601138", "bs": "sh.601138", "name": "工业富联", "kind": "stock"},
    # 红利低波及相关对照 ETF
    {"code": "512890", "bs": "sh.512890", "name": "红利低波ETF华泰柏瑞", "kind": "etf"},
    {"code": "563020", "bs": "sh.563020", "name": "红利低波ETF易方达", "kind": "etf"},
    {"code": "159547", "bs": "sz.159547", "name": "红利低波ETF华夏", "kind": "etf"},
    {"code": "159525", "bs": "sz.159525", "name": "红利低波ETF富国", "kind": "etf"},
    {"code": "560150", "bs": "sh.560150", "name": "红利低波ETF泰康", "kind": "etf"},
    {"code": "563690", "bs": "sh.563690", "name": "红利低波ETF永赢", "kind": "etf"},
    {"code": "560730", "bs": "sh.560730", "name": "红利低波ETF国泰海通", "kind": "etf"},
    {"code": "560890", "bs": "sh.560890", "name": "红利低波ETF新华", "kind": "etf"},
    {"code": "515080", "bs": "sh.515080", "name": "中证红利ETF招商", "kind": "etf"},
    {"code": "510300", "bs": "sh.510300", "name": "沪深300ETF华泰柏瑞", "kind": "etf"},
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _em_secid(code: str) -> str:
    code = code.strip()
    if code.startswith(("6", "9", "5")):
        return f"1.{code}"
    return f"0.{code}"


def _qq_code(code: str) -> str:
    code = code.strip()
    if code.startswith(("6", "9", "5")):
        return f"sh{code}"
    return f"sz{code}"


def curl_text(url: str, referer: str = "https://quote.eastmoney.com/", retries: int = 4) -> str:
    last_err = None
    for attempt in range(retries):
        r = subprocess.run(
            [
                "/usr/bin/curl", "-sS", "--noproxy", "*", "-L",
                "--max-time", str(TIMEOUT),
                "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "-H", f"Referer: {referer}",
                url,
            ],
            capture_output=True,
        )
        if r.returncode == 0 and r.stdout.strip():
            raw = r.stdout
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError:
                return raw.decode("gbk", errors="replace")
        last_err = f"curl {r.returncode}: {url} {r.stderr[:200]!r}"
        time.sleep(1.5 * (attempt + 1))
    raise ConnectionError(last_err or f"empty reply: {url}")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv_gz(path: Path, header: list[str], rows: list[list]) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return {
        "path": str(path),
        "rows": len(rows),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "first": rows[0] if rows else None,
        "last": rows[-1] if rows else None,
    }


def em_kline(code: str, klt: str, beg: str, end: str, lmt: int = 10000) -> list[list]:
    """东方财富 K 线。klt: 1=1分钟, 5=5分钟, 101=日线。"""
    params = {
        "secid": _em_secid(code),
        "klt": klt,
        "fqt": "1",
        "beg": beg.replace("-", ""),
        "end": end.replace("-", ""),
        "lmt": str(lmt),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
    }
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?" + urlencode(params)
    data = json.loads(curl_text(url))
    klines = (data.get("data") or {}).get("klines") or []
    rows = []
    for item in klines:
        parts = item.split(",")
        rows.append(parts)
    return rows


def em_trends2(code: str, ndays: int = 5) -> list[list]:
    params = {
        "secid": _em_secid(code),
        "ndays": str(ndays),
        "iscr": "0",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
    }
    url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get?" + urlencode(params)
    data = json.loads(curl_text(url))
    trends = (data.get("data") or {}).get("trends") or []
    return [t.split(",") for t in trends]


def em_rank_top(n: int = 15) -> list[dict]:
    params = {
        "pn": "1",
        "pz": str(n),
        "po": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fid": "f20",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
        "fields": "f12,f14,f2,f20,f21,f9,f23,f115,f18",
    }
    url = "https://push2delay.eastmoney.com/api/qt/clist/get?" + urlencode(params)
    data = json.loads(curl_text(url))
    out = []
    for x in (data.get("data") or {}).get("diff") or []:
        out.append({
            "code": x.get("f12"),
            "name": x.get("f14"),
            "price": x.get("f2"),
            "market_cap": x.get("f20"),
            "float_cap": x.get("f21"),
            "pe": x.get("f9"),
            "pb": x.get("f23"),
            "pe_lyr": x.get("f115"),
        })
    return out


def qq_quotes(codes: list[str]) -> dict[str, dict]:
    q = ",".join(_qq_code(c) for c in codes)
    raw = curl_text(f"https://qt.gtimg.cn/q={q}", referer="https://gu.qq.com/")
    result = {}
    for line in raw.split(";"):
        line = line.strip()
        if not line or '="' not in line:
            continue
        start = line.find('"')
        end = line.rfind('"')
        fields = line[start + 1:end].split("~")
        if len(fields) < 46:
            continue
        code = fields[2]
        result[code] = {
            "name": fields[1],
            "code": code,
            "price": fields[3],
            "prev_close": fields[4],
            "open": fields[5],
            "volume_hands": fields[6],
            "high": fields[33] if len(fields) > 33 else "",
            "low": fields[34] if len(fields) > 34 else "",
            "change_pct": fields[32],
            "turnover_amt_wan": fields[37] if len(fields) > 37 else "",
            "pe": fields[39] if len(fields) > 39 else "",
            "float_cap_yi": fields[44] if len(fields) > 44 else "",
            "market_cap_yi": fields[45] if len(fields) > 45 else "",
            "pb": fields[46] if len(fields) > 46 else "",
        }
    return result


def bs_login():
    import baostock as bs
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock login failed: {lg.error_msg}")
    return bs


def bs_history(bs, bs_code: str, fields: str, start: str, end: str, frequency: str, adjustflag: str) -> list[list]:
    rs = bs.query_history_k_data_plus(
        bs_code,
        fields,
        start_date=start,
        end_date=end,
        frequency=frequency,
        adjustflag=adjustflag,
    )
    if rs.error_code != "0":
        raise RuntimeError(f"BaoStock {bs_code} {frequency}: {rs.error_msg}")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    return rows


def cmd_probe(args):
    print("== 东方财富总市值排名 ==")
    for i, row in enumerate(em_rank_top(12), 1):
        print(f"{i:2d} {row['code']} {row['name']}  price={row['price']}  mcap={row['market_cap']}")

    print("\n== 腾讯行情交叉 ==")
    codes = [x["code"] for x in UNIVERSE if x["kind"] == "stock"]
    quotes = qq_quotes(codes)
    for c in codes:
        q = quotes.get(c, {})
        print(f"{c} {q.get('name')} price={q.get('price')} mcap_yi={q.get('market_cap_yi')}")

    print("\n== 东方财富 1分钟深度（600519）==")
    rows = em_kline("600519", "1", args.start, args.end, lmt=100000)
    print("klt=1 count", len(rows), "first", rows[0] if rows else None, "last", rows[-1] if rows else None)
    tr = em_trends2("600519", 5)
    print("trends2 count", len(tr), "first", tr[0] if tr else None, "last", tr[-1] if tr else None)

    print("\n== BaoStock 5分钟深度（600519，可能较慢）==")
    bs = bs_login()
    try:
        rows = bs_history(
            bs, "sh.600519",
            "date,time,code,open,high,low,close,volume,amount",
            args.start, args.end, "5", "2",
        )
        print("5min count", len(rows), "first", rows[0] if rows else None, "last", rows[-1] if rows else None)
    finally:
        bs.logout()


def cmd_snapshot(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rank = em_rank_top(20)
    quotes = qq_quotes([x["code"] for x in UNIVERSE])
    payload = {
        "as_of_utc": _now_iso(),
        "note": "周六/周日无交易时，价格为最近一个交易日收盘快照",
        "eastmoney_mcap_rank": rank,
        "tencent_quotes": quotes,
    }
    path = out / "snapshot_quotes.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote", path)


def cmd_download(args):
    out = Path(args.out)
    daily_dir = out / "daily"
    min5_dir = out / "minute_5"
    min1_dir = out / "minute_1_recent"
    for d in (daily_dir, min5_dir, min1_dir):
        d.mkdir(parents=True, exist_ok=True)

    manifest = {
        "generated_utc": _now_iso(),
        "start": args.start,
        "end": args.end,
        "limitations": {
            "one_minute_5y": False,
            "one_minute_note": "免费接口无法提供全市场近5年1分钟线。本目录 minute_1_recent 仅为东方财富最近1-5日。",
            "five_minute_5y": True,
            "five_minute_source": "BaoStock query_history_k_data_plus frequency=5 adjustflag=2(前复权)",
            "daily_sources": ["BaoStock frequency=d", "EastMoney klt=101 fqt=1(前复权)"],
        },
        "files": [],
        "errors": [],
    }

    print("snapshot quotes...")
    try:
        cmd_snapshot(args)
    except Exception as e:
        manifest["errors"].append({"step": "snapshot", "error": str(e)})

    # 东方财富日线 + 最近1分钟（无需登录，较快）
    for item in UNIVERSE:
        if getattr(args, "skip_eastmoney", False):
            break
        code, name = item["code"], item["name"]
        if args.only and code not in args.only:
            continue
        print(f"[EM daily] {code} {name}")
        try:
            rows = em_kline(code, "101", args.start, args.end, lmt=3000)
            meta = write_csv_gz(
                daily_dir / f"{code}_{name}_em_daily.csv.gz",
                ["datetime", "open", "close", "high", "low", "volume", "amount", "amplitude"],
                rows,
            )
            meta.update({"code": code, "name": name, "source": "eastmoney", "freq": "daily"})
            manifest["files"].append(meta)
        except Exception as e:
            manifest["errors"].append({"code": code, "source": "eastmoney-daily", "error": str(e)})
            print("  FAIL", e)
        time.sleep(0.8)

        if args.skip_intraday_recent:
            continue
        print(f"[EM 1min] {code} {name}")
        try:
            rows = em_kline(code, "1", args.start, args.end, lmt=2000)
            if len(rows) < 10:
                rows = em_trends2(code, 5)
                src = "eastmoney-trends2"
            else:
                src = "eastmoney-kline-1"
            meta = write_csv_gz(
                min1_dir / f"{code}_{name}_em_1min.csv.gz",
                ["datetime", "open", "close", "high", "low", "volume", "amount", "extra"],
                rows,
            )
            meta.update({"code": code, "name": name, "source": src, "freq": "1min_recent"})
            manifest["files"].append(meta)
        except Exception as e:
            manifest["errors"].append({"code": code, "source": "eastmoney-1min", "error": str(e)})
            print("  FAIL", e)
        time.sleep(0.8)

    if args.skip_minute and args.skip_baostock_daily:
        _write_manifest(out, manifest)
        return

    print("BaoStock login...")
    bs = bs_login()
    try:
        for item in UNIVERSE:
            code, name, bsc = item["code"], item["name"], item["bs"]
            if args.only and code not in args.only:
                continue
            if not args.skip_baostock_daily:
                print(f"[BS daily] {bsc} {name}")
                try:
                    rows = bs_history(
                        bs, bsc,
                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,pctChg",
                        args.start, args.end, "d", "2",
                    )
                    meta = write_csv_gz(
                        daily_dir / f"{code}_{name}_bs_daily.csv.gz",
                        ["date", "code", "open", "high", "low", "close", "preclose", "volume", "amount", "adjustflag", "turn", "pctChg"],
                        rows,
                    )
                    meta.update({"code": code, "name": name, "source": "baostock", "freq": "daily"})
                    manifest["files"].append(meta)
                except Exception as e:
                    manifest["errors"].append({"code": code, "source": "baostock-daily", "error": str(e)})
                    print("  FAIL", e)
                time.sleep(0.2)

            if args.skip_minute:
                continue
            dest = min5_dir / f"{code}_{name}_bs_5min.csv.gz"
            if dest.exists() and dest.stat().st_size > 1000 and not args.force:
                print(f"[BS 5min] skip existing {dest.name}")
                continue
            print(f"[BS 5min] {bsc} {name}  (可能需要数分钟)")
            t0 = time.time()
            try:
                rows = bs_history(
                    bs, bsc,
                    "date,time,code,open,high,low,close,volume,amount",
                    args.start, args.end, "5", "2",
                )
                meta = write_csv_gz(
                    dest,
                    ["date", "time", "code", "open", "high", "low", "close", "volume", "amount"],
                    rows,
                )
                meta.update({
                    "code": code,
                    "name": name,
                    "source": "baostock",
                    "freq": "5min",
                    "elapsed_sec": round(time.time() - t0, 1),
                })
                manifest["files"].append(meta)
                print(f"  rows={len(rows)} elapsed={meta['elapsed_sec']}s")
            except Exception as e:
                manifest["errors"].append({"code": code, "source": "baostock-5min", "error": str(e)})
                print("  FAIL", e)
            # 中途落盘，防止长任务中断丢失进度
            _write_manifest(out, manifest)
            time.sleep(0.4)
    finally:
        try:
            bs.logout()
        except Exception:
            pass

    _write_manifest(out, manifest)
    print("done. files", len(manifest["files"]), "errors", len(manifest["errors"]))


def _write_manifest(out: Path, manifest: dict):
    path = out / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print("manifest", path)


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--start", default=START_DEFAULT)
    common.add_argument("--end", default=END_DEFAULT)
    common.add_argument("--out", default="data/ashare")

    p = argparse.ArgumentParser(description="免费 A 股近5年分钟/日线下载")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("probe", parents=[common])

    d = sub.add_parser("download", parents=[common])
    d.add_argument("--skip-minute", action="store_true", help="跳过5分钟（最耗时）")
    d.add_argument("--skip-baostock-daily", action="store_true")
    d.add_argument("--skip-intraday-recent", action="store_true")
    d.add_argument("--skip-eastmoney", action="store_true", help="跳过东方财富（限流时用）")
    d.add_argument("--force", action="store_true")
    d.add_argument("--only", nargs="*", help="只下载这些代码")

    sub.add_parser("snapshot", parents=[common])

    args = p.parse_args()
    if args.cmd == "probe":
        cmd_probe(args)
    elif args.cmd == "download":
        cmd_download(args)
    elif args.cmd == "snapshot":
        cmd_snapshot(args)


if __name__ == "__main__":
    main()
