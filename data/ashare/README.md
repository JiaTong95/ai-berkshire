# A 股免费分钟/日线数据集

数据截止：2026-08-28（最近一个交易日；研究日为周六 2026-08-29）

## 这个目录里有什么

| 路径 | 内容 | 是否入库 |
|------|------|----------|
| `snapshot_quotes.json` | 东方财富总市值排名 + 腾讯行情交叉快照 | 是 |
| `daily/*_em_daily.csv.gz` | 东方财富前复权日线，约 2021-08-30 起 | 是 |
| `daily/*_bs_daily.csv.gz` | BaoStock 前复权日线（交叉验证） | 是 |
| `minute_1_recent/*` | 东方财富最近 1–5 日 1 分钟线 | 是 |
| `minute_5/*` | BaoStock 近 5 年 5 分钟线（体积大） | **否**，`.gitignore`，用脚本重下 |
| `manifest.json` | 每个文件的行数、首末 bar、sha256 | 是 |

## 如何复现

```bash
# 探测各源真实深度（含一次 5 分钟全样本，可能数分钟）
python3 tools/ashare_minute_download.py probe

# 下载研究宇宙：市值前十 + 对照 + 红利低波相关 ETF
python3 tools/ashare_minute_download.py download --out data/ashare

# 只更新行情快照
python3 tools/ashare_minute_download.py snapshot --out data/ashare
```

依赖：系统 `curl`；5 分钟/BaoStock 日线需要 `pip install baostock`。无需 Token。

## 必须先读的限制

**免费接口不能提供 A 股全市场近 5 年 1 分钟线。** 本仓库能稳定复现的“分钟级、近 5 年”序列是 BaoStock 的 **5 分钟** K 线。1 分钟只覆盖最近 1–5 个交易日。完整论证见 `reports/A股分钟数据/A股免费分钟数据源与下载说明-20260829.md`。
