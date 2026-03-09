# BYD Flash Charge Station Tracker ⚡

每日追踪比亚迪全国闪充站数据。

## 安装

```bash
pip install requests flask numpy
```

## 使用

### 1. 抓取数据
```bash
python scraper.py
```
首次运行会扫描全国（约 3000+ 网格点），大约需要 15-20 分钟。

### 2. 启动网页
```bash
python web_server.py
```
打开 http://localhost:5000 查看数据面板。

### 3. 每日定时抓取（可选）

Linux crontab:
```bash
# 每天凌晨 3 点执行
0 3 * * * cd /path/to/byd-flashcharge && python scraper.py >> data/cron.log 2>&1
```

Windows Task Scheduler:
- 创建基本任务，每天 03:00 触发
- 操作：启动程序 `python`，参数 `scraper.py`，起始于项目目录

## 文件说明

```
byd-flashcharge/
├── config.py          # 配置：API、网格点、数据库路径
├── database.py        # SQLite 数据库操作
├── scraper.py         # 数据抓取脚本
├── web_server.py      # Flask Web 服务
├── templates/
│   └── index.html     # 前端面板
├── data/              # 数据目录（自动创建）
│   ├── stations.db    # SQLite 数据库
│   ├── raw_YYYY-MM-DD.json  # 每日原始数据
│   └── scraper.log    # 运行日志
└── README.md
```

## 数据面板功能

- 📊 全国站点总数、充电桩总数、覆盖城市数
- 📈 每日增长趋势图
- 🏙️ 各城市站点排行（可搜索）
- ⚡ 站点类型分类（普通站/高速站/站中站）
