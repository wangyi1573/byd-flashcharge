#!/bin/bash
set -e

echo "=== BYD 闪充站数据更新 & 部署 ==="

echo "[1/3] 爬取最新数据..."
python scraper.py

echo "[2/3] 导出静态 JSON..."
python export_json.py

echo "[3/3] 提交并推送..."
git add public/api/
git commit -m "data: update $(date +%Y-%m-%d)"
git push

echo "=== 部署完成! Cloudflare Pages 将自动更新 ==="
