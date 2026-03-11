"""Download province-level GeoJSON files from DataV for map drill-down.

Downloads 34 province/municipality/region GeoJSON files to public/static/maps/
and generates province_map.json index.
"""

import json
import os
import requests
import time

# DataV Aliyun GeoJSON API
DATAV_BASE = "https://geo.datav.aliyun.com/areas_v3/bound"

# Province adcode → name mapping (all 34 provinces)
PROVINCES = {
    "110000": "北京",
    "120000": "天津",
    "130000": "河北",
    "140000": "山西",
    "150000": "内蒙古",
    "210000": "辽宁",
    "220000": "吉林",
    "230000": "黑龙江",
    "310000": "上海",
    "320000": "江苏",
    "330000": "浙江",
    "340000": "安徽",
    "350000": "福建",
    "360000": "江西",
    "370000": "山东",
    "410000": "河南",
    "420000": "湖北",
    "430000": "湖南",
    "440000": "广东",
    "450000": "广西",
    "460000": "海南",
    "500000": "重庆",
    "510000": "四川",
    "520000": "贵州",
    "530000": "云南",
    "540000": "西藏",
    "610000": "陕西",
    "620000": "甘肃",
    "630000": "青海",
    "640000": "宁夏",
    "650000": "新疆",
    "710000": "台湾",
    "810000": "香港",
    "820000": "澳门",
}

OUTPUT_DIR = os.path.join("public", "static", "maps")


def download_province(adcode: str, name: str) -> bool:
    """Download a province GeoJSON with city-level boundaries (full)."""
    url = f"{DATAV_BASE}/{adcode}_full.json"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        filename = f"{adcode}.json"
        path = os.path.join(OUTPUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

        features = len(data.get("features", []))
        size = os.path.getsize(path)
        print(f"  {name} ({adcode}): {features} cities, {size:,} bytes")
        return True
    except Exception as e:
        print(f"  {name} ({adcode}): FAILED - {e}")
        return False


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Downloading {len(PROVINCES)} province GeoJSON files to {OUTPUT_DIR}/")

    province_map = {}
    success = 0

    for adcode, name in PROVINCES.items():
        if download_province(adcode, name):
            province_map[name] = f"{adcode}.json"
            success += 1
        time.sleep(0.2)

    # Write province_map.json index
    index_path = os.path.join(OUTPUT_DIR, "province_map.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(province_map, f, ensure_ascii=False, indent=2)

    print(f"\nDone: {success}/{len(PROVINCES)} provinces downloaded")
    print(f"Index: {index_path}")


if __name__ == "__main__":
    main()
