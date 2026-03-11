# BYD Flash Charge Station Tracker - Configuration

API_URL = "https://chargeapp-cn.byd.auto/chargeMap/operator-server/app/V1/station/searchNearby"

# Request template (from captured HAR)
REQUEST_HEADERS = {
    "user-agent": "libcurl-agent/1.0",
    "accept": "*/*",
    "accept-encoding": "identity",
    "content-type": "application/json; charset=utf-8",
    "platform": "HARMONY",
    "softtype": "0",
    "version": "112",
}

# These fields stay constant
REQUEST_TEMPLATE = {
    "appChannel": "11",
    "imeiMD5": "1E88EEB22042C727264DB9ED716FB186",
    "identifier": "288253498326462464",
    "mapCode": "amap",
    "vehicleChargeType": 0,
    "orderBy": "general",
    "encryData": "3F0BB039B96DBD356777AFA2237A061BD63A0B1D6A558079917BACF67E81A002B904C45D290A7E48C0FF428865DCBDDE5974DEB7D03507643A8AA43AC0795D3A9ABEFD30844617DAE87CEC275C950F1F09BBEB3731E78DADA1FC959A948C66987A5FA9F03E2B4530CA246B74DE6FFB5682D37849C75044C6A49CEA7CAD3825F015108208ECD340F49AA1E9CC3E2277CC2A95B235DA1A21197426FBEB0FABAC73C22C9C3D56070237237E6FD778525C0CDF7958C269D86753F61BF2CE6281EFC5F546E3D0533E1876DD3406B63650C3CF2B8E844264FCBEFE14FF7F52BA858C8D96DBD51B5627D2E4E24EFBA4EE75DA4931773526AEAEC32FF2C02D8D5657F2DD5A10B7163B75D08A45E7F414D7F33771007E2F0D6973CE4EFD1D65EBE8C4441F029DFB5B477F0AFC4224CA7B8521A2A9F420D1D2A9887130ACE02019BA549166CB9E040E0293439D5DDFE9D3A3C4583CF18CD53CF928D462C153D099E8D8B456",
    "sign": "9A11D3526A4aF7f5DE1b965e589e964711533933",
}

# Concurrent workers for parallel API requests
CONCURRENT_WORKERS = 10

# Grid scan points covering populated China
# ~110km spacing, with population boundary filter
# Only scans areas likely to have stations (east of Hu Line + western corridors)
SCAN_GRID = []

lat_start, lat_end = 20.0, 50.0
lng_start, lng_end = 97.0, 135.5
lat_step = 0.72
lng_step = 0.90


def _in_populated_area(lat, lng):
    """Filter grid points to populated regions of China."""
    # Eastern China: lng >= 105 — densely populated, scan all
    if lng >= 105:
        return True
    # Southwest corridor (Yunnan, Guizhou, Sichuan, Chongqing): 97-105E, 20-35N
    if 97 <= lng < 105 and 20 <= lat <= 35:
        return True
    # Hexi corridor + Ningxia (Lanzhou → Urumqi belt): 100-108E, 35-42N
    if 100 <= lng < 108 and 35 < lat <= 42:
        return True
    return False


lat = lat_start
while lat <= lat_end:
    lng = lng_start
    while lng <= lng_end:
        if _in_populated_area(lat, lng):
            SCAN_GRID.append((round(lat, 4), round(lng, 4)))
        lng += lng_step
    lat += lat_step

# Amap (高德地图) reverse geocoding
AMAP_API_KEY = "eec97f01beba5127aaf51661d72b92d3"

# Database
DB_PATH = "data/stations.db"
DATA_DIR = "data"
