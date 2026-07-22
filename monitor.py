"""임진강·사미천 수문자료 10분 감시 서버 (Python 3.10+, 외부 의존성 없음)."""
from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from html import unescape

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
EXAMPLE_PATH = ROOT / "config.example.json"
DB_PATH = ROOT / "hydrology.sqlite3"
STATUS_CACHE_PATH = ROOT / "status_cache.json"
WEATHER_CACHE_PATH = ROOT / "weather_cache.json"
CCTV_CACHE_PATH = ROOT / "cctv_cache.json"
LOCK = threading.Lock()
LAST_RESULT: dict[str, Any] = {"updated_at": None, "errors": []}
CCTV_CACHE: dict[str, Any] = {"fetched_at": 0.0, "areas": [], "errors": []}
WEATHER_CACHE: dict[str, Any] = {"fetched_at": 0.0, "areas": [], "errors": []}
CCTV_AREA_LABELS = {
    "pilsunggyo": "\ud544\uc2b9\uad50 \uc218\ubb38 \uc778\uadfc CCTV",
    "samicheongyo": "\uc0ac\ubbf8\ucc9c\uad50 \uc218\ubb38 \uc778\uadfc CCTV",
    "gunnamdam": "\uad70\ub0a8\ub310 \uc218\ubb38 \uc778\uadfc CCTV",
    "jangnam": "\uc5f0\ucc9c\uad70 \uc7a5\ub0a8\uba74 \uc77c\ub300 CCTV",
    "baekhak": "\uc5f0\ucc9c\uad70 \ubc31\ud559\uba74 \uc77c\ub300 CCTV",
    "wangjing": "\uc5f0\ucc9c\uad70 \uc655\uc9d5\uba74 \uc77c\ub300 CCTV",
    "yangju_nammyeon": "\uc591\uc8fc\uc2dc \ub0a8\uba74 \uc77c\ub300 CCTV",
}


def load_config() -> dict[str, Any]:
    config = json.loads((CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_PATH).read_text(encoding="utf-8"))
    hfc_key = str(os.getenv("HRFCO_API_KEY") or config.get("hfc_api_key") or "").strip()
    public_data_key = str(os.getenv("PUBLIC_DATA_SERVICE_KEY") or config.get("public_data_service_key") or "").strip()
    its_key = str(os.getenv("ITS_CCTV_API_KEY") or config.get("its_cctv_api_key") or "").strip()
    config["kma_api_key"] = str(os.getenv("KMA_API_KEY") or config.get("kma_api_key") or "").strip()
    if hfc_key:
        hfc = config.get("sources", {}).get("hfc", {})
        hfc["url"] = str(hfc.get("url", "")).replace("${HRFCO_API_KEY}", hfc_key)
    if public_data_key:
        for source in config.get("sources", {}).values():
            source["url"] = str(source.get("url", "")).replace("${PUBLIC_DATA_SERVICE_KEY}", public_data_key)
            source["params"] = {key: str(value).replace("${PUBLIC_DATA_SERVICE_KEY}", public_data_key) for key, value in source.get("params", {}).items()}
    if its_key:
        its = config.get("sources", {}).get("ntic_cctv", {})
        its["params"] = {key: str(value).replace("${ITS_CCTV_API_KEY}", its_key) for key, value in its.get("params", {}).items()}
    return config


def save_hfc_key(key: str) -> None:
    key = key.strip()
    if len(key) < 8 or any(char.isspace() for char in key):
        raise ValueError("INVALID_HFC_KEY")
    config = json.loads((CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_PATH).read_text(encoding="utf-8"))
    config["hfc_api_key"] = key
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def save_public_data_key(key: str) -> None:
    key = key.strip()
    if len(key) < 8 or any(char.isspace() for char in key):
        raise ValueError("INVALID_PUBLIC_DATA_KEY")
    config = json.loads((CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_PATH).read_text(encoding="utf-8"))
    config["public_data_service_key"] = key
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def save_its_cctv_key(key: str) -> None:
    key = key.strip()
    if len(key) < 8 or any(char.isspace() for char in key):
        raise ValueError("INVALID_ITS_CCTV_KEY")
    config = json.loads((CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_PATH).read_text(encoding="utf-8"))
    config["its_cctv_api_key"] = key
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def save_kma_key(key: str) -> None:
    key = key.strip()
    if len(key) < 8 or any(char.isspace() for char in key):
        raise ValueError("INVALID_KMA_API_KEY")
    config = json.loads((CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_PATH).read_text(encoding="utf-8"))
    config["kma_api_key"] = key
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS readings (
            station_id TEXT NOT NULL, observed_at TEXT NOT NULL, fetched_at TEXT NOT NULL,
            level REAL, flow REAL, inflow REAL, outflow REAL,
            PRIMARY KEY (station_id, observed_at))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS weather_observations (
            target_id TEXT NOT NULL, observed_hour TEXT NOT NULL, rain REAL NOT NULL,
            fetched_at TEXT NOT NULL, PRIMARY KEY (target_id, observed_hour))""")


def expand(value: str) -> str:
    for key, env_value in os.environ.items():
        value = value.replace("${" + key + "}", env_value)
    now = datetime.now()
    now = now.replace(minute=now.minute // 10 * 10, second=0, microsecond=0)
    return (value.replace("{today}", now.strftime("%Y-%m-%d"))
                 .replace("{start_hfc}", (now - timedelta(hours=2)).strftime("%Y%m%d%H%M"))
                 .replace("{start_hfc_week}", (now - timedelta(days=7)).strftime("%Y%m%d%H%M"))
                 .replace("{end_hfc}", now.strftime("%Y%m%d%H%M")))


def find_value(data: Any, names: list[str]) -> Any:
    """중첩 JSON/XML 변환 결과에서 후보 필드명으로 첫 값을 찾는다."""
    wanted = {name.lower() for name in names}
    if isinstance(data, dict):
        for key, value in data.items():
            if key.lower() in wanted and value not in (None, ""):
                return value
        for value in data.values():
            found = find_value(value, names)
            if found not in (None, ""):
                return found
    elif isinstance(data, list):
        for value in data:
            found = find_value(value, names)
            if found not in (None, ""):
                return found
    return None


def as_float(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def dict_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        records = [data]
        for value in data.values():
            records.extend(dict_records(value))
        return records
    if isinstance(data, list):
        records: list[dict[str, Any]] = []
        for value in data:
            records.extend(dict_records(value))
        return records
    return []


def resolve_kwater_dam_code(service_key: str) -> str:
    params = urllib.parse.urlencode({"serviceKey": service_key, "pageNo": 1, "numOfRows": 100, "_type": "json"})
    url = "https://apis.data.go.kr/B500001/dam/damCode/damCodelist?" + params
    with urllib.request.urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    for record in dict_records(payload):
        name = str(find_value(record, ["damnm", "damname", "dam_name"]) or "")
        code = find_value(record, ["damcode", "dam_code"])
        if "\uad70\ub0a8" in name and code not in (None, ""):
            return str(code)
    raise RuntimeError("KWATER_GUNNAM_CODE_NOT_FOUND")


def request_json(source: dict[str, Any], station: dict[str, Any]) -> Any:
    url = expand(source["url"])
    path = station.get("path")
    if path:
        url = url.rstrip("/") + "/" + expand(str(path)).lstrip("/")
    if not url.startswith("http") or "PASTE_" in url:
        raise RuntimeError("API URL이 설정되지 않았습니다")
    params = {key: expand(str(value)) for key, value in source.get("params", {}).items()}
    params.update({key: expand(str(value)) for key, value in station.get("params", {}).items()})
    if "api.hrfco.go.kr" in url and "${HRFCO_API_KEY}" in url:
        raise RuntimeError("HFC_KEY_REQUIRED")
    if "apis.data.go.kr" in url and "${PUBLIC_DATA_SERVICE_KEY}" in str(params.get("serviceKey", "")):
        raise RuntimeError("PUBLIC_DATA_KEY_REQUIRED")
    if "openapi.its.go.kr" in url and "${ITS_CCTV_API_KEY}" in str(params.get("apiKey", "")):
        raise RuntimeError("ITS_CCTV_KEY_REQUIRED")
    if "B500001/dam/sluicePresentCondition" in url:
        params.setdefault("stdt", datetime.now().strftime("%Y-%m-%d"))
        params.setdefault("eddt", datetime.now().strftime("%Y-%m-%d"))
        params.setdefault("damcode", resolve_kwater_dam_code(str(params["serviceKey"])))
    if not path and not params.get("serviceKey") and not params.get("apiKey"):
        raise RuntimeError("필요한 API 인증키가 설정되지 않았습니다")
    separator = "&" if "?" in url else "?"
    request_url = url + separator + urllib.parse.urlencode(params)
    timeout_seconds = float(source.get("timeout_seconds", 20))
    with urllib.request.urlopen(request_url, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("JSON 응답이 아닙니다. API URL·출력형식을 확인하세요") from exc
    if "api.hrfco.go.kr" in url and isinstance(payload, dict) and payload.get("code") not in (None, "0", 0, "200", 200):
        raise RuntimeError(f"HFC_API_{payload.get('code')}")
    return payload


def record_candidates(data: Any, time_fields: list[str]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    wanted = {name.lower() for name in time_fields}
    if isinstance(data, dict):
        if any(key.lower() in wanted for key in data):
            candidates.append(data)
        for value in data.values():
            candidates.extend(record_candidates(value, time_fields))
    elif isinstance(data, list):
        for value in data:
            candidates.extend(record_candidates(value, time_fields))
    return candidates


def normalize(station: dict[str, Any], payload: Any) -> dict[str, Any]:
    fields = station["fields"]
    candidates = record_candidates(payload, fields["time"])
    latest = max(candidates, key=lambda item: str(find_value(item, fields["time"]) or ""), default=payload)
    observed = find_value(latest, fields["time"])
    # 관측 시각이 없으면 수집 시각을 쓰되, 화면에서 표시해 검증 가능하게 한다.
    observed_at = str(observed or datetime.now().strftime("%Y-%m-%d %H:%M"))
    row = {"station_id": station["id"], "observed_at": observed_at, "fetched_at": datetime.now().isoformat(timespec="seconds")}
    for metric in ("level", "flow", "inflow", "outflow"):
        row[metric] = as_float(find_value(latest, fields.get(metric, [])))
    return row


def normalize_all(station: dict[str, Any], payload: Any) -> list[dict[str, Any]]:
    candidates = record_candidates(payload, station["fields"]["time"])
    if not candidates:
        return [normalize(station, payload)]
    return [normalize(station, candidate) for candidate in candidates]


def is_half_hour_observation(row: dict[str, Any]) -> bool:
    """Keep the 00- and 30-minute records from the 10-minute source data."""
    observed = re.sub(r"[^0-9]", "", str(row.get("observed_at", "")))
    return len(observed) < 12 or observed[10:12] in ("00", "30")


def needs_hfc_history(station_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute("SELECT COUNT(*) FROM readings WHERE station_id=? AND level IS NOT NULL", (station_id,)).fetchone()[0]
    return count < 900


def save_rows(rows: list[dict[str, Any]], retention_days: int) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""INSERT OR REPLACE INTO readings
            (station_id, observed_at, fetched_at, level, flow, inflow, outflow)
            VALUES (:station_id, :observed_at, :fetched_at, :level, :flow, :inflow, :outflow)""", rows)
        cutoff = (datetime.now() - timedelta(days=retention_days)).isoformat(timespec="seconds")
        conn.execute("DELETE FROM readings WHERE fetched_at < ?", (cutoff,))


def collect_once() -> None:
    config = load_config()
    rows, errors = [], []
    for station in config["stations"]:
        station_errors = []
        for source_id in [station["source"], *station.get("fallback_sources", [])]:
            try:
                source = config["sources"][source_id]
                request_station = dict(station)
                if source_id == "hfc" and needs_hfc_history(station["id"]):
                    request_station["path"] = str(station.get("path", "")).replace("{start_hfc}", "{start_hfc_week}")
                payload = request_json(source, request_station)
                rows.extend(row for row in normalize_all(station, payload) if is_half_hour_observation(row))
                break
            except Exception as exc:  # 개별 지점 오류가 다른 지점 수집을 막지 않게 한다.
                station_errors.append(str(exc))
        else:
            errors.append({"station": station["name"], "message": " / ".join(station_errors)})
    if rows:
        save_rows(rows, int(config.get("retention_days", 30)))
    status = {"updated_at": datetime.now().isoformat(timespec="seconds"), "errors": errors}
    with LOCK:
        LAST_RESULT.update(status)
    STATUS_CACHE_PATH.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")


def cctv_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        if any(key.lower() == "cctvurl" for key in data):
            return [data]
        result: list[dict[str, Any]] = []
        for value in data.values():
            result.extend(cctv_records(value))
        return result
    if isinstance(data, list):
        result: list[dict[str, Any]] = []
        for value in data:
            result.extend(cctv_records(value))
        return result
    return []


def fetch_cctv_areas(config: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    source = config["sources"].get("ntic_cctv")
    if not source:
        return [], [{"station": "CCTV", "message": "CCTV 제공원 설정이 없습니다"}]
    water_ids = {"pilsunggyo", "samicheongyo", "gunnamdam"}
    areas, errors = [], []

    def fetch_area(area: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
        try:
            station = {"params": {"minX": area["minX"], "maxX": area["maxX"], "minY": area["minY"], "maxY": area["maxY"]}}
            # 화면 API와 분리된 백그라운드 수집이므로 ITS의 최신 정지화상 응답에는
            # 충분한 여유를 주되, 모든 구역은 병렬로 조회한다.
            cctv_source = {**source, "timeout_seconds": 35}
            records = cctv_records(request_json(cctv_source, station))
            cameras = []
            for item in records[:4]:
                url = find_value(item, ["cctvurl"])
                if url:
                    cameras.append({"name": str(find_value(item, ["cctvname"]) or "도로 CCTV"), "url": str(url), "updated": str(find_value(item, ["filecreatetime"]) or "")})
            if cameras:
                return {"id": area["id"], "name": CCTV_AREA_LABELS.get(area["id"], area["name"]), "kind": "water" if area["id"] in water_ids else "road", "cameras": cameras}, None
        except Exception as exc:
            return None, {"station": area["name"], "message": str(exc)}
        return None, None

    configured_areas = config.get("cctv_areas", [])
    with ThreadPoolExecutor(max_workers=min(7, max(1, len(configured_areas)))) as executor:
        for result, error in executor.map(fetch_area, configured_areas):
            if result:
                areas.append(result)
            if error:
                errors.append(error)
    return areas, errors


def cctv_data(config: dict[str, Any]) -> dict[str, Any]:
    # 화면이 1분마다 갱신되므로, 여러 브라우저가 열려도 외부 API는 분당 한 번만 호출한다.
    if time.time() - CCTV_CACHE["fetched_at"] >= 55:
        areas, errors = fetch_cctv_areas(config)
        if areas and any(area.get("cameras") for area in areas):
            persisted = {"saved_at": datetime.now().isoformat(timespec="seconds"), "areas": areas}
            CCTV_CACHE_PATH.write_text(json.dumps(persisted, ensure_ascii=False), encoding="utf-8")
        elif CCTV_CACHE_PATH.exists():
            try:
                persisted = json.loads(CCTV_CACHE_PATH.read_text(encoding="utf-8"))
                cached_areas = persisted.get("areas", [])
                if cached_areas:
                    for area in cached_areas:
                        area["name"] = CCTV_AREA_LABELS.get(str(area.get("id") or ""), area.get("name"))
                    areas = cached_areas
                    errors = []
            except (OSError, json.JSONDecodeError):
                pass
        with LOCK:
            CCTV_CACHE.update({"fetched_at": time.time(), "areas": areas, "errors": errors})
    with LOCK:
        updated_at = datetime.fromtimestamp(CCTV_CACHE["fetched_at"]).isoformat(timespec="seconds") if CCTV_CACHE["fetched_at"] else None
        return {"areas": CCTV_CACHE["areas"], "errors": CCTV_CACHE["errors"], "updated_at": updated_at}


def strip_tags(value: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", value)).strip()


def kma_observed_rain(target: dict[str, Any]) -> tuple[str, float]:
    params = urllib.parse.urlencode({"myPointCode": target["code"], "showMidterm": "N", "showSrt": "Y", "showVsrt": "Y"})
    url = "https://www.weather.go.kr/weather/special/api/iframe/dfs.jsp?" + params
    with urllib.request.urlopen(url, timeout=20) as response:
        raw = response.read()
    page = raw.decode("euc-kr", errors="replace")
    block = re.search(r'<div class="now_weather1".*?</div>', page, re.S)
    # The KMA page exposes the current point's 1-hour rainfall as the final
    # `now_weather1_right` value.  Do not fall back to a different station.
    values = re.findall(r'<dd class="now_weather1_right[^>]*">(.*?)</dd>', block.group(0) if block else "", re.S)
    rain_text = strip_tags(values[-1]).replace("mm", "").strip() if values else ""
    rain = 0.0 if rain_text in ("", "-", "&nbsp;") else as_float(rain_text)
    if rain is None:
        raise RuntimeError("KMA_OBS_RAIN_UNAVAILABLE")
    observed_hour = datetime.now().strftime("%Y-%m-%d %H:00")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""INSERT OR REPLACE INTO weather_observations
            (target_id, observed_hour, rain, fetched_at) VALUES (?, ?, ?, ?)""",
            (target["code"], observed_hour, rain, datetime.now().isoformat(timespec="seconds")))
    return observed_hour, rain


def weekly_observed_rain(target_id: str, week_start: datetime) -> dict[str, float]:
    begin = week_start.strftime("%Y-%m-%d")
    end = (week_start + timedelta(days=7)).strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""SELECT substr(observed_hour, 1, 10), round(sum(rain), 1)
            FROM weather_observations WHERE target_id=? AND observed_hour>=? AND observed_hour<?
            GROUP BY substr(observed_hour, 1, 10)""", (target_id, begin, end)).fetchall()
    return {str(day): float(rain) for day, rain in rows}


ASOS_RAIN_CACHE: dict[str, dict[str, Any]] = {}


def asos_daily_rain(auth_key: str, station: str) -> dict[str, float]:
    """Return recent ASOS daily rainfall (RN_DAY) for the nearest local station."""
    if not auth_key or not station:
        return {}
    cached = ASOS_RAIN_CACHE.get(str(station), {"fetched_at": 0.0, "days": {}})
    if time.time() - float(cached["fetched_at"]) < 600:
        return dict(cached["days"])
    today = datetime.now().date()
    result: dict[str, float] = {}
    for offset in range(6, -1, -1):
        day = today - timedelta(days=offset)
        params = urllib.parse.urlencode({"tm": day.strftime("%Y%m%d"), "stn": station,
            "help": "1", "authKey": auth_key})
        url = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php?" + params
        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                lines = response.read().decode("utf-8", errors="replace").splitlines()
            record = next((line.strip() for line in lines if re.match(r"^\d{8},", line.strip())), "")
            values = record.split(",")
            if len(values) > 38:
                rain = as_float(values[38])
                if rain is not None and rain >= 0:
                    result[day.isoformat()] = round(rain, 1)
        except Exception:
            continue
    if result:
        ASOS_RAIN_CACHE[str(station)] = {"fetched_at": time.time(), "days": result}
    return result


def merged_observed_rain(target: dict[str, Any], week_start: datetime, auth_key: str) -> dict[str, float]:
    target_id = str(target.get("code") or target.get("kma_station") or target["name"])
    # The point page provides the latest hourly observation, but it cannot
    # backfill days before this dashboard started.  Complete the current week
    # with KMA's official daily ASOS observation feed, retaining point values
    # only when the daily record is not published yet.
    point_days = weekly_observed_rain(target_id, week_start)
    daily_days = asos_daily_rain(auth_key, str(target.get("asos_station") or ""))
    return {**point_days, **daily_days}


def kma_grid(lat: float, lon: float) -> tuple[int, int]:
    re = 6371.00877 / 5.0
    deg = math.pi / 180.0
    slat1, slat2, olon, olat = 30.0 * deg, 60.0 * deg, 126.0 * deg, 38.0 * deg
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(
        math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5))
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5) ** sn * math.cos(slat1) / sn
    ro = re * sf / math.tan(math.pi * 0.25 + olat * 0.5) ** sn
    ra = re * sf / math.tan(math.pi * 0.25 + lat * deg * 0.5) ** sn
    theta = (lon * deg - olon) * sn
    return int(ra * math.sin(theta) + 43.0 + 0.5), int(ro - ra * math.cos(theta) + 136.0 + 0.5)


def kma_base_time(now: datetime) -> tuple[str, str]:
    available = now - timedelta(minutes=15)
    hours = [2, 5, 8, 11, 14, 17, 20, 23]
    candidates = [hour for hour in hours if hour <= available.hour]
    if candidates:
        return available.strftime("%Y%m%d"), f"{max(candidates):02d}00"
    previous = available - timedelta(days=1)
    return previous.strftime("%Y%m%d"), "2300"


def rain_number(value: Any) -> float:
    text = str(value or "").replace("강수없음", "0")
    match = re.search(r"[0-9.]+", text)
    return float(match.group(0)) if match else 0.0


def lightning_risk(row: dict[str, Any]) -> int:
    """Estimate hourly lightning probability from KMA forecast precipitation signals."""
    weather = str(row.get("weather") or "")
    pop = float(row.get("pop") or 0)
    rain = float(row.get("rain") or 0)
    if "천둥" in weather or "번개" in weather:
        return min(100, max(70, int(pop)))
    if rain > 0:
        return min(60, max(10, round(pop * 0.55)))
    return 0


def kma_api_weather(target: dict[str, Any], auth_key: str) -> dict[str, Any]:
    nx, ny = kma_grid(float(target["lat"]), float(target["lon"]))
    base_date, base_time = kma_base_time(datetime.now())
    params = urllib.parse.urlencode({"pageNo": 1, "numOfRows": 2000, "dataType": "JSON",
        "base_date": base_date, "base_time": base_time, "nx": nx, "ny": ny, "authKey": auth_key})
    url = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getVilageFcst?" + params
    with urllib.request.urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    header = payload.get("response", {}).get("header", {})
    if str(header.get("resultCode", "00")) != "00":
        raise RuntimeError("KMA_API_" + str(header.get("resultMsg") or header.get("resultCode")))
    items = payload.get("response", {}).get("body", {}).get("items", {}).get("item", [])
    grouped: dict[tuple[str, int], dict[str, Any]] = {}
    for item in items:
        date, hour = str(item.get("fcstDate", "")), int(str(item.get("fcstTime", "0000"))[:2])
        grouped.setdefault((date, hour), {})[str(item.get("category"))] = item.get("fcstValue")
    sky_names = {1: "맑음", 3: "구름많음", 4: "흐림"}
    pty_names = {1: "비", 2: "비/눈", 3: "눈", 4: "소나기"}
    rows = []
    for (date, hour), values in sorted(grouped.items()):
        if "TMP" not in values:
            continue
        pty = int(float(values.get("PTY", 0) or 0))
        sky = int(float(values.get("SKY", 1) or 1))
        rows.append({"date": f"{date[:4]}-{date[4:6]}-{date[6:8]}", "hour": hour,
            "temp": float(values["TMP"]), "rain": rain_number(values.get("PCP")),
            "pop": float(values.get("POP", 0) or 0), "weather": pty_names.get(pty, sky_names.get(sky, "-"))})
    if not rows:
        raise RuntimeError("KMA_API_NO_FORECAST")
    return build_weather_result(target, rows)


def build_weather_result(target: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(row["date"], []).append(row)
    forecast_days: dict[str, dict[str, Any]] = {}
    for date, daily in by_date.items():
        phases = {}
        for phase, start, end in (("morning", 1, 11), ("afternoon", 12, 17), ("night", 18, 24)):
            selected = [item for item in daily if start <= item["hour"] <= end]
            if selected:
                phases[phase] = {"rain": round(sum(item["rain"] for item in selected), 1), "low": min(item["temp"] for item in selected), "high": max(item["temp"] for item in selected), "weather": list(dict.fromkeys(item["weather"] for item in selected))}
        forecast_days[date] = {"date": date, "rain": round(sum(item["rain"] for item in daily), 1), "low": min(item["temp"] for item in daily), "high": max(item["temp"] for item in daily), "phases": phases, "available": True}
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    target_id = str(target.get("code") or target.get("kma_station") or target["name"])
    observed = merged_observed_rain(target, week_start, str(load_config().get("kma_api_key") or ""))
    days = []
    forecast_cumulative = observed_cumulative = 0.0
    for offset in range(7):
        date = (week_start + timedelta(days=offset)).isoformat()
        day = forecast_days.get(date, {"date": date, "rain": None, "low": None, "high": None, "phases": {}, "available": False})
        if day["rain"] is not None:
            forecast_cumulative += day["rain"]
            day["cumulative_rain"] = round(forecast_cumulative, 1)
        else:
            day["cumulative_rain"] = None
        day["actual_rain"] = observed.get(date)
        if day["actual_rain"] is not None:
            observed_cumulative += day["actual_rain"]
            day["actual_cumulative_rain"] = round(observed_cumulative, 1)
        else:
            day["actual_cumulative_rain"] = None
        days.append(day)
    today_rows = [dict(row, lightning_risk=lightning_risk(row)) for row in rows if row["date"] == today.isoformat()]
    return {"name": target["name"], "today_date": today.isoformat(), "today": today_rows, "days": days, "source": "기상청 APIHub"}


def kma_web_weather(target: dict[str, Any]) -> dict[str, Any]:
    params = urllib.parse.urlencode({"code": target["code"], "unit": "m/s", "hr1": "Y", "lat": target["lat"], "lon": target["lon"]})
    url = "https://www.weather.go.kr/w/wnuri-fct2021/main/digital-forecast.do?" + params
    with urllib.request.urlopen(url, timeout=20) as response:
        page = response.read().decode("utf-8")
    try:
        kma_observed_rain(target)
    except Exception:
        pass
    rows = []
    pattern = re.compile(r'<ul class="item\s+(?:vs-item|s-item).*?data-date="(?P<date>[^"]+)"\s+.*?data-time="(?P<time>[^"]+)".*?>(?P<body>.*?)</ul>', re.S)
    for match in pattern.finditer(page):
        body = match.group("body")
        temperature = re.search(r'hid feel">([0-9.-]+)℃', body)
        weather = re.search(r'날씨: </span><span[^>]*title="([^"]+)"', body)
        if not temperature:
            continue
        pop_match = re.search(r'강수확률: </span><span>(.*?)</span>', body, re.S)
        pop = rain_number(strip_tags(pop_match.group(1))) if pop_match else 0.0
        rows.append({"date": match.group("date"), "hour": int(match.group("time")[:2]), "temp": float(temperature.group(1)), "weather": weather.group(1) if weather else "-", "pop": pop})
    rain_match = re.search(r'<div class="rainchart".*?data-data="(?P<data>\[\[.*?\]\])"', page, re.S)
    rainfall = json.loads(unescape(rain_match.group("data")))[0] if rain_match else []
    for index, row in enumerate(rows):
        row["rain"] = float(rainfall[index]) if index < len(rainfall) else 0.0
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(row["date"], []).append(row)
    forecast_days: dict[str, dict[str, Any]] = {}
    for date, daily in by_date.items():
        phases = {}
        for phase, start, end in (("morning", 1, 11), ("afternoon", 12, 17), ("night", 18, 24)):
            selected = [item for item in daily if start <= item["hour"] <= end]
            if selected:
                phases[phase] = {"rain": round(sum(item["rain"] for item in selected), 1), "low": min(item["temp"] for item in selected), "high": max(item["temp"] for item in selected), "weather": list(dict.fromkeys(item["weather"] for item in selected))}
        forecast_days[date] = {"date": date, "rain": round(sum(item["rain"] for item in daily), 1), "low": min(item["temp"] for item in daily), "high": max(item["temp"] for item in daily), "phases": phases, "available": True}
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    observed = merged_observed_rain(target, week_start, str(load_config().get("kma_api_key") or ""))
    days = []
    for offset in range(7):
        date = (week_start + timedelta(days=offset)).isoformat()
        days.append(forecast_days.get(date, {"date": date, "rain": None, "low": None, "high": None, "phases": {}, "available": False}))
    forecast_cumulative = 0.0
    observed_cumulative = 0.0
    for day in days:
        if day["rain"] is not None:
            forecast_cumulative += day["rain"]
            day["cumulative_rain"] = round(forecast_cumulative, 1)
        else:
            day["cumulative_rain"] = None
        day["actual_rain"] = observed.get(day["date"])
        if day["actual_rain"] is not None:
            observed_cumulative += day["actual_rain"]
            day["actual_cumulative_rain"] = round(observed_cumulative, 1)
        else:
            day["actual_cumulative_rain"] = None
    today_rows = [dict(row, lightning_risk=lightning_risk(row)) for row in rows if row["date"] == today.isoformat()]
    return {"name": target["name"], "today_date": today.isoformat(), "today": today_rows, "days": days, "source": "기상청 웹 단기예보"}


def kma_weather(target: dict[str, Any], auth_key: str = "") -> dict[str, Any]:
    if auth_key:
        try:
            return kma_api_weather(target, auth_key)
        except Exception:
            pass
    return kma_web_weather(target)


def empty_weather_days(target_id: str) -> list[dict[str, Any]]:
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    observed = weekly_observed_rain(target_id, week_start)
    cumulative = 0.0
    days = []
    for offset in range(7):
        date = (week_start + timedelta(days=offset)).isoformat()
        actual = observed.get(date)
        if actual is not None:
            cumulative += actual
        days.append({"date": date, "rain": None, "low": None, "high": None,
            "phases": {}, "available": False, "cumulative_rain": None,
            "actual_rain": actual,
            "actual_cumulative_rain": round(cumulative, 1) if actual is not None else None})
    return days


def kma_north_public_current(station: str) -> dict[str, Any] | None:
    """Fallback for stations published on KMA's public North Korea land forecast page."""
    url = "https://www.weather.go.kr/w/forecast/life/nk/land.do"
    with urllib.request.urlopen(url, timeout=30) as response:
        page = response.read().decode("utf-8", errors="replace")
    block = re.search(r'<ul class="nkpo-' + re.escape(station) + r'">(?P<body>.*?)</ul>', page, re.S)
    if not block:
        return None
    body = block.group("body")
    temperature = re.search(r'<li class="lowhigh"><span>([-0-9.]+)℃</span>', body)
    weather = re.search(r'<i[^>]+title="([^"]+)"', body)
    clock = re.search(r'<p class="clock">\s*([0-9]{2})월\s*([0-9]{2})일\s*([0-9]{2})시\s*현재', page)
    if not temperature:
        return None
    now = datetime.now()
    observed_at = now.replace(hour=int(clock.group(3)) if clock else now.hour, minute=0, second=0, microsecond=0)
    if clock:
        observed_at = observed_at.replace(month=int(clock.group(1)), day=int(clock.group(2)))
    return {"date": observed_at.date().isoformat(), "hour": observed_at.hour,
        "temp": float(temperature.group(1)), "rain": 0.0,
        "weather": weather.group(1) if weather else "관측", "observed": True}


def coordinate_weather_fallback(target: dict[str, Any]) -> dict[str, Any] | None:
    """Use a global weather-model fallback when a North Korean station feed is unavailable."""
    latitude = target.get("fallback_latitude", target.get("latitude"))
    longitude = target.get("fallback_longitude", target.get("longitude"))
    if latitude is None or longitude is None:
        return None
    params = urllib.parse.urlencode({"latitude": latitude, "longitude": longitude,
        "hourly": "temperature_2m,precipitation,precipitation_probability,weather_code",
        "timezone": "Asia/Seoul", "forecast_days": 3})
    with urllib.request.urlopen("https://api.open-meteo.com/v1/forecast?" + params, timeout=30) as response:
        hourly = json.loads(response.read().decode("utf-8")).get("hourly", {})
    weather_names = {0: "맑음", 1: "대체로 맑음", 2: "구름조금", 3: "흐림", 45: "안개", 48: "안개",
        51: "이슬비", 53: "이슬비", 55: "이슬비", 61: "비", 63: "비", 65: "강한 비",
        71: "눈", 73: "눈", 75: "강한 눈", 80: "소나기", 81: "소나기", 82: "강한 소나기",
        95: "뇌우", 96: "뇌우", 99: "강한 뇌우"}
    rows = []
    for index, stamp in enumerate(hourly.get("time", [])):
        rows.append({"date": stamp[:10], "hour": int(stamp[11:13]),
            "temp": float(hourly["temperature_2m"][index]), "rain": float(hourly["precipitation"][index] or 0),
            "pop": float(hourly.get("precipitation_probability", [0] * len(hourly["time"]))[index] or 0),
            "weather": weather_names.get(int(hourly.get("weather_code", [0] * len(hourly["time"]))[index] or 0), "-" )})
    if not rows:
        return None
    fallback_name = str(target.get("fallback_name") or target["name"])
    fallback_target = dict(target, name=fallback_name)
    result = build_weather_result(fallback_target, rows)
    if target.get("fallback_name"):
        result["name"] = f"신계군 · {fallback_name} 대체"
        result["source"] = "토산군 좌표 기반 세계날씨 예보(Open-Meteo)"
        result["forecast_notice"] = "신계군 기상청 관측 미수신 시 토산군 대체 예보"
    else:
        result["name"] = str(target["name"])
        result["source"] = "개성 좌표 기반 세계날씨 예보(Open-Meteo)"
        result["forecast_notice"] = "기상청 북한 GTS 관측 미수신 시 세계날씨 예보로 보완"
    return result


def kma_north_weather(target: dict[str, Any], auth_key: str) -> dict[str, Any]:
    """Load KMA GTS/SYNOP observations for a North Korean station."""
    station = str(target["kma_station"])
    today = datetime.now().date()
    station_names = {"47070": "황해도 개성시", "47067": "신계군"}
    result = {"name": station_names.get(station, target["name"]), "today_date": today.isoformat(), "today": [],
        "days": empty_weather_days(station), "source": "기상청 북한기상관측(GTS/SYNOP)",
        "forecast_notice": "북한 지점 관측자료 · 지점별 시간예보 미제공"}
    if not auth_key:
        result["forecast_notice"] += " · API 인증키 필요"
        return result
    params = urllib.parse.urlencode({"tm": datetime.utcnow().strftime("%Y%m%d%H%M"), "dtm": 24,
        "stn": station, "help": 1, "authKey": auth_key})
    url = "https://apihub.kma.go.kr/api/typ01/url/gts_bufr_syn1.php?" + params
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            text = response.read().decode("utf-8", errors="replace")
        header = None
        records = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            fields = re.split(r"\s+", stripped.lstrip("#").strip())
            if "STN" in fields and "TA" in fields and ("TM" in fields or "YYMMDDHHMI" in fields):
                header = fields
                continue
            if header and not stripped.startswith("#"):
                values = re.split(r"\s+", stripped)
                if len(values) >= len(header):
                    records.append(dict(zip(header, values)))
        matching = [row for row in records if str(row.get("STN", "")) == station]
        if matching:
            row = matching[-1]
            timestamp = str(row.get("TM") or row.get("YYMMDDHHMI") or "")
            temp = as_float(row.get("TA"))
            rain = as_float(row.get("RN"))
            if temp is not None:
                utc_hour = int(timestamp[8:10]) if len(timestamp) >= 10 else datetime.utcnow().hour
                observed_at = datetime.utcnow().replace(hour=utc_hour, minute=0) + timedelta(hours=9)
                item = {"date": observed_at.date().isoformat(), "hour": observed_at.hour, "temp": temp,
                    "rain": max(0.0, rain or 0.0), "weather": "관측", "observed": True}
                result["today"] = [item] if observed_at.date() == today else []
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("""INSERT OR REPLACE INTO weather_observations
                        (target_id, observed_hour, rain, fetched_at) VALUES (?, ?, ?, ?)""",
                        (station, observed_at.strftime("%Y-%m-%d %H:00"), item["rain"],
                         datetime.now().isoformat(timespec="seconds")))
                result["days"] = empty_weather_days(station)
                result["forecast_notice"] = "북한 지점 실황관측 · 지점별 시간예보 미제공"
    except Exception as exc:
        status = "API 활용승인 필요" if "403" in str(exc) else "관측자료 일시 미수신"
        result["forecast_notice"] += " · " + status
        try:
            public = kma_north_public_current(station)
            if public:
                result["today"] = [public] if public["date"] == today.isoformat() else []
                result["source"] = "기상청 북한육상예보 공개 실황"
                result["forecast_notice"] = "기상청 공개 실황 보완 · 북한 GTS API 활용승인 필요"
        except Exception:
            pass
        try:
            fallback = coordinate_weather_fallback(target)
            if fallback:
                return fallback
        except Exception:
            pass
    return result


def weather_data(config: dict[str, Any]) -> dict[str, Any]:
    if time.time() - WEATHER_CACHE["fetched_at"] >= 600:
        areas, errors = [], []
        for target in config.get("weather_targets", []):
            try:
                if target.get("country") == "KP" and target.get("kma_station"):
                    areas.append(kma_north_weather(target, str(config.get("kma_api_key") or "")))
                elif target.get("country") == "KR" and target.get("code"):
                    areas.append(kma_weather(target, str(config.get("kma_api_key") or "")))
            except Exception as exc:
                errors.append({"station": target["name"], "message": str(exc)})
        if areas:
            persisted = {"saved_at": datetime.now().isoformat(timespec="seconds"), "areas": areas}
            WEATHER_CACHE_PATH.write_text(json.dumps(persisted, ensure_ascii=False), encoding="utf-8")
        elif WEATHER_CACHE_PATH.exists():
            try:
                persisted = json.loads(WEATHER_CACHE_PATH.read_text(encoding="utf-8"))
                areas = persisted.get("areas", [])
                if areas:
                    errors = []
            except (OSError, json.JSONDecodeError):
                pass
        with LOCK:
            WEATHER_CACHE.update({"fetched_at": time.time(), "areas": areas, "errors": errors})
    with LOCK:
        updated_at = datetime.fromtimestamp(WEATHER_CACHE["fetched_at"]).isoformat(timespec="seconds") if WEATHER_CACHE["fetched_at"] else None
        return {"areas": WEATHER_CACHE["areas"], "errors": WEATHER_CACHE["errors"], "updated_at": updated_at}


def loop() -> None:
    while True:
        try:
            collect_once()
            # 외부 제공기관이 지연되어도 /api/monitor 요청은 즉시 응답하도록
            # 날씨와 CCTV 갱신은 수집 스레드에서만 수행한다.
            config = load_config()
            weather_data(config)
            cctv_data(config)
            interval = int(config.get("poll_interval_seconds", 600))
        except Exception as exc:
            with LOCK:
                LAST_RESULT.update({"updated_at": datetime.now().isoformat(timespec="seconds"), "errors": [{"station": "서버", "message": str(exc)}]})
            interval = 600
        time.sleep(max(60, interval))


def trend(values: list[float | None]) -> str:
    valid = [value for value in values if value is not None]
    if len(valid) < 2:
        return "자료 부족"
    delta = valid[-1] - valid[0]
    if abs(delta) < 0.001:
        return "유지"
    return "상승" if delta > 0 else "하강"


def dashboard_data() -> dict[str, Any]:
    config = load_config()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        result = []
        for station in config["stations"]:
            rows = [dict(row) for row in conn.execute("""SELECT * FROM readings WHERE station_id=?
                AND (level IS NOT NULL OR flow IS NOT NULL OR inflow IS NOT NULL OR outflow IS NOT NULL)
                ORDER BY observed_at DESC LIMIT 1008""", (station["id"],))]
            rows.reverse()
            rows = [row for row in rows if is_half_hour_observation(row)]
            latest = rows[-1] if rows else None
            metrics = ["level", "flow"] if station["type"] == "river" else ["level", "inflow", "outflow"]
            changes = {}
            for metric in metrics:
                values = [row[metric] for row in rows]
                changes[metric] = {
                    "ten_min": None if len(values) < 2 or values[-1] is None or values[-2] is None else round(values[-1] - values[-2], 3),
                    "one_hour": None if len(values) < 3 or values[-1] is None or values[-3] is None else round(values[-1] - values[-3], 3),
                    "trend": trend(values[-3:]),
                }
            # Field name is retained for the existing frontend contract, but these
            # are now the 00- and 30-minute records rather than hourly samples.
            hourly_history = list(rows)
            result.append({"id": station["id"], "name": station["name"], "type": station["type"], "latest": latest, "history": rows, "hourly_history": hourly_history, "changes": changes})
    with LOCK:
        status = dict(LAST_RESULT)
    if STATUS_CACHE_PATH.exists():
        try:
            persisted_status = json.loads(STATUS_CACHE_PATH.read_text(encoding="utf-8"))
            if str(persisted_status.get("updated_at") or "") > str(status.get("updated_at") or ""):
                status = persisted_status
        except (OSError, json.JSONDecodeError):
            pass
    with LOCK:
        weather = {
            "areas": list(WEATHER_CACHE["areas"]),
            "errors": list(WEATHER_CACHE["errors"]),
            "updated_at": datetime.fromtimestamp(WEATHER_CACHE["fetched_at"]).isoformat(timespec="seconds") if WEATHER_CACHE["fetched_at"] else None,
        }
        cctv = {
            "areas": list(CCTV_CACHE["areas"]),
            "errors": list(CCTV_CACHE["errors"]),
            "updated_at": datetime.fromtimestamp(CCTV_CACHE["fetched_at"]).isoformat(timespec="seconds") if CCTV_CACHE["fetched_at"] else None,
        }
    return {
        "stations": result,
        "status": status,
        "weather": weather,
        "cctv": cctv,
        "configured": CONFIG_PATH.exists(),
    }


def water_history_data(start_text: str, end_text: str) -> dict[str, Any]:
    start = datetime.strptime(start_text, "%Y-%m-%d")
    end = datetime.strptime(end_text, "%Y-%m-%d") + timedelta(days=1) - timedelta(minutes=10)
    minimum = datetime(2020, 1, 1)
    maximum = datetime.now() + timedelta(days=1)
    if start < minimum or end < start or end > maximum:
        raise ValueError("DATE_OUT_OF_RANGE")
    if (end - start).days > 366:
        raise ValueError("DATE_RANGE_TOO_LARGE")
    config = load_config()
    source = config["sources"]["hfc"]
    stations = []
    for station in config["stations"]:
        collected: list[dict[str, Any]] = []
        cursor = start
        while cursor <= end:
            chunk_end = min(end, cursor + timedelta(days=30) - timedelta(minutes=10))
            request_station = dict(station)
            request_station["path"] = str(station["path"]).replace(
                "{start_hfc}", cursor.strftime("%Y%m%d%H%M")).replace(
                "{end_hfc}", chunk_end.strftime("%Y%m%d%H%M"))
            payload = request_json(source, request_station)
            collected.extend(row for row in normalize_all(station, payload) if is_half_hour_observation(row))
            cursor = chunk_end + timedelta(minutes=10)
        half_hourly: dict[str, dict[str, Any]] = {}
        for row in collected:
            observed = re.sub(r"[^0-9]", "", str(row.get("observed_at", "")))
            if len(observed) < 10 or row.get("level") is None:
                continue
            if not is_half_hour_observation(row):
                continue
            stamp = observed[:12]
            half_hourly[stamp] = {"observed_at": stamp, "level": row["level"]}
        stations.append({"id": station["id"], "name": station["name"],
            "history": sorted(half_hourly.values(), key=lambda row: row["observed_at"])})
    return {"start": start_text, "end": end_text, "stations": stations,
        "source": "한강홍수통제소 표준수문DB"}


class Handler(BaseHTTPRequestHandler):
    def send_json(self, status: int, value: dict[str, Any]) -> None:
        body = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/health":
            self.send_json(200, {"ok": True}); return
        if parsed.path == "/api/monitor":
            self.send_json(200, dashboard_data()); return
        if parsed.path == "/api/water-history":
            try:
                params = urllib.parse.parse_qs(parsed.query)
                value = water_history_data(str(params.get("start", [""])[0]), str(params.get("end", [""])[0]))
                self.send_json(200, value)
            except ValueError as exc:
                self.send_json(400, {"error": str(exc)})
            except Exception as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path in ("/", "/index.html"):
            html_path = ROOT / "static" / "index.html"
            html = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
            self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Cache-Control", "no-store"); self.end_headers(); self.wfile.write(html.encode()); return
        self.send_error(404)

    def do_POST(self) -> None:
        if self.path not in ("/api/hfc-key", "/api/public-data-key", "/api/its-cctv-key"):
            self.send_error(404); return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if size > 4096:
                raise ValueError("INVALID_HFC_KEY")
            data = json.loads(self.rfile.read(size).decode("utf-8"))
            if self.path == "/api/hfc-key":
                save_hfc_key(str(data.get("key", "")))
            elif self.path == "/api/public-data-key":
                save_public_data_key(str(data.get("key", "")))
            else:
                save_its_cctv_key(str(data.get("key", "")))
            threading.Thread(target=collect_once, daemon=True).start()
            self.send_json(200, {"ok": True})
        except (ValueError, json.JSONDecodeError):
            self.send_json(400, {"ok": False, "error": "INVALID_API_KEY"})

    def log_message(self, *_: Any) -> None:
        return


if __name__ == "__main__":
    init_db()
    threading.Thread(target=loop, daemon=True).start()
    port = int(os.getenv("PORT", "8787"))
    print(f"수문 감시 서버: http://0.0.0.0:{port}")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
