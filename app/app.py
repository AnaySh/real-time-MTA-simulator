"""Simple Flask API exposing boarding/off-boarding estimates."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
import sys
from pathlib import Path
from typing import Any, Dict

from flask import Flask, jsonify, render_template, request, send_from_directory

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import boardings as b


app = Flask(__name__, template_folder="templates", static_folder="static")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
_EXECUTOR = ThreadPoolExecutor(max_workers=4)
_RIDERSHIP_CACHE: dict[tuple, Dict[str, Any]] = {}
_IMAGES_DIR = Path(app.root_path).parent / "images"

# Build the SubwayGraph and caches up-front so the first request is fast.
b.init()


def _parse_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    required_fields = [
        "complex_id",
        "line",
        "direction",
        "year",
        "month",
        "day_of_week",
        "hour_of_day",
    ]
    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise BadRequest(f"Missing required fields: {', '.join(missing)}")

    try:
        return {
            "complex_id": str(payload["complex_id"]),
            "line": str(payload["line"]),
            "direction": int(payload["direction"]),
            "year": int(payload["year"]),
            "month": int(payload["month"]),
            "day_of_week": str(payload["day_of_week"]),
            "hour_of_day": int(payload["hour_of_day"]),
        }
    except (TypeError, ValueError) as exc:
        raise BadRequest(f"Invalid payload values: {exc}") from exc


@app.post("/ridership")
def ridership():
    payload = request.get_json(silent=True)
    if payload is None:
        raise BadRequest("Expected JSON payload.")

    data = _parse_payload(payload)

    cache_key = (
        data["complex_id"],
        data["line"],
        data["direction"],
        data["year"],
        data["month"],
        data["day_of_week"],
        data["hour_of_day"],
    )

    cached = _RIDERSHIP_CACHE.get(cache_key)
    if cached is not None:
        logger.info("Ridership cache hit for %s", cache_key)
        return jsonify(cached)

    future_onboards = _EXECUTOR.submit(
        b.get_onboards_data,
        data["complex_id"],
        data["year"],
        data["month"],
        data["day_of_week"],
        data["hour_of_day"],
    )
    future_offboards = _EXECUTOR.submit(
        b.get_offboards_data,
        data["complex_id"],
        data["year"],
        data["month"],
        data["day_of_week"],
        data["hour_of_day"],
    )

    df_onboards = future_onboards.result()
    df_offboards = future_offboards.result()

    future_onboardings = _EXECUTOR.submit(
        b.get_onboardings,
        data["complex_id"],
        data["line"],
        data["direction"],
        df_onboards,
    )
    future_offboardings = _EXECUTOR.submit(
        b.get_offboardings,
        data["complex_id"],
        data["line"],
        data["direction"],
        df_offboards,
    )

    onboard_total, onboard_df = future_onboardings.result()
    offboard_total, offboard_df = future_offboardings.result()

    logger.info(
        "Ridership request %s -> onboard_total=%.3f offboard_total=%.3f",
        data,
        onboard_total,
        offboard_total,
    )

    response_payload = {
        "inputs": data,
        "summary": {
            "onboard_total": onboard_total,
            "offboard_total": offboard_total,
        },
        "onboards": onboard_df.to_dict(orient="records"),
        "offboards": offboard_df.to_dict(orient="records"),
    }

    _RIDERSHIP_CACHE[cache_key] = response_payload

    return jsonify(response_payload)


@app.get("/stops")
def stops():
    line = request.args.get("line", type=str)
    direction = request.args.get("direction", type=int)

    if not line or direction is None:
        raise BadRequest("Query parameters 'line' and 'direction' are required.")

    try:
        station_ids = b.get_ordered_stops(line, direction, 1)
        station_names = b.get_ordered_stops(line, direction, 2)
    except Exception as exc:
        raise BadRequest(f"Unable to fetch stops: {exc}") from exc

    stops_data = [
        {"id": str(stop_id), "name": station_name}
        for stop_id, station_name in zip(station_ids, station_names)
    ]
    return jsonify({"stops": stops_data})


@app.get("/healthz")
def healthcheck():
    return jsonify({"status": "ok"})


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/images/<path:filename>")
def images(filename: str):
    image_path = _IMAGES_DIR / filename
    if not image_path.exists():
        raise NotFound(f"Image '{filename}' not found.")
    return send_from_directory(_IMAGES_DIR, filename)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

