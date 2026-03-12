#!/usr/bin/env python3
"""
Spark-friendly outbound lead research workflow.

What this does:
1) Pulls public business listings for a city from OpenStreetMap (Overpass API).
2) Scores and normalizes those records into the same CSV shape used by portal import.
3) Optionally ingests records directly into Firestore `lead_research_imports` with
   a normal team admin account (Firebase Auth REST + Firestore REST).

This script intentionally uses only public data and includes explicit source attribution.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

USER_AGENT = "BrickBrickLeadResearch/1.0 (public-data-workflow)"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DEFAULT_COUNTRY = "USA"

CATEGORY_PRESETS: Dict[str, Dict[str, object]] = {
    "dentist": {
        "tags": [("amenity", "dentist")],
        "service_hint": "Website Build",
    },
    "plumber": {
        "tags": [("craft", "plumber")],
        "service_hint": "Automation Ops",
    },
    "electrician": {
        "tags": [("craft", "electrician")],
        "service_hint": "Automation Ops",
    },
    "roofing": {
        "tags": [("craft", "roofer")],
        "service_hint": "Website Build",
    },
    "hvac": {
        "tags": [("craft", "hvac"), ("craft", "air_conditioning")],
        "service_hint": "Automation Ops",
    },
    "real_estate": {
        "tags": [("office", "estate_agent")],
        "service_hint": "Website Build",
    },
    "accounting": {
        "tags": [("office", "accountant")],
        "service_hint": "Automation Ops",
    },
    "auto_repair": {
        "tags": [("shop", "car_repair"), ("amenity", "car_repair")],
        "service_hint": "Website Build",
    },
    "restaurant": {
        "tags": [("amenity", "restaurant")],
        "service_hint": "Website Build",
    },
    "fitness": {
        "tags": [("leisure", "fitness_centre"), ("leisure", "sports_centre")],
        "service_hint": "Website Build",
    },
    "salon": {
        "tags": [("shop", "hairdresser"), ("shop", "beauty")],
        "service_hint": "Website Build",
    },
}

CSV_HEADERS = [
    "title",
    "company",
    "contact",
    "email",
    "phone",
    "website",
    "source",
    "sourceUrl",
    "serviceHint",
    "notes",
    "confidence",
    "city",
    "state",
    "country",
    "directory",
    "category",
]


def http_json(
    url: str,
    *,
    method: str = "GET",
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    retries: int = 4,
    backoff_seconds: float = 2.0,
) -> object:
    req_headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        req_headers.update(headers)

    attempts = max(1, retries)
    for attempt in range(attempts):
        req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
            return json.loads(raw)
        except urllib.error.HTTPError as err:
            is_retryable = err.code in {429, 500, 502, 503, 504}
            if not is_retryable or attempt >= attempts - 1:
                raise
            time.sleep(backoff_seconds * (2 ** attempt))
        except urllib.error.URLError:
            if attempt >= attempts - 1:
                raise
            time.sleep(backoff_seconds * (2 ** attempt))

    raise RuntimeError("HTTP request failed after retries.")


def normalize_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def normalize_email(value: str) -> str:
    return value.strip().lower()


def normalize_url(value: str) -> str:
    v = value.strip()
    if not v:
        return ""
    if v.lower().startswith("http://") or v.lower().startswith("https://"):
        return v
    if "." not in v:
        return ""
    return f"https://{v}"


def source_key_from_label_or_url(label: str, url: str) -> str:
    base = (label or "").strip().lower()
    if not base and url:
        try:
            base = urllib.parse.urlparse(url).hostname or ""
        except Exception:
            base = url
    cleaned = normalize_slug(base.replace("www.", ""))
    return cleaned[:40] if cleaned else "public-directory"


def request_city_bbox(city: str, state: str, country: str, contact_email: str) -> Tuple[float, float, float, float]:
    q_parts = [city.strip()]
    if state.strip():
        q_parts.append(state.strip())
    if country.strip():
        q_parts.append(country.strip())
    params = {
        "q": ", ".join(q_parts),
        "format": "jsonv2",
        "limit": "1",
    }
    if contact_email:
        params["email"] = contact_email.strip()
    url = f"{NOMINATIM_URL}?{urllib.parse.urlencode(params)}"
    payload = http_json(url)
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("City lookup failed in Nominatim.")
    top = payload[0]
    bbox = top.get("boundingbox")
    if not isinstance(bbox, list) or len(bbox) != 4:
        raise RuntimeError("Nominatim response did not include a bounding box.")
    south = float(bbox[0])
    north = float(bbox[1])
    west = float(bbox[2])
    east = float(bbox[3])
    return south, west, north, east


def overpass_query_for_tags(
    south: float,
    west: float,
    north: float,
    east: float,
    tags: Iterable[Tuple[str, str]],
    timeout_s: int,
) -> str:
    clauses = []
    for key, value in tags:
        escaped_key = key.replace('"', '\\"')
        escaped_value = value.replace('"', '\\"')
        bbox = f"({south},{west},{north},{east})"
        clauses.append(f'node["{escaped_key}"="{escaped_value}"]{bbox};')
        clauses.append(f'way["{escaped_key}"="{escaped_value}"]{bbox};')
        clauses.append(f'relation["{escaped_key}"="{escaped_value}"]{bbox};')
    return f"[out:json][timeout:{timeout_s}];(" + "".join(clauses) + ");out center tags;"


def confidence_score(tags: Dict[str, str], website: str, email: str, phone: str, category: str) -> int:
    score = 35
    if website:
        score += 22
    if email:
        score += 12
    if phone:
        score += 10
    if tags.get("opening_hours"):
        score += 5
    if tags.get("operator"):
        score += 6
    if category in {"dentist", "plumber", "electrician", "roofing", "hvac", "accounting"}:
        score += 8
    return max(0, min(100, score))


def build_note(tags: Dict[str, str], category: str) -> str:
    parts = [f"Public-source research category: {category}."]
    if not tags.get("website") and not tags.get("contact:website"):
        parts.append("No website field found in listing; good website-upgrade candidate.")
    if tags.get("description"):
        parts.append(f"Directory description: {tags['description'][:280]}")
    return " ".join(parts)


def extract_record(
    element: Dict[str, object],
    *,
    category: str,
    service_hint: str,
    city: str,
    state: str,
    country: str,
    directory_label: str,
) -> Optional[Dict[str, str]]:
    tags = element.get("tags")
    if not isinstance(tags, dict):
        return None
    name = str(tags.get("name", "")).strip()
    if not name:
        return None

    website = normalize_url(str(tags.get("website") or tags.get("contact:website") or tags.get("url") or "").strip())
    email = normalize_email(str(tags.get("email") or tags.get("contact:email") or "").strip())
    phone = str(tags.get("phone") or tags.get("contact:phone") or "").strip()
    contact = str(tags.get("contact:person") or tags.get("operator") or "").strip()
    elem_type = str(element.get("type") or "node").strip()
    elem_id = str(element.get("id") or "").strip()
    source_url = f"https://www.openstreetmap.org/{elem_type}/{elem_id}" if elem_id else ""
    conf = confidence_score(tags, website, email, phone, category)
    note = build_note(tags, category)

    return {
        "title": name,
        "company": name,
        "contact": contact,
        "email": email,
        "phone": phone,
        "website": website,
        "source": directory_label,
        "sourceUrl": source_url,
        "serviceHint": service_hint,
        "notes": note,
        "confidence": str(conf),
        "city": city,
        "state": state,
        "country": country,
        "directory": directory_label,
        "category": category,
    }


def dedupe_records(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    best_by_key: Dict[str, Dict[str, str]] = {}
    for row in rows:
        name_key = normalize_slug(row.get("company", "") or row.get("title", ""))
        email_key = normalize_email(row.get("email", ""))
        website_key = normalize_slug(urllib.parse.urlparse(row.get("website", "")).netloc)
        key = "|".join([name_key, email_key or website_key])
        if not key.strip("|"):
            continue
        existing = best_by_key.get(key)
        if existing is None:
            best_by_key[key] = row
            continue
        prev_score = int(existing.get("confidence", "0") or "0")
        next_score = int(row.get("confidence", "0") or "0")
        if next_score > prev_score:
            best_by_key[key] = row
    return list(best_by_key.values())


def run_public_research(
    city: str,
    state: str,
    country: str,
    categories: List[str],
    per_category_limit: int,
    contact_email: str,
    directory_label: str,
    overpass_timeout: int,
    throttle_ms: int,
) -> List[Dict[str, str]]:
    south, west, north, east = request_city_bbox(city, state, country, contact_email)
    all_rows: List[Dict[str, str]] = []

    for category in categories:
        preset = CATEGORY_PRESETS[category]
        tags = preset["tags"]  # type: ignore[index]
        service_hint = str(preset["service_hint"])
        query_text = overpass_query_for_tags(south, west, north, east, tags, overpass_timeout)
        payload = http_json(
            OVERPASS_URL,
            method="POST",
            data=query_text.encode("utf-8"),
            headers={"Content-Type": "text/plain;charset=utf-8"},
            timeout=max(60, overpass_timeout + 20),
        )
        elements = payload.get("elements") if isinstance(payload, dict) else []
        if not isinstance(elements, list):
            elements = []

        category_rows: List[Dict[str, str]] = []
        for element in elements:
            if not isinstance(element, dict):
                continue
            rec = extract_record(
                element,
                category=category,
                service_hint=service_hint,
                city=city,
                state=state,
                country=country,
                directory_label=directory_label,
            )
            if rec:
                category_rows.append(rec)
        category_rows = dedupe_records(category_rows)
        category_rows.sort(key=lambda r: int(r.get("confidence", "0")), reverse=True)
        all_rows.extend(category_rows[:per_category_limit])

        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)

    return dedupe_records(all_rows)


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_HEADERS})


def firebase_sign_in(api_key: str, email: str, password: str) -> str:
    url = (
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
        f"?key={urllib.parse.quote(api_key)}"
    )
    payload = json.dumps(
        {"email": email, "password": password, "returnSecureToken": True}
    ).encode("utf-8")
    resp = http_json(
        url,
        method="POST",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    if not isinstance(resp, dict) or not resp.get("idToken"):
        raise RuntimeError("Firebase sign-in failed; idToken missing.")
    return str(resp["idToken"])


def firestore_fields_for_record(
    row: Dict[str, str],
    *,
    source_file: str,
    created_by_email: str,
) -> Dict[str, object]:
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    source_label = row.get("source", "").strip() or "Public directory"
    source_url = row.get("sourceUrl", "").strip()
    source_key = source_key_from_label_or_url(source_label, source_url)
    confidence = int(row.get("confidence", "50") or "50")

    string_fields = {
        "title": row.get("title", "").strip(),
        "company": row.get("company", "").strip(),
        "contact": row.get("contact", "").strip(),
        "email": normalize_email(row.get("email", "")),
        "phone": row.get("phone", "").strip(),
        "website": normalize_url(row.get("website", "")),
        "serviceHint": row.get("serviceHint", "").strip(),
        "note": row.get("notes", "").strip(),
        "publicSourceLabel": source_label,
        "publicSourceUrl": source_url,
        "researchSourceKey": source_key,
        "city": row.get("city", "").strip(),
        "state": row.get("state", "").strip(),
        "country": row.get("country", "").strip(),
        "category": row.get("category", "").strip(),
        "sourceFile": source_file,
        "status": "pending",
        "createdByUid": "workflow-script",
        "createdByEmail": created_by_email.strip(),
    }

    fields: Dict[str, object] = {
        key: {"stringValue": value} for key, value in string_fields.items()
    }
    fields["confidence"] = {"integerValue": str(max(0, min(100, confidence)))}
    fields["createdAt"] = {"timestampValue": now_iso}
    fields["updatedAt"] = {"timestampValue": now_iso}
    return fields


def ingest_rows_to_firestore(
    rows: List[Dict[str, str]],
    *,
    project_id: str,
    api_key: str,
    email: str,
    password: str,
    source_file: str,
    throttle_ms: int,
) -> Tuple[int, int]:
    id_token = firebase_sign_in(api_key=api_key, email=email, password=password)
    endpoint = (
        "https://firestore.googleapis.com/v1/projects/"
        f"{urllib.parse.quote(project_id)}/databases/(default)/documents/lead_research_imports"
    )
    ok = 0
    failed = 0
    for row in rows:
        body = json.dumps(
            {"fields": firestore_fields_for_record(row, source_file=source_file, created_by_email=email)}
        ).encode("utf-8")
        req = urllib.request.Request(
            endpoint,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60):
                ok += 1
        except urllib.error.HTTPError as err:
            failed += 1
            detail = err.read().decode("utf-8", errors="ignore")
            print(f"[ingest] Failed for {row.get('title', 'lead')}: HTTP {err.code} {detail}", file=sys.stderr)
        except Exception as err:
            failed += 1
            print(f"[ingest] Failed for {row.get('title', 'lead')}: {err}", file=sys.stderr)
        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)
    return ok, failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build city-based public-source lead lists and optionally ingest into Firestore."
    )
    parser.add_argument("--city", required=True, help="City name, for example Austin")
    parser.add_argument("--state", default="", help="State or province, for example TX")
    parser.add_argument("--country", default=DEFAULT_COUNTRY, help="Country name, default USA")
    parser.add_argument(
        "--categories",
        default="dentist,plumber,hvac",
        help=f"Comma-separated categories. Available: {', '.join(sorted(CATEGORY_PRESETS.keys()))}",
    )
    parser.add_argument("--max-results-per-category", type=int, default=40)
    parser.add_argument("--directory-label", default="OpenStreetMap")
    parser.add_argument("--contact-email", default="", help="Optional contact email for Nominatim courtesy.")
    parser.add_argument("--overpass-timeout", type=int, default=45)
    parser.add_argument("--throttle-ms", type=int, default=900)
    parser.add_argument("--output-csv", default="", help="Output CSV path. Defaults to exports/<city>-<date>.csv")
    parser.add_argument("--ingest-firestore", action="store_true")
    parser.add_argument("--firebase-project-id", default=os.getenv("BRICKBRICK_FIREBASE_PROJECT_ID", ""))
    parser.add_argument("--firebase-api-key", default=os.getenv("BRICKBRICK_FIREBASE_API_KEY", ""))
    parser.add_argument("--firebase-email", default=os.getenv("BRICKBRICK_FIREBASE_EMAIL", ""))
    parser.add_argument("--firebase-password", default=os.getenv("BRICKBRICK_FIREBASE_PASSWORD", ""))
    return parser.parse_args()


def resolve_categories(raw: str) -> List[str]:
    wanted = [normalize_slug(item) for item in raw.split(",") if item.strip()]
    unique: List[str] = []
    for item in wanted:
        if item in CATEGORY_PRESETS and item not in unique:
            unique.append(item)
    if not unique:
        raise ValueError(
            f"No valid categories selected. Choose from: {', '.join(sorted(CATEGORY_PRESETS.keys()))}"
        )
    return unique


def default_output_path(city: str, state: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    city_slug = normalize_slug("-".join([city, state]).strip("-")) or "city"
    return Path("exports") / f"lead-research-{city_slug}-{stamp}.csv"


def main() -> int:
    args = parse_args()
    try:
        categories = resolve_categories(args.categories)
    except ValueError as err:
        print(str(err), file=sys.stderr)
        return 2

    try:
        rows = run_public_research(
            city=args.city.strip(),
            state=args.state.strip(),
            country=args.country.strip(),
            categories=categories,
            per_category_limit=max(1, int(args.max_results_per_category)),
            contact_email=args.contact_email.strip(),
            directory_label=args.directory_label.strip() or "OpenStreetMap",
            overpass_timeout=max(10, int(args.overpass_timeout)),
            throttle_ms=max(0, int(args.throttle_ms)),
        )
    except Exception as err:
        print(f"Lead research failed: {err}", file=sys.stderr)
        return 1

    if not rows:
        print("No public listings found for those filters.")
        return 0

    out_path = Path(args.output_csv) if args.output_csv else default_output_path(args.city, args.state)
    write_csv(out_path, rows)
    print(f"Wrote {len(rows)} records to {out_path}")

    if not args.ingest_firestore:
        print("Firestore ingest skipped (use --ingest-firestore to push into lead_research_imports).")
        return 0

    required = {
        "firebase_project_id": args.firebase_project_id.strip(),
        "firebase_api_key": args.firebase_api_key.strip(),
        "firebase_email": args.firebase_email.strip(),
        "firebase_password": args.firebase_password,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(
            "Missing Firestore ingest settings: "
            + ", ".join(missing)
            + ". Provide flags or BRICKBRICK_FIREBASE_* env vars.",
            file=sys.stderr,
        )
        return 2

    ok, failed = ingest_rows_to_firestore(
        rows,
        project_id=required["firebase_project_id"],
        api_key=required["firebase_api_key"],
        email=required["firebase_email"],
        password=required["firebase_password"],
        source_file=out_path.name,
        throttle_ms=max(0, int(args.throttle_ms)),
    )
    print(f"Ingested staged leads: success={ok}, failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
