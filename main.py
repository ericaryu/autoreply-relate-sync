# -*- coding: utf-8 -*-
"""
autoreply-relate-sync
- Google Sheets '자동회신' 탭에서 미처리 행을 읽어
  Relate에 Organization upsert + Contact upsert + List entry upsert
- 실행: python main.py
"""

import json
import os
import re

import gspread
import requests
from google.oauth2.service_account import Credentials

# --- 설정 ---
SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID", "1jLTdRD_31u_V9EbRsBPJKGVf6mLzpIlr_iLwc8JykuE"
)
SHEET_TAB = "자동회신"
RELATE_CONTACT_LIST_ID = "9OUvxB"  # 반드시 Contact 타입 리스트 ID 여야 함
RELATE_BASE_URL = "https://api.relate.so/v1"

# 열 인덱스 (0-based)
COL_EMAIL_SINGLE = 3  # D열: 이메일 (단일)
COL_EMAIL_MULTI = 6  # G열: 이메일 (복수, 파싱 필요)
COL_DATE = 9  # J열: 수신일
COL_STATUS = 13  # N열: 등록여부

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ── Google Sheets ─────────────────────────────────────────────
def get_gspread_client() -> gspread.Client:
    json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not json_str:
        raise EnvironmentError(
            "환경변수 GOOGLE_SERVICE_ACCOUNT_JSON 이 설정되지 않았습니다."
        )
    creds = Credentials.from_service_account_info(json.loads(json_str), scopes=SCOPES)
    return gspread.authorize(creds)


# ── Relate 공통 ───────────────────────────────────────────────
def rh(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def parse_emails(text: str) -> list[str]:
    """텍스트에서 이메일 주소를 모두 추출해 소문자 리스트로 반환."""
    if not text:
        return []
    return [
        e.lower()
        for e in re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    ]


INVALID_LOCAL_PARTS = {"no-reply", "noreply", "no_reply", "wordpress"}


def is_invalid_email(email: str) -> bool:
    local = email.lower().split("@")[0]
    return any(kw in local for kw in INVALID_LOCAL_PARTS)


def extract_domain(email: str) -> str | None:
    if "@" in email:
        return email.split("@", 1)[1].lower()
    return None


# ── 초기화: 수신일 커스텀 필드 확보 ──────────────────────────
def ensure_date_custom_field(api_key: str) -> None:
    r = requests.get(
        f"{RELATE_BASE_URL}/custom_fields", headers=rh(api_key), timeout=15
    )
    r.raise_for_status()
    existing = {f["name"] for f in r.json()["data"] if f.get("model") == "contact"}
    if "수신일" not in existing:
        r2 = requests.post(
            f"{RELATE_BASE_URL}/custom_fields",
            headers=rh(api_key),
            json={"name": "수신일", "model": "contact", "data_type": "text"},
            timeout=15,
        )
        status = "생성" if r2.ok else f"실패({r2.status_code})"
        print(f"  [Contact 커스텀필드 {status}] 수신일")
    else:
        print("  [Contact 커스텀필드] 수신일 — 이미 존재")


def validate_list_is_contact_type(api_key: str) -> None:
    """RELATE_CONTACT_LIST_ID 가 Contact 타입인지 검증. 아니면 즉시 종료."""
    r = requests.get(
        f"{RELATE_BASE_URL}/lists/{RELATE_CONTACT_LIST_ID}",
        headers=rh(api_key),
        timeout=15,
    )
    r.raise_for_status()
    entry_type = str(r.json().get("entry_type") or "").strip()
    if entry_type != "Contact":
        raise SystemExit(
            f"[오류] RELATE_CONTACT_LIST_ID={RELATE_CONTACT_LIST_ID} 의 "
            f"entry_type={entry_type!r} 입니다. "
            "Contact 타입 리스트 ID로 교체하세요."
        )


# ── 기존 데이터 로드 ──────────────────────────────────────────
def build_org_map_by_domain(api_key: str) -> dict[str, str]:
    """{domain: org_id} 맵 구성."""
    h = rh(api_key)
    orgs: list[dict] = []
    after = 0
    while True:
        r = requests.get(
            f"{RELATE_BASE_URL}/organizations",
            headers=h,
            params={"first": 100, "after": after},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        orgs.extend(data.get("data", []))
        if not data.get("pagination", {}).get("has_next_page"):
            break
        after = data["pagination"]["end_cursor"]

    out: dict[str, str] = {}
    for o in orgs:
        oid = str(o.get("id") or "").strip()
        for d in o.get("domains", []):
            domain = (
                str(d if isinstance(d, str) else d.get("domain", "")).strip().lower()
            )
            if domain and oid:
                out[domain] = oid
    return out


def build_contact_map_by_email(api_key: str) -> dict[str, str]:
    """{email(lower): contact_id} 맵 구성."""
    h = rh(api_key)
    contacts: list[dict] = []
    after = 0
    while True:
        r = requests.get(
            f"{RELATE_BASE_URL}/contacts",
            headers=h,
            params={"first": 100, "after": after},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        contacts.extend(data.get("data", []))
        if not data.get("pagination", {}).get("has_next_page"):
            break
        after = data["pagination"]["end_cursor"]

    out: dict[str, str] = {}
    for c in contacts:
        cid = str(c.get("id") or "").strip()
        for e in c.get("emails", []):
            em = e if isinstance(e, str) else e.get("email", "")
            em = str(em or "").strip().lower()
            if em and cid and em not in out:
                out[em] = cid
    return out


def build_list_entry_map(api_key: str) -> dict[str, str]:
    """{entryable_id: entry_id} 맵 구성."""
    h = rh(api_key)
    entries: list[dict] = []
    after = 0
    while True:
        r = requests.get(
            f"{RELATE_BASE_URL}/lists/{RELATE_CONTACT_LIST_ID}/entries",
            headers=h,
            params={"first": 100, "after": after},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        entries.extend(data.get("data", []))
        if not data.get("pagination", {}).get("has_next_page"):
            break
        after = data["pagination"]["end_cursor"]

    return {
        str(e.get("entryable_id") or "").strip(): str(e.get("id") or "").strip()
        for e in entries
        if e.get("entryable_id") and e.get("id")
    }


# ── contact 검색/패치 헬퍼 ────────────────────────────────────
def _extract_contact_id_by_email(contacts: list, email: str) -> str:
    """contacts 리스트에서 이메일 일치하는 contact_id 반환."""
    for c in contacts:
        cid = str(c.get("id") or "").strip()
        for e in c.get("emails", []):
            em = e if isinstance(e, str) else e.get("email", "")
            if str(em or "").strip().lower() == email.lower() and cid:
                return cid
    return ""


def _find_contact_in_org(h: dict, org_id: str, email: str) -> str:
    """org 내 contacts에서 이메일로 contact_id 검색."""
    r = requests.get(
        f"{RELATE_BASE_URL}/organizations/{org_id}/contacts", headers=h, timeout=15
    )
    if r.ok:
        return _extract_contact_id_by_email(r.json().get("data", []), email)
    return ""


def _find_contact_globally(h: dict, email: str) -> str:
    """전체 contacts를 페이징하여 이메일로 contact_id 검색 (필터 API 없음)."""
    after = 0
    while True:
        r = requests.get(
            f"{RELATE_BASE_URL}/contacts",
            headers=h,
            params={"first": 100, "after": after},
            timeout=20,
        )
        if not r.ok:
            break
        data = r.json()
        cid = _extract_contact_id_by_email(data.get("data", []), email)
        if cid:
            return cid
        if not data.get("pagination", {}).get("has_next_page"):
            break
        after = data["pagination"]["end_cursor"]
    return ""


def _patch_contact(
    h: dict, contact_id: str, email: str, custom_fields: list, org_id: str = ""
) -> None:
    """contact PATCH 업데이트."""
    payload: dict = {"emails": [email]}
    if org_id:
        payload["organization_id"] = org_id
    if custom_fields:
        payload["custom_fields"] = custom_fields
    requests.patch(
        f"{RELATE_BASE_URL}/contacts/{contact_id}", headers=h, json=payload, timeout=30
    )


# ── upsert 함수들 ─────────────────────────────────────────────
def upsert_organization(api_key: str, domain: str, org_map: dict) -> tuple[str, str]:
    """도메인 기준 Organization upsert. (org_id, action) 반환."""
    existing_id = org_map.get(domain)
    if existing_id:
        return existing_id, "existing"

    r = requests.post(
        f"{RELATE_BASE_URL}/organizations",
        headers=rh(api_key),
        json={"name": domain, "domains": [domain]},
        timeout=30,
    )
    if r.ok:
        org_id = str(r.json().get("id") or "").strip()
        org_map[domain] = org_id
        return org_id, "created"

    # 422 - 이미 존재하는 도메인 → 전체 org 재조회해서 ID 확보
    if r.status_code == 422 and "same organization domain" in r.text.lower():
        fresh = build_org_map_by_domain(api_key)
        org_map.update(fresh)
        if domain in org_map:
            return org_map[domain], "existing"
        # 재조회에서도 못 찾으면 → "auto"로 Contact 생성 시 Relate이 직접 매핑
        print(f"    [경고] Org 재조회에서 {domain} 미발견, auto 매핑으로 진행")
        return "auto", "auto"

    r.raise_for_status()
    return "", "created"


def upsert_contact(
    api_key: str, org_id: str, email: str, date_str: str, contact_map: dict
) -> tuple[str, str]:
    """이메일 기준 Contact upsert. (contact_id, action) 반환."""
    h = rh(api_key)
    existing_id = contact_map.get(email.lower())

    custom_fields = [{"name": "수신일", "value": date_str}] if date_str else []

    if existing_id:
        _patch_contact(h, existing_id, email, custom_fields, org_id=org_id)
        return existing_id, "updated"

    payload = {"organization_id": org_id if org_id else "auto", "emails": [email]}
    if custom_fields:
        payload["custom_fields"] = custom_fields
    r = requests.post(
        f"{RELATE_BASE_URL}/contacts", headers=h, json=payload, timeout=30
    )
    if r.ok:
        contact_id = str(r.json().get("id") or "").strip()
        contact_map[email.lower()] = contact_id
        return contact_id, "created"

    # 422 + already taken → 이메일로 contact 검색 후 PATCH 업데이트
    if r.status_code == 422 and "has already been taken" in r.text:
        # 1차: 해당 org 내에서 검색
        cid = _find_contact_in_org(h, org_id, email)
        if cid:
            contact_map[email.lower()] = cid
            _patch_contact(h, cid, email, custom_fields)
            return cid, "updated"

        # 2차: API에 이메일 필터가 없으므로 전체 contacts 페이징하여 검색
        print(f"    [경고] org 내 검색 실패, 전체 contacts에서 {email} 검색 중...")
        cid = _find_contact_globally(h, email)
        if cid:
            contact_map[email.lower()] = cid
            _patch_contact(h, cid, email, custom_fields, org_id=org_id)
            return cid, "updated"

    r.raise_for_status()
    return "", "created"


def upsert_list_entry(
    api_key: str, contact_id: str, entryable_type: str, entry_map: dict
) -> str:
    """Contact List entry upsert. action 반환."""
    existing_id = entry_map.get(contact_id)
    if existing_id:
        return "existing"

    r = requests.post(
        f"{RELATE_BASE_URL}/lists/{RELATE_CONTACT_LIST_ID}/entries",
        headers=rh(api_key),
        json={"entryable_id": contact_id, "entryable_type": entryable_type},
        timeout=30,
    )
    r.raise_for_status()
    entry_id = str(r.json().get("id") or "").strip()
    entry_map[contact_id] = entry_id
    return "created"


# ── 메인 ──────────────────────────────────────────────────────
def main() -> None:
    api_key = os.environ.get("RELATE_API_KEY")
    if not api_key:
        raise EnvironmentError("환경변수 RELATE_API_KEY 가 설정되지 않았습니다.")

    print("=== 초기화 ===")
    ensure_date_custom_field(api_key)
    validate_list_is_contact_type(api_key)
    print(f"  List({RELATE_CONTACT_LIST_ID}) entry_type 검증 OK: Contact")

    print("기존 데이터 로딩 중...")
    org_map = build_org_map_by_domain(api_key)
    print(f"  Organizations(domain): {len(org_map)}건")
    contact_map = build_contact_map_by_email(api_key)
    print(f"  Contacts(email): {len(contact_map)}건")
    entry_map = build_list_entry_map(api_key)
    print(f"  List entries: {len(entry_map)}건")

    print("스프레드시트 로딩 중...")
    client = get_gspread_client()
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TAB)
    all_values = ws.get_all_values()

    if len(all_values) < 2:
        print("데이터 없음.")
        return

    success_count = fail_count = skip_count = 0
    # 시트 쓰기는 배치로 모아서 처리 (429 방지)
    pending_updates: dict[int, str] = {}

    def flush_updates() -> None:
        if not pending_updates:
            return
        ws.batch_update(
            [
                {"range": f"N{row}", "values": [[val]]}
                for row, val in pending_updates.items()
            ]
        )
        pending_updates.clear()

    print(f"\n=== 처리 시작: 총 {len(all_values) - 1}행 ===\n")

    for i, row in enumerate(all_values[1:], start=2):
        # N열 상태 확인 — 값 있으면 스킵
        status_val = row[COL_STATUS].strip() if len(row) > COL_STATUS else ""
        if status_val:
            skip_count += 1
            continue

        # D열 (단일), G열 (복수) 이메일 수집
        email_d = row[COL_EMAIL_SINGLE].strip() if len(row) > COL_EMAIL_SINGLE else ""
        email_g_raw = row[COL_EMAIL_MULTI].strip() if len(row) > COL_EMAIL_MULTI else ""
        date_val = row[COL_DATE].strip() if len(row) > COL_DATE else ""

        emails: list[str] = []
        if email_d:
            parsed_d = parse_emails(email_d)
            emails.extend(parsed_d if parsed_d else [email_d.lower()])
        emails.extend(parse_emails(email_g_raw))
        emails = list(dict.fromkeys(emails))  # 순서 유지 중복 제거

        if not emails:
            pending_updates[i] = "이메일 없음"
            fail_count += 1
            continue

        valid_emails = [e for e in emails if not is_invalid_email(e)]

        if not valid_emails:
            pending_updates[i] = "부적합"
            skip_count += 1
            continue

        row_ok = True
        error_msg = ""

        for email in valid_emails:
            domain = extract_domain(email)
            if not domain:
                continue

            try:
                org_id, org_action = upsert_organization(api_key, domain, org_map)
                print(f"  [행 {i}] Org {org_action}: {domain} ({org_id})")
            except requests.HTTPError as e:
                error_msg = (
                    f"Org 실패: {e.response.status_code} {e.response.text[:120]}"
                )
                print(f"  [행 {i}] FAIL — {error_msg}")
                row_ok = False
                break
            except Exception as e:
                error_msg = f"Org 오류: {e}"
                print(f"  [행 {i}] FAIL — {error_msg}")
                row_ok = False
                break

            try:
                contact_id, contact_action = upsert_contact(
                    api_key, org_id, email, date_val, contact_map
                )
                print(f"  [행 {i}] Contact {contact_action}: {email} ({contact_id})")
            except requests.HTTPError as e:
                error_msg = (
                    f"Contact 실패: {e.response.status_code} {e.response.text[:120]}"
                )
                print(f"  [행 {i}] FAIL — {error_msg}")
                row_ok = False
                break
            except Exception as e:
                error_msg = f"Contact 오류: {e}"
                print(f"  [행 {i}] FAIL — {error_msg}")
                row_ok = False
                break

            try:
                entry_action = upsert_list_entry(
                    api_key, contact_id, "Contact", entry_map
                )
                print(f"  [행 {i}] List entry {entry_action}: {email}")
            except requests.HTTPError as e:
                error_msg = (
                    f"List 실패: {e.response.status_code} {e.response.text[:120]}"
                )
                print(f"  [행 {i}] FAIL — {error_msg}")
                row_ok = False
                break
            except Exception as e:
                error_msg = f"List 오류: {e}"
                print(f"  [행 {i}] FAIL — {error_msg}")
                row_ok = False
                break

        if row_ok:
            pending_updates[i] = "done"
            success_count += 1
        else:
            pending_updates[i] = error_msg[:100]
            fail_count += 1

        # 50행마다 중간 flush (크래시 시 진행분 보존)
        if len(pending_updates) >= 50:
            flush_updates()

    flush_updates()
    print(
        f"\n=== 완료: 성공 {success_count}건 / 실패 {fail_count}건 / 스킵 {skip_count}건 ==="
    )


if __name__ == "__main__":
    main()
