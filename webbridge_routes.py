# webbridge_routes.py — Second-half routes for webbridge.py.
# Contains: auth, user, suggest, job runner, porting, criteria and report endpoints.
# This module is imported at the bottom of webbridge.py after all shared state is defined.
# Circular import is safe because webbridge is already in sys.modules by the time this runs.

import os
import sys
import re
import json
import threading
import time
import uuid
import io
import hashlib
import logging
import heapq
import difflib
import secrets
from csv import DictWriter
from datetime import datetime
from functools import lru_cache, wraps
import requests
from cache_backend import cache_get, cache_set, SUGGEST_CACHE_TTL, LLM_CACHE_TTL
from flask import request, send_from_directory, jsonify, abort, Response, stream_with_context
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.security import generate_password_hash, check_password_hash

# ---------------------------------------------------------------------------
# __main__ / module-name fix — same pattern as webbridge_cv.py
if 'webbridge' not in sys.modules:
    _main = sys.modules.get('__main__')
    if _main is not None and os.path.basename(os.path.normpath(getattr(_main, '__file__', ''))) == 'webbridge.py':
        sys.modules['webbridge'] = _main
# ---------------------------------------------------------------------------

from webbridge import (
    app, logger, genai,
    BASE_DIR, OUTPUT_DIR, SEARCH_XLS_DIR, REPORT_TEMPLATES_DIR,
    BUCKET_COMPANIES, BUCKET_JOB_TITLES,
    SECTORS_INDEX,
    CSE_PAGE_SIZE, CSE_PAGE_DELAY,
    GOOGLE_CSE_API_KEY, GOOGLE_CSE_CX, SEARCH_RESULTS_TARGET,
    GEMINI_API_KEY, GEMINI_SUGGEST_MODEL,
    SINGAPORE_CONTEXT, SEARCH_RULES,
    CV_TRANSLATION_MAX_CHARS, LANG_DETECTION_SAMPLE_LENGTH, CV_ANALYSIS_MAX_CHARS,
    MAX_COMMENT_LENGTH, COMMENT_TRUNCATE_LENGTH,
    ASSESSMENT_EXCELLENT_THRESHOLD, ASSESSMENT_GOOD_THRESHOLD, ASSESSMENT_MODERATE_THRESHOLD,
    CITY_TO_COUNTRY_DATA,
    _CV_ANALYZE_SEMAPHORE, _SINGLE_FILE_MAX,
    _rate, _check_user_rate, _check_gp_rate_limit, _csrf_required, _require_admin, _require_session,
    _user_has_custom_providers,
    _is_pdf_bytes,
    _extract_json_object, _extract_confirmed_skills,
    translate_text_pipeline,
    _infer_region_from_country,
    _find_best_sector_match_for_text, _map_keyword_to_sector_label,
    _compute_search_target,
    _should_overwrite_existing, _ensure_rating_metadata_columns, _ensure_search_indexes,
    _persist_jskillset, _fetch_jskillset, _fetch_jskillset_from_process,
    _sync_login_jskillset_to_process, _sync_criteria_jskillset_to_process,
    _increment_cse_query_count, _increment_gemini_query_count, _load_rate_limits, _save_rate_limits,
    _make_flask_limit,
    _pg_connect, _ensure_admin_columns,
    dedupe,
    _normalize_seniority_single, _map_gemini_seniority_to_dropdown,
    _gemini_talent_pool_suggestion,
    _token_set, _build_sectors_token_index, _is_pharma_company, _sectors_allow_pharma,
    _nllb_available, nllb_translate,
    log_identity, log_infrastructure, log_financial, log_security, log_error, log_approval,
    read_all_logs,
    _APP_LOGGER_AVAILABLE,
    _load_search_provider_config,
    _load_llm_provider_config,
    _load_email_verif_config,
    _load_get_profiles_config,
)

# Thread-local flag set by unified_search_page when a per-user search key fails
# and the system falls back to admin config.  _job_runner reads this after each
# search to record fallback in the JOBS dict for the front-end.
_search_fallback_flag = threading.local()


class ProviderSearchError(Exception):
    """Raised when a selected API provider (ContactOut/Apollo/RocketReach) search
    fails or has no key configured.  The message contains the specific reason so
    it can be surfaced directly in job-status messages without a CSE fallback."""
    pass

@app.post("/login")
@_rate(_make_flask_limit("login"))
@_check_user_rate("login")
@_csrf_required
def login_account():
    data = request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    _ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if not (username and password):
        return jsonify({"error":"username and password required"}), 400

    try:
        import common_auth
        hash_password_fn = getattr(common_auth, "hash_password", None)
        verify_password_fn = getattr(common_auth, "verify_password", None)
    except Exception:
        hash_password_fn = None
        verify_password_fn = None

    try:
        import psycopg2
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")
        conn = _pg_connect()
        cur=conn.cursor()
        cur.execute("SELECT password, userid, cemail, fullname, role_tag, COALESCE(token,0) FROM login WHERE username=%s", (username,))
        row=cur.fetchone()
        cur.close(); conn.close()
        if not row:
            log_security("login_failed", username=username, ip_address=_ip,
                         detail="User not found", severity="warning")
            return jsonify({"error":"Invalid credentials"}), 401
        stored_pw, userid, cemail, fullname, role_tag, token_val = row
        stored_pw = stored_pw or ""

        if verify_password_fn:
            ok = False
            try:
                ok = bool(verify_password_fn(stored_pw, password))
            except Exception:
                ok = False
            if not ok:
                log_security("login_failed", username=username, ip_address=_ip,
                             detail="Password mismatch", severity="warning")
                return jsonify({"error":"Invalid credentials"}), 401
        else:
            def _local_hash_password(p: str) -> str:
                import hashlib
                salt = os.getenv("PASSWORD_SALT", "")
                return hashlib.sha256((salt + p).encode("utf-8")).hexdigest()
            hashed = hash_password_fn(password) if hash_password_fn else _local_hash_password(password)
            if stored_pw != hashed and stored_pw != password:
                log_security("login_failed", username=username, ip_address=_ip,
                             detail="Password mismatch", severity="warning")
                return jsonify({"error":"Invalid credentials"}), 401

        log_identity(userid=str(userid or ""), username=username,
                     ip_address=_ip, mfa_status="N/A")

        # Generate a cryptographic session_id and store it in the DB — same
        # pattern as server.js so either service can validate the session.
        new_session_id = secrets.token_hex(32)
        try:
            import psycopg2 as _pg2
            _sc = _pg2.connect(
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
                user=os.getenv("PGUSER", "postgres"),
                password=os.getenv("PGPASSWORD", ""),
                dbname=os.getenv("PGDATABASE", "candidate_db"),
            )
            _cc = _sc.cursor()
            _cc.execute("UPDATE login SET session_id = %s WHERE username = %s", (new_session_id, username))
            _sc.commit()
            _cc.close(); _sc.close()
        except Exception as _sess_err:
            logger.error("[login] Failed to store session_id: %s", _sess_err)

        resp = jsonify({"ok": True, "userid": userid or "", "username": username, "cemail": cemail or "", "fullname": fullname or "", "role_tag": role_tag or "", "token": int(token_val or 0)})
        _is_secure = os.getenv("FORCE_HTTPS", "0") == "1"
        # username and userid: httponly=False so AutoSourcing.html can read them
        # via document.cookie for UI display.
        _cookie_opts = dict(max_age=2592000, path="/", httponly=False, samesite="lax",
                            secure=_is_secure)
        resp.set_cookie("username", username, **_cookie_opts)
        resp.set_cookie("userid", str(userid or ""), **_cookie_opts)
        # session_id: httpOnly=True — not readable by JS, prevents forgery.
        resp.set_cookie("session_id", new_session_id,
                        max_age=2592000, path="/", httponly=True, samesite="lax",
                        secure=_is_secure)
        return resp, 200
    except Exception as e:
        log_error(source="login", message=str(e), severity="error",
                  username=username, endpoint="/login")
        return jsonify({"error": str(e)}), 500

@app.post("/logout")
@_csrf_required
def logout_account():
    # Invalidate the server-side session so the session_id can no longer be used.
    username = (request.cookies.get("username") or "").strip()
    if username:
        try:
            import psycopg2 as _pg2
            _sc = _pg2.connect(
                host=os.getenv("PGHOST", "localhost"),
                port=int(os.getenv("PGPORT", "5432")),
                user=os.getenv("PGUSER", "postgres"),
                password=os.getenv("PGPASSWORD", ""),
                dbname=os.getenv("PGDATABASE", "candidate_db"),
            )
            _cc = _sc.cursor()
            _cc.execute("UPDATE login SET session_id = NULL WHERE username = %s", (username,))
            _sc.commit()
            _cc.close(); _sc.close()
        except Exception as _sess_err:
            logger.error("[logout] Failed to clear session_id: %s", _sess_err)
    resp = jsonify({"ok": True})
    resp.delete_cookie("username", path="/")
    resp.delete_cookie("userid", path="/")
    resp.delete_cookie("session_id", path="/")
    return resp

@app.post("/register")
@_rate(_make_flask_limit("register"))
@_check_user_rate("register")
@_csrf_required
def register_account():
    data = request.get_json(force=True, silent=True) or {}

    fullname   = (data.get("fullname") or "").strip()
    corporation = (data.get("corporation") or "").strip()
    cemail     = (data.get("cemail") or "").strip()
    username   = (data.get("username") or "").strip()
    password   = data.get("password") or ""
    userid     = (data.get("userid") or "").strip()
    created_at = (data.get("created_at") or "").strip()

    if not (fullname and cemail and username and password):
        return jsonify({"error": "fullname, cemail, username, password are required"}), 400

    if not userid:
        userid = str(uuid.uuid4().int % 9000000 + 1000000)

    try:
        import common_auth
        hash_password_fn = getattr(common_auth, "hash_password", None)
    except Exception:
        hash_password_fn = None

    if hash_password_fn:
        try:
            hashed_pw = hash_password_fn(password)
        except Exception:
            hashed_pw = None
    else:
        hashed_pw = None

    if not hashed_pw:
        def _local_hash_password(p: str) -> str:
            import hashlib
            salt = os.getenv("PASSWORD_SALT", "")
            return hashlib.sha256((salt + p).encode("utf-8")).hexdigest()
        hashed_pw = _local_hash_password(password)

    try:
        import psycopg2
        from psycopg2 import sql as pgsql
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")

        conn = _pg_connect()
        cur=conn.cursor()

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='login'
        """)
        login_cols = {r[0].lower() for r in cur.fetchall()}

        if "username" in login_cols and "cemail" in login_cols:
            cur.execute("SELECT 1 FROM login WHERE username=%s OR cemail=%s LIMIT 1", (username, cemail))
            if cur.fetchone():
                cur.close(); conn.close()
                return jsonify({"error": "Username or email already registered"}), 409
        elif "username" in login_cols:
            cur.execute("SELECT 1 FROM login WHERE username=%s LIMIT 1", (username,))
            if cur.fetchone():
                cur.close(); conn.close()
                return jsonify({"error": "Username already registered"}), 409
        elif "cemail" in login_cols:
            cur.execute("SELECT 1 FROM login WHERE cemail=%s LIMIT 1", (cemail,))
            if cur.fetchone():
                cur.close(); conn.close()
                return jsonify({"error": "Email already registered"}), 409

        insert_cols = []
        insert_vals = []

        for col, val in [
            ("userid", userid),
            ("username", username),
            ("password", hashed_pw),
            ("fullname", fullname),
            ("cemail", cemail)
        ]:
            if col in login_cols:
                insert_cols.append(col)
                insert_vals.append(val)

        if "corporation" in login_cols and corporation:
            insert_cols.append("corporation"); insert_vals.append(corporation)
        if "created_at" in login_cols and created_at:
            insert_cols.append("created_at"); insert_vals.append(created_at)
        if "role_tag" in login_cols:
            insert_cols.append("role_tag"); insert_vals.append("")
        elif "roletag" in login_cols:
            insert_cols.append("roletag"); insert_vals.append("")
        if "token" in login_cols:
            insert_cols.append("token"); insert_vals.append(0)

        if not insert_cols:
            cur.close(); conn.close()
            return jsonify({"error": "No compatible columns found for registration"}), 500

        col_sql = pgsql.SQL(", ").join(pgsql.Identifier(c) for c in insert_cols)
        placeholders = pgsql.SQL(", ".join(["%s"] * len(insert_cols)))
        cur.execute(pgsql.SQL("INSERT INTO login ({}) VALUES ({})").format(col_sql, placeholders), insert_vals)
        conn.commit()
        cur.close(); conn.close()

        return jsonify({"ok": True, "message": "Registration successful", "username": username, "userid": userid}), 200
    except Exception as e:
        logger.error(f"[Register] {e}")
        return jsonify({"error": str(e)}), 500


# ── Sales-rep self-registration ──────────────────────────────────────────────
# Stores the profile in a dedicated `employee` table (created on first use).

def _safe_cookie_value(s: str) -> str:
    """Strip characters that are illegal in HTTP Set-Cookie values to prevent header injection."""
    import re as _re
    return _re.sub(r'[\x00-\x1f\x7f;,\\ "\'=]', '', str(s or ""))[:256]


def _ensure_employee_table(conn):
    """Create the employee table if it does not already exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employee (
            id                   SERIAL PRIMARY KEY,
            full_name            TEXT NOT NULL,
            username             TEXT UNIQUE NOT NULL,
            password             TEXT NOT NULL,
            nationality          TEXT,
            location             TEXT,
            skillsets            TEXT,
            industrial_vertical  TEXT,
            language_skills      TEXT,
            travel_availability  TEXT,
            commission           NUMERIC DEFAULT 0,
            ownership            INTEGER DEFAULT 0,
            created_at           TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Idempotently add commission/ownership to tables created before this migration.
    for ddl in [
        "ALTER TABLE employee ADD COLUMN IF NOT EXISTS commission NUMERIC DEFAULT 0",
        "ALTER TABLE employee ADD COLUMN IF NOT EXISTS ownership INTEGER DEFAULT 0",
    ]:
        try:
            cur.execute("SAVEPOINT sp_emp_col")
            cur.execute(ddl)
            cur.execute("RELEASE SAVEPOINT sp_emp_col")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_emp_col")
    conn.commit()
    cur.close()


@app.post("/employee/register")
@_csrf_required
def employee_register():
    data = request.get_json(force=True, silent=True) or {}

    full_name            = (data.get("full_name") or "").strip()
    username             = (data.get("username") or "").strip()
    password             = data.get("password") or ""
    nationality          = (data.get("nationality") or "").strip()
    location             = (data.get("location") or "").strip()
    skillsets            = (data.get("skillsets") or "").strip()
    industrial_vertical  = (data.get("industrial_vertical") or "").strip()
    language_skills      = (data.get("language_skills") or "").strip()
    travel_availability  = (data.get("travel_availability") or "").strip()

    if not (full_name and username and password and nationality and location
            and skillsets and industrial_vertical and language_skills and travel_availability):
        return jsonify({"error": "All fields are required."}), 400

    if len(password) < 8 or not (any(c.isalpha() for c in password) and any(c.isdigit() for c in password)):
        return jsonify({"error": "Password must be at least 8 characters and contain both letters and numbers."}), 400

    import re as _re
    if not _re.match(r'^[a-zA-Z0-9_\-\.]+$', username):
        return jsonify({"error": "Username may only contain letters, numbers, underscores, hyphens and dots."}), 400

    # Hash the password using the same mechanism as the main /register route
    try:
        import common_auth
        hash_password_fn = getattr(common_auth, "hash_password", None)
    except Exception:
        hash_password_fn = None

    if hash_password_fn:
        try:
            hashed_pw = hash_password_fn(password)
        except Exception:
            hashed_pw = None
    else:
        hashed_pw = None

    if not hashed_pw:
        hashed_pw = generate_password_hash(password)

    try:
        conn = _pg_connect()
        _ensure_employee_table(conn)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO employee
                   (full_name, username, password, nationality, location,
                    skillsets, industrial_vertical, language_skills, travel_availability)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING username, full_name""",
            (
                full_name[:100], username[:50], hashed_pw,
                nationality[:80], location[:100], skillsets[:1000],
                industrial_vertical[:200], language_skills[:200], travel_availability[:100],
            )
        )
        db_row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        # Use DB-returned values for cookies so the value originates from the database
        db_username  = db_row[0] if db_row else ""
        db_full_name = db_row[1] if db_row else ""
        _cookie_opts = dict(max_age=86400, path="/", httponly=False, samesite="lax",
                            secure=os.getenv("FORCE_HTTPS", "0") == "1")
        resp = jsonify({"ok": True, "message": "Sales rep registered successfully.", "username": db_username, "full_name": db_full_name})
        resp.set_cookie("emp_username", db_username, **_cookie_opts)
        resp.set_cookie("emp_full_name", db_full_name, **_cookie_opts)
        return resp, 201
    except Exception as e:
        if hasattr(e, 'pgcode') and e.pgcode == '23505':
            return jsonify({"error": "That username is already taken. Please choose another."}), 409
        logger.error(f"[employee_register] {e}")
        return jsonify({"error": "Registration failed due to an internal error. Please try again."}), 500


@app.post("/employee/login")
@_csrf_required
def employee_login():
    """Authenticate a sales rep against the employee table and set a session cookie."""
    data = request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not (username and password):
        return jsonify({"error": "Username and password are required."}), 400

    try:
        conn = _pg_connect()
        _ensure_employee_table(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT full_name, password, username FROM employee WHERE username = %s LIMIT 1",
            (username,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"[employee_login] DB error: {e}")
        return jsonify({"error": "Login failed due to an internal error. Please try again."}), 500

    if not row:
        return jsonify({"error": "Invalid username or password."}), 401

    full_name, stored_hash, db_username = row
    ok = False
    try:
        ok = check_password_hash(stored_hash or "", password)
    except Exception:
        pass
    # Fallback: check common_auth.verify_password if available
    if not ok:
        try:
            import common_auth
            verify_fn = getattr(common_auth, "verify_password", None)
            if verify_fn:
                ok = bool(verify_fn(stored_hash or "", password))
        except Exception:
            pass

    if not ok:
        return jsonify({"error": "Invalid username or password."}), 401

    _cookie_opts = dict(max_age=86400, path="/", httponly=False, samesite="lax",
                        secure=os.getenv("FORCE_HTTPS", "0") == "1")
    # Use DB-sourced username and full_name for cookies (not the raw user-supplied input)
    resp = jsonify({"ok": True, "username": db_username or "", "full_name": full_name or ""})
    resp.set_cookie("emp_username", db_username or "", **_cookie_opts)
    resp.set_cookie("emp_full_name", full_name or "", **_cookie_opts)
    return resp, 200


@app.post("/employee/logout")
@_csrf_required
def employee_logout():
    resp = jsonify({"ok": True})
    resp.delete_cookie("emp_username", path="/")
    resp.delete_cookie("emp_full_name", path="/")
    return resp, 200


@app.get("/employee/check-client")
def employee_check_client():
    """Check whether a corporation name already exists in the login table.
    Returns exact match, fuzzy/partial match, or no match."""
    emp_username = (request.cookies.get("emp_username") or "").strip()
    if not emp_username:
        return jsonify({"error": "Unauthorized"}), 401
    corp = request.args.get("corporation", "").strip()
    if not corp:
        return jsonify({"error": "corporation is required"}), 400
    try:
        conn = _pg_connect()
        with conn.cursor() as cur:
            # 1. Exact match (case-insensitive)
            cur.execute(
                "SELECT corporation FROM login WHERE LOWER(corporation) = LOWER(%s)"
                " AND corporation IS NOT NULL LIMIT 1",
                (corp,))
            row = cur.fetchone()
            if row:
                return jsonify({"exists": True, "match": row[0]})
            # 2. Partial match — query is contained in a corporation name
            cur.execute(
                "SELECT corporation FROM login WHERE LOWER(corporation) LIKE LOWER(%s)"
                " AND corporation IS NOT NULL ORDER BY corporation LIMIT 1",
                (f"%{corp}%",))
            row = cur.fetchone()
            if row:
                return jsonify({"exists": False, "fuzzy": True, "match": row[0]})
            # 3. Corporation name is contained in the query, or word-level match
            words = [w for w in corp.split() if len(w) >= 3]
            if words:
                conditions = " OR ".join(
                    "LOWER(corporation) LIKE LOWER(%s)" for _ in words)
                params = [f"%{w}%" for w in words]
                cur.execute(
                    f"SELECT corporation FROM login WHERE ({conditions})"
                    " AND corporation IS NOT NULL ORDER BY corporation LIMIT 1",
                    params)
                row = cur.fetchone()
                if row:
                    return jsonify({"exists": False, "fuzzy": True, "match": row[0]})
        return jsonify({"exists": False, "fuzzy": False})
    except Exception as e:
        logger.error(f"[employee_check_client] DB error: {e}")
        return jsonify({"error": "Server error"}), 500


@app.get("/employee/dashboard-data")
def employee_dashboard_data():
    """
    Return the sales rep's dashboard payload:
      - employee profile (full_name, username, …)
      - client list: corporations from login table where bd = emp_username
      - transaction logs: financial credit entries for each of those corporations' users
    """
    emp_username = (request.cookies.get("emp_username") or "").strip()
    if not emp_username:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        conn = _pg_connect()
        _ensure_employee_table(conn)
        cur = conn.cursor()

        # 1. Employee profile
        cur.execute(
            """SELECT full_name, username, nationality, location, skillsets,
                      industrial_vertical, language_skills, travel_availability,
                      COALESCE(commission, 0), COALESCE(ownership, 0)
               FROM employee WHERE username = %s LIMIT 1""",
            (emp_username,)
        )
        emp_row = cur.fetchone()
        if not emp_row:
            cur.close(); conn.close()
            return jsonify({"error": "Employee not found."}), 404

        employee = {
            "full_name":           emp_row[0] or "",
            "username":            emp_row[1] or "",
            "nationality":         emp_row[2] or "",
            "location":            emp_row[3] or "",
            "skillsets":           emp_row[4] or "",
            "industrial_vertical": emp_row[5] or "",
            "language_skills":     emp_row[6] or "",
            "travel_availability": emp_row[7] or "",
            "commission":          float(emp_row[8]),
            "ownership":           int(emp_row[9]),
        }

        # 2. Clients: rows in login table where bd = emp_username
        # All usernames assigned to this BD (used for transaction log filtering,
        # regardless of whether they have a corporation set)
        cur.execute(
            """SELECT DISTINCT username FROM login
               WHERE bd = %s AND username IS NOT NULL AND username != ''""",
            (emp_username,)
        )
        all_client_usernames: set = {r[0] for r in cur.fetchall()}

        # Clients with a named corporation (used for the Clients display table)
        cur.execute(
            """SELECT DISTINCT corporation, username
               FROM login
               WHERE bd = %s AND corporation IS NOT NULL AND corporation != ''
               ORDER BY corporation""",
            (emp_username,)
        )
        client_rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"[employee_dashboard_data] DB error: {e}")
        return jsonify({"error": "Failed to load dashboard data."}), 500

    # Build lookups for the log joining step
    # Map username -> corporation (for labelling log entries)
    username_to_corp: dict = {r[1]: r[0] for r in client_rows if r[1]}
    # Map corporation -> last_credited_date
    corp_credit_map: dict = {r[0]: {"last_credited_date": None} for r in client_rows if r[0]}

    # 3. Financial transaction logs — read the same files as /admin/logs
    transactions: list = []
    try:
        fin_logs = read_all_logs().get("financial", [])

        for entry in fin_logs:
            uname = entry.get("username") or ""
            if uname not in all_client_usernames:
                continue
            corp = username_to_corp.get(uname, "")
            ts = entry.get("timestamp") or ""
            txn_type = (entry.get("transaction_type") or "").lower()

            # Update last_credited_date for the corporation
            if txn_type == "credit" and corp in corp_credit_map:
                existing = corp_credit_map[corp]["last_credited_date"]
                if not existing or ts > existing:
                    corp_credit_map[corp]["last_credited_date"] = ts

            transactions.append({
                "timestamp":          ts,
                "username":           uname,
                "userid":             entry.get("userid") or "",
                "corporation":        corp,
                "transaction_type":   entry.get("transaction_type") or "",
                "transaction_amount": entry.get("transaction_amount"),
                "token_before":       entry.get("token_before"),
                "token_after":        entry.get("token_after"),
                "token_cost_sgd":     entry.get("token_cost_sgd"),
                "revenue_sgd":        entry.get("revenue_sgd"),
                "credits_spent":      entry.get("credits_spent"),
                "token_usage":        entry.get("token_usage"),
                "feature":            entry.get("feature") or "",
            })

        # Sort transactions newest-first
        transactions.sort(key=lambda x: x["timestamp"] or "", reverse=True)
    except Exception as log_err:
        logger.warning(f"[employee_dashboard_data] log read error: {log_err}")

    # 4. Assemble client list with last_credited_date
    clients = [
        {
            "corporation":       r[0],
            "username":          r[1],
            "last_credited_date": corp_credit_map.get(r[0], {}).get("last_credited_date"),
        }
        for r in client_rows if r[0]
    ]

    return jsonify({
        "employee":     employee,
        "clients":      clients,
        "transactions": transactions,
    }), 200


@app.get("/sales_rep_dashboard.html")
def sales_rep_dashboard_html():
    return send_from_directory(BASE_DIR, "sales_rep_dashboard.html")


@app.get("/admin/sales-rep")
@_require_admin
def admin_sales_rep():
    """
    Return one aggregated row per sales rep (bd) with:
      full_name (from employee table), username (bd value),
      total_clients (distinct corporations), tokens_credited (sum of credit txns),
      total_revenue (SGD from spend txns).
    """
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        _ensure_admin_columns(cur)
        conn.commit()
        _ensure_employee_table(conn)

        # All login rows that have a bd value set.
        cur.execute(
            """SELECT username, COALESCE(corporation, '') AS corporation,
                      COALESCE(bd, '') AS bd
               FROM login
               WHERE bd IS NOT NULL AND bd != ''"""
        )
        login_rows = cur.fetchall()

        # Employee full_name, commission, ownership by username.
        cur.execute("SELECT username, full_name, COALESCE(commission,0), COALESCE(ownership,0) FROM employee")
        emp_info = {r[0]: {"full_name": r[1] or r[0], "commission": float(r[2]), "ownership": int(r[3])}
                    for r in cur.fetchall()}

        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"[admin_sales_rep] DB error: {e}")
        return jsonify({"error": "Failed to query database."}), 500

    # Build per-bd structures.
    bd_corp_sets: dict = {}       # bd -> set of corporation names
    username_to_info: dict = {}   # login username -> { bd, corporation }
    for username, corporation, bd in login_rows:
        if not bd:
            continue
        bd_corp_sets.setdefault(bd, set())
        if corporation:
            bd_corp_sets[bd].add(corporation)
        username_to_info[username] = {"bd": bd, "corporation": corporation}

    bd_acc: dict = {bd: {"tokens_credited": 0, "total_revenue": 0.0, "total_tokens_consumed": 0}
                    for bd in bd_corp_sets}

    # Aggregate from financial logs.
    try:
        fin_logs = read_all_logs().get("financial", [])
        for entry in fin_logs:
            uname = entry.get("username") or ""
            info = username_to_info.get(uname)
            if not info:
                continue
            bd = info["bd"]
            if bd not in bd_acc:
                continue
            txn_type = (entry.get("transaction_type") or "").lower()
            amt = float(entry.get("transaction_amount") or 0)
            if txn_type == "credit":
                bd_acc[bd]["tokens_credited"] += amt
            elif txn_type == "spend":
                bd_acc[bd]["total_tokens_consumed"] += abs(amt)
                rev = float(entry.get("revenue_sgd") or 0)
                if rev > 0:
                    bd_acc[bd]["total_revenue"] += rev
                else:
                    cost = float(entry.get("token_cost_sgd") or 0.10)
                    bd_acc[bd]["total_revenue"] += abs(amt) * cost
    except Exception as log_err:
        logger.warning(f"[admin_sales_rep] log read error: {log_err}")

    result = [
        {
            "full_name":              emp_info.get(bd, {}).get("full_name", bd),
            "username":               bd,
            "total_clients":          len(bd_corp_sets[bd]),
            "tokens_credited":        round(bd_acc[bd]["tokens_credited"]),
            "total_tokens_consumed":  round(bd_acc[bd]["total_tokens_consumed"]),
            "total_revenue":          round(bd_acc[bd]["total_revenue"], 2),
            "commission":             emp_info.get(bd, {}).get("commission", 0),
            "ownership":              emp_info.get(bd, {}).get("ownership", 0),
        }
        for bd in bd_corp_sets
    ]
    result.sort(key=lambda x: (x["full_name"] or "").lower())

    return jsonify({"sales_rep": result}), 200


@app.patch("/admin/sales-rep/<username>")
@_require_admin
def admin_sales_rep_update(username):
    """Update commission rate and ownership period for a sales rep in the employee table."""
    body = request.get_json(silent=True) or {}
    commission = body.get("commission")
    ownership  = body.get("ownership")
    if commission is None and ownership is None:
        return jsonify({"error": "No fields to update."}), 400
    try:
        commission = float(commission) if commission is not None else None
        ownership  = int(ownership)   if ownership  is not None else None
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid commission or ownership value."}), 400
    try:
        conn = _pg_connect()
        cur  = conn.cursor()
        _ensure_employee_table(conn)
        if commission is not None and ownership is not None:
            cur.execute(
                "UPDATE employee SET commission=%s, ownership=%s WHERE username=%s",
                (commission, ownership, username)
            )
        elif commission is not None:
            cur.execute(
                "UPDATE employee SET commission=%s WHERE username=%s",
                (commission, username)
            )
        else:
            cur.execute(
                "UPDATE employee SET ownership=%s WHERE username=%s",
                (ownership, username)
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"[admin_sales_rep_update] DB error: {e}")
        return jsonify({"error": "Failed to update."}), 500
    return jsonify({"ok": True}), 200


@app.get("/admin/sales-rep/<username>/transactions")
@_require_admin
def admin_sales_rep_transactions(username):
    """Return all financial transaction log entries for a given BD (sales rep) username.
    Accepts optional `from` and `to` query-string params (YYYY-MM-DD) for date filtering.
    """
    date_from = (request.args.get("from") or "").strip()
    date_to   = (request.args.get("to")   or "").strip()

    try:
        conn = _pg_connect()
        cur  = conn.cursor()
        # All login-table usernames assigned to this BD
        cur.execute(
            """SELECT DISTINCT username FROM login
               WHERE bd = %s AND username IS NOT NULL AND username != ''""",
            (username,)
        )
        all_client_usernames: set = {r[0] for r in cur.fetchall()}
        # Map username -> corporation for log-entry labelling
        cur.execute(
            """SELECT DISTINCT username, COALESCE(corporation, '') FROM login
               WHERE bd = %s AND username IS NOT NULL AND username != ''""",
            (username,)
        )
        username_to_corp: dict = {r[0]: r[1] for r in cur.fetchall()}
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"[admin_sales_rep_transactions] DB error: {e}")
        return jsonify({"error": "Failed to query database."}), 500

    transactions: list = []
    try:
        fin_logs = read_all_logs().get("financial", [])
        for entry in fin_logs:
            uname = entry.get("username") or ""
            if uname not in all_client_usernames:
                continue
            ts = entry.get("timestamp") or ""
            # Date filter
            if date_from and ts[:10] < date_from:
                continue
            if date_to and ts[:10] > date_to:
                continue
            transactions.append({
                "timestamp":          ts,
                "username":           uname,
                "userid":             entry.get("userid") or "",
                "corporation":        username_to_corp.get(uname, "") or entry.get("corporation") or "",
                "transaction_type":   entry.get("transaction_type") or "",
                "transaction_amount": entry.get("transaction_amount"),
                "token_before":       entry.get("token_before"),
                "token_after":        entry.get("token_after"),
                "token_cost_sgd":     entry.get("token_cost_sgd"),
                "revenue_sgd":        entry.get("revenue_sgd"),
                "credits_spent":      entry.get("credits_spent"),
                "token_usage":        entry.get("token_usage"),
                "feature":            entry.get("feature") or "",
            })
        transactions.sort(key=lambda x: x["timestamp"] or "", reverse=True)
    except Exception as log_err:
        logger.warning(f"[admin_sales_rep_transactions] log read error: {log_err}")

    return jsonify({"transactions": transactions}), 200


@app.get("/token-config")
def token_config():
    """Return the token credit/deduction configuration from rate_limits.json.
    Used by AutoSourcing.html and SourcingVerify.html (Flask-served pages) to
    read dynamic token rates without reaching across to the Node.js server.
    Requires a valid username cookie so the endpoint is not publicly enumerable.
    """
    username = (request.cookies.get("username") or "").strip()
    if not username:
        return jsonify({"error": "Unauthorized"}), 401
    try:
        cfg = _load_rate_limits()
        t = cfg.get("tokens", {})
        s = cfg.get("system", {})
        return jsonify({
            "appeal_approve_credit":     t.get("appeal_approve_credit",     1),
            "verified_selection_deduct": t.get("verified_selection_deduct", 2),
            "rebate_credit_per_profile": t.get("rebate_credit_per_profile", 1),
            "analytic_token_cost":       t.get("analytic_token_cost",       1),
            "initial_token_display":     t.get("initial_token_display",     5000),
            "sourcing_rate_base":        t.get("sourcing_rate_base",        1),
            "sourcing_rate_best_mode":   t.get("sourcing_rate_best_mode",   1.5),
            "sourcing_rate_over50":      t.get("sourcing_rate_over50",      2),
            "sourcing_rate_best_over50": t.get("sourcing_rate_best_over50", 2.5),
            "jd_upload_max_count":       s.get("jd_upload_max_count",       5),
            "jd_upload_max_bytes":       s.get("jd_upload_max_bytes",       6291456),
            "jd_analysis_token_cost":    t.get("jd_analysis_token_cost",    1),
            "token_cost_sgd":            t.get("token_cost_sgd",            0.10),
        }), 200
    except Exception as e:
        logger.error(f"[token-config] {e}")
        return jsonify({"error": "Failed to load token config"}), 500


@app.get("/user/resolve")
def user_resolve():
    username = (request.args.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username required"}), 400
    try:
        import psycopg2
        pg_host=os.getenv("PGHOST","localhost")
        pg_port=int(os.getenv("PGPORT","5432"))
        pg_user=os.getenv("PGUSER","postgres")
        pg_password=os.getenv("PGPASSWORD", "")
        pg_db=os.getenv("PGDATABASE","candidate_db")
        conn = _pg_connect()
        cur=conn.cursor()
        cur.execute("SELECT userid, fullname, role_tag, COALESCE(token,0), COALESCE(target_limit,10), COALESCE(useraccess,'') FROM login WHERE username=%s", (username,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({"error":"not found"}), 404
        userid, fullname, login_role_tag, token_val, target_limit_val, useraccess_val = row
        # Use login.role_tag as the authoritative current session role for the recruiter.
        # sourcing.role_tag is per-candidate (for matching) and must not override the recruiter's
        # current active role — old sourcing records from previous searches would cause the session
        # badge to show a stale role even after the recruiter has started a new search.
        resolved_role_tag = login_role_tag or ""
        cur.close(); conn.close()
        return jsonify({"userid": userid or "", "fullname": fullname or "", "role_tag": resolved_role_tag, "token": int(token_val or 0), "target_limit": int(target_limit_val or 10), "useraccess": (useraccess_val or "").strip()}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Module-level flag: ALTER TABLE to add last_result_count runs at most once per
# server process so the idempotency guard column is created without per-request DDL.
_token_guard_column_ensured = False

# Module-level flag: ALTER TABLE to add role_tag_session column runs at most once
# per server process for both login and sourcing tables.
_role_tag_session_column_ensured = False

@app.post("/user/token_update")
@_csrf_required
@_require_session
def user_token_update():
    """
    POST /user/token_update
    Sets the token column in the login table to the supplied value.
    Used to persist the current "tokens left" figure after each
    token-consuming operation so the login table always reflects the
    most up-to-date balance.

    Body JSON: { "userid": "<id>", "token": <number>, "result_count": <int|optional> }

    When result_count is supplied the endpoint acts as an idempotent guard:
    the update only fires if result_count differs from the stored
    last_result_count, preventing the feedback loop where the same search
    result count is deducted repeatedly on every page refresh or new-tab load.
    """
    global _token_guard_column_ensured, _role_tag_session_column_ensured
    data = request.get_json(force=True, silent=True) or {}
    userid = (data.get("userid") or "").strip()
    token_val = data.get("token")
    delta_val = data.get("delta")
    # Optional caller-supplied feature tag for granular transaction logging.
    # Falls back to "token_update" when not provided for backward compatibility.
    caller_feature = (data.get("feature") or "").strip() or "token_update"
    # Snapshot the current per-token SGD cost for audit logging.
    _rl_cfg = _load_rate_limits()
    _token_cost_sgd = float((_rl_cfg.get("tokens") or {}).get("token_cost_sgd", 0.10))

    # Delta mode: increment/decrement by a relative amount (used by rebate flow to restore +1 token).
    if delta_val is not None and token_val is None:
        try:
            delta_int = int(delta_val)
        except (TypeError, ValueError):
            return jsonify({"error": "delta must be a number"}), 400
        if not userid:
            return jsonify({"error": "userid is required"}), 400
        try:
            import psycopg2
            pg_host = os.getenv("PGHOST", "localhost")
            pg_port = int(os.getenv("PGPORT", "5432"))
            pg_user = os.getenv("PGUSER", "postgres")
            pg_password = os.getenv("PGPASSWORD", "")
            pg_db = os.getenv("PGDATABASE", "candidate_db")
            conn = _pg_connect()
            cur = conn.cursor()
            # Skip token deduction (negative delta) for BYOK users
            if delta_int < 0:
                cur.execute(
                    "SELECT COALESCE(token, 0) AS t, useraccess FROM login WHERE userid = %s",
                    (userid,)
                )
                _byok_row = cur.fetchone()
                if _byok_row and (_byok_row[1] or "").strip().lower() == "byok":
                    cur.close()
                    conn.close()
                    return jsonify({"ok": True, "token": int(_byok_row[0])}), 200
                # Skip deduction when user has custom provider keys (server-side guard)
                _uname_for_check = getattr(request, '_session_user', '') or ''
                if _uname_for_check:
                    _cp = _user_has_custom_providers(_uname_for_check)
                    if _cp.get("email_verif") or _cp.get("llm"):
                        cur.close()
                        conn.close()
                        return jsonify({"ok": True, "token": int(_byok_row[0]) if _byok_row else 0, "skipped": True}), 200
            cur.execute(
                "UPDATE login SET token = COALESCE(token, 0) + %s WHERE userid = %s RETURNING token, username",
                (delta_int, userid)
            )
            row = cur.fetchone()
            conn.commit()
            cur.close(); conn.close()
            if not row:
                return jsonify({"error": "user not found"}), 404
            new_token, _uname = int(row[0]), (row[1] or "")
            if _APP_LOGGER_AVAILABLE:
                txn_type_d = "credit" if delta_int > 0 else "spend"
                log_financial(
                    username=_uname, userid=userid, feature=caller_feature,
                    transaction_type=txn_type_d,
                    token_before=new_token - delta_int, token_after=new_token,
                    transaction_amount=abs(delta_int),
                    token_usage=abs(delta_int) if txn_type_d == "spend" else 0,
                    token_cost_sgd=_token_cost_sgd,
                )
            return jsonify({"ok": True, "token": new_token}), 200
        except Exception as e:
            logger.error(f"[TokenUpdate/delta] {e}")
            return jsonify({"error": str(e)}), 500

    if not userid or token_val is None:
        return jsonify({"error": "userid and token are required"}), 400
    try:
        token_int = int(token_val)
    except (TypeError, ValueError):
        return jsonify({"error": "token must be a number"}), 400
    result_count_int = None
    rc = data.get("result_count")
    if rc is not None:
        try:
            result_count_int = int(rc)
        except (TypeError, ValueError):
            pass
    role_tag = (data.get("role_tag") or "").strip()
    _token_before_tx: int | None = None  # captured before the UPDATE for financial logging
    _username_tx: str = ""               # captured for financial logging
    try:
        import psycopg2
        pg_host = os.getenv("PGHOST", "localhost")
        pg_port = int(os.getenv("PGPORT", "5432"))
        pg_user = os.getenv("PGUSER", "postgres")
        pg_password = os.getenv("PGPASSWORD", "")
        pg_db = os.getenv("PGDATABASE", "candidate_db")
        conn = _pg_connect()
        cur = conn.cursor()
        # Skip token deduction for BYOK users (absolute set path)
        cur.execute("SELECT COALESCE(token, 0) AS t, useraccess FROM login WHERE userid = %s", (userid,))
        _byok_check = cur.fetchone()
        if _byok_check and (_byok_check[1] or "").strip().lower() == "byok":
            cur.close()
            conn.close()
            return jsonify({"ok": True, "token": int(_byok_check[0]), "skipped": True}), 200
        # Skip token deduction (lower balance) when user has custom provider keys
        if _byok_check and token_int < int(_byok_check[0]):
            _uname_for_check = getattr(request, '_session_user', '') or ''
            if _uname_for_check:
                _cp = _user_has_custom_providers(_uname_for_check)
                if _cp.get("email_verif") or _cp.get("llm"):
                    cur.close()
                    conn.close()
                    return jsonify({"ok": True, "token": int(_byok_check[0]), "skipped": True}), 200
        try:
            if result_count_int is not None:
                # Ensure idempotency columns exist — run at most once per process
                if not _token_guard_column_ensured:
                    cur.execute(
                        "ALTER TABLE login ADD COLUMN IF NOT EXISTS last_result_count INTEGER"
                    )
                    cur.execute(
                        "ALTER TABLE login ADD COLUMN IF NOT EXISTS last_deducted_role_tag TEXT"
                    )
                    _token_guard_column_ensured = True
                # Ensure session tracking columns exist — run at most once per process
                if not _role_tag_session_column_ensured:
                    cur.execute("ALTER TABLE login ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ")
                    cur.execute("ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ")
                    cur.execute("ALTER TABLE sourcing ALTER COLUMN session DROP DEFAULT")
                    _role_tag_session_column_ensured = True
                # Read current token, stored result count, stored role_tag, login role_tag, session, and username.
                # role_tag and role_tag_session are read so that we can auto-generate the session
                # timestamp for rows where role_tag is already set but role_tag_session is NULL
                # (e.g. rows that pre-existed before the role_tag_session column was added).
                cur.execute(
                    "SELECT token, last_result_count, last_deducted_role_tag,"
                    " role_tag, session, username FROM login WHERE userid = %s",
                    (userid,)
                )
                existing = cur.fetchone()
                if not existing:
                    conn.commit()
                    return jsonify({"error": "user not found"}), 404
                current_token, stored_count, _stored_role_tag_raw, login_role_tag, login_session_ts, login_username = existing
                stored_role_tag = (_stored_role_tag_raw or "").strip()
                _token_before_tx = int(current_token) if current_token is not None else None
                _username_tx = login_username or ""
                # Auto-backfill: if role_tag is already set in login but role_tag_session is NULL,
                # generate a session timestamp now and transfer it to sourcing where role_tag matches.
                # This ensures every role_tag entry is tied to a valid session reference even for
                # rows that existed before the role_tag_session column was introduced.
                if (login_role_tag or "").strip() and login_session_ts is None:
                    cur.execute(
                        "UPDATE login SET session = NOW() WHERE userid = %s RETURNING session",
                        (userid,)
                    )
                    ts_row = cur.fetchone()
                    login_session_ts = ts_row[0] if ts_row else None
                    if login_session_ts is not None and login_username:
                        cur.execute(
                            "UPDATE sourcing SET session = %s WHERE username = %s AND role_tag = %s",
                            (login_session_ts, login_username, login_role_tag)
                        )
                        logger.info(
                            f"[TokenUpdate] Auto-backfilled role_tag_session='{login_session_ts}' "
                            f"for user='{login_username}' (role_tag='{login_role_tag}')"
                        )
                # Backend idempotency guard: skip if same result_count was already persisted.
                # When role_tag is also provided, require that the stored role_tag also matches;
                # a NULL/empty stored role_tag with a provided role_tag is treated as a new session.
                if stored_count is not None and stored_count == result_count_int:
                    if (not role_tag) or (stored_role_tag and stored_role_tag == role_tag):
                        conn.commit()
                        return jsonify({"ok": True, "token": int(current_token) if current_token is not None else 0, "skipped": True}), 200
                # New deduction — persist updated balance, result count, and role_tag
                if role_tag:
                    cur.execute(
                        "UPDATE login SET token = %s, last_result_count = %s, last_deducted_role_tag = %s WHERE userid = %s RETURNING token",
                        (token_int, result_count_int, role_tag, userid)
                    )
                else:
                    cur.execute(
                        "UPDATE login SET token = %s, last_result_count = %s WHERE userid = %s RETURNING token",
                        (token_int, result_count_int, userid)
                    )
            else:
                # Legacy path: no result_count supplied.
                # Uses a session+role_tag guard: if login.session == sourcing.session
                # AND role_tags match, the deduction for this session was already
                # processed — skip to prevent repeated deductions on page refresh.
                if not _role_tag_session_column_ensured:
                    cur.execute("ALTER TABLE login ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ")
                    cur.execute("ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ")
                    cur.execute("ALTER TABLE sourcing ALTER COLUMN session DROP DEFAULT")
                    _role_tag_session_column_ensured = True
                cur.execute(
                    "SELECT role_tag, session, username, token FROM login WHERE userid = %s",
                    (userid,)
                )
                _legacy_row = cur.fetchone()
                if _legacy_row:
                    _legacy_role_tag, _legacy_session_ts, _legacy_username, _legacy_token = _legacy_row
                    _token_before_tx = int(_legacy_token) if _legacy_token is not None else None
                    _username_tx = _legacy_username or ""
                    # Auto-backfill: if role_tag is set but session is NULL, generate now
                    if (_legacy_role_tag or "").strip() and _legacy_session_ts is None:
                        cur.execute(
                            "UPDATE login SET session = NOW() WHERE userid = %s RETURNING session",
                            (userid,)
                        )
                        _legacy_ts_row = cur.fetchone()
                        _legacy_new_ts = _legacy_ts_row[0] if _legacy_ts_row else None
                        if _legacy_new_ts is not None:
                            _legacy_session_ts = _legacy_new_ts
                            if _legacy_username:
                                cur.execute(
                                    "UPDATE sourcing SET session = %s WHERE username = %s AND role_tag = %s",
                                    (_legacy_new_ts, _legacy_username, _legacy_role_tag)
                                )
                                logger.info(
                                    f"[TokenUpdate] Auto-backfilled role_tag_session='{_legacy_new_ts}' "
                                    f"for user='{_legacy_username}' (role_tag='{_legacy_role_tag}') via legacy path"
                                )
                    # Session+role_tag guard: skip deduction when both tables have
                    # the same session timestamp and role_tag (already processed).
                    if (_legacy_session_ts is not None and (_legacy_role_tag or "").strip()
                            and _legacy_username):
                        cur.execute(
                            "SELECT session FROM sourcing"
                            " WHERE username = %s AND role_tag = %s LIMIT 1",
                            (_legacy_username, _legacy_role_tag)
                        )
                        _src_row = cur.fetchone()
                        _src_session = _src_row[0] if _src_row else None
                        if _src_session is not None and _src_session == _legacy_session_ts:
                            conn.commit()
                            return jsonify({"ok": True,
                                            "token": int(_legacy_token) if _legacy_token is not None else 0,
                                            "skipped": True}), 200
                cur.execute(
                    "UPDATE login SET token = %s WHERE userid = %s RETURNING token",
                    (token_int, userid)
                )
            row = cur.fetchone()
            conn.commit()
        finally:
            cur.close()
            conn.close()
        if not row:
            return jsonify({"error": "user not found"}), 404
        new_token = int(row[0])
        # Log token spend/credit transaction
        if _APP_LOGGER_AVAILABLE:
            delta = (new_token - _token_before_tx) if _token_before_tx is not None else None
            if delta is None:
                txn_type = "adjustment"
            elif delta < 0:
                txn_type = "spend"
            elif delta > 0:
                txn_type = "credit"
            else:
                txn_type = "adjustment"
            log_financial(
                username=_username_tx,
                userid=userid,
                feature=caller_feature,
                transaction_type=txn_type,
                token_before=_token_before_tx,
                token_after=new_token,
                transaction_amount=abs(delta) if delta is not None else None,
                token_usage=abs(delta) if (delta is not None and txn_type == "spend") else 0,
                token_cost_sgd=_token_cost_sgd,
            )
        return jsonify({"ok": True, "token": new_token}), 200
    except Exception as e:
        logger.error(f"[TokenUpdate] {e}")
        return jsonify({"error": str(e)}), 500


# ==================== Fetch Skills Endpoint ====================

@app.route("/user/fetch_skills", methods=["GET"])
def user_fetch_skills():
    """
    GET /user/fetch_skills?username=<username>
    Returns the user's skill list from the login table (jskillset or skills column).
    Response: { "skills": ["Python", "C++", ...] }
    """
    username = (request.args.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username required"}), 400
    try:
        skills = _fetch_jskillset(username)
        return jsonify({"skills": skills}), 200
    except Exception as e:
        logger.error(f"[fetch_skills] Error for user='{username}': {e}")
        return jsonify({"error": str(e)}), 500

# ==================== Role Tag Update Endpoint ====================

@app.route("/user/update_role_tag", methods=["POST", "GET"])
def user_update_role_tag():
    """
    POST/GET /user/update_role_tag
    Updates role_tag in both login and sourcing tables for the given username.
    The sourcing table is the authoritative source for role-based job title assessment.

    Session tracking:
    - A timestamp (role_tag_session) is generated and stored in login when role_tag is set.
    - The same timestamp is transferred to sourcing only after validating that the
      role_tag value matches in both tables, ensuring cross-table traceability.
    """
    global _role_tag_session_column_ensured
    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        username = (data.get("username") or "").strip()
        role_tag = (data.get("role_tag") or "").strip()
    else:
        username = (request.args.get("username") or "").strip()
        role_tag = (request.args.get("role_tag") or "").strip()
    if not username:
        return jsonify({"error": "username required"}), 400
    conn = None
    cur = None
    try:
        import psycopg2
        pg_host = os.getenv("PGHOST", "localhost")
        pg_port = int(os.getenv("PGPORT", "5432"))
        pg_user = os.getenv("PGUSER", "postgres")
        pg_password = os.getenv("PGPASSWORD", "")
        pg_db = os.getenv("PGDATABASE", "candidate_db")
        conn = _pg_connect()
        cur = conn.cursor()
        # Ensure role_tag_session column exists in login and sourcing (once per process).
        # NOTE: This flag mirrors the _token_guard_column_ensured pattern; it is intentionally
        # not protected by a lock for the same reason — IF NOT EXISTS makes the DDL idempotent,
        # so concurrent first-time executions are safe.
        if not _role_tag_session_column_ensured:
            cur.execute("ALTER TABLE login ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ")
            cur.execute("ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ")
            cur.execute("ALTER TABLE sourcing ALTER COLUMN session DROP DEFAULT")
            _role_tag_session_column_ensured = True
        # Step 1: Update login — set role_tag and generate session timestamp atomically
        cur.execute(
            "UPDATE login SET role_tag=%s, session=NOW() WHERE username=%s",
            (role_tag, username)
        )
        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"error": "User not found"}), 404
        # Step 2: Read back the persisted role_tag and session timestamp from login
        cur.execute(
            "SELECT role_tag, session FROM login WHERE username=%s",
            (username,)
        )
        login_row = cur.fetchone()
        login_role_tag = login_row[0] if login_row else None
        login_session_ts = login_row[1] if login_row else None
        # Step 3: Update sourcing role_tag for all records of this user
        cur.execute("ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS role_tag TEXT DEFAULT ''")
        cur.execute("UPDATE sourcing SET role_tag=%s WHERE username=%s AND (role_tag IS NULL OR role_tag='')", (role_tag, username))
        # Step 4: Validate that role_tag matches in both login and sourcing, then transfer
        # the session timestamp from login to sourcing for consistency and traceability.
        if login_role_tag == role_tag and login_session_ts is not None:
            cur.execute(
                "UPDATE sourcing SET session=%s WHERE username=%s AND role_tag=%s",
                (login_session_ts, username, role_tag)
            )
        conn.commit()
        logger.info(
            f"[UpdateRoleTag] Set role_tag='{role_tag}' session_ts='{login_session_ts}' "
            f"for user='{username}' in login and sourcing tables"
        )
        # login_session_ts may be a datetime object or a plain string depending on
        # the column type and psycopg2 type-casting; handle both safely.
        if login_session_ts is not None:
            session_val = (login_session_ts.isoformat()
                           if hasattr(login_session_ts, 'isoformat')
                           else str(login_session_ts))
        else:
            session_val = None
        return jsonify({"ok": True, "username": username, "role_tag": role_tag,
                        "session": session_val}), 200
    except Exception as e:
        logger.exception(f"[UpdateRoleTag] Failed for user='{username}': {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# ==================== VSkillset Integration Endpoints ====================

@app.get("/user/jskillset")
def get_user_jskillset():
    """
    GET /user/jskillset?username=<username>
    Returns the user's jskillset from the login table.
    Response: { "jskillset": ["Python", "C++", ...] }
    """
    username = (request.args.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username required"}), 400
    
    try:
        skills = _fetch_jskillset(username)
        return jsonify({"jskillset": skills}), 200
    except Exception as e:
        logger.error(f"[get_user_jskillset] Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.post("/vskillset/infer")
@_rate(_make_flask_limit("vskillset_infer"))
@_check_user_rate("vskillset_infer")
def vskillset_infer():
    """
    POST /vskillset/infer
    Body: { 
        linkedinurl: "<url>", 
        skills: ["Python", "C++", ...], 
        assessment_level: "L1"|"L2", 
        username: "<optional>" 
    }
    
    Uses Gemini to evaluate each skill based on experience/cv.
    Returns: { 
        results: [ 
            { skill: "Python", probability: 85, category: "High", reason: "..." },
            ...
        ], 
        persisted: true 
    }
    """
    data = request.get_json(force=True, silent=True) or {}
    linkedinurl = (data.get("linkedinurl") or "").strip()
    skills = data.get("skills", [])
    assessment_level = (data.get("assessment_level") or "L2").upper()
    username = (data.get("username") or "").strip()
    force_regen = bool(data.get("force", False))
    
    if not linkedinurl or not skills:
        return jsonify({"error": "linkedinurl and skills required"}), 400
    
    if not isinstance(skills, list) or len(skills) == 0:
        return jsonify({"error": "skills must be a non-empty array"}), 400
    
    try:
        import psycopg2
        from psycopg2 import sql
        pg_host = os.getenv("PGHOST", "localhost")
        pg_port = int(os.getenv("PGPORT", "5432"))
        pg_user = os.getenv("PGUSER", "postgres")
        pg_password = os.getenv("PGPASSWORD", "")
        pg_db = os.getenv("PGDATABASE", "candidate_db")
        
        conn = _pg_connect()
        cur = conn.cursor()
        
        # Normalize linkedin URL
        normalized = linkedinurl.lower().strip().rstrip('/')
        if not normalized.startswith('http'):
            normalized = 'https://' + normalized
        
        # Idempotency guard: if vskillset already exists in DB, return it without
        # re-running Gemini. Pass force=true in the request body to override this.
        if not force_regen:
            try:
                cur.execute("""
                    SELECT vskillset FROM process
                    WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s
                       OR normalized_linkedin = %s
                    LIMIT 1
                """, (normalized, normalized))
                vs_row = cur.fetchone()
                if vs_row and vs_row[0]:
                    existing_vs = vs_row[0]
                    if isinstance(existing_vs, str):
                        existing_vs = json.loads(existing_vs)
                    if isinstance(existing_vs, list) and len(existing_vs) > 0:
                        high_skills = [i["skill"] for i in existing_vs if isinstance(i, dict) and i.get("category") == "High"]
                        cur.close()
                        conn.close()
                        logger.info(f"[vskillset_infer] Returning existing vskillset ({len(existing_vs)} items) for {linkedinurl[:50]} — use force=true to regenerate")
                        return jsonify({
                            "results": existing_vs,
                            "persisted": True,
                            "skipped": True,
                            "high_skills": high_skills,
                            "confirmed_skills": [i["skill"] for i in existing_vs if isinstance(i, dict) and i.get("source") == "confirmed"],
                            "inferred_skills":  [i["skill"] for i in existing_vs if isinstance(i, dict) and i.get("source") == "inferred"],
                        }), 200
            except Exception as _e:
                logger.warning(f"[vskillset_infer] Idempotency check failed ({_e}); proceeding with generation")
        
        # Fetch experience and cv from process table
        experience_text = ""
        cv_text = ""
        
        # Try by normalized_linkedin first, then linkedinurl
        cur.execute("""
            SELECT experience, cv 
            FROM process 
            WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s 
               OR normalized_linkedin = %s
            LIMIT 1
        """, (normalized, normalized))
        row = cur.fetchone()
        
        if row:
            experience_text = (row[0] or "").strip()
            cv_text = (row[1] or "").strip()
        
        # Use experience as primary, cv as fallback
        profile_context = experience_text if experience_text else cv_text
        
        if not profile_context:
            cur.close()
            conn.close()
            return jsonify({
                "error": "No experience or CV data found for this profile",
                "results": [],
                "persisted": False
            }), 404
        
        # STEP 1: Extractive pass - mark skills explicitly mentioned in experience text as confirmed/High
        explicitly_confirmed = _extract_confirmed_skills(profile_context, skills)
        confirmed_set = set(s.lower() for s in explicitly_confirmed)
        confirmed_results = [
            {
                "skill": skill,
                "probability": 100,
                "category": "High",
                "reason": "Explicitly mentioned in experience text",
                "source": "confirmed"
            }
            for skill in explicitly_confirmed
        ]
        logger.info(f"[vskillset_infer] Extractive pass: {len(confirmed_results)}/{len(skills)} skills confirmed from text")

        # STEP 2: Only send unconfirmed skills to Gemini for inference
        unconfirmed_skills = [s for s in skills if s.lower() not in confirmed_set]
        inferred_results = []

        if unconfirmed_skills:
            prompt = f"""SYSTEM:
You are an expert technical recruiter evaluating candidate skillsets based on their work experience.

TASK:
For each skill in the list below, evaluate the candidate's likely proficiency based on their experience.
These skills were NOT found explicitly in the experience text, so use contextual inference from
job titles, companies, products, sector, and experience patterns.
Assign a probability score (0-100) and categorize as Low (<40), Medium (40-74), or High (75-100).

CANDIDATE PROFILE:
{profile_context[:3000]}

SKILLS TO INFER (not found explicitly in experience text):
{json.dumps(unconfirmed_skills, ensure_ascii=False)}

OUTPUT FORMAT (JSON):
{{
  "evaluations": [
    {{
      "skill": "skill_name",
      "probability": 0-100,
      "category": "Low|Medium|High",
      "reason": "Brief explanation based on companies and roles"
    }}
  ]
}}

Return ONLY the JSON object, no other text."""

            raw_text = (unified_llm_call_text(prompt) or "").strip()
            _increment_gemini_query_count(username)

            parsed = _extract_json_object(raw_text)

            if not parsed or "evaluations" not in parsed:
                logger.warning(f"[vskillset_infer] Gemini returned invalid JSON: {raw_text[:200]}")
                # Fallback: create basic inferred results for unconfirmed skills
                for skill in unconfirmed_skills:
                    inferred_results.append({
                        "skill": skill,
                        "probability": 50,
                        "category": "Medium",
                        "reason": "Unable to parse Gemini response",
                        "source": "inferred"
                    })
            else:
                inferred_results = parsed["evaluations"]

            # Ensure all required fields are present and annotate source
            for item in inferred_results:
                if "probability" not in item:
                    item["probability"] = 50
                if "category" not in item:
                    prob = item.get("probability", 50)
                    if prob >= 75:
                        item["category"] = "High"
                    elif prob >= 40:
                        item["category"] = "Medium"
                    else:
                        item["category"] = "Low"
                if "reason" not in item:
                    item["reason"] = "No reasoning provided"
                item["source"] = "inferred"

        # STEP 3: Merge confirmed + inferred results
        results = confirmed_results + inferred_results
        
        # Persist to database
        # 1. Store full annotated results in vskillset column (JSON)
        # 2. Store only High skills in skillset column as comma-separated string
        
        vskillset_json = json.dumps(results, ensure_ascii=False)
        high_skills = [item["skill"] for item in results if item["category"] == "High"]
        # Ensure all skills are strings before joining
        skillset_str = ", ".join([str(s) for s in high_skills if s])
        
        # Check if vskillset column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='process' 
              AND column_name IN ('vskillset', 'skillset')
        """)
        available_cols = {r[0] for r in cur.fetchall()}
        
        # Update process table
        updates = []
        if 'vskillset' in available_cols:
            updates.append("vskillset = %s")
        
        if updates:
            update_sql = sql.SQL("UPDATE process SET {} WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s").format(sql.SQL(", ".join(updates)))
            update_values = []
            if 'vskillset' in available_cols:
                update_values.append(vskillset_json)
            update_values.append(normalized)
            cur.execute(update_sql, tuple(update_values))
        
        # Skillset: merge new High skills into existing value (add only; never remove or replace)
        if 'skillset' in available_cols and high_skills:
            cur.execute(
                "SELECT skillset FROM process WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s",
                (normalized,)
            )
            _sk_row = cur.fetchone()
            _existing_sk = (_sk_row[0] or "") if _sk_row else ""
            _existing_parts = [s.strip() for s in _existing_sk.split(",") if s.strip()]
            _existing_set = {s.lower() for s in _existing_parts}
            _new_high = [s for s in high_skills if s.lower() not in _existing_set]
            if _new_high:
                _merged_sk = ", ".join(_existing_parts + _new_high)
                cur.execute(
                    "UPDATE process SET skillset = %s"
                    " WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s",
                    (_merged_sk, normalized)
                )
                logger.info(f"[vskillset_infer] Merged {len(_new_high)} new High skills into skillset for {linkedinurl[:50]}")
            else:
                logger.info(f"[vskillset_infer] No new High skills for {linkedinurl[:50]} — skillset unchanged")
        
        conn.commit()
        
        cur.close()
        conn.close()
        
        return jsonify({
            "results": results,
            "persisted": True,
            "confirmed_skills": [item["skill"] for item in results if item.get("source") == "confirmed"],
            "inferred_skills": [item["skill"] for item in results if item.get("source") == "inferred"],
            "high_skills": high_skills
        }), 200
        
    except Exception as e:
        logger.error(f"[vskillset_infer] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e), "persisted": False}), 500

@app.get("/process/skillsets")
def get_process_skillsets():
    """
    GET /process/skillsets?linkedin=<linkedinurl>
    Returns the persisted skillset and vskillset for a candidate.
    Response: { 
        "skillset": ["Python", "C++", ...], 
        "vskillset": [ 
            { "skill": "Python", "probability": 85, "category": "High", "reason": "..." },
            ...
        ] 
    }
    """
    linkedinurl = (request.args.get("linkedin") or "").strip()
    if not linkedinurl:
        return jsonify({"error": "linkedin parameter required"}), 400
    
    try:
        import psycopg2
        from psycopg2 import sql as pgsql
        pg_host = os.getenv("PGHOST", "localhost")
        pg_port = int(os.getenv("PGPORT", "5432"))
        pg_user = os.getenv("PGUSER", "postgres")
        pg_password = os.getenv("PGPASSWORD", "")
        pg_db = os.getenv("PGDATABASE", "candidate_db")
        
        conn = _pg_connect()
        cur = conn.cursor()
        
        # Normalize linkedin URL
        normalized = linkedinurl.lower().strip().rstrip('/')
        if not normalized.startswith('http'):
            normalized = 'https://' + normalized
        
        # Check which columns exist
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='process' 
              AND column_name IN ('vskillset', 'skillset')
        """)
        available_cols = {r[0] for r in cur.fetchall()}
        
        # Build SELECT query based on available columns
        select_cols = []
        if 'vskillset' in available_cols:
            select_cols.append('vskillset')
        if 'skillset' in available_cols:
            select_cols.append('skillset')
        
        if not select_cols:
            cur.close()
            conn.close()
            return jsonify({"skillset": [], "vskillset": []}), 200
        
        query = pgsql.SQL("SELECT {} FROM process WHERE LOWER(TRIM(TRAILING '/' FROM linkedinurl)) = %s LIMIT 1").format(pgsql.SQL(", ").join(pgsql.Identifier(c) for c in select_cols))
        cur.execute(query, (normalized,))
        row = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not row:
            return jsonify({"skillset": [], "vskillset": []}), 200
        
        result = {}
        col_idx = 0
        
        if 'vskillset' in available_cols:
            vskillset_raw = row[col_idx]
            col_idx += 1
            if vskillset_raw:
                if isinstance(vskillset_raw, str):
                    try:
                        result["vskillset"] = json.loads(vskillset_raw)
                    except (json.JSONDecodeError, ValueError):
                        result["vskillset"] = []
                elif isinstance(vskillset_raw, list):
                    result["vskillset"] = vskillset_raw
                else:
                    result["vskillset"] = []
            else:
                result["vskillset"] = []
        else:
            result["vskillset"] = []
        
        if 'skillset' in available_cols:
            skillset_raw = row[col_idx]
            if skillset_raw:
                if isinstance(skillset_raw, str):
                    try:
                        parsed = json.loads(skillset_raw)
                        if isinstance(parsed, list):
                            result["skillset"] = parsed
                        else:
                            result["skillset"] = [s.strip() for s in skillset_raw.split(',') if s.strip()]
                    except (json.JSONDecodeError, ValueError):
                        result["skillset"] = [s.strip() for s in skillset_raw.split(',') if s.strip()]
                elif isinstance(skillset_raw, list):
                    result["skillset"] = skillset_raw
                else:
                    result["skillset"] = []
            else:
                result["skillset"] = []
        else:
            result["skillset"] = []
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"[get_process_skillsets] Error: {e}")
        return jsonify({"error": str(e)}), 500

# ==================== End VSkillset Integration ====================

# ... rest of file unchanged beyond this point ...

# Suggestion code: caching, supplemental lists, enforcement with sector-aware filtering
# SUGGEST_CACHE is backed by cache_backend (Redis when REDIS_URL is set, else in-process dict).
# The legacy dict and lock are kept as a secondary in-process layer for zero-dependency installs.
SUGGEST_CACHE: dict = {}   # kept for backward compat; cache_backend is the primary store
SUGGEST_CACHE_LOCK = threading.Lock()
MAX_SUGGESTIONS_PER_TAG = int(os.getenv("MAX_SUGGESTIONS_PER_TAG", "6"))
COMPANY_SUGGESTIONS_LIMIT = int(os.getenv("COMPANY_SUGGESTIONS_LIMIT", "30"))

def _clean_list(items, limit=20):
    out=[]; seen=set()
    for x in items or []:
        if not isinstance(x,str): continue
        t=re.sub(r'\s+',' ',x).strip()
        if not t: continue
        k=t.lower()
        if k in seen: continue
        t=re.sub(r'[;,/]+$','',t)
        seen.add(k); out.append(t)
        if len(out)>=limit: break
    return out

# Regex to match trailing corporate entity type suffixes (legal entity words, not brand words).
_CORP_SUFFIX_RE = re.compile(
    r'(\s*,?\s*'
    r'(?:Co\.\s*,?\s*Ltd\.?|Co\.?,?|Ltd\.?|Inc\.?|K\.K\.?|Corp\.?|Corporation|GmbH'
    r'|S\.A\.?|N\.V\.?|B\.V\.?|A\.G\.?|PLC|Plc|L\.L\.C\.?|LLC|Company,?)'
    r'\s*[,.]?\s*)$',
    re.IGNORECASE
)

def _strip_corp_suffix(name: str) -> str:
    """Strip trailing corporate entity suffixes (Co., Ltd., Inc., K.K., etc.) from a company name."""
    if not name:
        return name
    result = name.strip().rstrip(',').rstrip('.').strip()
    # Run up to 3 passes to handle compound suffixes such as "Co., Ltd." — each pass strips
    # one suffix token (e.g. pass 1 removes "Ltd.", pass 2 removes the trailing "Co.").
    for _ in range(3):
        new = _CORP_SUFFIX_RE.sub('', result).strip().rstrip(',').rstrip('.').strip()
        if new == result:
            break
        result = new
    # If stripping removed the entire name (edge case), fall back to the original.
    return result if result else name.strip()

def _country_to_region(country: str):
    c=(country or "").strip().lower()
    if not c: return None
    apac={"singapore","japan","taiwan","hong kong","china","south korea","korea","vietnam","thailand","malaysia","indonesia","philippines","australia","new zealand","india"}
    emea={"united kingdom","uk","england","ireland","germany","france","spain","italy","portugal","belgium","netherlands","switzerland","austria","poland","czech republic","czechia","sweden","norway","denmark","finland"}
    amer={"united states","usa","us","canada","mexico","brazil","argentina","chile","colombia"}
    if c in apac: return "apac"
    if c in emea: return "emea"
    if c in amer: return "americas"
    return None

COMPANY_REGION_PRESENCE = {
    "iqvia": {"apac","emea","americas"},
    "labcorp drug development": {"apac","emea","americas"},
    "labcorp": {"apac","emea","americas"},
    "ppd": {"apac","emea","americas"},
    "parexel": {"apac","emea","americas"},
    "icon": {"apac","emea","americas"},
    "syneos health": {"apac","emea","americas"},
    "novotech": {"apac"},
    "tigermed": {"apac"},
    "pfizer": {"apac","emea","americas"},
    "roche": {"apac","emea","americas"},
    "novartis": {"apac","emea","americas"},
    "johnson & johnson": {"apac","emea","americas"},
    "merck": {"apac","emea","americas"},
    "gsk": {"apac","emea","americas"},
    "sanofi": {"apac","emea","americas"},
    "astrazeneca": {"apac","emea","americas"},
    "bayer": {"apac","emea","americas"},
}

def _has_local_presence(company: str, country: str) -> bool:
    if not country: return True
    region=_country_to_region(country)
    k=(company or "").strip().lower()
    pres=COMPANY_REGION_PRESENCE.get(k)
    if pres:
        if region and region in pres: return True
        if country.strip().lower() in pres: return True
        return False
    return True

CRO_COMPETITORS = ["IQVIA","Labcorp Drug Development","Labcorp","ICON","Parexel","PPD","Syneos Health","Novotech","Tigermed"]
CRA_ADJACENT_ROLES = ["Clinical Trial Associate","Site Manager","Clinical Research Coordinator","Clinical Operations Lead","Study Start-Up Specialist","Clinical Project Manager"]

_BANNED_GENERIC_COMPANY_PHRASES = {
    "gaming studio","game studio","tech company","technology company","software company","pharma company","pharmaceutical company",
    "biotech company","marketing agency","consulting firm","it services provider","design agency","media company","advertising agency",
    "creative studio","blockchain company","web3 company","healthcare company","medical company","diagnostics company","clinical research company",
    "research organization","manufacturing company","energy company","data company"
}

def _is_real_company(name: str) -> bool:
    if not name or not isinstance(name, str):
        return False
    n = name.strip()
    if len(n) < 3:
        return False
    lower = n.lower()
    if lower in _BANNED_GENERIC_COMPANY_PHRASES:
        return False
    if re.search(r'[A-Z]', n):
        return True
    if '&' in n:
        return True
    return False

# Country/region words that Gemini appends to company names (e.g. "Electronic Arts China")
# Strip these trailing tokens so search results are based on the clean brand name.
_COMPANY_COUNTRY_SUFFIX_RE = re.compile(
    r'\s+(?:china|india|japan|korea|taiwan|singapore|malaysia|indonesia|thailand|vietnam|'
    r'philippines|australia|germany|france|uk|us|usa|emea|apac|latam|anz|mea|'
    r'asia(?:\s+pacific)?|pacific|americas|europe|international|global|limited|ltd\.?|'
    r'pte\.?\s*ltd\.?|inc\.?|corp(?:oration)?\.?|llc\.?|co\.?\s*ltd\.?|holdings?)$',
    re.IGNORECASE
)

# Parenthetical regional/status suffixes e.g. "(Japan)", "(Asia Pacific)", "(Merged)"
_COMPANY_PAREN_SUFFIX_RE = re.compile(r'\s*\([^)]+\)\s*$')

# Trailing "&" company-form patterns e.g. "& Co., Inc.", "& Co.", "& Sons"
_COMPANY_AMPERSAND_SUFFIX_RE = re.compile(
    r'\s*&\s*(?:co\.?\s*(?:,\s*)?(?:inc\.?|ltd\.?|llc\.?|plc\.?)?|sons?|partners?|associates?)\s*$',
    re.IGNORECASE
)

# Industry/entity-type descriptor words that Gemini appends to brand names
# e.g. "Takeda Pharmaceutical Company" → "Takeda", "Roche Diagnostics" → "Roche"
# Deliberately narrow: only strip words that are clearly generic descriptors, not brand differentiators.
_COMPANY_INDUSTRY_SUFFIX_RE = re.compile(
    r'\s+(?:pharmaceutical(?:s|(?:\s+company)?)?|diagnostics|biotech(?:nology)?|'
    r'life\s+sciences?|healthcare|health\s*care)$',
    re.IGNORECASE
)

def _strip_company_country_suffix(name: str) -> str:
    """
    Remove trailing suffixes from a company name to return the core brand name.
    Steps applied in order:
      1. Parenthetical regional/status suffixes: "(Japan)", "(Asia Pacific)"
      2. Ampersand company-form patterns: "& Co., Inc.", "& Sons"
      3. Industry/entity-type descriptors: "Pharmaceutical Company", "Diagnostics", "HealthCare"
         (applied up to 2 passes to handle chains like "Pharmaceutical Company")
      4. Country/region/legal-entity suffixes: "China", "Japan", "Ltd", "Inc", "Holdings"
         (applied up to 2 passes)
    Falls back to the original name if stripping reduces it to < 3 chars.
    """
    if not name:
        return name
    original = name.strip()
    s = original

    # Step 1: parenthetical suffixes
    s = _COMPANY_PAREN_SUFFIX_RE.sub('', s).strip()

    # Step 2: ampersand company-form patterns
    s = _COMPANY_AMPERSAND_SUFFIX_RE.sub('', s).strip()

    # Steps 3+4: interleave industry-type and country/legal suffix stripping.
    # Up to _MAX_SUFFIX_STRIP_PASSES passes so chained descriptors like
    # "Pharmaceuticals Corporation" are fully unwound in a single call.
    _MAX_PASSES = 3
    for _ in range(_MAX_PASSES):
        prev = s
        s2 = _COMPANY_INDUSTRY_SUFFIX_RE.sub('', s).strip()
        if s2 != s:
            s = s2
        s2 = _COMPANY_COUNTRY_SUFFIX_RE.sub('', s).strip()
        if s2 != s:
            s = s2
        if s == prev:
            break

    # Keep original if stripping makes it too short (< 3 chars)
    return s if len(s) >= 3 else original

def _supplement_companies(existing, country: str, limit: int, sectors=None):
    """
    Add companies from BUCKET_COMPANIES until we reach the desired limit,
    but do NOT include pharma companies unless sectors explicitly allow pharma.
    """
    pool=[]
    seen=set(x.lower() for x in existing)
    allow_pharma = _sectors_allow_pharma(sectors)
    for bucket, data in BUCKET_COMPANIES.items():
        for group in ("global","apac"):
            for c in data.get(group,[]) or []:
                cl=c.strip()
                if not cl: continue
                if cl.lower() in seen: continue
                # Skip pharma unless allowed by sectors
                if not allow_pharma and _is_pharma_company(cl):
                    continue
                if not _has_local_presence(cl, country):
                    continue
                pool.append(cl)
                seen.add(cl.lower())
                if len(existing)+len(pool) >= limit:
                    break
            if len(existing)+len(pool) >= limit:
                break
        if len(existing)+len(pool) >= limit:
            break
    return existing + pool[:max(0, limit-len(existing))]

def _enforce_company_limit(raw_list, country: str, limit: int, sectors=None):
    """
    Clean raw_list of strings into a limited list of companies.
    If result is shorter than limit, supplement from bucket list but avoid pharma unless sectors allow it.
    """
    cleaned=[]
    seen=set()
    allow_pharma = _sectors_allow_pharma(sectors)
    for c in raw_list or []:
        if not isinstance(c,str): continue
        t=_strip_company_country_suffix(c.strip())
        if not t: continue
        k=t.lower()
        if k in seen: continue
        if not _is_real_company(t):
            continue
        # Skip pharma unless allowed
        if not allow_pharma and _is_pharma_company(t):
            continue
        if not _has_local_presence(t, country):
            continue
        seen.add(k); cleaned.append(t)
        if len(cleaned) >= limit:
            break
    if len(cleaned) < limit:
        cleaned = _supplement_companies(cleaned, country, limit, sectors)
    return cleaned[:limit]

def _gemini_suggestions(job_titles, companies, industry, languages=None, sectors=None, country: str = None, products: list = None):
    languages = languages or []
    sectors = sectors or []
    products = products or []
    locality_hint = "Prioritize Singapore/APAC relevance where naturally applicable." if SINGAPORE_CONTEXT else ""
    
    # Add country-specific filtering instruction
    country_filter_hint = ""
    if country:
        country_filter_hint = f"\n- When suggesting companies, ONLY recommend companies with a legal entity or registered presence in {country}.\n- Exclude companies that do not operate in {country}.\n"

    # Add strict sector rule when sectors are provided to prevent cross-sector leakage
    sector_strict_hint = ""
    if sectors:
        sector_strict_hint = (
            "\n- STRICT SECTOR RULE for company.related: ONLY include companies whose PRIMARY BUSINESS and CORE"
            " OPERATIONS are direct competitors in the specified sector(s). EXCLUDE any company from a different"
            " industry that merely uses or purchases products/services in those sectors. Examples of what to exclude:\n"
            "  * For Gaming / Technology sectors: do NOT include pharma, healthcare, finance, insurance, or"
            " manufacturing companies, even if they use software or hire engineers internally.\n"
            "  * For Healthcare / Clinical Research sectors: do NOT include gaming, tech, or retail companies.\n"
            "  * For Industrial & Manufacturing sectors: do NOT include pure software, gaming, or financial services companies.\n"
            "  Competitors must share the same product/service focus as the job context.\n"
        )

    # Add product-based competitor hint when products are present.
    # Only applied when no companies are provided: when companies exist, they already
    # give Gemini strong competitor context, and adding products could create conflicting signals.
    product_hint = ""
    if products and not companies:
        product_hint = (
            f"\n- PRODUCT CONTEXT: The JD references these products/technologies: {', '.join(products[:10])}.\n"
            "  When no companies are explicitly mentioned, prioritize direct competitors that manufacture or sell these SAME products.\n"
            "  For example: if the JD mentions 'Aircon' or 'HVAC', suggest companies like Daikin, Carrier, Trane, Mitsubishi Electric, LG Electronics, etc.\n"
            "  Do NOT suggest companies from unrelated industries just to fill the list.\n"
        )

    input_obj = {
        "sectors": sectors,
        "jobTitles": job_titles,
        "companies": companies,
        "languages": languages,
        "location": (country or "").strip()
    }
    company_limit = COMPANY_SUGGESTIONS_LIMIT
    job_limit = MAX_SUGGESTIONS_PER_TAG
    prompt = (
        "SYSTEM:\nYou are a sourcing assistant. Produce concise, boolean-friendly suggestions.\n"
        "Return STRICT JSON ONLY in the form:\n"
        "{\"job\":{\"related\":[...]}, \"company\":{\"related\":[]}}\n"
        f"Hard requirements:\n"
        f"- Provide EXACTLY {job_limit} distinct, real, professional job title variants in job.related (if context allows; otherwise fill remaining with closest relevant titles).\n"
        f"- Provide EXACTLY {company_limit} distinct, real, company or organization names in company.related.\n"
        "- Company names MUST be real, brand-level entities (e.g., 'Ubisoft', 'Electronic Arts', 'Epic Games').\n"
        "- DO NOT output generic placeholders (e.g., 'Gaming Studio', 'Tech Company', 'Pharma Company', 'Consulting Firm', 'Marketing Agency').\n"
        + country_filter_hint
        + sector_strict_hint
        + product_hint
        + "- No duplicates, no commentary, no extra keys.\n"
        "- If insufficient context, fill remaining slots with well-known global or APAC companies relevant to the sectors/location.\n"
        "- Maintain JSON key order as shown.\n"
        f"{locality_hint}\n\nINPUT(JSON): {json.dumps(input_obj, ensure_ascii=False)}\n\nJSON:"
    )
    try:
        text = (unified_llm_call_text(
            prompt,
            cache_key="llm:suggest:" + hashlib.sha256(prompt.encode()).hexdigest(),
        ) or "").strip()
        start=text.find('{'); end=text.rfind('}')
        if start!=-1 and end!=-1 and end>start:
            parsed=json.loads(text[start:end+1])
            out={"job":{"related":[]}, "company":{"related":[]}}
            if isinstance(parsed,dict):
                jr=parsed.get("job",{}).get("related",[])
                cr=parsed.get("company",{}).get("related",[])
                jr_clean=_clean_list([s for s in jr if isinstance(s,str)], job_limit)
                if len(jr_clean) < job_limit:
                    heuristic_extra=_heuristic_job_suggestions(job_titles or jr_clean, industry, languages, sectors) or []
                    for h in heuristic_extra:
                        if h not in jr_clean and len(jr_clean) < job_limit:
                            jr_clean.append(h)
                # Pass sectors to enforce function so it can avoid adding pharma unless allowed
                cr_enforced=_enforce_company_limit(cr, country, company_limit, sectors)
                out["job"]["related"]=jr_clean[:job_limit]
                out["company"]["related"]=cr_enforced[:company_limit]
            return out
    except Exception as e:
        logger.warning(f"[Gemini Suggest] Failure: {e}")
    return None

def _heuristic_job_suggestions(job_titles, companies, industry, languages=None, sectors=None):
    out=set()
    languages = languages or []
    sectors = sectors or []
    for jt in job_titles:
        base=jt.strip()
        if not base: continue
        if "Senior" not in base and len(out)<MAX_SUGGESTIONS_PER_TAG: out.add(f"Senior {base}")
        if "Lead" not in base and len(out)<MAX_SUGGESTIONS_PER_TAG: out.add(f"Lead {base}")
        if industry=="Gaming" and "Game" not in base and len(out)<MAX_SUGGESTIONS_PER_TAG: out.add(f"Game {base}")
        if "Manager" not in base and not base.endswith("Manager") and len(out)<MAX_SUGGESTIONS_PER_TAG: out.add(f"{base} Manager")
        if len(out)>=MAX_SUGGESTIONS_PER_TAG: break
    if languages and len(out)<MAX_SUGGESTIONS_PER_TAG:
        for lang in languages:
            for role in [f"{lang} Translator", f"{lang} Interpreter", f"{lang} Localization", f"{lang} Linguist"]:
                if len(out)>=MAX_SUGGESTIONS_PER_TAG: break
                out.add(role)
            if len(out)>=MAX_SUGGESTIONS_PER_TAG: break
    if len(out)<MAX_SUGGESTIONS_PER_TAG:
        sect_join=" ".join(sectors).lower()
        jt_join=" ".join(job_titles).lower()
        if ("clinical research" in sect_join) or ("cra" in jt_join) or ("clinical research associate" in jt_join):
            for jt in CRA_ADJACENT_ROLES:
                if len(out)>=COMPANY_SUGGESTIONS_LIMIT: break
                out.add(jt)
    return dedupe(list(out))[:MAX_SUGGESTIONS_PER_TAG]

def _heuristic_company_suggestions(companies, languages=None, sectors=None, country: str = None):
    out=set()
    sectors = sectors or []
    for c in companies:
        base=c.strip()
        if not base: continue
        if base.endswith("Inc") or base.endswith("Inc."):
            cleaned=base.replace("Inc.","").replace("Inc","").strip()
            if cleaned: out.add(cleaned)
        if "Labs" not in base and len(out)<COMPANY_SUGGESTIONS_LIMIT: out.add(f"{base} Labs")
        if "Studio" not in base and len(out)<COMPANY_SUGGESTIONS_LIMIT: out.add(f"{base} Studio")
        if len(out)>=COMPANY_SUGGESTIONS_LIMIT: break
    if len(out)<COMPANY_SUGGESTIONS_LIMIT:
        sect_join=" ".join(sectors).lower()
        comp_join=" ".join(companies).lower()
        cro_context=("clinical research" in sect_join) or any(k in comp_join for k in ["iqvia","ppd","labcorp","parexel","icon","syneos","novotech","tigermed"])
        if cro_context:
            for cro in CRO_COMPETITORS:
                if len(out)>=COMPANY_SUGGESTIONS_LIMIT: break
                if _has_local_presence(cro, country):
                    out.add(cro)
    filtered=[c for c in out if _has_local_presence(c, country)]
    final=_enforce_company_limit(filtered, country, COMPANY_SUGGESTIONS_LIMIT)
    return final[:COMPANY_SUGGESTIONS_LIMIT]

def _prioritize_cross_sector(sets):
    freq={}
    for s in sets:
        for c in s: freq[c]=freq.get(c,0)+1
    cross=[c for c,f in freq.items() if f>1]; single=[c for c,f in freq.items() if f==1]
    ordered=[]; seen=set()
    for s in sets:
        for c in s:
            if c in cross and c not in seen: ordered.append(c); seen.add(c)
    for s in sets:
        for c in s:
            if c in single and c not in seen: ordered.append(c); seen.add(c)
    return ordered

def _heuristic_multi_sector(selected, user_job_title, user_company, languages=None):
    languages = languages or []
    # Use canonical bucket mapping to map selected sector labels to BUCKET_COMPANIES keys
    buckets=[_canon_sector_bucket(x) for x in selected] or ["other"]
    per_sets=[]
    for b in buckets:
        entries=BUCKET_COMPANIES.get(b, {})
        vals=entries.get("global", [])
        if SINGAPORE_CONTEXT:
            vals=list(dict.fromkeys(entries.get("apac", []) + vals))
        per_sets.append(set(vals))
    companies=_prioritize_cross_sector(per_sets)
    jobs=[]; seen=set()
    for b in buckets:
        for t in BUCKET_JOB_TITLES.get(b, []):
            k=t.lower()
            if k not in seen:
                seen.add(k); jobs.append(t)
    if not jobs:
        jobs=BUCKET_JOB_TITLES["other"][:]
    if languages:
        for lang in languages:
            for role in [f"{lang} Translator", f"{lang} Interpreter", f"{lang} Localization", f"{lang} Linguist"]:
                if role.lower() not in seen:
                    jobs.insert(0, role); seen.add(role.lower())
    companies=_enforce_company_limit(companies, None, 20)
    return {"job":{"related":jobs[:15]}, "company":{"related":companies[:20]}}

# Ensure canon mapping includes financial keywords
def _normalize_sector_name(s: str):
    s=(s or "").strip().lower()
    rep={"pharmaceutical":"pharmaceuticals","pharma":"pharmaceuticals","biotech":"biotechnology","med device":"medical devices",
         "medical device":"medical devices","devices":"medical devices","medtech":"medical devices","diagnostic":"diagnostics",
         "health tech":"healthtech","health tech.":"healthtech","healthcare tech":"healthtech","web3":"web3 & blockchain",
         "blockchain":"web3 & blockchain","ai":"ai & data","data":"ai & data","cyber security":"cybersecurity"}
    return rep.get(s, s).replace("&amp;","&").strip()

def _canon_sector_bucket(name: str):
    s=_normalize_sector_name(name)
    if not s:
        return "other"
    # Financial mappings
    if any(k in s for k in ["financial", "finance", "bank", "banking", "insurance", "investment", "asset", "asset management", "asset-management", "fintech", "wealth"]):
        return "financial_services"
    if any(k in s for k in ["pharmaceutical","pharmaceuticals","biotech","biotechnology"]): return "pharma_biotech"
    if "medical device" in s or "medtech" in s or "devices" in s: return "medical_devices"
    if "diagnostic" in s: return "diagnostics"
    if "healthtech" in s or "health tech" in s: return "healthtech"
    if "clinical_research" in s or "clinical research" in s: return "clinical_research"
    if "software" in s or "saas" in s or "technology" in s or "ai & data" in s or "ai" in s: return "technology"
    if "cybersecurity" in s: return "cybersecurity"
    if "automotive" in s or "manufactur" in s or "industrial" in s: return "manufacturing"
    if "energy" in s or "renewable" in s: return "energy"
    if "gaming" in s: return "gaming"
    if "web3" in s or "blockchain" in s: return "web3"
    return "other"

def _bucket_to_sector_label(bucket_name: str):
    """
    Map bucket names (from BUCKET_COMPANIES) to sectors.json labels.
    Returns a sector label that should exist in SECTORS_INDEX, or None.
    """
    bucket_to_label = {
        "pharma_biotech": "Healthcare > Pharmaceuticals",
        "medical_devices": "Healthcare > Medical Devices",
        "diagnostics": "Healthcare > Diagnostics",
        "clinical_research": "Healthcare > Clinical Research",
        "healthtech": "Healthcare > HealthTech",
        "technology": "Technology",
        "manufacturing": "Industrial & Manufacturing",
        "energy": "Energy & Environment",
        "gaming": "Media, Gaming & Entertainment > Gaming",
        "web3": "Emerging & Cross-Sector > Web3 & Blockchain",
        "financial_services": "Financial Services",
        "cybersecurity": "Technology > Cybersecurity",
        "other": None
    }
    
    label = bucket_to_label.get(bucket_name)
    # Verify the label exists in SECTORS_INDEX before returning
    if label and label in SECTORS_INDEX:
        return label
    
    # If exact match not found, try to find a partial match in SECTORS_INDEX
    if label:
        label_lower = label.lower()
        for idx_label in SECTORS_INDEX:
            idx_label_lower = idx_label.lower()
            if label_lower in idx_label_lower or idx_label_lower in label_lower:
                return idx_label
    
    return None

@app.post("/suggest")
def suggest():
    data = request.get_json(force=True, silent=True) or {}
    job_titles = data.get("jobTitles") or []
    companies = data.get("companies") or []
    industry = data.get("industry") or "Non-Gaming"
    languages = data.get("languages") or []
    sectors = data.get("sectors") or data.get("selectedSectors") or []
    country = (data.get("country") or "").strip()
    products = data.get("products") or []  # Product references extracted from JD
    _cache_key_parts = (
        tuple(sorted([jt.strip().lower() for jt in job_titles])),
        tuple(sorted([c.strip().lower() for c in companies])),
        industry.lower(),
        tuple(sorted([str(x).lower() for x in languages])),
        tuple(sorted([str(x).lower() for x in sectors])),
        country.lower(),
    )
    # Stable string key usable by both the local dict and Redis/cache_backend
    cache_key = "suggest:" + hashlib.sha256(repr(_cache_key_parts).encode()).hexdigest()
    cached = cache_get(cache_key)
    if cached:
        return jsonify(cached)

    user_jobs_clean = [jt.strip() for jt in job_titles if isinstance(jt, str) and jt.strip()]

    gem=_gemini_suggestions(job_titles, companies, industry, languages, sectors, country, products)
    if gem:
        existing_companies = {c.lower() for c in companies if isinstance(c, str) and c.strip()}
        gem_job_raw = [s for s in gem.get("job", {}).get("related", []) if isinstance(s, str) and s.strip()]
        gem_comp_raw = [s for s in gem.get("company", {}).get("related", []) if isinstance(s, str) and s.strip()]
        gem_job_filtered = [s for s in gem_job_raw if not any(s.strip().lower() == uj.lower() for uj in user_jobs_clean)]
        gem_comp_filtered = [s for s in gem_comp_raw if s.strip().lower() not in existing_companies]
        combined_jobs = list(gem_job_filtered)
        for uj in reversed(user_jobs_clean):
            if not any(uj.lower() == existing.lower() for existing in combined_jobs):
                combined_jobs.insert(0, uj)
        final_job_list = _clean_list(combined_jobs, MAX_SUGGESTIONS_PER_TAG)[:MAX_SUGGESTIONS_PER_TAG]
        final_company_list = gem_comp_filtered[:COMPANY_SUGGESTIONS_LIMIT]
        payload = {
            "job": {"related": final_job_list},
            "company": {"related": final_company_list},
            "engine": "gemini"
        }
    else:
        heuristic_jobs = _heuristic_job_suggestions(job_titles, industry, languages, sectors) or []
        heuristic_companies = _heuristic_company_suggestions(companies, languages, sectors, country) or []
        combined_jobs = list(heuristic_jobs)
        for uj in reversed(user_jobs_clean):
            if not any(uj.lower() == existing.lower() for existing in combined_jobs):
                combined_jobs.insert(0, uj)
        final_job_list = _clean_list(combined_jobs, MAX_SUGGESTIONS_PER_TAG)[:MAX_SUGGESTIONS_PER_TAG]
        final_company_list = heuristic_companies[:COMPANY_SUGGESTIONS_LIMIT]
        payload = {
            "job": {"related": final_job_list},
            "company": {"related": final_company_list},
            "engine": "heuristic"
        }

    cache_set(cache_key, payload, ttl=SUGGEST_CACHE_TTL)
    return jsonify(payload)

@app.post("/sector_suggest")
def sector_suggest():
    data = request.get_json(force=True, silent=True) or {}
    sectors_list = data.get("selectedSectors") or ([data.get("selectedSector")] if data.get("selectedSector") else [])
    sectors_list=[s for s in sectors_list if isinstance(s,str) and s.strip()]
    user_company=(data.get("userCompany") or "").strip()
    user_job_title=(data.get("userJobTitle") or "").strip()
    languages = data.get("languages") or []
    if not sectors_list and not user_company and not user_job_title and not languages:
        return jsonify({"job":{"related":[]}, "company":{"related":[]}}), 200
    normalized=[]
    for s in sectors_list:
        parts=[p.strip() for p in re.split(r'>', s) if p.strip()]
        normalized.append(parts[-1] if parts else s)
    normalized=[n for n in normalized if n]
    gem=_gemini_multi_sector(normalized, user_job_title, user_company, languages)
    if gem and (gem.get("job",{}).get("related") or gem.get("company",{}).get("related")):
        comp_rel = gem.get("company",{}).get("related") or []
        gem["company"]["related"] = [_strip_corp_suffix(c) for c in comp_rel if c]
        return jsonify(gem), 200
    result = _heuristic_multi_sector(normalized, user_job_title, user_company, languages)
    comp_rel = result.get("company",{}).get("related") or []
    result["company"]["related"] = [_strip_corp_suffix(c) for c in comp_rel if c]
    return jsonify(result), 200

# ── /prospect/source — CRM profile sourcing ──────────────────────────────────

_CRM_SALES_DIR = os.getenv(
    "CRM_SALES_DIR",
    r"F:\Recruiting Tools\Autosourcing\Sales",
)
_CRM_USERNAME_SAFE_RE = re.compile(r'[^A-Za-z0-9_-]')

# ── BD Activity store ─────────────────────────────────────────────────────────
_BD_ACTIVITY_PATH = os.getenv(
    "BD_ACTIVITY_PATH",
    os.path.join(
        os.getenv("CRM_SALES_DIR", r"F:\Recruiting Tools\Autosourcing\Sales"),
        "BD_Activity.json",
    ),
)
_bd_activity_lock = __import__("threading").Lock()


def _bd_load():
    """Return the BD Activity thread list (list of dicts). Never raises."""
    try:
        with open(_BD_ACTIVITY_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _bd_save(data):
    """Atomically write *data* (list) to BD_Activity.json."""
    os.makedirs(os.path.dirname(os.path.abspath(_BD_ACTIVITY_PATH)), exist_ok=True)
    tmp = _BD_ACTIVITY_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, _BD_ACTIVITY_PATH)


# GET /api/bd-activity — return all BD Activity threads (newest first)
@app.get("/api/bd-activity")
def bd_activity_get():
    emp_username = (request.cookies.get("emp_username") or "").strip()
    if not emp_username:
        return jsonify({"error": "Authentication required"}), 401
    with _bd_activity_lock:
        threads = _bd_load()
    threads_sorted = sorted(threads, key=lambda t: t.get("timestamp", ""), reverse=True)
    return jsonify({"threads": threads_sorted}), 200


# POST /api/bd-activity — create a new thread from a CRM status update
# Body: { company, comment, status }
@app.post("/api/bd-activity")
def bd_activity_post():
    emp_username = (request.cookies.get("emp_username") or "").strip()
    if not emp_username:
        return jsonify({"error": "Authentication required"}), 401
    # Validate emp_username exists in the employee table
    try:
        conn = _pg_connect()
        _ensure_employee_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM employee WHERE username = %s LIMIT 1", (emp_username,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"error": "Authentication required"}), 401
    except Exception as exc:
        logger.error("[bd-activity POST] employee check: %s", exc)
        return jsonify({"error": "Authentication service temporarily unavailable"}), 500
    body = request.get_json(silent=True) or {}
    company = str(body.get("company") or "").strip()[:200]
    comment = str(body.get("comment") or "").strip()[:1000]
    status  = str(body.get("status")  or "").strip()[:50]
    if not company and not status:
        return jsonify({"ok": False, "error": "company or status is required"}), 400
    entry = {
        "id":        __import__("uuid").uuid4().hex,
        "username":  emp_username,
        "company":   company,
        "comment":   comment,
        "status":    status,
        "timestamp": __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "replies":   [],
    }
    try:
        with _bd_activity_lock:
            threads = _bd_load()
            threads.append(entry)
            _bd_save(threads)
        return jsonify({"ok": True, "thread": entry}), 200
    except Exception as exc:
        logger.warning("[bd-activity POST] %s", exc)
        return jsonify({"ok": False, "error": "Could not save BD Activity entry."}), 500


# POST /api/bd-activity/<thread_id>/reply — append a reply to a thread
# Body: { text }
@app.post("/api/bd-activity/<thread_id>/reply")
def bd_activity_reply(thread_id):
    emp_username = (request.cookies.get("emp_username") or "").strip()
    if not emp_username:
        return jsonify({"error": "Authentication required"}), 401
    body = request.get_json(silent=True) or {}
    text = str(body.get("text") or "").strip()[:1000]
    if not text:
        return jsonify({"ok": False, "error": "text is required"}), 400
    reply = {
        "id":        __import__("uuid").uuid4().hex,
        "username":  emp_username,
        "text":      text,
        "timestamp": __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    try:
        with _bd_activity_lock:
            threads = _bd_load()
            for t in threads:
                if t.get("id") == thread_id:
                    t.setdefault("replies", []).append(reply)
                    _bd_save(threads)
                    return jsonify({"ok": True, "reply": reply}), 200
        return jsonify({"ok": False, "error": "Thread not found."}), 404
    except Exception as exc:
        logger.warning("[bd-activity reply POST] %s", exc)
        return jsonify({"ok": False, "error": "Could not save reply."}), 500


# DELETE /api/bd-activity/<thread_id> — remove a thread; only the owner may delete
@app.delete("/api/bd-activity/<thread_id>")
def bd_activity_delete(thread_id):
    emp_username = (request.cookies.get("emp_username") or "").strip()
    if not emp_username:
        return jsonify({"error": "Authentication required"}), 401
    username = emp_username
    try:
        with _bd_activity_lock:
            threads = _bd_load()
            match = next((t for t in threads if t.get("id") == thread_id), None)
            if match is None:
                return jsonify({"ok": False, "error": "Thread not found."}), 404
            if match.get("username") != username:
                return jsonify({"ok": False, "error": "Not authorised to delete this thread."}), 403
            threads = [t for t in threads if t.get("id") != thread_id]
            _bd_save(threads)
        return jsonify({"ok": True}), 200
    except Exception as exc:
        logger.warning("[bd-activity DELETE] %s", exc)
        return jsonify({"ok": False, "error": "Could not delete thread."}), 500

# Load countrycode.JSON once at startup for LinkedIn URL country resolution
_COUNTRYCODE_MAP: dict = {}
try:
    _cc_path = os.path.join(os.path.dirname(__file__), "countrycode.JSON")
    with open(_cc_path, "r", encoding="utf-8") as _cc_fh:
        _COUNTRYCODE_MAP = json.load(_cc_fh)
except Exception as _cc_exc:
    logger.warning("[CRM] Could not load countrycode.JSON: %s", _cc_exc)

_LINKEDIN_COUNTRY_RE = re.compile(r'https?://([a-z]{2})\.linkedin\.com/in/', re.I)


def _country_from_linkedin_url(url: str) -> str:
    """Infer the full country name from a LinkedIn URL subdomain.

    e.g. ``https://kr.linkedin.com/in/username`` → ``"Korea"``
    Returns empty string when the URL uses the global ``www`` subdomain or
    when the 2-letter code is not found in countrycode.JSON.
    """
    if not url:
        return ""
    m = _LINKEDIN_COUNTRY_RE.match(url)
    if not m:
        return ""
    code = m.group(1).lower()
    return _COUNTRYCODE_MAP.get(code, "")


def _build_prospect_query(job_titles, companies, sectors, country, seniority):
    """Build an X-ray LinkedIn people-search query from Prospect tab parameters."""
    parts = ["site:linkedin.com/in/"]

    title_terms = [jt.strip() for jt in (job_titles or []) if isinstance(jt, str) and jt.strip()]
    if title_terms:
        if len(title_terms) == 1:
            parts.append(f'"{title_terms[0]}"')
        else:
            parts.append("(" + " OR ".join(f'"{t}"' for t in title_terms) + ")")

    company_terms = [c.strip() for c in (companies or []) if isinstance(c, str) and c.strip()]
    if company_terms:
        if len(company_terms) == 1:
            parts.append(f'"{company_terms[0]}"')
        else:
            parts.append("(" + " OR ".join(f'"{c}"' for c in company_terms) + ")")

    sector_terms = [s.strip() for s in (sectors or []) if isinstance(s, str) and s.strip()]
    if sector_terms and not title_terms and not company_terms:
        # Only include sector terms when we have nothing else — avoids over-filtering
        parts.append("(" + " OR ".join(f'"{s}"' for s in sector_terms) + ")")

    if seniority and isinstance(seniority, str) and seniority.strip():
        parts.append(f'"{seniority.strip()}"')

    if country and isinstance(country, str) and country.strip():
        parts.append(country.strip())

    return " ".join(parts)


def _gemini_assess_crm_profile(name, job_title, company, snippet, sectors_hint, seniority_hint):
    """Use Gemini to infer sector and seniority for a sourced LinkedIn profile.

    Returns a dict: {"sector": str, "seniority": str}.  Falls back to the
    caller-supplied hints when the LLM is unavailable or returns garbage.
    """
    prompt = (
        "You are a talent-sourcing assistant.  Given the following person's details, "
        "return ONLY a JSON object with exactly two keys:\n"
        '  "sector": a concise industry sector label (e.g. "Technology", "Finance", "Healthcare")\n'
        '  "seniority": one of: C-Suite, VP, Director, Manager, Senior, Mid-Level, Junior, Intern\n\n'
        f"Name: {name or '(unknown)'}\n"
        f"Job Title: {job_title or '(unknown)'}\n"
        f"Company: {company or '(unknown)'}\n"
        f"Snippet: {(snippet or '')[:300]}\n"
    )
    if sectors_hint:
        prompt += f"Preferred sector(s): {', '.join(sectors_hint)}\n"
    prompt += "\nJSON only, no commentary:"
    try:
        raw = unified_llm_call_text(prompt, temperature=0.0, max_output_tokens=128)
        parsed = _extract_json_object(raw or "")
        if isinstance(parsed, dict):
            sector_val = parsed.get("sector")
            if not sector_val and sectors_hint:
                sector_val = sectors_hint[0]
            sector = str(sector_val or "").strip()
            seniority = str(parsed.get("seniority") or seniority_hint or "").strip()
            return {"sector": sector, "seniority": seniority}
    except Exception as exc:
        logger.warning("[CRM Gemini assess] %s", exc)
    return {
        "sector": sectors_hint[0] if sectors_hint else "",
        "seniority": seniority_hint or "",
    }



def _save_crm_json(username, profiles):
    """Append *profiles* to CRM_{username}.json in _CRM_SALES_DIR.

    Creates the directory and file if they do not exist.  Existing entries are
    preserved; duplicates (same LinkedIn URL) are skipped.
    """
    safe = _CRM_USERNAME_SAFE_RE.sub('_', username).strip('_') or 'user'
    crm_dir = _CRM_SALES_DIR
    try:
        os.makedirs(crm_dir, exist_ok=True)
    except OSError as exc:
        logger.warning("[CRM save] Cannot create directory %s: %s", crm_dir, exc)
        return
    crm_file = os.path.join(crm_dir, f"CRM_{safe}.json")
    # Security: ensure the resolved path stays within the intended directory
    abs_crm_dir = os.path.abspath(crm_dir)
    abs_file = os.path.abspath(crm_file)
    if not abs_file.startswith(abs_crm_dir + os.sep):
        logger.error("[CRM save] Path traversal blocked: %s", crm_file)
        return
    existing = []
    try:
        if os.path.exists(crm_file):
            with open(crm_file, "r", encoding="utf-8") as fh:
                existing = json.load(fh)
            if not isinstance(existing, list):
                existing = []
    except Exception as exc:
        logger.warning("[CRM save] Could not read existing file %s: %s", crm_file, exc)
        existing = []
    existing_urls = {(r.get("linkedinUrl") or "").lower() for r in existing if r.get("linkedinUrl")}
    added = 0
    for p in profiles:
        url = (p.get("linkedinUrl") or "").lower()
        if url and url in existing_urls:
            continue
        existing.append(p)
        if url:
            existing_urls.add(url)
        added += 1
    try:
        tmp = crm_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(existing, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, crm_file)
        logger.info("[CRM save] Saved %d new profile(s) to %s", added, crm_file)
    except Exception as exc:
        logger.warning("[CRM save] Write failed for %s: %s", crm_file, exc)


@app.post("/prospect/source")
@_require_session
def prospect_source():
    """Source LinkedIn profiles for the Sales Rep Prospect tab.

    Accepts JSON: {jobTitles, companies, sectors, country, seniority, mode, provider}

    Builds an X-ray/CSE query, fetches results via ``unified_search_page``,
    parses LinkedIn profile results, runs Gemini assessment for sector &
    seniority, persists to CRM_{username}.json, and returns the profiles.
    """
    username = request._session_user
    data = request.get_json(force=True, silent=True) or {}
    job_titles  = data.get("jobTitles")  or []
    companies   = data.get("companies")  or []
    sectors     = data.get("sectors")    or []
    country     = (data.get("country")   or "").strip()
    seniority   = (data.get("seniority") or "").strip()
    provider    = (data.get("provider")  or "").strip().lower() or None

    if not job_titles and not companies and not country and not sectors:
        return jsonify({"error": "At least one search parameter is required."}), 400

    query = _build_prospect_query(job_titles, companies, sectors, country, seniority)

    gl_hint = None
    if country:
        gl_hint = _infer_region_from_country(country) or None

    # Fetch up to 3 pages (30 results) from the configured provider
    all_results = []
    for page_start in [1, 11, 21]:
        try:
            items, _ = unified_search_page(
                query, 10, page_start,
                gl_hint=gl_hint,
                selected_provider=provider,
            )
            all_results.extend(items or [])
        except Exception as exc:
            logger.warning("[prospect/source] search page failed (start=%d): %s", page_start, exc)
        if len(all_results) >= 20:
            break

    # Filter to LinkedIn profile URLs and parse
    seen_urls = set()
    profiles = []
    for item in all_results:
        url = (item.get("link") or "").strip()
        if not is_linkedin_profile(url):
            continue
        url_norm = url.lower().rstrip('/')
        if url_norm in seen_urls:
            continue
        seen_urls.add(url_norm)

        title   = item.get("title") or ""
        snippet = item.get("snippet") or ""
        name, job_title_parsed, company_parsed = parse_linkedin_title(title)

        # Gemini assessment for sector + seniority
        assessed = _gemini_assess_crm_profile(
            name, job_title_parsed, company_parsed, snippet,
            sectors_hint=[s for s in sectors if isinstance(s, str) and s.strip()],
            seniority_hint=seniority,
        )

        profiles.append({
            "name":        name        or "",
            "jobTitle":    job_title_parsed or "",
            "company":     company_parsed   or "",
            "country":     _country_from_linkedin_url(url),
            "linkedinUrl": url,
            "sector":      assessed.get("sector")    or "",
            "seniority":   assessed.get("seniority") or seniority,
            "email":       "",
            "mobile":      "",
        })

    # Persist to CRM JSON file on disk
    if profiles:
        _save_crm_json(username, profiles)

    return jsonify({"profiles": profiles})


@app.get("/prospect/crm-data")
@_require_session
def prospect_crm_data_get():
    """Return the current user's CRM prospect list from CRM_{username}.json."""
    username = request._session_user
    safe = _CRM_USERNAME_SAFE_RE.sub('_', username).strip('_') or 'user'
    crm_file = os.path.join(_CRM_SALES_DIR, f"CRM_{safe}.json")
    abs_crm_dir = os.path.abspath(_CRM_SALES_DIR)
    abs_file = os.path.abspath(crm_file)
    if not abs_file.startswith(abs_crm_dir + os.sep):
        logger.error("[CRM load] Path traversal blocked: %s", crm_file)
        return jsonify({"profiles": []}), 200
    if not os.path.exists(crm_file):
        return jsonify({"profiles": []}), 200
    try:
        with open(crm_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            data = []
        return jsonify({"profiles": data}), 200
    except Exception as exc:
        logger.warning("[CRM load] Could not read %s: %s", crm_file, exc)
        return jsonify({"profiles": []}), 200


@app.delete("/prospect/crm-data")
@_require_session
def prospect_crm_data_delete():
    """Delete the current user's CRM_{username}.json file (called after XLS export)."""
    username = request._session_user
    safe = _CRM_USERNAME_SAFE_RE.sub('_', username).strip('_') or 'user'
    crm_file = os.path.join(_CRM_SALES_DIR, f"CRM_{safe}.json")
    abs_crm_dir = os.path.abspath(_CRM_SALES_DIR)
    abs_file = os.path.abspath(crm_file)
    if not abs_file.startswith(abs_crm_dir + os.sep):
        logger.error("[CRM delete] Path traversal blocked: %s", crm_file)
        return jsonify({"ok": False, "error": "Invalid path"}), 400
    try:
        if os.path.exists(crm_file):
            os.remove(crm_file)
            logger.info("[CRM delete] Removed %s", crm_file)
        return jsonify({"ok": True}), 200
    except Exception as exc:
        logger.warning("[CRM delete] Could not remove %s: %s", crm_file, exc)
        return jsonify({"ok": False, "error": "Could not delete CRM file."}), 500



@app.post("/prospect/crm-save")
@_require_session
def prospect_crm_save():
    """Overwrite the current user's CRM_{username}.json with the profiles sent from the frontend."""
    username = request._session_user
    safe = _CRM_USERNAME_SAFE_RE.sub('_', username).strip('_') or 'user'
    crm_file = os.path.join(_CRM_SALES_DIR, f"CRM_{safe}.json")
    abs_crm_dir = os.path.abspath(_CRM_SALES_DIR)
    abs_file = os.path.abspath(crm_file)
    if not abs_file.startswith(abs_crm_dir + os.sep):
        logger.error("[CRM save] Path traversal blocked: %s", crm_file)
        return jsonify({"ok": False, "error": "Invalid path"}), 400

    body = request.get_json(silent=True) or {}
    profiles = body.get("profiles")
    if not isinstance(profiles, list):
        return jsonify({"ok": False, "error": "profiles must be a list"}), 400

    # Sanitise each profile: keep only known safe fields
    _ALLOWED_FIELDS = {"name", "jobTitle", "company", "country", "sector", "seniority",
                       "email", "mobile", "comment", "status", "linkedinUrl"}
    clean = []
    for p in profiles:
        if not isinstance(p, dict):
            continue
        clean.append({k: str(v) for k, v in p.items() if k in _ALLOWED_FIELDS})

    try:
        os.makedirs(os.path.abspath(_CRM_SALES_DIR), exist_ok=True)
        tmp = crm_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(clean, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, crm_file)
        logger.info("[CRM save] Saved %d profile(s) to %s", len(clean), crm_file)
        return jsonify({"ok": True, "saved": len(clean)}), 200
    except Exception as exc:
        logger.warning("[CRM save] Write failed for %s: %s", crm_file, exc)
        return jsonify({"ok": False, "error": "Could not write CRM file."}), 500


@app.post("/prospect/crm-email-draft")
@_require_session
def prospect_crm_email_draft():
    """Generate an AI-drafted email body using LLM, based on subject and recipient context."""
    body = request.get_json(silent=True) or {}
    subject      = str(body.get("subject") or "").strip()
    existing     = str(body.get("existingBody") or "").strip()
    recipients   = body.get("recipients") or []
    if not isinstance(recipients, list):
        recipients = []

    # Build a concise recipient summary for the prompt
    rec_lines = []
    for r in recipients[:10]:
        parts = [r.get("name") or ""]
        if r.get("jobTitle"):  parts.append(r["jobTitle"])
        if r.get("company"):   parts.append("at " + r["company"])
        if r.get("sector"):    parts.append("(" + r["sector"] + ")")
        if r.get("seniority"): parts.append("[" + r["seniority"] + "]")
        rec_lines.append(", ".join(p for p in parts if p))
    rec_summary = "\n".join("- " + l for l in rec_lines) if rec_lines else "(no recipients)"

    prompt = (
        "You are a professional sales outreach copywriter.\n"
        "Draft a concise, personalised outreach email body for the following recipients.\n"
        "Use placeholder tags exactly as written so the sender can auto-fill them per recipient:\n"
        "  [Name]  [Job Title]  [Company Name]  [Country]  [Sector]  [Seniority]\n"
        "  [Your Name]  [Your Company Name]  [Meeting Date]  [Meeting Time]\n"
        "  [Video Conference Link]  [Scheduler]\n\n"
        f"Email Subject: {subject or '(not specified)'}\n\n"
        "Recipients:\n" + rec_summary + "\n\n"
        + (f"Existing draft to improve:\n{existing}\n\n" if existing else "")
        + "Return only the email body text (no subject line, no extra commentary)."
    )

    try:
        draft = unified_llm_call_text(prompt, temperature=0.7, max_output_tokens=512)
        if not draft:
            return jsonify({"ok": False, "error": "LLM returned no content. Check your AI provider configuration."}), 500
        return jsonify({"ok": True, "body": draft.strip()}), 200
    except Exception as exc:
        logger.warning("[CRM email draft] LLM call failed: %s", exc)
        return jsonify({"ok": False, "error": "AI draft failed. Check server logs for details."}), 500


# ── Verified Email helpers (mirrors server.js logic) ─────────────────────────

_VERIFIED_EMAIL_PATH = os.path.join(BASE_DIR, 'verified_email.json')
_verified_email_lock = threading.Lock()


def _load_verified_email():
    """Load verified_email.json, migrating legacy flat-array format if needed."""
    try:
        with open(_VERIFIED_EMAIL_PATH, 'r', encoding='utf-8') as _f:
            _data = json.load(_f)
        if isinstance(_data, list):
            _converted = {}
            for _entry in _data:
                _ck = re.sub(r'[^a-z0-9]', '_', (_entry.get('company') or 'unknown').lower())
                if _ck not in _converted:
                    _converted[_ck] = {'Domain': [], 'Confidence_threshold': 1}
                _converted[_ck]['Domain'].append({
                    **_entry,
                    'company': _ck,
                    'count': _entry.get('count') or 1,
                    'confidence': _entry.get('confidence') if _entry.get('confidence') is not None else 1,
                })
            return _converted
        for _ck, _cd in _data.items():
            if isinstance(_cd.get('Domain'), list):
                for _e in _cd['Domain']:
                    if _e.get('count') is None:
                        _e['count'] = 1
        return _data
    except Exception:
        return {}


def _save_verified_email(data):
    """Atomically save data to verified_email.json."""
    _tmp = _VERIFIED_EMAIL_PATH + '.tmp'
    with open(_tmp, 'w', encoding='utf-8') as _f:
        json.dump(data, _f, indent=2)
    os.replace(_tmp, _VERIFIED_EMAIL_PATH)


def _recalculate_confidences(company_data):
    """Redistribute confidence values proportionally based on each domain entry's count."""
    entries = company_data.get('Domain') or []
    if not entries:
        return
    total = sum(e.get('count') or 1 for e in entries)
    if not total:
        return
    sum_so_far = 0.0
    for i, e in enumerate(entries):
        if i == len(entries) - 1:
            e['confidence'] = round(max(0.0, 1.0 - sum_so_far), 2)
        else:
            e['confidence'] = round((e.get('count') or 1) / total, 2)
            sum_so_far += e['confidence']


@app.post("/prospect/crm-generate-email")
@_require_session
def prospect_crm_generate_email():
    """Generate email addresses for a CRM prospect.

    Checks verified_email.json for a known domain structure first; falls back
    to a plain Gemini inference prompt when no verified data is available.
    Mirrors the FIOE path in server.js /generate-email.
    """
    body = request.get_json(silent=True) or {}
    name    = str(body.get('name')    or '').strip()
    company = str(body.get('company') or '').strip()
    country = str(body.get('country') or '').strip()

    if not name or not company:
        return jsonify({'error': 'Name and Company are required.'}), 400

    try:
        company_key = re.sub(r'[^a-z0-9]', '_', company.lower())
        verified_data  = _load_verified_email()
        company_entry  = verified_data.get(company_key)

        email_source = 'gemini'
        verified_confidence = None

        if (company_entry
                and isinstance(company_entry.get('Domain'), list)
                and company_entry['Domain']):
            top_entry = max(company_entry['Domain'], key=lambda e: e.get('confidence') or 0)
            email_source = 'verified'
            verified_confidence = top_entry.get('confidence')
            gen_prompt = (
                f'You are an email address generator. The following verified email domain '
                f'structure has been confirmed for the company "{company}":\n'
                f'- Domain: {top_entry.get("domain", "")}\n'
                f'- Format: {top_entry.get("format", "")}\n'
                f'- Example: {top_entry.get("fake_example") or "(not available)"}\n\n'
                f'Using exactly this domain and format, generate 3 realistic email address '
                f'variations for a person named "{name}"'
                + (f' (located in {country})' if country else '') + '.\n'
                'Sort the list by highest probability of being the correct active email to lowest.\n'
                'Return strictly a JSON object: { "emails": ["email1", "email2", "email3"] }\n'
                'Do not include markdown formatting.'
            )
        else:
            gen_prompt = (
                f'Generate the most likely business email addresses for a person named '
                f'"{name}" working at the company "{company}"'
                + (f' (located in {country})' if country else '') + '.\n'
                'Infer the likely domain name based on the company name.\n'
                'For each email address candidate, estimate a probability (0–100) that it is the correct active email.\n'
                'Return strictly a JSON object: { "emails": [{ "email": "addr1", "probability": 85 }, { "email": "addr2", "probability": 10 }] }\n'
                'Sort by highest probability first. Include at least 1 and at most 3 candidates.\n'
                'Do not include markdown formatting.'
            )

        raw = (unified_llm_call_text(gen_prompt, temperature=0.3, max_output_tokens=256) or '').strip()
        _increment_gemini_query_count(request._session_user)

        cleaned = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
        cleaned = re.sub(r'\s*```$', '', cleaned, flags=re.MULTILINE).strip()

        try:
            data = json.loads(cleaned)
        except Exception:
            m = re.search(r'\[.*?\]', cleaned, re.DOTALL)
            if m:
                data = {'emails': json.loads(m.group(0))}
            else:
                raise ValueError('Failed to parse LLM email generation response')

        # Normalise: Gemini fallback may return [{email, probability}] objects; verified path returns strings.
        raw_emails = data.get('emails') or []
        top_probability = None
        email_probabilities = []
        if email_source == 'gemini' and raw_emails and isinstance(raw_emails[0], dict):
            email_list = [e['email'] for e in raw_emails if isinstance(e, dict) and e.get('email')]
            email_probabilities = [e.get('probability') for e in raw_emails if isinstance(e, dict) and e.get('email')]
            top_probability = email_probabilities[0] if email_probabilities else None
        else:
            email_list = [e if isinstance(e, str) else e.get('email', '') for e in raw_emails if e]
            # For the verified path generate distinct declining probabilities per email so
            # each selectable tag shows a unique confidence value.
            if email_source == 'verified' and email_list:
                base_pct = round((verified_confidence or 0.95) * 100)
                scale = [1.0, 0.85, 0.70]
                email_probabilities = [
                    min(100, round(base_pct * scale[min(i, len(scale) - 1)])) for i in range(len(email_list))
                ]
                top_probability = email_probabilities[0] if email_probabilities else None

        return jsonify({
            'emails': email_list,
            'source': email_source,
            'confidence': verified_confidence,
            'probability': top_probability,
            'email_probabilities': email_probabilities,
        }), 200

    except Exception as exc:
        logger.warning('[CRM generate-email] failed: %s', exc)
        return jsonify({'error': 'Email generation failed.'}), 500


@app.post("/prospect/crm-save-verified-email")
@_require_session
def prospect_crm_save_verified_email():
    """Persist a confirmed email address into verified_email.json.

    Uses the LLM to infer the email format pattern and generate a normalised
    fake example, then recalculates per-domain confidence scores.
    Mirrors the /save-verified-email endpoint in server.js.
    """
    body     = request.get_json(silent=True) or {}
    emails   = body.get('emails') or []
    name     = str(body.get('name')    or '').strip()
    company  = str(body.get('company') or '').strip()

    if not isinstance(emails, list) or not emails:
        return jsonify({'error': 'emails array is required.'}), 400
    if not name or not company:
        return jsonify({'error': 'name and company are required.'}), 400

    try:
        with _verified_email_lock:
            existing    = _load_verified_email()
            company_key = re.sub(r'[^a-z0-9]', '_', company.lower())
            if company_key not in existing:
                existing[company_key] = {'Domain': [], 'Confidence_threshold': 1}
            company_data = existing[company_key]

            new_entries = []
            for email in emails:
                if not email or not isinstance(email, str):
                    continue
                at_idx = email.rfind('@')
                if at_idx < 0:
                    continue
                local_part = email[:at_idx]
                domain     = email[at_idx + 1:].lower()

                existing_entry = next(
                    (e for e in company_data['Domain'] if e.get('domain') == domain), None
                )
                if existing_entry:
                    existing_entry['count']    = (existing_entry.get('count') or 1) + 1
                    existing_entry['saved_at'] = datetime.utcnow().isoformat() + 'Z'
                    new_entries.append(existing_entry)
                    continue

                norm_prompt = (
                    f'You are an email format analyst. Given:\n'
                    f'- Real name: "{name}"\n'
                    f'- Company: "{company}"\n'
                    f'- Observed email local part: "{local_part}"\n'
                    f'- Domain: "{domain}"\n\n'
                    'Analyze the format used for the local part of the email. Then:\n'
                    '1. Identify the format pattern (e.g. "first_name.last_name", '
                    '"firstnamelastname", "f.lastname", "firstlastname" etc.)\n'
                    '2. Generate a completely fake example email using a generic made-up name '
                    '(NOT the real name) that follows the same format.\n'
                    '   The fake name must be realistic-sounding but entirely fictional '
                    '(e.g. "John Tan", "Oliver Chan").\n'
                    '3. Return ONLY a JSON object with these fields:\n'
                    '   {\n'
                    '     "format": "<pattern string>",\n'
                    f'     "fake_example": "<fake_local_part>@{domain}",\n'
                    '     "fake_local_part": "<fake_local_part_only>"\n'
                    '   }\n'
                    'No markdown, no explanation.'
                )

                fmt = local_part
                fake_example    = ''
                fake_local_part = ''
                try:
                    llm_text = (
                        unified_llm_call_text(norm_prompt, temperature=0.2, max_output_tokens=128)
                        or ''
                    ).strip()
                    json_str = re.sub(r'```(?:json)?', '', llm_text).strip()
                    parsed   = json.loads(json_str)
                    fmt             = parsed.get('format')        or local_part
                    fake_example    = parsed.get('fake_example')    or ''
                    fake_local_part = parsed.get('fake_local_part') or ''
                except Exception as _e:
                    logger.debug('[crm-save-verified-email] LLM normalisation failed (non-fatal): %s', _e)

                new_entry = {
                    'company':        company_key,
                    'domain':         domain,
                    'format':         fmt,
                    'fake_example':   fake_example,
                    'fake_local_part': fake_local_part,
                    'saved_at':       datetime.utcnow().isoformat() + 'Z',
                    'count':          1,
                    'confidence':     1,
                }
                company_data['Domain'].append(new_entry)
                new_entries.append(new_entry)

            _recalculate_confidences(company_data)
            if new_entries:
                _save_verified_email(existing)

        return jsonify({'ok': True, 'added': len(new_entries)}), 200

    except Exception as exc:
        logger.warning('[CRM save-verified-email] failed: %s', exc)
        return jsonify({'error': 'Failed to save verified email.'}), 500


JOBS = {}
JOBS_LOCK = threading.Lock()
PERSIST_JOBS_TO_FILES = os.getenv("PERSIST_JOBS_TO_FILES", "1") == "1"
JOB_FILE_PREFIX="job_"; JOB_FILE_SUFFIX=".json"
_USERNAME_SAFE_RE = re.compile(r'[^A-Za-z0-9_-]')

# ---------------------------------------------------------------------------
# Job state backend — file (default), gcs, or redis.
# JOB_BACKEND env var selects the backend.  GCS requires JOB_GCS_BUCKET.
# Redis uses the same REDIS_URL as the cache backend.
# ---------------------------------------------------------------------------
JOB_BACKEND = os.getenv("JOB_BACKEND", "file")   # "file" | "gcs" | "redis"
JOB_GCS_BUCKET = os.getenv("JOB_GCS_BUCKET", "")
_JOB_GCS_PREFIX = "jobs/"
_JOB_REDIS_TTL = int(os.getenv("JOB_REDIS_TTL_SECONDS", str(7 * 24 * 3600)))  # 7 days


def _job_file(job_id: str, username: str = "") -> str:
    safe_username = _USERNAME_SAFE_RE.sub('', username or "")
    suffix = f"_{safe_username}" if safe_username else ""
    return os.path.join(OUTPUT_DIR, f"{JOB_FILE_PREFIX}{job_id}{suffix}{JOB_FILE_SUFFIX}")


def _job_gcs_object(job_id: str) -> str:
    return f"{_JOB_GCS_PREFIX}{job_id}.json"


def _job_redis_key(job_id: str) -> str:
    return f"job:{job_id}"


def persist_job(job_id: str):
    """Persist job state to the configured backend (file / gcs / redis)."""
    if JOB_BACKEND == "gcs":
        if not JOB_GCS_BUCKET:
            return
        try:
            from google.cloud import storage as gcs  # type: ignore
            with JOBS_LOCK:
                job = JOBS.get(job_id)
                if not job:
                    return
                payload = json.dumps(job, ensure_ascii=False)
            client = gcs.Client()
            bucket = client.bucket(JOB_GCS_BUCKET)
            blob = bucket.blob(_job_gcs_object(job_id))
            blob.upload_from_string(payload, content_type="application/json")
        except Exception as exc:
            logger.warning(f"[Persist/GCS] {exc}")
        return

    if JOB_BACKEND == "redis":
        try:
            from cache_backend import _get_redis
            r = _get_redis()
            if r is None:
                return
            with JOBS_LOCK:
                job = JOBS.get(job_id)
                if not job:
                    return
                payload = json.dumps(job, ensure_ascii=False)
            r.setex(_job_redis_key(job_id), _JOB_REDIS_TTL, payload)
        except Exception as exc:
            logger.warning(f"[Persist/Redis] {exc}")
        return

    # Default: file backend
    if not PERSIST_JOBS_TO_FILES:
        return
    try:
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if not job:
                return
            username = job.get("username") or ""
            tmp = _job_file(job_id, username) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(job, f, ensure_ascii=False, indent=2)
            os.replace(tmp, _job_file(job_id, username))
    except Exception as exc:
        logger.warning(f"[Persist] {exc}")


def _load_job_from_backend(job_id: str) -> dict | None:
    """Load a job from the configured backend.  Returns None if not found."""
    if JOB_BACKEND == "gcs":
        if not JOB_GCS_BUCKET:
            return None
        try:
            from google.cloud import storage as gcs  # type: ignore
            client = gcs.Client()
            bucket = client.bucket(JOB_GCS_BUCKET)
            blob = bucket.blob(_job_gcs_object(job_id))
            if not blob.exists():
                return None
            return json.loads(blob.download_as_text())
        except Exception as exc:
            logger.warning(f"[LoadJob/GCS] {exc}")
            return None

    if JOB_BACKEND == "redis":
        try:
            from cache_backend import _get_redis
            r = _get_redis()
            if r is None:
                return None
            raw = r.get(_job_redis_key(job_id))
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as exc:
            logger.warning(f"[LoadJob/Redis] {exc}")
            return None

    # File backend — scan OUTPUT_DIR for matching job files
    try:
        for fname in os.listdir(OUTPUT_DIR):
            if fname.startswith(JOB_FILE_PREFIX) and fname.endswith(JOB_FILE_SUFFIX):
                if job_id in fname:
                    fpath = os.path.join(OUTPUT_DIR, fname)
                    with open(fpath, encoding="utf-8") as f:
                        return json.load(f)
    except Exception as exc:
        logger.warning(f"[LoadJob/File] {exc}")
    return None
def add_message(job_id: str, text: str):
    with JOBS_LOCK:
        job=JOBS.get(job_id)
        if not job: return
        job['messages'].append(text)
        job['status_html']="<br>".join(job['messages'][-12:])
    persist_job(job_id)

# ... [Job helper functions] ...
LINKEDIN_PROFILE_RE = re.compile(r'(?:^|\.)linkedin\.com/(?:in|pub)/', re.I)
CLEAN_LINKEDIN_SUFFIX_RE = re.compile(r'\s*\|\s*LinkedIn.*$', re.I)
MULTI_SPACE_RE = re.compile(r'\s+')

def is_linkedin_profile(url: str) -> bool:
    return bool(url and LINKEDIN_PROFILE_RE.search(url))

def parse_linkedin_title(title: str):
    if not title: return None, None, None
    cleaned=CLEAN_LINKEDIN_SUFFIX_RE.sub('', title).strip()
    cleaned=cleaned.replace('–','-').replace('—','-')
    if '-' not in cleaned: return None, None, None
    name_part, rest = cleaned.split('-', 1)
    name=name_part.strip()
    if len(name.split())>9 or len(name)<2: return None, None, None
    if not re.search(r'[A-Za-z]', name): return None, None, None
    rest=rest.strip()
    company=""; jobtitle=rest
    at_idx=rest.lower().find(" at ")
    if at_idx!=-1:
        jobtitle=rest[:at_idx].strip()
        company=rest[at_idx+4:].strip()
    name=MULTI_SPACE_RE.sub(' ',name)
    jobtitle=MULTI_SPACE_RE.sub(' ',jobtitle)
    company=MULTI_SPACE_RE.sub(' ',company)
    return name or None, jobtitle or None, company or None


_SERP_DESC_DELIMITERS = (' · ', ' | ', ' - ', ' • ', '\n', ',')

def _parse_linkedin_description(text: str):
    """Extract (job_title, company) from a LinkedIn SERP description/snippet.

    Handles patterns such as:
      "Sales Manager at Acme Corp · 500+ connections"
      "Head of Sales at TechCorp | LinkedIn"
      "View John's profile: Sales Director at Acme | 200 connections"
    Returns (job_title, company) strings, or (None, None) on failure.
    """
    if not text:
        return None, None
    # Strip trailing LinkedIn suffix before parsing
    text = CLEAN_LINKEDIN_SUFFIX_RE.sub('', text).strip()
    # If the text starts with "View ... profile" noise, skip that preamble
    _colon = text.find(':')
    if _colon != -1 and _colon < 60:
        text = text[_colon + 1:].strip()
    at_idx = text.lower().find(' at ')
    if at_idx == -1:
        return None, None
    jobtitle_part = text[:at_idx].strip()
    company_part = text[at_idx + 4:].strip()
    # Truncate company at the first recognised delimiter
    for delim in _SERP_DESC_DELIMITERS:
        idx = company_part.find(delim)
        if idx != -1:
            company_part = company_part[:idx].strip()
    # Sanity limits — avoid capturing whole paragraphs
    if not jobtitle_part or not company_part:
        return None, None
    if len(jobtitle_part) > 120 or len(company_part) > 120:
        return None, None
    jobtitle_part = MULTI_SPACE_RE.sub(' ', jobtitle_part)
    company_part  = MULTI_SPACE_RE.sub(' ', company_part)
    return jobtitle_part or None, company_part or None

# ── LLM provider adapters ─────────────────────────────────────────────────────

def openai_call_text(prompt: str, api_key: str, model: str = "gpt-4o-mini",
                     system_prompt: str = None,
                     temperature: float = None,
                     max_output_tokens: int = None) -> str | None:
    """Call OpenAI Chat Completions API; returns text content or None on failure."""
    try:
        import openai as _openai  # type: ignore
    except ImportError:
        logger.warning("[OpenAI] openai package not installed; pip install openai")
        return None
    try:
        client = _openai.OpenAI(api_key=api_key.strip())
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        kwargs: dict = {"model": model, "messages": messages}
        if temperature is not None:
            kwargs["temperature"] = temperature
        if max_output_tokens is not None:
            kwargs["max_tokens"] = max_output_tokens
        resp = client.chat.completions.create(**kwargs)
        return resp.choices[0].message.content
    except Exception as exc:
        logger.warning(f"[OpenAI] call failed: {exc}")
        return None


def anthropic_call_text(prompt: str, api_key: str, model: str = "claude-3-5-haiku-20241022",
                        system_prompt: str = None,
                        temperature: float = None,
                        max_output_tokens: int = None) -> str | None:
    """Call Anthropic Messages API; returns text content or None on failure."""
    try:
        import anthropic as _anthropic  # type: ignore
    except ImportError:
        logger.warning("[Anthropic] anthropic package not installed; pip install anthropic")
        return None
    try:
        client = _anthropic.Anthropic(api_key=api_key.strip())
        kwargs: dict = {"model": model,
                        "max_tokens": max_output_tokens if max_output_tokens is not None else 4096,
                        "messages": [{"role": "user", "content": prompt}]}
        if system_prompt:
            kwargs["system"] = system_prompt
        if temperature is not None:
            kwargs["temperature"] = temperature
        resp = client.messages.create(**kwargs)
        return resp.content[0].text
    except Exception as exc:
        logger.warning(f"[Anthropic] call failed: {exc}")
        return None


def gemini_call_text(prompt: str, api_key: str, model: str = "gemini-2.5-flash-lite",
                     temperature: float = None,
                     max_output_tokens: int = None) -> str | None:
    """Call Gemini GenerativeModel; returns text or None on failure."""
    try:
        import google.generativeai as _genai  # type: ignore
        _genai.configure(api_key=api_key.strip())
        m = _genai.GenerativeModel(model)
        gen_cfg = {}
        if temperature is not None:
            gen_cfg["temperature"] = temperature
        if max_output_tokens is not None:
            gen_cfg["max_output_tokens"] = max_output_tokens
        resp = m.generate_content(prompt, generation_config=gen_cfg if gen_cfg else None)
        return resp.text
    except Exception as exc:
        logger.warning(f"[Gemini] call failed: {exc}")
        return None


def unified_llm_call_text(prompt: str, system_prompt: str = None,
                           temperature: float = None,
                           max_output_tokens: int = None,
                           cache_key: str = None) -> str | None:
    """Route an LLM text call through the active provider from llm_provider_config.json.
    Priority: active_provider field → Gemini fallback.
    Returns the text response or None if no provider is configured / all fail.

    cache_key: optional stable string key.  When provided and the cache backend is
    available the result is read from / written to the cache (TTL = LLM_CACHE_TTL).
    Only pass cache_key for deterministic, non-user-specific prompts (e.g. sector
    suggestions, company extraction).  Never pass it for CV analysis or email generation.
    """
    if cache_key:
        cached = cache_get(cache_key)
        if cached is not None:
            return cached

    cfg = _load_llm_provider_config()
    active = cfg.get("active_provider", "gemini")

    if active == "openai":
        oai = cfg.get("openai", {})
        key = (oai.get("api_key") or "").strip()
        model = oai.get("model", "gpt-4o-mini")
        if key and oai.get("enabled") == "enabled":
            result = openai_call_text(prompt, key, model, system_prompt,
                                      temperature=temperature,
                                      max_output_tokens=max_output_tokens)
            if result is not None:
                if cache_key:
                    cache_set(cache_key, result, ttl=LLM_CACHE_TTL)
                return result

    if active == "anthropic":
        ant = cfg.get("anthropic", {})
        key = (ant.get("api_key") or "").strip()
        model = ant.get("model", "claude-3-5-haiku-20241022")
        if key and ant.get("enabled") == "enabled":
            result = anthropic_call_text(prompt, key, model, system_prompt,
                                         temperature=temperature,
                                         max_output_tokens=max_output_tokens)
            if result is not None:
                if cache_key:
                    cache_set(cache_key, result, ttl=LLM_CACHE_TTL)
                return result

    # Gemini path (default / fallback)
    gem = cfg.get("gemini", {})
    gem_key = (gem.get("api_key") or "").strip() or (GEMINI_API_KEY or "").strip()
    gem_model = gem.get("model", GEMINI_SUGGEST_MODEL)
    if gem_key:
        result = gemini_call_text(prompt, gem_key, gem_model,
                                  temperature=temperature,
                                  max_output_tokens=max_output_tokens)
        if result is not None and cache_key:
            cache_set(cache_key, result, ttl=LLM_CACHE_TTL)
        return result

    return None

def google_cse_search_page(query: str, api_key: str, cx: str, num: int, start_index: int, gl_hint: str = None):
    if not api_key or not cx: return [], 0
    endpoint="https://www.googleapis.com/customsearch/v1"
    params={"key":api_key,"cx":cx,"q":query,"num":min(num,10),"start":start_index}
    if gl_hint: params["gl"]=gl_hint
    try:
        r=requests.get(endpoint, params=params, timeout=30)
        r.raise_for_status()
        data=r.json()
        items=data.get("items",[]) or []
        total_str=(data.get("searchInformation") or {}).get("totalResults","0") or "0"
        try:
            estimated_total=int(str(total_str).replace(",",""))
        except (ValueError, TypeError):
            estimated_total=0
        out=[]
        for it in items:
            out.append({"link":it.get("link") or "","title":it.get("title") or "","snippet":it.get("snippet") or "","displayLink":it.get("displayLink") or ""})
        return out, estimated_total
    except Exception as e:
        logger.warning(f"[CSE] page fetch failed: {e}")
        return [], 0

def serper_search_page(query: str, api_key: str, num: int, gl_hint: str = None, page: int = 1):
    """Fetch one page of results from the Serper.dev Google Search API.

    Returns the same ``(results, estimated_total)`` tuple as
    ``google_cse_search_page`` so callers are interchangeable.
    Serper does not support cursor-based pagination via a start index; it
    uses a ``page`` parameter instead.  The caller (``unified_search_page``)
    manages page increments.
    """
    if not api_key:
        return [], 0
    endpoint = "https://google.serper.dev/search"
    payload = {"q": query, "num": min(num, 10), "page": page}
    if gl_hint:
        payload["gl"] = gl_hint
    try:
        r = requests.post(
            endpoint,
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        organic = data.get("organic") or []
        total_str = str((data.get("searchParameters") or {}).get("totalResults", "0") or "0")
        try:
            estimated_total = int(total_str.replace(",", ""))
        except (ValueError, TypeError):
            estimated_total = 0
        out = []
        for it in organic:
            out.append({
                "link": it.get("link") or "",
                "title": it.get("title") or "",
                "snippet": it.get("snippet") or "",
                "displayLink": it.get("displayLink") or (it.get("link") or ""),
            })
        return out, estimated_total
    except Exception as e:
        logger.warning(f"[Serper] page fetch failed: {e}")
        return [], 0

def linkedin_search_page(query: str, api_key: str, num: int, gl_hint: str = None, page: int = 1):
    """Fetch one page of LinkedIn search results via LinkedAPI.io.

    Returns the same ``(results, estimated_total)`` tuple as
    ``google_cse_search_page`` so callers are interchangeable.
    """
    if not api_key:
        return [], 0
    endpoint = "https://api.linkedapi.io/v1/search"
    payload = {"q": query, "num": min(num, 10), "page": page}
    if gl_hint:
        payload["gl"] = gl_hint
    try:
        r = requests.post(
            endpoint,
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        organic = data.get("organic") or data.get("results") or []
        total_str = str(data.get("totalResults", "0") or "0")
        try:
            estimated_total = int(total_str.replace(",", ""))
        except (ValueError, TypeError):
            estimated_total = 0
        out = []
        for it in organic:
            out.append({
                "link": it.get("link") or it.get("url") or "",
                "title": it.get("title") or it.get("name") or "",
                "snippet": it.get("snippet") or it.get("description") or "",
                "displayLink": it.get("displayLink") or (it.get("link") or it.get("url") or ""),
            })
        return out, estimated_total
    except Exception as e:
        logger.warning(f"[LinkedIn] page fetch failed: {e}")
        return [], 0

_APOLLO_SENIORITY_MAP = {
    "intern": "intern", "junior": "entry", "entry": "entry", "associate": "entry",
    "senior": "senior", "sr": "senior", "lead": "senior",
    "manager": "manager", "mgr": "manager",
    "director": "director", "dir": "director",
    "vp": "vp", "vice president": "vp",
    "head": "head",
    "c_suite": "c_suite", "ceo": "c_suite", "cto": "c_suite", "cfo": "c_suite",
    "chief": "c_suite", "partner": "partner", "founder": "founder", "owner": "owner",
}

# Synthetic placeholder used when running a single provider-API search
_PROVIDER_API_PLACEHOLDER = "provider_api_search"


def _infer_apollo_seniority(job_titles: list) -> list:
    """Return a deduplicated list of Apollo seniority codes inferred from job titles.

    Uses word-boundary matching to avoid false positives (e.g. 'assistance'
    should not match 'senior' or 'associate').
    """
    result = []
    for title in (job_titles or []):
        low = (title or "").lower()
        for kw, code in _APOLLO_SENIORITY_MAP.items():
            # Use word-boundary regex to prevent substring false-positives
            if re.search(r'\b' + re.escape(kw) + r'\b', low) and code not in result:
                result.append(code)
    return result


def _build_contactout_params_from_fields(job_titles: list, companies: list,
                                          country: str, keywords: str = "") -> dict:
    """Build a ContactOut People Search payload directly from form fields.

    Attempts Gemini-based mapping guided by ``contactout_query_schema.json``
    first.  Falls back to hardcoded mapping if the LLM call fails or returns
    nothing useful, ensuring searches always proceed even without an active
    LLM key.
    """
    # --- Gemini-based mapping (preferred) ---
    schema = _load_provider_query_schema("contactout")
    if schema:
        llm_params = _llm_map_fields_to_provider_params(
            "contactout", job_titles, companies, country, keywords, schema
        )
        if llm_params:
            # Remove pagination keys — those are controlled by the caller
            llm_params.pop("page", None)
            llm_params.pop("limit", None)
            logger.debug(f"[ContactOut] Using LLM-mapped params: {llm_params}")
            return llm_params

    # --- Hardcoded fallback ---
    params: dict = {}
    titles = [t for t in (job_titles or []) if t]
    if titles:
        params["job_title"] = titles
    comps = [c for c in (companies or []) if c]
    if comps:
        params["company"] = comps
    if country:
        params["location"] = [country]
    kw = (keywords or "").strip()
    if kw:
        params["keyword"] = kw
    logger.debug(f"[ContactOut] Built params from fields (hardcoded fallback): {params}")
    return params


def _load_provider_query_schema(provider: str) -> dict:
    """Load the JSON query parameter schema for a provider.

    Schema files live in the same directory as webbridge.py (BASE_DIR).
    Returns an empty dict if the file is missing or unreadable.
    """
    filename = f"{provider}_query_schema.json"
    schema_path = os.path.join(BASE_DIR, filename)
    try:
        with open(schema_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        logger.debug(f"[{provider}] Query schema file not found: {schema_path}")
        return {}
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(f"[{provider}] Query schema {filename} contains invalid JSON: {exc}")
        return {}
    except Exception as exc:
        logger.debug(f"[{provider}] Could not load query schema {filename}: {exc}")
        return {}


def _llm_map_fields_to_provider_params(provider: str, job_titles: list,
                                        companies: list, country: str,
                                        keywords: str, schema: dict) -> dict:
    """Use the active LLM (Gemini) to map AutoSourcing.html form fields to the
    provider's native query parameters, guided by the supplied JSON schema.

    Returns a dict of provider-native query params on success, or an empty dict
    if the LLM call fails so the caller can fall back to hardcoded mapping.
    """
    if not schema:
        return {}

    # Summarise schema param names + descriptions for the prompt
    schema_summary_parts = []
    params_spec = schema.get("parameters") or schema.get("query_parameters") or {}
    for key, spec in params_spec.items():
        if isinstance(spec, dict):
            desc = spec.get("description", "")
            typ = spec.get("type", "")
            src = spec.get("source_field", "")
            schema_summary_parts.append(
                f'  "{key}" ({typ}): {desc}'
                + (f' [from form field: {src}]' if src else '')
            )
        else:
            schema_summary_parts.append(f'  "{key}": {spec}')
    schema_summary = "\n".join(schema_summary_parts) if schema_summary_parts else json.dumps(params_spec, indent=2)

    provider_label = provider.capitalize()
    form_fields = {
        "jobTitles": job_titles or [],
        "companyNames": companies or [],
        "country": country or "",
        "keywords": (keywords or "").strip(),
    }

    prompt = (
        f"You are a search API parameter mapper. Convert the following AutoSourcing.html "
        f"form field values into a JSON object of query parameters for the {provider_label} "
        f"People Search API.\n\n"
        f"Available {provider_label} query parameters (with source_field hints where applicable):\n{schema_summary}\n\n"
        f"Form field values:\n{json.dumps(form_fields, indent=2)}\n\n"
        f"Rules:\n"
        f"1. Use the 'source_field' hints in the parameter list above to map each form field to the correct parameter.\n"
        f"2. Infer seniority levels from job titles where applicable (e.g. director → director, VP → vp, manager → manager).\n"
        f"3. Only include parameters that have non-empty values.\n"
        f"4. For Apollo, always include include_similar_titles: true when person_titles is set.\n"
        f"5. Return ONLY valid JSON — no explanation, no markdown fences.\n"
    )
    try:
        raw = unified_llm_call_text(prompt, temperature=0.0, max_output_tokens=512)
        if raw and raw.strip():
            cleaned = raw.strip().strip("`")
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned, flags=re.MULTILINE)
            cleaned = re.sub(r'\s*```$', '', cleaned, flags=re.MULTILINE)
            cleaned = cleaned.strip()
            if cleaned:
                mapped = json.loads(cleaned)
                if isinstance(mapped, dict) and mapped:
                    logger.info(f"[{provider_label}] LLM-mapped params from form fields: {mapped}")
                    return mapped
    except Exception as exc:
        logger.warning(f"[{provider_label}] LLM field-mapping failed: {exc}")
    return {}


def _build_apollo_params_from_fields(job_titles: list, companies: list,
                                      country: str, keywords: str = "") -> dict:
    """Build an Apollo People Search payload directly from form fields.

    Attempts Gemini-based mapping guided by ``apollo_query_schema.json`` first.
    Falls back to hardcoded mapping if the LLM call fails or returns nothing
    useful, ensuring searches always proceed even without an active LLM key.
    """
    # --- Gemini-based mapping (preferred) ---
    schema = _load_provider_query_schema("apollo")
    if schema:
        llm_params = _llm_map_fields_to_provider_params(
            "apollo", job_titles, companies, country, keywords, schema
        )
        if llm_params:
            # Always ensure include_similar_titles is set when person_titles present
            if llm_params.get("person_titles") and "include_similar_titles" not in llm_params:
                llm_params["include_similar_titles"] = True
            logger.debug(f"[Apollo] Using LLM-mapped params: {llm_params}")
            return llm_params

    # --- Hardcoded fallback ---
    params: dict = {}
    titles = [t for t in (job_titles or []) if t]
    if titles:
        params["person_titles"] = titles
        params["include_similar_titles"] = True
    if country:
        params["person_locations"] = [country]
    seniority = _infer_apollo_seniority(job_titles)
    if seniority:
        params["person_seniorities"] = seniority
    comps = [c for c in (companies or []) if c]
    if comps:
        params["organization_names"] = comps
    kw = (keywords or "").strip()
    if kw:
        params["q_keywords"] = kw
    logger.debug(f"[Apollo] Built params from fields (hardcoded fallback): {params}")
    return params


def _build_rocketreach_params_from_fields(job_titles: list, companies: list,
                                           country: str, keywords: str = "") -> dict:
    """Build a RocketReach Person Search query payload directly from form fields.

    Attempts Gemini-based mapping guided by ``rocketreach_query_schema.json``
    first.  Falls back to hardcoded mapping if the LLM call fails or returns
    nothing useful.
    """
    # --- Gemini-based mapping (preferred) ---
    schema = _load_provider_query_schema("rocketreach")
    if schema:
        llm_params = _llm_map_fields_to_provider_params(
            "rocketreach", job_titles, companies, country, keywords, schema
        )
        if llm_params:
            logger.debug(f"[RocketReach] Using LLM-mapped params: {llm_params}")
            return llm_params

    # --- Hardcoded fallback ---
    params: dict = {}
    titles = [t for t in (job_titles or []) if t]
    if titles:
        params["current_title"] = titles
    comps = [c for c in (companies or []) if c]
    if comps:
        params["current_employer"] = comps
    if country:
        params["location"] = [country]
    kw = (keywords or "").strip()
    if kw:
        params["keyword"] = kw
    logger.debug(f"[RocketReach] Built params from fields (hardcoded fallback): {params}")
    return params


def _translate_xray_to_contactout_params(query: str) -> dict:
    """Use the active LLM to translate a Google Xray search string into a
    ContactOut People Search API JSON payload.

    The function extracts recognised ContactOut filter fields (job_title, skills,
    company, location, keyword, education, industry) from the Xray boolean query.
    Falls back to a ``{"keyword": <cleaned_query>}`` payload on any failure.
    """
    prompt = (
        "You are a search-query translator. Convert the Google Xray boolean search "
        "query below into a JSON object suitable for the ContactOut People Search API "
        "(POST https://api.contactout.com/v1/people/search).\n\n"
        "Rules:\n"
        "1. Extract job titles into \"job_title\" (array of strings, max 50).\n"
        "2. Extract required skills / technologies into \"skills\" (array of strings, max 50).\n"
        "3. Extract company names into \"company\" (array of strings).\n"
        "4. Extract location hints into \"location\" (array of strings).\n"
        "5. Extract field-of-study / degree / school into \"education\" (array of strings).\n"
        "6. Extract industry keywords into \"industry\" (array of strings).\n"
        "7. Put any remaining meaningful keywords into \"keyword\" (single string).\n"
        "8. Omit fields for which no clear value can be found.\n"
        "9. Remove all 'site:' operators, '-intitle:', '-inurl:', and boolean connectors (AND/OR/NOT).\n"
        "10. Return ONLY valid JSON — no explanation, no markdown fences.\n\n"
        f"Google Xray query:\n{query}"
    )
    try:
        raw = unified_llm_call_text(prompt, temperature=0.0, max_output_tokens=512)
        if raw and raw.strip():
            cleaned = raw.strip().strip("`")
            # Remove any ```json``` fences
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned, flags=re.MULTILINE)
            cleaned = re.sub(r'\s*```$', '', cleaned, flags=re.MULTILINE)
            params = json.loads(cleaned.strip())
            if isinstance(params, dict):
                logger.info(f"[ContactOut] Xray→ContactOut params: {params}")
                return params
    except Exception as exc:
        logger.warning(f"[ContactOut] Xray translation failed: {exc}")
    # Fallback: use the raw query as a keyword search
    # Strip obvious Google Xray operators before falling back
    kw = re.sub(r'site:\S+', '', query, flags=re.I)
    kw = re.sub(r'-(?:intitle|inurl|intext|allinurl):\S*', '', kw, flags=re.I)
    kw = re.sub(r'\b(?:AND|OR|NOT)\b', ' ', kw)
    kw = re.sub(r'[()"]', ' ', kw)
    kw = re.sub(r'\s+', ' ', kw).strip()
    return {"keyword": kw} if kw else {}


def contactout_people_search_page(query: str, api_key: str, num: int = 10,
                                   gl_hint: str = None, page: int = 1,
                                   raw_params: dict = None):
    """Call the ContactOut People Search API and map results to the standard
    ``(results, estimated_total)`` format used by the other search adapters.

    Each result dict has the standard ``link``, ``title`` and ``snippet`` keys
    plus a ``_source`` key set to ``'contactout'`` to identify the origin.

    When ``raw_params`` is supplied (built from form fields), Xray translation
    is bypassed and the params are used directly.
    """
    if not api_key:
        raise ProviderSearchError("ContactOut API key is not configured. Add CONTACTOUT_API_KEY in admin_rate_limits.html → Contact Generation.")
    params = dict(raw_params) if raw_params else _translate_xray_to_contactout_params(query)
    params["page"] = page
    params.setdefault("limit", num)
    logger.info(f"[ContactOut] Calling people/search — page={page} limit={params.get('limit', num)} params_keys={list(params.keys())}")
    try:
        r = requests.post(
            "https://api.contactout.com/v1/people/search",
            headers={
                "token": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=params,
            timeout=30,
        )
        logger.info(f"[ContactOut] HTTP status: {r.status_code}")
        try:
            resp_body = r.json()
        except (ValueError, requests.exceptions.JSONDecodeError):
            resp_body = r.text[:500]
        logger.debug(f"[ContactOut] Response body: {resp_body}")
        if not r.ok:
            # Extract the specific error message from the API response
            if isinstance(resp_body, dict):
                api_msg = (resp_body.get("message") or resp_body.get("error")
                           or resp_body.get("detail") or str(resp_body))
            else:
                api_msg = str(resp_body)[:300]
            logger.error(f"[ContactOut] API error HTTP {r.status_code}: {api_msg}")
            raise ProviderSearchError(f"ContactOut API error (HTTP {r.status_code}): {api_msg}")
        data = resp_body if isinstance(resp_body, dict) else {}
        people_raw = data.get("people") or data.get("profiles") or data.get("results") or []
        if isinstance(people_raw, list):
            people = people_raw
        elif isinstance(people_raw, dict):
            # Two possible dict structures:
            # 1. Nested list under a standard key: {"profiles": [...], "data": [...]}
            # 2. URL-keyed dict:  {"https://linkedin.com/in/john": {...profile...}, ...}
            nested = (people_raw.get("profiles") or people_raw.get("data")
                      or people_raw.get("list"))
            if isinstance(nested, list):
                people = nested
                logger.info(
                    f"[ContactOut] 'people' field is a dict — extracted {len(people)} items "
                    f"from nested list"
                )
            else:
                # Keys are likely LinkedIn URLs; values are profile dicts.
                # Inject the URL as linkedin_url if the profile doesn't already have one.
                people = []
                for k, v in people_raw.items():
                    if isinstance(v, dict):
                        p = dict(v)
                        if not p.get("linkedin_url") and not p.get("linkedin"):
                            p["linkedin_url"] = k
                        people.append(p)
                logger.info(
                    f"[ContactOut] 'people' field is a URL-keyed dict — "
                    f"extracted {len(people)} profiles "
                    f"(sample keys: {list(people_raw.keys())[:3]})"
                )
        else:
            people = []
            if people_raw:
                logger.warning(
                    f"[ContactOut] Unexpected 'people' field type "
                    f"({type(people_raw).__name__}); treating as empty"
                )
        _total_hint = (
            data.get("total") or data.get("count")
            or (
                (people_raw.get("total") or people_raw.get("count"))
                if isinstance(people_raw, dict) else None
            )
            or len(people)
        )
        estimated_total = int(_total_hint)
        if not people and resp_body:
            _preview_keys = list(resp_body.keys()) if isinstance(resp_body, dict) else str(type(resp_body))
            logger.info(
                f"[ContactOut] Zero results — HTTP {r.status_code} "
                f"response top-level keys: {_preview_keys}"
            )
        out = []
        skipped = 0
        for person in people:
            linkedin_url = (
                person.get("linkedin_url")
                or person.get("linkedin")
                or ""
            )
            if not linkedin_url:
                skipped += 1
                continue
            name = (person.get("name") or person.get("full_name") or "").strip()
            title = (person.get("title") or person.get("headline") or "").strip()
            company_obj = person.get("company") or {}
            if isinstance(company_obj, dict):
                company = company_obj.get("name") or ""
            else:
                company = str(company_obj)
            # Format title string so parse_linkedin_title can extract fields
            if name and (title or company):
                formatted_title = name
                if title:
                    formatted_title += f" - {title}"
                if company:
                    formatted_title += f" at {company}"
            elif name:
                formatted_title = name
            else:
                formatted_title = title or ""
            snippet_parts = []
            if person.get("location"):
                snippet_parts.append(str(person["location"]))
            if title:
                snippet_parts.append(title)
            if company:
                snippet_parts.append(company)
            out.append({
                "link": linkedin_url,
                "title": formatted_title,
                "snippet": " | ".join(snippet_parts),
                "displayLink": "linkedin.com",
                "_source": "contactout",
            })
        if skipped:
            logger.info(f"[ContactOut] skipped {skipped}/{len(people)} profile(s) with no linkedin_url")
        logger.info(f"[ContactOut] Returned {len(out)} results (total≈{estimated_total})")
        return out, estimated_total
    except ProviderSearchError:
        raise
    except requests.exceptions.Timeout:
        logger.error("[ContactOut] Request timed out after 30 s")
        raise ProviderSearchError("ContactOut API request timed out after 30 seconds.")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"[ContactOut] Connection error: {e}")
        raise ProviderSearchError(f"ContactOut connection error: {e}")
    except Exception as e:
        logger.error(f"[ContactOut] Unexpected error during people search: {e}")
        raise ProviderSearchError(f"ContactOut people search failed: {e}")


def _translate_xray_to_apollo_params(query: str) -> dict:
    """Use the active LLM to translate a Google Xray search string into an
    Apollo People Search API query payload.

    Extracts recognised Apollo filter fields from the Xray boolean query.
    Falls back to a ``{"q_keywords": <cleaned_query>}`` payload on any failure.
    """
    prompt = (
        "You are a search-query translator. Convert the Google Xray boolean search "
        "query below into a JSON object suitable for the Apollo People Search API "
        "(POST https://api.apollo.io/api/v1/mixed_people/search).\n\n"
        "Rules:\n"
        "1. Extract job titles into \"person_titles\" (array of strings).\n"
        "2. Extract location hints into \"person_locations\" (array of strings).\n"
        "3. Extract seniority levels into \"person_seniorities\" (array of strings — "
        "valid values: owner, founder, c_suite, partner, vp, head, director, manager, "
        "senior, entry, intern).\n"
        "4. Extract company domain names (e.g. microsoft.com) into "
        "\"q_organization_domains_list\" (array of strings).\n"
        "5. Put any remaining meaningful keywords into \"q_keywords\" (single string).\n"
        "6. Omit fields for which no clear value can be found.\n"
        "7. Remove all 'site:' operators, '-intitle:', '-inurl:', and boolean connectors (AND/OR/NOT).\n"
        "8. Return ONLY valid JSON — no explanation, no markdown fences.\n\n"
        f"Google Xray query:\n{query}"
    )
    try:
        raw = unified_llm_call_text(prompt, temperature=0.0, max_output_tokens=512)
        if raw and raw.strip():
            cleaned = raw.strip().strip("`")
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned, flags=re.MULTILINE)
            cleaned = re.sub(r'\s*```$', '', cleaned, flags=re.MULTILINE)
            params = json.loads(cleaned.strip())
            if isinstance(params, dict):
                logger.info(f"[Apollo] Xray→Apollo params: {params}")
                return params
    except Exception as exc:
        logger.warning(f"[Apollo] Xray translation failed: {exc}")
    # Fallback: use the raw query as a keyword search
    kw = re.sub(r'site:\S+', '', query, flags=re.I)
    kw = re.sub(r'-(?:intitle|inurl|intext|allinurl):\S*', '', kw, flags=re.I)
    kw = re.sub(r'\b(?:AND|OR|NOT)\b', ' ', kw)
    kw = re.sub(r'[()"]', ' ', kw)
    kw = re.sub(r'\s+', ' ', kw).strip()
    return {"q_keywords": kw} if kw else {}


def apollo_people_search_page(query: str, api_key: str, num: int = 10,
                               gl_hint: str = None, page: int = 1,
                               raw_params: dict = None):
    """Call the Apollo People Search API and map results to the standard
    ``(results, estimated_total)`` format used by the other search adapters.

    Each result dict has the standard ``link``, ``title`` and ``snippet`` keys
    plus a ``_source`` key set to ``'apollo'`` to identify the origin.
    A ``_apollo_id`` key is also stored to support the download-profile endpoint.

    When ``raw_params`` is supplied (built from form fields), Xray translation
    is bypassed and the params are used directly.
    """
    if not api_key:
        raise ProviderSearchError("Apollo API key is not configured. Add APOLLO_API_KEY in admin_rate_limits.html → Contact Generation.")
    params = dict(raw_params) if raw_params else _translate_xray_to_apollo_params(query)
    params["page"] = page
    params["per_page"] = num
    logger.debug(f"[Apollo] Calling mixed_people/search — page={page} per_page={num} params={params}")
    try:
        r = requests.post(
            "https://api.apollo.io/api/v1/mixed_people/search",
            headers={
                "x-api-key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=params,
            timeout=30,
        )
        logger.info(f"[Apollo] HTTP status: {r.status_code}")
        try:
            resp_body = r.json()
        except (ValueError, requests.exceptions.JSONDecodeError):
            resp_body = r.text[:500]
        logger.debug(f"[Apollo] Response body: {resp_body}")
        if not r.ok:
            if isinstance(resp_body, dict):
                api_msg = (resp_body.get("message") or resp_body.get("error")
                           or resp_body.get("detail") or str(resp_body))
            else:
                api_msg = str(resp_body)[:300]
            logger.error(f"[Apollo] API error HTTP {r.status_code}: {api_msg}")
            raise ProviderSearchError(f"Apollo API error (HTTP {r.status_code}): {api_msg}")
        data = resp_body if isinstance(resp_body, dict) else {}
        people = data.get("people") or []
        estimated_total = int(data.get("total_entries") or data.get("total") or len(people))
        out = []
        skipped = 0
        for person in people:
            # Apollo results may expose linkedin_url directly or via organization
            linkedin_url = (
                person.get("linkedin_url")
                or person.get("linkedin")
                or ""
            )
            if not linkedin_url:
                skipped += 1
                continue
            first = (person.get("first_name") or "").strip()
            last_raw = (person.get("last_name") or person.get("last_name_obfuscated") or "").strip()
            name = f"{first} {last_raw}".strip()
            title = (person.get("title") or "").strip()
            org = person.get("organization") or {}
            company = (org.get("name") or "").strip() if isinstance(org, dict) else ""
            if name and (title or company):
                formatted_title = name
                if title:
                    formatted_title += f" - {title}"
                if company:
                    formatted_title += f" at {company}"
            elif name:
                formatted_title = name
            else:
                formatted_title = title or ""
            snippet_parts = []
            city = (person.get("city") or "").strip()
            state = (person.get("state") or "").strip()
            country = (person.get("country") or "").strip()
            location_parts = [p for p in [city, state, country] if p]
            if location_parts:
                snippet_parts.append(", ".join(location_parts))
            if title:
                snippet_parts.append(title)
            if company:
                snippet_parts.append(company)
            result = {
                "link": linkedin_url,
                "title": formatted_title,
                "snippet": " | ".join(snippet_parts),
                "displayLink": "linkedin.com",
                "_source": "apollo",
            }
            apollo_id = (person.get("id") or "").strip()
            if apollo_id:
                result["_apollo_id"] = apollo_id
            out.append(result)
        if skipped:
            logger.info(f"[Apollo] skipped {skipped}/{len(people)} profile(s) with no linkedin_url")
        logger.info(f"[Apollo] Returned {len(out)} results (total≈{estimated_total})")
        return out, estimated_total
    except ProviderSearchError:
        raise
    except requests.exceptions.Timeout:
        logger.error("[Apollo] Request timed out after 30 s")
        raise ProviderSearchError("Apollo API request timed out after 30 seconds.")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"[Apollo] Connection error: {e}")
        raise ProviderSearchError(f"Apollo connection error: {e}")
    except Exception as e:
        logger.error(f"[Apollo] Unexpected error during people search: {e}")
        raise ProviderSearchError(f"Apollo people search failed: {e}")


def _translate_xray_to_rocketreach_params(query: str) -> dict:
    """Use the active LLM to translate a Google Xray search string into a
    RocketReach People Search API query payload.

    Extracts recognised RocketReach filter fields from the Xray boolean query.
    Falls back to a ``{"keyword": <cleaned_query>}`` payload on any failure.
    """
    prompt = (
        "You are a search-query translator. Convert the Google Xray boolean search "
        "query below into a JSON object suitable for the RocketReach Person Search API "
        "(POST https://api.rocketreach.co/api/v2/person/search).\n\n"
        "Rules:\n"
        "1. Extract job titles into \"current_title\" (array of strings).\n"
        "2. Extract company names into \"current_employer\" (array of strings).\n"
        "3. Extract location hints into \"location\" (array of strings).\n"
        "4. Extract person names into \"name\" (array of strings).\n"
        "5. Put any remaining meaningful keywords into \"keyword\" (single string).\n"
        "6. Omit fields for which no clear value can be found.\n"
        "7. Remove all 'site:' operators, '-intitle:', '-inurl:', and boolean connectors (AND/OR/NOT).\n"
        "8. Return ONLY valid JSON — no explanation, no markdown fences.\n\n"
        f"Google Xray query:\n{query}"
    )
    try:
        raw = unified_llm_call_text(prompt, temperature=0.0, max_output_tokens=512)
        if raw and raw.strip():
            cleaned = raw.strip().strip("`")
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned, flags=re.MULTILINE)
            cleaned = re.sub(r'\s*```$', '', cleaned, flags=re.MULTILINE)
            params = json.loads(cleaned.strip())
            if isinstance(params, dict):
                logger.info(f"[RocketReach] Xray→RocketReach params: {params}")
                return params
    except Exception as exc:
        logger.warning(f"[RocketReach] Xray translation failed: {exc}")
    # Fallback: use the raw query as a keyword search
    kw = re.sub(r'site:\S+', '', query, flags=re.I)
    kw = re.sub(r'-(?:intitle|inurl|intext|allinurl):\S*', '', kw, flags=re.I)
    kw = re.sub(r'\b(?:AND|OR|NOT)\b', ' ', kw)
    kw = re.sub(r'[()"]', ' ', kw)
    kw = re.sub(r'\s+', ' ', kw).strip()
    return {"keyword": kw} if kw else {}


def rocketreach_people_search_page(query: str, api_key: str, num: int = 10,
                                    gl_hint: str = None, page: int = 1,
                                    raw_params: dict = None):
    """Call the RocketReach Person Search API and map results to the standard
    ``(results, estimated_total)`` format used by the other search adapters.

    Each result dict has the standard ``link``, ``title`` and ``snippet`` keys
    plus a ``_source`` key set to ``'rocketreach'`` to identify the origin.

    When ``raw_params`` is supplied (built from form fields), Xray translation
    is bypassed and the params are used directly.
    """
    if not api_key:
        raise ProviderSearchError("RocketReach API key is not configured. Add ROCKETREACH_API_KEY in admin_rate_limits.html → Contact Generation.")
    params = dict(raw_params) if raw_params else _translate_xray_to_rocketreach_params(query)
    body = {"query": params, "start": ((page - 1) * num) + 1, "page_size": num}
    logger.info(f"[RocketReach] Calling api/v2/person/search — page={page} page_size={num} query_keys={list(params.keys())}")
    try:
        r = requests.post(
            "https://api.rocketreach.co/api/v2/person/search",
            headers={
                "Api-Key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=body,
            timeout=30,
        )
        logger.info(f"[RocketReach] HTTP status: {r.status_code}")
        try:
            resp_body = r.json()
        except (ValueError, requests.exceptions.JSONDecodeError):
            resp_body = r.text[:500]
        logger.debug(f"[RocketReach] Response body: {resp_body}")
        if not r.ok:
            if isinstance(resp_body, dict):
                api_msg = (resp_body.get("message") or resp_body.get("error")
                           or resp_body.get("detail") or str(resp_body))
            else:
                api_msg = str(resp_body)[:300]
            logger.error(f"[RocketReach] API error HTTP {r.status_code}: {api_msg}")
            raise ProviderSearchError(f"RocketReach API error (HTTP {r.status_code}): {api_msg}")
        data = resp_body if isinstance(resp_body, dict) else {}
        people_raw = data.get("profiles") or data.get("people") or data.get("results") or []
        if isinstance(people_raw, list):
            people = people_raw
        elif isinstance(people_raw, dict):
            # Two possible dict structures:
            # 1. Nested list under a standard key: {"data": [...], "list": [...]}
            # 2. URL-keyed dict:  {"https://linkedin.com/in/john": {...profile...}, ...}
            nested = (people_raw.get("profiles") or people_raw.get("data")
                      or people_raw.get("list"))
            if isinstance(nested, list):
                people = nested
                logger.info(
                    f"[RocketReach] 'profiles' field is a dict — extracted {len(people)} items "
                    f"from nested list"
                )
            else:
                # Keys are likely LinkedIn URLs; values are profile dicts.
                people = []
                for k, v in people_raw.items():
                    if isinstance(v, dict):
                        p = dict(v)
                        if not p.get("linkedin_url") and not p.get("linkedin"):
                            p["linkedin_url"] = k
                        people.append(p)
                logger.info(
                    f"[RocketReach] 'profiles' field is a URL-keyed dict — "
                    f"extracted {len(people)} profiles "
                    f"(sample keys: {list(people_raw.keys())[:3]})"
                )
        else:
            people = []
            if people_raw:
                logger.warning(
                    f"[RocketReach] Unexpected 'profiles' field type "
                    f"({type(people_raw).__name__}); treating as empty"
                )
        if not people and resp_body:
            _preview_keys = list(resp_body.keys()) if isinstance(resp_body, dict) else str(type(resp_body))
            logger.info(
                f"[RocketReach] Zero results — HTTP {r.status_code} "
                f"response top-level keys: {_preview_keys}"
            )
        estimated_total = int(
            data.get("pagination", {}).get("total")
            or data.get("total")
            or len(people)
        )
        out = []
        skipped = 0
        for person in people:
            linkedin_url = (
                person.get("linkedin_url")
                or person.get("linkedin")
                or ""
            )
            if not linkedin_url:
                skipped += 1
                continue
            first = (person.get("first_name") or "").strip()
            last = (person.get("last_name") or "").strip()
            name = f"{first} {last}".strip() or (person.get("name") or "").strip()
            title = (person.get("current_title") or person.get("title") or "").strip()
            company = (person.get("current_employer") or person.get("company") or "").strip()
            if name and (title or company):
                formatted_title = name
                if title:
                    formatted_title += f" - {title}"
                if company:
                    formatted_title += f" at {company}"
            elif name:
                formatted_title = name
            else:
                formatted_title = title or ""
            snippet_parts = []
            location = (person.get("location") or person.get("city") or "").strip()
            if location:
                snippet_parts.append(location)
            if title:
                snippet_parts.append(title)
            if company:
                snippet_parts.append(company)
            rr_id = str(person.get("id") or "").strip()
            result = {
                "link": linkedin_url,
                "title": formatted_title,
                "snippet": " | ".join(snippet_parts),
                "displayLink": "linkedin.com",
                "_source": "rocketreach",
            }
            if rr_id:
                result["_rocketreach_id"] = rr_id
            out.append(result)
        if skipped:
            logger.info(f"[RocketReach] skipped {skipped}/{len(people)} profile(s) with no linkedin_url")
        logger.info(f"[RocketReach] Returned {len(out)} results (total≈{estimated_total})")
        return out, estimated_total
    except ProviderSearchError:
        raise
    except requests.exceptions.Timeout:
        logger.error("[RocketReach] Request timed out after 30 s")
        raise ProviderSearchError("RocketReach API request timed out after 30 seconds.")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"[RocketReach] Connection error: {e}")
        raise ProviderSearchError(f"RocketReach connection error: {e}")
    except Exception as e:
        logger.error(f"[RocketReach] Unexpected error during people search: {e}")
        raise ProviderSearchError(f"RocketReach people search failed: {e}")


def dataforseo_search_page(query: str, login: str, password: str, num: int = 10, gl_hint: str = None, page: int = 1):
    """Fetch one page of results from the DataforSEO Google Organic Live API.

    Uses HTTP Basic Auth (RFC 7617, UTF-8 encoded) with an explicit Authorization
    header to avoid any character-encoding issues with intermediary libraries.
    Returns the same ``(results, estimated_total)`` tuple as the other adapters
    so callers are interchangeable.  DataforSEO supports an ``offset`` parameter
    for pagination (0-based), which ``unified_search_page`` maps from ``start_index``.
    """
    import base64
    # Strip any accidental whitespace that could corrupt the auth header
    login = (login or "").strip()
    password = (password or "").strip()
    if not login or not password:
        return [], 0
    endpoint = "https://api.dataforseo.com/v3/serp/google/organic/live/regular"
    offset = max(0, (page - 1) * num)
    task = {
        "keyword": query,
        "language_code": "en",
        "location_code": 2840,
        "device": "desktop",
        "depth": min(num, 100),
        "offset": offset,
    }
    # Build Basic Auth header manually using UTF-8 (RFC 7617) to avoid
    # requests' internal latin-1 encoding which can corrupt non-ASCII chars
    credentials = base64.b64encode(f"{login}:{password}".encode("utf-8")).decode("ascii")
    try:
        r = requests.post(
            endpoint,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/json",
            },
            json=[task],
            timeout=30,
        )
        if not r.ok:
            # Log the response body so we can see DataforSEO's actual error message
            try:
                err_body = r.json()
            except Exception:
                err_body = r.text[:300]
            logger.warning(f"[DataforSEO] page fetch failed: {r.status_code} — {err_body}")
            return [], 0
        data = r.json()
        tasks = data.get("tasks") or []
        if not tasks:
            return [], 0
        task_result = (tasks[0].get("result") or [{}])[0]
        items = task_result.get("items") or []
        estimated_total = task_result.get("items_count") or 0
        out = []
        for it in items:
            if it.get("type") != "organic":
                continue
            out.append({
                "link": it.get("url") or "",
                "title": it.get("title") or "",
                "snippet": it.get("description") or "",
                "displayLink": it.get("domain") or (it.get("url") or ""),
            })
        return out, estimated_total
    except Exception as e:
        logger.warning(f"[DataforSEO] page fetch failed: {e}")
        return [], 0


# ---------------------------------------------------------------------------
# Xray → provider-native query translation
# ---------------------------------------------------------------------------
# Providers that natively accept Google-style Xray queries (site:, OR, AND,
# -intitle:, etc.) and can be passed the query string unchanged.
_XRAY_NATIVE_PROVIDERS = frozenset({"serper", "dataforseo", "google_cse"})


@lru_cache(maxsize=64)
def _translate_xray_for_provider(query: str, provider: str) -> str:
    """Translate a Google Xray search query into the syntax expected by *provider*.

    For providers in ``_XRAY_NATIVE_PROVIDERS`` the query is returned as-is.
    For all other providers (e.g. LinkedIn / LinkedAPI.io) the active LLM
    (Gemini / OpenAI / Anthropic via ``unified_llm_call_text``) is used to
    rewrite the query into a simpler keyword-based format that the provider
    API understands.

    Returns the translated query, or falls back to the original query on any
    LLM failure so searches are never silently skipped.
    """
    if provider in _XRAY_NATIVE_PROVIDERS:
        return query

    prompt = (
        "You are a search-query translator.  The user has a Google Xray boolean "
        "search string designed for Google Custom Search / Serper / DataforSEO.  "
        "Rewrite it into a simple keyword query suitable for a generic people-search "
        "API (like LinkedIn search).  "
        "Rules:\n"
        "1. Remove any 'site:' operators entirely.\n"
        "2. Replace 'AND' with a space.\n"
        "3. Keep OR groupings but simplify them — e.g. (\"Java Developer\" OR \"Java Engineer\") "
        "stays as (\"Java Developer\" OR \"Java Engineer\").\n"
        "4. Remove exclusions like -intitle:\"jobs\", -inurl:\"dir/\", and seniority "
        "exclusion blocks (e.g. -(\"intern\" OR \"junior\")).\n"
        "5. Preserve quoted exact-match phrases.\n"
        "6. Return ONLY the rewritten query string — no explanation, no markdown.\n\n"
        f"Google Xray query:\n{query}"
    )

    try:
        translated = unified_llm_call_text(
            prompt,
            temperature=0.0,
            max_output_tokens=512,
        )
        if translated and translated.strip():
            cleaned = translated.strip().strip("`")
            logger.info(f"[Search] Xray→{provider} translation: {query!r} → {cleaned!r}")
            return cleaned
        # LLM returned empty/whitespace — fall through to fallback
        logger.warning(f"[Search] Xray→{provider} translation returned empty; using original query")
    except Exception as exc:
        logger.warning(f"[Search] Xray→{provider} translation failed: {exc}")

    # Fallback: return original query so searches still run
    return query


def unified_search_page(query: str, num: int, start_index: int, gl_hint: str = None,
                        user_provider: str = None, user_serper_key: str = None,
                        user_dfs_login: str = None, user_dfs_password: str = None,
                        user_linkedin_key: str = None,
                        selected_provider: str = None,
                        raw_form_fields: dict = None):
    """Search wrapper that routes to the configured active provider.

    Per-user provider (from Option A service config) takes priority over the global
    admin config.  Priority: per-user Serper → per-user DataforSEO → admin Serper →
    admin DataforSEO → Google CSE (fallback).

    When ``selected_provider`` is set (from the AutoSourcing.html toggle), the admin
    config lookup is overridden to route to that specific provider instead.
    ``selected_provider='cse'`` (or any value not matching a known provider) forces
    Google CSE — the admin-configured provider auto-detection is skipped entirely.
    Pass ``selected_provider=None`` only when no explicit selection was made (legacy
    callers), which preserves the original auto-detect behaviour.

    When ``selected_provider`` is 'contactout', 'apollo', or 'rocketreach', the search
    is executed *directly* against that provider's API using ``raw_form_fields`` (if
    supplied) to build native params without any Xray translation.  No fallback to
    Google CSE is performed — a ``ProviderSearchError`` is raised if the key is missing
    or the API call fails.

    If the per-user provider returns zero results (suggesting key failure / exhausted
    credits), falls back to the admin config and sets
    ``_search_fallback_flag.used = True`` so callers can detect the fallback.

    For providers that do not natively support Google Xray syntax the query is
    first translated via the active LLM (see ``_translate_xray_for_provider``).

    Returns ``(results, estimated_total)`` identical to the individual adapters.
    Raises ``ProviderSearchError`` for ContactOut/Apollo/RocketReach failures.
    """
    page = max(1, ((start_index - 1) // max(num, 1)) + 1)

    # Per-user search provider takes priority over the global admin config.
    # Exception: when the user has explicitly selected 'cse' (Default CSE) from the
    # AutoSourcing toggle, per-user API provider keys are bypassed so that the request
    # always routes to Google CSE regardless of what the user has configured in their
    # Option A service settings.  Also bypass when selected_provider is any unrecognised
    # value (per docstring: 'any value not matching a known provider' forces CSE).
    # Contact/enrichment providers (contactout/apollo/rocketreach) must also bypass
    # per-user *search* keys — those providers are not search providers and should
    # never be pre-empted by a per-user Serper/DataForSEO/LinkedIn key.
    _known_search_providers = ('serper', 'dataforseo', 'linkedin')
    _run_user_provider = (not selected_provider) or (selected_provider in _known_search_providers)
    if _run_user_provider:
        if user_provider == 'serper' and user_serper_key:
            results, total = serper_search_page(query, user_serper_key, num, gl_hint=gl_hint, page=page)
            if results:
                return results, total
            # User key returned empty — fall back to admin config
            logger.warning(f"[Search] User Serper key returned 0 results for query={query!r}; falling back to admin config")
            _search_fallback_flag.used = True
        if user_provider == 'dataforseo' and user_dfs_login and user_dfs_password:
            results, total = dataforseo_search_page(query, user_dfs_login, user_dfs_password, num, gl_hint=gl_hint, page=page)
            if results:
                return results, total
            logger.warning(f"[Search] User DataforSEO key returned 0 results for query={query!r}; falling back to admin config")
            _search_fallback_flag.used = True
        if user_provider == 'linkedin' and user_linkedin_key:
            li_query = _translate_xray_for_provider(query, 'linkedin')
            results, total = linkedin_search_page(li_query, user_linkedin_key, num, gl_hint=gl_hint, page=page)
            if results:
                return results, total
            logger.warning(f"[Search] User LinkedIn key returned 0 results for query={query!r}; falling back to admin config")
            _search_fallback_flag.used = True

    cfg = _load_search_provider_config()
    ev_cfg = _load_email_verif_config()

    # When a specific provider is selected via the AutoSourcing toggle, route to
    # that provider directly (if admin has keys configured for it).
    if selected_provider == 'serper':
        serper_cfg = cfg.get("serper", {})
        serper_key = serper_cfg.get("api_key", "")
        if serper_key:
            return serper_search_page(query, serper_key, num, gl_hint=gl_hint, page=page)
    elif selected_provider == 'dataforseo':
        dfs_cfg = cfg.get("dataforseo", {})
        dfs_login = (dfs_cfg.get("login") or "").strip()
        dfs_password = (dfs_cfg.get("password") or "").strip()
        if dfs_login and dfs_password:
            return dataforseo_search_page(query, dfs_login, dfs_password, num, gl_hint=gl_hint, page=page)
    elif selected_provider == 'linkedin':
        li_cfg = cfg.get("linkedin", {})
        li_key = li_cfg.get("api_key", "")
        if li_key:
            li_query = _translate_xray_for_provider(query, 'linkedin')
            return linkedin_search_page(li_query, li_key, num, gl_hint=gl_hint, page=page)

    # ── Provider API searches (ContactOut / Apollo / RocketReach) ────────────
    # These providers are called DIRECTLY with native API params built from the
    # form fields.  NO fallback to Google CSE is permitted.  If the key is
    # missing or the API call fails, ProviderSearchError is raised so the caller
    # can surface the specific reason in the job status messages.
    elif selected_provider == 'contactout':
        _co_cfg = ev_cfg.get("contactout", {})
        co_key = (_co_cfg.get("api_key") or "").strip() if _co_cfg.get("enabled") == "enabled" else ""
        if not co_key:
            raise ProviderSearchError(
                "ContactOut API key is not configured. Enable ContactOut and add the API key in "
                "admin_rate_limits.html → Contact Generation, then re-run the search."
            )
        # Build params directly from form fields when available; otherwise
        # translate the Xray query via LLM as fallback.
        if raw_form_fields:
            raw_params = _build_contactout_params_from_fields(
                job_titles=raw_form_fields.get("jobTitles") or [],
                companies=raw_form_fields.get("companyNames") or [],
                country=raw_form_fields.get("country") or (gl_hint or ""),
                keywords=raw_form_fields.get("keywords") or "",
            )
        else:
            raw_params = None
        # Raises ProviderSearchError on API failure — no CSE fallback
        return contactout_people_search_page(query, co_key, num, gl_hint=gl_hint, page=page, raw_params=raw_params)

    elif selected_provider == 'apollo':
        _ap_cfg = ev_cfg.get("apollo", {})
        ap_key = (_ap_cfg.get("api_key") or "").strip() if _ap_cfg.get("enabled") == "enabled" else ""
        if not ap_key:
            raise ProviderSearchError(
                "Apollo API key is not configured. Enable Apollo and add the API key in "
                "admin_rate_limits.html → Contact Generation, then re-run the search."
            )
        if raw_form_fields:
            raw_params = _build_apollo_params_from_fields(
                job_titles=raw_form_fields.get("jobTitles") or [],
                companies=raw_form_fields.get("companyNames") or [],
                country=raw_form_fields.get("country") or (gl_hint or ""),
                keywords=raw_form_fields.get("keywords") or "",
            )
        else:
            raw_params = None
        # Raises ProviderSearchError on API failure — no CSE fallback
        return apollo_people_search_page(query, ap_key, num, gl_hint=gl_hint, page=page, raw_params=raw_params)

    elif selected_provider == 'rocketreach':
        _rr_cfg = ev_cfg.get("rocketreach", {})
        rr_key = (_rr_cfg.get("api_key") or "").strip() if _rr_cfg.get("enabled") == "enabled" else ""
        if not rr_key:
            raise ProviderSearchError(
                "RocketReach API key is not configured. Enable RocketReach and add the API key in "
                "admin_rate_limits.html → Contact Generation, then re-run the search."
            )
        if raw_form_fields:
            raw_params = _build_rocketreach_params_from_fields(
                job_titles=raw_form_fields.get("jobTitles") or [],
                companies=raw_form_fields.get("companyNames") or [],
                country=raw_form_fields.get("country") or (gl_hint or ""),
                keywords=raw_form_fields.get("keywords") or "",
            )
        else:
            raw_params = None
        # Raises ProviderSearchError on API failure — no CSE fallback
        return rocketreach_people_search_page(query, rr_key, num, gl_hint=gl_hint, page=page, raw_params=raw_params)

    # Auto-detect the admin-configured provider ONLY when no explicit selection was
    # made (selected_provider is None/empty).  When the user has explicitly chosen
    # a provider — including 'cse' for Default CSE — skip this block so we never
    # silently route to a different provider than what was selected.
    if not selected_provider:
        serper_cfg = cfg.get("serper", {})
        serper_key = serper_cfg.get("api_key", "")
        if serper_cfg.get("enabled", "disabled") == "enabled" and serper_key:
            return serper_search_page(query, serper_key, num, gl_hint=gl_hint, page=page)

        dfs_cfg = cfg.get("dataforseo", {})
        dfs_login = (dfs_cfg.get("login") or "").strip()
        dfs_password = (dfs_cfg.get("password") or "").strip()
        if dfs_cfg.get("enabled", "disabled") == "enabled" and dfs_login and dfs_password:
            return dataforseo_search_page(query, dfs_login, dfs_password, num, gl_hint=gl_hint, page=page)

        li_cfg = cfg.get("linkedin", {})
        li_key = li_cfg.get("api_key", "")
        if li_cfg.get("enabled", "disabled") == "enabled" and li_key:
            li_query = _translate_xray_for_provider(query, 'linkedin')
            return linkedin_search_page(li_query, li_key, num, gl_hint=gl_hint, page=page)

    # Fall back to Google CSE
    return google_cse_search_page(query, GOOGLE_CSE_API_KEY, GOOGLE_CSE_CX, num, start_index, gl_hint=gl_hint)

def _is_private_host(url: str) -> bool:
    """Return True if the URL resolves to a private/loopback/reserved IP — used to block SSRF."""
    import socket
    import ipaddress
    from urllib.parse import urlparse
    try:
        host = urlparse(url).hostname
        if not host:
            return True
        for addrinfo in socket.getaddrinfo(host, None):
            ip = ipaddress.ip_address(addrinfo[4][0])
            if (ip.is_private or ip.is_loopback or ip.is_reserved
                    or ip.is_link_local or ip.is_multicast):
                return True
        return False
    except Exception:
        return True


def get_linkedin_profile_picture(linkedin_url: str, display_name: str = None):
    """
    Retrieve LinkedIn profile picture URL using scraping and Google Custom Search.
    Returns profile picture URL or None if not found.

    Priority:
    1. Try to fetch og:image meta tag directly from LinkedIn profile
    2. Google CSE text search for the LinkedIn profile page — extract
       pagemap.cse_thumbnail or pagemap.metatags[og:image] (most reliable,
       Google caches the metadata even for authenticated pages)
    3. Google CSE image search as a last-resort fallback
    4. Return None if no valid image found

    Security Note: Validates LinkedIn URLs to prevent SSRF attacks.
    """
    if not linkedin_url:
        return None

    # SECURITY: Validate LinkedIn URL to prevent SSRF
    # Must be a valid LinkedIn profile URL
    if not re.match(r'^https?://([a-z]+\.)?linkedin\.com/in/[a-zA-Z0-9\-._~%]+/?$', linkedin_url, re.IGNORECASE):
        logger.warning(f"[Profile Pic] Invalid LinkedIn URL format: {linkedin_url}")
        return None

    profile_pic_url = None

    # Method 1: Try to fetch og:image meta tag directly from LinkedIn profile
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(linkedin_url, headers=headers, timeout=10)
        
        # Handle rate limiting and forbidden responses
        if response.status_code == 429:
            logger.warning(f"[Profile Pic] Rate limited by LinkedIn: {linkedin_url}")
            # Continue to fallback method
        elif response.status_code == 403:
            logger.warning(f"[Profile Pic] Forbidden by LinkedIn (may require auth): {linkedin_url}")
            # Continue to fallback method
        elif response.status_code == 200:
            # Parse HTML to find og:image meta tag
            # Note: LinkedIn may actively block scraping - this is best-effort
            og_image_match = re.search(r'<meta[^>]*property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']', response.text, re.IGNORECASE)
            if not og_image_match:
                # Try reverse order (content before property)
                og_image_match = re.search(r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:image["\']', response.text, re.IGNORECASE)
            
            if og_image_match:
                profile_pic_url = og_image_match.group(1)
                logger.info(f"[Profile Pic] Found og:image from LinkedIn profile: {profile_pic_url}")
                
                # Validate that it's not a placeholder or default image
                if profile_pic_url and not any(placeholder in profile_pic_url.lower() for placeholder in ['default', 'placeholder', 'ghost']):
                    return profile_pic_url
                else:
                    logger.info(f"[Profile Pic] og:image appears to be placeholder, trying fallback")
    except Exception as e:
        logger.warning(f"[Profile Pic] Failed to fetch og:image from LinkedIn (may be blocked): {e}")

    # Method 2 & 3: search fallback (Serper.dev, DataforSEO, or Google CSE)
    _search_cfg = _load_search_provider_config()
    _serper_cfg = _search_cfg.get("serper", {})
    _serper_active = _serper_cfg.get("enabled", "disabled") == "enabled" and bool(_serper_cfg.get("api_key"))
    _dfs_cfg = _search_cfg.get("dataforseo", {})
    _dfs_active = (_dfs_cfg.get("enabled", "disabled") == "enabled"
                   and bool(_dfs_cfg.get("login")) and bool(_dfs_cfg.get("password")))
    _cse_available = bool(GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX)
    if not profile_pic_url and (_serper_active or _dfs_active or _cse_available):
        try:
            from urllib.parse import urlparse

            # Extract URL slug (e.g. john-doe-12345)
            match = re.search(r'linkedin\.com/in/([^/?#]+)', linkedin_url)
            if not match:
                logger.warning(f"[Profile Pic] Could not extract profile slug from URL: {linkedin_url}")
                return None

            profile_slug = match.group(1).rstrip('/')

            # ── Method 2: Text search — Google caches pagemap metadata including og:image ──
            # This works even when LinkedIn requires login to view the profile page.
            # Build query: prefer display name (more specific), fall back to slug.
            def _run_text_search(query_str: str) -> str | None:
                """Run a unified text search and extract the best profile picture URL."""
                try:
                    items, _ = unified_search_page(query_str, 5, 1)
                    for item in items:
                        pagemap = item.get("pagemap", {})
                        # Priority: cse_thumbnail (Google's cached thumbnail — no auth needed)
                        thumbnails = pagemap.get("cse_thumbnail") or []
                        if thumbnails and thumbnails[0].get("src"):
                            src = thumbnails[0]["src"]
                            logger.info(f"[Profile Pic] cse_thumbnail found: {src}")
                            return src
                        # Fallback: og:image from metatags
                        for mt in pagemap.get("metatags", []):
                            og = mt.get("og:image") or mt.get("twitter:image")
                            if og:
                                logger.info(f"[Profile Pic] og:image found via metatags: {og}")
                                return og
                except Exception as exc:
                    logger.warning(f"[Profile Pic] text search failed ({query_str!r}): {exc}")
                return None

            # Try display name first if provided
            if display_name and display_name.strip():
                clean_name = display_name.strip()
                profile_pic_url = _run_text_search(f'"{clean_name}" site:linkedin.com/in')
            # Fall back to URL slug
            if not profile_pic_url:
                profile_pic_url = _run_text_search(f'site:linkedin.com/in "{profile_slug}"')

            # ── Method 3: Image search — last resort (Google CSE only) ──
            if not profile_pic_url and _cse_available and not _serper_active:
                try:
                    endpoint = "https://www.googleapis.com/customsearch/v1"
                    # Build image query: display name is more useful than URL slug here
                    img_query = (
                        f'"{display_name.strip()}" site:linkedin.com/in'
                        if display_name and display_name.strip()
                        else f'site:linkedin.com/in "{profile_slug}"'
                    )
                    params = {
                        "key": GOOGLE_CSE_API_KEY,
                        "cx": GOOGLE_CSE_CX,
                        "q": img_query,
                        "searchType": "image",
                        "num": 5,
                    }
                    r = requests.get(endpoint, params=params, timeout=15)
                    r.raise_for_status()
                    items = r.json().get("items", [])
                    for item in items:
                        image_url = item.get("link", "")
                        context_link = item.get("image", {}).get("contextLink", "")
                        if not image_url:
                            continue
                        # SECURITY: context link must be from linkedin.com
                        try:
                            parsed = urlparse(context_link)
                            if not (parsed.netloc == "linkedin.com" or parsed.netloc.endswith(".linkedin.com")):
                                continue
                        except Exception:
                            continue
                        # Prefer square-ish, reasonably sized images
                        width = item.get("image", {}).get("width", 0)
                        height = item.get("image", {}).get("height", 0)
                        if width and height:
                            aspect = width / height if height else 0
                            if 0.7 <= aspect <= 1.4 and width < 1200:
                                profile_pic_url = image_url
                                logger.info(f"[Profile Pic] Image search hit: {image_url}")
                                break
                    # Last resort: take first image result regardless of dimensions
                    if not profile_pic_url and items:
                        profile_pic_url = items[0].get("link")
                        logger.info(f"[Profile Pic] Using first image result: {profile_pic_url}")
                except Exception as exc:
                    logger.warning(f"[Profile Pic] CSE image search failed: {exc}")

        except Exception as e:
            logger.warning(f"[Profile Pic] Search fallback failed: {e}")

    # Final validation: ensure URL is not empty or broken
    if profile_pic_url:
        try:
            # SECURITY: reject URLs that resolve to private/loopback addresses (SSRF)
            if _is_private_host(profile_pic_url):
                logger.warning(f"[Profile Pic] SSRF: blocked private-host URL: {profile_pic_url}")
                return None
            # Quick HEAD request to verify image exists; allow redirects and treat 2xx/3xx as valid
            try:
                head_response = requests.head(profile_pic_url, timeout=8, allow_redirects=True)
                status = head_response.status_code
                if 200 <= status < 400:
                    return profile_pic_url
                # Some CDNs reject HEAD but serve GET — fall through to return URL if content-type ok
                if status == 405:
                    # Method Not Allowed: server does not support HEAD, trust CSE result
                    return profile_pic_url
                logger.warning(f"[Profile Pic] Image URL returned status {status}: {profile_pic_url}")
                return None
            except requests.exceptions.Timeout:
                # If HEAD times out the URL is likely unreachable; return None
                logger.warning(f"[Profile Pic] HEAD request timed out for: {profile_pic_url}")
                return None
        except Exception as e:
            logger.warning(f"[Profile Pic] Failed to validate image URL: {e}")
            return None
    
    return None

def fetch_image_bytes_from_url(image_url: str, max_size_mb=5):
    """
    Fetch image bytes from a URL and return as bytes suitable for bytea storage.
    Returns bytes or None if fetch failed or image too large.
    
    Args:
        image_url: The URL of the image to fetch
        max_size_mb: Maximum allowed image size in MB (default: 5MB)
    """
    if not image_url:
        return None
    
    try:
        # SECURITY: block SSRF — reject URLs that resolve to private/loopback addresses
        if _is_private_host(image_url):
            logger.warning(f"[Fetch Image Bytes] SSRF: blocked private-host URL: {image_url}")
            return None
        response = requests.get(image_url, timeout=15, stream=True)
        response.raise_for_status()
        
        # Check content type
        content_type = response.headers.get('Content-Type', '')
        if not content_type.startswith('image/'):
            logger.warning(f"[Fetch Image Bytes] Invalid content type: {content_type}")
            return None
        
        # Check content length
        content_length = response.headers.get('Content-Length')
        if content_length and int(content_length) > max_size_mb * 1024 * 1024:
            logger.warning(f"[Fetch Image Bytes] Image too large: {content_length} bytes")
            return None
        
        # Read the image data
        image_bytes = response.content
        
        # Verify size after download
        if len(image_bytes) > max_size_mb * 1024 * 1024:
            logger.warning(f"[Fetch Image Bytes] Downloaded image too large: {len(image_bytes)} bytes")
            return None
        
        logger.info(f"[Fetch Image Bytes] Successfully fetched {len(image_bytes)} bytes from {image_url}")
        return image_bytes
        
    except Exception as e:
        logger.warning(f"[Fetch Image Bytes] Failed to fetch image from {image_url}: {e}")
        return None

def _dedupe_links(records):
    seen=set(); out=[]
    for r in records:
        link=r.get("link")
        if not link or link in seen: continue
        seen.add(link); out.append(r)
    return out

def _infer_primary_job_title(job_titles):
    if job_titles and isinstance(job_titles,list) and job_titles:
        return job_titles[0]
    return ""

def _perform_cse_queries(job_id, queries, target_limit, country,
                         user_provider=None, user_serper_key=None,
                         user_dfs_login=None, user_dfs_password=None,
                         user_linkedin_key=None,
                         selected_provider=None,
                         raw_form_fields=None):
    results=[]
    # For provider-API searches we extract the country hint from the form fields
    # rather than from Xray URL patterns (no site: operator present).
    if selected_provider in ('contactout', 'apollo', 'rocketreach') and raw_form_fields:
        country_code_hint = None  # provider APIs accept full country names
    else:
        m_cc = re.search(r'site:([a-z]{2})\.linkedin\.com/in', " ".join(queries), re.I)
        country_code_hint = m_cc.group(1).lower() if m_cc else None

    # Determine active search provider label for job status messages.
    # Explicit CSE selection (or any unrecognised value) always wins — per-user
    # provider labels must never override an explicit 'cse' selection.
    # Contact/enrichment providers (contactout/apollo/rocketreach) must also prevent
    # per-user search key labels from overriding the chosen contact provider.
    _known_api_providers_set = frozenset(('serper', 'dataforseo', 'linkedin', 'contactout', 'apollo', 'rocketreach'))
    _cse_forced = (not selected_provider) is False and (selected_provider not in _known_api_providers_set)
    _contact_provider_selected = selected_provider in ('contactout', 'apollo', 'rocketreach')
    if not _cse_forced and not _contact_provider_selected and user_provider == 'serper' and user_serper_key:
        _provider_label = "Serper (user)"
    elif not _cse_forced and not _contact_provider_selected and user_provider == 'dataforseo' and user_dfs_login and user_dfs_password:
        _provider_label = "DataforSEO (user)"
    elif not _cse_forced and not _contact_provider_selected and user_provider == 'linkedin' and user_linkedin_key:
        _provider_label = "LinkedIn (user)"
    else:
        _sp = _load_search_provider_config()
        if selected_provider == 'serper':
            _provider_label = "Serper (selected)"
        elif selected_provider == 'dataforseo':
            _provider_label = "DataforSEO (selected)"
        elif selected_provider == 'linkedin':
            _provider_label = "LinkedIn (selected)"
        elif selected_provider == 'contactout':
            _provider_label = "ContactOut (selected)"
        elif selected_provider == 'apollo':
            _provider_label = "Apollo (selected)"
        elif selected_provider == 'rocketreach':
            _provider_label = "RocketReach (selected)"
        elif not selected_provider:
            # No explicit selection — auto-detect from admin config
            _serper_on = (
                _sp.get("serper", {}).get("enabled", "disabled") == "enabled"
                and bool(_sp.get("serper", {}).get("api_key"))
            )
            _dfs_on = (
                _sp.get("dataforseo", {}).get("enabled", "disabled") == "enabled"
                and bool(_sp.get("dataforseo", {}).get("login"))
                and bool(_sp.get("dataforseo", {}).get("password"))
            )
            _li_on = (
                _sp.get("linkedin", {}).get("enabled", "disabled") == "enabled"
                and bool(_sp.get("linkedin", {}).get("api_key"))
            )
            _provider_label = "CSE"
            if _serper_on:
                _provider_label = "Serper"
            elif _dfs_on:
                _provider_label = "DataforSEO"
            elif _li_on:
                _provider_label = "LinkedIn"
        else:
            # Explicit 'cse' selection (or any unrecognised value) → use CSE label
            _provider_label = "CSE"

    # Determine the effective provider for Xray-translation decisions.
    # When the user has explicitly selected 'cse' (or any value not matching a
    # known API provider), _eff_provider must remain None so that the Xray
    # query is never translated for an API provider that won't actually be used.
    # Contact providers (contactout/apollo/rocketreach) must also not allow a
    # per-user search key to set _eff_provider (which would translate the Xray
    # query for the wrong provider before passing it to the contact API).
    _eff_provider = None
    if not _cse_forced and not _contact_provider_selected and user_provider in ('serper', 'dataforseo', 'linkedin'):
        _eff_provider = user_provider
    elif selected_provider in ('serper', 'dataforseo', 'linkedin', 'contactout', 'apollo', 'rocketreach'):
        _eff_provider = selected_provider
    elif not selected_provider:
        # No explicit selection — auto-detect from admin config (legacy behaviour)
        _sp_cfg = _load_search_provider_config()
        for _p in ('serper', 'dataforseo', 'linkedin'):
            _pc = _sp_cfg.get(_p, {})
            if _pc.get("enabled", "disabled") == "enabled" and (
                _pc.get("api_key") or (_pc.get("login") and _pc.get("password"))
            ):
                _eff_provider = _p
                break
    # else: explicit 'cse' (or unknown) selection → _eff_provider stays None (no translation)

    # Provider API searches (ContactOut/Apollo/RocketReach) use native params
    # built from form fields — no Xray translation needed.
    _is_provider_api = selected_provider in ('contactout', 'apollo', 'rocketreach')
    _needs_translation = not _is_provider_api and _eff_provider and _eff_provider not in _XRAY_NATIVE_PROVIDERS

    global_collected = 0

    # For provider API searches, execute only a single logical search (the
    # provider returns paginated results on its own).  For Xray-based searches,
    # iterate over each generated query string as before.
    _query_list = queries
    if _is_provider_api:
        # Use a single synthetic entry so the loop fires exactly once.
        _query_list = [_PROVIDER_API_PLACEHOLDER]

    for q in _query_list:
        # Global stop-loss: target already reached, no need to fire more queries.
        still_needed = target_limit - global_collected
        if still_needed <= 0:
            add_message(job_id, f"Target reached: {global_collected}/{target_limit} — skipping remaining queries")
            break

        # Each query tries to collect however many are still needed to reach the
        # overall target, so shortfalls from earlier queries are automatically filled.
        gathered=0; start_index=1; pages_fetched=0
        effective_target = still_needed

        if _needs_translation:
            add_message(job_id, f"Translating Xray query for {_provider_label}…")

        if _is_provider_api:
            add_message(job_id, f"Running {_provider_label}: direct API search (target={effective_target})")
        else:
            add_message(job_id, f"Running {_provider_label}: {q} target={effective_target} (need {still_needed} more to reach {target_limit})")

        while gathered < effective_target:
            remaining = effective_target - gathered
            page_size = min(CSE_PAGE_SIZE, remaining)

            try:
                page, estimated_total = unified_search_page(
                    q, page_size, start_index, gl_hint=country_code_hint,
                    user_provider=user_provider, user_serper_key=user_serper_key,
                    user_dfs_login=user_dfs_login, user_dfs_password=user_dfs_password,
                    user_linkedin_key=user_linkedin_key,
                    selected_provider=selected_provider,
                    raw_form_fields=raw_form_fields,
                )
            except ProviderSearchError as pse:
                add_message(job_id, f"ERROR [{_provider_label}]: {pse}")
                logger.error(f"[_perform_cse_queries] ProviderSearchError for {_provider_label}: {pse}")
                return _dedupe_links(results)  # abort — no CSE fallback

            pages_fetched+=1

            # Per-query stop-loss: if provider reports fewer total results than we
            # are requesting from this query, cap the query target to what it says
            # is actually available.  This prevents wasting API quota on
            # pages that will always return empty.
            if pages_fetched == 1 and estimated_total > 0 and estimated_total < effective_target:
                effective_target = estimated_total
                add_message(job_id, f"  Stop-loss: {_provider_label} reports ~{estimated_total} results for this query — capping to {effective_target}")

            if not page:
                add_message(job_id, f"  No results page start={start_index}")
                break

            results.extend(page); gathered+=len(page); global_collected+=len(page)
            if len(page) < page_size: break
            start_index += len(page)

            # Safety break — prevents runaway pagination on unexpectedly large indices
            if pages_fetched >= 20: break
            time.sleep(CSE_PAGE_DELAY)

        add_message(job_id, f"{_provider_label} done (collected {gathered}). pages={pages_fetched}")
    return _dedupe_links(results)

def _infer_seniority_from_titles(job_titles):
    if not job_titles: return None
    joined=" ".join([t or "" for t in job_titles])
    # Coordinator always maps to Associate (Junior) — checked first to prevent misclassification
    if re.search(r"\bCoordinator\b", joined, flags=re.I): return "Associate"
    if re.search(r"\bAssociate\b", joined, flags=re.I): return "Associate"
    if re.search(r"\bManager\b", joined, flags=re.I): return "Manager"
    if re.search(r"\bDirector\b", joined, flags=re.I): return "Director"
    return None

_SPECIALS = "<>àÀáÁâÂãÃäÄåÅæÆçÇèÈéÉêÊëËìÌíÍîÎïÏðÐñÑòÒóÓôÔõÖøØùÙúÚûÛüÜýÝÿŸšŠžŽłŁßþÞœŒ~"
_SPECIALS_RE = re.compile("[" + re.escape(_SPECIALS) + "]")

def _sanitize_for_excel(val: str) -> str:
    if not isinstance(val, str): return val or ""
    try:
        import unicodedata
        s=unicodedata.normalize("NFKC", val)
    except Exception:
        s=val
    s=(s.replace("–","-").replace("—","-").replace("’","'").replace("‘","'").replace("“",'"').replace("”",'"'))
    s=_SPECIALS_RE.sub("", s)
    try:
        import unicodedata
        s="".join(ch for ch in s if unicodedata.category(ch)[0]!="C" and ch not in {"\u200b","\u200c","\u200d","\ufeff"})
    except Exception:
        s=s.replace("\u200b","").replace("\u200c","").replace("\u200d","").replace("\ufeff","")
    s=re.sub(r"\s+"," ",s).strip()
    if len(s)>512: s=s[:512]
    return s

def _aggregate_company_dropdown(meta):
    if not isinstance(meta, dict):
        return []
    user = meta.get('user_companies') or []
    auto = meta.get('auto_suggest_companies') or []
    sectors = meta.get('selected_sectors') or []
    languages = meta.get('languages') or []
    sector_companies=[]
    try:
        if sectors:
            norm=[]
            for s in sectors:
                if not isinstance(s,str): continue
                parts=[p.strip() for p in re.split(r'>', s) if p.strip()]
                norm.append(parts[-1] if parts else s)
            sector_payload=_heuristic_multi_sector(norm,"","",languages)
            sector_companies = sector_payload.get('company',{}).get('related',[]) if sector_payload else []
    except Exception as e:
        logger.warning(f"[Dropdown] Sector heuristic failed: {e}")
    merged=[]; seen=set()
    for source in (user, auto, sector_companies):
        for c in source:
            if not isinstance(c,str): continue
            t=c.strip()
            if not t: continue
            k=t.lower()
            if k in seen: continue
            seen.add(k); merged.append(t)
            if len(merged)>=200: break
        if len(merged)>=200: break
    return merged

def _extract_company_from_jobtitle(job_title_raw: str, existing_company: str, company_list):
    if not job_title_raw or existing_company:
        return existing_company, job_title_raw
    seps=r"[\s\-\|,/@]"
    candidates=sorted([c for c in (company_list or []) if isinstance(c,str) and c.strip()], key=lambda x: len(x), reverse=True)
    for comp in candidates:
        pat=re.compile(rf"(^|{seps}+)" rf"({re.escape(comp)})" rf"({seps}+|$)", re.IGNORECASE)
        m=pat.search(job_title_raw)
        if not m: continue
        start_company,end_company=m.span(2)
        cleaned=job_title_raw[:start_company]+job_title_raw[end_company:]
        cleaned=re.sub(rf"({seps}+)", " ", cleaned).strip(" -|,/@").strip()
        return comp, cleaned
    return existing_company, job_title_raw

def _gemini_extract_company_from_jobtitle(job_title_raw: str, candidates=None):
    if not job_title_raw: return None, job_title_raw
    try:
        context={"jobTitle": job_title_raw.strip(), "knownCandidates": (candidates or [])[:30]}
        prompt=("Extract inline employer/company from jobTitle strictly if present. "
                "Return JSON {\"company\":\"\",\"jobTitleWithoutCompany\":\"\"}. "
                f"INPUT:\n{json.dumps(context,ensure_ascii=False)}\nOUTPUT:")
        text = (unified_llm_call_text(
            prompt,
            cache_key="llm:jobtitle2company:" + hashlib.sha256(prompt.encode()).hexdigest(),
        ) or "").strip()
        start=text.find('{'); end=text.rfind('}')
        if start==-1 or end==-1 or end<=start: return None, job_title_raw
        obj=json.loads(text[start:end+1])
        company=(obj.get("company") or "").strip()
        jt_wo=(obj.get("jobTitleWithoutCompany") or "").strip()
        if not company: return None, job_title_raw
        if not jt_wo: jt_wo=job_title_raw
        return company, jt_wo
    except Exception as e:
        logger.warning(f"[Gemini Title->Company] {e}")
        return None, job_title_raw


# ---------------------------------------------------------------------------
# Criteria-file helpers — must be defined BEFORE `import webbridge_cv` because
# webbridge_cv.py imports these names from this module at its top level.
CRITERIA_OUTPUT_DIR = os.getenv(
    "CRITERIA_OUTPUT_DIR",
    r"F:\Recruiting Tools\Autosourcing\output\Criteras"
)


def _get_criteria_filepath(username, role_tag):
    """Return the full path for the criteria JSON file for the given user/role_tag.
    Returns None if either argument is empty.
    """
    username = (username or "").strip()
    role_tag = (role_tag or "").strip()
    if not username or not role_tag:
        return None
    safe_role = re.sub(r'[<>:"/\\|?*\.]', '_', role_tag).strip('_')
    safe_user = re.sub(r'[<>:"/\\|?*\.]', '_', username).strip('_')
    if not safe_role or not safe_user:
        return None
    return os.path.join(CRITERIA_OUTPUT_DIR, f"{safe_role} {safe_user}.json")


def _read_search_criteria(username, role_tag):
    """Load and return the criteria dict from the saved JSON file, or None if not found."""
    filepath = _get_criteria_filepath(username, role_tag)
    if not filepath:
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data.get("criteria") or None
    except FileNotFoundError:
        return None
    except Exception:
        logger.warning(f"[load_search_criteria] Failed to read {filepath}", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Import second-half routes (job runner, sourcing, CV processing, bulk assess).
# webbridge_cv.py is a sibling module that imports shared state from this file;
# the circular import is safe because all names below are defined before this
# import statement is reached.

# ---------------------------------------------------------------------------
# Import CV-processing routes.
# webbridge_cv.py imports shared state from webbridge (and webbridge_routes);
# the circular import is safe because all names are defined before this line.
import webbridge_cv  # registers routes with `app`
from webbridge_cv import _gemini_multi_sector, _core_assess_profile  # backward refs


# ---------------------------------------------------------------------------
# Import third-segment routes (static HTML, porting, external APIs,
# criteria and report endpoints).
# webbridge_routes2.py imports shared state from webbridge and webbridge_routes;
# the circular import is safe because all names above are defined before this
# import statement is reached.
import webbridge_routes2  # registers second-half routes with `app`

# Re-export _load_user_gp_cfg so that webbridge.py's existing
# `from webbridge_routes import (..., _load_user_gp_cfg)` continues to work.
from webbridge_routes2 import _load_user_gp_cfg
