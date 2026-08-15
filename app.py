"""
Databricks App boilerplate:
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py
- Pulls data from the Massive API via massive_client.py and syncs it into Lakebase
- Provides a support ticketing system with ticket and message management

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
import re
from datetime import datetime

import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase
from massive_client import MassiveClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("massive-app")

app = Flask(__name__)
_w = WorkspaceClient()

TABLE_NAME = os.environ.get("MASSIVE_TABLE_NAME", "massive_records")
WATCHLIST_TABLE_NAME = os.environ.get("WATCHLIST_TABLE_NAME", "watchlist")
TICKETS_TABLE_NAME = "support_tickets"
MESSAGES_TABLE_NAME = "ticket_messages"

# Basic stock ticker shape check: 1-10 uppercase letters, with an optional
# ".X" or ".XX" share-class suffix (e.g. "BRK.B"). This rejects obviously
# malformed input before we even call the Massive API.
_TICKER_RE = re.compile(r"^[A-Z]{1,10}(\.[A-Z]{1,2})?$")

# Valid ticket statuses
VALID_TICKET_STATUSES = ["open", "in-progress", "resolved", "closed"]
VALID_PRIORITIES = ["low", "medium", "high", "urgent"]


def ensure_table():
    """Create the destination table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def ensure_watchlist_table():
    """Create the watchlist table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WATCHLIST_TABLE_NAME} (
            symbol TEXT NOT NULL,
            email TEXT NOT NULL,
            latest_price NUMERIC,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, email)
        )
        """
    )


def ensure_tickets_table():
    """Create the support tickets table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TICKETS_TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            subject TEXT NOT NULL,
            description TEXT NOT NULL,
            priority VARCHAR(10) NOT NULL DEFAULT 'medium',
            status VARCHAR(20) NOT NULL DEFAULT 'open',
            created_by TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def ensure_messages_table():
    """Create the ticket messages table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {MESSAGES_TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            ticket_id INTEGER NOT NULL REFERENCES {TICKETS_TABLE_NAME}(id) ON DELETE CASCADE,
            message TEXT NOT NULL,
            author TEXT NOT NULL,
            from_user BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def _current_user_email() -> str:
    """
    Resolve the current user's email so the watchlist can be personalized.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Support ticketing system UI."""
    return render_template("index.html")


@app.route("/records")
def list_records():
    """Read records already synced into Lakebase."""
    limit = int(request.args.get("limit", 100))
    rows = lakebase.run_query(
        f"SELECT id, payload, synced_at FROM {TABLE_NAME} ORDER BY synced_at DESC LIMIT %s",
        (limit,),
    )
    return jsonify(rows)


@app.route("/sync", methods=["POST"])
def sync_from_massive():
    """
    Pull data from the Massive API (paginated, potentially huge dataset) and
    upsert it into Lakebase in batches.
    """
    ensure_table()
    client = MassiveClient()

    path = request.json.get("path", "/records") if request.is_json else "/records"
    batch_size = int(request.args.get("batch_size", 500))

    batch = []
    total = 0
    for item in client.paginated_get(path):
        batch.append(item)
        if len(batch) >= batch_size:
            total += _upsert_batch(batch)
            batch = []

    if batch:
        total += _upsert_batch(batch)

    return jsonify({"synced": total})


@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    """Return the current user's watchlist symbols, with their last known price."""
    ensure_watchlist_table()
    email = _current_user_email()
    rows = lakebase.run_query(
        f"SELECT symbol, email, latest_price, updated_at FROM {WATCHLIST_TABLE_NAME} "
        f"WHERE email = %s ORDER BY symbol ASC",
        (email,),
    )
    return jsonify(rows)


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    """
    Fetch the latest price for a single stock symbol from Massive using
    exactly ONE API call (see MassiveClient.get_latest_price), then add/
    update that symbol on the watchlist in Lakebase.
    """
    ensure_watchlist_table()

    if request.is_json:
        symbol = request.json.get("symbol", "")
    else:
        symbol = request.form.get("symbol", "")

    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""

    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    client = MassiveClient()
    try:
        data = client.get_latest_price(symbol)  # <-- single API call, latest price only
    except requests.HTTPError:
        # Massive returns a 404/4xx for tickers it doesn't recognize.
        return jsonify({"error": f"Unknown ticker symbol: {symbol}"}), 400

    price = _extract_latest_price(data)
    if price is None:
        # No usable price in the response (e.g. delisted/invalid ticker
        # that still 200s with an empty result set) - don't add it.
        return jsonify({"error": f"No price data available for ticker: {symbol}"}), 400

    email = _current_user_email()

    lakebase.run_write(
        f"""
        INSERT INTO {WATCHLIST_TABLE_NAME} (symbol, email, latest_price, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (symbol, email) DO UPDATE
            SET latest_price = EXCLUDED.latest_price,
                updated_at = EXCLUDED.updated_at
        """,
        (symbol, email, price),
    )

    return jsonify({"symbol": symbol, "email": email, "latest_price": price})


# ============ Support Ticketing System Endpoints ============

@app.route("/tickets", methods=["GET"])
def list_tickets():
    """Get all support tickets for the current user, sorted by creation date."""
    ensure_tickets_table()
    email = _current_user_email()
    
    rows = lakebase.run_query(
        f"""
        SELECT id, subject, priority, status, created_by, created_at, updated_at
        FROM {TICKETS_TABLE_NAME}
        WHERE created_by = %s
        ORDER BY created_at DESC
        """,
        (email,),
    )
    
    # Convert rows to dictionaries with proper formatting
    tickets = []
    for row in rows:
        tickets.append({
            "id": row.get("id"),
            "subject": row.get("subject"),
            "priority": row.get("priority"),
            "status": row.get("status"),
            "created_by": row.get("created_by"),
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
        })
    
    return jsonify(tickets)


@app.route("/tickets", methods=["POST"])
def create_ticket():
    """Create a new support ticket."""
    ensure_tickets_table()
    ensure_messages_table()
    
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    
    subject = request.json.get("subject", "").strip()
    description = request.json.get("description", "").strip()
    priority = request.json.get("priority", "medium").lower()
    
    if not subject:
        return jsonify({"error": "Subject is required"}), 400
    if not description:
        return jsonify({"error": "Description is required"}), 400
    if priority not in VALID_PRIORITIES:
        return jsonify({"error": f"Invalid priority. Must be one of: {', '.join(VALID_PRIORITIES)}"}), 400
    
    email = _current_user_email()
    
    # Insert ticket
    ticket_id = None
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TICKETS_TABLE_NAME} (subject, description, priority, status, created_by)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (subject, description, priority, "open", email),
            )
            ticket_id = cur.fetchone()["id"]
            
            # Add initial message with the description
            cur.execute(
                f"""
                INSERT INTO {MESSAGES_TABLE_NAME} (ticket_id, message, author, from_user)
                VALUES (%s, %s, %s, %s)
                """,
                (ticket_id, description, email, True),
            )
            conn.commit()
    
    return jsonify({
        "id": ticket_id,
        "subject": subject,
        "priority": priority,
        "status": "open",
        "created_by": email,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }), 201


@app.route("/tickets/<int:ticket_id>", methods=["GET"])
def get_ticket_detail(ticket_id):
    """Get a specific ticket with all its messages."""
    ensure_tickets_table()
    ensure_messages_table()
    
    email = _current_user_email()
    
    # Get ticket details
    ticket_rows = lakebase.run_query(
        f"""
        SELECT id, subject, description, priority, status, created_by, created_at, updated_at
        FROM {TICKETS_TABLE_NAME}
        WHERE id = %s AND created_by = %s
        """,
        (ticket_id, email),
    )
    
    if not ticket_rows:
        return jsonify({"error": "Ticket not found"}), 404
    
    ticket = ticket_rows[0]
    
    # Get all messages for the ticket
    message_rows = lakebase.run_query(
        f"""
        SELECT id, message, author, from_user, created_at
        FROM {MESSAGES_TABLE_NAME}
        WHERE ticket_id = %s
        ORDER BY created_at ASC
        """,
        (ticket_id,),
    )
    
    messages = []
    for row in message_rows:
        messages.append({
            "id": row.get("id"),
            "message": row.get("message"),
            "author": row.get("author"),
            "from_user": row.get("from_user"),
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        })
    
    return jsonify({
        "id": ticket.get("id"),
        "subject": ticket.get("subject"),
        "description": ticket.get("description"),
        "priority": ticket.get("priority"),
        "status": ticket.get("status"),
        "created_by": ticket.get("created_by"),
        "created_at": ticket.get("created_at").isoformat() if ticket.get("created_at") else None,
        "updated_at": ticket.get("updated_at").isoformat() if ticket.get("updated_at") else None,
        "messages": messages,
    })


@app.route("/tickets/<int:ticket_id>/messages", methods=["POST"])
def add_ticket_message(ticket_id):
    """Add a message to an existing ticket and optionally update its status."""
    ensure_tickets_table()
    ensure_messages_table()
    
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    
    message = request.json.get("message", "").strip()
    new_status = request.json.get("status", "").strip().lower()
    
    if not message:
        return jsonify({"error": "Message is required"}), 400
    
    if new_status and new_status not in VALID_TICKET_STATUSES:
        return jsonify({"error": f"Invalid status. Must be one of: {', '.join(VALID_TICKET_STATUSES)}"}), 400
    
    email = _current_user_email()
    
    # Verify ticket exists and belongs to user
    ticket_rows = lakebase.run_query(
        f"SELECT id, status FROM {TICKETS_TABLE_NAME} WHERE id = %s AND created_by = %s",
        (ticket_id, email),
    )
    
    if not ticket_rows:
        return jsonify({"error": "Ticket not found"}), 404
    
    # Add message and optionally update status
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            # Add message
            cur.execute(
                f"""
                INSERT INTO {MESSAGES_TABLE_NAME} (ticket_id, message, author, from_user)
                VALUES (%s, %s, %s, %s)
                """,
                (ticket_id, message, email, True),
            )
            
            # Update status if provided
            if new_status:
                cur.execute(
                    f"""
                    UPDATE {TICKETS_TABLE_NAME}
                    SET status = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (new_status, ticket_id),
                )
            else:
                # Just update the timestamp
                cur.execute(
                    f"""
                    UPDATE {TICKETS_TABLE_NAME}
                    SET updated_at = now()
                    WHERE id = %s
                    """,
                    (ticket_id,),
                )
            
            conn.commit()
    
    return jsonify({
        "ticket_id": ticket_id,
        "message": message,
        "author": email,
        "status_updated": bool(new_status),
        "new_status": new_status if new_status else None,
    }), 201


def _extract_latest_price(data: dict) -> float | None:
    """Pull the trade price out of the Massive 'previous close' response shape.

    The /v2/aggs/ticker/{symbol}/prev endpoint returns "results" as a LIST
    containing a single aggregate bar (not a dict), e.g.:
        {"status": "OK", "resultsCount": 1, "results": [{"c": 148.845, ...}]}
    Previously this code treated "results" as a dict, so isinstance(results, dict)
    was always False for this endpoint's real shape and the price silently
    resolved to None. Unwrap the list here, and check "status"/"resultsCount"
    so invalid tickers (empty results) are detected instead of "succeeding"
    with a null price.

    Adjust the key lookup here if the real Massive API returns a different
    field name for the traded/close price.
    """
    if not isinstance(data, dict):
        return None
    if data.get("status") not in (None, "OK") or data.get("resultsCount") == 0:
        return None
    results = data.get("results", data)
    if isinstance(results, list):
        results = results[0] if results else None
    if isinstance(results, dict):
        for key in ("c", "p", "price", "last_price", "vw"):
            if key in results:
                return results[key]
    return None


def _upsert_batch(items: list[dict]) -> int:
    """Upsert a batch of Massive API items into Lakebase, one statement per row.

    For very large batches, consider psycopg2.extras.execute_values for
    higher throughput instead of per-row execute calls.
    """
    import json as _json

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (id, payload, synced_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                        SET payload = EXCLUDED.payload,
                            synced_at = EXCLUDED.synced_at
                    """,
                    (str(item.get("id")), _json.dumps(item)),
                )
                count += 1
            conn.commit()
    return count


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")
