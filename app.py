"""
Databricks App boilerplate:
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py
- Provides a support ticketing system with ticket and message management

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
from datetime import datetime

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("support-app")

app = Flask(__name__)
_w = WorkspaceClient()

TICKETS_TABLE_NAME = "support_tickets"
MESSAGES_TABLE_NAME = "ticket_messages"

# Valid ticket statuses
VALID_TICKET_STATUSES = ["open", "in-progress", "resolved", "closed"]
VALID_PRIORITIES = ["low", "medium", "high", "urgent"]


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
    Resolve the current user's email for ticket ownership.

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


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")
