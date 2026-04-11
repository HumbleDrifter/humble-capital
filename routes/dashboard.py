import os
from functools import wraps
from flask import Blueprint, render_template, request, redirect, session, url_for

dashboard_bp = Blueprint("dashboard", __name__)

API_SECRET = (
    os.getenv("INTERNAL_API_SECRET")
    or os.getenv("STATUS_SECRET")
    or os.getenv("WEBHOOK_SHARED_SECRET")
    or ""
)


def require_dashboard_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):

        if session.get("user_id"):
            return fn(*args, **kwargs)

        # fallback legacy secret access
        if API_SECRET:
            provided = (request.args.get("secret") or "").strip()
            if provided == API_SECRET:
                return fn(*args, **kwargs)

        return redirect(url_for("public.login", next=request.path))

    return wrapper


def _page_context(page_title):

    provided_secret = (request.args.get("secret") or "").strip()
    session_user = bool(session.get("user_id"))

    # Pass the API secret to authenticated dashboard sessions so secret-
    # protected API endpoints can be loaded through the existing authUrl helper.
    if session_user and API_SECRET:
        frontend_api_secret = API_SECRET
    elif (not session_user) and API_SECRET and provided_secret == API_SECRET:
        frontend_api_secret = provided_secret
    else:
        frontend_api_secret = ""

    return {
        "page_title": page_title,
        "api_secret": frontend_api_secret,
        "secret": provided_secret,
        "session_username": session.get("username", ""),
        "session_is_admin": int(session.get("is_admin", 0) or 0),
    }


@dashboard_bp.route("/dashboard", methods=["GET"])
@require_dashboard_auth
def dashboard():
    return render_template("app/dashboard.html", **_page_context("Dashboard"))


@dashboard_bp.route("/analytics", methods=["GET"])
@require_dashboard_auth
def analytics():
    return render_template("app/analytics.html", **_page_context("Portfolio"))


@dashboard_bp.route("/performance", methods=["GET"])
@require_dashboard_auth
def performance_page():
    return render_template("app/performance.html", **_page_context("Performance"))


@dashboard_bp.route("/backtest", methods=["GET"])
@require_dashboard_auth
def backtest_page():
    return render_template("app/backtest.html", **_page_context("Backtesting"))


@dashboard_bp.route("/portfolio-backtest", methods=["GET"])
@require_dashboard_auth
def portfolio_backtest_page():
    return render_template("app/portfolio_backtest.html", **_page_context("Portfolio Backtest"))


@dashboard_bp.route("/charts", methods=["GET"])
@require_dashboard_auth
def charts_page():
    return render_template("app/charts.html", **_page_context("Charts"))


@dashboard_bp.route("/algorithm", methods=["GET"])
@require_dashboard_auth
def algorithm_page():
    return render_template("app/algorithm.html", **_page_context("Algorithm"))


@dashboard_bp.route("/meme-rotation", methods=["GET"])
@require_dashboard_auth
def meme_rotation():
    return render_template("app/meme_rotation.html", **_page_context("Opportunities"))


@dashboard_bp.route("/trade-history", methods=["GET"])
@require_dashboard_auth
def trade_history():
    return render_template("app/trade_history.html", **_page_context("Activity"))


@dashboard_bp.route("/configuration", methods=["GET"])
@require_dashboard_auth
def configuration():
    return render_template("app/configuration.html", **_page_context("Automation"))


@dashboard_bp.route("/accounts", methods=["GET"])
@require_dashboard_auth
def accounts():
    return render_template("app/settings.html", **_page_context("Accounts"))


@dashboard_bp.route("/settings", methods=["GET"])
@require_dashboard_auth
def settings():
    return render_template("app/system_status.html", **_page_context("Settings"))


@dashboard_bp.route("/system-status", methods=["GET"])
@require_dashboard_auth
def system_status():
    return render_template("app/system_status.html", **_page_context("Settings"))
