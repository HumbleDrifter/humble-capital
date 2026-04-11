import os
from functools import wraps
from urllib.parse import urlencode

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


def _redirect_with_secret(path, hash_fragment=""):
    provided_secret = (request.args.get("secret") or "").strip()
    query = urlencode({"secret": provided_secret}) if provided_secret else ""
    target = path
    if query:
        target = f"{target}?{query}"
    if hash_fragment:
        target = f"{target}#{hash_fragment}"
    return redirect(target)


@dashboard_bp.route("/dashboard", methods=["GET"])
@require_dashboard_auth
def dashboard():
    return render_template("app/dashboard.html", **_page_context("Dashboard"))


@dashboard_bp.route("/portfolio", methods=["GET"])
@require_dashboard_auth
def portfolio_page():
    return render_template("app/portfolio.html", **_page_context("Portfolio"))


@dashboard_bp.route("/analytics", methods=["GET"])
@require_dashboard_auth
def analytics():
    if request.args.get("embed") == "1":
        return render_template("app/analytics.html", embed_mode=True, **_page_context("Portfolio Holdings"))
    return _redirect_with_secret("/portfolio")


@dashboard_bp.route("/performance", methods=["GET"])
@require_dashboard_auth
def performance_page():
    if request.args.get("embed") == "1":
        return render_template("app/performance.html", embed_mode=True, **_page_context("Portfolio Performance"))
    return _redirect_with_secret("/portfolio", "performance")


@dashboard_bp.route("/backtesting", methods=["GET"])
@require_dashboard_auth
def backtesting_page():
    return render_template("app/backtesting.html", **_page_context("Backtesting"))


@dashboard_bp.route("/backtest", methods=["GET"])
@require_dashboard_auth
def backtest_page():
    if request.args.get("embed") == "1":
        return render_template("app/backtest.html", embed_mode=True, **_page_context("Crypto Backtest"))
    return _redirect_with_secret("/backtesting", "crypto")


@dashboard_bp.route("/portfolio-backtest", methods=["GET"])
@require_dashboard_auth
def portfolio_backtest_page():
    if request.args.get("embed") == "1":
        return render_template("app/portfolio_backtest.html", embed_mode=True, **_page_context("Portfolio Backtest"))
    return _redirect_with_secret("/backtesting", "portfolio")


@dashboard_bp.route("/options", methods=["GET"])
@require_dashboard_auth
def options_page():
    return render_template("app/options.html", **_page_context("Options"))


@dashboard_bp.route("/algorithm", methods=["GET"])
@require_dashboard_auth
def algorithm_page():
    return render_template("app/algorithm.html", **_page_context("Algorithm"))


@dashboard_bp.route("/trading", methods=["GET"])
@require_dashboard_auth
def trading_page():
    return render_template("app/trading.html", **_page_context("Trading"))


@dashboard_bp.route("/meme-rotation", methods=["GET"])
@require_dashboard_auth
def meme_rotation():
    return _redirect_with_secret("/trading")


@dashboard_bp.route("/charts", methods=["GET"])
@require_dashboard_auth
def charts_page():
    return _redirect_with_secret("/trading", "charts")


@dashboard_bp.route("/activity", methods=["GET"])
@require_dashboard_auth
def activity_page():
    return render_template("app/trade_history.html", **_page_context("Activity"))


@dashboard_bp.route("/settings", methods=["GET"])
@require_dashboard_auth
def settings():
    return render_template("app/configuration.html", **_page_context("Settings"))


@dashboard_bp.route("/trade-history", methods=["GET"])
@require_dashboard_auth
def trade_history():
    return _redirect_with_secret("/activity")


@dashboard_bp.route("/configuration", methods=["GET"])
@require_dashboard_auth
def configuration():
    return _redirect_with_secret("/settings", "automation")


@dashboard_bp.route("/accounts", methods=["GET"])
@require_dashboard_auth
def accounts():
    return _redirect_with_secret("/settings", "accounts")


@dashboard_bp.route("/system-status", methods=["GET"])
@require_dashboard_auth
def system_status():
    return _redirect_with_secret("/settings", "system")


@dashboard_bp.route("/options-chart", methods=["GET"])
@require_dashboard_auth
def options_chart_page():
    if request.args.get("embed") == "1":
        return render_template("app/options_chart.html", embed_mode=True, **_page_context("Options Chart"))
    return _redirect_with_secret("/options", "charts")


@dashboard_bp.route("/options-strategy", methods=["GET"])
@require_dashboard_auth
def options_strategy_page():
    if request.args.get("embed") == "1":
        return render_template("app/options_strategy.html", embed_mode=True, **_page_context("Options Strategy"))
    return _redirect_with_secret("/options", "strategy")
