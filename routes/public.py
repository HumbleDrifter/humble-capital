import os
from flask import Blueprint, render_template, request, redirect, url_for, session

from storage import verify_user, update_last_login

public_bp = Blueprint("public", __name__)

API_SECRET = (
    os.getenv("INTERNAL_API_SECRET")
    or os.getenv("STATUS_SECRET")
    or os.getenv("WEBHOOK_SHARED_SECRET")
    or ""
)


@public_bp.route("/", methods=["GET"])
def home():

    host = (request.host or "").split(":")[0].lower()

    if host.startswith("app."):
        return redirect(url_for("public.login"))

    if host.startswith("webhook."):
        return "Webhook endpoint active", 200

    return render_template("public/home.html", page_title="Home")


@public_bp.route("/explore", methods=["GET"])
def explore():

    host = (request.host or "").split(":")[0].lower()

    if host.startswith("app."):
        return redirect(url_for("public.login"))

    if host.startswith("webhook."):
        return "Webhook endpoint active", 200

    return render_template("public/explore.html", page_title="Explore the Console")


@public_bp.route("/login", methods=["GET", "POST"])
def login():

    if session.get("user_id"):
        return redirect(url_for("dashboard.dashboard"))

    error = None
    host = (request.host or "").split(":")[0].lower()

    if host.startswith("webhook."):
        return redirect("https://app.humble-capital.com/login")

    if request.method == "POST":

        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        user = verify_user(username, password)

        if user:

            session.permanent = True
            session["user_id"] = int(user["id"])
            session["username"] = user["username"]
            session["is_admin"] = int(user["is_admin"] or 0)

            update_last_login(user["id"])

            next_url = (request.args.get("next") or "").strip()

            if next_url.startswith("/"):
                return redirect(next_url)

            return redirect(url_for("dashboard.dashboard"))

        error = "Invalid username or password."

    return render_template("public/login.html", page_title="Login", error=error)


@public_bp.route("/logout", methods=["GET"])
def logout():

    session.clear()

    return redirect(url_for("public.home"))
