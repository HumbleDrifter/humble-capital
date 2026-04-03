from flask import Flask
from dotenv import load_dotenv
from datetime import timedelta
import os

from routes.public import public_bp
from routes.webhook import webhook_bp
from routes.api import api_bp
from routes.dashboard import dashboard_bp

from workers.execution_queue import start_execution_worker
from storage import init_db, init_user_table

load_dotenv("/root/tradingbot/.env", override=True)


def create_app():
    app = Flask(__name__)

    app.secret_key = os.getenv("APP_SESSION_SECRET", "CHANGE_ME_TO_A_LONG_RANDOM_SECRET")

    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "1") == "1"
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

    init_db()
    init_user_table()
    start_execution_worker()

    app.register_blueprint(public_bp)
    app.register_blueprint(webhook_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(dashboard_bp)

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
