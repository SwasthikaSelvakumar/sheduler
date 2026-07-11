from flask import Flask
from flask_jwt_extended import JWTManager
from flask_cors import CORS

from .config import Config
from .models import db


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    JWTManager(app)
    CORS(app)

    from .routes.auth import auth_bp
    from .routes.projects import projects_bp
    from .routes.queues import queues_bp
    from .routes.jobs import jobs_bp
    from .routes.workers import workers_bp

    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    app.register_blueprint(projects_bp, url_prefix="/api/projects")
    app.register_blueprint(queues_bp, url_prefix="/api/queues")
    app.register_blueprint(jobs_bp, url_prefix="/api/jobs")
    app.register_blueprint(workers_bp, url_prefix="/api/workers")

    with app.app_context():
        db.create_all()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
