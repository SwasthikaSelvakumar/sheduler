from flask import Blueprint, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token

from ..models import db, User

auth_bp = Blueprint("auth", __name__)


@auth_bp.post("/signup")
def signup():
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    password = data.get("password")
    name = data.get("name", "")

    if not email or not password:
        return jsonify(error="email and password are required"), 400

    if User.query.filter_by(email=email).first():
        return jsonify(error="a user with this email already exists"), 409

    user = User(email=email, password_hash=generate_password_hash(password), name=name)
    db.session.add(user)
    db.session.commit()

    token = create_access_token(identity=user.id)
    return jsonify(access_token=token, user_id=user.id), 201


@auth_bp.post("/login")
def login():
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    password = data.get("password")

    user = User.query.filter_by(email=email).first()
    if not user or not check_password_hash(user.password_hash, password or ""):
        return jsonify(error="invalid email or password"), 401

    token = create_access_token(identity=user.id)
    return jsonify(access_token=token, user_id=user.id), 200
