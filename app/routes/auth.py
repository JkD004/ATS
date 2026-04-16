import random
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt, JWTError

from app.constants import SECRET_KEY, ALGORITHM
from app.database import db
from app.models import UserCreate
from app.utils import get_password_hash, verify_password, create_access_token, create_refresh_token, send_email

auth_router = APIRouter()


# Registration endpoint
@auth_router.post("/register", response_model=dict)
async def register(user: UserCreate):
    existing_user = await db["users"].find_one({"email": user.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="User already exists")
    hashed_password = await get_password_hash(user.password)
    otp = random.randint(1000, 9999)  # Generate a 4-digit OTP
    user_data = {
        "email": user.email,
        "password": hashed_password,
        "name": user.name,
        "mobile_number": user.mobile_number,
        "bin_threshold": 80.0,
        "role": "user",
        "created_at": datetime.utcnow(),
        "is_active": False,  # User is inactive until verified
        "otp": otp  # Store OTP in the database
    }
    await db["users"].insert_one(user_data)

    # Send OTP email
    email_body = f"""
    <h3>Hello {user.name},</h3>
    <p>Thank you for registering! Please verify your email using the following OTP:</p>
    <h2>{otp}</h2>
    """
    await send_email(to_email=user.email, subject="Verify your account", body=email_body)

    return {"message": "User registered successfully. Please check your email for the OTP."}


# Verify OTP endpoint and send back access and refresh tokens
@auth_router.post("/verify", response_model=dict)
async def verify_user(email: str, otp: int):
    # Fetch user from database
    user = await db["users"].find_one({"email": email})
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    if user['is_active']:
        raise HTTPException(status_code=400, detail="User already verified")

    if user.get('otp') is None:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")

    failed_attempts = user.get('otp_failed_attempts', 0)
    if failed_attempts >= 3:
        await db["users"].update_one({"email": email}, {"$set": {"otp": None, "otp_failed_attempts": 0}})
        raise HTTPException(status_code=429, detail="Too many failed attempts. Please request a new OTP.")

    if user['otp'] != otp:
        await db["users"].update_one({"email": email}, {"$inc": {"otp_failed_attempts": 1}})
        raise HTTPException(status_code=400, detail="Invalid OTP")

    # Mark the user as verified (active)
    await db["users"].update_one({"email": email}, {"$set": {"is_active": True, "otp": None}})
    # Create access and refresh tokens after verification
    access_token = create_access_token(data={"email": user["email"], "role": user["role"]})
    refresh_token = create_refresh_token(data={"email": user["email"], "role": user["role"]})

    return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}


@auth_router.post("/login", response_model=dict)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    # Look up the user by email or username
    db_user = await db["users"].find_one(
        {"$or": [{"email": form_data.username}, {"username": form_data.username}]}
    )

    if not db_user or not await verify_password(form_data.password, db_user["password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # Create the JWT tokens
    access_token = create_access_token(data={"email": db_user["email"], "role": db_user["role"]})
    refresh_token = create_refresh_token(data={"email": db_user["email"], "role": db_user["role"]})
    return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}


# Request password reset endpoint
@auth_router.post("/request-password-reset", response_model=dict)
async def request_password_reset(email: str):
    user = await db["users"].find_one({"email": email})
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Generate a 4-digit OTP
    otp = random.randint(1000, 9999)
    await db["users"].update_one({"email": email}, {"$set": {"otp": otp}})

    # Send reset OTP email
    email_body = f"""
    <h3>Hello {user['name']},</h3>
    <p>You requested a password reset. Please use the following OTP to reset your password:</p>
    <h2>{otp}</h2>
    """
    await send_email(to_email=user["email"], subject="Password Reset Request", body=email_body)

    return {"message": "Password reset OTP sent to your email."}


# Reset password endpoint
@auth_router.post("/reset-password", response_model=dict)
async def reset_password(email: str, otp: int, new_password: str):
    user = await db["users"].find_one({"email": email})
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    if user.get('otp') is None:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")

    failed_attempts = user.get('otp_failed_attempts', 0)
    if failed_attempts >= 3:
        await db["users"].update_one({"email": email}, {"$set": {"otp": None, "otp_failed_attempts": 0}})
        raise HTTPException(status_code=429, detail="Too many failed attempts. Please request a new OTP.")

    if user['otp'] != otp:
        await db["users"].update_one({"email": email}, {"$inc": {"otp_failed_attempts": 1}})
        raise HTTPException(status_code=400, detail="Invalid OTP")

    hashed_password = await get_password_hash(new_password)
    await db["users"].update_one(
        {"email": email}, {"$set": {"password": hashed_password, "otp": None, "is_active": True}})

    return {"message": "Password reset successfully."}


# Generate new access token using refresh token
@auth_router.post("/refresh-token", response_model=dict)
async def refresh_token(refresh_token: str):
    try:
        payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid token type")
        email: str = payload.get("email")
        if email is None:
            raise HTTPException(status_code=400, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = await db["users"].find_one({"email": email})
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Create a new access token
    access_token = create_access_token(data={"email": user["email"], "role": user["role"]})

    return {"access_token": access_token, "token_type": "bearer"}
