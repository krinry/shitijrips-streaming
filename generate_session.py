# Run once to generate session string
# python generate_session.py
from pyrogram import Client
import os
from dotenv import load_dotenv
load_dotenv()

with Client(
    "gen_session",
    api_id=int(os.environ["TELEGRAM_API_ID"]),
    api_hash=os.environ["TELEGRAM_API_HASH"],
) as app:
    session_string = app.export_session_string()
    print(f"\n✅ Session String:\n{session_string}\n")
    print("Add this to .env as TELEGRAM_SESSION_STRING=...")