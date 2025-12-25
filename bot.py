import os
import sys
import re
import time
import json
import atexit
import shutil
import signal
import zipfile
import sqlite3
import psutil
import tempfile
import logging
import threading
import subprocess
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import telebot
from telebot import types

# --- Flask Keep Alive (Railway uses PORT) ---
from flask import Flask
from threading import Thread

app = Flask("")

@app.route("/")
def home():
    return "I'am Atx File Host"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

def keep_alive():
    t = Thread(target=run_flask, daemon=True)
    t.start()
    print("Flask Keep-Alive server started.")
# --- End Flask Keep Alive ---


# =========================
# CONFIG (Use ENV on Railway)
# =========================
TOKEN = os.environ.get("BOT_TOKEN", "PUT_YOUR_TOKEN_HERE")
OWNER_ID = int(os.environ.get("OWNER_ID", "6231439063"))
ADMIN_ID = int(os.environ.get("ADMIN_ID", str(OWNER_ID)))
YOUR_USERNAME = os.environ.get("YOUR_USERNAME", "@ahmed_snde")
UPDATE_CHANNEL = os.environ.get("UPDATE_CHANNEL", "t.me/paivak")

FREE_USER_LIMIT = int(os.environ.get("FREE_USER_LIMIT", "2"))
SUBSCRIBED_USER_LIMIT = int(os.environ.get("SUBSCRIBED_USER_LIMIT", "15"))
ADMIN_LIMIT = int(os.environ.get("ADMIN_LIMIT", "999"))
OWNER_LIMIT = float("inf")

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, "upload_bots")
IROTECH_DIR = os.path.join(BASE_DIR, "inf")
DATABASE_PATH = os.path.join(IROTECH_DIR, "bot_data.db")

os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN, threaded=True)

# Runtime memory
bot_scripts = {}            # {script_key: {...}}
user_subscriptions = {}     # {user_id: {'expiry': datetime}}
user_files = {}             # {user_id: [(file_name, file_type), ...]}
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID}
bot_locked = False

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bot")

DB_LOCK = threading.Lock()


# =========================
# DATABASE
# =========================
def init_db():
    logger.info(f"Initializing DB: {DATABASE_PATH}")
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS subscriptions
                 (user_id INTEGER PRIMARY KEY, expiry TEXT)""")

    c.execute("""CREATE TABLE IF NOT EXISTS user_files
                 (user_id INTEGER, file_name TEXT, file_type TEXT,
                  PRIMARY KEY (user_id, file_name))""")

    c.execute("""CREATE TABLE IF NOT EXISTS active_users
                 (user_id INTEGER PRIMARY KEY)""")

    c.execute("""CREATE TABLE IF NOT EXISTS admins
                 (user_id INTEGER PRIMARY KEY)""")

    # ✅ Pending approvals
    c.execute("""CREATE TABLE IF NOT EXISTS pending_approvals
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  chat_id INTEGER,
                  file_name TEXT,
                  file_type TEXT,
                  created_at TEXT)""")

    c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (OWNER_ID,))
    if ADMIN_ID != OWNER_ID:
        c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (ADMIN_ID,))

    conn.commit()
    conn.close()

def load_data():
    logger.info("Loading DB data into memory...")
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()

    # subscriptions
    c.execute("SELECT user_id, expiry FROM subscriptions")
    for user_id, expiry in c.fetchall():
        try:
            user_subscriptions[int(user_id)] = {"expiry": datetime.fromisoformat(expiry)}
        except Exception:
            pass

    # user files
    c.execute("SELECT user_id, file_name, file_type FROM user_files")
    for user_id, fn, ft in c.fetchall():
        user_id = int(user_id)
        user_files.setdefault(user_id, []).append((fn, ft))

    # active users
    c.execute("SELECT user_id FROM active_users")
    for (uid,) in c.fetchall():
        active_users.add(int(uid))

    # admins
    c.execute("SELECT user_id FROM admins")
    for (uid,) in c.fetchall():
        admin_ids.add(int(uid))

    conn.close()
    logger.info(f"Loaded: users={len(active_users)}, subs={len(user_subscriptions)}, admins={len(admin_ids)}")

init_db()
load_data()


# =========================
# DB OPERATIONS
# =========================
def add_active_user(user_id: int):
    active_users.add(user_id)
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO active_users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()

def save_user_file(user_id: int, file_name: str, file_type: str):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)",
            (user_id, file_name, file_type),
        )
        conn.commit()
        conn.close()

    user_files.setdefault(user_id, [])
    user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
    user_files[user_id].append((file_name, file_type))

def remove_user_file_db(user_id: int, file_name: str):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("DELETE FROM user_files WHERE user_id=? AND file_name=?", (user_id, file_name))
        conn.commit()
        conn.close()

    if user_id in user_files:
        user_files[user_id] = [x for x in user_files[user_id] if x[0] != file_name]
        if not user_files[user_id]:
            del user_files[user_id]

def add_pending_approval(user_id: int, chat_id: int, file_name: str, file_type: str) -> int:
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute(
            "INSERT INTO pending_approvals (user_id, chat_id, file_name, file_type, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, chat_id, file_name, file_type, datetime.now().isoformat()),
        )
        conn.commit()
        pid = c.lastrowid
        conn.close()
        return pid

def get_pending_approval(pending_id: int):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute(
            "SELECT id, user_id, chat_id, file_name, file_type FROM pending_approvals WHERE id=?",
            (pending_id,),
        )
        row = c.fetchone()
        conn.close()
        return row

def delete_pending_approval(pending_id: int):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("DELETE FROM pending_approvals WHERE id=?", (pending_id,))
        conn.commit()
        conn.close()


# =========================
# HELPERS
# =========================
def get_user_folder(user_id: int) -> str:
    p = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(p, exist_ok=True)
    return p

def get_user_file_limit(user_id: int):
    if user_id == OWNER_ID:
        return OWNER_LIMIT
    if user_id in admin_ids:
        return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id].get("expiry", datetime.min) > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id: int) -> int:
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id: int, file_name: str) -> bool:
    script_key = f"{script_owner_id}_{file_name}"
    info = bot_scripts.get(script_key)
    if not info or not info.get("process"):
        return False
    try:
        p = psutil.Process(info["process"].pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except Exception:
        # cleanup
        try:
            if info.get("log_file") and not info["log_file"].closed:
                info["log_file"].close()
        except Exception:
            pass
        bot_scripts.pop(script_key, None)
        return False

def kill_process_tree(process_info: dict):
    process = process_info.get("process")
    script_key = process_info.get("script_key", "N/A")
    try:
        if process_info.get("log_file") and hasattr(process_info["log_file"], "close") and not process_info["log_file"].closed:
            try:
                process_info["log_file"].close()
            except Exception:
                pass

        if not process or not hasattr(process, "pid"):
            return

        pid = process.pid
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for ch in children:
            try:
                ch.terminate()
            except Exception:
                try:
                    ch.kill()
                except Exception:
                    pass
        psutil.wait_procs(children, timeout=1)
        try:
            parent.terminate()
            parent.wait(timeout=1)
        except Exception:
            try:
                parent.kill()
            except Exception:
                pass

        logger.info(f"Killed process tree for {script_key} (PID {pid})")
    except Exception as e:
        logger.error(f"kill_process_tree error {script_key}: {e}", exc_info=True)


# =========================
# MENU / MARKUP
# =========================
def create_reply_keyboard_main_menu(user_id: int):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)

    base = [
        ["📢 Updates Channel"],
        ["📤 Upload File", "📂 Check Files"],
        ["⚡ Bot Speed", "📊 Statistics"],
        ["📞 @AHMED_SNDE"]
    ]
    admin_extra = [
        ["🔒 Lock Bot", "🟢 Running All Code"],
        ["📢 Broadcast"],
    ]

    rows = base + (admin_extra if user_id in admin_ids else [])
    for row in rows:
        markup.add(*[types.KeyboardButton(x) for x in row])
    return markup

def create_main_menu_inline(user_id: int):
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(types.InlineKeyboardButton("📢 Updates Channel", url=UPDATE_CHANNEL))
    m.add(types.InlineKeyboardButton("📤 Upload File", callback_data="upload"),
          types.InlineKeyboardButton("📂 Check Files", callback_data="check_files"))
    m.add(types.InlineKeyboardButton("⚡ Bot Speed", callback_data="speed"),
          types.InlineKeyboardButton("📊 Statistics", callback_data="stats"))
    if user_id in admin_ids:
        m.add(types.InlineKeyboardButton("🔒 Lock/Unlock", callback_data="toggle_lock"))
    m.add(types.InlineKeyboardButton("📞 @AHMED_SNDE", url=f"https://t.me/{YOUR_USERNAME.replace('@','')}"))
    return m

def create_control_buttons(script_owner_id: int, file_name: str, is_running: bool):
    m = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        m.add(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f"stop_{script_owner_id}_{file_name}"),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f"restart_{script_owner_id}_{file_name}")
        )
        m.add(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{script_owner_id}_{file_name}"),
            types.InlineKeyboardButton("📜 Logs", callback_data=f"logs_{script_owner_id}_{file_name}")
        )
    else:
        m.add(
            types.InlineKeyboardButton("🟢 Start", callback_data=f"start_{script_owner_id}_{file_name}"),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{script_owner_id}_{file_name}")
        )
        m.add(types.InlineKeyboardButton("📜 View Logs", callback_data=f"logs_{script_owner_id}_{file_name}"))
    m.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data="check_files"))
    return m

def approval_markup(pending_id: int):
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{pending_id}"),
        types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{pending_id}")
    )
    return m


# =========================
# RUNNERS
# =========================
def install_requirements_if_present(user_folder: str, message_obj):
    """
    If requirements.txt exists in user_folder, install it.
    (Only called AFTER owner approval)
    """
    req_path = os.path.join(user_folder, "requirements.txt")
    if not os.path.exists(req_path):
        return True

    try:
        bot.reply_to(message_obj, "📦 Installing requirements.txt ...")
        cmd = [sys.executable, "-m", "pip", "install", "-r", req_path]
        r = subprocess.run(cmd, cwd=user_folder, capture_output=True, text=True, encoding="utf-8", errors="ignore")
        if r.returncode != 0:
            err = (r.stderr or r.stdout or "")[:3500]
            bot.reply_to(message_obj, f"❌ requirements install failed:\n```\n{err}\n```", parse_mode="Markdown")
            return False
        bot.reply_to(message_obj, "✅ requirements installed.")
        return True
    except Exception as e:
        bot.reply_to(message_obj, f"❌ requirements install error: {e}")
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply):
    script_key = f"{script_owner_id}_{file_name}"
    try:
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = open(log_path, "w", encoding="utf-8", errors="ignore")

        process = subprocess.Popen(
            [sys.executable, script_path],
            cwd=user_folder,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore"
        )

        bot_scripts[script_key] = {
            "process": process,
            "log_file": log_file,
            "file_name": file_name,
            "script_owner_id": script_owner_id,
            "start_time": datetime.now(),
            "user_folder": user_folder,
            "type": "py",
            "script_key": script_key
        }
        bot.reply_to(message_obj_for_reply, f"✅ Started `{file_name}` (PID: {process.pid})", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message_obj_for_reply, f"❌ Start error: {e}")
        bot_scripts.pop(script_key, None)
        try:
            if "log_file" in locals() and log_file and not log_file.closed:
                log_file.close()
        except Exception:
            pass

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply):
    script_key = f"{script_owner_id}_{file_name}"
    try:
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = open(log_path, "w", encoding="utf-8", errors="ignore")

        process = subprocess.Popen(
            ["node", script_path],
            cwd=user_folder,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore"
        )

        bot_scripts[script_key] = {
            "process": process,
            "log_file": log_file,
            "file_name": file_name,
            "script_owner_id": script_owner_id,
            "start_time": datetime.now(),
            "user_folder": user_folder,
            "type": "js",
            "script_key": script_key
        }
        bot.reply_to(message_obj_for_reply, f"✅ Started `{file_name}` (PID: {process.pid})", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message_obj_for_reply, f"❌ Start error: {e}")
        bot_scripts.pop(script_key, None)
        try:
            if "log_file" in locals() and log_file and not log_file.closed:
                log_file.close()
        except Exception:
            pass


# =========================
# ZIP HANDLER (SAVE ONLY, PENDING APPROVAL)
# =========================
def handle_zip_file(downloaded_file_content: bytes, file_name_zip: str, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_folder = get_user_folder(user_id)

    temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
    try:
        zip_path = os.path.join(temp_dir, file_name_zip)
        with open(zip_path, "wb") as f:
            f.write(downloaded_file_content)

        with zipfile.ZipFile(zip_path, "r") as z:
            # zip slip protection
            for m in z.infolist():
                p = os.path.abspath(os.path.join(temp_dir, m.filename))
                if not p.startswith(os.path.abspath(temp_dir)):
                    raise zipfile.BadZipFile("Unsafe zip paths detected")
            z.extractall(temp_dir)

        # Detect main script
        items = os.listdir(temp_dir)
        py_files = [x for x in items if x.endswith(".py")]
        js_files = [x for x in items if x.endswith(".js")]

        main_script_name = None
        file_type = None
        for cand in ["main.py", "bot.py", "app.py"]:
            if cand in py_files:
                main_script_name, file_type = cand, "py"
                break
        if not main_script_name:
            for cand in ["index.js", "main.js", "bot.js", "app.js"]:
                if cand in js_files:
                    main_script_name, file_type = cand, "js"
                    break
        if not main_script_name:
            if py_files:
                main_script_name, file_type = py_files[0], "py"
            elif js_files:
                main_script_name, file_type = js_files[0], "js"

        if not main_script_name:
            bot.reply_to(message, "❌ ZIP has no .py file.")
            return

        # Move extracted to user folder (overwrite)
        for item in os.listdir(temp_dir):
            if item == file_name_zip:
                continue
            src = os.path.join(temp_dir, item)
            dst = os.path.join(user_folder, item)
            if os.path.isdir(dst):
                shutil.rmtree(dst)
            elif os.path.exists(dst):
                os.remove(dst)
            shutil.move(src, dst)

        # Save file record (but DO NOT run)
        save_user_file(user_id, main_script_name, file_type)
        pending_id = add_pending_approval(user_id, chat_id, main_script_name, file_type)

        bot.reply_to(
            message,
            f"✅ ZIP extracted.\n⏳ Waiting for OWNER approval to run `{main_script_name}`.",
            parse_mode="Markdown"
        )

        # Notify owner
        try:
            owner_text = (
                f"🛑 ZIP Approval Required\n\n"
                f"👤 User: {message.from_user.first_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"📦 ZIP: `{file_name_zip}`\n"
                f"▶️ Main: `{main_script_name}` ({file_type})\n\n"
                f"Approve or Reject:"
            )
            bot.send_message(OWNER_ID, owner_text, parse_mode="Markdown", reply_markup=approval_markup(pending_id))
            bot.forward_message(OWNER_ID, chat_id, message.message_id)
        except Exception as e:
            logger.error(f"Owner notify zip failed: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"ZIP error: {e}", exc_info=True)
        bot.reply_to(message, f"❌ ZIP error: {e}")
    finally:
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass


# =========================
# CORE LOGIC
# =========================
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    user_username = message.from_user.username

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ Bot is locked by admin. Try later.")
        return

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            bot.send_message(
                OWNER_ID,
                f"🎉 New user!\n👤 Name: {user_name}\n✳️ User: @{user_username or 'N/A'}\n🆔 ID: `{user_id}`",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float("inf") else "Unlimited"

    status = "🆓 Free User"
    if user_id == OWNER_ID:
        status = "👑 Owner"
    elif user_id in admin_ids:
        status = "🛡️ Admin"
    elif user_id in user_subscriptions and user_subscriptions[user_id].get("expiry", datetime.min) > datetime.now():
        status = "⭐ Premium"

    text = (
        f"〽️ Welcome, {user_name}!\n\n"
        f"🆔 Your ID: `{user_id}`\n"
        f"✳️ Username: `@{user_username or 'Not set'}`\n"
        f"🔰 Status: {status}\n"
        f"📁 Files: {current_files} / {limit_str}\n\n"
        f"✅ Upload .py/.js/.zip\n"
        f"🛑 Files run only after OWNER approval.\n"
    )

    bot.send_message(chat_id, text, reply_markup=create_reply_keyboard_main_menu(user_id), parse_mode="Markdown")

def _logic_upload_file(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked.")
        return
    file_limit = get_user_file_limit(user_id)
    if get_user_file_count(user_id) >= file_limit:
        bot.reply_to(message, "⚠️ File limit reached. Delete some files first.")
        return
    bot.reply_to(message, "📤 Send `.py` / `.zip` file.\n🛑 It will wait for OWNER approval.")

def _logic_check_files(message):
    user_id = message.from_user.id
    files = user_files.get(user_id, [])
    if not files:
        bot.reply_to(message, "📂 No files uploaded yet.")
        return
    m = types.InlineKeyboardMarkup(row_width=1)
    for fn, ft in sorted(files):
        running = is_bot_running(user_id, fn)
        icon = "🟢 Running" if running else "🔴 Stopped"
        m.add(types.InlineKeyboardButton(f"{fn} ({ft}) - {icon}", callback_data=f"file_{user_id}_{fn}"))
    bot.reply_to(message, "📂 Your files:", reply_markup=m)

def _logic_bot_speed(message):
    t0 = time.time()
    msg = bot.reply_to(message, "🏃 Testing...")
    ms = round((time.time() - t0) * 1000, 2)
    bot.edit_message_text(f"⚡ Pong: {ms} ms", message.chat.id, msg.message_id)

def _logic_statistics(message):
    user_id = message.from_user.id
    total_users = len(active_users)
    total_files = sum(len(v) for v in user_files.values())
    running = sum(1 for k, v in list(bot_scripts.items()) if is_bot_running(int(k.split("_", 1)[0]), v["file_name"]))
    your_running = sum(1 for fn, ft in user_files.get(user_id, []) if is_bot_running(user_id, fn))
    bot.reply_to(
        message,
        f"📊 Stats\n\n👥 Users: {total_users}\n📂 Files: {total_files}\n🟢 Running bots: {running}\n🤖 Your running: {your_running}"
    )

def _logic_contact_owner(message):
    m = types.InlineKeyboardMarkup()
    m.add(types.InlineKeyboardButton("📞 @AHMED_SNDE", url=f"https://t.me/{YOUR_USERNAME.replace('@','')}"))
    bot.reply_to(message, "Contact:", reply_markup=m)

def _logic_toggle_lock_bot(message):
    global bot_locked
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin only.")
        return
    bot_locked = not bot_locked
    bot.reply_to(message, f"🔒 Locked" if bot_locked else "🔓 Unlocked")


BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": lambda m: bot.reply_to(m, "Updates:", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("📢 Channel", url=UPDATE_CHANNEL))),
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📊 Statistics": _logic_statistics,
    "📞 @AHMED_SNDE": _logic_contact_owner,
    "🔒 Lock Bot": _logic_toggle_lock_bot,
}


# =========================
# APPROVE / REJECT CALLBACKS
# =========================
def approve_pending_callback(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "Owner only.", show_alert=True)
        return

    pending_id = int(call.data.split("_", 1)[1])
    row = get_pending_approval(pending_id)
    if not row:
        bot.answer_callback_query(call.id, "Already handled.", show_alert=True)
        return

    _, user_id, chat_id, file_name, file_type = row
    user_id = int(user_id)
    chat_id = int(chat_id)

    user_folder = get_user_folder(user_id)
    file_path = os.path.join(user_folder, file_name)
    if not os.path.exists(file_path):
        bot.answer_callback_query(call.id, "File missing.", show_alert=True)
        delete_pending_approval(pending_id)
        return

    bot.answer_callback_query(call.id, "Approved ✅")
    bot.edit_message_text("✅ Approved. Installing (if any) + starting ...", call.message.chat.id, call.message.message_id)

    # ✅ Install requirements only after approve (ZIP or folder cases)
    ok = install_requirements_if_present(user_folder, call.message)
    if not ok:
        try:
            bot.send_message(chat_id, "❌ Your code was approved but dependencies failed to install. Re-upload with correct requirements.txt.")
        except Exception:
            pass
        delete_pending_approval(pending_id)
        return

    # ✅ Run
    if file_type == "py":
        threading.Thread(target=run_script, args=(file_path, user_id, user_folder, file_name, call.message), daemon=True).start()
    elif file_type == "js":
        threading.Thread(target=run_js_script, args=(file_path, user_id, user_folder, file_name, call.message), daemon=True).start()

    try:
        bot.send_message(chat_id, f"✅ Approved. Now running `{file_name}`.", parse_mode="Markdown")
    except Exception:
        pass

    delete_pending_approval(pending_id)

def reject_pending_callback(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "Owner only.", show_alert=True)
        return

    pending_id = int(call.data.split("_", 1)[1])
    row = get_pending_approval(pending_id)
    if not row:
        bot.answer_callback_query(call.id, "Already handled.", show_alert=True)
        return

    _, user_id, chat_id, file_name, file_type = row
    user_id = int(user_id)
    chat_id = int(chat_id)

    bot.answer_callback_query(call.id, "Rejected ❌")
    bot.edit_message_text("❌ Rejected. User notified.", call.message.chat.id, call.message.message_id)

    # Optional delete file
    try:
        user_folder = get_user_folder(user_id)
        fp = os.path.join(user_folder, file_name)
        if os.path.exists(fp):
            os.remove(fp)
    except Exception:
        pass

    try:
        remove_user_file_db(user_id, file_name)
    except Exception:
        pass

    try:
        bot.send_message(chat_id, "❌ FILE REJECTED ❌\n \nYour file has been rejected by the administrator.\nReason: The file contains suspicious code patterns that pose a security risk.\nPlease review your code and remove any potentially harmful operations.")
    except Exception:
        pass

    delete_pending_approval(pending_id)


# =========================
# HANDLERS
# =========================
@bot.message_handler(commands=["start", "help"])
def cmd_start(message):
    _logic_send_welcome(message)

@bot.message_handler(func=lambda m: m.text in BUTTON_TEXT_TO_LOGIC)
def handle_buttons(message):
    BUTTON_TEXT_TO_LOGIC[message.text](message)

@bot.message_handler(content_types=["document"])
def handle_file_upload_doc(message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked.")
        return

    # limits
    file_limit = get_user_file_limit(user_id)
    if get_user_file_count(user_id) >= file_limit:
        bot.reply_to(message, "⚠️ File limit reached. Delete files first.")
        return

    doc = message.document
    file_name = doc.file_name or ""
    ext = os.path.splitext(file_name)[1].lower()
    if ext not in [".py", ".js", ".zip"]:
        bot.reply_to(message, "⚠️ Only .py .zip allowed.")
        return

    # download
    bot.reply_to(message, f"⏳ Downloading `{file_name}` ...", parse_mode="Markdown")
    try:
        fi = bot.get_file(doc.file_id)
        content = bot.download_file(fi.file_path)
    except Exception as e:
        bot.reply_to(message, f"❌ Download error: {e}")
        return

    # handle zip
    if ext == ".zip":
        handle_zip_file(content, file_name, message)
        return

    # save file
    user_folder = get_user_folder(user_id)
    file_path = os.path.join(user_folder, file_name)
    with open(file_path, "wb") as f:
        f.write(content)

    file_type = "js" if ext == ".js" else "py"
    save_user_file(user_id, file_name, file_type)

    # ✅ PENDING APPROVAL (DO NOT RUN)
    pending_id = add_pending_approval(user_id, chat_id, file_name, file_type)

    bot.reply_to(message, "✅ File uploaded.\n⏳ Waiting for OWNER approval before running/hosting.")

    # notify owner (and forward original file)
    try:
        owner_text = (
            f"🛑 Approval Required\n\n"
            f"👤 User: {message.from_user.first_name}\n"
            f"🆔 ID: `{user_id}`\n"
            f"📄 File: `{file_name}` ({file_type})\n\n"
            f"Approve or Reject:"
        )
        bot.send_message(OWNER_ID, owner_text, parse_mode="Markdown", reply_markup=approval_markup(pending_id))
        bot.forward_message(OWNER_ID, chat_id, message.message_id)
    except Exception as e:
        logger.error(f"Owner notify failed: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda c: True)
def handle_callbacks(call):
    data = call.data

    # approve/reject
    if data.startswith("approve_"):
        return approve_pending_callback(call)
    if data.startswith("reject_"):
        return reject_pending_callback(call)

    user_id = call.from_user.id
    chat_id = call.message.chat.id

    if data == "upload":
        bot.answer_callback_query(call.id)
        return bot.send_message(chat_id, "📤 Send `.py` / `.zip` (waits for OWNER approval).")

    if data == "stats":
        bot.answer_callback_query(call.id)
        return _logic_statistics(call.message)

    if data == "speed":
        bot.answer_callback_query(call.id)
        return _logic_bot_speed(call.message)

    if data == "toggle_lock":
        bot.answer_callback_query(call.id)
        if user_id not in admin_ids:
            return bot.send_message(chat_id, "⚠️ Admin only.")
        global bot_locked
        bot_locked = not bot_locked
        return bot.edit_message_text(
            f"{'🔒 Locked' if bot_locked else '🔓 Unlocked'}",
            chat_id, call.message.message_id,
            reply_markup=create_main_menu_inline(user_id)
        )

    if data == "check_files":
        bot.answer_callback_query(call.id)
        files = user_files.get(user_id, [])
        m = types.InlineKeyboardMarkup(row_width=1)
        if not files:
            m.add(types.InlineKeyboardButton("🔙 Back", callback_data="back_main"))
            return bot.edit_message_text("📂 No files.", chat_id, call.message.message_id, reply_markup=m)
        for fn, ft in sorted(files):
            running = is_bot_running(user_id, fn)
            icon = "🟢 Running" if running else "🔴 Stopped"
            m.add(types.InlineKeyboardButton(f"{fn} ({ft}) - {icon}", callback_data=f"file_{user_id}_{fn}"))
        m.add(types.InlineKeyboardButton("🔙 Back", callback_data="back_main"))
        return bot.edit_message_text("📂 Your files:", chat_id, call.message.message_id, reply_markup=m)

    if data == "back_main":
        bot.answer_callback_query(call.id)
        return bot.edit_message_text("〽️ Main Menu", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))

    # file controls
    if data.startswith("file_"):
        bot.answer_callback_query(call.id)
        _, owner_str, fn = data.split("_", 2)
        owner = int(owner_str)
        if not (user_id == owner or user_id in admin_ids):
            return bot.send_message(chat_id, "⚠️ You can only manage your own files.")
        running = is_bot_running(owner, fn)
        ft = next((x[1] for x in user_files.get(owner, []) if x[0] == fn), "?")
        return bot.edit_message_text(
            f"⚙️ `{fn}` ({ft})\nStatus: {'🟢 Running' if running else '🔴 Stopped'}",
            chat_id, call.message.message_id,
            parse_mode="Markdown",
            reply_markup=create_control_buttons(owner, fn, running)
        )

    if data.startswith("start_"):
        bot.answer_callback_query(call.id)
        _, owner_str, fn = data.split("_", 2)
        owner = int(owner_str)
        if not (user_id == owner or user_id in admin_ids):
            return bot.send_message(chat_id, "⚠️ Permission denied.")
        if is_bot_running(owner, fn):
            return bot.send_message(chat_id, "⚠️ Already running.")
        ft = next((x[1] for x in user_files.get(owner, []) if x[0] == fn), None)
        if not ft:
            return bot.send_message(chat_id, "⚠️ File record not found.")
        folder = get_user_folder(owner)
        fp = os.path.join(folder, fn)
        if not os.path.exists(fp):
            remove_user_file_db(owner, fn)
            return bot.send_message(chat_id, "⚠️ File missing. Re-upload.")
        # install requirements only when owner starts? (safe)
        install_requirements_if_present(folder, call.message)
        if ft == "py":
            threading.Thread(target=run_script, args=(fp, owner, folder, fn, call.message), daemon=True).start()
        else:
            threading.Thread(target=run_js_script, args=(fp, owner, folder, fn, call.message), daemon=True).start()
        time.sleep(1)
        running = is_bot_running(owner, fn)
        return bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=create_control_buttons(owner, fn, running))

    if data.startswith("stop_"):
        bot.answer_callback_query(call.id)
        _, owner_str, fn = data.split("_", 2)
        owner = int(owner_str)
        if not (user_id == owner or user_id in admin_ids):
            return bot.send_message(chat_id, "⚠️ Permission denied.")
        key = f"{owner}_{fn}"
        if key in bot_scripts:
            kill_process_tree(bot_scripts[key])
            bot_scripts.pop(key, None)
        return bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=create_control_buttons(owner, fn, False))

    if data.startswith("restart_"):
        bot.answer_callback_query(call.id)
        _, owner_str, fn = data.split("_", 2)
        owner = int(owner_str)
        if not (user_id == owner or user_id in admin_ids):
            return bot.send_message(chat_id, "⚠️ Permission denied.")
        key = f"{owner}_{fn}"
        if key in bot_scripts:
            kill_process_tree(bot_scripts[key])
            bot_scripts.pop(key, None)
        time.sleep(1)
        ft = next((x[1] for x in user_files.get(owner, []) if x[0] == fn), None)
        folder = get_user_folder(owner)
        fp = os.path.join(folder, fn)
        install_requirements_if_present(folder, call.message)
        if ft == "py":
            threading.Thread(target=run_script, args=(fp, owner, folder, fn, call.message), daemon=True).start()
        else:
            threading.Thread(target=run_js_script, args=(fp, owner, folder, fn, call.message), daemon=True).start()
        time.sleep(1)
        running = is_bot_running(owner, fn)
        return bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=create_control_buttons(owner, fn, running))

    if data.startswith("delete_"):
        bot.answer_callback_query(call.id)
        _, owner_str, fn = data.split("_", 2)
        owner = int(owner_str)
        if not (user_id == owner or user_id in admin_ids):
            return bot.send_message(chat_id, "⚠️ Permission denied.")
        key = f"{owner}_{fn}"
        if key in bot_scripts:
            kill_process_tree(bot_scripts[key])
            bot_scripts.pop(key, None)
        folder = get_user_folder(owner)
        fp = os.path.join(folder, fn)
        lp = os.path.join(folder, f"{os.path.splitext(fn)[0]}.log")
        try:
            if os.path.exists(fp):
                os.remove(fp)
            if os.path.exists(lp):
                os.remove(lp)
        except Exception:
            pass
        remove_user_file_db(owner, fn)
        return bot.edit_message_text("🗑️ Deleted.", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))

    if data.startswith("logs_"):
        bot.answer_callback_query(call.id)
        _, owner_str, fn = data.split("_", 2)
        owner = int(owner_str)
        if not (user_id == owner or user_id in admin_ids):
            return bot.send_message(chat_id, "⚠️ Permission denied.")
        folder = get_user_folder(owner)
        lp = os.path.join(folder, f"{os.path.splitext(fn)[0]}.log")
        if not os.path.exists(lp):
            return bot.send_message(chat_id, "⚠️ No log file.")
        try:
            with open(lp, "r", encoding="utf-8", errors="ignore") as f:
                txt = f.read()
            if not txt.strip():
                txt = "(empty)"
            if len(txt) > 3500:
                txt = txt[-3500:]
            return bot.send_message(chat_id, f"📜 Logs for `{fn}`:\n```\n{txt}\n```", parse_mode="Markdown")
        except Exception as e:
            return bot.send_message(chat_id, f"❌ Log read error: {e}")

    bot.answer_callback_query(call.id, "Unknown action.")


# =========================
# CLEANUP
# =========================
def cleanup():
    logger.warning("Shutdown cleanup...")
    for key in list(bot_scripts.keys()):
        try:
            kill_process_tree(bot_scripts[key])
        except Exception:
            pass
        bot_scripts.pop(key, None)
    logger.warning("Cleanup done.")

atexit.register(cleanup)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    logger.info("=" * 55)
    logger.info("🤖 Bot starting...")
    logger.info(f"Python: {sys.version.split()[0]}")
    logger.info(f"BASE_DIR: {BASE_DIR}")
    logger.info(f"UPLOAD_BOTS_DIR: {UPLOAD_BOTS_DIR}")
    logger.info(f"DB: {DATABASE_PATH}")
    logger.info(f"OWNER_ID: {OWNER_ID}")
    logger.info(f"Admins: {admin_ids}")
    logger.info("=" * 55)

    keep_alive()

    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout:
            logger.warning("Polling ReadTimeout. Retry 5s...")
            time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Polling ConnectionError: {e}. Retry 15s...")
            time.sleep(15)
        except Exception as e:
            logger.critical(f"Polling crash: {e}", exc_info=True)
            time.sleep(10)
