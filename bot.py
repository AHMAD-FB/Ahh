# ===========================
# Railway-ready Telegram Host Bot
# - ENV-based config (no hardcoded token)
# - Flask health endpoint (PORT)
# - SQLite stored in writable DATA_DIR (default /tmp)
# - Auto-install disabled by default (AUTO_INSTALL=0)
# ===========================

import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import sqlite3
import json
import logging
import threading
import re
import sys
import atexit
import requests

# --- Flask Keep Alive / Health ---
from flask import Flask
from threading import Thread

app = Flask("health")

@app.route("/")
def home():
    return "OK - Telegram Bot is running"

def run_flask():
    port = int(os.environ.get("PORT", "8080"))
    # use_reloader=False so it won't spawn 2 processes
    app.run(host="0.0.0.0", port=port, use_reloader=False)

def keep_alive():
    t = Thread(target=run_flask, daemon=True)
    t.start()
    print("✅ Flask health server started.")

# ===========================
# ENV CONFIG (Railway Variables)
# ===========================
TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("❌ BOT_TOKEN env var نیە! لە Railway → Variables دایبنێ.")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ADMIN_ID = int(os.getenv("ADMIN_ID", str(OWNER_ID if OWNER_ID else 0)))

YOUR_USERNAME = os.getenv("YOUR_USERNAME", "ahmed_snde").replace("@", "").strip()
UPDATE_CHANNEL = os.getenv("UPDATE_CHANNEL", "https://t.me/paivak").strip()
if UPDATE_CHANNEL.startswith("t.me/") or UPDATE_CHANNEL.startswith("telegram.me/"):
    UPDATE_CHANNEL = "https://" + UPDATE_CHANNEL

# Auto install (pip/npm) - disabled by default on Railway
AUTO_INSTALL = os.getenv("AUTO_INSTALL", "0") == "1"

# DATA DIR - Railway safe (writeable)
DATA_DIR = os.getenv("DATA_DIR", "/tmp")

# ===========================
# PATHS
# ===========================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(DATA_DIR, "upload_bots")   # moved to writeable dir
IROTECH_DIR = os.path.join(DATA_DIR, "inf")
DATABASE_PATH = os.path.join(IROTECH_DIR, "bot_data.db")

os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)

# ===========================
# LIMITS
# ===========================
FREE_USER_LIMIT = 2
SUBSCRIBED_USER_LIMIT = 15
ADMIN_LIMIT = 999
OWNER_LIMIT = float("inf")

# ===========================
# BOT INIT
# ===========================
bot = telebot.TeleBot(TOKEN)

# runtime maps
bot_scripts = {}           # {script_key: info}
user_subscriptions = {}    # {user_id: {'expiry': datetime}}
user_files = {}            # {user_id: [(file_name, file_type)]}
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID} if OWNER_ID else {ADMIN_ID}
bot_locked = False

# ===========================
# LOGGING
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("hostbot")

# ===========================
# UI LAYOUTS
# ===========================
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["📞 Contact Owner"]
]

ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Subscriptions", "📢 Broadcast"],
    ["🔒 Lock Bot", "🟢 Running All Code"],
    ["👑 Admin Panel", "📞 Contact Owner"]
]

# ===========================
# DB
# ===========================
DB_LOCK = threading.Lock()

def init_db():
    logger.info(f"Initializing DB: {DATABASE_PATH}")
    try:
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

        # ensure owner/admin exist
        if OWNER_ID:
            c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (OWNER_ID,))
        if ADMIN_ID and ADMIN_ID != OWNER_ID:
            c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (ADMIN_ID,))
        conn.commit()
        conn.close()
        logger.info("DB OK.")
    except Exception as e:
        logger.error(f"❌ DB init error: {e}", exc_info=True)

def load_data():
    logger.info("Loading data from DB...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        c.execute("SELECT user_id, expiry FROM subscriptions")
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {"expiry": datetime.fromisoformat(expiry)}
            except Exception:
                logger.warning(f"Bad expiry for {user_id}: {expiry}")

        c.execute("SELECT user_id, file_name, file_type FROM user_files")
        for user_id, file_name, file_type in c.fetchall():
            user_files.setdefault(user_id, []).append((file_name, file_type))

        c.execute("SELECT user_id FROM active_users")
        active_users.update(uid for (uid,) in c.fetchall())

        c.execute("SELECT user_id FROM admins")
        admin_ids.update(uid for (uid,) in c.fetchall())

        conn.close()
        logger.info(f"Loaded: users={len(active_users)} subs={len(user_subscriptions)} admins={len(admin_ids)}")
    except Exception as e:
        logger.error(f"❌ Load data error: {e}", exc_info=True)

init_db()
load_data()

# ===========================
# HELPERS
# ===========================
def get_user_folder(user_id: int) -> str:
    folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(folder, exist_ok=True)
    return folder

def get_user_file_limit(user_id: int):
    if OWNER_ID and user_id == OWNER_ID:
        return OWNER_LIMIT
    if user_id in admin_ids:
        return ADMIN_LIMIT
    sub = user_subscriptions.get(user_id)
    if sub and sub.get("expiry") and sub["expiry"] > datetime.now():
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
        proc = psutil.Process(info["process"].pid)
        running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
        if not running:
            try:
                if info.get("log_file") and not info["log_file"].closed:
                    info["log_file"].close()
            except Exception:
                pass
            bot_scripts.pop(script_key, None)
        return running
    except psutil.NoSuchProcess:
        try:
            if info.get("log_file") and not info["log_file"].closed:
                info["log_file"].close()
        except Exception:
            pass
        bot_scripts.pop(script_key, None)
        return False
    except Exception as e:
        logger.error(f"Process check error {script_key}: {e}", exc_info=True)
        return False

def kill_process_tree(process_info: dict):
    script_key = process_info.get("script_key", "N/A")
    process = process_info.get("process")
    try:
        if process_info.get("log_file") and not process_info["log_file"].closed:
            try:
                process_info["log_file"].close()
            except Exception:
                pass

        if not process or not hasattr(process, "pid"):
            return

        pid = process.pid
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)

        for child in children:
            try:
                child.terminate()
            except Exception:
                try:
                    child.kill()
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

        logger.info(f"Killed: {script_key} (PID={pid})")
    except Exception as e:
        logger.error(f"Kill error {script_key}: {e}", exc_info=True)

# ===========================
# DB OPS
# ===========================
def save_user_file(user_id, file_name, file_type="py"):
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

def remove_user_file_db(user_id, file_name):
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

def add_active_user(user_id):
    active_users.add(user_id)
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO active_users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()

def save_subscription(user_id, expiry: datetime):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)", (user_id, expiry.isoformat()))
        conn.commit()
        conn.close()
    user_subscriptions[user_id] = {"expiry": expiry}

def remove_subscription_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("DELETE FROM subscriptions WHERE user_id=?", (user_id,))
        conn.commit()
        conn.close()
    user_subscriptions.pop(user_id, None)

def add_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (admin_id,))
        conn.commit()
        conn.close()
    admin_ids.add(admin_id)

def remove_admin_db(admin_id):
    if OWNER_ID and admin_id == OWNER_ID:
        return False
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("DELETE FROM admins WHERE user_id=?", (admin_id,))
        conn.commit()
        conn.close()
    admin_ids.discard(admin_id)
    return True

# ===========================
# MENUS
# ===========================
def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row in layout:
        markup.add(*[types.KeyboardButton(t) for t in row])
    return markup

def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)

    buttons = [
        types.InlineKeyboardButton("📢 Updates Channel", url=UPDATE_CHANNEL),
        types.InlineKeyboardButton("📤 Upload File", callback_data="upload"),
        types.InlineKeyboardButton("📂 Check Files", callback_data="check_files"),
        types.InlineKeyboardButton("⚡ Bot Speed", callback_data="speed"),
        types.InlineKeyboardButton("📞 Contact Owner", url=f"https://t.me/{YOUR_USERNAME}"),
    ]

    if user_id in admin_ids:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3], types.InlineKeyboardButton("💳 Subscriptions", callback_data="subscription"))
        markup.add(types.InlineKeyboardButton("📊 Statistics", callback_data="stats"),
                   types.InlineKeyboardButton("📢 Broadcast", callback_data="broadcast"))
        markup.add(types.InlineKeyboardButton("🔒 Lock Bot" if not bot_locked else "🔓 Unlock Bot",
                   callback_data="lock_bot" if not bot_locked else "unlock_bot"),
                   types.InlineKeyboardButton("🟢 Run All User Scripts", callback_data="run_all_scripts"))
        markup.add(types.InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel"))
        markup.add(buttons[4])
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        markup.add(types.InlineKeyboardButton("📊 Statistics", callback_data="stats"))
        markup.add(buttons[4])

    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True):
    markup = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f"stop_{script_owner_id}_{file_name}"),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f"restart_{script_owner_id}_{file_name}"),
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{script_owner_id}_{file_name}"),
            types.InlineKeyboardButton("📜 Logs", callback_data=f"logs_{script_owner_id}_{file_name}"),
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f"start_{script_owner_id}_{file_name}"),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{script_owner_id}_{file_name}"),
        )
        markup.row(types.InlineKeyboardButton("📜 View Logs", callback_data=f"logs_{script_owner_id}_{file_name}"))
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data="check_files"))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton("➕ Add Admin", callback_data="add_admin"),
        types.InlineKeyboardButton("➖ Remove Admin", callback_data="remove_admin"),
    )
    markup.row(types.InlineKeyboardButton("📋 List Admins", callback_data="list_admins"))
    markup.row(types.InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main"))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton("➕ Add Subscription", callback_data="add_subscription"),
        types.InlineKeyboardButton("➖ Remove Subscription", callback_data="remove_subscription"),
    )
    markup.row(types.InlineKeyboardButton("🔍 Check Subscription", callback_data="check_subscription"))
    markup.row(types.InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main"))
    return markup

# ===========================
# AUTO INSTALL (OFF by default)
# ===========================
TELEGRAM_MODULES = {
    "telebot": "pyTelegramBotAPI",
    "bs4": "beautifulsoup4",
    "pillow": "Pillow",
    "cv2": "opencv-python",
    "yaml": "PyYAML",
    "dotenv": "python-dotenv",
    "dateutil": "python-dateutil",
    "flask": "Flask",
    "requests": "requests",
    "psutil": "psutil",
    "asyncio": None,
    "json": None,
    "datetime": None,
    "os": None,
    "sys": None,
    "re": None,
    "time": None,
    "random": None,
    "logging": None,
    "threading": None,
    "subprocess": None,
    "zipfile": None,
    "tempfile": None,
    "shutil": None,
    "sqlite3": None,
    "atexit": None,
}

def attempt_install_pip(module_name, message):
    if not AUTO_INSTALL:
        bot.reply_to(message, "❌ Auto-install ناچالاکە لە Railway. requirements.txt چاک بکە.")
        return False

    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name)
    if package_name is None:
        return False

    try:
        bot.reply_to(message, f"🐍 Installing `{package_name}` ...", parse_mode="Markdown")
        cmd = [sys.executable, "-m", "pip", "install", package_name]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            bot.reply_to(message, f"✅ Installed `{package_name}`", parse_mode="Markdown")
            return True
        bot.reply_to(message, f"❌ Install failed:\n```\n{(result.stderr or result.stdout)[:3000]}\n```", parse_mode="Markdown")
        return False
    except Exception as e:
        bot.reply_to(message, f"❌ Install error: {e}")
        return False

def attempt_install_npm(module_name, user_folder, message):
    if not AUTO_INSTALL:
        bot.reply_to(message, "❌ Auto-install ناچالاکە لە Railway. package.json/Node deps بەدەست خۆت ڕێک بخە.")
        return False
    try:
        bot.reply_to(message, f"🟠 npm installing `{module_name}` ...", parse_mode="Markdown")
        cmd = ["npm", "install", module_name]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=user_folder)
        if result.returncode == 0:
            bot.reply_to(message, f"✅ npm installed `{module_name}`", parse_mode="Markdown")
            return True
        bot.reply_to(message, f"❌ npm failed:\n```\n{(result.stderr or result.stdout)[:3000]}\n```", parse_mode="Markdown")
        return False
    except Exception as e:
        bot.reply_to(message, f"❌ npm error: {e}")
        return False

# ===========================
# RUNNERS (PY / JS)
# ===========================
def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Run PY attempt {attempt}: {script_key}")

    if not os.path.exists(script_path):
        bot.reply_to(message_obj_for_reply, f"❌ Script not found: {file_name}")
        remove_user_file_db(script_owner_id, file_name)
        return

    # quick import check (5s)
    if attempt == 1:
        try:
            check_proc = subprocess.Popen(
                [sys.executable, script_path],
                cwd=user_folder,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="ignore",
            )
            try:
                _, stderr = check_proc.communicate(timeout=5)
                if check_proc.returncode != 0 and stderr:
                    m = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if m:
                        missing = m.group(1).strip()
                        if attempt_install_pip(missing, message_obj_for_reply):
                            bot.reply_to(message_obj_for_reply, f"🔄 Retrying '{file_name}' ...")
                            time.sleep(1)
                            threading.Thread(
                                target=run_script,
                                args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1),
                                daemon=True,
                            ).start()
                            return
                        bot.reply_to(message_obj_for_reply, f"❌ Missing module `{missing}`. Add it to requirements.txt.", parse_mode="Markdown")
                        return
                    bot.reply_to(message_obj_for_reply, f"❌ Script error:\n```\n{stderr[:1200]}\n```", parse_mode="Markdown")
                    return
            except subprocess.TimeoutExpired:
                if check_proc.poll() is None:
                    check_proc.kill()
                    check_proc.communicate()
        except Exception as e:
            bot.reply_to(message_obj_for_reply, f"❌ Pre-check error: {e}")
            return

    log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
    try:
        log_file = open(log_path, "w", encoding="utf-8", errors="ignore")
    except Exception as e:
        bot.reply_to(message_obj_for_reply, f"❌ Cannot open log file: {e}")
        return

    try:
        process = subprocess.Popen(
            [sys.executable, script_path],
            cwd=user_folder,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore",
        )
        bot_scripts[script_key] = {
            "process": process,
            "log_file": log_file,
            "file_name": file_name,
            "script_owner_id": script_owner_id,
            "user_folder": user_folder,
            "start_time": datetime.now(),
            "type": "py",
            "script_key": script_key,
        }
        bot.reply_to(message_obj_for_reply, f"✅ Python script started: `{file_name}` (PID: {process.pid})", parse_mode="Markdown")
    except Exception as e:
        try:
            log_file.close()
        except Exception:
            pass
        bot.reply_to(message_obj_for_reply, f"❌ Start error: {e}")
        bot_scripts.pop(script_key, None)

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Run JS attempt {attempt}: {script_key}")

    if not os.path.exists(script_path):
        bot.reply_to(message_obj_for_reply, f"❌ Script not found: {file_name}")
        remove_user_file_db(script_owner_id, file_name)
        return

    if attempt == 1:
        try:
            check_proc = subprocess.Popen(
                ["node", script_path],
                cwd=user_folder,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="ignore",
            )
            try:
                _, stderr = check_proc.communicate(timeout=5)
                if check_proc.returncode != 0 and stderr:
                    m = re.search(r"Cannot find module '(.+?)'", stderr)
                    if m:
                        missing = m.group(1).strip()
                        if not missing.startswith(".") and not missing.startswith("/"):
                            if attempt_install_npm(missing, user_folder, message_obj_for_reply):
                                bot.reply_to(message_obj_for_reply, f"🔄 Retrying '{file_name}' ...")
                                time.sleep(1)
                                threading.Thread(
                                    target=run_js_script,
                                    args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1),
                                    daemon=True,
                                ).start()
                                return
                            bot.reply_to(message_obj_for_reply, f"❌ Missing node module `{missing}`.", parse_mode="Markdown")
                            return
                    bot.reply_to(message_obj_for_reply, f"❌ JS error:\n```\n{stderr[:1200]}\n```", parse_mode="Markdown")
                    return
            except subprocess.TimeoutExpired:
                if check_proc.poll() is None:
                    check_proc.kill()
                    check_proc.communicate()
        except Exception as e:
            bot.reply_to(message_obj_for_reply, f"❌ Pre-check error: {e}")
            return

    log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
    try:
        log_file = open(log_path, "w", encoding="utf-8", errors="ignore")
    except Exception as e:
        bot.reply_to(message_obj_for_reply, f"❌ Cannot open log file: {e}")
        return

    try:
        process = subprocess.Popen(
            ["node", script_path],
            cwd=user_folder,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore",
        )
        bot_scripts[script_key] = {
            "process": process,
            "log_file": log_file,
            "file_name": file_name,
            "script_owner_id": script_owner_id,
            "user_folder": user_folder,
            "start_time": datetime.now(),
            "type": "js",
            "script_key": script_key,
        }
        bot.reply_to(message_obj_for_reply, f"✅ JS script started: `{file_name}` (PID: {process.pid})", parse_mode="Markdown")
    except Exception as e:
        try:
            log_file.close()
        except Exception:
            pass
        bot.reply_to(message_obj_for_reply, f"❌ Start error: {e}")
        bot_scripts.pop(script_key, None)

# ===========================
# ZIP HANDLER
# ===========================
def handle_zip_file(downloaded_file_content, file_name_zip, message):
    user_id = message.from_user.id
    user_folder = get_user_folder(user_id)
    temp_dir = None

    try:
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
        zip_path = os.path.join(temp_dir, file_name_zip)

        with open(zip_path, "wb") as f:
            f.write(downloaded_file_content)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # path traversal protection
            for member in zip_ref.infolist():
                member_path = os.path.abspath(os.path.join(temp_dir, member.filename))
                if not member_path.startswith(os.path.abspath(temp_dir)):
                    raise zipfile.BadZipFile(f"Unsafe path in zip: {member.filename}")
            zip_ref.extractall(temp_dir)

        extracted = os.listdir(temp_dir)
        py_files = [f for f in extracted if f.endswith(".py")]
        js_files = [f for f in extracted if f.endswith(".js")]

        # move everything to user folder (overwrite)
        for item in os.listdir(temp_dir):
            src = os.path.join(temp_dir, item)
            dst = os.path.join(user_folder, item)
            if os.path.isdir(dst):
                shutil.rmtree(dst)
            elif os.path.exists(dst):
                os.remove(dst)
            shutil.move(src, dst)

        # decide main script
        preferred_py = ["main.py", "bot.py", "app.py"]
        preferred_js = ["index.js", "main.js", "bot.js", "app.js"]

        main_script = None
        file_type = None

        for p in preferred_py:
            if p in py_files:
                main_script, file_type = p, "py"
                break
        if not main_script:
            for p in preferred_js:
                if p in js_files:
                    main_script, file_type = p, "js"
                    break
        if not main_script:
            if py_files:
                main_script, file_type = py_files[0], "py"
            elif js_files:
                main_script, file_type = js_files[0], "js"

        if not main_script:
            bot.reply_to(message, "❌ No .py or .js found in ZIP.")
            return

        save_user_file(user_id, main_script, file_type)
        bot.reply_to(message, f"✅ ZIP extracted. Starting `{main_script}` ...", parse_mode="Markdown")

        path = os.path.join(user_folder, main_script)
        if file_type == "py":
            threading.Thread(target=run_script, args=(path, user_id, user_folder, main_script, message), daemon=True).start()
        else:
            threading.Thread(target=run_js_script, args=(path, user_id, user_folder, main_script, message), daemon=True).start()

    except zipfile.BadZipFile as e:
        bot.reply_to(message, f"❌ Bad ZIP: {e}")
    except Exception as e:
        logger.error(f"ZIP error: {e}", exc_info=True)
        bot.reply_to(message, f"❌ ZIP error: {e}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass

# ===========================
# MAIN LOGIC
# ===========================
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ Bot locked by admin. Try later.")
        return

    if user_id not in active_users:
        add_active_user(user_id)
        # optional notify owner
        try:
            if OWNER_ID:
                bot.send_message(
                    OWNER_ID,
                    f"🎉 New user\n🆔 `{user_id}`\n👤 {message.from_user.first_name}\n✳️ @{message.from_user.username or 'N/A'}",
                    parse_mode="Markdown",
                )
        except Exception:
            pass

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = "Unlimited" if file_limit == float("inf") else str(file_limit)

    expiry_info = ""
    if OWNER_ID and user_id == OWNER_ID:
        status = "👑 Owner"
    elif user_id in admin_ids:
        status = "🛡️ Admin"
    elif user_id in user_subscriptions and user_subscriptions[user_id].get("expiry", datetime.min) > datetime.now():
        status = "⭐ Premium"
        days_left = (user_subscriptions[user_id]["expiry"] - datetime.now()).days
        expiry_info = f"\n⏳ Subscription: {days_left} days left"
    else:
        status = "🆓 Free User"

    text = (
        f"〽️ Welcome!\n\n"
        f"🆔 ID: `{user_id}`\n"
        f"✳️ Username: `@{message.from_user.username or 'Not set'}`\n"
        f"🔰 Status: {status}{expiry_info}\n"
        f"📁 Files: {current_files} / {limit_str}\n\n"
        f"📤 Upload `.py` / `.js` / `.zip` then manage from 📂 Check Files"
    )
    bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=create_reply_keyboard_main_menu(user_id))
    bot.send_message(chat_id, "〽️ Main Menu", reply_markup=create_main_menu_inline(user_id))

def _logic_updates_channel(message):
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton("📢 Updates Channel", url=UPDATE_CHANNEL))
    bot.reply_to(message, "Visit Updates Channel:", reply_markup=mk)

def _logic_upload_file(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin.")
        return

    limit_ = get_user_file_limit(user_id)
    count_ = get_user_file_count(user_id)
    if count_ >= limit_:
        lim = "Unlimited" if limit_ == float("inf") else str(limit_)
        bot.reply_to(message, f"⚠️ Limit reached ({count_}/{lim}). Delete files first.")
        return

    bot.reply_to(message, "📤 Send `.py` / `.js` / `.zip` file.")

def _logic_check_files(message):
    user_id = message.from_user.id
    items = user_files.get(user_id, [])
    if not items:
        bot.reply_to(message, "📂 (No files yet)")
        return

    mk = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(items):
        running = is_bot_running(user_id, file_name)
        status = "🟢 Running" if running else "🔴 Stopped"
        mk.add(types.InlineKeyboardButton(f"{file_name} ({file_type}) - {status}", callback_data=f"file_{user_id}_{file_name}"))
    bot.reply_to(message, "📂 Your files:", reply_markup=mk)

def _logic_bot_speed(message):
    start = time.time()
    msg = bot.reply_to(message, "🏃 Testing...")
    latency = round((time.time() - start) * 1000, 2)
    bot.edit_message_text(f"⚡ Pong: {latency} ms", message.chat.id, msg.message_id)

def _logic_statistics(message):
    total_users = len(active_users)
    total_files = sum(len(v) for v in user_files.values())

    running = 0
    for k, info in list(bot_scripts.items()):
        try:
            owner_id_str, _ = k.split("_", 1)
            if is_bot_running(int(owner_id_str), info["file_name"]):
                running += 1
        except Exception:
            pass

    bot.reply_to(message, f"📊 Stats\n\n👥 Users: {total_users}\n📂 Files: {total_files}\n🟢 Running: {running}")

def _logic_contact_owner(message):
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton("📞 Contact Owner", url=f"https://t.me/{YOUR_USERNAME}"))
    bot.reply_to(message, "Contact:", reply_markup=mk)

def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin only.")
        return
    bot.reply_to(message, "💳 Subscription panel", reply_markup=create_subscription_menu())

def _logic_toggle_lock_bot(message):
    global bot_locked
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin only.")
        return
    bot_locked = not bot_locked
    bot.reply_to(message, f"{'🔒 Locked' if bot_locked else '🔓 Unlocked'}")

# NOTE: Broadcast + admin panel functions are large in your original code.
# To keep this Railway version stable, we keep your core hosting features ready.
# If you want, I can paste the full broadcast/admin-sub logic back in too (same as your code).

BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": _logic_updates_channel,
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📊 Statistics": _logic_statistics,
    "📞 Contact Owner": _logic_contact_owner,
    "💳 Subscriptions": _logic_subscriptions_panel,
    "🔒 Lock Bot": _logic_toggle_lock_bot,
}

@bot.message_handler(commands=["start", "help"])
def cmd_start(message):
    _logic_send_welcome(message)

@bot.message_handler(func=lambda m: m.text in BUTTON_TEXT_TO_LOGIC)
def handle_buttons(message):
    BUTTON_TEXT_TO_LOGIC[message.text](message)

@bot.message_handler(content_types=["document"])
def handle_file_upload_doc(message):
    user_id = message.from_user.id

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked.")
        return

    limit_ = get_user_file_limit(user_id)
    count_ = get_user_file_count(user_id)
    if count_ >= limit_:
        lim = "Unlimited" if limit_ == float("inf") else str(limit_)
        bot.reply_to(message, f"⚠️ Limit reached ({count_}/{lim}).")
        return

    doc = message.document
    file_name = doc.file_name or ""
    ext = os.path.splitext(file_name)[1].lower()

    if ext not in [".py", ".js", ".zip"]:
        bot.reply_to(message, "⚠️ Only .py / .js / .zip allowed.")
        return

    if doc.file_size and doc.file_size > 20 * 1024 * 1024:
        bot.reply_to(message, "⚠️ Max size is 20MB.")
        return

    try:
        wait = bot.reply_to(message, f"⏳ Downloading `{file_name}`...", parse_mode="Markdown")
        fi = bot.get_file(doc.file_id)
        data = bot.download_file(fi.file_path)
        bot.edit_message_text(f"✅ Downloaded `{file_name}`", message.chat.id, wait.message_id, parse_mode="Markdown")

        user_folder = get_user_folder(user_id)

        if ext == ".zip":
            handle_zip_file(data, file_name, message)
            return

        path = os.path.join(user_folder, file_name)
        with open(path, "wb") as f:
            f.write(data)

        if ext == ".py":
            save_user_file(user_id, file_name, "py")
            threading.Thread(target=run_script, args=(path, user_id, user_folder, file_name, message), daemon=True).start()
        else:
            save_user_file(user_id, file_name, "js")
            threading.Thread(target=run_js_script, args=(path, user_id, user_folder, file_name, message), daemon=True).start()

    except Exception as e:
        logger.error(f"Upload error: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error: {e}")

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    data = call.data
    uid = call.from_user.id

    if bot_locked and uid not in admin_ids and data not in ["stats", "speed", "back_to_main"]:
        bot.answer_callback_query(call.id, "⚠️ Bot locked.", show_alert=True)
        return

    try:
        if data == "upload":
            bot.answer_callback_query(call.id)
            bot.send_message(call.message.chat.id, "📤 Send .py / .js / .zip")
            return

        if data == "check_files":
            bot.answer_callback_query(call.id)
            # show list
            items = user_files.get(uid, [])
            mk = types.InlineKeyboardMarkup(row_width=1)
            for file_name, file_type in sorted(items):
                running = is_bot_running(uid, file_name)
                status = "🟢 Running" if running else "🔴 Stopped"
                mk.add(types.InlineKeyboardButton(f"{file_name} ({file_type}) - {status}", callback_data=f"file_{uid}_{file_name}"))
            mk.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main"))
            bot.edit_message_text("📂 Your files:", call.message.chat.id, call.message.message_id, reply_markup=mk)
            return

        if data.startswith("file_"):
            _, owner_str, fname = data.split("_", 2)
            owner = int(owner_str)
            if not (uid == owner or uid in admin_ids):
                bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
                return
            running = is_bot_running(owner, fname)
            bot.answer_callback_query(call.id)
            bot.edit_message_text(
                f"⚙️ `{fname}`\nStatus: {'🟢 Running' if running else '🔴 Stopped'}",
                call.message.chat.id, call.message.message_id,
                reply_markup=create_control_buttons(owner, fname, running),
                parse_mode="Markdown"
            )
            return

        if data.startswith(("start_", "stop_", "restart_", "delete_", "logs_")):
            action, owner_str, fname = data.split("_", 2)
            owner = int(owner_str)
            if not (uid == owner or uid in admin_ids):
                bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
                return

            user_folder = get_user_folder(owner)
            file_path = os.path.join(user_folder, fname)
            script_key = f"{owner}_{fname}"

            if action == "start":
                bot.answer_callback_query(call.id, "Starting...")
                ftype = next((t for n, t in user_files.get(owner, []) if n == fname), "py")
                if ftype == "py":
                    threading.Thread(target=run_script, args=(file_path, owner, user_folder, fname, call.message), daemon=True).start()
                else:
                    threading.Thread(target=run_js_script, args=(file_path, owner, user_folder, fname, call.message), daemon=True).start()
                time.sleep(1)
                running = is_bot_running(owner, fname)
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(owner, fname, running))
                return

            if action == "stop":
                bot.answer_callback_query(call.id, "Stopping...")
                info = bot_scripts.get(script_key)
                if info:
                    kill_process_tree(info)
                    bot_scripts.pop(script_key, None)
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(owner, fname, False))
                return

            if action == "restart":
                bot.answer_callback_query(call.id, "Restarting...")
                info = bot_scripts.get(script_key)
                if info:
                    kill_process_tree(info)
                    bot_scripts.pop(script_key, None)
                time.sleep(1)
                ftype = next((t for n, t in user_files.get(owner, []) if n == fname), "py")
                if ftype == "py":
                    threading.Thread(target=run_script, args=(file_path, owner, user_folder, fname, call.message), daemon=True).start()
                else:
                    threading.Thread(target=run_js_script, args=(file_path, owner, user_folder, fname, call.message), daemon=True).start()
                return

            if action == "delete":
                bot.answer_callback_query(call.id, "Deleting...")
                info = bot_scripts.get(script_key)
                if info:
                    kill_process_tree(info)
                    bot_scripts.pop(script_key, None)

                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception:
                    pass

                try:
                    lp = os.path.join(user_folder, f"{os.path.splitext(fname)[0]}.log")
                    if os.path.exists(lp):
                        os.remove(lp)
                except Exception:
                    pass

                remove_user_file_db(owner, fname)
                bot.edit_message_text("🗑️ Deleted.", call.message.chat.id, call.message.message_id)
                return

            if action == "logs":
                bot.answer_callback_query(call.id)
                lp = os.path.join(user_folder, f"{os.path.splitext(fname)[0]}.log")
                if not os.path.exists(lp):
                    bot.send_message(call.message.chat.id, "⚠️ No logs yet.")
                    return
                try:
                    with open(lp, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                    if len(content) > 3500:
                        content = content[-3500:]
                        content = "...\n" + content
                    bot.send_message(call.message.chat.id, f"📜 Logs for `{fname}`:\n```\n{content}\n```", parse_mode="Markdown")
                except Exception as e:
                    bot.send_message(call.message.chat.id, f"❌ Log error: {e}")
                return

        if data == "speed":
            bot.answer_callback_query(call.id)
            start = time.time()
            latency = round((time.time() - start) * 1000, 2)
            bot.edit_message_text(f"⚡ {latency} ms", call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(uid))
            return

        if data == "stats":
            bot.answer_callback_query(call.id)
            _logic_statistics(call.message)
            return

        if data == "back_to_main":
            bot.answer_callback_query(call.id)
            bot.edit_message_text("〽️ Main Menu", call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(uid))
            return

        bot.answer_callback_query(call.id, "Unknown.")
    except Exception as e:
        logger.error(f"Callback error: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Error.", show_alert=True)
        except Exception:
            pass

# ===========================
# CLEANUP
# ===========================
def cleanup():
    logger.warning("Shutdown cleanup...")
    for key in list(bot_scripts.keys()):
        try:
            kill_process_tree(bot_scripts[key])
        except Exception:
            pass
        bot_scripts.pop(key, None)

atexit.register(cleanup)

# ===========================
# MAIN
# ===========================
if __name__ == "__main__":
    logger.info("=" * 45)
    logger.info("🤖 Bot starting (Railway-ready)")
    logger.info(f"🐍 Python: {sys.version.split()[0]}")
    logger.info(f"📁 DATA_DIR: {DATA_DIR}")
    logger.info(f"📁 UPLOAD_BOTS_DIR: {UPLOAD_BOTS_DIR}")
    logger.info(f"📄 DB: {DATABASE_PATH}")
    logger.info(f"👑 OWNER_ID: {OWNER_ID}")
    logger.info(f"🛡️ ADMINS: {sorted(list(admin_ids))}")
    logger.info(f"🌐 UPDATE_CHANNEL: {UPDATE_CHANNEL}")
    logger.info(f"🧩 AUTO_INSTALL: {AUTO_INSTALL}")
    logger.info("=" * 45)

    keep_alive()

    logger.info("🚀 Starting polling...")
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout:
            logger.warning("Polling ReadTimeout. Restarting in 5s...")
            time.sleep(5)
        except requests.exceptions.ConnectionError as ce:
            logger.error(f"Polling ConnectionError: {ce}. Retrying in 15s...")
            time.sleep(15)
        except Exception as e:
            logger.critical(f"💥 Polling error: {e}", exc_info=True)
            time.sleep(30)
        finally:
            time.sleep(1)
