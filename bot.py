#!/usr/bin/env python3
"""
Advanced Telegram Multi-Platform Channel Dispatcher Bot
100% Stable Production Build | Thread-Safe SQLite & Bulletproof Auto-Forward
Timezone: Asia/Kolkata (IST)
"""

import asyncio
import datetime
import html
import json
import logging
import os
import re
import sqlite3
import sys
import threading
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from flask import Flask, jsonify
from telegram import (
    ChatMember,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
TOKEN = os.getenv("BOT_TOKEN", "8863781796:AAF7LQwOQJkWYSNtqaz1P-pZzVNIxfzCGUs").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "8984439807"))
PORT = int(os.getenv("PORT", 10000))

DATA_DIR = os.getenv("DATA_DIR", "/var/data") if os.path.exists("/var/data") else os.path.abspath("./data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "dispatcher.db")

TIMEZONE_STR = "Asia/Kolkata"
IST = ZoneInfo(TIMEZONE_STR)

logging.basicConfig(
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ChannelDispatcherBot")

# ==============================================================================
# THREAD-SAFE DATABASE ACCESS & CLEANUP
# ==============================================================================
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=60.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                username TEXT,
                can_post INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS platforms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                batch_limit INTEGER DEFAULT 5,
                backup_link TEXT,
                header_text TEXT,
                footer_text TEXT,
                is_paused INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        col_names = [row["name"] for row in cursor.execute("PRAGMA table_info(platforms);").fetchall()]
        if "header_text" not in col_names:
            cursor.execute("ALTER TABLE platforms ADD COLUMN header_text TEXT;")
        if "footer_text" not in col_names:
            cursor.execute("ALTER TABLE platforms ADD COLUMN footer_text TEXT;")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS platform_domains (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id INTEGER NOT NULL,
                domain TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (platform_id) REFERENCES platforms (id) ON DELETE CASCADE
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS platform_channels (
                platform_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (platform_id, channel_id),
                FOREIGN KEY (platform_id) REFERENCES platforms (id) ON DELETE CASCADE,
                FOREIGN KEY (channel_id) REFERENCES channels (channel_id) ON DELETE CASCADE
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id INTEGER NOT NULL,
                schedule_time TEXT NOT NULL,
                last_dispatched_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (platform_id, schedule_time),
                FOREIGN KEY (platform_id) REFERENCES platforms (id) ON DELETE CASCADE
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS queued_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id INTEGER,
                message_type TEXT NOT NULL,
                file_id TEXT,
                content_text TEXT,
                caption TEXT,
                entities_json TEXT,
                detected_urls TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (platform_id) REFERENCES platforms (id) ON DELETE SET NULL
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                platform_id INTEGER,
                channel_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                delivered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('is_paused', '0');")

        # Cleanup any orphaned records from earlier versions
        cursor.execute("DELETE FROM platform_domains WHERE platform_id NOT IN (SELECT id FROM platforms);")
        cursor.execute("DELETE FROM platform_channels WHERE platform_id NOT IN (SELECT id FROM platforms);")
        cursor.execute("DELETE FROM schedules WHERE platform_id NOT IN (SELECT id FROM platforms);")

        conn.commit()
    logger.info("Database initialized successfully at %s", DB_PATH)


# ==============================================================================
# UTILITIES & BULLETPROOF LINK EXTRACTION
# ==============================================================================
def normalize_domain(raw_domain: str) -> str:
    d = raw_domain.strip().lower()
    if not d.startswith("http://") and not d.startswith("https://"):
        d = "http://" + d
    try:
        parsed = urlparse(d)
        host = parsed.netloc or parsed.path.split("/")[0]
        host = host.split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host.strip()
    except Exception:
        return raw_domain.strip().lower()


def extract_urls(text: str) -> list[str]:
    if not text:
        return []
    url_pattern = re.compile(
        r"(?:https?://)?(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(?::\d+)?(?:/[^\s]*)?",
        re.IGNORECASE,
    )
    matches = url_pattern.findall(text)
    cleaned = []
    for u in matches:
        u_clean = u.rstrip(".,;!?'\")>]}\n\r ")
        if "." in u_clean:
            if not u_clean.startswith("http://") and not u_clean.startswith("https://"):
                u_clean = "https://" + u_clean
            if u_clean not in cleaned:
                cleaned.append(u_clean)
    return cleaned


def serialize_entities(entities) -> str | None:
    if not entities:
        return None
    raw = []
    for ent in entities:
        try:
            ent_type = str(ent.type.value if hasattr(ent.type, "value") else ent.type)
            raw.append({
                "type": ent_type,
                "offset": int(ent.offset),
                "length": int(ent.length),
                "url": str(ent.url) if ent.url else None,
                "language": str(ent.language) if getattr(ent, "language", None) else None,
            })
        except Exception:
            continue
    return json.dumps(raw)


def build_final_caption(platform: sqlite3.Row, detected_urls_str: str) -> str:
    urls = [u.strip() for u in (detected_urls_str or "").split(",") if u.strip()]
    header = platform["header_text"]
    footer = platform["footer_text"]

    body_parts = []
    if len(urls) == 1:
        body_parts.append(f"🔗 <b>Link:</b> {urls[0]}")
    elif len(urls) > 1:
        for idx, u in enumerate(urls, 1):
            body_parts.append(f"🔗 <b>Part {idx}:</b> {u}")
    else:
        if platform["backup_link"]:
            body_parts.append(f"🔗 <b>Link:</b> {platform['backup_link']}")

    links_block = "\n".join(body_parts)

    output_blocks = []
    if header and header.strip():
        output_blocks.append(header.strip())
    if links_block:
        output_blocks.append(links_block)
    if footer and footer.strip():
        output_blocks.append(footer.strip())

    return "\n\n".join(output_blocks)


def build_main_menu() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton("➕ Add Channel"), KeyboardButton("📢 Channels")],
        [KeyboardButton("➕ Add Platform"), KeyboardButton("⚙️ Manage Platforms")],
        [KeyboardButton("📥 Queues"), KeyboardButton("⏰ Schedules")],
        [KeyboardButton("📊 Statistics"), KeyboardButton("⏸️ Pause / Resume")],
        [KeyboardButton("🧭 Review Unknown"), KeyboardButton("🗑️ Remove Channel")],
        [KeyboardButton("🧹 Clear Queue"), KeyboardButton("❓ Help")],
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, is_persistent=True)


def is_owner(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id == OWNER_ID)


async def send_unauthorized(update: Update):
    msg = "⛔ <b>Access Denied</b>: You are not authorized to control this bot."
    if update.callback_query:
        await update.callback_query.answer("Access Denied", show_alert=True)
        if update.callback_query.message:
            await update.callback_query.message.reply_text(msg, parse_mode=ParseMode.HTML)
    elif update.effective_message:
        await update.effective_message.reply_text(msg, parse_mode=ParseMode.HTML)


async def safe_edit_text(query, text: str, reply_markup: InlineKeyboardMarkup = None):
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.warning("safe_edit_text: %s", e)


# ==============================================================================
# FLASK SERVER
# ==============================================================================
flask_app = Flask(__name__)

@flask_app.route("/")
def health_check():
    try:
        with get_db() as conn:
            paused = conn.execute("SELECT value FROM settings WHERE key='is_paused';").fetchone()[0]
            q_count = conn.execute("SELECT COUNT(*) FROM queued_posts WHERE status='pending';").fetchone()[0]
        return jsonify({
            "status": "healthy",
            "bot_active": paused == "0",
            "pending_queue": q_count,
            "timezone": TIMEZONE_STR,
            "timestamp": datetime.datetime.now(IST).isoformat(),
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def run_flask_server():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# ==============================================================================
# DISPATCHER LOOP
# ==============================================================================
async def send_post_to_channel(bot, channel_id: int, post: sqlite3.Row, platform: sqlite3.Row) -> tuple[bool, str | None]:
    m_type = post["message_type"]
    file_id = post["file_id"]
    final_caption = build_final_caption(platform, post["detected_urls"])

    # Caption length guard for Telegram API limits
    if final_caption and len(final_caption) > 1024 and m_type != "text":
        final_caption = final_caption[:1020] + "..."

    try:
        if m_type == "text":
            text_to_send = final_caption if final_caption.strip() else (post["content_text"] or "")
            if len(text_to_send) > 4096:
                text_to_send = text_to_send[:4090] + "..."
            await bot.send_message(
                chat_id=channel_id,
                text=text_to_send,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )
        elif m_type == "photo":
            await bot.send_photo(chat_id=channel_id, photo=file_id, caption=final_caption, parse_mode=ParseMode.HTML)
        elif m_type == "video":
            await bot.send_video(chat_id=channel_id, video=file_id, caption=final_caption, parse_mode=ParseMode.HTML)
        elif m_type == "document":
            await bot.send_document(chat_id=channel_id, document=file_id, caption=final_caption, parse_mode=ParseMode.HTML)
        elif m_type == "animation":
            await bot.send_animation(chat_id=channel_id, animation=file_id, caption=final_caption, parse_mode=ParseMode.HTML)
        else:
            return False, f"Unsupported message type: {m_type}"
        return True, None
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        return False, f"RetryAfter {e.retry_after}s"
    except Forbidden:
        return False, "Bot lack admin rights or was removed"
    except TelegramError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


async def dispatcher_job(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    now_ist = datetime.datetime.now(IST)
    current_time_str = now_ist.strftime("%H:%M")
    today_str = now_ist.strftime("%Y-%m-%d")

    try:
        with get_db() as conn:
            global_paused = conn.execute("SELECT value FROM settings WHERE key='is_paused';").fetchone()[0]
            if global_paused == "1":
                return

            platforms = conn.execute("SELECT * FROM platforms WHERE is_paused = 0;").fetchall()

            for plat in platforms:
                p_id = plat["id"]
                batch_limit = plat["batch_limit"] or 5

                schedules = conn.execute("SELECT * FROM schedules WHERE platform_id = ?;", (p_id,)).fetchall()

                should_dispatch = False
                if not schedules:
                    should_dispatch = True
                else:
                    for sch in schedules:
                        if sch["schedule_time"] == current_time_str:
                            if sch["last_dispatched_date"] != f"{today_str}_{current_time_str}":
                                should_dispatch = True
                                conn.execute(
                                    "UPDATE schedules SET last_dispatched_date = ? WHERE id = ?;",
                                    (f"{today_str}_{current_time_str}", sch["id"]),
                                )
                                conn.commit()
                                break

                if not should_dispatch:
                    continue

                assigned_channels = conn.execute(
                    """
                    SELECT c.channel_id, c.title
                    FROM channels c
                    JOIN platform_channels pc ON c.channel_id = pc.channel_id
                    WHERE pc.platform_id = ? AND c.can_post = 1;
                    """,
                    (p_id,),
                ).fetchall()

                if not assigned_channels:
                    continue

                posts = conn.execute(
                    """
                    SELECT * FROM queued_posts
                    WHERE platform_id = ? AND status = 'pending'
                    ORDER BY id ASC
                    LIMIT ?;
                    """,
                    (p_id, batch_limit),
                ).fetchall()

                if not posts:
                    continue

                for post in posts:
                    post_id = post["id"]
                    all_successful = True

                    for ch in assigned_channels:
                        c_id = ch["channel_id"]
                        success, err_msg = await send_post_to_channel(bot, c_id, post, plat)

                        conn.execute(
                            """
                            INSERT INTO delivery_logs (post_id, platform_id, channel_id, status, error_message)
                            VALUES (?, ?, ?, ?, ?);
                            """,
                            (post_id, p_id, c_id, "success" if success else "failed", err_msg),
                        )

                        if not success:
                            all_successful = False
                            if err_msg and "Bot lack admin rights" in err_msg:
                                conn.execute("UPDATE channels SET can_post = 0 WHERE channel_id = ?;", (c_id,))

                        await asyncio.sleep(0.5)

                    new_status = "sent" if all_successful else "failed"
                    conn.execute("UPDATE queued_posts SET status = ? WHERE id = ?;", (new_status, post_id))
                    conn.commit()
    except Exception as e:
        logger.error("Dispatcher loop exception: %s", e)


# ==============================================================================
# AUTO CHANNEL DISCOVERY
# ==============================================================================
async def chat_member_updated_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result: ChatMemberUpdated = update.my_chat_member
    if not result:
        return

    chat = result.chat
    new_member = result.new_chat_member

    if chat.type != "channel":
        return

    is_admin = new_member.status in [ChatMember.ADMINISTRATOR, "administrator"]
    can_post = getattr(new_member, "can_post_messages", True) if is_admin else False

    try:
        with get_db() as conn:
            if is_admin:
                conn.execute(
                    """
                    INSERT INTO channels (channel_id, title, username, can_post)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        title = excluded.title,
                        username = excluded.username,
                        can_post = excluded.can_post;
                    """,
                    (chat.id, chat.title or "Untitled Channel", chat.username, 1 if can_post else 0),
                )
                conn.commit()

                await context.bot.send_message(
                    chat_id=OWNER_ID,
                    text=(
                        f"🎉 <b>Channel Registered Successfully!</b>\n\n"
                        f"📢 <b>Title:</b> {html.escape(chat.title or '')}\n"
                        f"🆔 <b>ID:</b> <code>{chat.id}</code>\n"
                        f"🔗 <b>Username:</b> @{chat.username or 'Private'}\n"
                        f"✍️ <b>Posting Rights:</b> {'✅ Granted' if can_post else '❌ Denied'}\n\n"
                        f"Ekhon <b>⚙️ Manage Platforms</b> theke eta assign korte parben."
                    ),
                    parse_mode=ParseMode.HTML,
                )
            else:
                conn.execute("DELETE FROM channels WHERE channel_id = ?;", (chat.id,))
                conn.commit()
    except Exception as e:
        logger.error("Chat member update error: %s", e)


# ==============================================================================
# UNIVERSAL MESSAGE DISPATCHER & STATE MACHINE (Anti-Crash & Guaranteed Queue)
# ==============================================================================
async def universal_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await send_unauthorized(update)
        return

    msg = update.effective_message
    if not msg:
        return

    text = msg.text or ""
    current_state = context.user_data.get("state")

    # Detect if message is forwarded from anywhere
    is_forwarded = bool(
        getattr(msg, "forward_origin", None)
        or getattr(msg, "forward_date", None)
        or getattr(msg, "forward_from", None)
        or getattr(msg, "forward_from_chat", None)
    )

    # 1. Handle Top-Level ReplyKeyboardMarkup Clicks
    if text in [
        "➕ Add Channel", "📢 Channels", "➕ Add Platform", "⚙️ Manage Platforms",
        "📥 Queues", "⏰ Schedules", "📊 Statistics", "⏸️ Pause / Resume",
        "🧭 Review Unknown", "🗑️ Remove Channel", "🧹 Clear Queue", "❓ Help"
    ]:
        context.user_data.clear()
        if text == "➕ Add Channel":
            await add_channel_instruction(update, context)
        elif text == "📢 Channels":
            await list_channels_handler(update, context)
        elif text == "➕ Add Platform":
            await add_platform_start(update, context)
        elif text == "⚙️ Manage Platforms":
            await manage_platforms_handler(update, context)
        elif text == "📥 Queues":
            await queues_handler(update, context)
        elif text == "⏰ Schedules":
            await schedules_main_handler(update, context)
        elif text == "📊 Statistics":
            await statistics_handler(update, context)
        elif text == "⏸️ Pause / Resume":
            await toggle_global_pause(update, context)
        elif text == "🧭 Review Unknown":
            await review_unknown_handler(update, context)
        elif text == "🗑️ Remove Channel":
            await remove_channel_menu(update, context)
        elif text == "🧹 Clear Queue":
            await clear_queue_prompt(update, context)
        elif text == "❓ Help":
            await help_handler(update, context)
        return

    # 2. Handle Form Inputs ONLY IF NOT A FORWARDED POST
    if current_state and not is_forwarded:
        if text.strip().lower() == "/cancel":
            context.user_data.clear()
            await msg.reply_text("❌ Action cancel kora hoyeche.", reply_markup=build_main_menu())
            return

        if current_state == "PLATFORM_NAME":
            name = text.strip()
            with get_db() as conn:
                exists = conn.execute("SELECT id FROM platforms WHERE LOWER(name) = LOWER(?);", (name,)).fetchone()
            if exists:
                await msg.reply_text(f"'{name}' name-e platform already ache. Onno name din:\n/cancel likhle cancel hobe.")
                return
            context.user_data["new_plat_name"] = name
            context.user_data["state"] = "PLATFORM_DOMAINS"
            await msg.reply_text(
                f"Platform: <b>{html.escape(name)}</b>\n\nDomain gulo comma diye likhun (e.g. <code>flezen.com, links.flezen.net</code>):",
                parse_mode=ParseMode.HTML,
            )
            return

        elif current_state == "PLATFORM_DOMAINS":
            raw_domains = [d.strip() for d in text.split(",") if d.strip()]
            p_name = context.user_data.get("new_plat_name")
            if not raw_domains:
                await msg.reply_text("At least ekta valid domain din:")
                return
            normalized = [normalize_domain(d) for d in raw_domains]
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO platforms (name) VALUES (?);", (p_name,))
                platform_id = cursor.lastrowid
                saved_domains = []
                for dom in set(normalized):
                    try:
                        cursor.execute("INSERT INTO platform_domains (platform_id, domain) VALUES (?, ?);", (platform_id, dom))
                        saved_domains.append(dom)
                    except sqlite3.IntegrityError:
                        pass
                conn.commit()
            context.user_data.clear()
            await msg.reply_text(
                f"✅ <b>Platform Created!</b>\nPlatform: <code>{html.escape(p_name)}</code>\nDomains: <code>{', '.join(saved_domains)}</code>",
                reply_markup=build_main_menu(),
                parse_mode=ParseMode.HTML,
            )
            return

        elif current_state == "ADD_DOMAIN":
            p_id = context.user_data.get("active_p_id")
            dom = normalize_domain(text)
            with get_db() as conn:
                try:
                    conn.execute("INSERT INTO platform_domains (platform_id, domain) VALUES (?, ?);", (p_id, dom))
                    conn.commit()
                    out = f"✅ Domain <code>{html.escape(dom)}</code> add hoyeche."
                except sqlite3.IntegrityError:
                    out = f"⚠️ Domain <code>{html.escape(dom)}</code> already ache."
            context.user_data.clear()
            await msg.reply_text(out, reply_markup=build_main_menu(), parse_mode=ParseMode.HTML)
            return

        elif current_state == "SET_HEADER":
            p_id = context.user_data.get("active_p_id")
            val = None if text.strip().lower() == "/skip" else text.strip()
            with get_db() as conn:
                conn.execute("UPDATE platforms SET header_text = ? WHERE id = ?;", (val, p_id))
                conn.commit()
            context.user_data.clear()
            await msg.reply_text("✅ Header updated successfully!", reply_markup=build_main_menu())
            return

        elif current_state == "SET_FOOTER":
            p_id = context.user_data.get("active_p_id")
            val = None if text.strip().lower() == "/skip" else text.strip()
            with get_db() as conn:
                conn.execute("UPDATE platforms SET footer_text = ? WHERE id = ?;", (val, p_id))
                conn.commit()
            context.user_data.clear()
            await msg.reply_text("✅ Footer updated successfully!", reply_markup=build_main_menu())
            return

        elif current_state == "SET_BATCH":
            p_id = context.user_data.get("active_p_id")
            try:
                val = int(text.strip())
                if not (1 <= val <= 50):
                    raise ValueError()
            except ValueError:
                await msg.reply_text("1 theke 50 er moddhe integer number din:")
                return
            with get_db() as conn:
                conn.execute("UPDATE platforms SET batch_limit = ? WHERE id = ?;", (val, p_id))
                conn.commit()
            context.user_data.clear()
            await msg.reply_text(f"✅ Batch limit set to: {val}", reply_markup=build_main_menu())
            return

        elif current_state == "SET_BACKUP":
            p_id = context.user_data.get("active_p_id")
            link = None if text.strip().lower() == "/skip" else text.strip()
            with get_db() as conn:
                conn.execute("UPDATE platforms SET backup_link = ? WHERE id = ?;", (link, p_id))
                conn.commit()
            context.user_data.clear()
            await msg.reply_text(f"✅ Backup link set: {link or 'None'}", reply_markup=build_main_menu())
            return

    # 3. If Forwarded or Standard Post -> Clear hanging state and process queue!
    context.user_data.clear()

    m_type = "text"
    file_id = None
    caption = msg.caption
    content_text = msg.text
    full_content = (caption or "") + "\n" + (content_text or "")

    if msg.photo:
        m_type = "photo"
        file_id = msg.photo[-1].file_id
    elif msg.video:
        m_type = "video"
        file_id = msg.video.file_id
    elif msg.document:
        m_type = "document"
        file_id = msg.document.file_id
    elif msg.animation:
        m_type = "animation"
        file_id = msg.animation.file_id

    # Comprehensive URL extraction
    urls_found = extract_urls(full_content)
    entities = (msg.entities or ()) + (msg.caption_entities or ())
    for ent in entities:
        ent_type_str = str(ent.type.value if hasattr(ent.type, "value") else ent.type).lower()
        if "text_link" in ent_type_str and ent.url:
            if ent.url not in urls_found:
                urls_found.append(ent.url)
        elif "url" in ent_type_str:
            raw_u = full_content[ent.offset : ent.offset + ent.length].strip()
            if raw_u and raw_u not in urls_found:
                if not raw_u.startswith("http://") and not raw_u.startswith("https://"):
                    raw_u = "https://" + raw_u
                urls_found.append(raw_u)

    matched_platform_id = None
    matched_platform_name = None
    matching_urls = []

    try:
        with get_db() as conn:
            all_domains = conn.execute(
                """
                SELECT pd.domain, pd.platform_id, p.name
                FROM platform_domains pd
                JOIN platforms p ON pd.platform_id = p.id;
                """
            ).fetchall()

            for u in urls_found:
                norm_u = normalize_domain(u)
                for row in all_domains:
                    stored_dom = row["domain"].lower().strip()
                    if norm_u == stored_dom or norm_u.endswith("." + stored_dom) or stored_dom in u.lower():
                        if not matched_platform_id:
                            matched_platform_id = row["platform_id"]
                            matched_platform_name = row["name"]
                        if u not in matching_urls:
                            matching_urls.append(u)
                        break

            entities_json = serialize_entities(entities)
            saved_urls_list = matching_urls if matching_urls else urls_found

            # Remove duplicate links maintaining order
            seen_u = set()
            unique_saved_urls = []
            for u in saved_urls_list:
                if u not in seen_u:
                    seen_u.add(u)
                    unique_saved_urls.append(u)

            detected_urls_str = ",".join(unique_saved_urls)

            conn.execute(
                """
                INSERT INTO queued_posts (platform_id, message_type, file_id, content_text, caption, entities_json, detected_urls, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending');
                """,
                (matched_platform_id, m_type, file_id, content_text, caption, entities_json, detected_urls_str),
            )
            conn.commit()

            # Information status for user
            if matched_platform_id:
                assigned_count = conn.execute("SELECT COUNT(*) FROM platform_channels WHERE platform_id = ?;", (matched_platform_id,)).fetchone()[0]
                schedule_count = conn.execute("SELECT COUNT(*) FROM schedules WHERE platform_id = ?;", (matched_platform_id,)).fetchone()[0]

                timing_info = f"⏰ Scheduled ({schedule_count} ti time slot)" if schedule_count > 0 else "⚡ Continuous (Every 15s)"
                chan_warning = "\n⚠️ <i>Warning: Ei platform-e ekhono kono channel assign kora nei! Manage Platforms theke channel add korun.</i>" if assigned_count == 0 else ""

                await msg.reply_text(
                    f"✅ <b>Post Queued for Platform:</b> <code>{html.escape(matched_platform_name)}</code>\n"
                    f"🔗 <b>Captured Links:</b> {len(unique_saved_urls)} ti (Part-wise format)\n"
                    f"📦 <b>Type:</b> <code>{m_type.upper()}</code>\n"
                    f"⏱️ <b>Dispatch:</b> {timing_info}{chan_warning}",
                    parse_mode=ParseMode.HTML,
                )
            else:
                await msg.reply_text(
                    "⚠️ <b>Post Added to Unknown Queue!</b>\n\n"
                    f"Detected URLs: <code>{html.escape(str(urls_found) if urls_found else 'None')}</code>\n"
                    "Kono platform domain match koreni.\n"
                    "Main menu theke <b>🧭 Review Unknown</b> e giye eta assign korte paren.",
                    parse_mode=ParseMode.HTML,
                )
    except Exception as e:
        logger.exception("Error during post queueing: %s", e)
        await msg.reply_text(f"⚠️ Internal error while queueing post: <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)


# ==============================================================================
# MENU IMPLEMENTATIONS
# ==============================================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if not is_owner(update):
        await send_unauthorized(update)
        return
    await update.effective_message.reply_text(
        "🚀 <b>Telegram Multi-Platform Channel Dispatcher Bot</b>\n\n"
        "• Automatically capture, filter, clean captions, and dispatch.\n"
        "• Timezone: <code>Asia/Kolkata</code> (IST)\n\n"
        "Nicher menu theke select korun:",
        reply_markup=build_main_menu(),
        parse_mode=ParseMode.HTML,
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "📖 <b>Operator Manual</b>\n\n"
        "1. <b>Add Channel:</b> Bot ke channel-e administrator banan (post permission on thakbe).\n"
        "2. <b>Add Platform:</b> Platform toiri korun ebong matching domain din.\n"
        "3. <b>Header & Footer:</b> Manage Platforms-e giye Platform Header ebong Footer set korun.\n"
        "4. <b>Schedules:</b> 24-hour chart theke 30 min ontor time select korun.\n"
        "5. <b>Auto-Queue:</b> Direct forward kora post theke ager caption muche Header + Clean Link + Footer toiri hoye post hobe.",
        reply_markup=build_main_menu(),
        parse_mode=ParseMode.HTML,
    )


async def add_channel_instruction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "➕ <b>Channel Connect Korar Niyom:</b>\n\n"
        "1. Apnar Telegram Channel-e jan.\n"
        "2. <b>Channel Settings ➔ Administrators ➔ Add Administrator</b>-e click korun.\n"
        "3. Bot ke search kore admin hisebe add korun.\n"
        "4. <b>Post Messages</b> permission on rekhe Save korun.\n"
        "5. Bot sathe sathe detect kore channel register kore nebe.",
        reply_markup=build_main_menu(),
        parse_mode=ParseMode.HTML,
    )


async def list_channels_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        channels = conn.execute("SELECT * FROM channels ORDER BY created_at DESC;").fetchall()

    if not channels:
        await update.effective_message.reply_text(
            "📢 Kono channel add kora nei. <b>➕ Add Channel</b> instruction dekhun.",
            reply_markup=build_main_menu(),
            parse_mode=ParseMode.HTML,
        )
        return

    lines = ["📢 <b>Registered Telegram Channels:</b>\n"]
    for idx, ch in enumerate(channels, 1):
        status = "✅ Active" if ch["can_post"] == 1 else "⚠️ Permission Missing"
        uname = f"@{ch['username']}" if ch["username"] else "Private"
        lines.append(f"{idx}. <b>{html.escape(ch['title'])}</b>\n   🆔 <code>{ch['channel_id']}</code> | {uname} | {status}")

    await update.effective_message.reply_text("\n\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=build_main_menu())


async def remove_channel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        channels = conn.execute("SELECT channel_id, title FROM channels;").fetchall()

    if not channels:
        await update.effective_message.reply_text("Remove korar moto kono channel nei.", reply_markup=build_main_menu())
        return

    keyboard = [[InlineKeyboardButton(f"🗑️ Remove: {ch['title']}", callback_data=f"delchan_{ch['channel_id']}")] for ch in channels]
    await update.effective_message.reply_text("Kono channel disconnect korte chaile select korun:", reply_markup=InlineKeyboardMarkup(keyboard))


async def remove_channel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    channel_id = int(query.data.split("_")[1])
    with get_db() as conn:
        conn.execute("DELETE FROM channels WHERE channel_id = ?;", (channel_id,))
        conn.commit()
    await safe_edit_text(query, f"✅ Channel <code>{channel_id}</code> removed successfully.")


async def toggle_global_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        current = conn.execute("SELECT value FROM settings WHERE key='is_paused';").fetchone()[0]
        new_val = "1" if current == "0" else "0"
        conn.execute("UPDATE settings SET value = ? WHERE key='is_paused';", (new_val,))
        conn.commit()

    status_str = "⏸️ <b>DISPATCHER PAUSED</b>" if new_val == "1" else "▶️ <b>DISPATCHER RESUMED & ACTIVE</b>"
    await update.effective_message.reply_text(
        f"{status_str}\n\nAutomated dispatching ekhon {'bondho ache' if new_val == '1' else 'chalu ache'}.",
        reply_markup=build_main_menu(),
        parse_mode=ParseMode.HTML,
    )


async def statistics_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        total_channels = conn.execute("SELECT COUNT(*) FROM channels;").fetchone()[0]
        total_platforms = conn.execute("SELECT COUNT(*) FROM platforms;").fetchone()[0]
        total_queued = conn.execute("SELECT COUNT(*) FROM queued_posts WHERE status='pending';").fetchone()[0]
        total_delivered = conn.execute("SELECT COUNT(*) FROM delivery_logs WHERE status='success';").fetchone()[0]
        total_failed = conn.execute("SELECT COUNT(*) FROM delivery_logs WHERE status='failed';").fetchone()[0]
        platform_stats = conn.execute(
            """
            SELECT p.name,
                   (SELECT COUNT(*) FROM queued_posts qp WHERE qp.platform_id = p.id AND qp.status = 'pending') as pending_count,
                   (SELECT COUNT(*) FROM delivery_logs dl WHERE dl.platform_id = p.id AND dl.status = 'success') as delivered_count
            FROM platforms p;
            """
        ).fetchall()
        today = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        today_deliveries = conn.execute("SELECT COUNT(*) FROM delivery_logs WHERE status='success' AND delivered_at LIKE ?;", (f"{today}%",)).fetchone()[0]

    report = [
        "📊 <b>System Dispatcher Analytics</b>\n",
        f"📢 <b>Registered Channels:</b> {total_channels}",
        f"🌐 <b>Platforms:</b> {total_platforms}",
        f"📥 <b>Pending in Queue:</b> {total_queued}",
        f"✅ <b>Total Delivered:</b> {total_delivered}",
        f"❌ <b>Delivery Failures:</b> {total_failed}",
        f"📈 <b>Delivered Today ({today}):</b> {today_deliveries}\n",
        "<b>Platform-wise Breakdown:</b>",
    ]
    for ps in platform_stats:
        report.append(f"• <b>{html.escape(ps['name'])}</b>: {ps['pending_count']} pending | {ps['delivered_count']} sent")

    await update.effective_message.reply_text("\n".join(report), reply_markup=build_main_menu(), parse_mode=ParseMode.HTML)


async def queues_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        total_pending = conn.execute("SELECT COUNT(*) FROM queued_posts WHERE status='pending';").fetchone()[0]
        unknown_pending = conn.execute("SELECT COUNT(*) FROM queued_posts WHERE platform_id IS NULL AND status='pending';").fetchone()[0]
        sent_count = conn.execute("SELECT COUNT(*) FROM queued_posts WHERE status='sent';").fetchone()[0]
        failed_count = conn.execute("SELECT COUNT(*) FROM queued_posts WHERE status='failed';").fetchone()[0]
        platforms = conn.execute(
            """
            SELECT p.name, COUNT(q.id) as q_count
            FROM platforms p
            LEFT JOIN queued_posts q ON p.id = q.platform_id AND q.status = 'pending'
            GROUP BY p.id;
            """
        ).fetchall()

    lines = [
        "📥 <b>Queue Overview</b>\n",
        f"• <b>Total Pending:</b> {total_pending}",
        f"• <b>Unknown Queue:</b> {unknown_pending}",
        f"• <b>Completed:</b> {sent_count}",
        f"• <b>Failed:</b> {failed_count}\n",
        "<b>Platforms:</b>",
    ]
    for p in platforms:
        lines.append(f"• <b>{html.escape(p['name'])}</b>: {p['q_count']} pending")

    await update.effective_message.reply_text("\n".join(lines), reply_markup=build_main_menu(), parse_mode=ParseMode.HTML)


async def clear_queue_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        platforms = conn.execute("SELECT id, name FROM platforms;").fetchall()

    keyboard = [[InlineKeyboardButton(f"🧹 Clear: {p['name']}", callback_data=f"flushq_{p['id']}")] for p in platforms]
    keyboard.append([InlineKeyboardButton("🧹 Clear Unknown Queue", callback_data="flushq_unknown")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="close_dialog")])
    await update.effective_message.reply_text("Kon queue clear korte chan select korun:", reply_markup=InlineKeyboardMarkup(keyboard))


async def clear_queue_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("_")[1]
    with get_db() as conn:
        if data == "unknown":
            conn.execute("DELETE FROM queued_posts WHERE platform_id IS NULL AND status='pending';")
            target = "Unknown Queue"
        else:
            plat_id = int(data)
            conn.execute("DELETE FROM queued_posts WHERE platform_id = ? AND status='pending';", (plat_id,))
            target = f"Platform ID {plat_id}"
        conn.commit()
    await safe_edit_text(query, f"✅ <b>{html.escape(target)}</b> queue cleared.")


async def review_unknown_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        post = conn.execute(
            """
            SELECT * FROM queued_posts
            WHERE platform_id IS NULL AND status = 'pending'
            ORDER BY id ASC LIMIT 1;
            """
        ).fetchone()

    if not post:
        await update.effective_message.reply_text(
            "🧭 <b>Unknown Queue Empty!</b>\nKono unmatched post pending nei.",
            reply_markup=build_main_menu(),
            parse_mode=ParseMode.HTML,
        )
        return

    preview_text = html.escape((post["content_text"] or post["caption"] or f"[{post['message_type'].upper()} MEDIA]")[:200])
    urls = html.escape(post["detected_urls"] or "None")

    text = (
        f"🧭 <b>Review Unmatched Post (ID: {post['id']})</b>\n\n"
        f"📅 <b>Received:</b> {post['created_at']}\n"
        f"📦 <b>Type:</b> {post['message_type'].upper()}\n"
        f"🔗 <b>URLs:</b> <code>{urls}</code>\n\n"
        f"📝 <b>Preview:</b>\n{preview_text}\n\n"
        f"Action select korun:"
    )

    keyboard = [
        [InlineKeyboardButton("➕ Assign to Platform", callback_data=f"uk_assign_{post['id']}")],
        [InlineKeyboardButton("🗑️ Delete Post", callback_data=f"uk_delete_{post['id']}")],
        [InlineKeyboardButton("⏩ Skip", callback_data="uk_skip")],
    ]
    await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)


async def unknown_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "uk_skip":
        await safe_edit_text(query, "Post skipped.")
        return

    if data.startswith("uk_delete_"):
        post_id = int(data.split("_")[2])
        with get_db() as conn:
            conn.execute("DELETE FROM queued_posts WHERE id = ?;", (post_id,))
            conn.commit()
        await safe_edit_text(query, f"🗑️ Post <code>{post_id}</code> Unknown Queue theke delete kora hoyeche.")
        return

    if data.startswith("uk_assign_"):
        post_id = int(data.split("_")[2])
        with get_db() as conn:
            platforms = conn.execute("SELECT id, name FROM platforms;").fetchall()

        if not platforms:
            await safe_edit_text(query, "Kono platform nei. Age platform toiri korun.")
            return

        keyboard = [[InlineKeyboardButton(p["name"], callback_data=f"uk_set_{post_id}_{p['id']}")] for p in platforms]
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="close_dialog")])
        await safe_edit_text(query, "Kon platform-e assign korte chan?", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("uk_set_"):
        _, _, post_id_str, plat_id_str = data.split("_")
        post_id = int(post_id_str)
        plat_id = int(plat_id_str)
        with get_db() as conn:
            conn.execute("UPDATE queued_posts SET platform_id = ? WHERE id = ?;", (plat_id, post_id))
            plat_name = conn.execute("SELECT name FROM platforms WHERE id = ?;", (plat_id,)).fetchone()[0]
            conn.commit()
        await safe_edit_text(query, f"✅ Post <code>{post_id}</code> platform <b>{html.escape(plat_name)}</b>-e assign hoyeche.")


# ==============================================================================
# PLATFORM MANAGEMENT
# ==============================================================================
async def add_platform_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = "PLATFORM_NAME"
    await update.effective_message.reply_text(
        "➕ <b>Create New Platform (Step 1/2)</b>\n\n"
        "Platform-er unique name likhun (e.g. <code>Flezen</code>):\n\n"
        "/cancel likhle cancel hobe.",
        parse_mode=ParseMode.HTML,
    )


async def manage_platforms_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        platforms = conn.execute("SELECT id, name, is_paused FROM platforms;").fetchall()

    if not platforms:
        await update.effective_message.reply_text("⚙️ Kono platform toiri kora nei. <b>➕ Add Platform</b> click korun.", reply_markup=build_main_menu(), parse_mode=ParseMode.HTML)
        return

    keyboard = [[InlineKeyboardButton(f"{'⏸️' if p['is_paused'] else '▶️'} {p['name']}", callback_data=f"plat_{p['id']}")] for p in platforms]
    text = "⚙️ <b>Manage Platforms</b>\nPlatform select kore Header, Footer, Channel ba Schedule set korun:"
    if update.callback_query:
        await safe_edit_text(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)


async def platform_selected_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])

    with get_db() as conn:
        plat = conn.execute("SELECT * FROM platforms WHERE id = ?;", (p_id,)).fetchone()
        domains = conn.execute("SELECT domain FROM platform_domains WHERE platform_id = ?;", (p_id,)).fetchall()
        channel_count = conn.execute("SELECT COUNT(*) FROM platform_channels WHERE platform_id = ?;", (p_id,)).fetchone()[0]
        schedule_count = conn.execute("SELECT COUNT(*) FROM schedules WHERE platform_id = ?;", (p_id,)).fetchone()[0]

    if not plat:
        await safe_edit_text(query, "Platform khuje paoa jayni.")
        return

    dom_list = ", ".join([d["domain"] for d in domains]) or "None"
    status_str = "⏸️ Paused" if plat["is_paused"] else "▶️ Active"

    text = (
        f"⚙️ <b>Platform Control:</b> <code>{html.escape(plat['name'])}</code>\n\n"
        f"• <b>Status:</b> {status_str}\n"
        f"• <b>Header:</b> <code>{html.escape((plat['header_text'] or 'Not Set')[:30])}</code>\n"
        f"• <b>Footer:</b> <code>{html.escape((plat['footer_text'] or 'Not Set')[:30])}</code>\n"
        f"• <b>Batch Limit:</b> {plat['batch_limit']} posts/cycle\n"
        f"• <b>Backup Link:</b> {html.escape(plat['backup_link'] or 'Not Set')}\n"
        f"• <b>Assigned Channels:</b> {channel_count}\n"
        f"• <b>Active Schedules:</b> {schedule_count}\n"
        f"• <b>Domains:</b> <code>{html.escape(dom_list)}</code>\n\n"
        f"Nicher option theke select korun:"
    )

    pause_label = "▶️ Resume Platform" if plat["is_paused"] else "⏸️ Pause Platform"

    keyboard = [
        [InlineKeyboardButton("📢 Assign Channels", callback_data=f"pchan_{p_id}")],
        [InlineKeyboardButton("📝 Set Header", callback_data=f"phead_{p_id}"), InlineKeyboardButton("📄 Set Footer", callback_data=f"pfoot_{p_id}")],
        [InlineKeyboardButton("👁️ Preview Post Format", callback_data=f"pprev_{p_id}")],
        [InlineKeyboardButton("🌐 Manage Domains", callback_data=f"pdom_{p_id}")],
        [InlineKeyboardButton("⏰ Set Schedule (24h Chart)", callback_data=f"pschgrid_{p_id}")],
        [InlineKeyboardButton("🔢 Set Batch Limit", callback_data=f"pbatch_{p_id}"), InlineKeyboardButton("🔗 Set Backup Link", callback_data=f"pback_{p_id}")],
        [InlineKeyboardButton("📊 Platform Statistics", callback_data=f"pstat_{p_id}")],
        [InlineKeyboardButton(pause_label, callback_data=f"ptoggle_{p_id}")],
        [InlineKeyboardButton("🗑️ Delete Platform", callback_data=f"pdel_{p_id}")],
        [InlineKeyboardButton("⬅️ Back to Platforms", callback_data="pback_list")],
    ]
    await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))


async def platform_preview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])

    with get_db() as conn:
        plat = conn.execute("SELECT * FROM platforms WHERE id = ?;", (p_id,)).fetchone()
        domains = conn.execute("SELECT domain FROM platform_domains WHERE platform_id = ?;", (p_id,)).fetchall()

    first_dom = domains[0]["domain"] if domains else "flezen.com"
    dummy_urls = f"https://{first_dom}/sample1,https://{first_dom}/sample2"
    preview_output = build_final_caption(plat, dummy_urls)

    text = (
        f"👁️ <b>Live Preview for:</b> <code>{html.escape(plat['name'])}</code>\n"
        f"----------------------------------------\n\n"
        f"{preview_output}\n\n"
        f"----------------------------------------\n"
        f"Channel-e post dispatch korar somoy thik ei format-e caption jabe."
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back to Platform", callback_data=f"plat_{p_id}")]]
    await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))


async def platform_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    p_id = int(query.data.split("_")[1])
    with get_db() as conn:
        plat = conn.execute("SELECT is_paused FROM platforms WHERE id = ?;", (p_id,)).fetchone()
        new_val = 0 if plat["is_paused"] == 1 else 1
        conn.execute("UPDATE platforms SET is_paused = ? WHERE id = ?;", (new_val, p_id))
        conn.commit()
    await platform_selected_callback(update, context)


async def platform_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])
    with get_db() as conn:
        conn.execute("DELETE FROM platforms WHERE id = ?;", (p_id,))
        conn.commit()
    await safe_edit_text(query, "🗑️ Platform deleted successfully.")


async def platform_channels_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])

    with get_db() as conn:
        plat = conn.execute("SELECT name FROM platforms WHERE id = ?;", (p_id,)).fetchone()
        all_channels = conn.execute("SELECT channel_id, title FROM channels ORDER BY title ASC;").fetchall()
        assigned_ids = {row[0] for row in conn.execute("SELECT channel_id FROM platform_channels WHERE platform_id = ?;", (p_id,)).fetchall()}

    keyboard = []
    if all_channels:
        for ch in all_channels:
            cid = ch["channel_id"]
            is_assigned = cid in assigned_ids
            action = "unassign" if is_assigned else "assign"
            keyboard.append([InlineKeyboardButton(f"{'✅' if is_assigned else '❌'} {ch['title']}", callback_data=f"pch_{p_id}_{cid}_{action}")])
    else:
        keyboard.append([InlineKeyboardButton("➕ Age Channel Add Korun", callback_data="none")])

    keyboard.append([InlineKeyboardButton("⬅️ Back to Platform", callback_data=f"plat_{p_id}")])
    await safe_edit_text(
        query,
        f"📢 <b>Assign Channels to:</b> <code>{html.escape(plat['name'])}</code>\n\nClick korle Toggle (✅ On / ❌ Off) hobe:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def platform_channel_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    p_id, c_id, action = int(parts[1]), int(parts[2]), parts[3]

    with get_db() as conn:
        if action == "assign":
            conn.execute("INSERT OR IGNORE INTO platform_channels (platform_id, channel_id) VALUES (?, ?);", (p_id, c_id))
        else:
            conn.execute("DELETE FROM platform_channels WHERE platform_id = ? AND channel_id = ?;", (p_id, c_id))
        conn.commit()

    query.data = f"pchan_{p_id}"
    await platform_channels_menu(update, context)


def generate_time_slots():
    return [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]


async def platform_schedule_grid_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])

    with get_db() as conn:
        plat = conn.execute("SELECT name FROM platforms WHERE id = ?;", (p_id,)).fetchone()
        active_times = {row[0] for row in conn.execute("SELECT schedule_time FROM schedules WHERE platform_id = ?;", (p_id,)).fetchall()}

    all_slots = generate_time_slots()
    keyboard = []
    row = []

    for slot in all_slots:
        is_active = slot in active_times
        label = f"✅ {slot}" if is_active else slot
        action = "rm" if is_active else "add"
        row.append(InlineKeyboardButton(label, callback_data=f"ts_{p_id}_{slot}_{action}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("🗑️ Clear All Times", callback_data=f"clrsch_{p_id}")])
    keyboard.append([InlineKeyboardButton("✅ Done (Save & Back)", callback_data=f"plat_{p_id}")])

    await safe_edit_text(
        query,
        f"⏰ <b>24-Hour Schedule Chart:</b> <code>{html.escape(plat['name'])}</code>\n"
        f"Timezone: <code>{TIMEZONE_STR}</code> (IST)\n\n"
        f"• Proti 30 minute ontor button click kore ON (✅) ebong OFF korun.\n"
        f"• Sesh hole <b>Done</b> click korun.\n"
        f"• <b>Active Slots:</b> {len(active_times)} ti.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def toggle_schedule_slot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    p_id, slot_time, action = int(parts[1]), parts[2], parts[3]

    with get_db() as conn:
        if action == "add":
            conn.execute("INSERT OR IGNORE INTO schedules (platform_id, schedule_time) VALUES (?, ?);", (p_id, slot_time))
        else:
            conn.execute("DELETE FROM schedules WHERE platform_id = ? AND schedule_time = ?;", (p_id, slot_time))
        conn.commit()

    query.data = f"pschgrid_{p_id}"
    await platform_schedule_grid_menu(update, context)


async def clear_all_schedules_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Shob time clear kora holo!")
    p_id = int(query.data.split("_")[1])
    with get_db() as conn:
        conn.execute("DELETE FROM schedules WHERE platform_id = ?;", (p_id,))
        conn.commit()
    query.data = f"pschgrid_{p_id}"
    await platform_schedule_grid_menu(update, context)


async def schedules_main_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_db() as conn:
        platforms = conn.execute("SELECT id, name, is_paused FROM platforms;").fetchall()
        schedules = conn.execute("SELECT platform_id, schedule_time FROM schedules ORDER BY schedule_time ASC;").fetchall()

    if not platforms:
        await update.effective_message.reply_text("Kono platform toiri kora nei.", reply_markup=build_main_menu())
        return

    lines = ["⏰ <b>Configured Schedules (Asia/Kolkata):</b>\n"]
    for p in platforms:
        p_times = [s["schedule_time"] for s in schedules if s["platform_id"] == p["id"]]
        times_str = ", ".join(p_times) if p_times else "Continuous (Every 15s)"
        status = "⏸️ Paused" if p["is_paused"] else "▶️ Active"
        lines.append(f"• <b>{html.escape(p['name'])}</b> ({status})\n  Times: <code>{times_str}</code>")

    lines.append("\nSchedule change korte: <b>⚙️ Manage Platforms ➔ Select Platform ➔ Set Schedule</b>-e jan.")
    await update.effective_message.reply_text("\n".join(lines), reply_markup=build_main_menu(), parse_mode=ParseMode.HTML)


async def platform_domains_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])

    with get_db() as conn:
        plat = conn.execute("SELECT name FROM platforms WHERE id = ?;", (p_id,)).fetchone()
        domains = conn.execute("SELECT id, domain FROM platform_domains WHERE platform_id = ?;", (p_id,)).fetchall()

    keyboard = [[InlineKeyboardButton(f"🗑️ Remove {d['domain']}", callback_data=f"deldom_{p_id}_{d['id']}")] for d in domains]
    keyboard.append([InlineKeyboardButton("➕ Add New Domain", callback_data=f"adddom_{p_id}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Platform", callback_data=f"plat_{p_id}")])

    await safe_edit_text(
        query,
        f"🌐 <b>Manage Domains:</b> <code>{html.escape(plat['name'])}</code>\n\nDomain delete korte click korun ba notun add korun:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def domain_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, p_id_str, d_id_str = query.data.split("_")
    p_id, d_id = int(p_id_str), int(d_id_str)
    with get_db() as conn:
        conn.execute("DELETE FROM platform_domains WHERE id = ?;", (d_id,))
        conn.commit()
    query.data = f"pdom_{p_id}"
    await platform_domains_menu(update, context)


async def platform_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])

    with get_db() as conn:
        plat = conn.execute("SELECT name FROM platforms WHERE id = ?;", (p_id,)).fetchone()
        pending = conn.execute("SELECT COUNT(*) FROM queued_posts WHERE platform_id = ? AND status='pending';", (p_id,)).fetchone()[0]
        delivered = conn.execute("SELECT COUNT(*) FROM delivery_logs WHERE platform_id = ? AND status='success';", (p_id,)).fetchone()[0]
        failed = conn.execute("SELECT COUNT(*) FROM delivery_logs WHERE platform_id = ? AND status='failed';", (p_id,)).fetchone()[0]

    text = (
        f"📊 <b>Platform Metrics:</b> <code>{html.escape(plat['name'])}</code>\n\n"
        f"• <b>Pending Queue:</b> {pending}\n"
        f"• <b>Successful Sent:</b> {delivered}\n"
        f"• <b>Failed:</b> {failed}"
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back to Platform", callback_data=f"plat_{p_id}")]]
    await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))


# ==============================================================================
# FSM LAUNCHERS
# ==============================================================================
async def launch_add_domain(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])
    context.user_data["active_p_id"] = p_id
    context.user_data["state"] = "ADD_DOMAIN"
    await query.message.reply_text("🌐 Notun domain likhun (e.g. <code>mirror.flezen.com</code>):\n/cancel likhle cancel hobe.", parse_mode=ParseMode.HTML)


async def launch_set_header(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])
    context.user_data["active_p_id"] = p_id
    context.user_data["state"] = "SET_HEADER"
    await query.message.reply_text(
        "📝 <b>Platform Header Text likhun:</b>\n\n"
        "Eta shob post-e link gular <b>OPOR-E</b> thakbe.\n"
        "Clear korte <code>/skip</code> | Cancel korte <code>/cancel</code>",
        parse_mode=ParseMode.HTML,
    )


async def launch_set_footer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])
    context.user_data["active_p_id"] = p_id
    context.user_data["state"] = "SET_FOOTER"
    await query.message.reply_text(
        "📄 <b>Platform Footer Text likhun:</b>\n\n"
        "Eta shob post-e link gular <b>NICHE</b> thakbe.\n"
        "Clear korte <code>/skip</code> | Cancel korte <code>/cancel</code>",
        parse_mode=ParseMode.HTML,
    )


async def launch_set_batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])
    context.user_data["active_p_id"] = p_id
    context.user_data["state"] = "SET_BATCH"
    await query.message.reply_text("🔢 Proti cycle-e koyti post send korte chan (1 - 50 sonkha din):")


async def launch_set_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    p_id = int(query.data.split("_")[1])
    context.user_data["active_p_id"] = p_id
    context.user_data["state"] = "SET_BACKUP"
    await query.message.reply_text("🔗 Backup URL din:\nClear korte <code>/skip</code> | Cancel korte <code>/cancel</code>", parse_mode=ParseMode.HTML)


async def close_dialog_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit_text(query, "Closed.")


# ==============================================================================
# GLOBAL ERROR SHIELD
# ==============================================================================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, BadRequest) and "Message is not modified" in str(context.error):
        return
    logger.error("System exception: %s", context.error, exc_info=context.error)


# ==============================================================================
# BOOTSTRAP
# ==============================================================================
def main():
    init_db()

    flask_thread = threading.Thread(target=run_flask_server, daemon=True)
    flask_thread.start()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(ChatMemberHandler(chat_member_updated_handler, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_handler))

    # All Inline Callback Handlers
    app.add_handler(CallbackQueryHandler(manage_platforms_handler, pattern=r"^pback_list$"))
    app.add_handler(CallbackQueryHandler(platform_selected_callback, pattern=r"^plat_\d+$"))
    app.add_handler(CallbackQueryHandler(platform_preview_callback, pattern=r"^pprev_\d+$"))
    app.add_handler(CallbackQueryHandler(platform_toggle_callback, pattern=r"^ptoggle_\d+$"))
    app.add_handler(CallbackQueryHandler(platform_delete_callback, pattern=r"^pdel_\d+$"))

    app.add_handler(CallbackQueryHandler(launch_add_domain, pattern=r"^adddom_\d+$"))
    app.add_handler(CallbackQueryHandler(launch_set_header, pattern=r"^phead_\d+$"))
    app.add_handler(CallbackQueryHandler(launch_set_footer, pattern=r"^pfoot_\d+$"))
    app.add_handler(CallbackQueryHandler(launch_set_batch, pattern=r"^pbatch_\d+$"))
    app.add_handler(CallbackQueryHandler(launch_set_backup, pattern=r"^pback_\d+$"))

    app.add_handler(CallbackQueryHandler(platform_channels_menu, pattern=r"^pchan_\d+$"))
    app.add_handler(CallbackQueryHandler(platform_channel_action_callback, pattern=r"^pch_\d+_-?\d+_\w+$"))

    app.add_handler(CallbackQueryHandler(platform_schedule_grid_menu, pattern=r"^pschgrid_\d+$"))
    app.add_handler(CallbackQueryHandler(toggle_schedule_slot_callback, pattern=r"^ts_\d+_\d{2}:\d{2}_\w+$"))
    app.add_handler(CallbackQueryHandler(clear_all_schedules_callback, pattern=r"^clrsch_\d+$"))

    app.add_handler(CallbackQueryHandler(platform_domains_menu, pattern=r"^pdom_\d+$"))
    app.add_handler(CallbackQueryHandler(domain_delete_callback, pattern=r"^deldom_\d+_\d+$"))
    app.add_handler(CallbackQueryHandler(platform_stats_callback, pattern=r"^pstat_\d+$"))
    app.add_handler(CallbackQueryHandler(remove_channel_callback, pattern=r"^delchan_-?\d+$"))
    app.add_handler(CallbackQueryHandler(clear_queue_callback, pattern=r"^flushq_\w+$"))
    app.add_handler(CallbackQueryHandler(unknown_callback_handler, pattern=r"^uk_\w+"))
    app.add_handler(CallbackQueryHandler(close_dialog_callback, pattern=r"^close_dialog$"))

    # Universal Handler: Guaranteed catching of forwarded posts & form actions
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, universal_message_handler))

    app.add_error_handler(error_handler)

    # 15s Dispatcher task
    app.job_queue.run_repeating(dispatcher_job, interval=15, first=5)

    logger.info("Bot starting with Bulletproof Queue Engine...")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
