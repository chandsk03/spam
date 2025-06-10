import telegram
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ConversationHandler
import json
import os
import zipfile
import asyncio
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.errors import (
    PeerIdInvalidError, SessionPasswordNeededError, UserNotParticipantError,
    ChatWriteForbiddenError, FloodWaitError, ChannelPrivateError, ChannelInvalidError,
    InviteHashInvalidError, UserAlreadyParticipantError, ChatAdminRequiredError,
    AuthKeyDuplicatedError, PhoneNumberInvalidError
)
from telethon.tl.types import Channel
import logging
import logging.handlers
import shutil
from pathlib import Path
from dotenv import load_dotenv
import re
import sys
import time

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
allowed_users_str = os.getenv("ALLOWED_USERS")
logger = logging.getLogger(__name__)
logger.error(f"Raw ALLOWED_USERS from env: {allowed_users_str}")
try:
    user_ids = [id.strip() for id in allowed_users_str.split(',') if id.strip()]
    ALLOWED_USERS = set(map(int, user_ids))
    logger.error(f"Parsed ALLOWED_USERS: {ALLOWED_USERS}")
except ValueError as e:
    logger.error(f"Failed to parse ALLOWED_USERS: {e}. Falling back to default user.")
    ALLOWED_USERS = {7398854594}

SET_MESSAGE, SET_INTERVAL, JOIN_GROUP, LEAVE_GROUP, UPLOAD_SESSION, REFRESH_SESSIONS = range(6)

CONFIG_FILE = "bot_config.json"
SESSION_DIR = "sessions"
SESSION_BACKUP_DIR = "sessions_backup"
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(SESSION_BACKUP_DIR, exist_ok=True)
ALLOWED_EXTENSIONS = {'.session', '.zip'}
MAX_SPAM_RATE = 1
RATE_LIMIT_MESSAGES_PER_SECOND = 30
RATE_LIMIT_MESSAGES_PER_MINUTE_PER_GROUP = 20
MAX_CONSECUTIVE_FAILURES = 3

class SafeBotTokenFilter(logging.Filter):
    def filter(self, record):
        record.msg = record.msg.replace(BOT_TOKEN, "BOT_TOKEN_MASKED")
        return True

log_handler = logging.handlers.RotatingFileHandler(
    "bot.log",
    maxBytes=10*1024*1024,
    backupCount=5,
    encoding='utf-8'
)
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[
        log_handler,
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger.addFilter(SafeBotTokenFilter())

is_spamming = False
client = None
user_chat_id = None
last_message_times = {}
global_message_count = 0
last_global_reset = time.time()
chat_failure_counts = {}

if not os.path.exists(CONFIG_FILE):
    config = {
        "groups": {},
        "message": "Default spam message",
        "interval": 5
    }
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4)
else:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)
        updated_groups = {}
        for chat_id, info in config["groups"].items():
            if not chat_id.startswith("-"):
                new_chat_id = f"-100{chat_id}"
                updated_groups[new_chat_id] = info
            else:
                updated_groups[chat_id] = info
        config["groups"] = updated_groups
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)

def save_config():
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        logger.error(f"Error saving config: {e}")

def validate_chat_input(chat_input):
    chat_id_pattern = re.compile(r'^-100\d+$|^-\d+$')
    link_pattern = re.compile(r'^https://t\.me/[\w-]+(?:/\d+)?$')
    return chat_id_pattern.match(chat_input) or link_pattern.match(chat_input)

def validate_chat_id(chat_id):
    chat_id_pattern = re.compile(r'^-100\d+$|^-\d+$')
    return chat_id_pattern.match(chat_id)

def is_private_group_link(invite_link):
    """Check if the invite link indicates a private group (e.g., starts with https://t.me/+)."""
    private_link_pattern = re.compile(r'^https://t\.me/\+[\w-]+$')
    return bool(private_link_pattern.match(invite_link))

KEYBOARD = [
    ["📋 Manage Groups", "✍️ Manage Message"],
    ["⏰ Manage Interval", "➕ Join Group"],
    ["➖ Leave Group", "🚀 Start Posting"],
    ["🛑 Stop Posting", "📂 Upload Session"],
    ["🔄 Refresh Sessions"]
]
REPLY_MARKUP = ReplyKeyboardMarkup(KEYBOARD, resize_keyboard=True, one_time_keyboard=False)

def restrict_access(handler):
    async def wrapper(update, context, *args, **kwargs):
        user_id = update.message.from_user.id
        if user_id not in ALLOWED_USERS:
            await update.message.reply_text("🚫 You are not authorized to use this bot.")
            logger.error(f"Unauthorized access attempt by user {user_id}")
            return
        return await handler(update, context, *args, **kwargs)
    return wrapper

async def rate_limit_check(chat_id):
    global global_message_count, last_global_reset
    current_time = time.time()
    if current_time - last_global_reset >= 1:
        global_message_count = 0
        last_global_reset = current_time
    global_message_count += 1
    if global_message_count > RATE_LIMIT_MESSAGES_PER_SECOND:
        await asyncio.sleep(1)

    if chat_id not in last_message_times:
        last_message_times[chat_id] = []
    now = time.time()
    last_message_times[chat_id] = [t for t in last_message_times[chat_id] if now - t < 60]
    last_message_times[chat_id].append(now)
    if len(last_message_times[chat_id]) > RATE_LIMIT_MESSAGES_PER_MINUTE_PER_GROUP:
        wait_time = 60 - (now - last_message_times[chat_id][0])
        await asyncio.sleep(wait_time)

async def attempt_leave_group(chat_id, group_info=None):
    """Attempt to leave the group and return whether the operation was successful."""
    try:
        await client(LeaveChannelRequest(int(chat_id)))
        return True
    except Exception as leave_err:
        logger.error(f"Failed to leave channel {chat_id}: {leave_err}")
        return False

async def handle_telethon_error(error, update=None, context=None, application=None, operation="operation", chat_id=None, user_id=None, invite_link=None):
    """Handle Telethon errors and return a user-friendly message and action."""
    user_message = f"❌ Error during {operation}: {str(error)}"
    action = None  
    auto_leave = False  
    is_private = invite_link and is_private_group_link(invite_link)

    logger.error(f"Handling Telethon error of type {type(error).__name__}: {str(error)}")

    if isinstance(error, FloodWaitError):
        wait_time = error.seconds + 1
        user_message = f"⏳ Rate limit exceeded. Waiting for {wait_time} seconds."
        logger.error(f"FloodWaitError during {operation} for chat {chat_id}: {error}. Waiting {wait_time} seconds.")
        await asyncio.sleep(wait_time)
        action = "retry"
    elif isinstance(error, UserNotParticipantError) or "Cannot get entity from a channel (or group) that you are not part of" in str(error):
        user_message = "🚨 The bot is not a member of this group."
        if is_private:
            user_message = "🚨 This appears to be a private group. The bot cannot join or access it without being a member."
        logger.error(f"UserNotParticipantError during {operation} for chat {chat_id}: {error}")
        if chat_id and chat_id in config["groups"]:
            group_info = config["groups"].get(chat_id, {})
            auto_leave = True
            if await attempt_leave_group(chat_id, group_info):
                user_message += f" The bot has left the group {group_info.get('title', 'Unknown')} (ID: {chat_id}) due to this error."
            config["groups"].pop(chat_id, None)
            save_config()
            chat_failure_counts.pop(chat_id, None)
            user_message += f" Removed {group_info.get('title', 'Unknown')} (ID: {chat_id}) from target list."
        action = "remove"
    elif isinstance(error, ChannelPrivateError):
        was_member = chat_id and chat_id in config["groups"]
        user_message = "🚨 The group/channel is private or the bot has been banned."
        if is_private:
            user_message = "🚨 This is a private group/channel. The bot lacks permission to join, or it may have been banned."
        if was_member:
            group_info = config["groups"].get(chat_id, {})
            user_message = f"🚫 The bot was likely banned from {group_info.get('title', 'Unknown')} (ID: {chat_id})."
            auto_leave = True
            if await attempt_leave_group(chat_id, group_info):
                user_message += f" The bot has left the group due to this error."
            config["groups"].pop(chat_id, None)
            save_config()
            chat_failure_counts.pop(chat_id, None)
            user_message += f" Removed from target list."
        logger.error(f"ChannelPrivateError during {operation} for chat {chat_id}: {error}")
        action = "remove"
    elif isinstance(error, ChannelInvalidError):
        user_message = "🚨 The group/channel ID is invalid."
        if chat_id and chat_id in config["groups"]:
            group_info = config["groups"].get(chat_id, {})
            auto_leave = True
            if await attempt_leave_group(chat_id, group_info):
                user_message += f" The bot has left the group {group_info.get('title', 'Unknown')} (ID: {chat_id}) due to this error."
            config["groups"].pop(chat_id, None)
            save_config()
            chat_failure_counts.pop(chat_id, None)
            user_message += f" Removed {group_info.get('title', 'Unknown')} (ID: {chat_id}) from target list."
        logger.error(f"ChannelInvalidError during {operation} for chat {chat_id}: {error}")
        action = "remove"
    elif isinstance(error, ChatWriteForbiddenError):
        user_message = "🚫 The bot cannot send messages (likely banned or lacks permissions)."
        if chat_id and chat_id in config["groups"]:
            group_info = config["groups"].get(chat_id, {})
            auto_leave = True
            if await attempt_leave_group(chat_id, group_info):
                user_message += f" The bot has left the group {group_info.get('title', 'Unknown')} (ID: {chat_id}) due to this error."
            config["groups"].pop(chat_id, None)
            save_config()
            chat_failure_counts.pop(chat_id, None)
            user_message += f" Removed {group_info.get('title', 'Unknown')} (ID: {chat_id}) from target list."
        logger.error(f"ChatWriteForbiddenError during {operation} for chat {chat_id}: {error}")
        action = "remove"
    elif isinstance(error, PeerIdInvalidError):
        user_message = "❌ Invalid group ID or link."
        logger.error(f"PeerIdInvalidError during {operation}: {error}")
        action = "notify"
    elif isinstance(error, InviteHashInvalidError):
        user_message = "❌ The invite link is invalid or expired."
        logger.error(f"InviteHashInvalidError during {operation}: {error}")
        action = "notify"
    elif isinstance(error, UserAlreadyParticipantError):
        user_message = "👥 The bot is already a member of this group."
        logger.error(f"UserAlreadyParticipantError during {operation}: {error}")
        action = "notify"
    elif isinstance(error, ChatAdminRequiredError):
        user_message = "🚫 Admin privileges are required to perform this action."
        if chat_id and chat_id in config["groups"]:
            group_info = config["groups"].get(chat_id, {})
            auto_leave = True
            if await attempt_leave_group(chat_id, group_info):
                user_message += f" The bot has left the group {group_info.get('title', 'Unknown')} (ID: {chat_id}) due to this error."
            config["groups"].pop(chat_id, None)
            save_config()
            chat_failure_counts.pop(chat_id, None)
            user_message += f" Removed {group_info.get('title', 'Unknown')} (ID: {chat_id}) from target list."
        logger.error(f"ChatAdminRequiredError during {operation} for chat {chat_id}: {error}")
        action = "remove"
    elif isinstance(error, SessionPasswordNeededError):
        user_message = "🔒 2FA is enabled on this account. Please provide the password using /set_password <password>."
        logger.error(f"SessionPasswordNeededError during {operation}: {error}")
        action = "notify"
    elif isinstance(error, AuthKeyDuplicatedError):
        user_message = "🔒 Session file is being used elsewhere. Please upload a new session file."
        logger.error(f"AuthKeyDuplicatedError during {operation}: {error}")
        if client:
            session_file = client.session.filename
            try:
                await client.disconnect()
                backup_path = os.path.join(SESSION_BACKUP_DIR, f"{os.path.basename(session_file)}_{int(time.time())}.bak")
                shutil.move(session_file, backup_path)
            except Exception as e:
                logger.error(f"Error handling AuthKeyDuplicatedError: {e}")
        action = "notify"
    elif isinstance(error, PhoneNumberInvalidError):
        user_message = "❌ The phone number associated with the session is invalid. Please upload a new session file."
        logger.error(f"PhoneNumberInvalidError during {operation}: {error}")
        if client:
            session_file = client.session.filename
            try:
                await client.disconnect()
                backup_path = os.path.join(SESSION_BACKUP_DIR, f"{os.path.basename(session_file)}_{int(time.time())}.bak")
                shutil.move(session_file, backup_path)
            except Exception as e:
                logger.error(f"Error handling PhoneNumberInvalidError: {e}")
        action = "notify"
    else:
        user_message = f"🚫 Unexpected error during {operation}: {str(error)}"
        logger.error(f"Unexpected error during {operation}: {error}")
        if chat_id and chat_id in config["groups"]:
            group_info = config["groups"].get(chat_id, {})
            auto_leave = True
            if await attempt_leave_group(chat_id, group_info):
                user_message += f" The bot has left the group {group_info.get('title', 'Unknown')} (ID: {chat_id}) due to this error."
            config["groups"].pop(chat_id, None)
            save_config()
            chat_failure_counts.pop(chat_id, None)
            user_message += f" Removed {group_info.get('title', 'Unknown')} (ID: {chat_id}) from target list."
        action = "remove"

    if update and context:  
        await update.message.reply_text(user_message, reply_markup=REPLY_MARKUP)
    elif user_chat_id and application: 
        await application.bot.send_message(user_chat_id, user_message)

    return action

async def attempt_auto_login(application):
    global client
    session_files = [f for f in os.listdir(SESSION_DIR) if f.endswith('.session')]
    if not session_files:
        for admin_id in ALLOWED_USERS:
            try:
                await application.bot.send_message(
                    admin_id,
                    "📂 No session file found on startup. Please upload a session file using 'Upload Session'."
                )
            except telegram.error.TelegramError as te:
                logger.error(f"Failed to notify admin {admin_id} about missing session: {te.message}")
        return False

    session_file = os.path.join(SESSION_DIR, session_files[0])
    client = TelegramClient(session_file, API_ID, API_HASH)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise Exception("Session is not authorized")
            await client.start()
            return True
        except Exception as e:
            action = await handle_telethon_error(
                error=e,
                update=None,
                context=None,
                application=application,
                operation="auto-login",
                user_id=None
            )
            if action == "retry":
                continue
            logger.error(f"Auto-login attempt {attempt + 1}/{max_retries} failed with session {session_files[0]}: {e}")
            if attempt == max_retries - 1:
                try:
                    if client:
                        await client.disconnect()
                        client = None
                except Exception as disconnect_err:
                    logger.error(f"Error disconnecting client before backup: {disconnect_err}")
                max_attempts = 3
                for attempt in range(max_attempts):
                    try:
                        backup_path = os.path.join(SESSION_BACKUP_DIR, f"{session_files[0]}_{int(time.time())}.bak")
                        shutil.move(session_file, backup_path)
                        break
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            logger.error(f"Failed to backup invalid session file {session_files[0]} after {max_attempts} attempts: {e}")
                        else:
                            await asyncio.sleep(1)
                return False
            await asyncio.sleep(2)

@restrict_access
async def start(update, context):
    global user_chat_id
    user_id = update.message.from_user.id
    user_chat_id = update.message.chat_id
    await update.message.reply_text(
        "🎉 Bot is ready! Use the buttons to manage groups, messages, intervals, and more. 🛠️",
        reply_markup=REPLY_MARKUP
    )

@restrict_access
async def set_logging(update, context):
    user_id = update.message.from_user.id
    if not context.args:
        await update.message.reply_text("Usage: /set_logging <level> (DEBUG, INFO, ERROR)", reply_markup=REPLY_MARKUP)
        return
    level = context.args[0].upper()
    levels = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "ERROR": logging.ERROR}
    if level not in levels:
        await update.message.reply_text("Invalid level. Use DEBUG, INFO, or ERROR.", reply_markup=REPLY_MARKUP)
        return
    logging.getLogger().setLevel(levels[level])
    await update.message.reply_text(f"Logging level set to {level}.", reply_markup=REPLY_MARKUP)

@restrict_access
async def manage_groups(update, context):
    user_id = update.message.from_user.id
    groups = config["groups"]
    if not groups:
        await update.message.reply_text("📭 No groups added yet.", reply_markup=REPLY_MARKUP)
    else:
        group_list = []
        for chat_id, info in groups.items():
            title = info.get("title", "Unknown")
            username = info.get("username", "Unknown")
            if username != "Unknown":
                username = f"@{username}"
            group_list.append(f"- {title} ({username}) [ID: {chat_id}]")
        group_display = "\n".join(group_list)
        await update.message.reply_text(
            f"📋 Current groups:\n{group_display}",
            reply_markup=REPLY_MARKUP
        )

@restrict_access
async def join_group(update, context):
    user_id = update.message.from_user.id
    await update.message.reply_text(
        "🔗 Please send the group invite link (e.g., https://t.me/groupname).",
        reply_markup=ReplyKeyboardRemove()
    )
    return JOIN_GROUP

@restrict_access
async def do_join_group(update, context):
    global client
    user_id = update.message.from_user.id
    invite_link = update.message.text.strip()

    if not client:
        await update.message.reply_text("📂 Please upload a session file first using 'Upload Session'.", reply_markup=REPLY_MARKUP)
        return ConversationHandler.END

    if not validate_chat_input(invite_link):
        await update.message.reply_text(
            "❌ Invalid link. Please send a valid Telegram group link (e.g., https://t.me/groupname).",
            reply_markup=REPLY_MARKUP
        )
        return ConversationHandler.END

    if is_private_group_link(invite_link):
        await update.message.reply_text(
            "⚠️ This appears to be a private group. The bot may not be able to join if it lacks permission or if the invite link is invalid/expired.",
            reply_markup=REPLY_MARKUP
        )

    chat_id = None
    max_retries = 3
    for attempt in range(max_retries):
        try:
            chat = await client(JoinChannelRequest(invite_link))
            entity = chat.chats[0]
            chat_id = f"-100{entity.id}"

            if chat_id in config["groups"]:
                await update.message.reply_text(
                    f"✅ Group {entity.title} (ID: {chat_id}) is already in the list.",
                    reply_markup=REPLY_MARKUP
                )
            else:
                username = getattr(entity, 'username', 'Unknown')
                config["groups"][chat_id] = {
                    "title": entity.title,
                    "username": username if username else "Unknown"
                }
                save_config()
                username_display = f"@{username}" if username != "Unknown" else "No username"
                await update.message.reply_text(
                    f"🎉 Joined group {entity.title} ({username_display}) [ID: {chat_id}] and added to target list. ✅",
                    reply_markup=REPLY_MARKUP
                )
            return ConversationHandler.END
        except Exception as e:
            action = await handle_telethon_error(
                error=e,
                update=update,
                context=context,
                application=None,
                operation="joining group",
                chat_id=chat_id,
                user_id=user_id,
                invite_link=invite_link
            )
            if action == "retry":
                continue
            elif action in ["remove", "notify"]:
                return ConversationHandler.END
            if attempt == max_retries - 1:
                await update.message.reply_text(
                    f"❌ Failed to join group after {max_retries} attempts: {str(e)}",
                    reply_markup=REPLY_MARKUP
                )
                return ConversationHandler.END
            await asyncio.sleep(2)

@restrict_access
async def remove_group(update, context):
    user_id = update.message.from_user.id
    try:
        chat_id = str(context.args[0])
        if chat_id not in config["groups"]:
            await update.message.reply_text(f"❌ Group with ID {chat_id} is not in the list.", reply_markup=REPLY_MARKUP)
            return
        group_info = config["groups"].pop(chat_id)
        save_config()
        await update.message.reply_text(
            f"🗑️ Removed group {group_info['title']} (ID: {chat_id}) from the target list.",
            reply_markup=REPLY_MARKUP
        )
    except (IndexError, ValueError):
        await update.message.reply_text("📋 Usage: /remove_group <chat_id>", reply_markup=REPLY_MARKUP)
    except Exception as e:
        await update.message.reply_text(f"❌ Error removing group: {e}", reply_markup=REPLY_MARKUP)
        logger.error(f"Error removing group for user {user_id}: {e}")

@restrict_access
async def manage_message(update, context):
    user_id = update.message.from_user.id
    await update.message.reply_text(
        f"📩 Current message: {config['message']}\n\nPlease send the new message.",
        reply_markup=ReplyKeyboardRemove()
    )
    return SET_MESSAGE

@restrict_access
async def set_message(update, context):
    user_id = update.message.from_user.id
    message = update.message.text.strip()
    if not message:
        await update.message.reply_text("❌ Message cannot be empty.", reply_markup=REPLY_MARKUP)
        return ConversationHandler.END
    config["message"] = message
    save_config()
    await update.message.reply_text(f"✅ Spam message set to: {config['message']} 📩", reply_markup=REPLY_MARKUP)
    return ConversationHandler.END

@restrict_access
async def manage_interval(update, context):
    user_id = update.message.from_user.id
    await update.message.reply_text(
        f"⏰ Current interval: {config['interval']} minutes\n\nPlease send the new interval in minutes.",
        reply_markup=ReplyKeyboardRemove()
    )
    return SET_INTERVAL

@restrict_access
async def set_interval(update, context):
    user_id = update.message.from_user.id
    try:
        interval = int(update.message.text.strip())
        if interval < MAX_SPAM_RATE:
            await update.message.reply_text(
                f"❌ Interval must be at least {MAX_SPAM_RATE} minutes to prevent abuse.",
                reply_markup=REPLY_MARKUP
            )
            return ConversationHandler.END
        config["interval"] = interval
        save_config()
        await update.message.reply_text(f"✅ Interval set to {interval} minutes. ⏰", reply_markup=REPLY_MARKUP)
    except ValueError:
        await update.message.reply_text("❌ Please send a valid number of minutes.", reply_markup=REPLY_MARKUP)
    return ConversationHandler.END

@restrict_access
async def leave_group(update, context):
    user_id = update.message.from_user.id
    groups = config["groups"]
    if not groups:
        await update.message.reply_text("📭 No groups to leave.", reply_markup=REPLY_MARKUP)
        return ConversationHandler.END
    group_list = []
    for chat_id, info in groups.items():
        title = info.get("title", "Unknown")
        username = info.get("username", "Unknown")
        if username != "Unknown":
            username = f"@{username}"
        group_list.append(f"- {title} ({username}) [ID: {chat_id}]")
    group_display = "\n".join(group_list)
    await update.message.reply_text(
        f"📋 Current groups:\n{group_display}\n\n🔢 Please send the chat ID of the group to leave (e.g., -1001234567890).",
        reply_markup=ReplyKeyboardRemove()
    )
    return LEAVE_GROUP

@restrict_access
async def do_leave_group(update, context):
    global client
    user_id = update.message.from_user.id
    chat_id = str(update.message.text.strip())

    if not client:
        await update.message.reply_text("📂 Please upload a session file first using 'Upload Session'.", reply_markup=REPLY_MARKUP)
        return ConversationHandler.END

    if not validate_chat_id(chat_id):
        await update.message.reply_text(
            "❌ Invalid chat ID format. Please provide a valid group ID (e.g., -1001234567890).",
            reply_markup=REPLY_MARKUP
        )
        return ConversationHandler.END

    if chat_id not in config["groups"]:
        await update.message.reply_text(f"❌ Group with ID {chat_id} is not in the list.", reply_markup=REPLY_MARKUP)
        return ConversationHandler.END

    max_retries = 3
    for attempt in range(max_retries):
        try:
            await client(LeaveChannelRequest(int(chat_id)))
            group_info = config["groups"].pop(chat_id)
            save_config()
            await update.message.reply_text(
                f"👋 Left group {group_info['title']} (ID: {chat_id}) and removed from target list. ✅",
                reply_markup=REPLY_MARKUP
            )
            return ConversationHandler.END
        except Exception as e:
            action = await handle_telethon_error(
                error=e,
                update=update,
                context=context,
                application=None,
                operation="leaving group",
                chat_id=chat_id,
                user_id=user_id
            )
            if action == "retry":
                continue
            elif action in ["remove", "notify"]:
                return ConversationHandler.END
            if attempt == max_retries - 1:
                group_info = config["groups"].pop(chat_id, None)
                save_config()
                await update.message.reply_text(
                    f"❌ Error leaving group {group_info['title']} (ID: {chat_id}) after {max_retries} attempts: {e}. Removed from target list.",
                    reply_markup=REPLY_MARKUP
                )
                return ConversationHandler.END
            await asyncio.sleep(2)

@restrict_access
async def upload_session(update, context):
    user_id = update.message.from_user.id
    if client and await client.is_user_authorized():
        await update.message.reply_text(
            "🔒 A session is already loaded and authorized. Do you want to replace it? Reply with 'yes' to continue, or 'cancel' to abort.",
            reply_markup=ReplyKeyboardRemove()
        )
        return UPLOAD_SESSION
    await update.message.reply_text(
        "📂 Please upload a .session file or a .zip file containing Telegram session files.",
        reply_markup=ReplyKeyboardRemove()
    )
    return UPLOAD_SESSION

@restrict_access
async def handle_session_file(update, context):
    global client
    user_id = update.message.from_user.id
    message_text = update.message.text.strip().lower() if update.message.text else None
    document = update.message.document

    if client and await client.is_user_authorized() and message_text:
        if message_text == 'yes':
            await update.message.reply_text(
                "📂 Please upload the new .session file or .zip file.",
                reply_markup=ReplyKeyboardRemove()
            )
            return UPLOAD_SESSION
        elif message_text == 'cancel':
            await update.message.reply_text("❌ Operation cancelled.", reply_markup=REPLY_MARKUP)
            return ConversationHandler.END
        else:
            await update.message.reply_text(
                "🔄 Please reply with 'yes' to replace the session, or 'cancel' to abort.",
                reply_markup=ReplyKeyboardRemove()
            )
            return UPLOAD_SESSION

    if not document:
        await update.message.reply_text("❌ Please upload a file.", reply_markup=REPLY_MARKUP)
        return ConversationHandler.END

    file_name = document.file_name
    file_extension = Path(file_name).suffix.lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        await update.message.reply_text("❌ Only .session or .zip files are allowed.", reply_markup=REPLY_MARKUP)
        return ConversationHandler.END

    temp_file_path = os.path.join(SESSION_DIR, f"temp_{file_name}")
    final_file_path = os.path.join(SESSION_DIR, file_name)
    try:
        file = await document.get_file()
        await file.download_to_drive(temp_file_path)

        if file_extension == '.zip':
            with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                session_files = [f for f in zip_ref.namelist() if f.endswith('.session')]
                if not session_files:
                    await update.message.reply_text("❌ No .session files found in the .zip.", reply_markup=REPLY_MARKUP)
                    return ConversationHandler.END
                for session_file in session_files:
                    zip_ref.extract(session_file, SESSION_DIR)
        else:
            shutil.move(temp_file_path, final_file_path)

        session_files = [f for f in os.listdir(SESSION_DIR) if f.endswith('.session')]
        if not session_files:
            await update.message.reply_text("❌ No .session files found after extraction.", reply_markup=REPLY_MARKUP)
            return ConversationHandler.END

        session_file = os.path.join(SESSION_DIR, session_files[-1])
        if client:
            await client.disconnect()

        client = TelegramClient(session_file, API_ID, API_HASH)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    raise Exception("Session is not authorized")
                await client.start()
                await update.message.reply_text("✅ Accounts loaded successfully! Bot is ready to use. 🎉", reply_markup=REPLY_MARKUP)
                return ConversationHandler.END
            except Exception as e:
                action = await handle_telethon_error(
                    error=e,
                    update=update,
                    context=context,
                    application=None,
                    operation="session validation",
                    user_id=user_id
                )
                if action == "retry":
                    continue
                if attempt == max_retries - 1:
                    try:
                        if client:
                            await client.disconnect()
                            client = None
                    except Exception as disconnect_err:
                        logger.error(f"Error disconnecting client before removing session file: {disconnect_err}")
                    max_attempts = 3
                    for attempt in range(max_attempts):
                        try:
                            backup_path = os.path.join(SESSION_BACKUP_DIR, f"{session_files[-1]}_{int(time.time())}.bak")
                            shutil.move(session_file, backup_path)
                            break
                        except Exception as e:
                            if attempt == max_attempts - 1:
                                logger.error(f"Failed to backup invalid session file {session_files[-1]} after {max_attempts} attempts: {e}")
                            else:
                                await asyncio.sleep(1)
                    client = None
                    return ConversationHandler.END
                await asyncio.sleep(2)
    except telegram.error.TelegramError as te:
        if te.message == "FILE_TOO_LARGE":
            await update.message.reply_text(
                "❌ The uploaded file is too large. Please upload a smaller file.",
                reply_markup=REPLY_MARKUP
            )
        else:
            await update.message.reply_text(
                f"🚫 Telegram API error: {te.message}",
                reply_markup=REPLY_MARKUP
            )
        logger.error(f"Telegram API error for user {user_id}: {te.message}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error processing session file: {e}", reply_markup=REPLY_MARKUP)
        logger.error(f"Error processing session file for user {user_id}: {e}")
    finally:
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except Exception as e:
                logger.error(f"Could not delete temporary file {temp_file_path}: {e}")
    return ConversationHandler.END

@restrict_access
async def refresh_sessions(update, context):
    global client
    user_id = update.message.from_user.id

    session_files = [f for f in os.listdir(SESSION_DIR) if f.endswith('.session')]
    if not session_files:
        await update.message.reply_text("📭 No session files found to refresh.", reply_markup=REPLY_MARKUP)
        return

    valid_sessions = []
    invalid_sessions = []

    current_session = None
    if client and await client.is_user_authorized():
        current_session = os.path.basename(client.session.filename)
        valid_sessions.append(current_session)

    for session_file in session_files:
        if current_session and session_file == current_session:
            continue
        session_path = os.path.join(SESSION_DIR, session_file)
        temp_client = TelegramClient(session_path, API_ID, API_HASH)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await temp_client.connect()
                if not await temp_client.is_user_authorized():
                    raise Exception("Session is not authorized")
                await temp_client.start()
                if await temp_client.is_user_authorized():
                    valid_sessions.append(session_file)
                else:
                    invalid_sessions.append(session_file)
                await temp_client.disconnect()
                break
            except Exception as e:
                action = await handle_telethon_error(
                    error=e,
                    update=update,
                    context=context,
                    application=None,
                    operation="session refresh",
                    user_id=user_id
                )
                if action == "retry":
                    continue
                if attempt == max_retries - 1:
                    invalid_sessions.append(session_file)
                    try:
                        await temp_client.disconnect()
                    except Exception as disconnect_err:
                        logger.error(f"Error disconnecting temp client before removing session file: {disconnect_err}")
                    max_attempts = 3
                    for attempt in range(max_attempts):
                        try:
                            backup_path = os.path.join(SESSION_BACKUP_DIR, f"{session_file}_{int(time.time())}.bak")
                            shutil.move(session_path, backup_path)
                            break
                        except Exception as e:
                            if attempt == max_attempts - 1:
                                logger.error(f"Failed to backup invalid session file {session_file} after {max_attempts} attempts: {e}")
                            else:
                                await asyncio.sleep(1)
                    break
                await asyncio.sleep(2)

    if current_session and current_session not in valid_sessions:
        client = None
    if valid_sessions and not client:
        session_file = os.path.join(SESSION_DIR, valid_sessions[0])
        client = TelegramClient(session_file, API_ID, API_HASH)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await client.start()
                break
            except Exception as e:
                action = await handle_telethon_error(
                    error=e,
                    update=update,
                    context=context,
                    application=None,
                    operation="session reconnection",
                    user_id=user_id
                )
                if action == "retry":
                    continue
                if attempt == max_retries - 1:
                    client = None
                    break
                await asyncio.sleep(2)

    response = []
    if valid_sessions:
        response.append(f"✅ Valid sessions: {', '.join(valid_sessions)}")
    if invalid_sessions:
        response.append(f"🗑️ Removed invalid sessions: {', '.join(invalid_sessions)}")
    if not response:
        response.append("📭 No valid or invalid sessions found.")

    await update.message.reply_text("\n".join(response), reply_markup=REPLY_MARKUP)

@restrict_access
async def set_password(update, context):
    user_id = update.message.from_user.id
    if not context.args:
        await update.message.reply_text("🔒 Usage: /set_password <password>", reply_markup=REPLY_MARKUP)
        return

    password = context.args[0]
    global client
    if not client:
        await update.message.reply_text("📂 Please upload a session file first using 'Upload Session'.", reply_markup=REPLY_MARKUP)
        return

    max_retries = 3
    for attempt in range(max_retries):
        try:
            await client.start(password=password)
            await update.message.reply_text("✅ 2FA password accepted. Bot is ready to use. 🎉", reply_markup=REPLY_MARKUP)
            return
        except Exception as e:
            action = await handle_telethon_error(
                error=e,
                update=update,
                context=context,
                application=None,
                operation="2FA authentication",
                user_id=user_id
            )
            if action == "retry":
                continue
            if attempt == max_retries - 1:
                return
            await asyncio.sleep(2)

async def spam_groups(client, application):
    global is_spamming, user_chat_id, chat_failure_counts
    is_spamming = True
    while is_spamming:
        if not config["groups"]:
            if user_chat_id:
                await application.bot.send_message(user_chat_id, "📭 No groups to spam. Add groups using 'Join Group'.")
            await asyncio.sleep(config["interval"] * 60)
            continue
        for chat_id in list(config["groups"].keys()):
            if not is_spamming:
                break
            max_retries = 3
            success = False
            for attempt in range(max_retries):
                if not is_spamming:
                    break
                try:
                    entity_id = int(chat_id) if chat_id.startswith("-") else int(f"-100{chat_id}")
                    entity = await client.get_entity(entity_id)
                    is_channel = isinstance(entity, Channel) and entity.broadcast
                    await rate_limit_check(chat_id)

                    if is_channel:
                        try:
                            await client.send_message(entity, config["message"])
                            if user_chat_id:
                                await application.bot.send_message(user_chat_id, f"📬 Sent message to channel {chat_id}")
                            success = True
                            chat_failure_counts[chat_id] = 0
                        except Exception as e:
                            if "Chat admin privileges are required" in str(e):
                                group_info = config["groups"].pop(chat_id, None)
                                save_config()
                                if user_chat_id and group_info:
                                    await application.bot.send_message(
                                        user_chat_id,
                                        f"📺 This is a channel ({group_info['title']}, ID: {chat_id}), and I don’t have permission to send messages in the channel. Leaving the channel automatically."
                                    )
                                try:
                                    await client(LeaveChannelRequest(entity_id))
                                except Exception as e:
                                    logger.error(f"Failed to leave channel {chat_id}: {e}")
                                break
                    else:
                        await client.send_message(entity, config["message"])
                        if user_chat_id:
                            await application.bot.send_message(user_chat_id, f"📬 Sent spam to group {chat_id}")
                        success = True
                        chat_failure_counts[chat_id] = 0
                    break
                except Exception as e:
                    action = await handle_telethon_error(
                        error=e,
                        update=None,
                        context=None,
                        application=application,
                        operation="spamming group",
                        chat_id=chat_id,
                        user_id=None
                    )
                    if action == "retry":
                        continue
                    elif action in ["remove", "notify"]:
                        break
                    chat_failure_counts[chat_id] = chat_failure_counts.get(chat_id, 0) + 1
                    if chat_failure_counts[chat_id] >= MAX_CONSECUTIVE_FAILURES:
                        group_info = config["groups"].pop(chat_id, None)
                        save_config()
                        chat_failure_counts.pop(chat_id, None)
                        if user_chat_id and group_info:
                            await application.bot.send_message(
                                user_chat_id,
                                f"❌ Failed to spam group {group_info['title']} (ID: {chat_id}) after {MAX_CONSECUTIVE_FAILURES} failures. Removed from target list."
                            )
                        break
                    await asyncio.sleep(2)
            if success:
                chat_failure_counts[chat_id] = 0
        if is_spamming:
            await asyncio.sleep(config["interval"] * 60)

@restrict_access
async def start_spamming(update, context):
    global is_spamming, client
    user_id = update.message.from_user.id

    if is_spamming:
        await update.message.reply_text("🚀 Spamming is already running!", reply_markup=REPLY_MARKUP)
        return

    if not client:
        await update.message.reply_text("📂 Please upload a session file first using 'Upload Session'.", reply_markup=REPLY_MARKUP)
        return

    if not await client.is_user_authorized():
        await update.message.reply_text("🔒 Telethon client is not authorized. Please ensure the session is valid and 2FA is handled.", reply_markup=REPLY_MARKUP)
        return

    if not config["groups"]:
        await update.message.reply_text("📭 No groups to spam. Add groups using 'Join Group'.", reply_markup=REPLY_MARKUP)
        return

    await update.message.reply_text("🚀 Starting spam process to the following groups:\n" +
                                    "\n".join(f"- {info['title']} (ID: {chat_id})" for chat_id, info in config["groups"].items()),
                                    reply_markup=REPLY_MARKUP)
    context.user_data['spam_task'] = asyncio.create_task(spam_groups(client, context.application))

@restrict_access
async def stop_spamming(update, context):
    global is_spamming
    user_id = update.message.from_user.id

    if not is_spamming:
        await update.message.reply_text("🛑 Spamming is not running!", reply_markup=REPLY_MARKUP)
        return

    is_spamming = False
    spam_task = context.user_data.get('spam_task')
    if spam_task:
        spam_task.cancel()
        try:
            await spam_task
        except asyncio.CancelledError:
            pass
        context.user_data['spam_task'] = None
    await update.message.reply_text("🛑 Spamming stopped.", reply_markup=REPLY_MARKUP)

@restrict_access
async def cancel(update, context):
    user_id = update.message.from_user.id
    await update.message.reply_text("❌ Operation cancelled.", reply_markup=REPLY_MARKUP)
    return ConversationHandler.END

async def main(loop):
    application = Application.builder().token(BOT_TOKEN).build()
    await application.initialize()
    await application.start()

    session_available = await attempt_auto_login(application)

    if not session_available:
        for admin_id in ALLOWED_USERS:
            try:
                await application.bot.send_message(
                    admin_id,
                    "📂 No session file found on startup or session was invalid. Please upload a session file using 'Upload Session'."
                )
            except telegram.error.TelegramError as te:
                logger.error(f"Failed to notify admin {admin_id} about missing session: {te.message}")

    set_message_conv = ConversationHandler(
        entry_points=[CommandHandler("set_message", manage_message), MessageHandler(filters.Regex("Manage Message"), manage_message)],
        states={SET_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_message)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    set_interval_conv = ConversationHandler(
        entry_points=[CommandHandler("set_interval", manage_interval), MessageHandler(filters.Regex("Manage Interval"), manage_interval)],
        states={SET_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_interval)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    join_group_conv = ConversationHandler(
        entry_points=[CommandHandler("join_group", join_group), MessageHandler(filters.Regex("Join Group"), join_group)],
        states={JOIN_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_join_group)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    leave_group_conv = ConversationHandler(
        entry_points=[CommandHandler("leave_group", leave_group), MessageHandler(filters.Regex("Leave Group"), leave_group)],
        states={LEAVE_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_leave_group)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    upload_session_conv = ConversationHandler(
        entry_points=[CommandHandler("upload_session", upload_session), MessageHandler(filters.Regex("Upload Session"), upload_session)],
        states={UPLOAD_SESSION: [MessageHandler(filters.Document.ALL | filters.TEXT, handle_session_file)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("set_logging", set_logging))
    application.add_handler(MessageHandler(filters.Regex("Manage Groups"), manage_groups))
    application.add_handler(CommandHandler("remove_group", remove_group))
    application.add_handler(set_message_conv)
    application.add_handler(set_interval_conv)
    application.add_handler(join_group_conv)
    application.add_handler(leave_group_conv)
    application.add_handler(upload_session_conv)
    application.add_handler(MessageHandler(filters.Regex("Refresh Sessions"), refresh_sessions))
    application.add_handler(CommandHandler("set_password", set_password))
    application.add_handler(CommandHandler("start_spamming", start_spamming))
    application.add_handler(MessageHandler(filters.Regex("Start Posting"), start_spamming))
    application.add_handler(CommandHandler("stop_spamming", stop_spamming))
    application.add_handler(MessageHandler(filters.Regex("Stop Posting"), stop_spamming))

    try:
        await application.updater.start_polling(allowed_updates=["message", "callback_query"])
        while True:
            await asyncio.sleep(1)
    except telegram.error.TelegramError as te:
        logger.error(f"Telegram API error during bot startup: {te.message}")
        raise
    except Exception as e:
        logger.error(f"Error in bot operation: {e}")
        raise
    finally:
        if application.updater.running:
            await application.updater.stop()
        await application.stop()
        for task in asyncio.all_tasks(loop):
            if task is not asyncio.current_task(loop):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await application.shutdown()
        if client:
            try:
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting Telethon client: {e}")
        logger.error("Bot shut down")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main(loop))
    except KeyboardInterrupt:
        logger.error("Bot stopped by user (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Error running main: {e}")
    finally:
        for task in asyncio.all_tasks(loop):
            if task is not asyncio.current_task(loop):
                task.cancel()
                try:
                    loop.run_until_complete(task)
                except asyncio.CancelledError:
                    pass
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error shutting down asyncgens: {e}")
        loop.close()
        logger.error("Event loop closed")