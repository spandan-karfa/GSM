#!/usr/bin/env python3
"""
AutoFarm Bot with user farming logic
- Multi-user supported
- /approve <id>, /unapprove <id>
- /setup → login (phone → OTP → password)
- /toggle → On/Off inline buttons
- /cancel → stop farming completely (like Ctrl+C)
- Farming logic intact, Engage variants restored
- All farming actions delayed ~0.2~0.35s (random jitter)
- CAPTCHA alerts sent to DM (not group)
- Debug logging added
- Fixed: No /explore during battles or CAPTCHAs
- /rate → Change user-specific pearl and ticket rates
- CAPTCHA detection only in BOT_ID DMs
- Group notifications for special events
- MongoDB integration for persistent storage
- Admin management system added
"""

import os, re, time, threading, asyncio, random, logging, json
from typing import Dict, Optional
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
import telebot
from telebot import types 
from mongo_db import mongo_manager  # MongoDB integration

# ==============================
# CONFIG
# ==============================
BOT_TOKEN = "8321893069:AAEklcRLmi_QOZruzXW2ZpSCh1w5aNzU4f8"
BOT_OWNER_ID = 6284630712   # your Telegram user id

API_ID = 28484298            # your Telegram API ID
API_HASH = "f716ac1f16c0806b0c82580ae3f3b65e" # your Telegram API HASH
BOT_ID = 5364964725  # replace with the game bot's id

MAX_PEARL_PRICE = 250
MAX_TICKET_PRICE = 500

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# ==============================
# LOGGING
# ==============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("AutoFarm")

debug_users = set()

def dbg(uid, msg):
    if uid in debug_users:
        log.debug(f"[User {uid}] {msg}")

# ==============================
# STATE - MONGODB INTEGRATION
# ==============================
# Load approved users from MongoDB
approved_users = mongo_manager.get_approved_users() if mongo_manager else {}
log.info(f"✅ Loaded {len(approved_users)} approved users from MongoDB")

# Load user config from MongoDB
user_config = mongo_manager.get_all_user_configs() if mongo_manager else {}
log.info(f"✅ Loaded {len(user_config)} user configs from MongoDB")

# Load user data from MongoDB
user_data = mongo_manager.get_all_user_data() if mongo_manager else {}
log.info(f"✅ Loaded {len(user_data)} user data from MongoDB")

# Load admins from MongoDB
admins = set()
if mongo_manager:
    try:
        admins = mongo_manager.get_admins()
        log.info(f"✅ Loaded {len(admins)} admins from MongoDB")
    except Exception as e:
        log.error(f"❌ Error loading admins: {e}")

def save_admins():
    """Save admins to MongoDB"""
    try:
        if mongo_manager:
            return mongo_manager.save_admins(admins)
        return False
    except Exception as e:
        log.error(f"❌ Error saving admins: {e}")
        return False

def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in admins or user_id == BOT_OWNER_ID

def admin_only(handler):
    """Decorator to restrict commands to admins and owner"""
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        if is_admin(user_id):
            return handler(message, *args, **kwargs)
        else:
            bot.reply_to(message, "❌ Admin access required.")
    return wrapper

def owner_only_strict(handler):
    """Decorator that only allows the bot owner (not admins)"""
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        if user_id == BOT_OWNER_ID:
            return handler(message, *args, **kwargs)
        else:
            bot.reply_to(message, "❌ Bot owner access required.")
    return wrapper

def save_approvals():
    """Save approved users to MongoDB - now handled individually"""
    log.debug("✅ Approved users are automatically saved to MongoDB")

def save_user_config():
    """Save user config to MongoDB - now handled individually"""
    log.debug("✅ User configs are automatically saved to MongoDB")

def save_user_data():
    """Save user data to MongoDB - now handled individually"""
    log.debug("✅ User data is automatically saved to MongoDB")

def get_user_data(user_id):
    """Get user data from MongoDB"""
    if user_id not in user_data:
        user_data[user_id] = mongo_manager.get_user_data(user_id) if mongo_manager else {'gc_noti': False, 'group_id': None}
    return user_data[user_id]

def get_user_pearl_price(user_id):
    if mongo_manager:
        config = mongo_manager.get_user_config(user_id)
        return config.get('max_pearl_price', MAX_PEARL_PRICE)
    else:
        return user_config.get(str(user_id), {}).get('max_pearl_price', MAX_PEARL_PRICE)

def get_user_ticket_price(user_id):
    if mongo_manager:
        config = mongo_manager.get_user_config(user_id)
        return config.get('max_ticket_price', MAX_TICKET_PRICE)
    else:
        return user_config.get(str(user_id), {}).get('max_ticket_price', MAX_TICKET_PRICE)

def set_user_pearl_price(user_id, price):
    if mongo_manager:
        config = mongo_manager.get_user_config(user_id)
        config['max_pearl_price'] = price
        mongo_manager.save_user_config(user_id, config)
    else:
        if str(user_id) not in user_config:
            user_config[str(user_id)] = {}
        user_config[str(user_id)]['max_pearl_price'] = price
        save_user_config()

def set_user_ticket_price(user_id, price):
    if mongo_manager:
        config = mongo_manager.get_user_config(user_id)
        config['max_ticket_price'] = price
        mongo_manager.save_user_config(user_id, config)
    else:
        if str(user_id) not in user_config:
            user_config[str(user_id)] = {}
        user_config[str(user_id)]['max_ticket_price'] = price
        save_user_config()

# ==============================
# TIME HELPERS
# ==============================
def parse_duration(duration_str):
    """Parse duration string like '1d', '1w', '1m' and return seconds"""
    if not duration_str or duration_str.lower() == 'p':
        return None  # Permanent
    
    try:
        num = int(duration_str[:-1])
        unit = duration_str[-1].lower()
        
        if unit == 'd':
            return num * 24 * 60 * 60  # days to seconds
        elif unit == 'w':
            return num * 7 * 24 * 60 * 60  # weeks to seconds
        elif unit == 'm':
            return num * 30 * 24 * 60 * 60  # months to seconds (approx)
        else:
            return None
    except (ValueError, IndexError):
        return None

def get_expiration_time(duration_str):
    """Get expiration timestamp from duration string"""
    duration_seconds = parse_duration(duration_str)
    if duration_seconds is None:  # Permanent
        return None
    return int(time.time()) + duration_seconds

def is_approved(user_id):
    """Check if user is approved and not expired"""
    if user_id not in approved_users:
        return False
    
    expiration = approved_users[user_id]
    # None means permanent approval
    if expiration is None:
        return True
    
    # Check if approval has expired
    if time.time() > expiration:
        # Remove expired approval
        approved_users.pop(user_id)
        if mongo_manager:
            mongo_manager.remove_approved_user(user_id)
        else:
            save_approvals()
        return False
    
    return True

def format_time_remaining(expiration):
    """Format remaining time in a human-readable way"""
    if expiration is None:
        return "permanent"
    
    remaining = expiration - time.time()
    if remaining <= 0:
        return "expired"
    
    days = remaining // (24 * 3600)
    hours = (remaining % (24 * 3600)) // 3600
    minutes = (remaining % 3600) // 60
    
    if days > 0:
        return f"{int(days)}d {int(hours)}h"
    elif hours > 0:
        return f"{int(hours)}h {int(minutes)}m"
    else:
        return f"{int(minutes)}m"

# ==============================
# SESSION MEMORY MANAGEMENT
# ==============================
user_clients: Dict[int, TelegramClient] = {}
pending_clients: Dict[int, Optional[TelegramClient]] = {}
pending_expect: Dict[int, Optional[str]] = {}
farming_enabled: Dict[int, bool] = {}
waiting_for_phone: Dict[int, bool] = {}
user_session_state: Dict[int, Dict] = {}
last_explore: Dict[int, float] = {}

# Track user login states
user_login_states: Dict[int, bool] = {}

def is_user_logged_in(user_id: int) -> bool:
    """Check if user is already logged in"""
    return user_login_states.get(user_id, False)

def set_user_logged_in(user_id: int, status: bool):
    """Set user login status"""
    user_login_states[user_id] = status
    log.info(f"[🔐] User {user_id} login status set to: {status}")

def cleanup_user_session(user_id: int):
    """Clean up user session from memory only (not from MongoDB)"""
    user_clients.pop(user_id, None)
    pending_clients.pop(user_id, None)
    farming_enabled.pop(user_id, None)
    pending_expect.pop(user_id, None)
    waiting_for_phone.pop(user_id, None)
    user_session_state.pop(user_id, None)
    last_explore.pop(user_id, None)
    user_login_states.pop(user_id, None)
    log.info(f"[🧹] Cleaned up session for user {user_id} from memory (session preserved in MongoDB)")

async def restore_existing_session(user_id: int):
    """Restore an existing session from MongoDB"""
    try:
        session = f"session_{user_id}"
        session_file = f"{session}.session"
        
        # Get session data from MongoDB
        if mongo_manager and mongo_manager.session_file_exists(user_id):
            session_data = mongo_manager.get_session_file(user_id)
            if session_data:
                # Create temporary session file for Telethon
                with open(session_file, 'wb') as f:
                    f.write(session_data)
                
                client = TelegramClient(session, API_ID, API_HASH)
                await client.connect()
                
                if await client.is_user_authorized():
                    user_clients[user_id] = client
                    farming_enabled[user_id] = False
                    set_user_logged_in(user_id, True)
                    await attach_handlers(user_id, client)
                    bot.send_message(user_id, "✅ Session restored! Use /toggle to start farming.")
                    return True
                else:
                    await client.disconnect()
                    # Session is invalid, remove it
                    if os.path.exists(session_file):
                        os.remove(session_file)
                    mongo_manager.delete_session_file(user_id)
        
        # If we get here, session restoration failed
        bot.send_message(user_id, "❌ Session restoration failed. Please login again with /setup")
        return False
        
    except Exception as e:
        log.error(f"Session restoration failed for user {user_id}: {e}")
        bot.send_message(user_id, f"❌ Session restoration failed: {e}")
        return False

loop = asyncio.get_event_loop()

# ==============================
# HELPERS
# ==============================
def owner_only(handler):
    def wrapper(message,*a,**kw):
        if is_admin(message.from_user.id):
            return handler(message,*a,**kw)
        else:
            bot.reply_to(message,"❌ Not authorized.")
    return wrapper

def get_user_name(user_id):
    """Get user's first name or full name if available"""
    try:
        user_info = bot.get_chat(user_id)
        if user_info.first_name and user_info.last_name:
            return f"{user_info.first_name} {user_info.last_name}"
        elif user_info.first_name:
            return user_info.first_name
        else:
            return f"User_{user_id}"
    except Exception:
        return f"User_{user_id}"

def send_captcha(user_id,text):
    try:
        user_name = get_user_name(user_id)
        bot.send_message(user_id,f"❗ CAPTCHA for {user_name}:\n{text}")
    except Exception as e:
        log.error(f"Captcha send fail: {e}")

def send_group_notification(user_id, notification_text):
    """Send notification to group if enabled, otherwise to user DM"""
    user_data_obj = get_user_data(user_id)
    user_name = get_user_name(user_id)
    
    # Replace user ID with user name in notification text
    notification_text = notification_text.replace(f"user {user_id}", f"user {user_name}")
    notification_text = notification_text.replace(f"for user {user_id}", f"for {user_name}")
    
    if user_data_obj['gc_noti'] and user_data_obj['group_id']:
        try:
            bot.send_message(user_data_obj['group_id'], notification_text)
            return True
        except Exception as e:
            log.error(f"Group notification failed for user {user_id}: {e}")
            # Fallback to DM if group notification fails
            bot.send_message(user_id, notification_text)
            return False
    else:
        bot.send_message(user_id, notification_text)
        return False

async def jitter_sleep(min_s=0.2, max_s=0.35):
    await asyncio.sleep(random.uniform(min_s, max_s))

async def safe_explore(client, uid):
    now = time.time()
    if now - last_explore.get(uid, 0) > 1:  # 1s cooldown
        last_explore[uid] = now
        await client.send_message(BOT_ID, "/explore")
        log.info(f"[✓] Sent /explore for user {uid}")

# ==============================
# FARMING LOGIC (UNCHANGED)
# ==============================
async def send_explore_with_timeout(client, user_id, retry_on_fail=False):
    try:
        # Check if we're in a state where we shouldn't explore
        state = user_session_state.get(user_id, {})
        if state.get('in_combat_or_capture', False) or state.get('captcha_active', False):
            log.info(f"[⏸️] Skipping /explore - in combat/captcha state for user {user_id}")
            return
            
        await jitter_sleep()
        state['explore_response_event'] = asyncio.Event()  # reset before sending

        await safe_explore(client, user_id)

        try:
            # Wait max 5s for ANY response (normal encounter or captcha)
            await asyncio.wait_for(state['explore_response_event'].wait(), timeout=5)
            log.info(f"[✓] Got response for /explore user {user_id}")
        except asyncio.TimeoutError:
            log.warning(f"[✗] No response after /explore for {user_id}, retrying...")
            if farming_enabled.get(user_id, False):
                await jitter_sleep(0.5, 1.0)
                await safe_explore(client, user_id)

    except Exception as e:
        log.error(f"[✗] Failed to send /explore: {e}")
        if retry_on_fail:
            await jitter_sleep(0.3, 0.6)
            await send_explore_with_timeout(client, user_id, False)

async def handle_buttons(event, user_id, stage, prevent_repeat=False):
    
    if not farming_enabled.get(user_id, False):
        return
    
    if user_id not in user_session_state:
        user_session_state[user_id] = {'latest_msg_id': None}
    
    if prevent_repeat and event.id == user_session_state[user_id]['latest_msg_id']:
        return
    user_session_state[user_id]['latest_msg_id'] = event.id

    if not event.buttons:
        return

    engage_variants = [
        "Eńɢaǵe","ⴹnɠаge","Ꮛngаge","Ɛṅgaɢe","𝓔ṅg͜age","𝐄ŋɡạɠe","Eṅɡaḡe",
        "Εñgαge","Ẹngaɢe","Ɛńɡàɡe","Ẹɲgḁge","Ꮛngаge","Eŋ͎gấɠє","Ẹ͛nɡᶏɠe","engage"
    ]

    for row in event.buttons:
        for button in row:
            text = button.text.lower().replace(" ", "")
            if any(v.lower().replace(" ", "") in text for v in engage_variants) or "prestige" in text:
                try:
                    await jitter_sleep()
                    await button.click()
                    log.info(f"[✓] Clicked Engage/Prestige for {user_id}: {button.text}")
                    return
                except Exception as e:
                    log.error(f"[✗] Failed to click Engage/Prestige: {e}")
                    return

async def handle_combat(event, user_id):
    if not farming_enabled.get(user_id, False):
        return False

    state = user_session_state.get(user_id, {})
    if state.get("captcha_active", False):
        log.warning(f"[⏸️] Combat halted for user {user_id} - captcha active")
        return False

    text = event.raw_text
    try:
        if "dealt" in text or "blocked" in text:
            await jitter_sleep()
            await event.click(0)
            return True

        if any(word in text for word in [
            "Ring of Life","Demonic seal","Insanity Rune ","Eternal Elixir",
            "Cursed sword","Flame Amulet","Phantom of Death","Venomous Dagger",
            "Resurrection Lyre","Will of Wind","Evasion Boot","Chaotic Totem",
            "Iris Talisman","Sensory Stone","Pathbreaker Veil","Frostbound Prism",
            "Friendship Band","Unity Pendant","Comrade Emblem","Anguish Sigil",
            "Blood Sigil","Hypnotic Orb","Dreamer Lamp","Echoing Barrier","Invincible Aura",
            "Craftman Hammer","Anti Matter","Starforged Aegis","Guardian Mantle","Identical Mask",
            "Celestial shield","Devine Relic","Diamond Gauntlet","Lucky Dice","Sukuna Finger","Thunder Spear",
            "Philosopher Stone","Devil Fruit","SeaPrism Stone","Vivre Card","Reverse Blade Sword","Elixir of Life",
            "Raphael","Hogyoku","Zanpakuto","Soul Candy","Mana Crystal"
        ]):
            await jitter_sleep()
            await event.click(1, 0)
            return True

        if "battle status" in text or "dizzy" in text:
            await jitter_sleep()
            await event.click(1, 1)
            return True

    except Exception as e:
        log.error(f"[✗] Error in combat: {e}")
        return False

    return False

async def attach_handlers(user_id, client: TelegramClient):
    if user_id not in user_session_state:
        user_session_state[user_id] = {
            'explore_response_event': asyncio.Event(),
            'explore_waiting': False,
            'in_combat_or_capture': False,
            'captcha_active': False,
            'latest_msg_id': None
        }

    async def handle_game_event(event, edited=False):
        state = user_session_state[user_id]
        text = event.raw_text.lower()

        # Only process messages from BOT_ID in direct messages
        if event.is_private and event.sender_id == BOT_ID:
            
            # ==============================
            # ESSENCES FOUND NOTIFICATION
            # ==============================
            if "essences" in text:
                farming_enabled[user_id] = False
                user_name = get_user_name(user_id)
                notification_text = f"🧪 Farming paused for {user_name} - Essences found!\n\n{event.raw_text}"
                send_group_notification(user_id, notification_text)
                return

            # Update combat state
            state['in_combat_or_capture'] = any(k in text for k in ["move", "moves", "randomly attack", "⚔️", "trader"])

            # CAPTCHA detection - only in BOT_ID DMs
            captcha_detected = any(ph in text for ph in [
                "defeat before you can continue", "upon an Ancient", "you like to enter",
                "select the correct number of monsters", "rich merchant", "found a Village",
                "ship", "are few eggs", "you stumble upon evil mystic wizard"
            ])
            
            # Update captcha state only if detected in BOT_ID DM
            state['captcha_active'] = captcha_detected

            # ==============================
            # CAPTCHA NOTIFICATION
            # ==============================
            if "have incoming connections from" in text or state['captcha_active']:
                state['explore_response_event'].set()
                state['captcha_active'] = True
                user_name = get_user_name(user_id)
                notification_text = f"❗ CAPTCHA detected for {user_name}!\n\n{event.raw_text}"
                send_group_notification(user_id, notification_text)
                return

            # Encounter detection (includes ⚔️ and note)
            if any(k in text for k in ["/explore", "threat level", "you run into", "encounter", "⚔️", "note"]):
                state['explore_response_event'].set()
                await handle_buttons(event, user_id, "Monster", True)

            # Safe explore loop - ONLY if not in combat or captcha
            if any(k in text for k in [
                "wishing fountain","make a wish","successfully traded with","walked away"
                "exploring","while","you earned","pocket","core","away with","traded with","merchant left"
            ]):
                if "check out the offers" not in text and "offers you" not in text:
                    if farming_enabled.get(user_id, False) and not state['in_combat_or_capture'] and not state['captcha_active']:
                        await jitter_sleep()
                        await send_explore_with_timeout(client, user_id, True)

            # Combat
            if state['in_combat_or_capture']:
                await handle_combat(event, user_id)

            # Continue exploring - ONLY if not in combat or captcha
            elif "also found" in text or "you get" in text:
                # Only send explore if we haven't already processed this message for explore
                if not state.get('explore_sent_for_message', False):
                    state['explore_sent_for_message'] = True
                    if farming_enabled.get(user_id, False) and not state['in_combat_or_capture'] and not state['captcha_active']:
                        await jitter_sleep()
                        await send_explore_with_timeout(client, user_id, True)
                else:
                    # Reset the flag for the next message
                    state['explore_sent_for_message'] = False

    # Register both new and edited messages - only from BOT_ID
    @client.on(events.NewMessage(from_users=BOT_ID))
    async def on_new(event):
        await handle_game_event(event, edited=False)

    @client.on(events.MessageEdited(from_users=BOT_ID))
    async def on_edit(event):
        await handle_game_event(event, edited=True)

    @client.on(events.NewMessage(from_users=BOT_ID))
    @client.on(events.MessageEdited(from_users=BOT_ID))
    async def trader(event):
        if not farming_enabled.get(user_id, False):
            return
        t = event.raw_text.lower()
    
        # Add the new condition first
        if "successfully traded with trader" in t:
            await jitter_sleep()
            await client.send_message(BOT_ID, "/explore")
            return
    
        if "trader" in t:
            for row in event.buttons:
                for button in row:
                    if "check out offers" in button.text.lower():
                        await jitter_sleep(0.7, 0.9)
                        await button.click()
                        return
        if "offers you" in t:
            per_pearl, per_ticket = None, None
            for line in t.split("\n"):
                if "pearls for" in line:
                    m = re.search(r'for (\d+)', line)
                    per_pearl = int(m.group(1)) if m else None
                elif "tickets for" in line:
                    m = re.search(r'for (\d+)', line)
                    per_ticket = int(m.group(1)) if m else None
            if per_pearl and per_pearl <= get_user_pearl_price(user_id):
                await jitter_sleep()
                await event.click(0)
            elif per_ticket and per_ticket <= get_user_ticket_price(user_id):
                await jitter_sleep()
                await event.click(0)
            else:
                await jitter_sleep()
                await safe_explore(client, user_id)

    @client.on(events.NewMessage(from_users=BOT_ID))
    async def fight_new(event):
        if not farming_enabled.get(user_id, False):
            return
        if "defeat before you can continue" in event.raw_text.lower():
            await jitter_sleep()
            await client.send_message(BOT_ID,"/fight")

    @client.on(events.MessageEdited(from_users=BOT_ID))
    async def fight_edit(event):
        if not farming_enabled.get(user_id, False):
            return
        if "defeat before you can continue" in event.raw_text.lower():
            await jitter_sleep()
            await client.send_message(BOT_ID,"/explore")

    @client.on(events.NewMessage(from_users=BOT_ID))
    @client.on(events.MessageEdited(from_users=BOT_ID))
    async def pet(event):
        # Extract user_id from client session filename
        session_name = client.session.filename
        user_id = int(session_name.replace("session_", "").replace(".session", ""))
        
        if not farming_enabled.get(user_id, False):
            return
        
        text = event.raw_text.lower()

        # Step 1: Capture attempt
        if "and capture it" in text or "to try" in text or "you want to try" in text:
            await asyncio.sleep(0.5)
            await event.click(0, 1)
            log.info(f"[🎯] Tried to capture pet for user {user_id}")
            return

        # Step 2: Rarity check after capture
        if any(r in text for r in ["rarity : rare", "rarity : common"]):
            log.info(f"[⚪] Common/Rare pet detected for user {user_id} - walking away...")
            for row in event.buttons:
                for button in row:
                    if "walk away" in button.text.lower():
                        await asyncio.sleep(0.5)
                        await button.click()
                        log.info(f"[🚶] Clicked: {button.text} for user {user_id}")
                        break
        
            # Added the requested line
            if "walked away" in event.raw_text.lower():
                await jitter_sleep()
                await client.send_message(BOT_ID, "/explore")
            
            if farming_enabled.get(user_id, False):
                await asyncio.sleep(0.5)
                await client.send_message(BOT_ID, "/explore")
            return

        if any(r in text for r in ["rarity : epic", "rarity : crossover", "rarity : exotic", "rarity : exclusive"]):
            log.info(f"[✨] Special pet detected for user {user_id} - notifying user")
            farming_enabled[user_id] = False  # pause farming for this user
            
            # GROUP NOTIFICATION FOR SPECIAL PETS
            user_name = get_user_name(user_id)
            notification_text = f"✨ Special pet appeared for {user_name}:\n\n{event.raw_text}"
            send_group_notification(user_id, notification_text)
            
            return

# ==============================
# LOGIN (MODIFIED FOR MONGODB SESSION STORAGE)
# ==============================
async def start_client(user_id: int, phone: str):
    if user_id in pending_clients:
        bot.send_message(user_id, "⚠️ Login already in progress.")
        return None

    old = user_clients.get(user_id)
    if old:
        try:
            await old.disconnect()
        except:
            pass
        user_clients.pop(user_id, None)

    session = f"session_{user_id}"
    session_file = f"{session}.session"

    # Check if session exists in MongoDB instead of local file
    if mongo_manager and mongo_manager.session_file_exists(user_id):
        try:
            # Get session data from MongoDB
            session_data = mongo_manager.get_session_file(user_id)
            if session_data:
                # Create a temporary session file for Telethon to use
                with open(session_file, 'wb') as f:
                    f.write(session_data)
                log.info(f"[📁] Loaded session from MongoDB for user {user_id}")
        except Exception as e:
            log.warning(f"[!] Could not load session from MongoDB: {e}")

    try:
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()
    except Exception as e:
        bot.send_message(user_id, f"❌ Failed to start login: {e}")
        return None

    if not await client.is_user_authorized():
        try:
            await client.send_code_request(phone)
            pending_expect[user_id] = "otp"
            pending_clients[user_id] = client
            bot.send_message(user_id, "📲 Enter the OTP (like 1 2 3 4 5).")
        except Exception as e:
            bot.send_message(user_id, f"❌ Could not send code: {e}")
            return None
    else:
        await attach_handlers(user_id, client)
        user_clients[user_id] = client
        farming_enabled[user_id] = False
        set_user_logged_in(user_id, True)  # Mark as logged in
        
        # Save session to MongoDB after successful authorization
        if mongo_manager and os.path.exists(session_file):
            try:
                with open(session_file, 'rb') as f:
                    session_data = f.read()
                mongo_manager.save_session_file(user_id, session_data)
                log.info(f"[💾] Session saved to MongoDB for user {user_id}")
            except Exception as e:
                log.error(f"[❌] Failed to save session to MongoDB: {e}")
        
        bot.send_message(user_id, "✅ Session restored! Use /toggle to start farming.")

    return client

async def complete_login(user_id, code=None, password=None):
    client = user_clients.get(user_id) or pending_clients.get(user_id)
    if not client:
        bot.send_message(user_id, "⚠️ No pending login session.")
        return False

    try:
        if code:
            try:
                await client.sign_in(code=code)
            except SessionPasswordNeededError:
                pending_expect[user_id] = "password"
                bot.send_message(user_id, "🔑 Enter your 2FA password:")
                return False
        elif password:
            await client.sign_in(password=password)

        if await client.is_user_authorized():
            user_clients[user_id] = client
            pending_clients.pop(user_id, None)
            pending_expect[user_id] = None
            set_user_logged_in(user_id, True)  # Mark as logged in
            
            # Save session to MongoDB after successful login
            session_file = f"session_{user_id}.session"
            if mongo_manager and os.path.exists(session_file):
                try:
                    with open(session_file, 'rb') as f:
                        session_data = f.read()
                    mongo_manager.save_session_file(user_id, session_data)
                    log.info(f"[💾] Session saved to MongoDB for user {user_id}")
                except Exception as e:
                    log.error(f"[❌] Failed to save session to MongoDB: {e}")
            
            bot.send_message(user_id, "✅ Login done! Use /toggle to farm.")
            await attach_handlers(user_id, client)
            return True

    except Exception as e:
        bot.send_message(user_id, f"❌ Login failed: {e}")
        return False

# ==============================
# GROUP NOTIFICATIONS
# ==============================
@bot.message_handler(commands=['gcnoti'])
def cmd_gcnoti(message):
    uid = message.from_user.id
    user_data_obj = get_user_data(uid)
    
    # Save to MongoDB
    if mongo_manager:
        mongo_manager.save_user_data(uid, user_data_obj)
    
    if user_data_obj['group_id'] is None:
        # First time - ask for group ID
        msg = bot.reply_to(message, "📋 Please send the group ID where you want notifications:")
        bot.register_next_step_handler(msg, process_group_id, uid)
    else:
        # Show toggle with Change Group button
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🟢 On", callback_data=f"gcnoti_on:{uid}"),
            types.InlineKeyboardButton("🔴 Off", callback_data=f"gcnoti_off:{uid}")
        )
        markup.add(
            types.InlineKeyboardButton("🔄 Change Group", callback_data=f"gcnoti_change:{uid}")
        )
        
        status = "🟢 ON" if user_data_obj['gc_noti'] else "🔴 OFF"
        bot.send_message(
            uid, 
            f"Do you want to send notifications to group {user_data_obj['group_id']}?\nCurrent status: {status}",
            reply_markup=markup
        )

def process_group_id(message, uid):
    try:
        group_id = int(message.text.strip())
        user_data_obj = get_user_data(uid)
        user_data_obj['group_id'] = group_id
        
        # Save to MongoDB
        if mongo_manager:
            mongo_manager.save_user_data(uid, user_data_obj)
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🟢 On", callback_data=f"gcnoti_on:{uid}"),
            types.InlineKeyboardButton("🔴 Off", callback_data=f"gcnoti_off:{uid}")
        )
        markup.add(
            types.InlineKeyboardButton("🔄 Change Group", callback_data=f"gcnoti_change:{uid}")
        )
        
        bot.send_message(
            uid,
            f"Do you want to send notifications to group {group_id}?",
            reply_markup=markup
        )
    except ValueError:
        bot.reply_to(message, "❌ Invalid group ID. Please send a numeric group ID.")

def process_change_group(message, uid):
    try:
        new_group_id = int(message.text.strip())
        user_data_obj = get_user_data(uid)
        old_group_id = user_data_obj['group_id']
        user_data_obj['group_id'] = new_group_id
        
        # Save to MongoDB
        if mongo_manager:
            mongo_manager.save_user_data(uid, user_data_obj)
        
        # Send confirmation to the new group
        try:
            bot.send_message(new_group_id, f"✅ Group notifications have been set up for user {uid}. Future notifications will be sent here.")
        except Exception as e:
            log.error(f"Could not send confirmation to new group {new_group_id}: {e}")
        
        # Send confirmation to user
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🟢 On", callback_data=f"gcnoti_on:{uid}"),
            types.InlineKeyboardButton("🔴 Off", callback_data=f"gcnoti_off:{uid}")
        )
        markup.add(
            types.InlineKeyboardButton("🔄 Change Group", callback_data=f"gcnoti_change:{uid}")
        )
        
        bot.send_message(
            uid,
            f"✅ Group changed from {old_group_id} to {new_group_id}!\n\nDo you want to send notifications to the new group?",
            reply_markup=markup
        )
        
    except ValueError:
        msg = bot.reply_to(message, "❌ Invalid group ID. Please send a numeric group ID:")
        bot.register_next_step_handler(msg, process_change_group, uid)

# ==============================
# BOT COMMANDS WITH MONGODB INTEGRATION
# ==============================
@bot.message_handler(commands=['start'])
def send_welcome(message):
    try:
        # Get user information
        user_id = message.from_user.id
        first_name = message.from_user.first_name
        last_name = message.from_user.last_name
        
        # Format user's name
        user_name = first_name
        if last_name:
            user_name += f" {last_name}"
        
        # Get user profile photos
        photos = bot.get_user_profile_photos(user_id, limit=1)
        
        welcome_text = f"✨ Welcome {user_name}!\n\n🤖 I am Aura Farming Bot.\n\n👉 Use /help to see all commands."
        
        # Check if user has profile picture
        if photos.total_count > 0:
            # Get the largest version of the profile picture
            photo = photos.photos[0][-1]
            file_id = photo.file_id
            
            # Send photo with caption
            bot.send_photo(message.chat.id, file_id, caption=welcome_text)
        else:
            # If no profile picture, send text only
            bot.send_message(message.chat.id, welcome_text)
            
    except Exception as e:
        # Fallback in case of any error
        welcome_text = f"✨ Welcome!\n\n🤖 I am Aura Farming Bot.\n\n👉 Use /help to see all commands."
        bot.send_message(message.chat.id, welcome_text)
        print(f"Error in start command: {e}")

@bot.message_handler(commands=['help'])
def cmd_help(message):
    """Send the help menu with buttons"""
    help_text, markup = create_help_message()
    bot.send_message(
        message.chat.id,
        help_text,
        reply_markup=markup,
        parse_mode="Markdown"
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith('help_'))
def help_callback(call):
    """Handle help button presses safely"""
    try:
        # Determine which section to show
        if call.data == 'help_user':
            text = get_user_commands()
        elif call.data == 'help_owner':
            text = get_owner_commands()
        else:
            bot.answer_callback_query(call.id)
            return

        try:
            # Use MarkdownV2 for safer formatting
            bot.edit_message_text(
                text=text,
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                parse_mode="MarkdownV2"
            )
        except Exception:
            # Fallback to plain text if MarkdownV2 fails
            bot.edit_message_text(
                text=text,
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )

        bot.answer_callback_query(call.id)

    except Exception as e:
        log.error(f"Help callback error: {e}")
        bot.answer_callback_query(call.id, "Error loading help.")


def create_help_message():
    """Create help message with inline buttons"""
    markup = types.InlineKeyboardMarkup()
    
    # Buttons for user & owner help
    user_btn = types.InlineKeyboardButton("User Cmds", callback_data="help_user")
    owner_btn = types.InlineKeyboardButton("Owner Cmds", callback_data="help_owner")
    
    markup.add(user_btn, owner_btn)
    help_text = "📖 *Aura Farming Bot – Help Menu*\n\nChoose a section below:"
    
    return help_text, markup


def get_user_commands():
    """User commands help text"""
    return (
        "🤖 *User Commands:*\n\n"
        "*Farming:*\n"
        "/toggle - Start aura farming system\n\n"
        "*Settings:*\n"
        "/gcnoti - Toggle group notifications\n"
        "/setup - Login to aura bot\n"
        "/delete - Delete your session\n"
        "/approval_status - Check approval status\n\n"
        "*Info:*\n"
        "/help - Show this help message\n"
        "/ping - Check bot latency"
    )


def get_owner_commands():
    """Owner commands help text"""
    return (
        "👑 *Owner & Admin Commands:*\n\n"
        "*User Management:*\n"
        "/approve <id> [duration] - Approve user\n"
        "/unapprove <id> - Remove approval\n"
        "/approvelist - List approved users\n"
        "/dbstats - Database Statistics\n\n"
        "*Admin Management (Owner Only):*\n"
        "/promote <id> - Promote user to admin\n"
        "/demote <id> - Demote admin\n"
        "/adminlist - List all admins\n\n"
        "*Duration formats:*\n"
        "`1d` - 1 day, `1w` - 1 week, `1m` - 1 month, `p` - permanent"
    )

@bot.message_handler(commands=['ping'])
def cmd_ping(message):
    t0=time.time()
    sent=bot.reply_to(message,"Pinging…")
    elapsed=int((time.time()-t0)*1000)
    bot.edit_message_text(f"PONG {elapsed} ms",sent.chat.id,sent.message_id)

# ==============================
# NEW ADMIN MANAGEMENT COMMANDS
# ==============================

@bot.message_handler(commands=['promote'])
@owner_only_strict
def cmd_promote(message):
    """Promote a user to admin (owner only)"""
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /promote <user_id>")
        return
    
    try:
        user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ Invalid user ID")
        return
    
    # Check if user exists
    try:
        user_info = bot.get_chat(user_id)
        user_name = f"{user_info.first_name or ''} {user_info.last_name or ''}".strip() or f"User_{user_id}"
    except Exception:
        user_name = f"User_{user_id}"
    
    # Check if already admin
    if user_id in admins:
        bot.reply_to(message, f"❌ {user_name} ({user_id}) is already an admin")
        return
    
    # Promote user
    admins.add(user_id)
    if save_admins():
        bot.reply_to(message, f"✅ Promoted {user_name} ({user_id}) to admin")
        
        # Notify the promoted user
        try:
            bot.send_message(user_id, "🎉 You have been promoted to admin! You now have access to admin commands.")
        except Exception as e:
            log.error(f"Could not notify promoted user {user_id}: {e}")
    else:
        bot.reply_to(message, f"❌ Failed to promote {user_name} ({user_id})")

@bot.message_handler(commands=['demote'])
@owner_only_strict
def cmd_demote(message):
    """Demote an admin (owner only)"""
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /demote <user_id>")
        return
    
    try:
        user_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ Invalid user ID")
        return
    
    # Check if user is actually an admin
    if user_id not in admins:
        bot.reply_to(message, f"❌ User {user_id} is not an admin")
        return
    
    # Get user name
    try:
        user_info = bot.get_chat(user_id)
        user_name = f"{user_info.first_name or ''} {user_info.last_name or ''}".strip() or f"User_{user_id}"
    except Exception:
        user_name = f"User_{user_id}"
    
    # Demote user
    admins.remove(user_id)
    if save_admins():
        bot.reply_to(message, f"✅ Demoted {user_name} ({user_id}) from admin")
        
        # Notify the demoted user
        try:
            bot.send_message(user_id, "🔻 You have been demoted from admin role.")
        except Exception as e:
            log.error(f"Could not notify demoted user {user_id}: {e}")
    else:
        bot.reply_to(message, f"❌ Failed to demote {user_name} ({user_id})")

@bot.message_handler(commands=['adminlist'])
@owner_only_strict
def cmd_adminlist(message):
    """List all admins (owner only)"""
    if not admins:
        bot.reply_to(message, "No admins found.")
        return
    
    response = "👑 Admins:\n"
    for admin_id in admins:
        try:
            user_info = bot.get_chat(admin_id)
            user_name = f"{user_info.first_name or ''} {user_info.last_name or ''}".strip() or f"User_{admin_id}"
        except Exception:
            user_name = f"User_{admin_id}"
        
        response += f"• {user_name} ({admin_id})\n"
    
    response += f"\nTotal: {len(admins)} admin(s)"
    bot.reply_to(message, response)

@bot.message_handler(commands=['approve'])
@admin_only
def cmd_approve(message):
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /approve <user_id> [duration]\nDurations: 1d, 1w, 1m, p (permanent)")
        return
    
    try:
        uid = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ Invalid user ID")
        return
    
    duration_str = parts[2] if len(parts) > 2 else 'p'
    expiration = get_expiration_time(duration_str)
    
    if expiration is None and duration_str != 'p':
        bot.reply_to(message, "❌ Invalid duration. Use: 1d, 1w, 1m, or p (permanent)")
        return
    
    # Save to MongoDB
    approved_users[uid] = expiration
    if mongo_manager:
        mongo_manager.save_approved_user(uid, expiration)
    
    duration_text = "permanent" if expiration is None else format_time_remaining(expiration)
    user_name = get_user_name(uid)
    bot.reply_to(message, f"✅ Approved {user_name} ({uid}) for {duration_text}")

@bot.message_handler(commands=['unapprove'])
@admin_only
def cmd_unapprove(message):
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /unapprove <user_id>")
        return
    
    try:
        uid = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ Invalid user ID")
        return
    
    if uid in approved_users:
        approved_users.pop(uid)
        if mongo_manager:
            mongo_manager.remove_approved_user(uid)
        user_name = get_user_name(uid)
        bot.reply_to(message, f"❌ Unapproved {user_name} ({uid})")
    else:
        bot.reply_to(message, f"❌ User {uid} was not approved")

@bot.message_handler(commands=['approval_status'])
def cmd_approval_status(message):
    uid = message.from_user.id
    if uid not in approved_users:
        bot.reply_to(message, "❌ You are not approved")
        return
    
    expiration = approved_users[uid]
    if expiration is None:
        bot.reply_to(message, "✅ Your approval is permanent")
    else:
        remaining = format_time_remaining(expiration)
        if remaining == "expired":
            bot.reply_to(message, "❌ Your approval has expired")
            approved_users.pop(uid)
            if mongo_manager:
                mongo_manager.remove_approved_user(uid)
        else:
            bot.reply_to(message, f"✅ Your approval expires in {remaining}")

@bot.message_handler(commands=['approvelist'])
@admin_only
def cmd_list_approvals(message):
    if not approved_users:
        bot.reply_to(message, "No approved users")
        return
    
    response = "Approved users:\n"
    for uid, expiration in approved_users.items():
        user_name = get_user_name(uid)
        status = "permanent" if expiration is None else format_time_remaining(expiration)
        response += f"• {user_name} ({uid}): {status}\n"
    
    bot.reply_to(message, response)

@bot.message_handler(commands=['cancel'])
def cmd_cancel(message):
    uid = message.from_user.id
    client = user_clients.get(uid) or pending_clients.get(uid)

    if not client and not waiting_for_phone.get(uid, False):
        bot.reply_to(message, "⚠️ No active farming or login session to cancel.")
        return

    async def do_cancel():
        try:
            if client:
                await client.disconnect()
        except:
            pass
        
        # Only clean up from memory, NOT from MongoDB
        user_clients.pop(uid, None)
        pending_clients.pop(uid, None)
        farming_enabled[uid] = False
        pending_expect[uid] = None
        waiting_for_phone[uid] = False
        user_session_state.pop(uid, None)
        set_user_logged_in(uid, False)  # Mark as logged out
        
        bot.send_message(uid, "⛔ Session cancelled. Use /setup to start again.")

    asyncio.run_coroutine_threadsafe(do_cancel(), loop)

@bot.message_handler(commands=['delete'])
def cmd_delete(message):
    uid = message.from_user.id
    
    client = user_clients.get(uid) or pending_clients.get(uid)
    
    if not client and not waiting_for_phone.get(uid, False):
        bot.reply_to(message, "⚠️ No active session to delete.")
        return

    async def do_delete():
        try:
            if client:
                await client.disconnect()
        except:
            pass
        
        # Clean up user session from memory
        cleanup_user_session(uid)
        
        # Delete session file from MongoDB
        if mongo_manager:
            mongo_manager.delete_session_file(uid)
            log.info(f"[🗑️] Deleted session file from MongoDB for user {uid}")
        
        # Also delete local session file if it exists
        session_file = f"session_{uid}.session"
        if os.path.exists(session_file):
            os.remove(session_file)
            log.info(f"[🗑️] Deleted local session file for user {uid}")
        
        bot.send_message(uid, "🗑️ Session deleted successfully! Use /setup to login again with your phone number and OTP.")

    asyncio.run_coroutine_threadsafe(do_delete(), loop)
    
@bot.message_handler(commands=['setup'])
def cmd_setup(message):
    uid = message.from_user.id
    if not is_approved(uid):
        bot.reply_to(message, "🚫 Not approved or approval expired.")
        return

    # Check if user already has an active session in memory
    if uid in user_clients:
        bot.reply_to(message, "✅ You are already logged in! Use /toggle to start farming.")
        return

    # Check if session exists in MongoDB but not in memory (after /cancel)
    if mongo_manager and mongo_manager.session_file_exists(uid):
        # Session exists in DB but not in memory - restore it
        bot.reply_to(message, "🔄 Restoring your existing session...")
        asyncio.run_coroutine_threadsafe(restore_existing_session(uid), loop)
    else:
        # No existing session - start new login
        waiting_for_phone[uid] = True
        msg = bot.reply_to(message, "📱 Send your phone number with country code:")
        bot.register_next_step_handler(msg, lambda m: process_phone(m, uid))

def process_phone(message, uid):
    if not waiting_for_phone.get(uid, False):
        return

    phone = message.text.strip()
    if not phone.startswith("+"):
        msg = bot.reply_to(message, "❌ Invalid. Retry.")
        bot.register_next_step_handler(msg, lambda m: process_phone(m, uid))
        return

    waiting_for_phone[uid] = False
    future = asyncio.run_coroutine_threadsafe(start_client(uid, phone), loop)
    try:
        future.result()
    except Exception as e:
        bot.send_message(uid, f"❌ Failed to start login: {e}")

@bot.message_handler(commands=['toggle'])
def cmd_toggle(message):
    uid=message.from_user.id
    if uid not in user_clients:
        bot.reply_to(message,"⚠️ Not logged in.")
        return
    markup=types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🟢 On",callback_data=f"on:{uid}"),
               types.InlineKeyboardButton("🔴 Off",callback_data=f"off:{uid}"))
    bot.send_message(uid,"Toggle Userbot:",reply_markup=markup)

@bot.callback_query_handler(func=lambda c:c.data.startswith("on:") or c.data.startswith("off:"))
def cb_toggle(call):
    action,uid_str=call.data.split(":")
    uid=int(uid_str)
    if call.from_user.id!=uid and call.from_user.id!=BOT_OWNER_ID and not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id,"Not allowed")
        return
    client=user_clients.get(uid)
    if not client:
        return
    if action=="on":
        farming_enabled[uid]=True
        asyncio.run_coroutine_threadsafe(send_explore_with_timeout(client, uid, True), loop)
        bot.send_message(uid,"🟢 Farming started")
        bot.answer_callback_query(call.id,"Started")
    else:
        farming_enabled[uid]=False
        bot.send_message(uid,"🔴 Farming stopped")
        bot.answer_callback_query(call.id,"Stopped")

@bot.message_handler(commands=['rate'])
def cmd_rate(message):
    uid = message.from_user.id
    if not is_approved(uid):
        bot.reply_to(message, "🚫 Not approved or approval expired.")
        return

    current_pearl = get_user_pearl_price(uid)
    current_ticket = get_user_ticket_price(uid)
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("💎 Pearl Price", callback_data=f"rate_pearl:{uid}"),
        types.InlineKeyboardButton("🎫 Ticket Price", callback_data=f"rate_ticket:{uid}")
    )
    
    bot.reply_to(
        message,
        f"Current rates:\n💎 Pearl: {current_pearl}\n🎫 Ticket: {current_ticket}\nSelect to change:",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith("rate_"))
def cb_rate(call):
    action, uid_str = call.data.split(":")
    uid = int(uid_str)
    
    if call.from_user.id != uid and call.from_user.id != BOT_OWNER_ID and not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Not allowed")
        return
    
    if action == "rate_pearl":
        msg = bot.send_message(uid, f"Enter new max pearl price (current: {get_user_pearl_price(uid)}):")
        bot.register_next_step_handler(msg, process_pearl_price, uid)
    elif action == "rate_ticket":
        msg = bot.send_message(uid, f"Enter new max ticket price (current: {get_user_ticket_price(uid)}):")
        bot.register_next_step_handler(msg, process_ticket_price, uid)
    
    bot.answer_callback_query(call.id)

def process_pearl_price(message, uid):
    try:
        price = int(message.text.strip())
        if price < 0:
            bot.reply_to(message, "❌ Price must be positive")
            return
        set_user_pearl_price(uid, price)
        bot.reply_to(message, f"✅ Pearl price set to {price}")
    except ValueError:
        bot.reply_to(message, "❌ Invalid number")

def process_ticket_price(message, uid):
    try:
        price = int(message.text.strip())
        if price < 0:
            bot.reply_to(message, "❌ Price must be positive")
            return
        set_user_ticket_price(uid, price)
        bot.reply_to(message, f"✅ Ticket price set to {price}")
    except ValueError:
        bot.reply_to(message, "❌ Invalid number")

@bot.message_handler(commands=['debug'])
def cmd_debug(message):
    uid = message.from_user.id
    if uid in debug_users:
        debug_users.remove(uid)
        bot.reply_to(message, "🔴 Debug off")
    else:
        debug_users.add(uid)
        bot.reply_to(message, "🟢 Debug on")

@bot.callback_query_handler(func=lambda c: c.data.startswith("gcnoti_"))
def cb_gcnoti(call):
    action, uid_str = call.data.split(":")
    uid = int(uid_str)
    
    if call.from_user.id != uid and call.from_user.id != BOT_OWNER_ID and not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Not allowed")
        return
    
    user_data_obj = get_user_data(uid)
    
    if action == "gcnoti_on":
        user_data_obj['gc_noti'] = True
        if mongo_manager:
            mongo_manager.save_user_data(uid, user_data_obj)
        bot.send_message(uid, "🟢 Group notifications ON")
    elif action == "gcnoti_off":
        user_data_obj['gc_noti'] = False
        if mongo_manager:
            mongo_manager.save_user_data(uid, user_data_obj)
        bot.send_message(uid, "🔴 Group notifications OFF")
    elif action == "gcnoti_change":
        msg = bot.send_message(uid, "📋 Send me your new group's chat ID:")
        bot.register_next_step_handler(msg, process_change_group, uid)
    
    bot.answer_callback_query(call.id)

# ==============================
# NEW COMMAND: DATABASE STATS
# ==============================

@bot.message_handler(commands=['dbstats'])
@admin_only
def cmd_dbstats(message):
    """Show MongoDB database statistics"""
    try:
        if not mongo_manager:
            bot.reply_to(message, "❌ MongoDB is not connected")
            return
            
        stats = mongo_manager.get_database_stats()
        
        response = "📊 **Database Statistics:**\n\n"
        response += f"👥 Approved Users: `{stats.get('approved_users', 0)}`\n"
        response += f"⚙️ User Configs: `{stats.get('user_configs', 0)}`\n"
        response += f"📝 User Data: `{stats.get('user_data', 0)}`\n"
        response += f"💾 Session Files: `{stats.get('session_files', 0)}`\n"
        response += f"💾 Active Sessions: `{stats.get('sessions', 0)}`\n"
        response += f"👑 Admins: `{len(admins)}`\n\n"
        response += "💡 All data is now stored in MongoDB!"
        
        bot.reply_to(message, response, parse_mode="Markdown")
        
    except Exception as e:
        bot.reply_to(message, f"❌ Error getting database stats: {e}")

@bot.message_handler(func=lambda m:True,content_types=['text'])
def generic_text(message):
    uid = message.from_user.id
    txt = message.text.strip()
    expect = pending_expect.get(uid)
    if expect=="otp":
        code = re.sub(r"\s+","",txt)
        asyncio.run_coroutine_threadsafe(complete_login(uid,code=code),loop)
    elif expect=="password":
        asyncio.run_coroutine_threadsafe(
            complete_login(uid, password=txt), loop
        )

# ==============================
# PERIODIC CLEANUP
# ==============================
async def cleanup_expired_approvals():
    """Periodically clean up expired approvals"""
    while True:
        await asyncio.sleep(3600)  # Check every hour
        current_time = time.time()
        expired_users = []
        
        for uid, expiration in approved_users.items():
            if expiration is None:
                continue
            if current_time > expiration:
                expired_users.append(uid)
        
        for uid in expired_users:
            approved_users.pop(uid)
            log.info(f"Removed expired approval for user {uid}")
        
        if expired_users and mongo_manager:
            mongo_manager.cleanup_expired_approvals()

# ==============================
# MAIN WITH KEEP-ALIVE
# ==============================
def start_polling():
    bot.infinity_polling(timeout=60, long_polling_timeout=60)

if __name__ == "__main__":
    # Start web server in background thread
    keep_alive()  # This starts the Flask web server from web_server.py
    
    # Start the periodic cleanup task
    asyncio.run_coroutine_threadsafe(cleanup_expired_approvals(), loop)
    
    threading.Thread(target=start_polling, daemon=True).start()
    loop.run_forever()
