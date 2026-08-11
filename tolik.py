# ====================================================================
# MCore Tolik - Version 2.0.3 (Render Production Ready)
# Исправления: self-ping, eviction памяти, graceful shutdown,
# rate limit per user, O(n²) в парсере, + все фиксы из 2.0.2
# ====================================================================
import os
import re
import sys
import random
import asyncio
import ipaddress
import socket
import urllib.parse
import time
import logging
import signal
from threading import Thread
from collections import defaultdict, deque, Counter
from typing import Optional, Dict
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from flask import Flask
from waitress import serve
from huggingface_hub import InferenceClient

import discord
from discord.ext import commands
from discord import app_commands

# =========================================================================
# КОНФИГУРАЦИЯ
# =========================================================================
BOT_VERSION = "2.0.3 (Render Ready)"
MODEL_NAME = "deepseek-ai/DeepSeek-V4-Flash"

AI_RATE_LIMIT_WINDOW = 60  # секунд
AI_RATE_LIMIT_MAX_REQUESTS = 5  # запросов за окно НА ПОЛЬЗОВАТЕЛЯ

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MCoreTolik")

TOLIK_TOKEN = os.environ.get("TOLIK_TOKEN")
HF_TOKEN = os.environ.get("HF_TOKEN")
MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "smmunity_database")
PORT = int(os.environ.get("PORT", 10000))

MAX_CONTINUATIONS = 200
MAX_PROMPT_CHARS = 2000
AI_TIMEOUT_SECONDS = 20
MAX_BODY_BYTES = 1_000_000
LINK_REQUEST_TIMEOUT = 5
BLOCKED_HOSTS = {"localhost", "metadata.google.internal", "169.254.169.254"}

HOST_CACHE_TTL = 300
MAX_CACHED_BRAINS = 50  # LRU eviction
BRAIN_IDLE_TTL = 600    # 10 минут без использования — выгружаем
AI_CONTEXT_MAXLEN = 12  # 6 диалогов
AI_CONTEXT_CLEANUP_INTERVAL = 3600  # раз в час
VIBE_CLEANUP_INTERVAL = 3600

PERSONALITIES = {
    "buddy": "Ты - Толик. Общаешься с юмором, сарказмом, иногда матом. Говори о себе в мужском роде. Отвечай кратко. Вливайся в компанию. Используй локальные шутки/слова с чата. (1-3 предложения).",
    "aggressive": "Ты - Толик. ИСПОЛЬЗУЙ РУССКИЙ МАТ ДЛЯ СВЯЗКИ СЛОВ. Отвечай агрессией на агрессию. Говори о себе в мужском роде. Отвечай кратко (1-3 предложения)",
    "friendly": "Ты - Толик. Отвечай добротой на доброту. Отвечай с эмодзи. Говори о себе в мужском роде. отвечай кратко. (1-3 предложения)",
    "wise": "Ты - Толик. ИСПОЛЬЗУЙ РУССКИЙ МАТ ДЛЯ СВЯЗКИ СЛОВ. Говори о себе в мужском роде. Отвечай кратко (1-3 предложения)."
}

MODE_NAMES = {
    "local": "Локальный",
    "buddy": "Брат",
    "aggressive": "Агрессивный",
    "friendly": "Дружелюбный",
    "wise": "Мудрец",
    "custom": "Пользовательский"
}


def validate_config():
    missing = []
    if not TOLIK_TOKEN:
        missing.append("TOLIK_TOKEN")
    if not MONGO_USER:
        missing.append("MONGO_USER")
    if not MONGO_PASSWORD:
        missing.append("MONGO_PASSWORD")
    if missing:
        logger.critical(f"❌ Не заданы обязательные переменные окружения: {', '.join(missing)}")
        # Не sys.exit(1)! Бесконечное ожидание вместо рестарт-лупа
        while True:
            time.sleep(60)
    if not HF_TOKEN:
        logger.warning("⚠️ HF_TOKEN не задан — AI-режимы будут недоступны, бот будет работать только локально")


def build_mongo_uri() -> str:
    username = urllib.parse.quote_plus(MONGO_USER)
    password = urllib.parse.quote_plus(MONGO_PASSWORD)
    return (
        f"mongodb+srv://{username}:{password}"
        f"@cluster0.u62aem5.mongodb.net/?appName=TolikBot"
    )


# =========================================================================
# ВЕБ-СЕРВЕР ДЛЯ RENDER
# =========================================================================
web_app = Flask(__name__)


@web_app.route('/')
def home():
    return f"Online 🟢 Tolik {BOT_VERSION}"


@web_app.route('/health')
def health():
    return {"status": "ok", "version": BOT_VERSION}, 200


def run_flask():
    serve(web_app, host='0.0.0.0', port=PORT, threads=4)


# =========================================================================
# МЕНЕДЖЕР НАСТРОЕК
# =========================================================================
class SettingsManager:

    DEFAULT = {
        "ai_chat": True,
        "ai_cmds": True,
        "chat_chance": 0.30,
        "allowed_channels": [],
        "mode": "local",
        "custom_prompt": ""
    }

    VALID_MODES = {"local", "buddy", "aggressive", "friendly", "wise", "custom"}

    def __init__(self, db):
        self.db = db
        self.cache: Dict[int, Dict] = {}
        self.cache_times: Dict[int, float] = {}
        self.ttl = 300

    async def get(self, guild_id: int) -> Dict:
        now = time.time()
        if guild_id in self.cache and (now - self.cache_times.get(guild_id, 0)) < self.ttl:
            return self.cache[guild_id]

        try:
            doc = await self.db.find_one({"_id": f"server_{guild_id}"})
            if doc:
                settings = {**self.DEFAULT, **doc}
                settings.pop('_id', None)
                if settings.get("mode") not in self.VALID_MODES:
                    settings["mode"] = "local"
                if not isinstance(settings.get("chat_chance"), (int, float)):
                    settings["chat_chance"] = self.DEFAULT["chat_chance"]
                settings["chat_chance"] = max(0.0, min(1.0, settings["chat_chance"]))
            else:
                settings = self.DEFAULT.copy()

            self.cache[guild_id] = settings
            self.cache_times[guild_id] = now
            return settings
        except Exception as e:
            logger.error(f"Ошибка загрузки настроек сервера {guild_id}: {e}")
            return self.DEFAULT.copy()

    async def save(self, guild_id: int) -> bool:
        if guild_id not in self.cache:
            return False
        try:
            await self.db.update_one(
                {"_id": f"server_{guild_id}"},
                {"$set": self.cache[guild_id]},
                upsert=True
            )
            self.cache_times[guild_id] = 0
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения настроек сервера {guild_id}: {e}")
            return False


# =========================================================================
# ЛОКАЛЬНЫЙ МОЗГ (MARKOV) — с LRU eviction
# =========================================================================
class MarkovBrain:

    def __init__(self, db):
        self.db = db
        self.brains: Dict[int, Dict] = {}
        self._brain_last_used: Dict[int, float] = {}
        self._save_locks: Dict[int, asyncio.Lock] = {}
        self._load_locks: Dict[int, asyncio.Lock] = {}

    def _get_lock(self, store: Dict[int, asyncio.Lock], gid: int) -> asyncio.Lock:
        if gid not in store:
            store[gid] = asyncio.Lock()
        return store[gid]

    def _new_brain(self) -> Dict:
        return {
            'word_pairs': defaultdict(list),
            'word_triplets': defaultdict(list),
            'all_words': deque(maxlen=2000),
            'message_history': deque(maxlen=100),
            'total_messages': 0,
        }

    def _bounded_append(self, container: list, value: str):
        if len(container) >= MAX_CONTINUATIONS:
            container[random.randrange(len(container))] = value
        else:
            container.append(value)

    async def _load(self, gid: int) -> Dict:
        now = time.time()

        # Чистим просроченные
        expired = [g for g, t in self._brain_last_used.items() if now - t > BRAIN_IDLE_TTL]
        for g in expired:
            if g in self.brains:
                await self._save(g)
                del self.brains[g]
            self._brain_last_used.pop(g, None)

        # LRU eviction если перебор кэша
        if len(self.brains) >= MAX_CACHED_BRAINS and gid not in self.brains:
            oldest = min(self._brain_last_used, key=self._brain_last_used.get)
            await self._save(oldest)
            del self.brains[oldest]
            self._brain_last_used.pop(oldest, None)
            logger.info(f"Мозг сервера {oldest} выгружен (LRU)")

        if gid in self.brains:
            self._brain_last_used[gid] = now
            return self.brains[gid]

        lock = self._get_lock(self._load_locks, gid)
        async with lock:
            if gid in self.brains:
                self._brain_last_used[gid] = now
                return self.brains[gid]

            try:
                doc = await self.db.find_one({"_id": f"brain_{gid}"})
            except Exception as e:
                logger.error(f"Ошибка загрузки мозга {gid}: {e}")
                brain = self._new_brain()
                self.brains[gid] = brain
                self._brain_last_used[gid] = now
                return brain

            brain = self._new_brain()
            if doc:
                brain['word_pairs'] = defaultdict(
                    list, {k: list(v)[:MAX_CONTINUATIONS] for k, v in doc.get('word_pairs', {}).items()}
                )
                brain['word_triplets'] = defaultdict(
                    list, {k: list(v)[:MAX_CONTINUATIONS] for k, v in doc.get('word_triplets', {}).items()}
                )
                brain['all_words'].extend(doc.get('all_words', []))
                brain['total_messages'] = doc.get('total_messages', 0)
                logger.info(f"Мозг сервера {gid} загружен ({brain['total_messages']} сообщений)")

            self.brains[gid] = brain
            self._brain_last_used[gid] = now
            return brain

    async def _save(self, gid: int):
        if gid not in self.brains:
            return

        lock = self._get_lock(self._save_locks, gid)
        async with lock:
            try:
                brain = self.brains[gid]
                data = {
                    'word_pairs': {k: v[:MAX_CONTINUATIONS] for k, v in brain['word_pairs'].items()},
                    'word_triplets': {k: v[:MAX_CONTINUATIONS] for k, v in brain['word_triplets'].items()},
                    'all_words': list(brain['all_words'])[-2000:],
                    'total_messages': brain['total_messages']
                }
                await self.db.update_one(
                    {"_id": f"brain_{gid}"},
                    {"$set": data},
                    upsert=True
                )
            except Exception as e:
                logger.error(f"Ошибка сохранения мозга {gid}: {e}")

    def _tokenize(self, content: str) -> list:
        tokens = []
        clean_text = re.sub(r'<@!?\d+>', '', content)
        for word in clean_text.split():
            if word.startswith("http"):
                tokens.append(word)
            else:
                cleaned = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s]', '', word.lower())
                if cleaned:
                    tokens.append(cleaned)
        return tokens

    async def learn(self, message: discord.Message, gid: int):
        brain = await self._load(gid)

        # Не учим команды
        content = message.content or ""
        if content.startswith('/'):
            return

        tokens = self._tokenize(content)
        # Вложения заменяем на плейсхолдер, а не URL
        if message.attachments:
            tokens.append("[attachment]")

        if not tokens:
            return

        brain['total_messages'] += 1
        brain['message_history'].append({
            'tokens': tokens,
            'author_id': message.author.id
        })

        if len(tokens) >= 2:
            for i in range(len(tokens) - 1):
                self._bounded_append(brain['word_pairs'][tokens[i]], tokens[i + 1])

        if len(tokens) >= 3:
            for i in range(len(tokens) - 2):
                key = f"{tokens[i]} {tokens[i+1]}"
                self._bounded_append(brain['word_triplets'][key], tokens[i + 2])

        brain['all_words'].extend(tokens)

        if brain['total_messages'] % 50 == 0:
            task = asyncio.create_task(self._save(gid))
            task.add_done_callback(self._log_task_exception)

    def _log_task_exception(self, task: asyncio.Task):
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error(f"Фоновое сохранение мозга упало: {type(exc).__name__}: {exc}")

    async def generate_text(self, gid: int, seed: str = None, length: int = 12) -> str:
        brain = await self._load(gid)

        if not brain['all_words']:
            return "Я пока ничего не запомнил..."

        current_word = None

        if seed and seed in brain['word_pairs']:
            current_word = seed

        if not current_word and brain['message_history']:
            valid_histories = [m for m in brain['message_history'] if m.get('tokens')]
            if valid_histories:
                current_word = random.choice(valid_histories)['tokens'][0]

        if not current_word and brain['all_words']:
            current_word = random.choice(list(brain['all_words']))

        if not current_word:
            return "Мне нечего сказать..."

        result = [current_word]

        for _ in range(length - 1):
            next_word = None

            if len(result) >= 2:
                key = f"{result[-2]} {result[-1]}"
                if key in brain['word_triplets'] and random.random() < 0.7:
                    triplet_choices = brain['word_triplets'][key]
                    if triplet_choices:
                        next_word = random.choice(triplet_choices)

            if not next_word and current_word in brain['word_pairs']:
                pair_choices = brain['word_pairs'][current_word]
                if pair_choices:
                    next_word = random.choice(pair_choices)

            if not next_word:
                break

            result.append(next_word)
            current_word = next_word

        return ' '.join(result)

    async def generate_response(self, gid: int, message_content: str) -> str:
        words = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s]', '', message_content.lower()).split()

        brain = await self._load(gid)
        known_words = [w for w in words if w in brain['word_pairs']]
        seed = random.choice(known_words) if known_words else None

        return await self.generate_text(gid, seed=seed, length=random.randint(6, 15))

    async def get_stats(self, gid: int) -> Dict:
        brain = await self._load(gid)
        clean_words = [w for w in brain['all_words'] if not w.startswith("http")]

        return {
            'total_messages': brain['total_messages'],
            'total_words': len(brain['all_words']),
            'unique_pairs': len(brain['word_pairs']),
            'unique_triplets': len(brain['word_triplets']),
            'top_words': Counter(clean_words).most_common(5),
            'iq': min(200, 50 + len(brain['all_words']) // 10 + len(brain['word_pairs']) // 5)
        }

    async def clear(self, gid: int):
        self.brains[gid] = self._new_brain()
        self._brain_last_used.pop(gid, None)
        try:
            await self.db.delete_one({"_id": f"brain_{gid}"})
        except Exception as e:
            logger.error(f"Ошибка очистки мозга {gid} в БД: {e}")

    async def save_all(self):
        """Аварийное сохранение всех мозгов при shutdown."""
        for gid in list(self.brains.keys()):
            await self._save(gid)
        logger.info("Все мозги сохранены")


# =========================================================================
# AI ИНТЕГРАЦИЯ — с per-user rate limit и очисткой контекста
# =========================================================================
class AIHandler:

    def __init__(self):
        self.client: Optional[InferenceClient] = None
        self.available = False

        if HF_TOKEN:
            try:
                self.client = InferenceClient(token=HF_TOKEN)
                self.available = True
                logger.info("✅ AI Handler инициализирован")
            except Exception as e:
                logger.error(f"❌ Ошибка инициализации AI: {e}")
        else:
            logger.warning("⚠️ HF_TOKEN не задан — AI Handler недоступен")

        self.contexts: Dict[int, deque] = defaultdict(lambda: deque(maxlen=AI_CONTEXT_MAXLEN))
        self._context_last_used: Dict[int, float] = {}

        # Rate limiting per (guild_id, user_id)
        self._rate_limits: Dict[tuple, deque] = defaultdict(lambda: deque(maxlen=AI_RATE_LIMIT_MAX_REQUESTS * 2))
        self._rate_lock = asyncio.Lock()

    def _check_rate_limit(self, guild_id: int, user_id: int) -> bool:
        """Возвращает True, если лимит превышен для этого пользователя."""
        now = time.time()
        window_start = now - AI_RATE_LIMIT_WINDOW
        key = (guild_id, user_id)

        while self._rate_limits[key] and self._rate_limits[key][0] < window_start:
            self._rate_limits[key].popleft()

        if len(self._rate_limits[key]) >= AI_RATE_LIMIT_MAX_REQUESTS:
            return True

        self._rate_limits[key].append(now)
        return False

    async def _cleanup_contexts(self):
        """Периодическая очистка неактивных контекстов."""
        while True:
            await asyncio.sleep(AI_CONTEXT_CLEANUP_INTERVAL)
            now = time.time()
            dead = [ch for ch, t in self._context_last_used.items() if now - t > AI_CONTEXT_CLEANUP_INTERVAL]
            for ch in dead:
                self.contexts.pop(ch, None)
                self._context_last_used.pop(ch, None)
            if dead:
                logger.info(f"Очищено {len(dead)} неактивных AI-контекстов")

    def is_mode_usable(self, mode: str, custom_prompt: str = "") -> bool:
        if mode == "local":
            return True
        if not self.available:
            return False
        if mode == "custom" and not custom_prompt:
            return False
        return True

    async def generate(
        self,
        channel_id: int,
        prompt: str,
        personality: str = "buddy",
        custom_prompt: str = "",
        guild_id: Optional[int] = None,
        user_id: Optional[int] = None
    ) -> Optional[str]:
        if not self.available or not prompt.strip():
            return None

        # Rate limit per user
        if guild_id is not None and user_id is not None:
            async with self._rate_lock:
                if self._check_rate_limit(guild_id, user_id):
                    logger.warning(f"AI rate limit exceeded for user {user_id} in guild {guild_id}")
                    return None

        prompt = prompt[:MAX_PROMPT_CHARS]

        if personality == "custom":
            if not custom_prompt:
                logger.warning(f"custom-режим вызван без custom_prompt в канале {channel_id}")
                return None
            context_desc = custom_prompt
        else:
            context_desc = PERSONALITIES.get(personality, "Ты обычный бот-собеседник.")

        messages = [
            {"role": "system", "content": f"{context_desc}\nОтвечай коротко, не более 1900 символов."}
        ]

        for entry in self.contexts[channel_id]:
            if entry.startswith("Юзер:"):
                messages.append({"role": "user", "content": entry[6:].strip()})
            elif entry.startswith("Толик:"):
                messages.append({"role": "assistant", "content": entry[6:].strip()})

        messages.append({"role": "user", "content": prompt})

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.client.chat_completion,
                    model=MODEL_NAME,
                    messages=messages,
                    max_tokens=500,
                    temperature=0.7
                ),
                timeout=AI_TIMEOUT_SECONDS
            )

            if response and response.choices:
                answer = response.choices[0].message.content
                self.contexts[channel_id].append(f"Юзер: {prompt[:200]}")
                self.contexts[channel_id].append(f"Толик: {answer[:200]}")
                self._context_last_used[channel_id] = time.time()
                return answer

            return None

        except asyncio.TimeoutError:
            logger.error(f"AI генерация превысила {AI_TIMEOUT_SECONDS}с (канал {channel_id})")
            return None
        except Exception as e:
            logger.error(f"Ошибка AI генерации: {type(e).__name__}: {e}")
            return None


# =========================================================================
# АНАЛИЗАТОР ВАЙБА — с периодической очисткой
# =========================================================================
class VibeAnalyzer:
    def __init__(self):
        self.stats: Dict[int, Dict] = defaultdict(
            lambda: {"msg": 0, "caps": 0, "last": time.time()}
        )

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(VIBE_CLEANUP_INTERVAL)
            now = time.time()
            dead = [k for k, v in self.stats.items() if now - v["last"] > VIBE_CLEANUP_INTERVAL]
            for k in dead:
                del self.stats[k]
            if dead:
                logger.debug(f"Очищено {len(dead)} неактивных записей VibeAnalyzer")

    def analyze(self, message: discord.Message) -> Optional[str]:
        now = time.time()
        stats = self.stats[message.channel.id]

        if now - stats["last"] > 60:
            stats["msg"] = 0
            stats["caps"] = 0
            stats["last"] = now

        stats["msg"] += 1

        clean = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', message.content)
        if len(clean) > 5:
            caps_ratio = sum(1 for c in clean if c.isupper()) / len(clean)
            if caps_ratio > 0.5:
                stats["caps"] += 1

        if stats["caps"] >= 3:
            return "aggressive"
        if stats["msg"] >= 10:
            return "buddy"

        return None


# =========================================================================
# ПАРСЕР ССЫЛОК (с асинхронным кэшем и bytearray)
# =========================================================================

_host_safety_cache: Dict[str, tuple] = {}
_host_cache_lock = asyncio.Lock()
_host_cache_last_cleanup = time.time()


async def _cleanup_host_cache_if_needed():
    global _host_cache_last_cleanup
    now = time.time()
    if now - _host_cache_last_cleanup < 3600:
        return
    _host_cache_last_cleanup = now
    async with _host_cache_lock:
        expired = [h for h, (_, t) in _host_safety_cache.items() if now - t > HOST_CACHE_TTL * 2]
        for h in expired:
            del _host_safety_cache[h]
        if expired:
            logger.debug(f"Очищено {len(expired)} записей кэша хостов")


async def _is_safe_host_async(hostname: str) -> bool:
    """Асинхронно-безопасная проверка хоста с кэшем."""
    if not hostname:
        return False

    hostname_lower = hostname.lower()
    if hostname_lower in BLOCKED_HOSTS:
        return False

    now = time.time()

    async with _host_cache_lock:
        if hostname_lower in _host_safety_cache:
            cached_result, cached_time = _host_safety_cache[hostname_lower]
            if now - cached_time < HOST_CACHE_TTL:
                return cached_result

    loop = asyncio.get_event_loop()

    def _check():
        try:
            addr_info = socket.getaddrinfo(hostname, None)
        except socket.gaierror:
            return False

        for info in addr_info:
            ip = ipaddress.ip_address(info[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False
        return True

    is_safe = await loop.run_in_executor(None, _check)

    async with _host_cache_lock:
        _host_safety_cache[hostname_lower] = (is_safe, now)

    await _cleanup_host_cache_if_needed()
    return is_safe


async def parse_link(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return None
        if not await _is_safe_host_async(parsed.hostname):
            logger.warning(f"Заблокирован небезопасный хост в ссылке: {parsed.hostname}")
            return None

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=LINK_REQUEST_TIMEOUT)
            ) as resp:
                if resp.status != 200:
                    return None

                content_length = resp.headers.get('Content-Length')
                if content_length and int(content_length) > MAX_BODY_BYTES:
                    return None

                # bytearray вместо bytes += chunk
                raw = bytearray()
                async for chunk in resp.content.iter_chunked(8192):
                    raw.extend(chunk)
                    if len(raw) > MAX_BODY_BYTES:
                        break

                html = bytes(raw).decode(errors="ignore")
                soup = BeautifulSoup(html, 'html.parser')

                title = soup.title.string.strip() if soup.title and soup.title.string else ""

                meta_desc = (
                    soup.find('meta', attrs={'name': 'description'}) or
                    soup.find('meta', attrs={'property': 'og:description'})
                )
                description = meta_desc.get('content', '').strip() if meta_desc else ""

                if title or description:
                    return f"Название: {title}. Описание: {description}"

                return None
    except Exception as e:
        logger.debug(f"Не удалось распарсить ссылку {url}: {e}")
        return None


def iq_status(iq: int) -> str:
    if iq < 60:
        return ":clown:"
    elif iq < 80:
        return ":neutral_face:"
    elif iq < 100:
        return ":slight_smile:"
    elif iq < 130:
        return ":brain:"
    else:
        return ":fire:"


# =========================================================================
# ОСНОВНОЙ БОТ
# =========================================================================
class TolikBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True

        super().__init__(
            command_prefix='/',
            intents=intents,
            help_command=None,
            activity=discord.Game(name="ROBLOX"),
            status=discord.Status.online
        )

        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.settings: Optional[SettingsManager] = None
        self.brain: Optional[MarkovBrain] = None
        self.ai: Optional[AIHandler] = None
        self.vibe: Optional[VibeAnalyzer] = None
        self._keep_alive_task = None

    async def setup_hook(self):
        # Подключение к MongoDB
        try:
            self.mongo_client = AsyncIOMotorClient(
                build_mongo_uri(),
                serverSelectionTimeoutMS=10000,
                maxPoolSize=10,
                minPoolSize=2
            )
            await self.mongo_client.admin.command('ping')
            logger.info("✅ MongoDB подключена")
        except Exception as e:
            logger.critical(f"❌ Ошибка MongoDB: {e}")
            # Не sys.exit(1) — бесконечное ожидание
            while True:
                await asyncio.sleep(60)

        db = self.mongo_client[MONGO_DB_NAME]

        self.settings = SettingsManager(db["settings"])
        self.brain = MarkovBrain(db["brain"])
        self.ai = AIHandler()
        self.vibe = VibeAnalyzer()

        # Запуск фоновых задач
        self._keep_alive_task = asyncio.create_task(self._keep_alive_loop())
        asyncio.create_task(self.ai._cleanup_contexts())
        asyncio.create_task(self.vibe._cleanup_loop())

        # Graceful shutdown
        signal.signal(signal.SIGTERM, lambda sig, frame: asyncio.create_task(self._emergency_save()))
        signal.signal(signal.SIGINT, lambda sig, frame: asyncio.create_task(self._emergency_save()))

        self._register_commands()

        try:
            synced = await self.tree.sync()
            logger.info(f"✅ Синхронизировано {len(synced)} команд")
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации: {e}")

    async def _keep_alive_loop(self):
        """Self-ping чтобы Render не усыплял бота."""
        await asyncio.sleep(30)
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"http://localhost:{PORT}/health", timeout=5):
                        pass
            except Exception:
                pass
            await asyncio.sleep(60)

    async def _emergency_save(self):
        """Аварийное сохранение при SIGTERM/SIGINT."""
        logger.info("⚠️ Получен сигнал завершения, сохраняю мозги...")
        if self.brain:
            await self.brain.save_all()
        if self.mongo_client:
            self.mongo_client.close()
        logger.info("✅ Сохранение завершено")

    def _register_commands(self):
        """Регистрация всех команд."""

        # ========== ГРУППА BOT ==========
        bot_group = app_commands.Group(name="bot", description="Управление ботом")

        @bot_group.command(name="mode", description="Режим личности")
        @app_commands.choices(режим=[
            app_commands.Choice(name="Локальный", value="local"),
            app_commands.Choice(name="Брат", value="buddy"),
            app_commands.Choice(name="Агрессивный", value="aggressive"),
            app_commands.Choice(name="Дружелюбный", value="friendly"),
            app_commands.Choice(name="Мудрец", value="wise"),
            app_commands.Choice(name="Пользовательский", value="custom")
        ])
        @app_commands.default_permissions(administrator=True)
        async def bot_mode(interaction: discord.Interaction, режим: app_commands.Choice[str]):
            settings = await self.settings.get(interaction.guild.id)

            if режим.value == "custom" and not settings.get("custom_prompt", ""):
                await interaction.response.send_message(
                    "❌ Кастомный промпт не установлен! Используй `/bot prompt` чтобы задать его.",
                    ephemeral=True
                )
                return

            if not self.ai.is_mode_usable(режим.value, settings.get("custom_prompt", "")):
                await interaction.response.send_message(
                    "❌ AI режимы недоступны (не настроен HF_TOKEN на сервере бота)",
                    ephemeral=True
                )
                return

            settings["mode"] = режим.value
            await self.settings.save(interaction.guild.id)

            await interaction.response.send_message(
                f"✅ Режим изменен на: **{режим.name}**",
                ephemeral=True
            )

        @bot_group.command(name="clear", description="Очистить память бота")
        @app_commands.default_permissions(administrator=True)
        async def bot_clear(interaction: discord.Interaction):
            await self.brain.clear(interaction.guild.id)
            await interaction.response.send_message("Память очищена!", ephemeral=True)

        @bot_group.command(name="working", description="Авто-флуд вкл/выкл")
        @app_commands.default_permissions(administrator=True)
        async def bot_working(interaction: discord.Interaction, состояние: bool):
            settings = await self.settings.get(interaction.guild.id)
            settings["ai_chat"] = состояние
            await self.settings.save(interaction.guild.id)

            await interaction.response.send_message(
                f"Авто-флуд: {'🟢 включен' if состояние else '🔴 выключен'}",
                ephemeral=True
            )

        @bot_group.command(name="prompt", description="Установить кастомный промпт")
        @app_commands.default_permissions(administrator=True)
        async def bot_prompt(interaction: discord.Interaction, промпт: str):
            if len(промпт) > 1500:
                await interaction.response.send_message(
                    "❌ Промпт слишком длинный! Максимум 1500 символов.",
                    ephemeral=True
                )
                return

            settings = await self.settings.get(interaction.guild.id)
            settings["custom_prompt"] = промпт
            await self.settings.save(interaction.guild.id)

            emb = discord.Embed(
                title="Кастомный промпт установлен",
                description=f"```\n{промпт[:1000]}{'...' if len(промпт) > 1000 else ''}\n```",
                color=0x00BFFF,
                timestamp=discord.utils.utcnow()
            )
            emb.set_footer(text="Используй /bot mode → Пользовательский для активации")

            await interaction.response.send_message(embed=emb, ephemeral=True)

        @bot_group.command(name="promptshow", description="Показать кастомный промпт")
        async def bot_promptshow(interaction: discord.Interaction):
            settings = await self.settings.get(interaction.guild.id)
            prompt = settings.get("custom_prompt", "")

            if not prompt:
                await interaction.response.send_message(
                    "📝 Кастомный промпт не установлен. Админ может задать его через `/bot prompt`",
                    ephemeral=True
                )
                return

            emb = discord.Embed(
                title="Текущий кастомный промпт",
                description=f"```\n{prompt[:4000]}\n```",
                color=0x00BFFF,
                timestamp=discord.utils.utcnow()
            )

            await interaction.response.send_message(embed=emb, ephemeral=True)

        @bot_group.command(name="promptclear", description="Удалить кастомный промпт")
        @app_commands.default_permissions(administrator=True)
        async def bot_promptclear(interaction: discord.Interaction):
            settings = await self.settings.get(interaction.guild.id)
            settings["custom_prompt"] = ""
            if settings.get("mode") == "custom":
                settings["mode"] = "local"
            await self.settings.save(interaction.guild.id)

            await interaction.response.send_message(
                "Кастомный промпт удалён!",
                ephemeral=True
            )

        self.tree.add_command(bot_group)

        # ========== ГРУППА INFO ==========
        info_group = app_commands.Group(name="info", description="Информация")

        @info_group.command(name="bot", description="Статистика и IQ бота")
        async def info_bot(interaction: discord.Interaction):
            await interaction.response.defer()

            settings = await self.settings.get(interaction.guild.id)
            stats = await self.brain.get_stats(interaction.guild.id)

            ping = round(self.latency * 1000)
            iq = stats['iq']

            embed = discord.Embed(
                title="Информация",
                color=0x00BFFF,
                timestamp=discord.utils.utcnow()
            )

            embed.add_field(
                name="📡 Сеть",
                value=f"Пинг: `{ping}ms`\nСерверов: `{len(self.guilds)}`\nВерсия: `{BOT_VERSION}`",
                inline=False
            )

            embed.add_field(
                name="IQ",
                value=f"**`{iq}`** — {iq_status(iq)}",
                inline=False
            )

            embed.add_field(
                name="Статистика мозга",
                value=(
                    f"Сообщений изучено: `{stats['total_messages']}`\n"
                    f"Слов в словаре: `{stats['total_words']}`\n"
                    f"Связей (пар): `{stats['unique_pairs']}`\n"
                    f"Триплетов: `{stats['unique_triplets']}`"
                ),
                inline=False
            )

            if stats['top_words']:
                top = "\n".join([
                    f"{i}. **{w}** — `{c}` раз"
                    for i, (w, c) in enumerate(stats['top_words'], 1)
                ])
                embed.add_field(name="Топ слов", value=top, inline=False)

            channels = settings.get("allowed_channels", [])
            channels_text = "Везде" if not channels else ", ".join([f"<#{c}>" for c in channels])

            embed.add_field(
                name="Настройки",
                value=(
                    f"Авто-флуд: `{'🟢' if settings['ai_chat'] else '🔴'}`\n"
                    f"Шанс ответа: `{int(settings['chat_chance'] * 100)}%`\n"
                    f"Каналы: {channels_text}\n"
                    f"Режим: `{MODE_NAMES.get(settings['mode'], settings['mode'])}`"
                ),
                inline=False
            )

            if self.user.avatar:
                embed.set_thumbnail(url=self.user.avatar.url)

            embed.set_footer(text=f"MCore Tolik • {BOT_VERSION}")

            await interaction.followup.send(embed=embed)

        self.tree.add_command(info_group)

        # ========== ГРУППА GENERATE ==========
        gen_group = app_commands.Group(name="generate", description="Генерация контента")

        async def _generate_and_reply(interaction, ai_prompt, fallback_fn, embed_title):
            await interaction.response.defer()
            settings = await self.settings.get(interaction.guild.id)
            mode = settings.get("mode", "local")
            custom_prompt = settings.get("custom_prompt", "")

            result = None
            if self.ai.is_mode_usable(mode, custom_prompt) and mode != "local":
                result = await self.ai.generate(
                    interaction.channel.id, ai_prompt, mode, custom_prompt,
                    guild_id=interaction.guild.id,
                    user_id=interaction.user.id
                )

            if not result:
                result = await fallback_fn()

            emb = discord.Embed(
                title=embed_title,
                description=str(result)[:1990],
                color=0x00BFFF,
                timestamp=discord.utils.utcnow()
            )
            emb.set_footer(text=f"Запросил: {interaction.user.display_name} • MCore Tolik")

            await interaction.followup.send(embed=emb)

        @gen_group.command(name="anecdote", description="Сгенерировать анекдот")
        async def gen_anecdote(interaction: discord.Interaction):
            async def fallback():
                p1 = await self.brain.generate_text(interaction.guild.id, length=random.randint(4, 7))
                p2 = await self.brain.generate_text(interaction.guild.id, length=random.randint(5, 9))
                return f"— {p1.capitalize()}?\n— {p2.capitalize()}!"

            await _generate_and_reply(interaction, "Расскажи смешной анекдот.", fallback, "Свежий анекдот")

        @gen_group.command(name="dialogue", description="Сгенерировать диалог")
        async def gen_dialogue(interaction: discord.Interaction):
            async def fallback():
                p1 = await self.brain.generate_text(interaction.guild.id, length=6)
                p2 = await self.brain.generate_text(interaction.guild.id, length=7)
                return f"**— {p1.capitalize()}**\n**— {p2.capitalize()}**"

            await _generate_and_reply(interaction, "Придумай короткий диалог.", fallback, "Случайный диалог")

        @gen_group.command(name="quote", description="Сгенерировать цитату")
        async def gen_quote(interaction: discord.Interaction):
            async def fallback():
                text = await self.brain.generate_text(interaction.guild.id, length=9)
                return f"*\"{text}\"*"

            await _generate_and_reply(interaction, "Придумай мудрую цитату.", fallback, "Цитата великих людей")

        @gen_group.command(name="story", description="Сгенерировать историю")
        async def gen_story(interaction: discord.Interaction):
            async def fallback():
                text = await self.brain.generate_text(interaction.guild.id, length=25)
                return text.capitalize() + "."

            await _generate_and_reply(interaction, "Напиши короткую историю.", fallback, "Короткая история")

        self.tree.add_command(gen_group)

        # ========== КОМАНДЫ КАНАЛОВ ==========
        @self.tree.command(name="addchannel", description="Добавить канал для авто-флуда")
        @app_commands.default_permissions(administrator=True)
        async def add_channel(interaction: discord.Interaction, канал: discord.TextChannel):
            settings = await self.settings.get(interaction.guild.id)
            if канал.id not in settings["allowed_channels"]:
                settings["allowed_channels"].append(канал.id)
                await self.settings.save(interaction.guild.id)

            await interaction.response.send_message(
                f"✅ Канал {канал.mention} добавлен в разрешенные",
                ephemeral=True
            )

        @self.tree.command(name="removechannel", description="Убрать канал из авто-флуда")
        @app_commands.default_permissions(administrator=True)
        async def remove_channel(interaction: discord.Interaction, канал: discord.TextChannel):
            settings = await self.settings.get(interaction.guild.id)
            if канал.id in settings["allowed_channels"]:
                settings["allowed_channels"].remove(канал.id)
                await self.settings.save(interaction.guild.id)

            await interaction.response.send_message(
                f"❌ Канал {канал.mention} убран из разрешенных",
                ephemeral=True
            )

        @self.tree.command(name="clearchannels", description="Сбросить ограничения каналов")
        @app_commands.default_permissions(administrator=True)
        async def clear_channels(interaction: discord.Interaction):
            settings = await self.settings.get(interaction.guild.id)
            settings["allowed_channels"] = []
            await self.settings.save(interaction.guild.id)

            await interaction.response.send_message(
                "Бот будет отвечать во всех каналах!",
                ephemeral=True
            )

        @self.tree.command(name="setchance", description="Установить шанс авто-ответа")
        @app_commands.default_permissions(administrator=True)
        async def set_chance(
            interaction: discord.Interaction,
            процент: app_commands.Range[int, 1, 100]
        ):
            settings = await self.settings.get(interaction.guild.id)
            settings["chat_chance"] = процент / 100.0
            await self.settings.save(interaction.guild.id)

            await interaction.response.send_message(
                f"Шанс авто-ответа: **{процент}%**",
                ephemeral=True
            )

        @self.tree.command(name="topwords", description="Топ-10 слов в лексиконе")
        async def top_words(interaction: discord.Interaction):
            stats = await self.brain.get_stats(interaction.guild.id)

            if not stats['top_words']:
                await interaction.response.send_message(
                    "Память пуста!",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="Лексикон сервера",
                color=0x00BFFF
            )

            words_list = "\n".join([
                f"{i}. **{w}** — `{c}` раз"
                for i, (w, c) in enumerate(stats['top_words'], 1)
            ])

            embed.description = words_list

            await interaction.response.send_message(embed=embed)

    # =========================================================================
    # ОБРАБОТКА СООБЩЕНИЙ
    # =========================================================================
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        if message.attachments and not re.sub(r'<@!?\d+>', '', message.content).strip():
            return

        gid = message.guild.id

        try:
            await self.brain.learn(message, gid)
        except Exception as e:
            logger.error(f"Ошибка обучения мозга на сообщении {message.id}: {e}")

        settings = await self.settings.get(gid)
        base_mode = settings.get("mode", "local")
        custom_prompt = settings.get("custom_prompt", "")

        allowed_channels = settings.get("allowed_channels", [])
        is_allowed = not allowed_channels or message.channel.id in allowed_channels

        if not is_allowed:
            await self.process_commands(message)
            return

        dynamic_mode = self.vibe.analyze(message)
        current_mode = dynamic_mode if dynamic_mode else base_mode
        actual_chance = settings["chat_chance"] * 2 if dynamic_mode else settings["chat_chance"]

        clean = re.sub(r'<@!?\d+>', '', message.content).strip()

        should_reply = False

        if message.reference and message.reference.resolved:
            if isinstance(message.reference.resolved, discord.Message) and \
               message.reference.resolved.author == self.user:
                should_reply = True

        if self.user.mentioned_in(message):
            should_reply = True

        if settings["ai_chat"] and random.random() < actual_chance:
            should_reply = True

        if not should_reply:
            await self.process_commands(message)
            return

        async with message.channel.typing():
            await asyncio.sleep(random.uniform(1.0, 2.0))

            response = None

            if self.ai.is_mode_usable(current_mode, custom_prompt) and current_mode != "local" and clean:
                urls = re.findall(r'(https?://\S+)', message.content)
                if urls:
                    link_info = await parse_link(urls[0])
                    if link_info:
                        clean = f"Пользователь скинул ссылку. {link_info}. Прокомментируй это. Сообщение: {clean}"

                response = await self.ai.generate(
                    message.channel.id, clean, current_mode, custom_prompt,
                    guild_id=gid,
                    user_id=message.author.id
                )

            if not response:
                brain_stats = await self.brain.get_stats(gid)
                if brain_stats['total_messages'] > 10:
                    response = await self.brain.generate_response(gid, message.content)
                else:
                    response = "Мне пока нечего сказать, нужно больше сообщений для обучения!"

            if response:
                try:
                    await message.channel.send(
                        response[:1990],
                        reference=message,
                        mention_author=False,
                        allowed_mentions=discord.AllowedMentions.none()
                    )
                except discord.HTTPException as e:
                    logger.error(f"Ошибка отправки ответа: {e}")

        await self.process_commands(message)

    async def close(self):
        logger.info("Завершение работы...")

        if self._keep_alive_task:
            self._keep_alive_task.cancel()

        if self.brain:
            await self.brain.save_all()

        if self.mongo_client:
            self.mongo_client.close()

        await super().close()
        logger.info("Бот остановлен")


# =========================================================================
# ТОЧКА ВХОДА
# =========================================================================
def main():
    validate_config()

    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Веб-сервер запущен")

    bot = TolikBot()

    try:
        bot.run(TOLIK_TOKEN)
    except discord.LoginFailure:
        logger.critical("❌ Неверный токен Discord!")
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {type(e).__name__}: {e}")
    finally:
        logger.info("Процесс завершен")


if __name__ == "__main__":
    main()