# ====================================================================
# MCore - Version 2.1.0 (Render Production Ready)
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
from contextlib import asynccontextmanager
from threading import Thread
from collections import defaultdict, deque, Counter
from typing import Optional, Dict
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from flask import Flask
from waitress import serve

import discord
from discord.ext import commands
from discord import app_commands

# =========================================================================
# КОНФИГУРАЦИЯ
# =========================================================================
BOT_VERSION = "2.1.0"
MODEL_NAME = os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

AI_RATE_LIMIT_WINDOW = 60  # секунд
AI_RATE_LIMIT_MAX_REQUESTS = 5  # запросов за окно на пользователя

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MCore")

MCORE_TOKEN = os.environ.get("MCORE_TOKEN")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "smmunity_database")
PORT = int(os.environ.get("PORT", 10000))

MAX_CONTINUATIONS = 200
MAX_PROMPT_CHARS = 2000
MAX_CUSTOM_PROMPT = 1500
CHANCE_STEP = 10  # шаг кнопок +/- шанса ответа (%)
SETTINGS_PANEL_TIMEOUT = 600  # секунд
AI_TIMEOUT_SECONDS = 20
MAX_BODY_BYTES = 1_000_000
LINK_REQUEST_TIMEOUT = 5
BLOCKED_HOSTS = {"localhost", "metadata.google.internal", "169.254.169.254"}

HOST_CACHE_TTL = 300
MAX_CACHED_BRAINS = 50  # LRU eviction
BRAIN_IDLE_TTL = 600    # 10 минут без использования - выгружаем
AI_CONTEXT_MAXLEN = 12  # 6 диалогов
CLEANUP_INTERVAL = 3600  # раз в час

PERSONALITIES = {
    "buddy": "Ты - MCore. Общаешься с юмором, сарказмом, иногда матом. Говори о себе в мужском роде. Отвечай кратко. Вливайся в компанию. Используй локальные шутки/слова с чата. (1-3 предложения).",
    "aggressive": "Ты - MCore. ИСПОЛЬЗУЙ РУССКИЙ МАТ ДЛЯ СВЯЗКИ СЛОВ. Отвечай агрессией на агрессию. Говори о себе в мужском роде. Отвечай кратко (1-3 предложения)",
    "friendly": "Ты - MCore. Отвечай добротой на доброту. Говори о себе в мужском роде. Отвечай кратко. (1-3 предложения)",
    "wise": "Ты - MCore. ИСПОЛЬЗУЙ РУССКИЙ МАТ ДЛЯ СВЯЗКИ СЛОВ. Говори о себе в мужском роде. Отвечай кратко (1-3 предложения)."
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
    if not MCORE_TOKEN:
        missing.append("MCORE_TOKEN")
    if not MONGO_USER:
        missing.append("MONGO_USER")
    if not MONGO_PASSWORD:
        missing.append("MONGO_PASSWORD")
    if missing:
        logger.critical(f"Не заданы обязательные переменные окружения: {', '.join(missing)}")
        # Не sys.exit(1): бесконечное ожидание вместо рестарт-лупа
        while True:
            time.sleep(60)
    if not GROQ_API_KEY:
        logger.warning("GROQ_API_KEY не задан - AI-режимы будут недоступны, бот будет работать только локально")


def build_mongo_uri() -> str:
    username = urllib.parse.quote_plus(MONGO_USER)
    password = urllib.parse.quote_plus(MONGO_PASSWORD)
    return (
        f"mongodb+srv://{username}:{password}"
        f"@cluster0.u62aem5.mongodb.net/?appName=MCore"
    )


# =========================================================================
# ВЕБ-СЕРВЕР ДЛЯ RENDER
# =========================================================================
web_app = Flask(__name__)


@web_app.route('/')
def home():
    return f"Online MCore {BOT_VERSION}"


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
# ЛОКАЛЬНЫЙ МОЗГ (MARKOV) - с LRU eviction
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

        for i in range(len(tokens) - 1):
            self._bounded_append(brain['word_pairs'][tokens[i]], tokens[i + 1])

        for i in range(len(tokens) - 2):
            key = f"{tokens[i]} {tokens[i + 1]}"
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
        """Сохранение всех мозгов при завершении работы."""
        for gid in list(self.brains.keys()):
            await self._save(gid)
        logger.info("Все мозги сохранены")


# =========================================================================
# AI ИНТЕГРАЦИЯ - с per-user rate limit и очисткой контекста
# =========================================================================
class AIHandler:

    def __init__(self):
        self.available = bool(GROQ_API_KEY)

        if self.available:
            logger.info(f"AI Handler инициализирован (Groq, модель {MODEL_NAME})")
        else:
            logger.warning("GROQ_API_KEY не задан - AI Handler недоступен")

        # Контекст канала: пары (роль, текст)
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
            await asyncio.sleep(CLEANUP_INTERVAL)
            now = time.time()
            dead = [ch for ch, t in self._context_last_used.items() if now - t > CLEANUP_INTERVAL]
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

        for role, text in self.contexts[channel_id]:
            messages.append({"role": role, "content": text})

        messages.append({"role": "user", "content": prompt})

        try:
            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": MODEL_NAME,
                "messages": messages,
                "max_tokens": 1000,
                "temperature": 0.7
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    GROQ_URL,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=AI_TIMEOUT_SECONDS)
                ) as resp:
                    if resp.status == 429:
                        logger.warning("Groq: превышен лимит запросов (429)")
                        return None
                    if resp.status != 200:
                        body = (await resp.text())[:300]
                        logger.error(f"Groq вернул {resp.status}: {body}")
                        return None
                    data = await resp.json()

            choices = data.get("choices") or []
            answer = (choices[0].get("message", {}).get("content") or "").strip() if choices else ""

            if answer:
                self.contexts[channel_id].append(("user", prompt[:200]))
                self.contexts[channel_id].append(("assistant", answer[:200]))
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
# АНАЛИЗАТОР ВАЙБА - с периодической очисткой
# =========================================================================
class VibeAnalyzer:
    def __init__(self):
        self.stats: Dict[int, Dict] = defaultdict(
            lambda: {"msg": 0, "caps": 0, "last": time.time()}
        )

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL)
            now = time.time()
            dead = [k for k, v in self.stats.items() if now - v["last"] > CLEANUP_INTERVAL]
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

    loop = asyncio.get_running_loop()

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
        return "очень низкий"
    elif iq < 80:
        return "низкий"
    elif iq < 100:
        return "средний"
    elif iq < 130:
        return "высокий"
    else:
        return "гений"


# =========================================================================
# СТАТУС "ПЕЧАТАЕТ..." (безопасная обертка)
# =========================================================================
@asynccontextmanager
async def safe_typing(channel):
    """Показывает 'Печатает...' в канале. Если нет прав/канала - молча работает без него."""
    cm = None
    if channel is not None and hasattr(channel, "typing"):
        try:
            cm = channel.typing()
            await cm.__aenter__()
        except Exception as e:
            logger.debug(f"Не удалось включить typing: {e}")
            cm = None
    try:
        yield
    finally:
        if cm is not None:
            try:
                await cm.__aexit__(None, None, None)
            except Exception:
                pass


# =========================================================================
# ПАНЕЛЬ НАСТРОЕК (/settings)
# =========================================================================
class AdminView(discord.ui.View):
    """Базовый View: взаимодействовать могут только администраторы."""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        perms = getattr(interaction.user, "guild_permissions", None)
        if perms is not None and perms.administrator:
            return True
        await interaction.response.send_message(
            "Эта панель доступна только администраторам.", ephemeral=True
        )
        return False


class PromptModal(discord.ui.Modal):
    def __init__(self, parent: "SettingsView", current: str):
        super().__init__(title="Кастомный промпт")
        self.parent = parent
        self.prompt_input = discord.ui.TextInput(
            label="Промпт личности",
            style=discord.TextStyle.paragraph,
            placeholder="Пусто = удалить промпт",
            required=False,
            max_length=MAX_CUSTOM_PROMPT,
            default=current[:MAX_CUSTOM_PROMPT] or None
        )
        self.add_item(self.prompt_input)

    async def on_submit(self, interaction: discord.Interaction):
        gid = self.parent.guild.id
        new_prompt = str(self.prompt_input.value or "").strip()

        settings = await self.parent.bot.settings.get(gid)
        settings["custom_prompt"] = new_prompt
        notice = "Кастомный промпт сохранен. Выбери режим «Пользовательский», чтобы его использовать."
        if not new_prompt:
            notice = "Кастомный промпт удален."
            if settings.get("mode") == "custom":
                settings["mode"] = "local"
                notice += " Режим переключен на «Локальный»."

        await self.parent._save_and_refresh(interaction, settings, notice)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Ошибка в PromptModal: {type(error).__name__}: {error}")
        if not interaction.response.is_done():
            await interaction.response.send_message("Что-то пошло не так.", ephemeral=True)


class ChannelsView(AdminView):
    """Select со списком каналов. Выбранные = разрешенные; пусто = везде."""

    def __init__(self, parent: "SettingsView", current_ids: list):
        super().__init__(timeout=120)
        self.parent = parent

        defaults = []
        for cid in current_ids:
            ch = parent.guild.get_channel(cid)
            if isinstance(ch, discord.TextChannel):
                defaults.append(ch)
        defaults = defaults[:25]

        self.channel_select = discord.ui.ChannelSelect(
            placeholder="Каналы для авто-флуда (пусто = везде)",
            channel_types=[discord.ChannelType.text],
            min_values=0,
            max_values=25,
            default_values=defaults
        )
        self.channel_select.callback = self._on_select
        self.add_item(self.channel_select)

    async def _on_select(self, interaction: discord.Interaction):
        gid = self.parent.guild.id
        ids = [c.id for c in self.channel_select.values]

        settings = await self.parent.bot.settings.get(gid)
        settings["allowed_channels"] = ids
        ok = await self.parent.bot.settings.save(gid)

        if not ok:
            text = "Не удалось сохранить настройки, попробуй ещё раз."
        elif ids:
            text = "Авто-флуд разрешен в: " + ", ".join(f"<#{c}>" for c in ids)
        else:
            text = "Ограничений нет - бот отвечает во всех каналах."

        await interaction.response.edit_message(content=text, view=None)
        self.stop()
        await self.parent.update_origin(settings)


class SettingsView(AdminView):
    def __init__(self, bot: "MCoreBot", guild: discord.Guild, settings: Dict):
        super().__init__(timeout=SETTINGS_PANEL_TIMEOUT)
        self.bot = bot
        self.guild = guild
        self.origin: Optional[discord.Interaction] = None
        self._clear_armed = False
        self._apply_state(settings)

    # ---------- состояние и отрисовка ----------
    def _apply_state(self, settings: Dict):
        mode = settings.get("mode", "local")
        # Новые SelectOption на каждый вызов - объекты не общие между View
        self.mode_select.options = [
            discord.SelectOption(label=name, value=key, default=(key == mode))
            for key, name in MODE_NAMES.items()
        ]

        ai_chat = settings.get("ai_chat", True)
        self.toggle_button.label = f"Авто-флуд: {'ВКЛ' if ai_chat else 'ВЫКЛ'}"
        self.toggle_button.style = discord.ButtonStyle.success if ai_chat else discord.ButtonStyle.danger

        pct = round(settings.get("chat_chance", 0.3) * 100)
        self.chance_down.disabled = pct <= 0
        self.chance_up.disabled = pct >= 100

        self.clear_button.label = "Точно очистить?" if self._clear_armed else "Очистить память"
        self.clear_button.style = discord.ButtonStyle.danger

    def build_embed(self, settings: Dict) -> discord.Embed:
        mode = settings.get("mode", "local")
        pct = round(settings.get("chat_chance", 0.3) * 100)
        channels = settings.get("allowed_channels", [])
        channels_text = "Везде" if not channels else ", ".join(f"<#{c}>" for c in channels)
        prompt = settings.get("custom_prompt", "")

        embed = discord.Embed(
            title="Настройки MCore",
            color=0x00BFFF,
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(name="Режим", value=MODE_NAMES.get(mode, mode), inline=True)
        embed.add_field(
            name="Авто-флуд",
            value="Включен" if settings.get("ai_chat", True) else "Выключен",
            inline=True
        )
        embed.add_field(name="Шанс ответа", value=f"{pct}%", inline=True)
        embed.add_field(name="Каналы", value=channels_text[:1024], inline=False)

        if prompt:
            preview = prompt[:300].replace("```", "'''")
            embed.add_field(
                name="Промпт",
                value=f"```\n{preview}{'...' if len(prompt) > 300 else ''}\n```",
                inline=False
            )
        else:
            embed.add_field(name="Промпт", value="Не задан", inline=False)

        embed.set_footer(text=f"MCore | {BOT_VERSION}")
        return embed

    async def _refresh(self, interaction: discord.Interaction, settings: Dict,
                       notice: Optional[str] = None, armed: bool = False):
        self._clear_armed = armed
        self._apply_state(settings)
        await interaction.response.edit_message(
            content=notice, embed=self.build_embed(settings), view=self
        )

    async def _save_and_refresh(self, interaction: discord.Interaction, settings: Dict,
                                notice: Optional[str] = None):
        ok = await self.bot.settings.save(self.guild.id)
        await self._refresh(
            interaction, settings,
            notice if ok else "Не удалось сохранить настройки, попробуй ещё раз."
        )

    async def update_origin(self, settings: Dict):
        """Обновляет основную панель из другого сообщения (например, из выбора каналов)."""
        if self.origin is None:
            return
        try:
            self._clear_armed = False
            self._apply_state(settings)
            await self.origin.edit_original_response(
                content=None, embed=self.build_embed(settings), view=self
            )
        except discord.HTTPException as e:
            logger.debug(f"Не удалось обновить панель настроек: {e}")

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.origin is not None:
            try:
                await self.origin.edit_original_response(view=self)
            except discord.HTTPException:
                pass

    # ---------- компоненты ----------
    @discord.ui.select(
        placeholder="Режим личности",
        options=[discord.SelectOption(label=n, value=k) for k, n in MODE_NAMES.items()],
        row=0
    )
    async def mode_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        mode = select.values[0]
        settings = await self.bot.settings.get(self.guild.id)
        prompt = settings.get("custom_prompt", "")

        if mode == "custom" and not prompt:
            await self._refresh(interaction, settings, "Сначала задай кастомный промпт кнопкой «Промпт».")
            return

        if not self.bot.ai.is_mode_usable(mode, prompt):
            await self._refresh(interaction, settings, "AI-режимы недоступны (не настроен GROQ_API_KEY).")
            return

        settings["mode"] = mode
        await self._save_and_refresh(interaction, settings)

    @discord.ui.button(label="Авто-флуд", style=discord.ButtonStyle.secondary, row=1)
    async def toggle_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = await self.bot.settings.get(self.guild.id)
        settings["ai_chat"] = not settings.get("ai_chat", True)
        await self._save_and_refresh(interaction, settings)

    async def _change_chance(self, interaction: discord.Interaction, delta: int):
        settings = await self.bot.settings.get(self.guild.id)
        pct = round(settings.get("chat_chance", 0.3) * 100)
        pct = max(0, min(100, pct + delta))
        settings["chat_chance"] = pct / 100.0
        await self._save_and_refresh(interaction, settings)

    @discord.ui.button(label=f"-{CHANCE_STEP}%", style=discord.ButtonStyle.secondary, row=1)
    async def chance_down(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._change_chance(interaction, -CHANCE_STEP)

    @discord.ui.button(label=f"+{CHANCE_STEP}%", style=discord.ButtonStyle.secondary, row=1)
    async def chance_up(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._change_chance(interaction, CHANCE_STEP)

    @discord.ui.button(label="Каналы", style=discord.ButtonStyle.primary, row=2)
    async def channels_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = await self.bot.settings.get(self.guild.id)
        view = ChannelsView(self, settings.get("allowed_channels", []))
        await interaction.response.send_message(
            "Выбери каналы, где разрешен авто-флуд. Снятые галочки = канал убран. "
            "Пустой выбор = бот отвечает везде.",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Промпт", style=discord.ButtonStyle.primary, row=2)
    async def prompt_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = await self.bot.settings.get(self.guild.id)
        await interaction.response.send_modal(PromptModal(self, settings.get("custom_prompt", "")))

    @discord.ui.button(label="Очистить память", style=discord.ButtonStyle.danger, row=2)
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = await self.bot.settings.get(self.guild.id)

        if not self._clear_armed:
            await self._refresh(
                interaction, settings,
                "Нажми «Точно очистить?» ещё раз, чтобы стереть память бота на этом сервере.",
                armed=True
            )
            return

        await self.bot.brain.clear(self.guild.id)
        await self._refresh(interaction, settings, "Память очищена.")


# =========================================================================
# ОСНОВНОЙ БОТ
# =========================================================================
class MCoreBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True

        super().__init__(
            command_prefix='/',
            intents=intents,
            help_command=None,
            status=discord.Status.online,
            # Реальное число серверов известно только после on_ready - там статус обновится
            activity=discord.Activity(type=discord.ActivityType.watching, name="серверов...")
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
            logger.info("MongoDB подключена")
        except Exception as e:
            logger.critical(f"Ошибка MongoDB: {e}")
            # Не sys.exit(1): бесконечное ожидание
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

        # Graceful shutdown: при SIGTERM/SIGINT вызываем close(), он сохраняет мозги
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.close()))
            except NotImplementedError:
                pass

        self._register_commands()

        try:
            synced = await self.tree.sync()
            logger.info(f"Синхронизировано {len(synced)} команд")
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")

    # =========================================================================
    # СТАТУС (Activity): "Смотрит: X серверов"
    # =========================================================================
    @staticmethod
    def _make_activity(count: int) -> discord.Activity:
        return discord.Activity(type=discord.ActivityType.watching, name=f"{count} серверов")

    async def _update_presence(self):
        try:
            await self.change_presence(
                status=discord.Status.online,
                activity=self._make_activity(len(self.guilds))
            )
        except Exception as e:
            logger.warning(f"Не удалось обновить статус: {type(e).__name__}: {e}")

    async def on_ready(self):
        logger.info(f"Бот запущен как {self.user} (серверов: {len(self.guilds)})")
        await self._update_presence()

    async def on_guild_join(self, guild: discord.Guild):
        logger.info(f"Новый сервер: {guild.name} ({guild.id})")
        await self._update_presence()

    async def on_guild_remove(self, guild: discord.Guild):
        logger.info(f"Удален с сервера: {guild.name} ({guild.id})")
        await self._update_presence()

    async def _keep_alive_loop(self):
        """Self-ping чтобы Render не усыплял бота."""
        await asyncio.sleep(30)
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"http://localhost:{PORT}/health", timeout=aiohttp.ClientTimeout(total=5)):
                        pass
            except Exception:
                pass
            await asyncio.sleep(60)

    def _register_commands(self):
        """Регистрация всех команд."""

        # ========== ГРУППА BOT (остались только clear и promptshow) ==========
        bot_group = app_commands.Group(name="bot", description="Управление ботом")

        @bot_group.command(name="clear", description="Очистить память бота")
        @app_commands.default_permissions(administrator=True)
        async def bot_clear(interaction: discord.Interaction):
            await self.brain.clear(interaction.guild.id)
            await interaction.response.send_message("Память очищена!", ephemeral=True)

        @bot_group.command(name="promptshow", description="Показать кастомный промпт")
        async def bot_promptshow(interaction: discord.Interaction):
            settings = await self.settings.get(interaction.guild.id)
            prompt = settings.get("custom_prompt", "")

            if not prompt:
                await interaction.response.send_message(
                    "Кастомный промпт не установлен. Админ может задать его через `/settings`",
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

        self.tree.add_command(bot_group)

        # ========== ПАНЕЛЬ НАСТРОЕК ==========
        @self.tree.command(name="settings", description="Панель настроек бота (только админы)")
        @app_commands.default_permissions(administrator=True)
        @app_commands.guild_only()
        async def settings_panel(interaction: discord.Interaction):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message(
                    "Эта команда доступна только администраторам.", ephemeral=True
                )
                return

            settings = await self.settings.get(interaction.guild.id)
            view = SettingsView(self, interaction.guild, settings)
            await interaction.response.send_message(
                embed=view.build_embed(settings), view=view, ephemeral=True
            )
            view.origin = interaction

        # ========== INFO ==========
        @self.tree.command(name="info", description="Статистика, IQ и настройки бота")
        @app_commands.guild_only()
        async def info_cmd(interaction: discord.Interaction):
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
                name="Сеть",
                value=f"Пинг: `{ping}ms`\nСерверов: `{len(self.guilds)}`\nВерсия: `{BOT_VERSION}`",
                inline=False
            )

            embed.add_field(
                name="IQ",
                value=f"**`{iq}`** - {iq_status(iq)}",
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
                    f"{i}. **{w}** - `{c}` раз"
                    for i, (w, c) in enumerate(stats['top_words'], 1)
                ])
                embed.add_field(name="Топ слов", value=top, inline=False)

            channels = settings.get("allowed_channels", [])
            channels_text = "Везде" if not channels else ", ".join([f"<#{c}>" for c in channels])

            embed.add_field(
                name="Настройки",
                value=(
                    f"Авто-флуд: `{'вкл' if settings['ai_chat'] else 'выкл'}`\n"
                    f"Шанс ответа: `{int(round(settings['chat_chance'] * 100))}%`\n"
                    f"Каналы: {channels_text}\n"
                    f"Режим: `{MODE_NAMES.get(settings['mode'], settings['mode'])}`"
                ),
                inline=False
            )

            if self.user.avatar:
                embed.set_thumbnail(url=self.user.avatar.url)

            embed.set_footer(text=f"MCore | {BOT_VERSION}")

            await interaction.followup.send(embed=embed)

        # ========== ГРУППА GENERATE ==========
        gen_group = app_commands.Group(name="generate", description="Генерация контента")

        async def _generate_and_reply(interaction, ai_prompt, fallback_fn, embed_title):
            await interaction.response.defer()
            settings = await self.settings.get(interaction.guild.id)
            mode = settings.get("mode", "local")
            custom_prompt = settings.get("custom_prompt", "")

            result = None
            async with safe_typing(interaction.channel):
                if self.ai.is_mode_usable(mode, custom_prompt) and mode != "local":
                    result = await self.ai.generate(
                        interaction.channel_id, ai_prompt, mode, custom_prompt,
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
            emb.set_footer(text=f"Запросил: {interaction.user.display_name} | MCore")

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

        @self.tree.command(name="topwords", description="Топ слов в лексиконе")
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

            embed.description = "\n".join([
                f"{i}. **{w}** - `{c}` раз"
                for i, (w, c) in enumerate(stats['top_words'], 1)
            ])

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

        async with safe_typing(message.channel):
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

    bot = MCoreBot()

    try:
        bot.run(MCORE_TOKEN)
    except discord.LoginFailure:
        logger.critical("Неверный токен Discord!")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {type(e).__name__}: {e}")
    finally:
        logger.info("Процесс завершен")


if __name__ == "__main__":
    main()
