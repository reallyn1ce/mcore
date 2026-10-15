import os
import re
import sys
import random
import asyncio
import logging
import signal
import time
from threading import Thread
from collections import defaultdict, deque, Counter
from urllib.parse import urlparse

import aiohttp
import discord
from discord.ext import commands
from discord import app_commands
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from huggingface_hub import InferenceClient
from flask import Flask, jsonify
from waitress import serve

# ==============================================================================
# КОНФИГ И НАСТРОЙКИ
# ==============================================================================
BOT_VERSION = "2.1.0 (Human Refactored)"
# V4-Flash не существует, поправил на актуальную быструю модель
MODEL_NAME = "deepseek-ai/DeepSeek-V3" 

# Render на бесплатном тарифе убивает инстанс через 15 мин бездействия. 
# Этот костыль с self-ping'ом держит его в тонусе.
PORT = int(os.environ.get("PORT", 10000))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Tolik")

TOLIK_TOKEN = os.environ.get("TOLIK_TOKEN")
HF_TOKEN = os.environ.get("HF_TOKEN")
MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "smmunity_database")

MAX_CONTINUATIONS = 200
AI_TIMEOUT_SECONDS = 20
BLOCKED_HOSTS = {"localhost", "metadata.google.internal", "169.254.169.254"}

PERSONALITIES = {
    "buddy": "Ты - Толик. Общаешься с юмором, сарказмом, иногда матом. Говори о себе в мужском роде. Отвечай кратко. Вливайся в компанию. (1-3 предложения).",
    "aggressive": "Ты - Толик. ИСПОЛЬЗУЙ РУССКИЙ МАТ ДЛЯ СВЯЗКИ СЛОВ. Отвечай агрессией на агрессию. Кратко и по делу.",
    "friendly": "Ты - Толик. Отвечай добротой на доброту, добавляй эмодзи. Говори о себе в мужском роде. Кратко.",
    "wise": "Ты - Толик. Мудрый, но с перчинкой. Можешь использовать мат для связки слов. Отвечай кратко и философски."
}

MODE_NAMES = {
    "local": "Локальный",
    "buddy": "Брат",
    "aggressive": "Агрессивный",
    "friendly": "Дружелюбный",
    "wise": "Мудрец",
    "custom": "Пользовательский"
}

# ==============================================================================
# ВЕБ-СЕРВЕР (ДЛЯ RENDER HEALTH CHECK)
# ==============================================================================
web_app = Flask(__name__)

@web_app.route('/')
def home():
    return f"Online 🟢 Tolik v{BOT_VERSION}"

@web_app.route('/health')
def health():
    return jsonify(status="ok", version=BOT_VERSION), 200

def run_flask():
    # waitress тихий и надежный для таких фоновых задач
    serve(web_app, host='0.0.0.0', port=PORT, threads=2)

# ==============================================================================
# МЕНЕДЖЕР НАСТРОЕК
# ==============================================================================
class SettingsManager:
    DEFAULT = {
        "ai_chat": True,
        "chat_chance": 0.30,
        "allowed_channels": [],
        "mode": "local",
        "custom_prompt": ""
    }
    VALID_MODES = set(MODE_NAMES.keys())

    def __init__(self, db):
        self.db = db
        self.cache = {}
        self.cache_times = {}
        self.ttl = 300

    async def get(self, guild_id: int) -> dict:
        now = time.time()
        if guild_id in self.cache and (now - self.cache_times.get(guild_id, 0)) < self.ttl:
            return self.cache[guild_id]

        try:
            doc = await self.db.find_one({"_id": f"server_{guild_id}"})
            settings = {**self.DEFAULT, **(doc or {})}
            settings.pop('_id', None)
            
            # Валидация на всякий случай, если в БД попал мусор
            if settings["mode"] not in self.VALID_MODES:
                settings["mode"] = "local"
            settings["chat_chance"] = max(0.0, min(1.0, float(settings["chat_chance"])))

            self.cache[guild_id] = settings
            self.cache_times[guild_id] = now
            return settings
        except Exception as e:
            logger.error(f"Ошибка загрузки настроек {guild_id}: {e}")
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
            self.cache_times[guild_id] = 0 # Сброс TTL
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения настроек {guild_id}: {e}")
            return False

# ==============================================================================
# ЛОКАЛЬНЫЙ МОЗГ (MARKOV)
# ==============================================================================
class MarkovBrain:
    def __init__(self, db):
        self.db = db
        self.brains = {}
        self._last_used = {}
        self._locks = {}
        self.MAX_CACHED = 50
        self.IDLE_TTL = 600

    def _get_lock(self, gid: int) -> asyncio.Lock:
        # setdefault атомарен и избавляет от лишних проверок
        return self._locks.setdefault(gid, asyncio.Lock())

    def _new_brain(self) -> dict:
        # Используем deque с maxlen. Это избавляет от ручной обрезки списков!
        return {
            'word_pairs': defaultdict(lambda: deque(maxlen=MAX_CONTINUATIONS)),
            'word_triplets': defaultdict(lambda: deque(maxlen=MAX_CONTINUATIONS)),
            'all_words': deque(maxlen=2000),
            'message_history': deque(maxlen=100),
            'total_messages': 0,
        }

    async def _load(self, gid: int) -> dict:
        now = time.time()

        # Чистим просроченные (LRU eviction)
        expired = [g for g, t in self._last_used.items() if now - t > self.IDLE_TTL]
        for g in expired:
            await self._save(g)
            self.brains.pop(g, None)
            self._last_used.pop(g, None)

        if len(self.brains) >= self.MAX_CACHED and gid not in self.brains:
            oldest = min(self._last_used, key=self._last_used.get)
            await self._save(oldest)
            self.brains.pop(oldest, None)
            self._last_used.pop(oldest, None)
            logger.info(f"Мозг сервера {oldest} выгружен из памяти (LRU)")

        if gid in self.brains:
            self._last_used[gid] = now
            return self.brains[gid]

        async with self._get_lock(gid):
            if gid in self.brains: # double-check locking
                self._last_used[gid] = now
                return self.brains[gid]

            try:
                doc = await self.db.find_one({"_id": f"brain_{gid}"})
            except Exception as e:
                logger.error(f"Ошибка загрузки мозга {gid}: {e}")
                doc = None

            brain = self._new_brain()
            if doc:
                # deque сам обрежет лишнее при расширении
                for k, v in doc.get('word_pairs', {}).items():
                    brain['word_pairs'][k].extend(v)
                for k, v in doc.get('word_triplets', {}).items():
                    brain['word_triplets'][k].extend(v)
                brain['all_words'].extend(doc.get('all_words', []))
                brain['total_messages'] = doc.get('total_messages', 0)
                logger.info(f"Мозг сервера {gid} загружен ({brain['total_messages']} соо.)")

            self.brains[gid] = brain
            self._last_used[gid] = now
            return brain

    async def _save(self, gid: int):
        if gid not in self.brains:
            return

        async with self._get_lock(gid):
            try:
                brain = self.brains[gid]
                data = {
                    'word_pairs': {k: list(v) for k, v in brain['word_pairs'].items()},
                    'word_triplets': {k: list(v) for k, v in brain['word_triplets'].items()},
                    'all_words': list(brain['all_words']),
                    'total_messages': brain['total_messages']
                }
                await self.db.update_one({"_id": f"brain_{gid}"}, {"$set": data}, upsert=True)
            except Exception as e:
                logger.error(f"Ошибка сохранения мозга {gid}: {e}")

    def _tokenize(self, content: str) -> list:
        clean_text = re.sub(r'<@!?\d+>', '', content)
        tokens = []
        for word in clean_text.split():
            if word.startswith("http"):
                tokens.append("[ссылка]")
            else:
                cleaned = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9]', '', word.lower())
                if cleaned:
                    tokens.append(cleaned)
        return tokens

    async def learn(self, message: discord.Message, gid: int):
        if message.content.startswith('/'):
            return

        brain = await self._load(gid)
        tokens = self._tokenize(message.content)
        if message.attachments:
            tokens.append("[вложение]")

        if not tokens:
            return

        brain['total_messages'] += 1
        brain['message_history'].append({'tokens': tokens, 'author_id': message.author.id})

        # Заполняем пары и триплеты. deque(maxlen) сам вытеснит старое!
        for i in range(len(tokens) - 1):
            brain['word_pairs'][tokens[i]].append(tokens[i + 1])

        for i in range(len(tokens) - 2):
            key = f"{tokens[i]} {tokens[i+1]}"
            brain['word_triplets'][key].append(tokens[i + 2])

        brain['all_words'].extend(tokens)

        if brain['total_messages'] % 50 == 0:
            asyncio.create_task(self._save(gid))

    async def generate_response(self, gid: int, message_content: str) -> str:
        brain = await self._load(gid)
        if not brain['all_words']:
            return "Я пока ничего не запомнил... Напиши что-нибудь."

        words = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s]', '', message_content.lower()).split()
        known_words = [w for w in words if w in brain['word_pairs']]
        current_word = random.choice(known_words) if known_words else random.choice(list(brain['all_words']))

        result = [current_word]
        for _ in range(random.randint(6, 15)):
            next_word = None
            
            # Приоритет триплетам для более связной речи
            if len(result) >= 2:
                key = f"{result[-2]} {result[-1]}"
                if key in brain['word_triplets'] and random.random() < 0.7:
                    next_word = random.choice(list(brain['word_triplets'][key]))

            if not next_word and current_word in brain['word_pairs']:
                next_word = random.choice(list(brain['word_pairs'][current_word]))

            if not next_word:
                break

            result.append(next_word)
            current_word = next_word

        return ' '.join(result).capitalize() + "."

    async def get_stats(self, gid: int) -> dict:
        brain = await self._load(gid)
        clean_words = [w for w in brain['all_words'] if not w.startswith("[")]
        top_words = Counter(clean_words).most_common(5)
        
        iq = min(200, 50 + len(brain['all_words']) // 10 + len(brain['word_pairs']) // 5)
        return {
            'total_messages': brain['total_messages'],
            'total_words': len(brain['all_words']),
            'unique_pairs': len(brain['word_pairs']),
            'top_words': top_words,
            'iq': iq
        }

    async def clear(self, gid: int):
        self.brains.pop(gid, None)
        self._last_used.pop(gid, None)
        await self.db.delete_one({"_id": f"brain_{gid}"})

    async def save_all(self):
        for gid in list(self.brains.keys()):
            await self._save(gid)
        logger.info("Все мозги сохранены перед выходом.")

# ==============================================================================
# AI ИНТЕГРАЦИЯ
# ==============================================================================
class AIHandler:
    def __init__(self):
        self.client = None
        self.available = False
        self.contexts = defaultdict(lambda: deque(maxlen=12))
        self._rate_limits = defaultdict(lambda: deque(maxlen=10))
        self._rate_lock = asyncio.Lock()

        if HF_TOKEN:
            try:
                self.client = InferenceClient(token=HF_TOKEN)
                self.available = True
                logger.info("✅ AI Handler инициализирован")
            except Exception as e:
                logger.error(f"❌ Ошибка инициализации AI: {e}")

    def _check_rate_limit(self, guild_id: int, user_id: int) -> bool:
        now = time.time()
        key = (guild_id, user_id)
        
        # Очистка старых запросов (окно 60 сек)
        while self._rate_limits[key] and self._rate_limits[key][0] < now - 60:
            self._rate_limits[key].popleft()

        if len(self._rate_limits[key]) >= 5: # 5 запросов в минуту на юзера
            return True

        self._rate_limits[key].append(now)
        return False

    async def generate(self, channel_id: int, prompt: str, personality: str, custom_prompt: str, guild_id: int, user_id: int) -> str | None:
        if not self.available or not prompt.strip():
            return None

        async with self._rate_lock:
            if self._check_rate_limit(guild_id, user_id):
                logger.warning(f"AI rate limit: пользователь {user_id}")
                return None

        context_desc = custom_prompt if personality == "custom" else PERSONALITIES.get(personality, PERSONALITIES["buddy"])
        
        messages = [{"role": "system", "content": f"{context_desc}\nОтвечай коротко, не более 1900 символов."}]
        
        for entry in self.contexts[channel_id]:
            role, content = entry.split(":", 1)
            messages.append({"role": role.strip().lower(), "content": content.strip()})

        messages.append({"role": "user", "content": prompt[:2000]})

        try:
            # asyncio.to_thread блокирует меньше, чем run_in_executor для huggingface_hub
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.client.chat_completion,
                    model=MODEL_NAME,
                    messages=messages,
                    max_tokens=400,
                    temperature=0.7
                ),
                timeout=AI_TIMEOUT_SECONDS
            )
            
            if response and response.choices:
                answer = response.choices[0].message.content
                self.contexts[channel_id].append(f"User: {prompt[:100]}")
                self.contexts[channel_id].append(f"Assistant: {answer[:100]}")
                return answer
        except asyncio.TimeoutError:
            logger.warning(f"AI таймаут для канала {channel_id}")
        except Exception as e:
            logger.error(f"Ошибка AI: {e}")
            
        return None

# ==============================================================================
# ПАРСЕР ССЫЛОК (Упрощенный и надежный)
# ==============================================================================
_link_cache = {}

async def parse_link(url: str) -> str | None:
    if url in _link_cache:
        return _link_cache[url]

    try:
        parsed = urlparse(url)
        if parsed.hostname and parsed.hostname.lower() in BLOCKED_HOSTS:
            return None

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return None
                
                # Читаем только начало, чтобы не жрать память на огромных страницах
                html = await resp.text(15000) 
                soup = BeautifulSoup(html, 'html.parser')
                
                title = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', attrs={'property': 'og:description'})
                description = meta_desc.get('content', '').strip() if meta_desc else ""

                if title or description:
                    result = f"Ссылка: {title}. {description}"[:200]
                    _link_cache[url] = result
                    return result
    except Exception:
        pass # Игнорируем битые ссылки, это не критично
        
    return None

# ==============================================================================
# ОСНОВНОЙ БОТ
# ==============================================================================
class TolikBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True

        super().__init__(
            command_prefix='/',
            intents=intents,
            help_command=None,
            activity=discord.Game(name="в прятки с разработчиком"),
            status=discord.Status.online
        )

        self.mongo_client = None
        self.settings = None
        self.brain = None
        self.ai = None

    async def setup_hook(self):
        try:
            uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@cluster0.u62aem5.mongodb.net/?appName=TolikBot"
            self.mongo_client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=10000)
            await self.mongo_client.admin.command('ping')
            logger.info("✅ MongoDB подключена")
        except Exception as e:
            logger.critical(f"❌ Ошибка MongoDB: {e}. Бот будет работать в режиме 'только локальный мозг'.")
            # Костыль для Render: не даем процессу умереть сразу, чтобы он не рестартовал бесконечно
            while True:
                await asyncio.sleep(60)

        db = self.mongo_client[os.environ.get("MONGO_DB_NAME", "smmunity_database")]
        self.settings = SettingsManager(db["settings"])
        self.brain = MarkovBrain(db["brain"])
        self.ai = AIHandler()

        try:
            synced = await self.tree.sync()
            logger.info(f"✅ Синхронизировано {len(synced)} команд")
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации команд: {e}")

    async def _keep_alive_loop(self):
        """Self-ping чтобы Render не усыплял бота."""
        await asyncio.sleep(30)
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    await s.get(f"http://localhost:{PORT}/health", timeout=3)
            except Exception:
                pass
            await asyncio.sleep(60)

    def _register_commands(self):
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
            if режим.value == "custom" and not settings.get("custom_prompt"):
                await interaction.response.send_message("❌ Сначала задай промпт через `/bot prompt`", ephemeral=True)
                return

            settings["mode"] = режим.value
            await self.settings.save(interaction.guild.id)
            await interaction.response.send_message(f"✅ Режим изменен на: **{режим.name}**", ephemeral=True)

        @bot_group.command(name="clear", description="Очистить память бота")
        @app_commands.default_permissions(administrator=True)
        async def bot_clear(interaction: discord.Interaction):
            await self.brain.clear(interaction.guild.id)
            await interaction.response.send_message("🧹 Память очищена! Я забыл всё плохое (и хорошее).", ephemeral=True)

        @bot_group.command(name="prompt", description="Установить кастомный промпт")
        @app_commands.default_permissions(administrator=True)
        async def bot_prompt(interaction: discord.Interaction, промпт: str):
            if len(промпт) > 1500:
                await interaction.response.send_message("❌ Промпт слишком длинный! Максимум 1500 символов.", ephemeral=True)
                return
            
            settings = await self.settings.get(interaction.guild.id)
            settings["custom_prompt"] = промпт
            await self.settings.save(interaction.guild.id)
            await interaction.response.send_message("✅ Кастомный промпт сохранен!", ephemeral=True)

        self.tree.add_command(bot_group)

        info_group = app_commands.Group(name="info", description="Информация")

        @info_group.command(name="bot", description="Статистика и IQ бота")
        async def info_bot(interaction: discord.Interaction):
            await interaction.response.defer()
            settings = await self.settings.get(interaction.guild.id)
            stats = await self.brain.get_stats(interaction.guild.id)
            ping = round(self.latency * 1000)
            
            iq_emoji = ":clown:" if stats['iq'] < 60 else ":brain:" if stats['iq'] < 130 else ":fire:"

            embed = discord.Embed(title="Мозг Толика", color=0x00BFFF)
            embed.add_field(name="📡 Сеть", value=f"Пинг: `{ping}ms`\nВерсия: `{BOT_VERSION}`", inline=False)
            embed.add_field(name="🧠 IQ", value=f"**`{stats['iq']}`** {iq_emoji}", inline=False)
            embed.add_field(
                name="Статистика",
                value=f"Сообщений: `{stats['total_messages']}`\nСлов: `{stats['total_words']}`\nСвязей: `{stats['unique_pairs']}`",
                inline=False
            )
            if stats['top_words']:
                embed.add_field(name="Топ слов", value="\n".join([f"{i}. **{w}** ({c})" for i, (w, c) in enumerate(stats['top_words'], 1)]), inline=False)

            embed.set_footer(text=f"MCore Tolik • {BOT_VERSION}")
            await interaction.followup.send(embed=embed)

        self.tree.add_command(info_group)

        @self.tree.command(name="setchance", description="Установить шанс авто-ответа (1-100)")
        @app_commands.default_permissions(administrator=True)
        async def set_chance(interaction: discord.Interaction, процент: app_commands.Range[int, 1, 100]):
            settings = await self.settings.get(interaction.guild.id)
            settings["chat_chance"] = процент / 100.0
            await self.settings.save(interaction.guild.id)
            await interaction.response.send_message(f"✅ Шанс авто-ответа теперь: **{процент}%**", ephemeral=True)

    async def on_ready(self):
        logger.info(f"✅ {self.user} успешно залогинился!")
        self._register_commands()
        asyncio.create_task(self._keep_alive_loop())

    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        if message.attachments and not re.sub(r'<@!?\d+>', '', message.content).strip():
            return

        gid = message.guild.id
        settings = await self.settings.get(gid)

        # Ограничение по каналам
        allowed = settings.get("allowed_channels", [])
        if allowed and message.channel.id not in allowed:
            await self.process_commands(message)
            return

        # Учимся всегда, если не команда
        if not message.content.startswith('/'):
            try:
                await self.brain.learn(message, gid)
            except Exception as e:
                logger.error(f"Ошибка обучения: {e}")

        # Логика триггера ответа
        should_reply = False
        if message.reference and message.reference.resolved and message.reference.resolved.author == self.user:
            should_reply = True
        elif self.user.mentioned_in(message):
            should_reply = True
        elif settings["ai_chat"] and random.random() < settings["chat_chance"]:
            should_reply = True

        if not should_reply:
            await self.process_commands(message)
            return

        async with message.channel.typing():
            await asyncio.sleep(random.uniform(0.8, 1.8)) # Имитация набора текста

            response = None
            clean_content = re.sub(r'<@!?\d+>', '', message.content).strip()

            # Пытаемся использовать AI
            if self.ai.available and settings["mode"] != "local" and clean_content:
                urls = re.findall(r'(https?://\S+)', message.content)
                prompt = clean_content
                if urls:
                    link_info = await parse_link(urls[0])
                    if link_info:
                        prompt = f"[Контекст ссылки: {link_info}]\nСообщение юзера: {clean_content}\nПрокомментируй это."

                response = await self.ai.generate(
                    message.channel.id, prompt, settings["mode"], 
                    settings.get("custom_prompt", ""), gid, message.author.id
                )

            # Фоллбэк на Маркова
            if not response:
                stats = await self.brain.get_stats(gid)
                if stats['total_messages'] > 15:
                    response = await self.brain.generate_response(gid, message.content)
                else:
                    response = "Мне нужно больше сообщений для обучения! Напишите что-нибудь."

            # Пасхалка для любопытных
            if "ты бот" in message.content.lower() or "ты человек" in message.content.lower():
                response = "Я Толик. Я живу в серверах, ем токены и иногда притворяюсь человеком. Этого достаточно."

            if response:
                try:
                    await message.channel.send(
                        response[:1990],
                        reference=message,
                        mention_author=False,
                        allowed_mentions=discord.AllowedMentions.none()
                    )
                except discord.HTTPException as e:
                    logger.error(f"Не удалось отправить сообщение: {e}")

        await self.process_commands(message)

    async def close(self):
        logger.info("Получен сигнал завершения. Сохраняю данные...")
        if self.brain:
            await self.brain.save_all()
        if self.mongo_client:
            self.mongo_client.close()
        await super().close()

# ==============================================================================
# ТОЧКА ВХОДА
# ==============================================================================
def main():
    if not os.environ.get("TOLIK_TOKEN"):
        logger.critical("❌ Не задан TOLIK_TOKEN! Проверь переменные окружения.")
        sys.exit(1)

    # Запускаем Flask в отдельном потоке
    Thread(target=run_flask, daemon=True).start()
    logger.info("Веб-сервер запущен на порту %s", PORT)

    bot = TolikBot()
    
    # Graceful shutdown для Linux/Render (правильный async-способ)
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(sig, lambda: asyncio.create_task(bot.close()))

    try:
        bot.run(os.environ.get("TOLIK_TOKEN"))
    except discord.LoginFailure:
        logger.critical("❌ Неверный токен Discord!")
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    main()