# MCore — Discord-бот

Discord-бот, который учится на сообщениях сервера (цепи Маркова), умеет отвечать через AI (Groq) и подстраивает режим под «вайб» чата.

## СТЕК

- Python 3.10+
- [discord.py](https://github.com/Rapptz/discord.py) — Discord API, слэш-команды, UI-панель
- [motor](https://motor.readthedocs.io/) + MongoDB Atlas — хранение настроек и «мозга»
- aiohttp — запросы к Groq API и парсинг ссылок
- BeautifulSoup4 — разбор HTML ссылок
- Flask + Waitress — `/health` эндпоинт для хостинга (keep-alive)
- Groq API (по умолчанию `openai/gpt-oss-120b`)

## УСТАНОВКА

```bash
git clone <your-repo>
cd <your-repo>
pip install -r requirements.txt
```

Заполни переменные окружения (см. ниже) и запусти:

```bash
python main.py
```

На хостинге (например, Botkeep) добавь переменные в панели — бот также читает файл `.env`.

## ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (ENV)

| Переменная | Обязательна | Описание |
|---|---|---|
| `MCORE_TOKEN` | да | Токен Discord-бота |
| `MONGO_DB_NAME` | да | Имя базы данных MongoDB |
| `MONGO_USER` | да* | Пользователь MongoDB |
| `MONGO_PASSWORD` | да* | Пароль MongoDB (спецсимволы экранируются автоматически) |
| `MONGO_HOST` | да* | Хост кластера, например `cluster0.xxxxx.mongodb.net` |
| `MONGO_URI` | нет | Готовая строка подключения. Если задана — `MONGO_USER/PASSWORD/HOST` не нужны |
| `MONGO_APP_NAME` | нет | `appName` в строке подключения (по умолчанию `MCore`) |
| `GROQ_API_KEY` | нет | Ключ Groq. Без него работает только локальный режим |
| `GROQ_MODEL` | нет | Модель Groq (по умолчанию `openai/gpt-oss-120b`) |
| `PORT` | нет | Порт веб-сервера (по умолчанию `10000`) |

\* не нужны, если задан `MONGO_URI`.

Пример `.env`:

```env
MCORE_TOKEN=
GROQ_API_KEY=
GROQ_MODEL=openai/gpt-oss-120b
MONGO_USER=
MONGO_PASSWORD=
MONGO_HOST=
MONGO_APP_NAME=MCore
MONGO_DB_NAME=
PORT=10000
```

> Никогда не коммить `.env` и токены в репозиторий.

## НАСТРОЙКИ DISCORD

В [Developer Portal](https://discord.com/developers/applications) → Bot включи **Message Content Intent**. Права при приглашении: `bot` + `applications.commands`.

## КОМАНДЫ 

| Команда | Описание |
|---|---|
| `/settings` | Панель настроек (только админы): режим, авто-флуд, шанс, вайб, каналы, промпт, очистка памяти |
| `/info` | Статистика, IQ, настройки |
| `/generate anecdote\|dialogue\|quote\|story` | Генерация контента |
| `/topwords` | Топ слов |
| `/bot clear` | Очистить память (админ) |
| `/bot promptshow` | Показать кастомный промпт |

## ВАЙБ

Анализатор вайба временно меняет режим (`aggressive` при капсе, `buddy` при активном чате) и удваивает шанс ответа. В `/settings` его можно выключить кнопкой «Вайб» — тогда режим и шанс всегда берутся из настроек.
