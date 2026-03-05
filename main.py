import warnings

warnings.filterwarnings("ignore", message=".*LibreSSL.*")
warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

import os
import logging
import requests
import asyncio
import re
import time
import json
import pickle
from dotenv import load_dotenv

load_dotenv()

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from collections import defaultdict
from aiogram import Bot, Dispatcher, types
from aiogram.utils.exceptions import MessageNotModified
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    CallbackQuery,
)
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import csv
from io import StringIO
import shutil

try:
    import gspread
    from google.oauth2.service_account import Credentials

    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    logging.warning("⚠️ Google Sheets библиотеки не установлены")

# ════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ════════════════════════════════════════════════════════════════════
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "bot.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ════════════════════════════════════════════════════════════════════
DB_PATH = "bot_data.db"

CONFIG = {
    "timeout": 15,
    "cache_ttl": 1800,
    "fallback_prices": {
        "Пшеница": 15650,
        "Ячмень": 13300,
        "Кукуруза": 14000,
        "Соя": 40900,
        "Подсолнечник": 38600,
    },
    "south_regions": ["Краснодар", "Ростов", "Астрахань", "Волгоград", "Ставрополь"],
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# ════════════════════════════════════════════════════════════════════
# ПУТИ К ФАЙЛАМ - ОПРЕДЕЛЕНИЕ ОДИН РАЗ!
# ════════════════════════════════════════════════════════════════════
DATA_DIR = "data"
PULLS_FILE = os.path.join(DATA_DIR, "pulls.pkl")
os.makedirs(DATA_DIR, exist_ok=True)

USERSFILE = os.path.join(DATA_DIR, "users.pkl")
BATCHESFILE = os.path.join(DATA_DIR, "batches.pkl")
EXPEDITORSFILE = os.path.join(DATA_DIR, "expeditors.pkl")
USERS_JSON = os.path.join(DATA_DIR, "users.json")
BATCHES_JSON = os.path.join(DATA_DIR, "batches.json")
PULLS_JSON = os.path.join(DATA_DIR, "pulls.json")
PRICES_FILE = os.path.join(DATA_DIR, "prices.json")
NEWS_FILE = os.path.join(DATA_DIR, "news.json")
GOOGLE_SHEETS_CREDENTIALS = "credentials.json"
# ════════════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ БОТА - ТОЛЬКО ОДИН РАЗ!
# ════════════════════════════════════════════════════════════════════
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

# ════════════════════════════════════════════════════════════════════
# ХРАНИЛИЩА ДАННЫХ - БЕЗ ДУБЛИРОВАНИЯ!
# ════════════════════════════════════════════════════════════════════

# Основные хранилища
users = {}
user_requests = {}
batches = {}
pulls = {}
pullparticipants = {}
matches = {}
deals = {}
logger = logging.getLogger(__name__)

# Логистика и доставка
shipping_requests = {}
logistics_requests = {}
farmer_logistics_requests = {}
farmer_shipping_requests = {}

# Карточки и предложения
logistics_cards = {}
expeditor_cards = {}
expeditor_offers = {}
logistic_offers = {}

# Рейтинги
logistic_ratings = {}
last_start_times = {}
welcome_message_ids = {}
# Счётчики
batch_counter = 0
pull_counter = 0
deal_counter = 0
match_counter = 0
logistics_request_counter = 0
logistics_offer_counter = 0

# Кэш цен и новостей
prices_cache = {"data": {}, "updated": None}
news_cache = {"data": [], "updated": None}
last_prices_update = None
last_news_update = None


# ============================================================================
# КОНСТАНТЫ
# ============================================================================
TRANSPORT_TYPES = {
    "ЖД": "🚂",
    "Зерновоз": "🚚",
    "Фура": "🚛",
}

TRANSPORT_EMOJIS = {
    "ЖД": "🚂",
    "Зерновоз": "🚚",
    "Фура": "🚛",
}

status_map = {
    "active": "✅ Активна",
    "in_progress": "🔄 В работе",
    "completed": "✅ Завершено",
    "canceled": "❌ Отменено",
    "reserved": "🔒 Зарезервирована",
    "sold": "💰 Продана",
    "filled": "📥 Заполнен",
    "closed": "🔒 Закрыт",
    "cancelled": "❌ Отменён",
    "open": "📂 Открыт",
    "pending": "⏳ Ожидание",
    "delivered": "🚚 Доставлен",
}

ROLES = {
    "farmer": "🌾 Фермер",
    "exporter": "📦 Экспортёр",
    "logistic": "🚚 Логист",
}

CULTURES = ["Пшеница", "Ячмень", "Кукуруза", "Подсолнечник", "Рапс", "Соя"]

QUALITY_CLASSES = ["1 класс", "2 класс", "3 класс", "4 класс", "5 класс"]

STORAGE_TYPES = ["Элеватор", "Склад", "Напольное хранение", "Силос"]

DEAL_STATUSES = {
    "pending": "🔄 В процессе",
    "matched": "🎯 Найден партнёр",
    "shipping": "🚛 Организация перевозки",
    "completed": "✅ Завершена",
    "cancelled": "❌ Отменена",
}

PORTS = [
    "Ариб",
    "ПКФ «Волга-Порт»",
    "ПКФ «Юг-Тер»",
    "ПАО «Астр.Порт»",
    "Универ.Порт",
    "Юж.Порт",
    "Агрофуд",
    "Моспорт",
    "ЦГП",
    "АМП",
    "Армада",
    "Стрелец",
    "Альфа",
]


def translate_pull_status(
    status: str = None, current_volume: float = None, target_volume: float = None
) -> str:
    """✅ Переводит статус пула на русский с эмодзи"""

    # ✅ Если статус не задан, но известны объёмы — определяем автоматически
    if not status and current_volume is not None and target_volume is not None:
        if target_volume > 0 and current_volume >= target_volume:
            status = "filled"
        elif current_volume > 0:
            status = "active"
        else:
            status = "active"

    # ✅ Словарь статусов
    status_map = {
        "active": "🟢 Активный",
        "активный": "🟢 Активный",
        "открыт": "🟢 Активный",
        "open": "🟢 Активный",
        "filling": "🟡 Заполняется",  # ← ДОБАВЛЕНО
        "filled": "🎉 Заполнен",  # ← УБРАНО "(100%)"
        "заполнен": "🎉 Заполнен",
        "closed": "🔴 Закрыт",
        "закрыт": "🔴 Закрыт",  # ← ДОБАВЛЕНО
        "завершён": "✔️ Завершён",
        "completed": "✔️ Завершён",
        "cancelled": "❌ Отменён",
        "отменён": "❌ Отменён",  # ← ДОБАВЛЕНО
    }

    # ✅ Приводим к нижнему регистру и убираем пробелы
    if status:
        normalized_status = str(status).lower().strip()
    else:
        return "❓ Неизвестен"  # ← Если статус None

    return status_map.get(normalized_status, f"⚪ {status}")


def migrate_all_existing_pulls():
    """✅ КРИТИЧЕСКАЯ МИГРАЦИЯ: Добавляем новые поля во ВСЕ старые пулы"""
    logging.info("🔄 ЗАПУСК МИГРАЦИИ ПУЛОВ...")
    migrated = 0

    # ✅ ПРАВИЛЬНО: Итерируем pulls['pulls'], а не pulls
    for pull_id, pull in list(pulls.get("pulls", {}).items()):
        if not isinstance(pull, dict):
            logging.warning(
                f"⚠️ Пул {pull_id} имеет неправильный тип {type(pull)}, пропускаю"
            )
            continue

        changed = False

        # Добавляем новые поля если их нет
        if "creator_id" not in pull and "exporter_id" in pull:
            pull["creator_id"] = pull["exporter_id"]
            changed = True

        if "price_per_ton" not in pull and "price" in pull:
            pull["price_per_ton"] = float(pull.get("price", 0))
            changed = True

        if "farmer_ids" not in pull:
            pull["farmer_ids"] = pull.get("participants", [])
            changed = True

        if "batches" not in pull:
            pull["batches"] = pull.get("batch_ids", [])
            changed = True

        if "batch_ids" not in pull:
            pull["batch_ids"] = []
            changed = True

        if "logist_ids" not in pull:
            pull["logist_ids"] = []
            changed = True

        if "expeditor_ids" not in pull:
            pull["expeditor_ids"] = []
            changed = True

        if changed:
            migrated += 1
            logging.info(
                f"   ✅ Пул #{pull_id} мигрирован: farmer_ids={len(pull.get('farmer_ids', []))} фермеров"
            )

    if migrated > 0:
        save_pulls_to_pickle()
        logging.info(f"✅ МИГРАЦИЯ ЗАВЕРШЕНА: {migrated} пулов обновлено!")
    else:
        logging.info("ℹ️ Все пулы уже миграцированы")

    return migrated


def migrate_old_pulls():
    """Устанавливает статус для старых пулов без статуса"""
    migrated_count = 0

    all_pulls = pulls.get("pulls", {})  # ✅ ДОБАВЛЕНО

    for pull_id, pull in all_pulls.items():  # ✅ ИСПРАВЛЕНО
        if not isinstance(pull, dict):  # ✅ ДОБАВЛЕНО
            continue

        current_status = pull.get("status", None)

        # Логируем текущее состояние пула
        logging.info(
            f"🔍 Пул #{pull_id}: статус='{current_status}' (тип: {type(current_status).__name__}), "
            f"объём={pull.get('current_volume', 0)}/{pull.get('target_volume', 0)}"
        )

        # Проверяем: статус отсутствует, пустой, None или некорректный
        needs_migration = (
            current_status is None
            or current_status == ""
            or current_status.lower() not in valid_statuses
        )

        if needs_migration:
            # Определяем статус по заполненности
            current_vol = pull.get("current_volume", 0)
            target_vol = pull.get("target_volume", 1)

            if current_vol >= target_vol and target_vol > 0:
                pull["status"] = "filled"
                logging.info(
                    f"✅ Пул #{pull_id} обновлён: 'filled' (заполнен {current_vol}/{target_vol} т)"
                )
                migrated_count += 1
            else:
                pull["status"] = "active"
                logging.info(
                    f"✅ Пул #{pull_id} обновлён: 'active' (активен {current_vol}/{target_vol} т)"
                )
                migrated_count += 1
        else:
            logging.info(
                f"ℹ️ Пул #{pull_id}: статус '{current_status}' корректен, пропускаем"
            )

    if migrated_count > 0:
        save_pulls_to_pickle()
        logging.info(f"✅ Миграция завершена: обновлено {migrated_count} пулов")
    else:
        logging.info("ℹ️ Миграция не требуется: все пулы уже имеют корректный статус")


# ✅ БЕЗ async - только обычная функция!
def find_matching_batches(pull_data):
    """Поиск подходящих партий для пула"""
    matching_batches = []

    if not batches or not isinstance(batches, dict):
        logging.warning(f"⚠️ Батчи пусты или неправильный тип: {type(batches)}")
        return matching_batches

    try:
        for user_id, user_batches in batches.items():
            # Если это словарь батчей (batch_id: {...})
            if isinstance(user_batches, dict):
                for batch_id, batch in user_batches.items():
                    if not isinstance(batch, dict):
                        continue

                    if _batch_matches_pull(batch, pull_data):
                        batch_copy = batch.copy()
                        batch_copy["farmer_id"] = user_id
                        batch_copy["batch_id"] = batch_id
                        matching_batches.append(batch_copy)

            # Если это список батчей
            elif isinstance(user_batches, list):
                for idx, batch in enumerate(user_batches):
                    if not isinstance(batch, dict):
                        continue

                    if _batch_matches_pull(batch, pull_data):
                        batch_copy = batch.copy()
                        batch_copy["farmer_id"] = user_id
                        batch_copy["batch_id"] = batch.get("id", idx)
                        matching_batches.append(batch_copy)

    except Exception as e:
        logging.error(f"❌ Ошибка поиска батчей: {e}", exc_info=True)

    logging.info(f"🔍 Найдено батчей: {len(matching_batches)}")
    return matching_batches


def _batch_matches_pull(batch, pull_data):
    """✅ Логика сравнения батча и пула"""
    return (
        and batch.get("price", float("inf"))
        <= pull_data.get("price", float("inf")) * 0.75
    )


def parse_join_pull_callback(callback_data: str) -> dict:
    """Универсальный парсер callback для join_pull"""
    try:
        parts = callback_data.split(":")
        pull_id = int(parts[1])
        timestamp = parts[2] if len(parts) >= 3 else None

        logging.info(f"🔗 Parsed callback: pull_id={pull_id}, timestamp={timestamp}")
        return {"pull_id": pull_id, "timestamp": timestamp}
    except (ValueError, IndexError) as e:
        logging.error(f"❌ Ошибка парсинга '{callback_data}': {e}")
        raise


def validate_batch_volume(batch: dict, pull: dict) -> tuple:
    """Проверяет, поместится ли партия в пул"""
    batch_volume = batch.get("volume", 0)
    current_volume = pull.get("current_volume", 0)
    target_volume = pull.get("target_volume", 0)
    available = target_volume - current_volume

    if batch_volume > available:
        return (
            False,
            f"❌ Объём партии ({batch_volume} т) превышает доступный ({available} т)",
        )

    return (True, "✅ Партия подходит")


async def check_and_close_pool_if_full(pull_id: int):
    """Автоматически закрывает пул если current_volume >= target_volume"""
        logging.error(f"❌ Пул #{pull_id} не найден")
        return
    current = pull.get("current_volume", 0)
    target = pull.get("target_volume", 0)

    logging.info(f"🔍 Проверка автозакрытия пула #{pull_id}: {current}/{target} т")

    if current >= target:
        pull["status"] = "closed"  # Правильный формат статуса
        pull["closed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_pulls_to_pickle()

        logging.info(f"🔒 Пул #{pull_id} АВТОМАТИЧЕСКИ ЗАКРЫТ ({current}/{target} т)")

        notification_text = (
            f"🎉 <b>Пул #{pull_id} закрыт!</b>\n\n"
            f"📦 Культура: {pull.get('culture', 'Неизвестна')}\n"
            f"📊 Объём: {current}/{target} т\n"
        )

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "📋 Посмотреть пул", callback_data=f"view_pull:{pull_id}"
            )
        )

        # Уведомляем экспортёра
        exporter_id = pull.get("exporter_id")
        if exporter_id:
            try:
                await bot.send_message(
                    exporter_id,
                    notification_text,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as e:
                logging.error(f"❌ Ошибка уведомления экспортёра: {e}")

        # Уведомляем фермеров
        farmer_ids = pull.get("farmer_ids", [])
        for farmer_id in farmer_ids:
            try:
                await bot.send_message(
                    farmer_id,
                    notification_text,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as e:
                logging.error(f"❌ Ошибка уведомления фермера {farmer_id}: {e}")

        # Уведомляем логистов
        logist_ids = pull.get("logist_ids", [])
        for logist_id in logist_ids:
            try:
                await bot.send_message(
                    logist_id,
                    notification_text,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as e:
                logging.error(f"❌ Ошибка уведомления логиста {logist_id}: {e}")


# ==================== КОНЕЦ НОВЫХ ФУНКЦИЙ ====================


def save_deals_to_pickle():
    """Сохранение сделок в pickle"""
    try:
        deals_file = os.path.join(DATA_DIR, "deals.pkl")
        with open(deals_file, "wb") as f:
            pickle.dump(deals, f)
        logging.info("✅ Сделки сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения сделок: {e}")


def save_farmers_logistics():
    try:
            pickle.dump(farmer_logistics_requests, f)
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения farmer_logistics: {e}")


def save_logistics_requests_data():
    """Сохранение заявок логистов"""
    try:
        path = os.path.join(DATA_DIR, "logistics_requests.pkl")
        with open(path, "wb") as f:
            pickle.dump(logistics_requests, f)
        logging.info(f"✅ Заявки логистов: {len(logistics_requests)}")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения logistics_requests: {e}")


def save_shipping_requests_data():
    """Сохранение заявок на доставку экспортеров"""
    try:
        path = os.path.join(DATA_DIR, "shipping_requests.pkl")
        with open(path, "wb") as f:
            pickle.dump(shipping_requests, f)
        logging.info(f"✅ Заявки на доставку: {len(shipping_requests)}")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения shipping_requests: {e}")


def save_logistics_offers_data():
    """Сохранение предложений логистов"""
    try:
        with open(path, "wb") as f:
    except Exception as e:


def save_expeditor_data():
    """Сохранение данных экспедиторов"""
    try:
        path = os.path.join(DATA_DIR, "expeditor_cards.pkl")
        with open(path, "wb") as f:
            pickle.dump(expeditor_cards, f)
        logging.info(f"✅ Карточки экспедиторов: {len(expeditor_cards)}")

        path = os.path.join(DATA_DIR, "expeditor_offers.pkl")
        with open(path, "wb") as f:
            pickle.dump(expeditor_offers, f)
        logging.info(f"✅ Предложения экспедиторов: {len(expeditor_offers)}")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения expeditor: {e}")


def save_logistics_cards_data():
    """Сохранение карточек логистов"""
    try:
        path = os.path.join(DATA_DIR, "logistics_cards.pkl")
        with open(path, "wb") as f:
            pickle.dump(logistics_cards, f)
        logging.info(f"✅ Карточки логистов: {len(logistics_cards)}")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения logistics_cards: {e}")


def save_data():
    """✅ ОСНОВНАЯ функция сохранения ВСЕ данных"""
    try:
        logging.info("🔄 Начинаю сохранение данных...")
        os.makedirs(DATA_DIR, exist_ok=True)

        # Сохраняем заявки
        save_farmers_logistics()
        save_logistics_requests_data()
        save_shipping_requests_data()

        # Сохраняем предложения
        save_logistics_offers_data()
        save_expeditor_data()

        # Сохраняем карточки
        save_logistics_cards_data()

        # Сохраняем базовые данные через существующие функции
        save_users_to_pickle()
        save_batches_to_pickle()
        save_pulls_to_pickle()
        save_deals_to_pickle()

        logging.info("✅ ВСЕ ДАННЫЕ СОХРАНЕНЫ УСПЕШНО")
        return True

    except Exception as e:
        logging.error(f"❌ ОШИБКА сохранения: {e}", exc_info=True)
        return False


def load_data():
    """✅ Загрузка ВСЕ данных при старте"""

    try:
        logging.info("🔄 Начинаю загрузку данных...")
        os.makedirs(DATA_DIR, exist_ok=True)

        # ════════════════════════════════════════════════════════════════════════════
        # ✅ ЗАГРУЖАЕМ USERS
        # ════════════════════════════════════════════════════════════════════════════
                logging.info(f"✅ Пользователи загружены: {len(users)}")

        # ════════════════════════════════════════════════════════════════════════════
        # ✅ ЗАГРУЖАЕМ BATCHES
        # ════════════════════════════════════════════════════════════════════════════
        if os.path.exists(os.path.join(DATA_DIR, "batches.pkl")):
            with open(os.path.join(DATA_DIR, "batches.pkl"), "rb") as f:

        # ════════════════════════════════════════════════════════════════════════════
        # ✅ ЗАГРУЖАЕМ PULLS С PULLPARTICIPANTS (КРИТИЧНОЕ!)
        # ════════════════════════════════════════════════════════════════════════════
        if os.path.exists(os.path.join(DATA_DIR, "pulls.pkl")):
            with open(os.path.join(DATA_DIR, "pulls.pkl"), "rb") as f:
                pulls_loaded = pickle.load(f)

                # Если структура правильная - распаковываем
                if isinstance(pulls_loaded, dict):
                    # ═══════════════════════════════════════════════════════
                    # ✅ ЗАГРУЖАЕМ PULLS
                    # ═══════════════════════════════════════════════════════
                    if "pulls" in pulls_loaded:
                        pulls_data = pulls_loaded.get("pulls", {})

                        # ✅ ПРОВЕРКА: Если это один пул (есть 'id'), оборачиваем в словарь
                        if isinstance(pulls_data, dict):
                            if "id" in pulls_data:
                                # ❌ ЭТО ОДИН ПУЛ! Сохраняем его правильно
                                logging.warning(
                                    "⚠️ pulls['pulls'] - это один пул! Конвертирую..."
                                )
                                pull_id = pulls_data.get("id", 1)
                                pulls["pulls"] = {pull_id: pulls_data}
                                logging.info(
                                    f"✅ Пул #{pull_id} сохранён в новой структуре"
                                )
                            else:
                                # ✅ Это словарь пулов
                        else:
                            pulls["pulls"] = {}
                    else:
                        pulls["pulls"] = {}

                    # ═══════════════════════════════════════════════════════
                    # ✅ ЗАГРУЖАЕМ PULLPARTICIPANTS
                    # ═══════════════════════════════════════════════════════
                    if "pullparticipants" in pulls_loaded:
                        pullparticipants_data = pulls_loaded.get("pullparticipants", {})

                        # ✅ ПРОВЕРКА: Если это данные пула, а не участники
                        if isinstance(pullparticipants_data, dict):
                            # Проверяем ключи - если это данные пула, очищаем
                            if (
                                "farmer_ids" in pullparticipants_data
                                or "status" in pullparticipants_data
                                or "batches" in pullparticipants_data
                                or "batch_ids" in pullparticipants_data
                            ):
                                # ❌ ЭТО ДАННЫЕ ПУЛА! Создаём пустой словарь
                                logging.warning(
                                    "⚠️ pullparticipants содержит данные пула, а не участников!"
                                )
                                logging.warning("⚠️ Создаю пустой pullparticipants...")
                                pulls["pullparticipants"] = {}
                            else:
                                # ✅ Это словарь участников (ключи = ID пулов)
                        else:
                            pulls["pullparticipants"] = {}
                    else:
                        pulls["pullparticipants"] = {}

                    logging.info(f"✅ Пулы загружены: {len(pulls.get('pulls', {}))}")
                    logging.info(
                        f"✅ Участники пулов загружены: {len(pulls.get('pullparticipants', {}))}"
                    )

                    if pulls.get("pullparticipants"):
                        logging.info(
                            f"   📊 pullparticipants.keys() = {list(pulls.get('pullparticipants', {}).keys())}"
                        )
                else:
                    logging.warning("⚠️ pulls.pkl содержит неправильный тип данных!")
                    pulls["pulls"] = {}
                    pulls["pullparticipants"] = {}
        else:
            logging.info("ℹ️ pulls.pkl не найден - создаю пустую структуру")
            pulls["pulls"] = {}
            pulls["pullparticipants"] = {}

        # ════════════════════════════════════════════════════════════════════════════
        # ✅ ЗАГРУЖАЕМ ОСТАЛЬНЫЕ ДАННЫЕ
        # ════════════════════════════════════════════════════════════════════════════

        # Загружаем заявки фермеров
        if os.path.exists(os.path.join(DATA_DIR, "farmer_logistics_requests.pkl")):
            with open(
                os.path.join(DATA_DIR, "farmer_logistics_requests.pkl"), "rb"
            ) as f:
                farmer_logistics_requests = pickle.load(f)
                logging.info(
                    f"✅ Заявки фермеров загружены: {len(farmer_logistics_requests)}"
                )

        if os.path.exists(os.path.join(DATA_DIR, "logistics_requests.pkl")):
            with open(os.path.join(DATA_DIR, "logistics_requests.pkl"), "rb") as f:
                logistics_requests = pickle.load(f)
                logging.info(f"✅ Заявки логистов загружены: {len(logistics_requests)}")

        if os.path.exists(os.path.join(DATA_DIR, "shipping_requests.pkl")):
            with open(os.path.join(DATA_DIR, "shipping_requests.pkl"), "rb") as f:
                shipping_requests = pickle.load(f)
                logging.info(f"✅ Заявки доставки загружены: {len(shipping_requests)}")

        # Загружаем предложения
                logging.info(
                )

        if os.path.exists(os.path.join(DATA_DIR, "expeditor_cards.pkl")):
            with open(os.path.join(DATA_DIR, "expeditor_cards.pkl"), "rb") as f:
                expeditor_cards = pickle.load(f)
                logging.info(
                    f"✅ Карточки экспедиторов загружены: {len(expeditor_cards)}"
                )

        if os.path.exists(os.path.join(DATA_DIR, "expeditor_offers.pkl")):
            with open(os.path.join(DATA_DIR, "expeditor_offers.pkl"), "rb") as f:
                expeditor_offers = pickle.load(f)
                logging.info(
                    f"✅ Предложения экспедиторов загружены: {len(expeditor_offers)}"
                )

        if os.path.exists(os.path.join(DATA_DIR, "logistics_cards.pkl")):
            with open(os.path.join(DATA_DIR, "logistics_cards.pkl"), "rb") as f:
                logistics_cards = pickle.load(f)
                logging.info(f"✅ Карточки логистов загружены: {len(logistics_cards)}")

                # ✅ ЗАГРУЖАЕМ batch_counter
        global batch_counter
        if os.path.exists(os.path.join(DATA_DIR, "batch_counter.pkl")):
            with open(os.path.join(DATA_DIR, "batch_counter.pkl"), "rb") as f:
                batch_counter = pickle.load(f)
                logging.info(f"✅ batch_counter загружен: {batch_counter}")
        else:
            # Если файла нет, вычисляем максимальный ID из существующих партий
            max_id = 0
            batch_counter = max_id
            logging.info(f"✅ batch_counter вычислен из партий: {batch_counter}")

        logging.info("✅ ВСЕ ДАННЫЕ ЗАГРУЖЕНЫ УСПЕШНО!")
        return True

    except Exception as e:
        logging.error(f"❌ ОШИБКА загрузки: {e}", exc_info=True)
        return False


def get_logistics_by_port(port):
    """Поиск логистов по порту с полной информацией"""
    result = []
    for uid, card in logistics_cards.items():
        ports = card.get("ports", [])
        if isinstance(ports, str):
            ports = [p.strip() for p in ports.split(",")]
        if port in ports or "Все порты" in ports:
            result.append(
                {
                    "user_id": uid,
                    "name": user.get("name", "Н/Д"),
                    "company": card.get("company", user.get("name", "Н/Д")),
                    "inn": user.get("inn", "Н/Д"),  # ✅ ДОБАВИЛИ
                    "ogrn": user.get("ogrn", "Не указан"),  # ✅ ДОБАВИЛИ
                    "phone": user.get("phone", "Н/Д"),
                    "email": user.get("email", "Н/Д"),  # ✅ ДОБАВИЛИ
                    "price_per_ton": card.get("price_per_ton", 0),
                    "transport_type": card.get("transport_type", "Н/Д"),
                }
            )
    return result


    result = []
    for uid, card in expeditor_cards.items():
        ports = card.get("ports", [])
        if isinstance(ports, str):
        if port in ports or "Все порты" in ports:
            result.append(
                {
                    "user_id": uid,
                    "name": user.get("name", "Н/Д"),
                    "phone": user.get("phone", "Н/Д"),
                }
            )
    return result


# ════════════════════════════════════════════════════════════════════════════════
# 🔄 КОНВЕРТЕРЫ ДАННЫХ - для работы с текущим форматом пулов
# ════════════════════════════════════════════════════════════════════════════════


def get_pull_with_participants(pull_id):
    """Получить пул с участниками в одном объекте (твой текущий формат)"""
    if not pulls or "pulls" not in pulls:
        return None



def get_farmers_from_pull(pull_id):
    """Получить фермеров, присоединившихся к пулу"""
    pull_data = get_pull_with_participants(pull_id)
    if not pull_data:
        return []

    # ✅ Читаем данные в твоём текущем формате
    farmer_ids = pull_data.get("farmer_ids", [])

    if isinstance(farmer_ids, list):
        return farmer_ids

    return []


def get_batches_from_pull(pull_id):
    """Получить партии, присоединившиеся к пулу"""
    pull_data = get_pull_with_participants(pull_id)
    if not pull_data:
        return []

    # ✅ Читаем батчи в твоём формате
    batches = pull_data.get("batches", [])

    if isinstance(batches, list):
        return batches

    return []


def get_all_pulls_with_format():
    """Получить ВСЕ пулы в твоём текущем формате"""
    if not pulls or "pulls" not in pulls:
        return {}

    return pulls.get("pulls", {})


def get_vehicle_emoji(transport_type):
    """Возвращает эмодзи для типа транспорта"""
    transport_type_lower = transport_type.lower()

    if "фура" in transport_type_lower:
        return "🚛"
    elif "грузовик" in transport_type_lower:
        return "🚚"
    elif "газель" in transport_type_lower:
        return "🚐"
    elif "ж/д" in transport_type_lower or "вагон" in transport_type_lower:
        return "🚂"
    elif "баржа" in transport_type_lower or "судно" in transport_type_lower:
        return "🚢"
    elif "контейнер" in transport_type_lower:
        return "📦"
    elif "рефрижератор" in transport_type_lower:
        return "🧊"
    else:
        return "🚛"  # По умолчанию


def generate_unique_id():
    """Генерация уникального ID"""
    import uuid
    import time

    timestamp = int(time.time() * 1000) % 1000000
    unique = str(uuid.uuid4())[:8].upper()
    return f"{timestamp}{unique}"


def generate_id():
    """Генерация уникального ID"""
    import uuid, time

    return f"{int(time.time())%1000000}{str(uuid.uuid4())[:6].upper()}"


def format_logistics_cards(logistics):
    """Форматирование карточек логистов для отображения в списке"""
    if not logistics:
        return "❌ Логисты не найдены для данного портового комплекса"

    text = "<b>🚛 НАЙДЕННЫЕ ЛОГИСТЫ:</b>\n\n"

    for i, log in enumerate(logistics[:5], 1):
        # Получаем эмодзи для типа транспорта
        emoji = get_vehicle_emoji(log.get("transport_type", ""))

        # Основные данные
        company = log.get("company", log.get("name", "Н/Д"))
        phone = log.get("phone", "Н/Д")
        price_per_ton = log.get("price_per_ton", 0)
        transport_type = log.get("transport_type", "Н/Д")

        # Реквизиты
        inn = log.get("inn", "Н/Д")
        ogrn = log.get("ogrn", "Не указан")
        email = log.get("email", "Н/Д")

        # Формируем карточку
        text += f"{i}. {emoji} <b>{company}</b>\n"
        text += f"   🏢 ИНН: {inn} | ОГРН: {ogrn}\n"
        text += f"   📱 <code>{phone}</code>\n"

        if email != "Н/Д":
            text += f"   📧 <code>{email}</code>\n"

        text += f"   🚗 {transport_type}\n"
        text += f"   💰 {price_per_ton:,.0f} ₽/т\n\n"

    # Информация об остальных
    if len(logistics) > 5:
        text += f"<i>ℹ️ Ещё {len(logistics) - 5} логистов в результатах</i>\n"
        text += "❓ Запросите доступ к полному списку"

    return text


    if not expeditors:
        return "❌ Экспедиторы не найдены для данного портового комплекса"

    text = "<b>📋 НАЙДЕННЫЕ ЭКСПЕДИТОРЫ:</b>\n\n"

    for i, exp in enumerate(expeditors[:5], 1):
        company = exp.get("company", exp.get("name", "Н/Д"))
        phone = exp.get("phone", "Н/Д")
        inn = exp.get("inn", "Н/Д")
        ogrn = exp.get("ogrn", "Не указан")

        text += f"{i}. 📋 <b>{company}</b>\n"
        text += f"   🏢 ИНН: {inn} | ОГРН: {ogrn}\n"
        text += f"   📱 <code>{phone}</code>\n"
        if email != "Н/Д":
            text += f"   📧 <code>{email}</code>\n"


    if len(expeditors) > 5:
        text += f"<i>ℹ️ Ещё {len(expeditors) - 5} экспедиторов в результатах</i>\n"
        text += "❓ Запросите доступ к полному списку"

    return text


def save_logistics_requests_to_pickle():
    """Сохранение заявок на логистику"""
    try:
        with open(os.path.join(DATA_DIR, "logistics_requests.pkl"), "wb") as f:
            pickle.dump(logistics_requests, f)
        logging.info("✅ Заявки сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")


def save_logistics_offers_to_pickle():
    """Сохранение предложений"""
    try:
        logging.info("✅ Предложения сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")


def save_logistics_cards_to_pickle():
    """Сохранение карточек логистов"""
    try:
        with open(os.path.join(DATA_DIR, "logistics_cards.pkl"), "wb") as f:
            pickle.dump(logistics_cards, f)
        logging.info("✅ Карточки логистов сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")


def save_expeditor_cards_to_pickle():
    """Сохранение карточек экспедиторов"""
    try:
        with open(os.path.join(DATA_DIR, "expeditor_cards.pkl"), "wb") as f:
            pickle.dump(expeditor_cards, f)
        logging.info("✅ Карточки экспедиторов сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")


def save_logistic_offers():
    try:
        with open(os.path.join(DATA_DIR, "logistic_offers.pkl"), "wb") as f:
            pickle.dump(logistic_offers, f)
        logging.info("✅ Logistic offers saved")
    except Exception as e:
        logging.error(f"❌ Error saving logistic offers: {e}")


def load_logistic_offers():
    global logistic_offers
    try:
        filepath = os.path.join(DATA_DIR, "logistic_offers.pkl")
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
            logging.info(f"✅ Loaded {len(logistic_offers)} logistic offers")
        else:
            logistic_offers = {}
    except Exception as e:
        logging.error(f"❌ Error loading logistic offers: {e}")
        logistic_offers = {}


def save_deliveries():
    try:
        with open(os.path.join(DATA_DIR, "deliveries.pkl"), "wb") as f:
            pickle.dump(deliveries, f)
        logging.info("✅ Deliveries saved")
    except Exception as e:
        logging.error(f"❌ Error saving deliveries: {e}")


def load_deliveries():
    global deliveries
    try:
        filepath = os.path.join(DATA_DIR, "deliveries.pkl")
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
            logging.info(f"✅ Loaded {len(deliveries)} deliveries")
        else:
            deliveries = {}
    except Exception as e:
        logging.error(f"❌ Error loading deliveries: {e}")
        deliveries = {}


def save_expeditor_offers():
    """Сохранение предложений экспедиторов"""
    try:
            pickle.dump(expeditor_offers, f)
        logging.info(f"✅ Сохранено {len(expeditor_offers)} предложений экспедиторов")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения expeditor_offers: {e}")


def clean_text(text):
    """Очистка текста"""
    if not text:
        return ""
    text = str(text).strip()
    text = re.sub(r"[^\w\s\-.,ёЁА-Яа-я0-9]", "", text)
    return text


def load_expeditor_offers():
    """Загрузка предложений экспедиторов из файла"""
    global expeditor_offers

    try:
        file_path = os.path.join(DATA_DIR, "expeditor_offers.pkl")
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
            logging.info(f"✅ Loaded {len(expeditor_offers)} expeditor offers")
        else:
            expeditor_offers = {}
            logging.info("ℹ️ No expeditor offers file found, starting fresh")
    except Exception as e:
        logging.error(f"❌ Error loading expeditor offers: {e}")
        expeditor_offers = {}


def load_expeditor_cards():
    """Загрузка карточек экспедиторов из файла"""
    global expeditor_cards

    try:
        file_path = os.path.join(DATA_DIR, "expeditor_cards.pkl")
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
            logging.info(f"✅ Загружено карточек экспедиторов: {len(expeditor_cards)}")
        else:
            expeditor_cards = {}
            logging.info("ℹ️ Файл expeditor_cards.pkl не найден, создаём новый")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки expeditor_cards: {e}")
        expeditor_cards = {}


# ============================================================================
# КРИТИЧЕСКАЯ ФУНКЦИЯ: Парсинг ID из callback_data
# ============================================================================



    # ✅ Список префиксов которые НЕ содержат ID
    non_id_prefixes = ["culture:", "port:", "region:", "back_to_pulls"]

    # ✅ Проверяем префиксы без ID
    for prefix in non_id_prefixes:
        if callback_data == prefix or callback_data.startswith(prefix):
            return None

    try:
        if ":" in callback_data:
            id_str = callback_data.split(":")[-1]
        elif "_" in callback_data:
            id_str = callback_data.split("_")[-1]
        else:
            id_str = callback_data

        return None

    except (ValueError, IndexError) as e:
        logging.error(f"❌ parse_callback_id ошибка: {e} для {callback_data}")
        raise ValueError(f"Не могу парсить ID из '{callback_data}'")


class RegistrationStatesGroup(StatesGroup):
    name = State()
    phone = State()
    email = State()
    region = State()
    role = State()
    inn = State()
    ogrn = State()
    company_details = State()


class EditRequestStates(StatesGroup):
    waiting_for_choice = State()
    waiting_for_custom_value = State()
    waiting_for_number = State()


class FarmerShippingRequestStates(StatesGroup):

    select_batch = State()
    select_transport = State()
    confirm = State()


# FSM для поиска по культуре
class SearchByCulture(StatesGroup):
    waiting_culture = State()


# FSM для создания карточки логиста
class CreateLogisticCardStates(StatesGroup):
    routes = State()
    price_per_km = State()
    price_per_ton = State()
    min_volume = State()
    transport_type = State()
    ports = State()
    additional_info = State()


# FSM для создания карточки экспедитора
class CreateExpeditorCardStates(StatesGroup):
    experience = State()
    description = State()


class AddBatch(StatesGroup):
    """Добавление партии фермером с расширенными полями"""

    culture = State()
    region = State()
    volume = State()
    price = State()
    humidity = State()
    impurity = State()
    quality_class = State()
    storage_type = State()
    readiness_date = State()


class EditBatch(StatesGroup):
    """Редактирование партии"""

    field = State()
    new_value = State()


class DeleteBatch(StatesGroup):
    """Удаление партии"""

    confirmation = State()


class EditProfile(StatesGroup):
    """Редактирование профиля"""

    field = State()
    new_value = State()


class CreatePullStatesGroup(StatesGroup):
    culture = State()  # Выбор культуры
    volume = State()  # Объем
    price = State()  # Цена
    port = State()  # Порт
    moisture = State()  # Влажность
    nature = State()  # Натура
    impurity = State()  # Сорная примесь
    weed = State()  # Зерновая примесь
    documents = State()  # Документы
    doctype = State()  # Тип документов


class JoinPullStatesGroup(StatesGroup):
    """Присоединение фермера к пулу"""

    select_pull = State()
    select_batch = State()
    enter_volume = State()
    confirm_join = State()
    volume = State()


class LogisticsOffer(StatesGroup):
    """Предложение логиста"""

    transport_type = State()
    route_from = State()
    route_to = State()
    price_per_ton = State()
    delivery_days = State()
    additional_info = State()


class LogisticOfferStates(StatesGroup):
    """Состояния для создания предложения логиста"""

    request_id = State()
    vehicle_type = State()
    price = State()
    delivery_date = State()
    confirm = State()


class ExpeditorOfferStates(StatesGroup):
    """Состояния для создания предложения экспедитора"""

    service_type = State()
    ports = State()
    price = State()
    terms = State()
    confirm = State()


class ExpeditorOffer(StatesGroup):
    """Предложение экспедитора"""

    services = State()
    price = State()
    terms_days = State()
    additional_info = State()


class Broadcast(StatesGroup):
    """Рассылка админа"""

    message = State()
    confirm = State()


class AdminStates(StatesGroup):
    """Состояния админ панели"""

    waiting_broadcast_message = State()
    waiting_user_search = State()
    waiting_export_format = State()
    viewing_user_details = State()
    waiting_manual_match = State()


class AdminStats(StatesGroup):
    """Статистика админа"""

    period = State()


class ExportData(StatesGroup):
    """Экспорт данных админом"""

    data_type = State()


class EditPullStatesGroup(StatesGroup):
    """Редактирование пула экспортёром"""

    select_pull = State()
    select_field = State()
    edit_culture = State()
    edit_volume = State()
    edit_price = State()
    edit_port = State()
    edit_region = State()
    edit_moisture = State()
    edit_nature = State()
    edit_impurity = State()
    edit_weed = State()
    confirm_changes = State()


class QuickBatchStatesGroup(StatesGroup):
    """FSM для быстрого создания партии при присоединении к пуллу"""

    pull_id = State()  # ID пулла
    volume = State()
    price = State()
    quality = State()
    moisture = State()
    nature = State()
    impurity = State()
    documents = State()


class SearchBatchesStatesGroup(StatesGroup):
    """Состояния для расширенного поиска партий"""

    culture = State()
    enter_culture = State()
    enter_max_price = State()
    enter_max_volume = State()
    enter_min_price = State()
    enter_min_volume = State()
    enter_quality_class = State()
    enter_region = State()
    enter_storage_type = State()
    region = State()


class ExpeditorEditStates(StatesGroup):
    """Состояния редактирования карточки экспедитора"""

    price = State()
    capacity = State()
    regions = State()
    description = State()


class AttachFilesStatesGroup(StatesGroup):
    """Прикрепление файлов к партии"""

    upload_files = State()
    confirm_upload = State()


class ShippingRequestStatesGroup(StatesGroup):
    """Заявка на логистику"""

    pull_id = State()
    route_from = State()
    route_to = State()
    price_rub = State()
    volume = State()
    culture = State()
    desired_date = State()


# Логистика - заявка экспортёра
class CreateLogisticRequestStatesGroup(StatesGroup):
    pull_choose = State()
    transport_type = State()
    culture = State()
    route_from = State()
    port = State()
    loading_date = State()
    desired_price = State()
    notes = State()


# Логистика - отклик логиста
class LogisticOfferStatesGroup(StatesGroup):
    """FSM для создания предложения логиста"""

    request_id = State()  # ID заявки
    vehicle_type = State()  # Тип транспорта
    price = State()  # Цена доставки
    delivery_date = State()  # Дата доставки
    additional_info = State()  # Дополнительная информация
    confirm = State()  # Подтверждение


class LogisticCardStates(StatesGroup):
    vehicle_type = State()
    capacity = State()
    regions = State()
    price_per_km = State()
    description = State()


# ← ДОБАВЬТЕ ЭТО:
class EditCardStates(StatesGroup):
    vehicle_type = State()
    capacity = State()
    regions = State()
    price_per_km = State()
    description = State()


def validate_phone(phone):
    """Валидация номера телефона"""
    cleaned = re.sub(r"[\s\-\(\)\+]", "", phone)
    return len(cleaned) >= 10 and cleaned.isdigit()


def validate_email(email):
    """Проверка email на валидность с правильным regex"""
    if not email or not isinstance(email, str):
        return False
    # Правильный паттерн: должна быть локальная часть перед @
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}₽"
    return bool(re.match(pattern, email.strip()))


def validate_inn(inn):
    """Валидация ИНН"""
    return inn.isdigit() and len(inn) in [10, 12]


def validate_volume(volume):
    """Валидация объема"""
    try:
        vol = float(volume)
        return vol > 0
    except ValueError:
        return False


def validate_price(price):
    """Валидация цены"""
    try:
        pr = float(price)
        return pr > 0
    except ValueError:
        return False


def validate_percentage(value):
    """Валидация процентного значения"""
    try:
        val = float(value)
        return 0 <= val <= 100
    except ValueError:
        return False


def farmer_keyboard():
    """Клавиатура для фермера"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add("➕ Добавить партию")
    keyboard.add("🚚 Создать заявку на доставку")
    keyboard.row("🔧 Мои партии", "🎯 Пулы")
    keyboard.row("🔍 Поиск экспортёров", "📋 Мои сделки")
    keyboard.row("🚚 Предложения логистов")
    keyboard.row("👤 Профиль")
    keyboard.add("📈 Цены на зерно", "📰 Новости рынка")
    return keyboard


def exporter_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(KeyboardButton("➕ Создать пул"), KeyboardButton("📋 Мои пулы"))
    keyboard.add(
        KeyboardButton("🚚 Заявка на логистику"),  # ← НОВАЯ КНОПКА
        KeyboardButton("📋 Мои сделки"),
    )
    keyboard.add(KeyboardButton("🔍 Найти партии"), KeyboardButton("👤 Профиль"))
    keyboard.add(KeyboardButton("📈 Цены на зерно"), KeyboardButton("📰 Новости рынка"))
    return keyboard


def logistic_keyboard():
    """Клавиатура логиста"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🚚 Активные заявки"), KeyboardButton("💼 Мои предложения")
    )
    keyboard.add(
        KeyboardButton("🚛 Мои доставки"), KeyboardButton("🚛 База экспедиторов")
    )
    keyboard.add(KeyboardButton("📈 Цены на зерно"), KeyboardButton("📰 Новости рынка"))
    keyboard.add(KeyboardButton("👤 Профиль"))
    return keyboard


def expeditor_keyboard():
    """Клавиатура экспедитора"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🚚 Доступные заявки"), KeyboardButton("🚛 Мои доставки")
    )
    keyboard.add(
        KeyboardButton("✔️ История доставок"), KeyboardButton("💳 Моя карточка")
    )
    keyboard.add(KeyboardButton("📈 Цены на зерно"), KeyboardButton("📰 Новости рынка"))
    keyboard.add(KeyboardButton("👤 Профиль"))
    return keyboard


def vehicle_type_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚛 Автомобиль", callback_data="vehicle:truck"),
        InlineKeyboardButton("🚂 Ж/д", callback_data="vehicle:train"),
    )
    keyboard.add(
        InlineKeyboardButton("🚢 Судно", callback_data="vehicle:ship"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel"),
    )
    return keyboard


def expeditor_service_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📄 Документы", callback_data="service:docs"),
        InlineKeyboardButton("🏢 Таможня", callback_data="service:customs"),
    )
    keyboard.add(
        InlineKeyboardButton("🚢 Фрахт", callback_data="service:freight"),
        InlineKeyboardButton("📦 Полный сервис", callback_data="service:full"),
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    return keyboard


@dp.callback_query_handler(
    lambda c: c.data.startswith("expeditor_view_deal:"), state="*"
)
async def expeditor_view_deal_details(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей сделки"""
    await state.finish()

    try:
        deal_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return

    msg = f"📋 <b>Сделка #{deal_id}</b>\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Взять в работу", callback_data=f"expeditor_take:{deal_id}"
        ),
        InlineKeyboardButton("◀️ Назад", callback_data="expeditor_available_deals"),
    )

    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("expeditor_take:"), state="*")
async def expeditor_take_deal(callback: types.CallbackQuery, state: FSMContext):
    """Взять сделку в работу"""
    await state.finish()

    user_id = callback.from_user.id

    try:
        deal_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return

    if deal.get("expeditor_id"):
        await callback.answer(
            "❌ Сделка уже взята другим экспедитором", show_alert=True
        )
        return

    # Назначаем экспедитора
    deal["expeditor_id"] = user_id
    deal["status"] = "in_progress"

    save_deals_to_pickle()

    await callback.answer("✅ Сделка взята в работу!", show_alert=True)

    await callback.message.edit_text(
        f"✅ <b>Сделка #{deal_id} взята в работу!</b>\n\n"
        parse_mode="HTML",
    )

    # Уведомляем экспортёра
    try:
        await bot.send_message(
            deal["exporter_id"],
            f"✅ <b>Сделка #{deal_id} взята в работу!</b>\n\n"
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"Ошибка уведомления экспортёра: {e}")


    return keyboard


def adminkeyboard():
    """Алиас для совместимости"""
    return admin_keyboard()


def format_admin_statistics():
    total_users = len(users)



    total_requests = len(shipping_requests)
    )

    msg = "📊 <b>Статистика бота</b>\n\n"
    msg += "👥 <b>Пользователи:</b>\n"
    msg += f"• Всего: {total_users}\n"

    msg += f"• Всего: {total_pulls}\n"
    msg += f"• Активные: {active_pulls}\n\n"

    msg += f"• Всего: {total_batches}\n\n"

    msg += "🚚 <b>Заявки на логистику:</b>\n"
    msg += f"• Всего: {total_requests}\n"

    return msg


@dp.message_handler(commands=["reset"], state="*")
async def reset_account(message: types.Message, state: FSMContext):
    """Удалить свой аккаунт для повторной регистрации"""
    user_id = message.from_user.id

    # Очищаем состояние FSM
    await state.finish()

    # Создаём клавиатуру подтверждения
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, удалить", callback_data=f"confirm_reset:{user_id}"
        ),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_reset"),
    )

    await message.answer(
        "⚠️ *Удаление аккаунта*\n\n"
        "Вы уверены что хотите удалить свой аккаунт?\n\n"
        "Это действие удалит:\n"
        "• Ваш профиль\n"
        "• Все ваши партии (для фермера)\n"
        "• Все ваши пулы (для экспортёра)\n"
        "• Данные из памяти и Google Sheets\n\n"
        "⚠️ *Это действие необратимо!*",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


@dp.callback_query_handler(lambda c: c.data.startswith("confirm_reset:"), state="*")
async def confirm_reset_account(callback: CallbackQuery, state: FSMContext):
    user_id = parse_callback_id(callback.data)
        await callback.answer("❌ Это не ваш аккаунт", show_alert=True)
        return

    deleted_items = []

    # 1. Удаляем пользователя из памяти
    if user_id in users:
        del users[user_id]
        deleted_items.append(f"профиль ({role})")
        logging.info(f"✅ Удалён user {user_id} из памяти")

    user_batches = batches.pop(user_id, [])
    batch_count = len(user_batches)
    if batch_count > 0:
        deleted_items.append(f"{batch_count} партий")
        logging.info(f"✅ Удалено {batch_count} партий фермера {user_id}")

            for pull in pulls["pulls"].values():
                pull["batch_ids"] = [
                    bid
                    for bid in pull.get("batch_ids", [])
                    if bid not in batch_ids_to_delete
                ]
                pull["batches"] = [
                    bid
                    for bid in pull.get("batches", [])
                    if bid not in batch_ids_to_delete
                ]
                pull["farmer_ids"] = [
                ]
                    for b in pull.get("batches_data", [])
                    if b.get("id") not in batch_ids_to_delete
                    if pull.get("batches_data")
                    else 0
                )
                    pull["status"] = "active"

        logging.info(
        )

        try:
            cell = worksheet.find(str(user_id))
            if cell:
                worksheet.delete_rows(cell.row)
                deleted_items.append("запись в Google Sheets (Users)")
                logging.info(f"✅ Удалён аккаунт {user_id} из Google Sheets (Users)")
        except Exception as e:
            logging.error(f"❌ Ошибка удаления из Google Sheets (Users): {e}")

        try:
            all_values = worksheet.get_all_values()
            rows_to_delete = []
            for i, row in enumerate(all_values[1:], start=2):
                    rows_to_delete.append(i)
            for row_num in reversed(rows_to_delete):
                worksheet.delete_rows(row_num)
            if rows_to_delete:
                deleted_items.append(f"{len(rows_to_delete)} партий из Google Sheets")
        except Exception as e:
            logging.error(f"❌ Ошибка удаления партий из Google Sheets: {e}")

        try:
            all_values = worksheet.get_all_values()
            rows_to_delete = []
            for i, row in enumerate(all_values[1:], start=2):
                    rows_to_delete.append(i)
            for row_num in reversed(rows_to_delete):
                worksheet.delete_rows(row_num)
            if rows_to_delete:
                deleted_items.append(f"{len(rows_to_delete)} пуллов из Google Sheets")
        except Exception as e:
            logging.error(f"❌ Ошибка удаления пуллов из Google Sheets: {e}")

    if deleted_items:
        items_text = "\n".join([f"• {item}" for item in deleted_items])
        result_msg = (
            f"Удалено:\n{items_text}\n\n"
        )
    else:
        result_msg = "⚠️ Аккаунт не найден или уже удалён"

    await callback.message.edit_text(result_msg, parse_mode="Markdown")
    await callback.answer("✅ Аккаунт удалён")


@dp.callback_query_handler(lambda c: c.data == "cancel_reset", state="*")
async def cancel_reset_account(callback: CallbackQuery):
    """Отмена удаления аккаунта"""
    await callback.message.edit_text("❌ Удаление отменено")
    await callback.answer("Отменено")


def format_admin_analytics():
    for user in users.values():
        region = user.get("region", "Не указан")





    msg = "📈 <b>Аналитика бота</b>\n\n"

    msg += "🗺 <b>Топ-5 регионов:</b>\n"

    msg += "\n🌾 <b>Топ-5 культур:</b>\n"


    return msg


def format_admin_users():
    """Форматирование списка пользователей для админа"""
    farmers = [u for u in users.values() if u.get("role") == "farmer"]
    exporters = [u for u in users.values() if u.get("role") == "exporter"]

    msg = "👥 <b>Пользователи системы</b>\n\n"
    msg += f"Всего: {len(users)}\n\n"

    if farmers:
        msg += f"<b>🌾 Фермеры ({len(farmers)})</b>\n"
        for u in farmers[:5]:
            name = u.get("name", "Без имени")
            phone = u.get("phone", "Нет телефона")
            region = u.get("region", "Не указан")
            msg += f"  • {name}\n    📱 {phone}\n    📍 {region}\n"
        if len(farmers) > 5:
            msg += f"  ... и ещё {len(farmers) - 5}\n"
        msg += "\n"

    if exporters:
        msg += f"<b>🚢 Экспортёры ({len(exporters)})</b>\n"
        for u in exporters[:5]:
            name = u.get("name", "Без имени")
            phone = u.get("phone", "Нет телефона")
            msg += f"  • {name}\n    📱 {phone}\n"
        if len(exporters) > 5:
            msg += f"  ... и ещё {len(exporters) - 5}\n"
        msg += "\n"

    if logistics:
        msg += f"<b>🚚 Логисты ({len(logistics)})</b>\n"
        for u in logistics[:5]:
            name = u.get("name", "Без имени")
            phone = u.get("phone", "Нет телефона")
            msg += f"  • {name}\n    📱 {phone}\n"
        if len(logistics) > 5:
            msg += f"  ... и ещё {len(logistics) - 5}\n"
        msg += "\n"

    if expeditors:
        msg += f"<b>📋 Экспедиторы ({len(expeditors)})</b>\n"
        for u in expeditors[:3]:
            name = u.get("name", "Без имени")
            phone = u.get("phone", "Нет телефона")
            msg += f"  • {name}\n    📱 {phone}\n"
        if len(expeditors) > 3:
            msg += f"  ... и ещё {len(expeditors) - 3}\n"

    return msg


def join_pull_keyboard(pull_id):
    """Клавиатура для присоединения к пулу"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_pools_list"),
    )
    return keyboard


def get_pull_details_keyboard(pull_id, user_id, pull):
    """Создание клавиатуры для карточки пула"""
    keyboard = InlineKeyboardMarkup(row_width=2)

        keyboard.add(
            InlineKeyboardButton(
                "👥 Участники", callback_data=f"viewparticipants:{pull_id}"
            ),
            InlineKeyboardButton(
                "✏️ Редактировать", callback_data=f"editpull_{pull_id}"
            ),
        )
        keyboard.add(
            InlineKeyboardButton(
            ),
            InlineKeyboardButton("❌ Удалить", callback_data=f"deletepull_{pull_id}"),
        )

        keyboard.add(
            InlineKeyboardButton(
                "✅ Присоединиться", callback_data=f"join_pull:{pull_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton(
                "👥 Участники", callback_data=f"viewparticipants:{pull_id}"
            )
        )

    else:
        keyboard.add(
            InlineKeyboardButton(
                "👥 Участники", callback_data=f"viewparticipants:{pull_id}"
            )
        )

    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_pools"))

    return keyboard


def logistics_offer_keyboard():
    """Клавиатура для логистических услуг"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚛 Фура", callback_data="transport_type:truck"),
        InlineKeyboardButton("🚂 Ж/Д", callback_data="transport_type:train"),
        InlineKeyboardButton("🚢 Судно", callback_data="transport_type:ship"),
    )
    return keyboard


def admin_broadcast_keyboard():
    """Клавиатура подтверждения рассылки"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Отправить", callback_data="broadcast_confirm"),
        InlineKeyboardButton("❌ Отменить", callback_data="broadcast_cancel"),
    )
    return keyboard


def culture_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    crops = ["Пшеница", "Ячмень", "Кукуруза", "Подсолнечник", "Рапс", "Соя"]
    buttons = [
        InlineKeyboardButton(crop, callback_data=f"culture:{crop}") for crop in crops
    ]
    keyboard.add(*buttons)
    return keyboard


def region_keyboard():
    """Клавиатура выбора региона"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    regions = [
        "Астраханская область",
        "Краснодарский край",
        "Ставропольский край",
        "Ростовская область",
        "Волгоградская область",
        "Воронежская область",
        "Курская область",
        "Белгородская область",
        "Саратовская область",
        "Оренбургская область",
        "Алтайский край",
        "Омская область",
        "Новосибирская область",
    ]
    for region in regions:
        keyboard.add(InlineKeyboardButton(region, callback_data=f"region:{region}"))
    return keyboard


def port_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "ПКФ «Волга-Порт»",
        "ПКФ «Юг-Тер»",
        "ПАО «Астр.Порт»",
        "Универ.Порт",
        "Юж.Порт",
        "Агрофуд",
        "Моспорт",
        "ЦГП",
        "АМП",
        "Армада",
        "Стрелец",
        "Альфа",
    ]

    buttons = [
        InlineKeyboardButton(port, callback_data=f"selectport_{port}") for port in ports
    ]
    keyboard.add(*buttons)
    return keyboard


def quality_class_keyboard():
    """Клавиатура выбора класса качества"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    for quality_class in QUALITY_CLASSES:
        keyboard.add(
            InlineKeyboardButton(
                quality_class, callback_data=f"quality:{quality_class}"
            )
        )
    return keyboard


def storage_type_keyboard():
    """Клавиатура выбора типа хранения"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    for storage_type in STORAGE_TYPES:
        keyboard.add(
            InlineKeyboardButton(storage_type, callback_data=f"storage:{storage_type}")
        )
    return keyboard


def confirm_keyboard(action="confirm"):
    """Клавиатура подтверждения действия"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да", callback_data=f"confirm:{action}"),
        InlineKeyboardButton("❌ Нет", callback_data=f"cancel:{action}"),
    )
    return keyboard


def batch_actions_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_batch:{batch_id}"),
        InlineKeyboardButton("🗑 Удалить", callback_data=f"delete_batch:{batch_id}"),
    )
    keyboard.add(
        InlineKeyboardButton(
            "📎 Прикрепить файлы", callback_data=f"attach_files:{batch_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton(
            "📄 Просмотр файлов", callback_data=f"view_files:{batch_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton(
        )
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_batches"))
    return keyboard


def edit_batch_fields_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "🌾 Культура", callback_data=f"editfield_crop_{batch_id}"
        )  # ← ДОБАВЬ ЭТО
    )
    keyboard.add(
        InlineKeyboardButton("💰 Цена", callback_data="edit_field:price"),
        InlineKeyboardButton("📦 Объём", callback_data="edit_field:volume"),
    )
    keyboard.add(
        InlineKeyboardButton("💧 Влажность", callback_data="edit_field:humidity"),
        InlineKeyboardButton("🌾 Сорность", callback_data="edit_field:impurity"),
    )
    keyboard.add(
        InlineKeyboardButton(
            "⭐ Класс качества", callback_data="edit_field:quality_class"
        ),
        InlineKeyboardButton(
            "🏭 Тип хранения", callback_data="edit_field:storage_type"
        ),
    )
    keyboard.add(
        InlineKeyboardButton(
            "📅 Дата готовности", callback_data="edit_field:readiness_date"
        ),
        InlineKeyboardButton("📊 Статус", callback_data="edit_field:status"),
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel"))
    return keyboard


def status_keyboard():
    """Клавиатура выбора статуса партии"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    statuses = ["Активна", "Зарезервирована", "Продана", "Снята с продажи"]
    for status in statuses:
        keyboard.add(InlineKeyboardButton(status, callback_data=f"status:{status}"))
    return keyboard


def profile_edit_keyboard():
    """Клавиатура редактирования профиля"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📱 Телефон", callback_data="edit_profile:phone"),
        InlineKeyboardButton("📧 Email", callback_data="edit_profile:email"),
    )
    keyboard.add(
        InlineKeyboardButton("📍 Регион", callback_data="edit_profile:region"),
        InlineKeyboardButton(
            "🏢 Реквизиты", callback_data="edit_profile:company_details"
        ),
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel"))
    return keyboard


def search_criteria_keyboard():
    """Клавиатура выбора критериев поиска"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 По культуре", callback_data="search_by:culture"),
        InlineKeyboardButton("📍 По региону", callback_data="search_by:region"),
    )
    keyboard.add(
        InlineKeyboardButton(
            "🌾 Все доступные партии", callback_data="search_by:available"
        )
    )
    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    return keyboard


@dp.callback_query_handler(lambda c: c.data == "back_to_main", state="*")
async def back_to_main_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню из расширенного поиска"""
    await state.finish()

    user_id = callback.from_user.id

        await callback.message.answer(
            "❌ Пользователь не найден. Используйте /start для регистрации"
        )
        await callback.answer()
        return
    role = user.get("role", "unknown")
    name = user.get("name", "Пользователь")

    # Удаляем inline сообщение
    try:
        await callback.message.delete()
    except Exception as e:

    # Отправляем новое сообщение с ReplyKeyboard для роли
    if role == "farmer":
        keyboard = farmer_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n🌾 <b>Меню фермера</b>"
    elif role == "exporter":
        keyboard = exporter_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n📦 <b>Меню экспортёра</b>"
        keyboard = logistic_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n🚚 <b>Меню логиста</b>"
        keyboard = expeditor_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n🏭 <b>Меню экспедитора</b>"
    else:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        welcome_text = f"👋 С возвращением, {name}!"

    await callback.message.answer(
        welcome_text, reply_markup=keyboard, parse_mode="HTML"
    )

    await callback.answer()


def deal_actions_keyboard(deal_id):
    """Клавиатура действий со сделкой"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "📞 Контакты партнёра", callback_data=f"contact_partner:{deal_id}"
        ),
        InlineKeyboardButton("🚚 Логистика", callback_data=f"logistics:{deal_id}"),
    )
    keyboard.add(
        InlineKeyboardButton(
            "✅ Завершить сделку", callback_data=f"complete_deal:{deal_id}"
        ),
        InlineKeyboardButton(
            "❌ Отменить сделку", callback_data=f"cancel_deal:{deal_id}"
        ),
    )
    keyboard.add(
        InlineKeyboardButton("🔙 К списку сделок", callback_data="back_to_deals")
    )
    return keyboard


def format_news_message():
    """Форматирование сообщения с новостями"""
    if not news_cache or not news_cache.get("data"):
        return (
            "📰 <b>Новости рынка</b>\n\n"
            "⚠️ Новости ещё не загружены.\n"
            "Попробуйте позже или используйте /start для обновления."
        )

    news_list = news_cache["data"]
    updated_time = (
        news_cache["updated"].strftime("%d.%m.%Y %H:%M")
        if news_cache.get("updated")
        else "Неизвестно"
    )

    if not news_list:
        return (
            "📰 <b>Новости рынка</b>\n\n"
            "🤷‍♂️ Новостей не найдено.\n"
            "Попробуйте позже."
        )

    message = "📰 <b>Последние новости рынка</b>\n\n"

    for i, news_item in enumerate(news_list[:5], 1):
        title = news_item.get("title", "Без названия")
        link = news_item.get("link", "")
        date = news_item.get("date", "Неизвестно")

        if link:
            message += f"{i}. <a href='{link}'>{title}</a>\n"
        else:
            message += f"{i}. {title}\n"

        message += f"   📅 <i>{date}</i>\n\n"

    message += f"🕐 Обновлено: {updated_time}"

    return message


def format_prices_message():
    """✅ Форматирование сообщения с ценами - ТОЛЬКО РУБЛИ"""
    if not prices_cache or not prices_cache.get("data"):
        return (
            "📊 <b>Актуальные цены на зерно</b>\n\n"
            "⚠️ Данные ещё не загружены.\n"
            "Используйте /start для обновления."
        )

    updated_time = (
        prices_cache["updated"].strftime("%d.%m.%Y %H:%M")
        if prices_cache.get("updated")
        else "Неизвестно"
    )

    data = prices_cache["data"]
    russia = data.get("russia_south", {})
    fob = data.get("fob", 0)
    cbot = data.get("cbot", {})

    message = "📊 <b>Актуальные цены на зерно</b>\n\n"

    if russia:
        message += "🇷🇺 <b>Юг России</b>\n"
        for culture, price in russia.items():
            if isinstance(price, (int, float)):
                message += f"  • {culture}: <code>{price:,.0f} ₽/т</code>\n"
            else:
                message += f"  • {culture}: <code>{price}</code>\n"

    message += "\n🚢 <b>FOB Черное море</b>\n"
    if isinstance(fob, (int, float)):
        fob_rub = int(fob * 95)  # Курс ~95₽/₽
        message += f"  • Пшеница: <code>{fob_rub:,} ₽/т</code>\n"
    else:
        message += f"  • Пшеница: <code>{fob}</code>\n"

    if cbot:
        message += "\n🌎 <b>CBOT (США)</b>\n"
        for culture, price in cbot.items():
            if price:
                message += f"  • {culture}: <code>{price}</code>\n"

    message += f"\n🕐 Обновлено: {updated_time}"

    return message


def format_farmer_card(farmer_id, batch_id=None):
    """Форматирование полной карточки фермера с контактами"""
        return "❌ Фермер не найден"

    msg = f"👤 <b>Фермер: {farmer.get('name', 'Неизвестно')}</b>\n\n"
    msg += "<b>📞 Контакты:</b>\n"
    msg += f"📱 Телефон: <code>{farmer.get('phone', 'Не указан')}</code>\n"
    msg += f"📧 Email: <code>{farmer.get('email', 'Не указан')}</code>\n"
    msg += f"📍 Регион: {farmer.get('region', 'Не указан')}\n\n"

    if farmer.get("inn"):
        msg += "<b>🏢 Реквизиты:</b>\n"
        msg += f"ИНН: <code>{farmer.get('inn')}</code>\n"
        if farmer.get("company_details"):
            details = farmer["company_details"][:200]
            msg += (
                f"{details}...\n"
                if len(farmer["company_details"]) > 200
                else f"{details}\n"
            )
        msg += "\n"

    # ✅ ИСПРАВЛЕНО: безопасное обращение к полям партии
            if batch["id"] == batch_id:
                msg += f"<b>📦 Партия #{batch_id}:</b>\n"
                msg += f"🌾 Культура: {batch.get('culture', 'Не указано')}\n"
                msg += f"📦 Объём: {batch.get('volume', 0)} т\n"
                msg += f"💰 Цена: {batch.get('price', 0):,.0f} ₽/т\n"

                # ✅ КАЧЕСТВО - только если есть
                if "moisture" in batch or "nature" in batch:
                    msg += "\n<b>🔬 Качество:</b>\n"
                    if "nature" in batch:
                        msg += (
                            f"   🌾 Натура: {batch.get('nature', 'Не указано')} г/л\n"
                        )
                    if "moisture" in batch:
                        msg += f"   💧 Влажность: {batch['moisture']}%\n"
                    if "impurity" in batch:
                        msg += (
                            f"   🌿 Сорность: {batch.get('impurity', 'Не указано')}%\n"
                        )

                # ✅ СТАТУС
                msg += f"\n📊 Статус: {batch.get('status', 'Активна')}\n"
                break

    # ✅ СТАТИСТИКА
    if farmer_id in batches:
        total_batches = len(batches[farmer_id])
        active_batches = len(
        )

        msg += "\n<b>📊 Статистика:</b>\n"
        msg += f"Всего партий: {total_batches}\n"
        msg += f"Активных: {active_batches}\n"

    return msg


def get_role_keyboard(role):
    role = str(role).lower()
    if role in ["farmer", "фермер"]:
        return farmer_keyboard()
    elif role in ["exporter", "экспортёр"]:
        return exporter_keyboard()
        return logistic_keyboard()
    elif role in ["expeditor", "экспедитор", "broker", "брокер"]:
        return expeditor_keyboard()
    else:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add("🏠 Главное меню", "ℹ️ Помощь")
        return keyboard


def determine_quality_class(humidity, impurity):
    """Определение класса качества зерна"""
    if humidity <= 14 and impurity <= 1:
        return "1 класс"
    elif humidity <= 14.5 and impurity <= 2:
        return "2 класс"
    elif humidity <= 15 and impurity <= 3:
        return "3 класс"
    elif humidity <= 16 and impurity <= 5:
        return "4 класс"
    else:
        return "5 класс"


def validate_date(date_str):
    """Проверка формата даты ДД.ММ.ГГГГ"""
    try:
        datetime.strptime(date_str, "%d.%m.%Y")
        return True
        return False


async def find_matching_exporters(batch):
    """Поиск подходящих пулов экспортёров для партии фермера"""
    matching_pulls = []

    try:
        batch_culture = batch.get("culture", "").strip()
        batch_volume = batch.get("volume", 0)

        if not batch_culture or batch_volume <= 0:
            logging.warning(
                f"Некорректные данные партии: culture={batch_culture}, volume={batch_volume}"
            )
            return []

        all_pulls = pulls.get("pulls", {})  # ✅ ДОБАВЛЕНО

        for pull_id, pull in all_pulls.items():  # ✅ ИСПРАВЛЕНО
            if not isinstance(pull, dict):  # ✅ ДОБАВЛЕНО
                continue

            pull_culture = pull.get("culture", "").strip()
            pull_current_volume = pull.get("current_volume", 0)
            pull_target_volume = pull.get("target_volume", 0)

            if (
                pull_culture.lower() == batch_culture.lower()
                and pull_current_volume < pull_target_volume
            ):

                free_space = pull_target_volume - pull_current_volume

                if free_space > 0:
                    exporter_id = pull.get("exporter_id")

                    matching_pulls.append(
                        {
                            "pull_id": pull_id,
                            "pull": pull,
                            "exporter": exporter,
                            "exporter_id": exporter_id,
                            "exporter_name": exporter.get("name", "Неизвестно"),
                            "exporter_company": exporter.get("company", "Неизвестно"),
                            "exporter_phone": exporter.get("phone", "Не указан"),
                            "culture": pull_culture,
                            "price": pull.get("price", 0),
                            "port": pull.get("port", "Не указан"),
                            "free_space": free_space,
                            "current_volume": pull_current_volume,
                            "target_volume": pull_target_volume,
                        }
                    )

        if matching_pulls:
            logging.info(
                f"✅ Найдено {len(matching_pulls)} пулов для партии {batch.get('id')}"
            )
        else:
            logging.info(f"❌ Пулов для {batch_culture} не найдено")

        return matching_pulls

    except Exception as e:
        logging.error(f"❌ Ошибка find_matching_exporters: {e}")
        return []


async def create_match_notification(batch_id, pull_id):
    """Создание уведомления о совпадении"""
    global match_counter
    match_counter += 1

    match_data = {
        "id": match_counter,
        "batch_id": batch_id,
        "pull_id": pull_id,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "active",
    }

    matches[match_counter] = match_data
    return match_counter


async def notify_match(farmer_id, batch, matching_pulls, extra=None, *args, **kwargs):
    """Уведомление фермеру о найденных совпадениях с контактами экспортёра"""
    try:
        if not matching_pulls:
            return

        batch_culture = batch.get("culture", "Неизвестно")
        batch_volume = batch.get("volume", 0)

        text = "🎯 <b>Найдены подходящие пулы!</b>\n\n"
        text += f"📦 <b>Ваша партия:</b> {batch_culture} - {batch_volume} т\n"
        text += f"🔍 <b>Найдено пулов:</b> {len(matching_pulls)}\n\n"

        kb = InlineKeyboardMarkup(row_width=1)

        for idx, pull_data in enumerate(matching_pulls[:5], 1):
            # ✅ Получаем ID пула
            pull_id = pull_data.get("id") or pull_data.get("pull_id", "?")

            # ✅ Получаем ID экспортёра
            exporter_id = pull_data.get("exporter_id")

            # ✅ Получаем данные экспортёра из базы users
            exporter_name = pull_data.get("exporter_name", "Не указано")
            company_name = exporter_info.get("company_name", exporter_name)
            phone = exporter_info.get("phone", "Не указан")
            email = exporter_info.get("email", "Не указан")

            # ✅ Данные пула
            price = pull_data.get("price", 0)
            port = pull_data.get("port", "Не указан")
            current_volume = pull_data.get("current_volume", 0)
            target_volume = pull_data.get("target_volume", 0)
            doc_type = pull_data.get("doc_type", "Не указан")

            # Добавляем кнопку
            unique_id = int(time.time() * 1000) + idx
            kb.add(
                InlineKeyboardButton(
                    f"🔗 Присоединиться к Пулу #{pull_id}",
                    callback_data=f"join_pull:{pull_id}:{unique_id}",
                )
            )

            # Формируем текст
            text += f"<b>{idx}. Пул #{pull_id}</b>\n"
            text += f"🏢 <b>Компания:</b> {company_name}\n"
            text += f"👤 <b>Экспортёр:</b> {exporter_name}\n"
            text += f"📞 <b>Телефон:</b> <code>{phone}</code>\n"
            text += f"📧 <b>Email:</b> {email}\n"
            text += f"💰 <b>Цена:</b> {price:,.0f} ₽/т\n"
            text += f"🚢 <b>Порт:</b> {port}\n"
            text += f"📋 <b>Условия:</b> {doc_type}\n"
            text += f"📊 <b>Заполнено:</b> {current_volume}/{target_volume} т\n"
            text += "\n"

        text += "💡 <i>Свяжитесь с экспортёром для обсуждения условий!</i>"

        logging.info(f"🔄 Отправляю уведомление фермеру {farmer_id}...")
        logging.info(f"📝 Текст сообщения ({len(text)} символов): {text[:200]}...")
        logging.info(f"🔘 Кнопок: {len(kb.inline_keyboard)}")

        await bot.send_message(farmer_id, text, parse_mode="HTML", reply_markup=kb)
        logging.info(f"✅ Уведомление фермеру {farmer_id} УСПЕШНО отправлено!")

    except Exception as e:
        logging.error(f"❌ Ошибка уведомления фермеру {farmer_id}: {e}", exc_info=True)


async def auto_match_batches_and_pulls():
    """
    Ищет совпадения партий и пулов, обновляет статусы и отправляет уведомления.
    ТОЛЬКО ФЕРМЕРЫ получают уведомления о совпадениях!
    """
    try:
        logging.info("🔄 Запуск автоматического поиска совпадений...")
        if "pulls" not in pulls or not isinstance(pulls["pulls"], dict):
            logging.warning(
                "⚠️ Структура pulls некорректна или ключ 'pulls' отсутствует"
            )
            return 0

        matching_count = 0

        for pull_id, pull_data in pulls["pulls"].items():
                continue

            # Проверяем статус пула и культуру
            pull_culture = pull_data.get("culture", "").lower().strip()

            if pull_status == "filled":
                continue  # Пропускаем полностью заполненные пулы


                # ✅ ПРОВЕРКА РОЛИ: Только фермеры получают уведомления
                if farmer_id not in users:
                    logging.debug(f"⚠️ Пользователь {farmer_id} не найден в базе")
                    continue

                if user_role != "farmer":
                    logging.debug(
                        f"⚠️ Пользователь {farmer_id} (партия #{batch_id}) имеет роль '{user_role}', а не 'farmer'. Пропускаем."
                    )
                    continue

                batch_culture = batch.get("culture", "").lower().strip()
                batch_status = batch.get("status", "").lower()

                # Логика совпадения по культуре и статусу партии
                if batch_culture == pull_culture and batch_status in [
                    "активна",
                    "active",
                    "available",
                ]:
                    logging.info(
                        f"  Фермер {farmer_id} (роль: {user_role}): Партия #{batch_id} ({batch_culture})\n"
                        f"  Пул #{pull_id} ({pull_culture})"
                    )

                    try:
                        # Отправляем уведомление только фермеру
                        await notify_match(farmer_id, batch, [pull_data])
                        logging.info(f"✅ Уведомление отправлено фермеру {farmer_id}")
                    except Exception as e:
                        logging.error(
                            f"❌ Ошибка отправки уведомления фермеру {farmer_id}: {e}"
                        )

                    # Обновляем статусы в партиях и пулах
                    batch["status"] = "matched"
                    pull_data["status"] = "processing"
                    pulls["pulls"][pull_id] = pull_data

                    matching_count += 1

        if matching_count > 0:
            try:
                save_pulls_to_pickle()
                save_batches_to_pickle()
                logging.info(
                    f"✅ Найдено {matching_count} совпадений, данные сохранены"
                )
            except Exception as e:
                logging.error(f"❌ Ошибка сохранения данных: {e}")
        else:
            logging.info("ℹ️ Совпадений не найдено")

        return matching_count

    except Exception as e:
        logging.error(f"❌ Ошибка в auto_match_batches_and_pulls: {e}", exc_info=True)
        return 0


# ============================================================================
# ОБРАБОТЧИК КОМАНДЫ /start
# ============================================================================
# Глобальный словарь для защиты от дублирования
last_start_times = {}


@dp.message_handler(commands=["start"], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
    user_id = message.from_user.id

    # ✅ СРАЗУ УДАЛЯЕМ КОМАНДУ /start
    try:
        await message.delete()

    # ✅ ПРОВЕРКА 1: Если идёт регистрация, игнорируем
    current_state = await state.get_state()
    if current_state and "Registration" in current_state:
        return

    await state.finish()

    # ✅ ПРОВЕРКА 2: Защита от дублирования
    import time

    current_time = time.time()

    if user_id in last_start_times:
        time_diff = current_time - last_start_times[user_id]
        if time_diff < 3:
            return

    last_start_times[user_id] = current_time
    logging.info(f"🚀 /start от пользователя {user_id}")

    if user_id in users:
        # Зарегистрированный пользователь
        role = user.get("role", "unknown")
        name = user.get("name", "Пользователь")

        logging.info(f"✅ Пользователь {user_id} уже зарегистрирован как {role}")

        welcome_text = f"👋 Добро пожаловать, {name}!\n\nВыберите действие:"

        if role == "farmer":
            keyboard = farmer_keyboard()
        elif role == "exporter":
            keyboard = exporter_keyboard()
            keyboard = logistic_keyboard()
            keyboard = expeditor_keyboard()
        elif role == "admin":
            keyboard = admin_keyboard()
        else:
            keyboard = ReplyKeyboardMarkup(resize_keyboard=True)

        await message.answer(welcome_text, reply_markup=keyboard, parse_mode="HTML")

    else:
        # Новый пользователь
        logging.info(f"👤 Новый пользователь {user_id} - показываем приветствие")

        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(KeyboardButton("📝 Зарегистрироваться"))

        # ✅ ПРИВЕТСТВИЕ ОСТАЁТСЯ В ЧАТЕ
        await message.answer(
            "🌾 <b>EXPORTUM</b>\n\n"
            "Платформа зернового рынка для:\n\n"
            "• 👨‍🌾 Фермеров — продажа партий зерна\n"
            "• 📦 Экспортёров — создание пуллов и закупка\n"
            "• 🚛 Логистов — предложение транспортных услуг\n"
            "• 📋 Экспедиторов — таможенное оформление\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📢 Канал: @EXPORTUM\n"
            "💬 Чат: @exportum_chat\n"
            "🤖 Бот: @exportumbot\n\n"
            "📊 Котировки и новости обновляются ежедневно!\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "👇 Нажмите кнопку ниже для регистрации:",
            parse_mode="HTML",
            reply_markup=keyboard,
        )


# ============================================================================
# ADMIN CALLBACK HANDLERS - ВЫСОКИЙ ПРИОРИТЕТ (ПЕРЕД ВСЕМИ ОСТАЛЬНЫМИ!)
# ============================================================================
    """Форматирование статистики"""
    total_users = len(users)

    total_pulls = len(pulls)


    total_requests = len(shipping_requests)
    active_requests = sum(
    )

    msg = "📊 <b>Статистика бота</b>\n\n"
    msg += "👥 <b>Пользователи:</b>\n"
    msg += f"• Всего: {total_users}\n"
    msg += f"• Фермеры: {farmers}\n"
    msg += f"• Экспортёры: {exporters}\n"
    msg += f"• Логисты: {logists}\n"
    msg += f"• Экспедиторы: {expeditors}\n\n"

    msg += "💼 <b>Пулы:</b>\n"
    msg += f"• Всего: {total_pulls}\n"
    msg += f"• Активные: {active_pulls}\n\n"

    msg += "🌾 <b>Партии:</b>\n"
    msg += f"• Всего: {total_batches}\n\n"

    msg += "🚚 <b>Заявки на логистику:</b>\n"
    msg += f"• Всего: {total_requests}\n"
    msg += f"• Активные: {active_requests}"

    return msg


    """Форматирование аналитики"""
    regions = {}
    for user in users.values():
        region = user.get("region", "Не указан")
        regions[region] = regions.get(region, 0) + 1

    top_regions = sorted(regions.items(), key=lambda x: x[1], reverse=True)[:5]

    cultures = {}
        culture = batch.get("culture", "Неизвестно")
        cultures[culture] = cultures.get(culture, 0) + 1

    top_cultures = sorted(cultures.items(), key=lambda x: x[1], reverse=True)[:5]

    pull_statuses = {}
        status = pull.get("status", "Неизвестно")
        pull_statuses[status] = pull_statuses.get(status, 0) + 1

    msg = "📈 <b>Аналитика бота</b>\n\n"

    msg += "🗺 <b>Топ-5 регионов:</b>\n"
    for i, (region, count) in enumerate(top_regions, 1):
        msg += f"{i}. {region}: {count} польз.\n"

    msg += "\n🌾 <b>Топ-5 культур:</b>\n"
    for i, (culture, count) in enumerate(top_cultures, 1):
        msg += f"{i}. {culture}: {count} партий\n"

    msg += "\n💼 <b>Статусы пулов:</b>\n"
    for status, count in pull_statuses.items():
        msg += f"{status_emoji} {status.capitalize()}: {count}\n"

    return msg


    """Форматирование списка пользователей"""
    if not users:
        return "❌ Нет пользователей"

    msg = "👥 <b>Пользователи системы</b>\n\n"

    roles = {
        "farmer": "🌾 Фермеры",
        "exporter": "💼 Экспортёры",
        "logistic": "🚚 Логисты",
        "expeditor": "⚓ Экспедиторы",
    }

    for role, title in roles.items():
        role_users = [u for u in users.values() if u.get("role") == role]
        if role_users:
            msg += f"{title}: {len(role_users)}\n"
            for user in role_users[:3]:
                name = user.get("name", "Без названия")
                phone = user.get("phone", "Нет телефона")
                msg += f"• {name} ({phone})\n"
            if len(role_users) > 3:
                msg += f"... и ещё {len(role_users) - 3}\n"
            msg += "\n"

    return msg



    current_state = await state.get_state()
    if current_state:
        logging.info(f"⚠️ Сбрасываем state: {current_state}")
        await state.finish()

    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return


        msg = format_admin_statistics()
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminstat"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"),
        )
        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer("✅ Статистика обновлена")

        msg = format_admin_analytics()
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminanalytics"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"),
        )
        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer("✅ Аналитика обновлена")

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("👥 Пользователи", callback_data="exportusers"),
    )
    keyboard.add(
        InlineKeyboardButton("🌾 Партии", callback_data="exportbatches"),
        InlineKeyboardButton("📋 Заявки", callback_data="exportrequests"),
    )
    keyboard.add(
    )

    await callback.message.edit_text(
        "📤 <b>Экспорт данных</b>\n\nВыберите данные для экспорта:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()

        msg = format_admin_users()
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminusers"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"),
        )
        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"))

    await callback.message.edit_text(
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()



    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"))

        await callback.message.edit_text(
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except Exception as e:



    await state.finish()

    if callback.from_user.id != ADMIN_ID:
        return



# ============================================================================
# АВТОМАТИЧЕСКОЕ ЗАКРЫТИЕ ПУЛА И СОЗДАНИЕ СДЕЛКИ
# ============================================================================
def check_and_close_pull_if_full(pull_id):
    """Проверяет заполненность пула и закрывает его при 100%"""
        return False
    current = pull.get("current_volume", 0)
    target = pull.get("target_volume", 0)

        pull["closed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        deal_id = create_deal_from_full_pull(pull)
        pull["deal_id"] = deal_id

        save_pulls_to_pickle()
        logging.info(f"✅ Pull {pull_id} auto-closed → Deal {deal_id}")

        # Запустить массовое уведомление всех логистов и участников
        asyncio.create_task(notify_all_about_pull_closure(pull, deal_id))
        return True

    return False


def create_deal_from_full_pull(pull):
    """Создаёт сделку из заполненного пула"""
    global deal_counter

    deal_counter += 1

    farmer_ids = []
    batch_details = []

    for participant in pull.get("participants", []):
        f_id = participant.get("farmer_id")
        b_id = participant.get("batch_id")
        volume = participant.get("volume", 0)

        if f_id and f_id not in farmer_ids:
            farmer_ids.append(f_id)

        batch_details.append(
            {
                "farmer_id": f_id,
                "batch_id": b_id,
                "volume": volume,
            }
        )

    deal = {
        "id": deal_counter,
        "pull_id": pull["id"],
        "type": "pool_deal",
        "exporter_id": pull["exporter_id"],
        "exporter_name": pull["exporter_name"],
        "farmer_ids": farmer_ids,
        "batches": batch_details,
        "logistic_id": None,
        "expeditor_id": None,
        "culture": pull["culture"],
        "volume": pull["current_volume"],
        "price": pull["price"],
        "total_sum": pull["current_volume"] * pull["price"],
        "port": pull["port"],
        "quality": {
            "moisture": pull.get("moisture", 0),
            "nature": pull.get("nature", 0),
            "impurity": pull.get("impurity", 0),
            "weed": pull.get("weed", 0),
        },
        "documents": pull.get("documents", ""),
        "doc_type": pull.get("doc_type", "FOB"),
        "status": "new",
        "payment_status": "pending",
        "delivery_status": "pending",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    deals[deal_counter] = deal
    save_deals_to_pickle()
    logging.info(f"✅ Deal {deal_counter} created from pull {pull['id']}")
    return deal_counter


async def notify_all_about_pull_closure(pull, deal_id):
    """
    Массовое уведомление всем логистам и фермерам-участникам о закрытии или сборе пула.
    """
    pull_id = pull["id"]
    exporter_id = pull["exporter_id"]
    port = pull.get("port", "Не указан")


    all_notify_ids = set(farmer_ids) | set(logist_ids)

    notify_text = (
        f"🔒 <b>ПУЛ #{pull_id} СОБРАН/ЗАКРЫТ!</b>\n\n"
        f"🌾 Культура: {pull.get('culture')}\n"
        f"🎯 Объём: {pull.get('current_volume')} / {pull.get('target_volume')} т\n"
        f"🏢 Порт: {port}\n"
        f"💰 Цена: {pull.get('price', 0):,.0f} ₽/т\n"
        f"✅ Сделка #{deal_id} создана\n"
    )

    sent_count = 0
    failed_ids = []
        try:
            await bot.send_message(notify_id, notify_text, parse_mode="HTML")
            sent_count += 1
        except Exception as e:
            logging.error(
                f"[NOTIFY ERROR] Ошибка при уведомлении user_id={notify_id}: {e}"
            )
            failed_ids.append(notify_id)

    logging.info(
        f"{len(failed_ids)} не отправлено: {failed_ids}"
    )

    exporter_text = (
        f"📦 Культура: {pull.get('culture')}\n"
        f"🎯 Объём: {pull.get('current_volume')} / {pull.get('target_volume')} т\n"
        f"🏢 Порт: {port}\n"
    )

    try:
            InlineKeyboardButton(
            )
        )
        await bot.send_message(
        )

    if expeditors:
        )
                    InlineKeyboardButton(
                    )
                )
                await bot.send_message(
                )
            except Exception as e:

    logging.info(f"✅ Mass notifications sent for pull {pull_id}, deal {deal_id}")


async def export_callbacks_router(callback: types.CallbackQuery, state: FSMContext):
    """Роутер для всех export callback handlers"""

    # Сбрасываем state
    current_state = await state.get_state()
    if current_state:
        logging.info(f"⚠️ Сбрасываем state: {current_state}")
        await state.finish()

    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    data = callback.data
    logging.info(f"📤 Export callback: {data} from {callback.from_user.id}")

    await callback.answer("⏳ Экспортирую данные...")

    try:
        import pandas as pd
        from datetime import datetime
        import os

        if data == "exportusers":
            users_data = []
            for uid, user in users.items():
                users_data.append(
                    {
                        "ID": uid,
                        "Имя": user.get("name", ""),
                        "Роль": user.get("role", ""),
                        "Телефон": user.get("phone", ""),
                        "Регион": user.get("region", ""),
                        "ИНН": user.get("inn", ""),
                        "Дата": user.get("registered_at", ""),
                    }
                )

            df = pd.DataFrame(users_data)
            filename = f'users_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False, engine="openpyxl")

            with open(filename, "rb") as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт пользователей\n\nВсего: {len(users_data)}",
                )

            os.remove(filename)

        elif data == "exportpulls":
            pulls_data = []
            all_pulls = pulls.get("pulls", {})  # ✅ ИСПРАВЛЕНО

            for pull_id, pull in all_pulls.items():  # ✅ ИСПРАВЛЕНО
                if not isinstance(pull, dict):  # ✅ ДОБАВЛЕНО
                    continue

                pulls_data.append(
                    {
                        "ID": pull_id,
                        "Культура": pull.get("culture", ""),
                        "Объём": pull.get("current_volume", 0),
                        "Цена": pull.get("price", 0),
                        "Порт": pull.get("port", ""),
                        "Статус": pull.get("status", ""),
                        "Экспортёр": pull.get("exporter_name", ""),
                    }
                )

            df = pd.DataFrame(pulls_data)
            filename = f'pulls_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False, engine="openpyxl")

            with open(filename, "rb") as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт пуллов\n\nВсего: {len(pulls_data)}",
                )

            os.remove(filename)

        elif data == "exportbatches":
            batches_data = []
            for farmer_id, user_batches in batches.items():
                for batch in user_batches:
                    batches_data.append(
                        {
                            "ID": batch.get("id", ""),
                            "Фермер": farmer_name,
                            "Культура": batch.get("culture", ""),
                            "Объём": batch.get("volume", 0),
                            "Цена": batch.get("price", 0),
                            "Регион": batch.get("region", ""),
                            "Статус": batch.get("status", ""),
                        }
                    )

            df = pd.DataFrame(batches_data)
            filename = f'batches_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False, engine="openpyxl")

            with open(filename, "rb") as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт партий\n\nВсего: {len(batches_data)}",
                )

            os.remove(filename)

        elif data == "exportrequests":
            requests_data = []
            for req_id, req in shipping_requests.items():
                requests_data.append(
                    {
                        "ID": req_id,
                        "От": req.get("from_city", ""),
                        "До": req.get("to_city", ""),
                        "Объём": req.get("volume", 0),
                        "Дата": req.get("loading_date", ""),
                        "Статус": req.get("status", ""),
                    }
                )

            df = pd.DataFrame(requests_data)
            filename = (
                f'requests_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            )
            df.to_excel(filename, index=False, engine="openpyxl")

            with open(filename, "rb") as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт заявок\n\nВсего: {len(requests_data)}",
                )

            os.remove(filename)

        elif data == "exportfull":
            import zipfile
            import io
            import json

            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(
                    "users.json", json.dumps(users, ensure_ascii=False, indent=2)
                )
                zip_file.writestr(
                    "pulls.json", json.dumps(pulls, ensure_ascii=False, indent=2)
                )
                zip_file.writestr(
                    "batches.json", json.dumps(batches, ensure_ascii=False, indent=2)
                )
                zip_file.writestr(
                    "deals.json", json.dumps(deals, ensure_ascii=False, indent=2)
                )
                zip_file.writestr(
                    "shipping_requests.json",
                    json.dumps(shipping_requests, ensure_ascii=False, indent=2),
                )

                backup_info = {
                    "created_at": datetime.now().isoformat(),
                    "total_users": len(users),
                    "total_pulls": len(pulls.get("pulls", {})),  # ✅ ИСПРАВЛЕНО
                }
                zip_file.writestr(
                    "backup_info.json",
                    json.dumps(backup_info, ensure_ascii=False, indent=2),
                )

            zip_buffer.seek(0)

            filename = f'exportum_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            await callback.message.answer_document(
                types.InputFile(zip_buffer, filename=filename),
                f"👥 Пользователей: {len(users)}\n"
                f"📦 Пуллов: {len(pulls.get('pulls', {}))}\n"  # ✅ ИСПРАВЛЕНО
            )

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="adminexport"))
        await callback.message.edit_reply_markup(reply_markup=keyboard)

    except Exception as e:
        logging.error(f"Ошибка экспорта: {e}")
        await callback.message.answer(f"❌ Ошибка: {e}")


@dp.message_handler(lambda m: m.text == "📝 Зарегистрироваться", state="*")
async def registration_entry(message: types.Message, state: FSMContext):
    """Обработчик кнопки регистрации"""
    await state.finish()
    user_id = message.from_user.id

    logging.info(f"📝 Пользователь {user_id} начинает регистрацию")

    # ✅ УДАЛЯЕМ ТОЛЬКО НАЖАТИЕ КНОПКИ
    try:
        await message.delete()

    # НАЧИНАЕМ РЕГИСТРАЦИЮ
    msg = await message.answer(
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.name.set()


@dp.callback_query_handler(lambda c: c.data == "start_registration", state="*")
async def start_registration(callback: types.CallbackQuery, state: FSMContext):
    """Начало регистрации через callback"""
    await callback.message.edit_text(
        "📝 <b>Регистрация</b>\n\n" "▓░░░░░░░ 1/8\n\n" "Введите ваше полное имя:",
        parse_mode="HTML",
    )

    # Сохраняем ID сообщения
    await state.update_data(last_bot_msg_id=callback.message.message_id)
    await RegistrationStatesGroup.name.set()
    await callback.answer()


# ========== ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ УДАЛЕНИЯ ==========
async def delete_previous_messages(message: types.Message, state: FSMContext):
    """Удаляет предыдущее сообщение бота и сообщение пользователя"""
    data = await state.get_data()
    last_msg_id = data.get("last_bot_msg_id")

    # Удаляем сообщение бота
    if last_msg_id:
        try:
            await bot.delete_message(message.chat.id, last_msg_id)
        except Exception as e:
            logging.debug(f"Не удалось удалить сообщение бота: {e}")

    # Удаляем сообщение пользователя
    try:
        await message.delete()
    except Exception as e:
        logging.debug(f"Не удалось удалить сообщение пользователя: {e}")


# ========== ШАГ 1: ИМЯ ==========
@dp.message_handler(state=RegistrationStatesGroup.name)
async def registration_name(message: types.Message, state: FSMContext):
    """Получение имени при регистрации"""
    name = message.text.strip()

    if len(name) < 2:
        await message.answer("❌ Имя слишком короткое. Введите полное имя:")
        return

    await state.update_data(name=name)

    # Удаляем предыдущие сообщения
    await delete_previous_messages(message, state)

    msg = await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "▓▓░░░░░░ 2/8\n\n"
        "📱 Введите ваш номер телефона\n\n"
        "Например: +79991234567",
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.phone.set()


# ========== ШАГ 2: ТЕЛЕФОН ==========
@dp.message_handler(state=RegistrationStatesGroup.phone)
async def registration_phone(message: types.Message, state: FSMContext):
    """Получение телефона при регистрации"""
    phone = message.text.strip()

    if len(phone) < 10 or not any(char.isdigit() for char in phone):
        await message.answer("❌ Некорректный номер телефона. Попробуйте ещё раз:")
        return

    await state.update_data(phone=phone)
    await delete_previous_messages(message, state)

    msg = await message.answer(
        "📝 <b>Регистрация</b>\n\n" "▓▓▓░░░░░ 3/8\n\n" "📧 Введите ваш email:",
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.email.set()


# ========== ШАГ 3: EMAIL ==========
@dp.message_handler(state=RegistrationStatesGroup.email)
async def registration_email(message: types.Message, state: FSMContext):
    """Получение email при регистрации"""
    email = message.text.strip()

    if "@" not in email or "." not in email.split("@")[-1]:
        await message.answer("❌ Некорректный email. Попробуйте ещё раз:")
        return

    await state.update_data(email=email)
    await delete_previous_messages(message, state)

    msg = await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "▓▓▓▓░░░░ 4/8\n\n"
        "🏢 Введите ИНН вашей компании\n\n"
        "ИНН должен состоять из 10 или 12 цифр",
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.inn.set()


# ========== ШАГ 4: ИНН ==========
@dp.message_handler(state=RegistrationStatesGroup.inn)
async def registration_inn(message: types.Message, state: FSMContext):
    """Получение ИНН при регистрации"""
    inn = message.text.strip()

    if not validate_inn(inn):
        await message.answer(
            "❌ Некорректный ИНН. Должно быть 10 или 12 цифр. Попробуйте ещё раз:"
        )
        return

    await state.update_data(inn=inn)
    await delete_previous_messages(message, state)

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("➡️ Пропустить ОГРН", callback_data="skip_ogrn"))

    msg = await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "▓▓▓▓▓░░░ 5/8\n\n"
        "📋 Введите ОГРН вашей компании\n\n"
        "ОГРН должен состоять из 15 цифр\n\n"
        "Или нажмите кнопку ниже:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.ogrn.set()


# ========== ШАГ 5: ОГРН ==========
@dp.message_handler(state=RegistrationStatesGroup.ogrn)
async def registration_ogrn(message: types.Message, state: FSMContext):
    """Получение ОГРН при регистрации"""
    ogrn = message.text.strip().replace(" ", "")

    if not ogrn.isdigit() or len(ogrn) != 15:
        await message.answer("❌ ОГРН должен содержать 15 цифр. Попробуйте ещё раз:")
        return

    await state.update_data(ogrn=ogrn)
    await delete_previous_messages(message, state)

    msg = await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "▓▓▓▓▓▓░░ 6/8\n\n"
        "📍 Введите юридический адрес компании\n\n"
        "Например: г. Краснодар, ул. Красная, д. 1, оф. 10",
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.company_details.set()


@dp.callback_query_handler(
    lambda c: c.data == "skip_ogrn", state=RegistrationStatesGroup.ogrn
)
async def skip_ogrn(callback: types.CallbackQuery, state: FSMContext):
    """Пропустить ОГРН"""
    await state.update_data(ogrn=None)

    # Удаляем сообщение с кнопкой
    try:
        await callback.message.delete()

    msg = await callback.message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "▓▓▓▓▓▓░░ 6/8\n\n"
        "📍 Введите юридический адрес компании\n\n"
        "Например: г. Краснодар, ул. Красная, д. 1, оф. 10",
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.company_details.set()
    await callback.answer()


# ========== ШАГ 6: ЮРИДИЧЕСКИЙ АДРЕС ==========
@dp.message_handler(state=RegistrationStatesGroup.company_details)
async def registration_company_details(message: types.Message, state: FSMContext):
    """Получение юридического адреса компании"""
    company_details = message.text.strip()

    if len(company_details) < 10:
        await message.answer(
            "❌ Слишком короткий адрес. Введите полный юридический адрес:"
        )
        return

    await state.update_data(company_details=company_details)
    await delete_previous_messages(message, state)

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("👨‍🌾 Фермер", callback_data="role:farmer"),
        InlineKeyboardButton("📦 Экспортёр", callback_data="role:exporter"),
    )
    keyboard.add(
        InlineKeyboardButton("🚚 Логист", callback_data="role:logistic"),
        InlineKeyboardButton("🚛 Экспедитор", callback_data="role:expeditor"),
    )

    msg = await message.answer(
        "📝 <b>Регистрация</b>\n\n" "▓▓▓▓▓▓▓░ 7/8\n\n" "Выберите вашу роль:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.role.set()


# ========== ШАГ 7: РОЛЬ ==========
@dp.callback_query_handler(
    lambda c: c.data.startswith("role:"), state=RegistrationStatesGroup.role
)
async def registration_role(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора роли"""
    role = callback.data.split(":", 1)[1]

    logging.info(f"📝 Пользователь {callback.from_user.id} выбрал роль: {role}")

    await state.update_data(role=role)

    # Удаляем сообщение с кнопками
    try:
        await callback.message.delete()

    msg = await callback.message.answer(
        "📝 <b>Регистрация</b>\n\n" "▓▓▓▓▓▓▓▓ 8/8\n\n" "Выберите ваш регион:",
        reply_markup=region_keyboard(),
        parse_mode="HTML",
    )

    await state.update_data(last_bot_msg_id=msg.message_id)
    await RegistrationStatesGroup.region.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("join_pull:"), state="*")
async def join_pull_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса присоединения к пулу"""

    try:
        parsed = parse_join_pull_callback(callback.data)
        pull_id = parsed["pull_id"]
        logging.info(
            f"🔗 join_pull callback: {callback.data}, извлечён pull_id: {pull_id}"
        )
    except (IndexError, ValueError) as e:
        logging.error(f"❌ Ошибка парсинга callback: {callback.data}, ошибка: {e}")
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

    all_pulls = pulls.get("pulls", {})

        logging.warning(
            f"❌ Пул {pull_id} не найден. Доступные: {list(all_pulls.keys())}"
        )
        await callback.answer("❌ Пул не найден", show_alert=True)
        return

        await callback.answer(
            show_alert=True,
        )
        return

    user_id = callback.from_user.id

        await callback.answer("❌ Пользователь не зарегистрирован", show_alert=True)
        return

        await callback.answer(
            "❌ Только фермеры могут присоединяться к пулам", show_alert=True
        )
        return

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
                text="✅ Создать партию прямо сейчас",
                callback_data=f"createbatchforpull:{pull_id}",
            ),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"),
        )
        await callback.message.answer(
            f"🌾 У вас нет партий культуры <b>{pull.get('culture', '?')}</b> для этого пула.\n\n"
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        await callback.answer()
        return

    already_joined_batch_ids = []

                    p["batch_id"]
                ]

    # 🔍 ДИАГНОСТИКА - КРИТИЧНО ДЛЯ ОТЛАДКИ
    logging.info(
        f"🔍 Партии уже в пуле от фермера {user_id}: {already_joined_batch_ids}"
    )

    logging.info(f"📋 Культура пула: '{pull.get('culture')}'")
    logging.info(f"📋 Партии пользователя {user_id}:")
        logging.info(
        )

    active_batches = [
        b
    ]

    logging.info(f"✅ Найдено активных партий: {len(active_batches)}")

    if not active_batches:
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
                text="✅ Создать партию прямо сейчас",
                callback_data=f"createbatchforpull:{pull_id}",
            ),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"),
        )
        await callback.message.answer(
            f"🌾 У вас нет партий культуры <b>{pull.get('culture', 'Неизвестно')}</b> для этого пула.\n\n"
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        await callback.answer()
        return

    await state.update_data(join_pull_id=pull_id)

    keyboard = InlineKeyboardMarkup(row_width=1)
    for batch in active_batches:
        button_text = (
        )
        keyboard.add(
            InlineKeyboardButton(
            )
        )
        logging.info(f"   Добавлена партия: {button_text}")

    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data=f"view_pull:{pull_id}"))

    await JoinPullStatesGroup.select_batch.set()

    await callback.message.edit_text(
        f"🎯 <b>Выберите партию для присоединения к пулу #{pull_id}</b>\n\n"
        f"🌾 Культура: {pull.get('culture', 'Неизвестно')}\n"
        f"📦 Целевой объём: {pull.get('target_volume', 0)} т\n"
        f"📊 Текущий объём: {pull.get('current_volume', 0)} т\n"
        f"📉 Доступно: {pull.get('target_volume', 0) - pull.get('current_volume', 0)} т\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


# Быстрое создание партии для пулла
@dp.callback_query_handler(lambda c: c.data.startswith("quickbatch:"), state="*")
async def quick_batch_start(callback: types.CallbackQuery, state: FSMContext):
    """Начать создание партии для присоединения к пуллу"""
    await state.finish()


    # ✅ ИСПРАВЛЕНО: Получаем пулы из правильного места
    all_pulls = pulls.get("pulls", {})
    pull = (
        all_pulls.get(pull_id)
        or all_pulls.get(str(pull_id))
        or all_pulls.get(int(pull_id) if str(pull_id).isdigit() else None)
    )

    if not pull:
        await callback.answer("❌ Пулл не найден", show_alert=True)
        return

    await state.update_data(pull_id=pull_id)
    await QuickBatchStatesGroup.volume.set()

    # ✅ ИСПРАВЛЕНО: Используем правильные поля
    target_volume = pull.get("target_volume", 0)
    current_volume = pull.get("current_volume", 0)
    available_volume = target_volume - current_volume

    msg = (
        f"🌾 Культура: {pull.get('culture', '?')}\n"
        f"📍 Регион: {pull.get('region', '?')}\n"
        f"📊 Доступно в пулле: {available_volume:,.0f} т\n\n"
    )

    await callback.message.edit_text(msg, parse_mode="HTML")
    await callback.answer()


# Обработка объёма
@dp.message_handler(state=QuickBatchStatesGroup.volume)
async def quick_batch_volume(message: types.Message, state: FSMContext):
    """Получение объёма партии"""
    try:
        volume = float(message.text.replace(",", ".").replace(" ", ""))
        if volume <= 0:
            await message.answer("❌ Объём должен быть больше нуля")
            return

        data = await state.get_data()
        pull_id = data.get("pull_id")

        # ✅ ИСПРАВЛЕНО: Получаем пулы из правильного места
        all_pulls = pulls.get("pulls", {})
        pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

        if not pull:
            await message.answer("❌ Пул не найден")
            await state.finish()
            return

        # ✅ ИСПРАВЛЕНО: Используем правильные поля
        target_volume = pull.get("target_volume", 0)
        current_volume = pull.get("current_volume", 0)
        available = target_volume - current_volume

        if volume > available:
            await message.answer(
                f"⚠️ В пулле доступно только {available:,.0f} т\n"
                f"Введите объём не больше {available:,.0f} т:"
            )
            return

        await state.update_data(volume=volume)
        await QuickBatchStatesGroup.price.set()
        await message.answer(
            f"Цена пула: {pull.get('price', 0):,.0f} ₽/т\n\n"
            parse_mode="HTML",
        )

    except ValueError:
        await message.answer("❌ Введите корректное число")


# Обработка цены
@dp.message_handler(state=QuickBatchStatesGroup.price)
async def quick_batch_price(message: types.Message, state: FSMContext):
    """Получение цены"""
    try:
        price = float(message.text.replace(",", ".").replace(" ", ""))
        if price <= 0:
            await message.answer("❌ Цена должна быть больше нуля")
            return

        await state.update_data(price=price)

        # Спрашиваем про качество
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("Да", callback_data="quickquality:yes"),
            InlineKeyboardButton("Нет, пропустить", callback_data="quickquality:no"),
        )

        await message.answer(
            "Хотите указать параметры качества (влажность, натура, примеси)?",
            reply_markup=keyboard,
        )

    except ValueError:
        await message.answer("❌ Введите корректное число")


# Выбор - указывать качество или нет
@dp.callback_query_handler(
    lambda c: c.data.startswith("quickquality:"), state=QuickBatchStatesGroup.price
)
async def quick_batch_quality_choice(callback: types.CallbackQuery, state: FSMContext):
    """Выбор - указывать параметры качества"""

    if choice == "yes":
        await QuickBatchStatesGroup.quality.set()
        await callback.message.edit_text("Введите натуру (г/л):")
    else:
        # Пропускаем параметры качества, сразу завершаем
        await finish_quick_batch(callback.message, state, callback.from_user.id)
        await state.finish()

    await callback.answer()


# Качество: натура
@dp.message_handler(state=QuickBatchStatesGroup.quality)
async def quick_batch_quality(message: types.Message, state: FSMContext):
    """Натура"""
    try:
        nature = float(message.text.replace(",", "."))
        await state.update_data(nature=nature)
        await QuickBatchStatesGroup.moisture.set()
        await message.answer("Введите влажность (%):")
    except ValueError:
        await message.answer("❌ Введите число")


# Влажность
@dp.message_handler(state=QuickBatchStatesGroup.moisture)
async def quick_batch_moisture(message: types.Message, state: FSMContext):
    """Влажность"""
    try:
        moisture = float(message.text.replace(",", "."))
        await state.update_data(moisture=moisture)
        await QuickBatchStatesGroup.impurity.set()
        await message.answer("Введите сорность (%):")
    except ValueError:
        await message.answer("❌ Введите число")


# Сорность
@dp.message_handler(state=QuickBatchStatesGroup.impurity)
async def quick_batch_impurity(message: types.Message, state: FSMContext):
    """Сорность"""
    try:
        impurity = float(message.text.replace(",", "."))
        await state.update_data(impurity=impurity)

        # Завершаем создание партии
        await finish_quick_batch(message, state, message.from_user.id)
        await state.finish()

    except ValueError:
        await message.answer("❌ Введите число")


# Функция завершения создания быстрой партии
async def finish_quick_batch(message_or_callback, state: FSMContext, user_id: int):
    """Создать партию и добавить в пулл"""
    data = await state.get_data()
    pull_id = data.get("pull_id")

    # ✅ ИСПРАВЛЕНО: Получаем пулы из правильного места
    all_pulls = pulls.get("pulls", {})
    pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

    if not pull:
        if hasattr(message_or_callback, "answer"):
            await message_or_callback.answer("❌ Пул не найден")
        else:
            await message_or_callback.message.answer("❌ Пул не найден")
        await state.finish()
        return

    batch = {
        "id": batch_id,
        "farmer_id": user_id,
        "culture": pull.get("culture"),
        "region": pull.get("region", "?"),
        "status": "active",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Добавляем качество если есть
    if "nature" in data:
        batch["nature"] = data.get("nature")
        batch["moisture"] = data.get("moisture")
        batch["impurity"] = data.get("impurity")

    # Сохраняем партию
    if user_id not in batches:
        batches[user_id] = []
    batches[user_id].append(batch)

    pull_id_str = str(pull_id)


        {
            "farmer_id": user_id,
            "batch_id": batch_id,
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

    # Добавляем в пулл
    if "batches" not in pull:
        pull["batches"] = []

    pull["batches"].append(
        {
            "id": batch_id,
            "farmer_id": user_id,
            "culture": batch.get("culture"),
            "volume": batch.get("volume"),
            "price": batch.get("price"),
            "moisture": batch.get("moisture", 0),
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

    if "batch_ids" not in pull:
        pull["batch_ids"] = []
    pull["batch_ids"].append(batch_id)

    if "farmer_ids" not in pull:
        pull["farmer_ids"] = []
        pull["farmer_ids"].append(user_id)

    # ✅ ИСПРАВЛЕНО: Используем current_volume

    deal = {
        "id": deal_id,
        "pull_id": pull_id,
        "batch_id": batch_id,
        "farmer_id": user_id,
        "exporter_id": pull.get("exporter_id"),
        "status": "matched",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    deals[deal_id] = deal

    # Сохраняем
    save_batches_to_pickle()
    save_pulls_to_pickle()
    save_deals_to_pickle()

    # Уведомления
    exporter_id = pull.get("exporter_id")
    target_volume = pull.get("target_volume", 0)
    fill_percent = (
        (pull["current_volume"] / target_volume * 100) if target_volume > 0 else 0
    )

    await bot.send_message(
        user_id,
        f"🌾 {batch['culture']} • {batch['volume']:,.0f} т • {batch['price']:,.0f} ₽/т\n"
        f"📊 Пулл заполнен: {pull['current_volume']:,.0f}/{target_volume:,.0f} т ({fill_percent:.0f}%)",
        parse_mode="HTML",
    )

    if exporter_id:
        await bot.send_message(
            exporter_id,
            f"📦 <b>Новая партия добавлена в ваш пулл #{pull_id}!</b>\n\n"
            f"👤 Фермер: {farmer.get('name')}\n"
            f"🌾 {batch['culture']} • {batch['volume']:,.0f} т • {batch['price']:,.0f} ₽/т\n"
            f"📊 Заполнено: {pull['current_volume']:,.0f}/{target_volume:,.0f} т ({fill_percent:.0f}%)",
            parse_mode="HTML",
        )

    # Проверяем заполнение пулла
    if pull["current_volume"] >= target_volume:
        pull["status"] = "filled"
        logging.info(f"🎉 Пул #{pull_id} заполнен!")

    # Возвращаемся к пуллу
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📊 К пуллу", callback_data=f"view_pull:{pull_id}")
    )

    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(
            "✅ Готово! Партия добавлена в пулл", reply_markup=keyboard
        )
    else:
        try:
            await message_or_callback.message.edit_text(
                "✅ Готово! Партия добавлена в пулл", reply_markup=keyboard
            )
            await message_or_callback.message.answer(
                "✅ Готово! Партия добавлена в пулл", reply_markup=keyboard
            )


@dp.callback_query_handler(
    lambda c: c.data.startswith("createbatchforpull:"), state="*"
)
async def create_batch_for_pull_callback(
    callback: types.CallbackQuery, state: FSMContext
):
    """Создание партии для присоединения к пулу"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    # Сохраняем ID пула и культуру для последующего присоединения

    await callback.message.answer(
        f"**📦 Создание партии для пула #{pull_id}**\n\n"
        reply_markup=region_keyboard(),
        parse_mode="Markdown",
    )

    await AddBatch.region.set()  # ✅ ИСПРАВЛЕНО: AddBatch вместо AddBatchStatesGroup
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("selectbatchjoin:"),
    state=JoinPullStatesGroup.select_batch,
)
async def select_batch_for_join(callback: types.CallbackQuery, state: FSMContext):
    """Выбор партии для присоединения к пулу"""
    try:
        batch_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        await state.finish()
        return

    data = await state.get_data()
    pull_id = data.get("join_pull_id")

    if not pull_id:
        await callback.answer("❌ Пул не найден. Попробуйте снова.", show_alert=True)
        await state.finish()
        return

    all_pulls = pulls.get("pulls", {})
    pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))
    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        await state.finish()
        return

    user_id = callback.from_user.id


    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return

    # Сравнение культур с учетом регистра и пробелов
    batch_culture = (batch.get("culture") or "").strip().lower()
    pull_culture = (pull.get("culture") or "").strip().lower()
    if batch_culture != pull_culture:
        await callback.answer(
            "❌ Культура партии не совпадает с пулом!", show_alert=True
        )
        await state.finish()
        return

    target_volume = pull.get("target_volume", 0)
    current_volume = pull.get("current_volume", 0)
    available = target_volume - current_volume

    if batch.get("volume", 0) > available:
        await callback.answer(
            "❌ Объем партии больше доступного в пуле!", show_alert=True
        )
        await state.finish()
        return

    pull_id_str = str(pull_id)

    participant = {
        "farmer_id": user_id,
        "batch_id": batch_id,
        "volume": batch.get("volume", 0),
        "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    pull.setdefault("batch_ids", [])

    pull.setdefault("farmer_ids", [])
        pull["farmer_ids"].append(user_id)

    pull.setdefault("batches", [])
        pull["batches"].append(
            {
                "farmer_id": user_id,
                "culture": batch.get("culture"),
                "volume": batch.get("volume"),
                "price": batch.get("price"),
                "moisture": batch.get("moisture", 0),
                "quality_class": batch.get("quality_class", ""),
                "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    pull["current_volume"] = current_volume + batch.get("volume", 0)

    # Закрытие пула при заполнении
    if pull["current_volume"] >= target_volume:
        pull["status"] = "filled"
        logging.info(f"🎉 Пул #{pull_id} заполнен на 100%!")

    # Уведомление экспортера с данными фермера
    exporter_id = pull.get("exporter_id")
    if exporter_id:
        farmer_name = farmer.get("name", "Неизвестно")
        farmer_phone = farmer.get("phone", "Не указан")
        farmer_email = farmer.get("email", "Не указан")
        farmer_company = farmer.get("company_details", "Не указано")

        message_exporter = (
            f"Культура: {pull.get('culture')}\n"
            f"Имя: {farmer_name}\n"
            f"Компания: {farmer_company}\n"
            f"Телефон: {farmer_phone}\n"
            f"Email: {farmer_email}"
        )
        try:
            await bot.send_message(exporter_id, message_exporter, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Ошибка уведомления экспортёру: {e}")

    # Уведомление фермера с данными экспортера
    if exporter:
        exporter_name = exporter.get("name", "Неизвестно")
        exporter_phone = exporter.get("phone", "Не указан")
        exporter_email = exporter.get("email", "Не указан")
        exporter_company = exporter.get("company_details", "Не указано")

        message_farmer = (
            f"✅ <b>Вы присоединились к пулу #{pull_id}!</b>\n"
            f"Культура: {batch.get('culture')}\n"
            f"Объём: {batch.get('volume')} т\n"
            f"Цена: {batch.get('price'):,.0f} ₽/т\n\n"
            f"Имя: {exporter_name}\n"
            f"Компания: {exporter_company}\n"
            f"Телефон: {exporter_phone}\n"
            f"Email: {exporter_email}"
        )
        try:
            await bot.send_message(user_id, message_farmer, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Ошибка уведомления фермеру: {e}")


    save_pulls_to_pickle()
    save_batches_to_pickle()

    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")

        await callback.answer(
            "✅ Партия добавлена! Пул заполнен на 100%!", show_alert=True
        )
    else:
        await callback.answer("✅ Успешно присоединились к пулу!", show_alert=True)

    await state.finish()


@dp.callback_query_handler(lambda c: c.data.startswith("viewparticipants:"), state="*")
async def view_pullparticipants(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр участников пула с полными контактами"""
    await state.finish()

    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
        return


    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return


    if not participants:
        await callback.answer("В пуле пока нет участников", show_alert=True)
        return

    msg = f"👥 <b>Участники пула #{pull_id}</b>\n\n"
    msg += f"🌾 Культура: {pull.get('culture', '?')}\n"
    msg += f"📦 Целевой объём: {pull.get('target_volume', 0)} т\n"
    msg += f"📊 Текущий объём: {pull.get('current_volume', 0)} т\n"

    target_vol = pull.get("target_volume", 1)
    current_vol = pull.get("current_volume", 0)
    progress = (current_vol / target_vol * 100) if target_vol > 0 else 0

    msg += f"📈 Заполнено: {progress:.1f}%\n\n"
    msg += f"<b>Участники ({len(participants)}):</b>\n\n"

    for i, p in enumerate(participants, 1):
        farmer_id = p.get("farmer_id")

        msg += f"{i}. <b>{p.get('farmer_name', '?')}</b>\n"
        msg += f"   📦 Объём: {p.get('volume', 0)} т\n"

        batch_id = p.get("batch_id")
        batch = None
        if farmer_id in batches:
            for b in batches[farmer_id]:
                    batch = b
                    break

        if batch:
            msg += f"   💰 Цена: {batch.get('price', 0):,.0f} ₽/т\n"
            msg += f"   📍 Регион: {batch.get('region', 'Не указано')}\n"

        msg += f"   📅 Присоединился: {p.get('joined_at', '?')}\n"

        phone = farmer.get("phone", "Не указан")
        email = farmer.get("email", "Не указан")

            msg += f"   📱 Телефон: <code>{phone}</code>\n"
            msg += f"   📧 Email: <code>{email}</code>\n"
        msg += "\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("◀️ Назад к пулу", callback_data=f"view_pull:{pull_id}")
    )

    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("region:"), state=RegistrationStatesGroup.region
)
async def registration_region(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора региона"""

    region = callback.data.split(":", 1)[1]

    if region == "other":
        await callback.message.answer("Введите название вашего региона:")
        return

    await state.update_data(region=region)

    data = await state.get_data()
    user_id = callback.from_user.id
    role = data.get("role")

    logging.info(
        f"📝 Завершение регистрации user_id={user_id}, role={role}, region={region}"
    )

    users[user_id] = {
        "name": data.get("name"),
        "phone": data.get("phone"),
        "email": data.get("email"),
        "inn": data.get("inn"),
        "ogrn": data.get("ogrn"),
        "company_details": data.get("company_details"),
        "role": role,
        "region": region,
        "registered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    save_users_to_pickle()

    # Синхронизация с Google Sheets
    if gs and gs.spreadsheet:
        try:
            gs.sync_user_to_sheets(users[user_id], user_id)
        except Exception as e:
            logging.error(f"Ошибка синхронизации с Google Sheets: {e}")

    await state.finish()

    # ✅ УДАЛЯЕМ СООБЩЕНИЕ С ВЫБОРОМ РЕГИОНА
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"⚠️ Не удалось удалить сообщение региона: {e}")

    keyboard = get_role_keyboard(role)

    role_names_display = {
        "farmer": "Фермер",
        "exporter": "Экспортёр",
        "logistic": "Логист",
        "expeditor": "Экспедитор",
    }

    # ✅ ОТПРАВЛЯЕМ НОВОЕ СООБЩЕНИЕ (не редактируем)
    await callback.message.answer(
        f"👤 Имя: {data.get('name')}\n"
        f"📱 Телефон: {data.get('phone')}\n"
        f"📧 Email: {data.get('email')}\n"
        f"🎭 Роль: {role_names_display.get(role, role)}\n"
        f"📍 Регион: {region}\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )

    await callback.answer("✅ Регистрация завершена!")


async def admin_menu(message: types.Message, state: FSMContext):
    """Админ меню"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("🚫 У вас нет доступа к этой команде.")
        return

    await message.answer(
        "🔐 <b>Админ панель</b>\n\n" "Выберите действие:",
        reply_markup=admin_keyboard(),
        parse_mode="HTML",
    )


# ✅ ОСТАВЛЯЕМ НАЗВАНИЯ КАК ЕСТЬ!
# ✅ ТОЛЬКО ИСПРАВЛЯЕМ ЛОГИКУ ВНУТРИ!


@dp.message_handler(lambda m: m.text == "📊 Статистика бота", state="*")
async def admin_stats_button(message: types.Message, state: FSMContext):
    """Обработчик кнопки Статистика бота"""
    await state.finish()
    user_id = message.from_user.id

    if user_id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён")
        return

    try:
        # Собираем статистику
        total_users = len(users)
        farmers = len([u for u in users.values() if u.get("role") == "farmer"])
        exporters = len([u for u in users.values() if u.get("role") == "exporter"])

        # ✅ ИСПРАВЛЕНО: Правильный перебор batches
        total_batches = sum(len(farmer_batches) for farmer_batches in batches.values())
        active_batches = sum(
            len(
                [
                    b
                    for b in farmer_batches
                    if b.get("status") in ["active", "pending", None]
                ]
            )
            for farmer_batches in batches.values()
        )

        open_pulls = len(
        )

        total_deals = len(deals) if "deals" in dir() else 0
        active_deals = (
            len(
                [
                    d
                    for d in deals.values()
                ]
            )
            if "deals" in dir()
            else 0
        )

        total_matches = len(matches) if "matches" in dir() else 0
        active_matches = (
            if "matches" in dir()
            else 0
        )

        stats_msg = f"""📊 <b>Статистика бота</b>

👥 <b>Пользователи:</b>
• Всего: {total_users}
• 🌾 Фермеров: {farmers}
• 📦 Экспортёров: {exporters}
• 🚚 Логистов: {logists}
• 🚛 Экспедиторов: {expeditors}

📦 <b>Партии:</b>
• Всего: {total_batches}
• Активных: {active_batches}

🎯 <b>Пулы:</b>
• Всего: {total_pulls}
• Открытых: {open_pulls}

🤝 <b>Сделки:</b>
• Всего: {total_deals}
• Активных: {active_deals}

🔗 <b>Совпадения:</b>
• Всего: {total_matches}
• Активных: {active_matches}"""

        await message.answer(stats_msg, parse_mode="HTML")

    except Exception as e:
        logging.error(f"❌ ОШИБКА в статистике: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {str(e)}")


@dp.message_handler(lambda m: m.text == "📊 Аналитика", state="*")
async def admin_analytics_button(message: types.Message, state: FSMContext):
    """Обработчик кнопки Аналитика"""
    await state.finish()
    user_id = message.from_user.id

    if user_id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён")
        return

    try:
        # ✅ ИСПРАВЛЕНО: Правильный перебор batches
        total_batch_volume = 0
        prices = []

        for farmer_id, farmer_batches in batches.items():
            for batch in farmer_batches:
                total_batch_volume += batch.get("volume", 0)
                if batch.get("price"):
                    prices.append(batch["price"])

        avg_price = sum(prices) / len(prices) if prices else 0
        min_price = min(prices) if prices else 0
        max_price = max(prices) if prices else 0

        # ✅ ИСПРАВЛЕНО: Правильный перебор batches
        cultures_stats = {}
        for farmer_id, farmer_batches in batches.items():
            for batch in farmer_batches:
                culture = batch.get("culture", "Неизвестно")
                if culture not in cultures_stats:
                    cultures_stats[culture] = {"count": 0, "volume": 0}
                cultures_stats[culture]["count"] += 1
                cultures_stats[culture]["volume"] += batch.get("volume", 0)

        analytics_msg = f"""📊 <b>Расширенная аналитика</b>

📦 <b>Объёмы:</b>
• Общий объём партий: {total_batch_volume:,.0f} т

💰 <b>Цены:</b>
• Средняя цена: {avg_price:,.0f} ₽/т
• Минимальная: {min_price:,.0f} ₽/т
• Максимальная: {max_price:,.0f} ₽/т

🌾 <b>По культурам:</b>"""

        for culture, stats in cultures_stats.items():
            analytics_msg += (
                f"\n• {culture}: {stats['count']} партий, {stats['volume']:,.0f} т"
            )

        analytics_msg += (
            f"\n\n📅 Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )

        await message.answer(analytics_msg, parse_mode="HTML")

    except Exception as e:
        logging.error(f"❌ ОШИБКА в аналитике: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {str(e)}")


@dp.message_handler(lambda m: m.text == "📂 Экспорт данных", state="*")
async def admin_export_button(message: types.Message, state: FSMContext):
    """Обработчик кнопки Экспорт данных"""
    await state.finish()
    user_id = message.from_user.id

    if user_id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён")
        return

    try:
        await message.answer("⏳ Формирую экспорт данных...")

        # ✅ ИСПРАВЛЕНО: Правильная структура batches
        export_data = {
            "users": users,
            "batches": batches,
            "pulls": pulls,
            "shipping_requests": shipping_requests,
            "expeditor_offers": expeditor_offers,
            "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Сохраняем в файл
        filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)

        # Отправляем файл
        with open(filename, "rb") as f:
            await message.answer_document(
                f,
                f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                f"👥 Пользователей: {len(users)}\n"
                f"🎯 Пулов: {len(pulls)}",
                parse_mode="HTML",
            )

        # Удаляем временный файл
        os.remove(filename)

    except Exception as e:
        logging.error(f"❌ ОШИБКА при экспорте: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка при экспорте: {str(e)}")


@dp.message_handler(lambda m: m.text == "🔍 Найти совпадения", state="*")
async def admin_manual_match(message: types.Message, state: FSMContext):
    """Ручной поиск совпадений"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    await message.answer("🔍 Запуск поиска совпадений...")

    matches_found = await auto_match_batches_and_pulls()

    await message.answer(
        f"🔍 Найдено совпадений: {matches_found}\n"
        f"📊 Всего активных: {len([m for m in matches.values() if m.get('status') == 'active'])}",
        parse_mode="HTML",
    )


@dp.message_handler(lambda m: m.text == "◀️ Назад", state="*")
async def admin_back(message: types.Message, state: FSMContext):
    """Возврат из админ панели"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    if user_id in users:
        keyboard = get_role_keyboard(role)
        await message.answer("◀️ Возврат в главное меню", reply_markup=keyboard)
    else:
        await message.answer("◀️ Возврат")


@dp.message_handler(commands=["match"], state="*")
async def cmd_manual_match(message: types.Message, state: FSMContext):
    """Ручной запуск поиска совпадений"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("❌ Эта команда доступна только администратору")
        return

    await message.answer("🔄 Запуск ручного поиска совпадений...")

    matches_found = await auto_match_batches_and_pulls()

    await message.answer(
        f"Найдено новых совпадений: {matches_found}\n"
    )


@dp.message_handler(lambda m: m.text == "👤 Профиль", state="*")
async def cmd_profile(message: types.Message, state: FSMContext):
    """Показать расширенный профиль пользователя"""
    await state.finish()

    user_id = message.from_user.id
        await message.answer("❌ Вы не зарегистрированы. Используйте /start")
        return



    if user.get("company_details"):
        profile_text += f"\n🏢 <b>Реквизиты компании:</b>\n{user['company_details']}"

    keyboard = profile_edit_keyboard()

    await message.answer(profile_text, parse_mode="HTML", reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data.startswith("edit_profile:"), state="*")
async def start_edit_profile(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование профиля"""
    field = callback.data.split(":", 1)[1]

    await state.update_data(edit_field=field)

    field_names = {
        "phone": "номер телефона",
        "email": "email",
        "region": "регион",
        "company_details": "реквизиты компании",
    }

    if field == "region":
        await callback.message.edit_text(
            "Выберите новый регион:", reply_markup=region_keyboard()
        )
    else:
        await callback.message.answer(
            f"Введите новый {field_names.get(field, 'значение')}:"
        )

    await EditProfile.new_value.set()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("region:"), state=EditProfile.new_value
)
async def edit_profile_region(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора региона при редактировании профиля"""
    new_region = callback.data.split(":", 1)[1]

    data = await state.get_data()
    field = data.get("edit_field")
    user_id = callback.from_user.id

    if field != "region":
        await callback.answer("❌ Ошибка")
        return

    users[user_id]["region"] = new_region

    save_users_to_json()

    if gs and gs.spreadsheet:
        gs.update_user_in_sheets(user_id, users[user_id])

    await state.finish()

    keyboard = get_role_keyboard(role)

    await callback.message.edit_text(
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_region}"
    )

    await callback.message.answer("Профиль обновлён!", reply_markup=keyboard)
    await callback.answer("✅ Регион обновлён")


@dp.message_handler(state=EditProfile.new_value)
async def edit_profile_value(message: types.Message, state: FSMContext):
    """Сохранить новое значение профиля"""
    user_id = message.from_user.id
    data = await state.get_data()
    field = data.get("edit_field")
    new_value = message.text.strip()

    if field == "email":
        if not validate_email(new_value):
            await message.answer("❌ Некорректный email. Попробуйте ещё раз:")
            return
    elif field == "phone":
        if not validate_phone(new_value):
            await message.answer("❌ Некорректный номер телефона. Попробуйте ещё раз:")
            return

    users[user_id][field] = new_value

    save_users_to_json()

    if gs and gs.spreadsheet:
        gs.update_user_in_sheets(user_id, users[user_id])

    await state.finish()

    keyboard = get_role_keyboard(role)

    field_names = {
        "phone": "Телефон",
        "email": "Email",
        "company_details": "Реквизиты компании",
    }

    await message.answer(
        f"✅ {field_names.get(field, field.capitalize())} обновлён!",
        reply_markup=keyboard,
    )


@dp.message_handler(lambda m: m.text == "📈 Цены на зерно", state="*")
async def show_prices_menu(message: types.Message, state: FSMContext):
    """Показать цены сразу без меню"""
    await state.finish()

    prices_msg = format_prices_message()

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить цены", callback_data="refresh_prices")
    )

    await message.answer(prices_msg, parse_mode="HTML", reply_markup=keyboard)


@dp.message_handler(lambda m: m.text == "📰 Новости рынка", state="*")
async def show_news_menu(message: types.Message, state: FSMContext):
    """Показать новости сразу без меню"""
    await state.finish()
    news_msg = format_news_message()
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить новости", callback_data="refresh_news")
    )

    await message.answer(
        news_msg,
        parse_mode="HTML",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


@dp.callback_query_handler(lambda c: c.data == "view_news", state="*")
async def show_news(callback: types.CallbackQuery):
    """Показать новости"""
    news_msg = format_news_message()
    await callback.message.edit_text(
        news_msg, parse_mode="HTML", disable_web_page_preview=True
    )
    await callback.answer()


def parse_soy_from_zol():
    """
    Парсинг цен на сою с ZOL.RU (региональная аналитика)

    ZOL.RU публикует еженедельную аналитику цен по регионам России
    Цены указаны в руб/кг, конвертируются в ₽/т

    Returns:
        int: Средняя цена в ₽/т или None при ошибке
    """
    try:
        logging.info("🌱 Парсинг сои с ZOL.RU...")

        # ZOL.RU публикует аналитику еженедельно
        base_urls = [
            "https://www.zol.ru/n/3fa47",  # 01.10.2025
            "https://www.zol.ru/n/3faf3",  # резерв 1
            "https://www.zol.ru/n/3f7b3",  # резерв 2
            "https://www.zol.ru/soya.htm",  # общая страница
        ]

        for url in base_urls:
            try:
                response = requests.get(
                    url,
                    timeout=10,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    },
                )

                if response.status_code != 200:
                    continue

                text = response.text.lower()

                # Паттерны для парсинга
                patterns = [
                    r"соя\s*[=:]\s*(\d+\.?\d*)",
                    r"soy\s*[=:]\s*(\d+\.?\d*)",
                ]

                prices = []

                for pattern in patterns:
                    matches = re.findall(pattern, text)
                    if matches:
                        for match in matches:
                            try:
                                # Цена в руб/кг, переводим в ₽/т
                                price_kg = float(match)
                                price_ton = int(price_kg * 1000)

                                # Валидация (18,000 - 60,000 ₽/т)
                                if 18000 <= price_ton <= 60000:
                                    prices.append(price_ton)
                                continue

                # Убираем дубликаты
                prices = list(set(prices))

                if prices and len(prices) >= 1:
                    avg = int(sum(prices) / len(prices))
                    logging.info(f"✅ Соя (ZOL.RU): найдено {len(prices)} регионов")
                    for i, price in enumerate(sorted(prices), 1):
                        logging.info(f"   Регион {i}: {price:,} ₽/т")
                    logging.info(
                        f"✅ Соя: средняя {avg:,} ₽/т ({len(prices)} регионов) [СПАРСЕНО]"
                    )
                    return avg

            except Exception as e:
                logging.debug(f"Попытка {url}: {e}")
                continue

        logging.warning("⚠️ Соя (ZOL.RU): не удалось спарсить")
        return None

    except Exception as e:
        logging.error(f"❌ Ошибка парсинга сои с ZOL.RU: {e}")
        return None


def parse_russia_regional_prices():
    """Парсинг региональных цен на зерно в России с zerno.ru"""

    logging.info("🌾 Парсинг РФ: начало...")
    result = {}

    # 1. ПШЕНИЦА (оставить как есть - работает отлично)
    try:
        url_wheat = "https://www.zerno.ru/regional-prices-wheat-minimum-and-maximum"
        response = requests.get(url_wheat, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        table = soup.find("table")
        if table:
            rows = table.find_all("tr")[1:]
            logging.info(f"📋 Пшеница: найдено строк {len(rows)}")

            wheat_prices = []
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                region = cells[0].get_text(strip=True)

                for i in range(1, min(4, len(cells))):
                    price_text = cells[i].get_text(strip=True)
                    if not price_text or price_text == "-":
                        continue

                    if "-" in price_text and not price_text.startswith("-"):
                        prices_range = price_text.split("-")
                        for p in prices_range:
                            try:
                                price_clean = re.sub(r"[^0-9]", "", p)
                                if price_clean:
                                    price_value = int(price_clean)
                                    if 8000 <= price_value <= 30000:
                                        wheat_prices.append(price_value)
                                        logging.info(
                                            f"✅ Пшеница: {price_value} ₽/т из {region}"
                                        )
                                continue
                    else:
                        try:
                            price_clean = re.sub(r"[^0-9]", "", price_text)
                            if price_clean:
                                price_value = int(price_clean)
                                if 8000 <= price_value <= 30000:
                                    wheat_prices.append(price_value)
                                    logging.info(
                                        f"✅ Пшеница: {price_value} ₽/т из {region}"
                                    )
                            continue

            if wheat_prices:
                result["Пшеница"] = int(sum(wheat_prices) / len(wheat_prices))
                logging.info(
                    f"✅ Пшеница: средняя {result['Пшеница']} ₽/т ({len(wheat_prices)} цен)"
                )
            else:
                result["Пшеница"] = 15000
                logging.warning("⚠️ Пшеница: используем резервное значение")
        else:
            result["Пшеница"] = 15000
            logging.warning("⚠️ Пшеница: таблица не найдена")

    except Exception as e:
        logging.error(f"❌ Ошибка парсинга пшеницы: {e}")
        result["Пшеница"] = 15000

    # 2. УЛУЧШЕННЫЙ ПАРСИНГ ДРУГИХ КУЛЬТУР
    today = datetime.now().strftime("%Y-%m-%d")

    cereals_urls = {
        "Ячмень": f"https://www.zerno.ru/cerealspricesdate/{today}/barley",
        "Кукуруза": f"https://www.zerno.ru/cerealspricesdate/{today}/corn",
        "Подсолнечник": f"https://www.zerno.ru/cerealspricesdate/{today}/sunflower",
    }

    fallback_prices = {
        "Ячмень": 14000,
        "Кукуруза": 14000,
        "Соя": 25000,
        "Подсолнечник": 30000,
    }

    price_ranges = {
        "Ячмень": (7000, 25000),
        "Кукуруза": (10000, 30000),  # РАСШИРЕНО!
        "Соя": (18000, 60000),  # РАСШИРЕНО!
        "Подсолнечник": (15000, 50000),
    }

    for culture, url in cereals_urls.items():
        try:
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                logging.warning(
                    f"⚠️ {culture}: страница недоступна (код {response.status_code})"
                )
                result[culture] = fallback_prices[culture]
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table")

            if not table:
                logging.warning(f"⚠️ {culture}: таблица не найдена")
                result[culture] = fallback_prices[culture]
                continue

            prices = []
            rows = table.find_all("tr")
            logging.info(f"📋 {culture}: найдено строк {len(rows)}")

            for row in rows:
                cells = row.find_all("td")

                # Пропускаем короткие строки
                if len(cells) < 3:
                    continue

                # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Пропускаем заголовки
                first_cell = cells[0].get_text(strip=True)
                if any(
                    keyword in first_cell
                    for keyword in ["Класс", "Город", "цена", "изм.", "тренд", "Валюта"]
                ):
                    continue

                # Получаем город/источник для логирования
                city = first_cell if first_cell else "Неизвестно"

                # Ищем цену в разных колонках (приоритет: 2, 1, 3, 4)
                for col_idx in [2, 1, 3, 4]:
                    if len(cells) <= col_idx:
                        continue

                    price_text = cells[col_idx].get_text(strip=True)

                    # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Пропускаем служебные значения
                    if not price_text or price_text in [
                        "default_value",
                        "-",
                        "0",
                        "",
                        "руб/т",
                    ]:
                        continue

                    try:
                        # Извлекаем только цифры
                        price_clean = re.sub(r"[^0-9]", "", price_text)
                        if not price_clean:
                            continue

                        price_value = int(price_clean)

                        # Валидация с расширенными диапазонами
                        min_p, max_p = price_ranges[culture]
                        if min_p <= price_value <= max_p:
                            prices.append(price_value)
                            logging.info(f"✅ {culture}: {price_value} ₽/т из {city}")
                            break  # Нашли цену, переходим к следующей строке
                        continue

            # Результат
            if prices:
                avg = int(sum(prices) / len(prices))
                result[culture] = avg
                logging.info(f"✅ {culture}: средняя {avg} ₽/т ({len(prices)} цен)")
            else:
                result[culture] = fallback_prices[culture]
                logging.warning(
                    f"⚠️ {culture}: используем резервное значение {fallback_prices[culture]} ₽/т"
                )

        except Exception as e:
            result[culture] = fallback_prices[culture]
            logging.error(f"❌ {culture}: {e}")

    try:
        soy_price = parse_soy_from_zol()

        if soy_price:
            result["Соя"] = soy_price
        else:
            # Резервное значение на основе последних реальных данных
            result["Соя"] = 28000
            logging.warning("⚠️ Соя: используем резервное значение 28,000 ₽/т")

    except Exception as e:
        result["Соя"] = 28000
        logging.error(f"❌ Соя: {e}, используем резервное")

    logging.info(f"📊 Парсинг завершён: {len(result)} культур")
    return result


def parse_fob_black_sea():
    """✅ Парсинг FOB (Черное море)"""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/ZW=F"
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        data = response.json()

        if "chart" in data and "result" in data["chart"] and data["chart"]["result"]:
            result = data["chart"]["result"][0]
            if "meta" in result and "regularMarketPrice" in result["meta"]:
                price_cents = result["meta"]["regularMarketPrice"]
                price_dollars = price_cents / 100
                fob_price = round(price_dollars * 36.74, 2)
                logging.info(f"✅ FOB: ₽{fob_price}/т")
                return fob_price

        logging.warning("⚠️ FOB: используем fallback")
        return 210.0

    except Exception as e:
        logging.error(f"❌ parse_fob_black_sea: {e}")
        return 210.0


def parse_cbot_futures():
    """✅ Парсинг фьючерсов CBoT"""
    prices = {}

    try:
        symbols = {
            "Пшеница (CBoT)": "ZW=F",
            "Кукуруза (CBoT)": "ZC=F",
            "Соя (CBoT)": "ZS=F",
        }

        for name, symbol in symbols.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                response = requests.get(
                    url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10
                )
                data = response.json()

                if (
                    "chart" in data
                    and "result" in data["chart"]
                    and data["chart"]["result"]
                ):
                    result = data["chart"]["result"][0]
                    if "meta" in result and "regularMarketPrice" in result["meta"]:
                        price_cents = result["meta"]["regularMarketPrice"]
                        price_dollars = price_cents / 100
                        prices[name] = f"₽{price_dollars:.2f}/bu"
                        logging.info(f"✅ {name}: ₽{price_dollars:.2f}/bu")
            except Exception as e:
                logging.error(f"❌ {name}: {e}")
                continue

        if not prices:
            prices = {
                "Пшеница (CBoT)": "₽5.50/bu",
                "Кукуруза (CBoT)": "₽4.20/bu",
                "Соя (CBoT)": "₽10.80/bu",
            }
            logging.warning("⚠️ CBoT: используем fallback")

        return prices

    except Exception as e:
        logging.error(f"❌ parse_cbot_futures: {e}")
        return {
            "Пшеница (CBoT)": "₽5.50/bu",
            "Кукуруза (CBoT)": "₽4.20/bu",
            "Соя (CBoT)": "₽10.80/bu",
        }


def parse_grain_news(limit=5):
    """✅ Парсинг новостей с zerno.ru"""
    newslist = []

    try:
        url = "https://www.zerno.ru"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9",
        }

        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        links = soup.find_all("a", href=re.compile(r"/node/\d+"))
        seen_titles = set()

        keywords = [
            "экспорт",
            "россия",
            "астрахань",
            "зерно",
            "пшениц",
            "урожай",
            "fob",
            "черное море",
            "цен",
            "рынок",
        ]

        for link in links[:20]:
            title = link.text.strip()
            href = link.get("href", "")

            title_lower = title.lower()

            if title and len(title) > 30 and title not in seen_titles:
                if any(kw in title_lower for kw in keywords):
                    seen_titles.add(title)

                    date = datetime.now().strftime("%d.%m.%Y")
                    full_link = (
                        f"https://www.zerno.ru{href}" if href.startswith("/") else href
                    )

                    newslist.append({"title": title, "link": full_link, "date": date})

                    if len(newslist) >= limit:
                        break

        logging.info(f"✅ Спарсено новостей: {len(newslist)}")
        return newslist

    except Exception as e:
        logging.error(f"❌ parse_grain_news: {e}")
        return []


async def update_prices_cache():
    """Обновление кэша цен"""
    global prices_cache, last_prices_update

    try:
        logging.info("🔄 Обновление цен...")
        loop = asyncio.get_event_loop()

        russia_prices = await loop.run_in_executor(None, parse_russia_regional_prices)
        fob_price = await loop.run_in_executor(None, parse_fob_black_sea)
        cbot_prices = await loop.run_in_executor(None, parse_cbot_futures)

        prices_cache = {
            "data": {
                "russia_south": russia_prices,
                "fob": fob_price,
                "cbot": cbot_prices,
            },
            "updated": datetime.now(),
        }

        last_prices_update = datetime.now()
        logging.info("✅ Цены обновлены")

    except Exception as e:
        logging.error(f"❌ update_prices_cache: {e}")


async def update_news_cache():
    """Обновление кэша новостей - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    global news_cache

    try:
        logging.info("🔄 Обновление новостей...")
        loop = asyncio.get_event_loop()
        news = await loop.run_in_executor(None, parse_grain_news)

        news_cache = {"data": news, "updated": datetime.now()}

        logging.info(f"✅ Новости обновлены: {len(news)} записей")

    except Exception as e:
        logging.error(f"❌ update_news_cache: {e}")
        news_cache = {"data": [], "updated": datetime.now()}


def load_users_from_json():
    """Загрузка пользователей из JSON"""
    global users
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            logging.info(f"✅ Пользователи загружены: {len(users)}")
        else:
            logging.info("ℹ️ Файл пользователей не найден, создан новый")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки пользователей: {e}")


def save_users_to_json():
    """Сохранение пользователей в JSON"""
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        logging.info("✅ Пользователи сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пользователей: {e}")


def load_batches_from_pickle():
    """Загрузка партий из файла"""
    global batches
    try:
        file_path = os.path.join(DATA_DIR, "batches.pkl")
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                loaded_data = pickle.load(f)

            if isinstance(loaded_data, dict):
                # Проверяем структуру
                first_key = next(iter(loaded_data), None)

                if first_key and isinstance(loaded_data[first_key], list):
                    # ПРАВИЛЬНЫЙ формат: {user_id: [batches]}
                    batches = loaded_data
                    total_batches = sum(
                        len(b) for b in batches.values() if isinstance(b, list)
                    )
                    unique_farmers = len(batches)
                    logging.info(
                        f"✅ Загружено партий: {total_batches} от {unique_farmers} фермеров"
                    )

                elif first_key and isinstance(loaded_data[first_key], dict):
                    # НЕПРАВИЛЬНЫЙ формат: {batch_id: batch}
                    # Конвертируем обратно в {user_id: [batches]}
                    logging.warning("⚠️ Обнаружен неправильный формат, конвертируем...")
                    batches = {}
                    for batch_id, batch in loaded_data.items():
                        user_id = batch.get("user_id") or batch.get("farmer_id")
                        if user_id:
                            if user_id not in batches:
                                batches[user_id] = []
                            batches[user_id].append(batch)

                    logging.info(
                        f"✅ Конвертировано {total_batches} партий в правильный формат"
                    )
                else:
                    batches = {}
                    logging.warning("⚠️ Неизвестный формат batches.pkl")
            else:
                batches = {}
                logging.warning("⚠️ Неизвестный тип данных batches.pkl")
        else:
            batches = {}
            logging.info("ℹ️ Файл batches.pkl не найден")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки партий: {e}")
        batches = {}


async def load_requests_from_file():
    """Загружает заявки из файла при старте бота"""
    global user_requests
    try:
            data = json.load(f)
            logging.info(f"✅ Загружено {len(user_requests)} заявок из файла")
    except FileNotFoundError:
        logging.info("ℹ️ Файл requests.json не найден (первый запуск)")
        user_requests = {}
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки заявок: {e}")
        user_requests = {}


def save_requests_to_file():
    """Сохраняет заявки в JSON файл"""
    try:
            data_to_save = {str(k): v for k, v in user_requests.items()}
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        logging.info(f"✅ Заявки сохранены: {len(user_requests)} заявок")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения заявок: {e}")


    """Сохранение партий в pickle"""
    try:

        # ✅ ПРОВЕРЯЕМ ЧТО СОХРАНЯЕМ СЛОВАРЬ
        if not isinstance(batches, dict):
            logging.error(f"❌ batches имеет неправильный тип: {type(batches)}")
            return

            pickle.dump(batches, f)

        # ✅ СОХРАНЯЕМ batch_counter
            pickle.dump(batch_counter, f)

        logging.info(
            f"✅ Партии сохранены: {total_batches} партий, batch_counter={batch_counter}"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения партий: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# ФУНКЦИИ СОХРАНЕНИЯ/ЗАГРУЗКИ ПУЛОВ И ПОЛЬЗОВАТЕЛЕЙ
# ═══════════════════════════════════════════════════════════════════════════


def save_pulls_to_pickle():
    """Сохранение пулов в pickle файл с проверкой структуры и созданием папки"""
    global pulls

    try:
        if not isinstance(pulls, dict):
            logging.error(f"❌ pulls должен быть dict, а не {type(pulls)}")

        if "pulls" not in pulls:
            pulls["pulls"] = {}

        pulls_dict = pulls.get("pulls", {})
        if pulls_dict:
            first_key = next(iter(pulls_dict))
            first_value = pulls_dict[first_key]
            if not (
                isinstance(first_value, dict)
                and ("farmer_ids" in first_value or "created_at" in first_value)
            ):
                logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА: pulls['pulls'] - это ОДИН пул!")
                logging.warning("🔧 Исправляю структуру на пустую...")

        # Создаем папку, если не существует
        dir_path = os.path.dirname(PULLS_FILE)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"✅ Создана директория для пулов: {dir_path}")

        # Сохраняем файл
        with open(PULLS_FILE, "wb") as f:


        logging.info(f"✅ save_pulls_to_pickle() - сохранено {pulls_count} пулов")
        logging.info(
            f"✅ save_pulls_to_pickle() - {participants_count} групп участников"
        )

        # Проверка файла
        if not os.path.isfile(PULLS_FILE):
            logging.error(f"❌ Файл пулов не создан: {PULLS_FILE}")
        else:
            logging.info(
                f"✅ Файл пулов успешно сохранён: {os.path.abspath(PULLS_FILE)}"
            )

        return True

    except Exception as e:
        logging.error(f"❌ save_pulls_to_pickle() error: {e}", exc_info=True)
        return False


def load_pulls_from_pickle():
    """Загрузка пулов из pickle файла"""
    pull_counter = 0

    try:
        if not os.path.exists(PULLS_FILE):
            logging.info("ℹ️ pulls.pkl не существует, создаю новую структуру")
            pull_counter = 0
            return False

        with open(PULLS_FILE, "rb") as f:
            data = pickle.load(f)

        logging.info(f"📂 Загружен файл pulls.pkl, тип: {type(data)}")

        # Проверяем структуру данных
        if not isinstance(data, dict):
            logging.error(f"❌ Некорректный тип данных: {type(data)}")
            pull_counter = 0
            return False

        # ════════════════════════════════════════════════════════════════════════════
        # ОПРЕДЕЛЯЕМ СТРУКТУРУ
        # ════════════════════════════════════════════════════════════════════════════

        if "pulls" in data and "pullparticipants" in data:
            # НОВАЯ структура: {'pulls': {...}, 'pullparticipants': {...}}
            logging.info("✅ Обнаружена НОВАЯ структура с 'pulls' и 'pullparticipants'")

            pulls_dict = data.get("pulls", {})
            participants_dict = data.get("pullparticipants", {})

            # ✅ ПРАВИЛЬНАЯ ПРОВЕРКА: Смотрим на ПЕРВЫЙ элемент
            if pulls_dict:
                first_key = next(iter(pulls_dict))
                first_value = pulls_dict[first_key]

                if isinstance(first_value, dict):
                    # Проверяем что это действительно СЛОВАРЬ пулов
                    has_pull_fields = any(
                        field in first_value
                        for field in [
                            "id",
                            "farmer_ids",
                            "batches",
                            "status",
                            "created_at",
                            "culture",
                        ]
                    )

                    if has_pull_fields:
                        logging.info(
                            "✅ pulls['pulls'] - это СЛОВАРЬ пулов (правильно)"
                        )
                    else:
                        # ❌ ОШИБКА: Один пул попал в структуру
                        logging.error(
                        )
                        logging.warning("🔧 Исправляю структуру...")
                        pull_counter = 0
                else:
                    logging.error(
                        f"❌ Некорректный тип значения в pulls['pulls']: {type(first_value)}"
                    )
                    pull_counter = 0
            else:
                # pulls['pulls'] пуст - это нормально
                logging.info("ℹ️ pulls['pulls'] пуст")
                pull_counter = 0

        else:
            # СТАРАЯ структура: просто словарь пулов или неизвестный формат
            logging.warning(
                "⚠️ Обнаружена СТАРАЯ структура (без 'pulls' и 'pullparticipants')"
            )

            # Проверяем что это словарь пулов, а не один пул
            if data:
                first_key = next(iter(data))
                first_value = data[first_key]

                if isinstance(first_value, dict) and any(
                    field in first_value
                    for field in ["id", "farmer_ids", "batches", "status"]
                ):
                    # Это словарь пулов - конвертируем в новую структуру
                    logging.info("🔄 Конвертирую старую структуру в новую...")
                    pull_counter = len(data)
                else:
                    # Один пул - инициализируем пусто
                    logging.error("❌ Неизвестная структура, инициализирую пусто")
                    pull_counter = 0
            else:
                pull_counter = 0

        # ════════════════════════════════════════════════════════════════════════════
        # ЛОГИРОВАНИЕ РЕЗУЛЬТАТА
        # ════════════════════════════════════════════════════════════════════════════

        pulls_count = len(pulls.get("pulls", {}))

        logging.info(
            f"✅ load_pulls_from_pickle() - загружено {pulls_count} пулов (pull_counter={pull_counter})"
        )
        logging.info(
            f"✅ load_pulls_from_pickle() - {participants_count} групп участников"
        )

        if pulls_count > 0:
            logging.info(f"   📊 Примеры pull_id: {pull_ids}")

        if participants_count > 0:
            logging.info(f"   📊 Примеры pullparticipants: {participant_ids}")

        return True

    except Exception as e:
        logging.error(f"❌ load_pulls_from_pickle() error: {e}", exc_info=True)
        pull_counter = 0
        return False


def save_users_to_pickle():
    """✅ ПРАВИЛЬНОЕ сохранение пользователей"""
    try:
        with open(USERSFILE, "wb") as f:
            pickle.dump(users, f, protocol=pickle.HIGHEST_PROTOCOL)
        logging.info(
            f"✅ save_users_to_pickle() - сохранено {len(users)} пользователей"
        )
        return True
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пользователей: {e}", exc_info=True)
        return False


def load_users_from_pickle():
    """✅ ПРАВИЛЬНАЯ загрузка пользователей"""
    global users
    try:
        if os.path.exists(USERSFILE):
            with open(USERSFILE, "rb") as f:
                loaded = pickle.load(f)
                if isinstance(loaded, dict):
                    logging.info(
                        f"✅ load_users_from_pickle() - загружено {len(users)} пользователей"
                    )
                    return True
                else:
                    logging.warning("⚠️ users.pkl имеет неправильный формат")
                    users = {}
                    return False
        else:
            logging.info("📂 Файл users.pkl не найден")
            users = {}
            return False
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки пользователей: {e}", exc_info=True)
        users = {}
        return False


class GoogleSheetsManager:
    """Менеджер для работы с Google Sheets"""

    def __init__(self, credentials_file, spreadsheet_id):
        self.spreadsheet_id = spreadsheet_id
        self.client = None
        self.spreadsheet = None

        try:
            if not os.path.exists(credentials_file):
                logging.warning(
                    f"⚠️ Файл {credentials_file} не найден. Google Sheets будет отключен."
                )
                return

            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]

            creds = Credentials.from_service_account_file(
                credentials_file, scopes=scope
            )
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)
            logging.info("✅ Google Sheets подключен успешно")

        except Exception as e:
            logging.error(f"❌ Ошибка подключения к Google Sheets: {e}")

    def get_or_create_worksheet(self, title, headers):
        """Получить или создать worksheet"""
        if not self.spreadsheet:
            return None

        try:
            worksheet = self.spreadsheet.worksheet(title)
            worksheet = self.spreadsheet.add_worksheet(
                title=title, rows=1000, cols=len(headers)
            )
            worksheet.append_row(headers)
            logging.info(f"✅ Создан worksheet: {title}")

        return worksheet

    def update_user_in_sheets(self, user_id, user_data):
        """Обновление или добавление пользователя в Google Sheets"""
        try:
            if not self.spreadsheet:
                logging.error("❌ Нет подключения к Google Sheets")
                return

            headers = [
                "User ID",
                "Имя",
                "Роль",
                "Телефон",
                "Email",
                "ИНН",
                "Регион",
                "Реквизиты",
                "Дата регистрации",
                "Обновлено",
            ]
            worksheet = self.get_or_create_worksheet("Users", headers)

            if not worksheet:
                return

            row_data = [
                str(user_id),
                str(user_data.get("name", "")),
                str(user_data.get("role", "")),
                str(user_data.get("phone", "")),
                str(user_data.get("email", "")),
                str(user_data.get("inn", "")),
                str(user_data.get("region", "")),
                str(user_data.get("company_requisites", "")),
                str(
                    user_data.get(
                        "registration_date",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                ),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ]

            try:
                cell = worksheet.find(str(user_id))
                if cell:
                    worksheet.update(f"A{cell.row}:J{cell.row}", [row_data])
                    logging.info(f"✅ Пользователь {user_id} обновлен в Google Sheets")
                else:
                    worksheet.append_row(row_data)
                    logging.info(f"✅ Пользователь {user_id} добавлен в Google Sheets")
                worksheet.append_row(row_data)
                logging.info(f"✅ Пользователь {user_id} добавлен в Google Sheets")

        except Exception as e:
            logging.error(f"❌ Ошибка обновления пользователя в Google Sheets: {e}")

    def sync_batch_to_sheets(self, batch):
        """Синхронизация партии фермера в Google Sheets"""
        try:
            if not self.spreadsheet:
                logging.error("❌ Нет подключения к Google Sheets")
                return

            headers = [
                "ID",
                "Фермер ID",
                "Культура",
                "Объём (т)",
                "Цена (₽/т)",
                "Регион",
                "Влажность (%)",
                "Протеин (%)",
                "Клейковина (%)",
                "Сорность (%)",
                "Дата готовности",
                "Статус",
                "Создано",
                "Обновлено",
            ]
            worksheet = self.get_or_create_worksheet("Batches", headers)

            if not worksheet:
                return

            row_data = [
                str(batch.get("id", "")),
                str(batch.get("farmer_id", "")),
                str(batch.get("culture", "")),
                str(batch.get("volume", 0)),
                str(batch.get("price", 0)),
                str(batch.get("region", "")),
                str(batch.get("moisture", "")),
                str(batch.get("protein", "")),
                str(batch.get("gluten", "")),
                str(batch.get("weediness", "")),
                str(batch.get("readiness_date", "")),
                str(batch.get("status", "active")),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ]

            batch_id = str(batch.get("id", ""))
            try:
                cell = worksheet.find(batch_id)
                if cell:
                    worksheet.update(f"A{cell.row}:N{cell.row}", [row_data])
                    logging.info(f"✅ Партия {batch_id} обновлена в Google Sheets")
                else:
                    worksheet.append_row(row_data)
                    logging.info(f"✅ Партия {batch_id} добавлена в Google Sheets")
                worksheet.append_row(row_data)
                logging.info(f"✅ Партия {batch_id} добавлена в Google Sheets")

        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации партии в Google Sheets: {e}")

    def update_batch_in_sheets(self, batch):
        """Обновление партии в Google Sheets"""
        self.sync_batch_to_sheets(batch)

    def delete_batch_from_sheets(self, batch_id):
        """Удаление партии из Google Sheets"""
        try:
            if not self.spreadsheet:
                logging.error("❌ Нет подключения к Google Sheets")
                return

            worksheet = self.spreadsheet.worksheet("Batches")
            cell = worksheet.find(str(batch_id))
            if cell:
                worksheet.delete_rows(cell.row)
                logging.info(f"✅ Партия {batch_id} удалена из Google Sheets")

        except Exception as e:
            logging.error(f"❌ Ошибка удаления партии из Google Sheets: {e}")

    def sync_pull_to_sheets(self, pull):
        """Синхронизация пула в Google Sheets"""
        try:
            if not self.spreadsheet:
                logging.error("❌ Нет подключения к Google Sheets")
                return

            headers = [
                "ID",
                "Экспортер ID",
                "Культура",
                "Целевой объём (т)",
                "Текущий объём (т)",
                "Цена (₽/т)",
                "Влажность (%)",
                "Сорность (%)",
                "Статус",
                "Создано",
                "Обновлено",
            ]
            worksheet = self.get_or_create_worksheet("Pulls", headers)

            if not worksheet:
                return

            row_data = [
                str(pull.get("id", "")),
                str(pull.get("exporter_id", "")),
                str(pull.get("culture", "")),
                str(pull.get("target_volume", 0)),
                str(pull.get("current_volume", 0)),
                str(pull.get("price", 0)),
                str(pull.get("moisture", "")),
                str(pull.get("impurity", "")),
                str(pull.get("status", "active")),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ]

            pull_id = str(pull.get("id", ""))
            try:
                cell = worksheet.find(pull_id)
                if cell:
                    worksheet.update(f"A{cell.row}:K{cell.row}", [row_data])
                    logging.info(f"✅ Пул {pull_id} обновлен в Google Sheets")
                else:
                    worksheet.append_row(row_data)
                    logging.info(f"✅ Пул {pull_id} добавлен в Google Sheets")
                worksheet.append_row(row_data)
                logging.info(f"✅ Пул {pull_id} добавлен в Google Sheets")

        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации пула в Google Sheets: {e}")

    def update_pull_in_sheets(self, pull):
        """Обновление пула в Google Sheets"""
        self.sync_pull_to_sheets(pull)

    def sync_deal_to_sheets(self, deal):
        """Синхронизация сделки в Google Sheets"""
        try:
            if not self.spreadsheet:
                logging.error("❌ Нет подключения к Google Sheets")
                return

            headers = [
                "ID",
                "Пул ID",
                "Партия ID",
                "Фермер ID",
                "Экспортер ID",
                "Объём (т)",
                "Цена (₽/т)",
                "Статус",
                "Создано",
            ]
            worksheet = self.get_or_create_worksheet("Deals", headers)

            if not worksheet:
                return

            row_data = [
                str(deal.get("id", "")),
                str(deal.get("pull_id", "")),
                str(deal.get("batch_id", "")),
                str(deal.get("farmer_id", "")),
                str(deal.get("exporter_id", "")),
                str(deal.get("volume", 0)),
                str(deal.get("price", 0)),
                str(deal.get("status", "pending")),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ]

            worksheet.append_row(row_data)
            logging.info("✅ Сделка добавлена в Google Sheets")

        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации сделки в Google Sheets: {e}")


# ====================================================================
# ИНИЦИАЛИЗАЦИЯ GOOGLE SHEETS (асинхронная)
# ====================================================================
gs = None


async def init_google_sheets():
    """Асинхронная инициализация Google Sheets"""
    global gs
    try:
        if not GOOGLE_SHEETS_AVAILABLE:
            logging.warning("⚠️ Google Sheets отключён (GOOGLE_SHEETS_AVAILABLE=False)")
            return

        if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS):
            logging.warning(
                f"⚠️ Файл credentials не найден: {GOOGLE_SHEETS_CREDENTIALS}"
            )
            return

        logging.info("🔄 Инициализация Google Sheets Manager...")

        # Создаём менеджер в отдельном потоке чтобы не блокировать
        loop = asyncio.get_event_loop()
        gs = await loop.run_in_executor(
            None, GoogleSheetsManager, GOOGLE_SHEETS_CREDENTIALS, SPREADSHEET_ID
        )

        if gs and gs.spreadsheet:
            logging.info("✅ Google Sheets Manager инициализирован")
        else:
            logging.warning("⚠️ Google Sheets Manager создан но spreadsheet недоступен")
            gs = None

    except Exception as e:
        logging.error(f"❌ Ошибка инициализации Google Sheets: {e}")
        gs = None


# ============================================================================
# ОБРАБОТЧИКИ ДЛЯ ПРЕДЛОЖЕНИЙ ЛОГИСТОВ (ФЕРМЕР)
# ============================================================================
@dp.message_handler(
    lambda message: message.text == "🚚 Предложения логистов", state="*"
)
async def farmer_view_logistics_offers(message: types.Message, state: FSMContext):
    await state.finish()

    user_id = message.from_user.id

    # Проверяем роль пользователя
        await message.answer("❌ Эта функция доступна только фермерам")
        return

        await message.answer(
            "📭 <b>Предложения логистов пока отсутствуют</b>\n\n"
            parse_mode="HTML",
        )
        return

    active_offers = [
    ]

    if not active_offers:
        await message.answer(
            parse_mode="HTML",
        )
        return

    text = "<b>🚚 ДОСТУПНЫЕ ПРЕДЛОЖЕНИЯ ЛОГИСТОВ</b>\n\n"
    text += f"📊 Найдено предложений: <b>{len(active_offers)}</b>\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)




        button_text = (
            f"{emoji} {logist_name[:15]} | "
        )

        keyboard.add(
            InlineKeyboardButton(
                button_text, callback_data=f"farmer_view_offer:{offer_id}"
            )
        )

    if len(active_offers) > 10:
        text += f"\n<i>ℹ️ Ещё {len(active_offers) - 10} предложений доступно</i>"

    keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_menu"))

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query_handler(lambda c: c.data.startswith("farmer_view_offer:"), state="*")
async def farmer_view_offer_details(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    try:
        offer_id = parse_callback_id(callback.data)
    except ValueError:
        await callback.answer("❌ Ошибка при загрузке предложения", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

        await callback.answer("❌ Логист не найден", show_alert=True)
        return


    text = f"""

<b>📅 Создано:</b> {offer.get('created_at', 'Не указано')}

Имя: {logist.get('name', 'Не указано')}
ИНН: <code>{logist.get('inn', 'Не указан')}</code>
ОГРН: <code>{logist.get('ogrn', 'Не указан')}</code>
📱 Телефон: <code>{logist.get('phone', 'Не указан')}</code>
📧 Email: <code>{logist.get('email', 'Не указан')}</code>




    keyboard = InlineKeyboardMarkup(row_width=2)

    phone = logist.get("phone", "")
    if phone:
        keyboard.add(
            InlineKeyboardButton("📞 Позвонить", url=f"tel:{phone}"),
            InlineKeyboardButton("💬 Написать", url=f"tg://user?id={logist_id}"),
        )

        keyboard.add(
            InlineKeyboardButton(
            ),
            InlineKeyboardButton(
                "❌ Отклонить", callback_data=f"farmer_reject_offer:{offer_id}"
            ),
        )

    keyboard.add(
        InlineKeyboardButton("◀️ Назад к списку", callback_data="farmer_back_to_offers")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("farmer_accept_offer:"), state="*"
)
async def farmer_accept_logistics_offer(
    callback: types.CallbackQuery, state: FSMContext
):
    await state.finish()

    user_id = callback.from_user.id

    try:
        offer_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка при обработке", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

        return

    offer["farmer_id"] = user_id



    farmer_msg = f"""
✅ <b>ПРЕДЛОЖЕНИЕ ПРИНЯТО!</b>

📋 ID предложения: #{offer_id}
📅 Время принятия: {offer['accepted_at']}

Имя: {logist_user.get('name', 'Не указано')}
📱 Телефон: <code>{logist_user.get('phone', 'Не указан')}</code>
📧 Email: <code>{logist_user.get('email', 'Не указан')}</code>



    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            "📞 Позвонить логисту", url=f"tel:{logist_user.get('phone', '')}"
        ),
        InlineKeyboardButton("💬 Написать в Telegram", url=f"tg://user?id={logist_id}"),
    )
    keyboard.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_menu"))

    await callback.message.edit_text(
        farmer_msg, reply_markup=keyboard, parse_mode="HTML"
    )

        logist_msg = f"""

📋 ID предложения: #{offer_id}
📅 Время принятия: {offer['accepted_at']}

Имя: {farmer_user.get('name', 'Не указано')}
Регион: {farmer_user.get('region', 'Не указан')}
📱 Телефон: <code>{farmer_user.get('phone', 'Не указан')}</code>
📧 Email: <code>{farmer_user.get('email', 'Не указан')}</code>

<b>🚛 ДЕТАЛИ ПЕРЕВОЗКИ:</b>


        try:
            await bot.send_message(logist_id, logist_msg, parse_mode="HTML")
            logging.info(f"Уведомление логисту #{logist_id} отправлено успешно")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления логисту: {e}")

    await callback.answer(
        "✅ Предложение принято! Логист получит уведомление.", show_alert=True
    )


@dp.callback_query_handler(
    lambda c: c.data.startswith("farmer_reject_offer:"), state="*"
)
async def farmer_reject_logistics_offer(
    callback: types.CallbackQuery, state: FSMContext
):
    await state.finish()

    user_id = callback.from_user.id

    try:
        offer_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка при обработке", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

        return

    offer["farmer_id"] = user_id



    farmer_msg = f"""
❌ <b>ПРЕДЛОЖЕНИЕ ОТКЛОНЕНО</b>

📋 ID предложения: #{offer_id}
📅 Время отклонения: {offer['rejected_at']}

Логист получит уведомление об отклонении.

"""

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            "🔄 Другие предложения", callback_data="view_logistics_offers_btn"
        )
    )
    keyboard.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_menu"))

    await callback.message.edit_text(
        farmer_msg, reply_markup=keyboard, parse_mode="HTML"
    )

        logist_msg = f"""

📋 ID предложения: #{offer_id}
📅 Время отклонения: {offer['rejected_at']}


"""
        try:
            await bot.send_message(logist_id, logist_msg, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления об отклонении: {e}")

    await callback.answer("✅ Предложение отклонено", show_alert=True)


@dp.message_handler(lambda message: message.text == "📋 История предложений", state="*")
async def view_farmer_offers_history(message: types.Message, state: FSMContext):
    await state.finish()

    user_id = message.from_user.id

        await message.answer("❌ Эта функция доступна только фермерам")
        return

        await message.answer("📭 История предложений пуста")
        return


    if active:
        for offer in active[:5]:
        text += "\n"

    if accepted:
        for offer in accepted[:5]:
        text += "\n"

    if rejected:
        for offer in rejected[:5]:

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            "🚚 Новые предложения", callback_data="view_logistics_offers_btn"
        )
    )
    keyboard.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_menu"))

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query_handler(lambda c: c.data == "farmer_back_to_offers", state="*")
    """Возврат к списку предложений"""
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("view_deal:"), state="*")
async def view_deal_details(callback: types.CallbackQuery):
    """Просмотр деталей сделки"""
    deal_id = parse_callback_id(callback.data)

        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return

    text = f"📋 <b>Сделка #{deal_id}</b>\n\n"

    text += f"📊 Статус: {DEAL_STATUSES.get(deal.get('status', 'pending'), deal.get('status'))}\n"


    if deal.get("exporter_id"):
        text += f"📦 Экспортёр: {exporter_name}\n"

        text += f"🌾 Фермеров: {farmers_count}\n"

    if deal.get("logistic_id"):
        text += f"🚚 Логист: {logistic_name}\n"

    if deal.get("expeditor_id"):
        text += f"🚛 Экспедитор: {expeditor_name}\n"

    if deal.get("created_at"):

    if deal.get("completed_at"):

    keyboard = deal_actions_keyboard(deal_id)

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.message_handler(lambda m: m.text == "🔍 Поиск экспортёров", state="*")
async def search_exporters(message: types.Message, state: FSMContext):
    """Поиск экспортёров для фермера — универсальная версия"""
    await state.finish()

    user_id = message.from_user.id
        await message.answer("❌ Эта функция доступна только фермерам")
        return

    # ═══════════════════════════════════════════════════════════════
    # УНИВЕРСАЛЬНАЯ ОБРАБОТКА СТРУКТУРЫ BATCHES
    # ═══════════════════════════════════════════════════════════════

    farmer_batches = {}

    for key, value in batches.items():
        # Случай 1: Новая структура {batch_id: {farmer_id: ..., ...}}
        if isinstance(value, dict):
            # Проверяем наличие farmer_id в словаре
                if value.get("status") in ["active", "Активна", "активна", "", None]:
                    farmer_batches[key] = value

        # Случай 2: Старая структура {user_id: [batch1, batch2, ...]}
        elif isinstance(value, list):
            # Если ключ совпадает с user_id, это список партий этого фермера
                for batch in value:
                    if isinstance(batch, dict):
                        batch_id = batch.get("id")
                        if batch.get("status") in [
                            "active",
                            "Активна",
                            "активна",
                            "",
                            None,
                        ]:
                            farmer_batches[batch_id] = batch

    if not farmer_batches:
        await message.answer(
            "📦 У вас пока нет активных партий для поиска экспортёров.\n\n"
            "Сначала добавьте партию через меню '➕ Добавить партию'",
            reply_markup=farmer_keyboard(),
        )
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    for batch_id, batch in list(farmer_batches.items())[:10]:
        culture = batch.get("culture", "—")
        volume = batch.get("volume", 0)
        price = batch.get("price", 0)
        button_text = f"🌾 {culture} - {volume:.0f} т • {price:,.0f} ₽/т"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"findexporters:{batch_id}")
        )

    await message.answer(
        "🔍 <b>Поиск экспортёров</b>\n\n"
        "Выберите партию для поиска подходящих экспортёров:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.callback_query_handler(lambda c: c.data.startswith("findexporters:"), state="*")
async def process_find_exporters(callback: types.CallbackQuery):
    """Обработка выбора партии для поиска экспортёров — финальная версия"""
    try:
        batch_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    await callback.answer("🔍 Ищем подходящих экспортёров...")

    # ═══════════════════════════════════════════════════════════════
    # УНИВЕРСАЛЬНЫЙ ПОИСК ПАРТИИ В ОБЕИХ СТРУКТУРАХ
    # ═══════════════════════════════════════════════════════════════

    batch = None

    # Случай 1: Новая структура {batch_id: {farmer_id: ..., ...}}
    if batch_id in batches and isinstance(batches[batch_id], dict):
        batch = batches[batch_id]

    # Случай 2: Старая структура {user_id: [batch1, batch2, ...]}
    else:
        for key, value in batches.items():
            if isinstance(value, list):
                for b in value:
                        batch = b
                        break
            if batch:
                break

    if not batch:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_batches")
        )
        await callback.message.edit_text("❌ Партия не найдена", reply_markup=keyboard)
        return

    # Получаем реальные пулы (универсальная структура)
    true_pulls = None
    if (
        isinstance(pulls, dict)
        and "pulls" in pulls
        and isinstance(pulls["pulls"], dict)
    ):
        true_pulls = pulls["pulls"]
    elif isinstance(pulls, dict):
        true_pulls = pulls
    else:
        true_pulls = {}

    # Фильтрация совпадений
    matching_pulls = []
    for pull_id, pull in true_pulls.items():
        # Проверка что pull - словарь
        if not isinstance(pull, dict):
            logging.warning(f"⚠️ Пул {pull_id} не является словарём: {type(pull)}")
            continue

        # Проверка статуса
            continue

        # Проверка культуры
        if pull.get("culture") != batch.get("culture"):
            continue

        # Проверка объёма
        target_volume = pull.get("target_volume", 0)
        current_volume = pull.get("current_volume", 0)
        remaining = target_volume - current_volume

        if remaining < batch.get("volume", 0):
            continue

        # Получаем экспортёра
        exporter_id = pull.get("exporter_id")

        # Флаг предупреждения о цене
        pull_price = pull.get("price", 0)
        batch_price = batch.get("price", 0)
        price_warning = pull_price < batch_price

        matching_pulls.append(
            {
                "pull_id": pull_id,
                "pull": pull,
                "exporter": exporter,
                "exporter_id": exporter_id,
                "remaining_volume": remaining,
                "price_warning": price_warning,
            }
        )

    # Если ничего не найдено
    if not matching_pulls:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_batches")
        )

        await callback.message.edit_text(
            f"🔍 <b>Результаты поиска для партии #{batch_id}</b>\n\n"
            f"🌾 {batch.get('culture', '—')} - {batch.get('volume', 0):.0f} т\n"
            f"💰 {batch.get('price', 0):,.0f} ₽/т\n\n"
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        return

    # Формируем сообщение с результатами
    text = (
        f"🎯 <b>Найдено {len(matching_pulls)} подходящих экспортёров</b>\n\n"
        f"🌾 {batch.get('culture', '—')} - {batch.get('volume', 0):.0f} т\n"
        f"💰 {batch.get('price', 0):,.0f} ₽/т\n"
        f"📍 {batch.get('region', 'Не указан')}\n\n"
    )

    for idx, match in enumerate(matching_pulls[:10], 1):
        pull = match["pull"]
        exporter = match["exporter"]
        company = exporter.get("company_name") or exporter.get("name", "—")
        phone = exporter.get("phone", "Не указан")
        email = exporter.get("email", "Не указан")
        pull_price = pull.get("price", 0)

        text += (
            f"{idx}. <b>Экспортёр:</b> {company}\n" f"   💰 Цена: {pull_price:,.0f} ₽/т"
        )

        # Добавляем предупреждение о низкой цене
        if match.get("price_warning"):
            text += f" ⚠️ <i>(ниже вашей {batch.get('price', 0):,.0f} ₽/т)</i>"

        text += (
            f"   📦 Нужно: {pull.get('target_volume', 0):.0f} т (свободно: {match['remaining_volume']:.0f} т)\n"
            f"   🚢 Порт: {pull.get('port', '—')}\n"
            f"   📞 Телефон: <code>{phone}</code>\n"
            f"   📧 Email: <code>{email}</code>\n\n"
        )

    if len(matching_pulls) > 10:
        text += f"... и ещё {len(matching_pulls) - 10} предложений\n\n"

    text += "💡 Свяжитесь с экспортёром для обсуждения условий!"

    # ═══════════════════════════════════════════════════════════════
    # КЛАВИАТУРА: ТОЛЬКО КНОПКА "НАЗАД"
    # ═══════════════════════════════════════════════════════════════
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_batches"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")


@dp.message_handler(lambda m: m.text == "➕ Добавить партию", state="*")
async def add_batch_start(message: types.Message, state: FSMContext):
    """Начало добавления партии фермером — исправленная версия"""
    await state.finish()

    user_id = message.from_user.id
        await message.answer("❌ Эта функция доступна только фермерам")
        return

    await message.answer(
        "📦 <b>Добавление партии</b>\n\n" "Шаг 1 из 9\n\n" "Выберите культуру:",
        reply_markup=culture_keyboard(),
        parse_mode="HTML",
    )

    await AddBatch.culture.set()  # Переход FSM ПОСЛЕ отправки сообщения


@dp.callback_query_handler(
    lambda c: c.data.startswith("culture:"), state=SearchByCulture.waiting_culture
)
async def search_by_culture_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик выбора культуры для поиска — исправленная версия с полным функционалом"""
    try:
        # Извлекаем название культуры
        culture = callback.data.split(":", 1)[1]

        # Логирование для отладки
        logging.info(f"🔍 Ищем партии по культуре: {culture}")
        logging.info(f"📊 Всего партий в базе: {len(batches)}")

    except (IndexError, ValueError) as e:
        logging.error(f"❌ Ошибка извлечения культуры: {e}")
        await callback.answer("❌ Ошибка выбора культуры", show_alert=True)
        return

    # ════════════════════════════════════════════════════════════════════════════
    # ПОИСК ПАРТИЙ ПО КУЛЬТУРЕ
    # ════════════════════════════════════════════════════════════════════════════

    found_batches = []

        # Извлекаем данные партии
        batch_culture = batch.get("culture", "")
        batch_status = batch.get("status", "")

        logging.info(
            f"  Партия #{batch_id}: culture='{batch_culture}', status='{batch_status}'"
        )

        # Сравниваем культуры (игнорируем регистр и пробелы)
        if batch_culture.strip().lower() == culture.strip().lower():
            # Проверяем статус
            if batch_status.lower() in [
                "active",
                "активна",
                "available",
                "доступна",
                "",  # пустой статус = активна
            ]:

    logging.info(f"🎯 Итого найдено партий: {len(found_batches)}")

    # ════════════════════════════════════════════════════════════════════════════
    # ОБРАБОТКА РЕЗУЛЬТАТОВ ПОИСКА
    # ════════════════════════════════════════════════════════════════════════════

    if not found_batches:
        # Партии не найдены
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🔍 Назад к поиску", callback_data="back_to_search")
        )

        await callback.message.edit_text(
            f"❌ <b>По культуре '{culture}' партий не найдено.</b>\n\n"
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        await state.finish()
        await callback.answer()
        return

    # ════════════════════════════════════════════════════════════════════════════
    # ФОРМИРОВАНИЕ РЕЗУЛЬТАТА
    # ════════════════════════════════════════════════════════════════════════════

    # Сортируем по цене
    found_batches_sorted = sorted(found_batches, key=lambda x: x.get("price", 0))

    # Формируем сообщение
    text = f"🌾 <b>Найдено партий:</b> {len(found_batches)}\n\n"
    text += f"<b>Культура:</b> {culture}\n"
    text += f"<b>Количество партий:</b> {len(found_batches)}\n\n"

    # Добавляем детали первых 10 партий
    for idx, batch in enumerate(found_batches_sorted[:10], 1):
        batch_id = batch.get("batch_id") or batch.get("id", 0)
        batch_culture = batch.get("culture", "Не указана")
        volume = batch.get("volume", 0)
        price = batch.get("price", 0)
        region = batch.get("region", "Не указан")
        farmer_id = batch.get("farmer_id")

        # Получаем информацию о фермере
        farmer_name = farmer.get("name", "Не указано")
        farmer_phone = farmer.get("phone", "Не указан")

        text += f"{idx}. <b>{batch_culture}</b> - {volume:.0f} т\n"
        text += f"   💰 {price:,.0f} ₽/т | 📍 {region}\n"
        text += f"   👤 Фермер: {farmer_name}\n"
        text += f"   📞 Телефон: {farmer_phone}\n\n"

    if len(found_batches_sorted) > 10:
        text += f"...и ещё {len(found_batches_sorted) - 10} партий\n"

    # ════════════════════════════════════════════════════════════════════════════
    # СОЗДАНИЕ КЛАВИАТУРЫ
    # ════════════════════════════════════════════════════════════════════════════

    keyboard = InlineKeyboardMarkup(row_width=1)

    # Добавляем кнопки для первых 10 партий
    for batch in found_batches_sorted[:10]:
        batch_id = batch.get("batch_id") or batch.get("id", 0)
        batch_culture = batch.get("culture", "Не указана")
        volume = batch.get("volume", 0)
        price = batch.get("price", 0)

        keyboard.add(
            InlineKeyboardButton(
                f"{batch_culture} - {volume:.0f} т | {price:,.0f} ₽/т",
                callback_data=f"view_batch:{batch_id}",
            )
        )

    keyboard.add(
        InlineKeyboardButton("◀️ Назад к поиску", callback_data="back_to_search")
    )

    # ════════════════════════════════════════════════════════════════════════════
    # ОТПРАВКА РЕЗУЛЬТАТА
    # ════════════════════════════════════════════════════════════════════════════

    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка edit_text: {e}")
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")

    await state.finish()

    logging.info(f"✅ Показано {len(found_batches)} партий по культуре '{culture}'")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("contact_farmer:"))
async def contact_farmer_callback(callback: types.CallbackQuery):
    """Обработчик кнопки Связаться с фермером - показывает полные контакты"""
    try:
        logging.info(f"📞 Запрос контактов для партии {batch_id}")

            await callback.answer("❌ Партия не найдена", show_alert=True)
            logging.warning(f"❌ Партия {batch_id} не найдена в batches")
            return

        # Проверяем, что это словарь
        if not isinstance(found_batch, dict):
            await callback.answer("❌ Ошибка данных партии", show_alert=True)
            logging.error(
                f"❌ Партия {batch_id} не является словарём: {type(found_batch)}"
            )
            return

        # Получаем farmer_id из партии
        if not farmer_id:
            await callback.answer("❌ Фермер не указан", show_alert=True)
            logging.warning(f"❌ У партии {batch_id} нет farmer_id")
            return

        # Получаем информацию о фермере
        farmer_name = farmer.get("name", "Фермер")
        farmer_phone = farmer.get("phone", "Не указан")
        farmer_email = farmer.get("email", "Не указан")
        farmer_company = farmer.get("company_name", "Не указана")
        farmer_inn = farmer.get("inn", "Не указан")
        farmer_region = farmer.get("region", "Не указан")

        # Получаем информацию о партии
        culture = found_batch.get("culture", "N/A")
        volume = found_batch.get("volume", 0)
        price = found_batch.get("price", 0)
        region = found_batch.get("region", "Не указан")
        quality = found_batch.get("quality_class", "Не указан")
        moisture = found_batch.get("moisture", "—")
        impurity = found_batch.get("impurity", "—")
        storage = found_batch.get("storage_type", "Не указан")
        readiness = found_batch.get("readiness_date", "Не указана")

        # Формируем красивое сообщение с контактами
        text += f"<b>📦 ПАРТИЯ #{batch_id}</b>\n"
        text += f"<b>Культура:</b> {culture}\n"
        text += f"<b>Объём:</b> {volume:,.1f} т\n"
        text += f"<b>Цена:</b> {price:,.0f} ₽/т\n"
        text += f"<b>Регион:</b> {region}\n"
        text += f"<b>Класс качества:</b> {quality}\n"
        text += f"<b>Влажность:</b> {moisture}%\n"
        text += f"<b>Сорность:</b> {impurity}%\n"
        text += f"<b>Хранение:</b> {storage}\n"
        text += f"<b>Готовность:</b> {readiness}\n\n"
        text += f"<b>Имя:</b> {farmer_name}\n"
        text += f"<b>Компания:</b> {farmer_company}\n"
        text += f"<b>ИНН:</b> {farmer_inn}\n"
        text += f"<b>Регион:</b> {farmer_region}\n"
        text += f"<b>Телефон:</b> <code>{farmer_phone}</code>\n"
        text += f"<b>Email:</b> <code>{farmer_email}</code>\n\n"

        keyboard = InlineKeyboardMarkup(row_width=1)
        # Кнопки для прямого контакта
        if farmer_phone and farmer_phone != "Не указан":
            keyboard.add(
                InlineKeyboardButton(
                    f"📞 Позвонить {farmer_phone}", url=f"tel:{farmer_phone}"
                )
            )
        keyboard.add(
            InlineKeyboardButton(
                "💬 Написать в Telegram", url=f"tg://user?id={farmer_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton("🔙 Назад к результатам", callback_data="startsearch")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        logging.info(
            f"✅ Контакты отправлены для партии {batch_id}, фермер {farmer_id}"
        )

    except Exception as e:
        logging.error(f"❌ Ошибка при получении контактов: {e}")
        await callback.answer("❌ Ошибка получения контактов", show_alert=True)

    await callback.answer()


# Глобальные переменные (если ещё не определены)
@dp.callback_query_handler(
    lambda c: c.data.startswith("culture:"), state=AddBatch.culture
)
async def add_batch_culture(callback: types.CallbackQuery, state: FSMContext):
    """Выбор культуры для партии"""
    try:
        culture = callback.data.split(":", 1)[1]
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга: {e}, data: {callback.data}")
        return

    await state.update_data(culture=culture)

    await callback.message.edit_text(
        "📦 <b>Добавление партии</b>\n\n" "Шаг 2 из 9\n\n" "Выберите регион:",
        reply_markup=region_keyboard(),
        parse_mode="HTML",
    )
    await AddBatch.region.set()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("region:"), state=AddBatch.region
)
async def add_batch_region(callback: types.CallbackQuery, state: FSMContext):
    """Выбор региона для партии"""
    region = callback.data.split(":", 1)[1]
    await state.update_data(region=region)

    await callback.message.edit_text(
        "📦 <b>Добавление партии</b>\n\n"
        "Шаг 3 из 9\n\n"
        "Введите объём партии (в тоннах):",
        parse_mode="HTML",
    )
    await AddBatch.volume.set()
    await callback.answer()


@dp.message_handler(state=AddBatch.volume)
async def add_batch_volume(message: types.Message, state: FSMContext):
    """Ввод объёма партии"""
    try:
        volume = float(message.text.strip().replace(",", "."))
        if volume <= 0:
            raise ValueError

        await state.update_data(volume=volume)

        await message.answer(
            "📦 <b>Добавление партии</b>\n\n"
            "Шаг 4 из 9\n\n"
            "Введите цену (₽/тонна):",
            parse_mode="HTML",
        )
        await AddBatch.price.set()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число больше 0:")


@dp.message_handler(state=AddBatch.price)
async def add_batch_price(message: types.Message, state: FSMContext):
    """Ввод цены партии"""
    try:
        price = float(message.text.strip().replace(",", "."))
        if price <= 0:
            raise ValueError

        await state.update_data(price=price)

        await message.answer(
            "📦 <b>Добавление партии</b>\n\n" "Шаг 5 из 9\n\n" "Введите влажность (%):",
            parse_mode="HTML",
        )
        await AddBatch.humidity.set()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число больше 0:")


@dp.message_handler(state=AddBatch.humidity)
async def add_batch_humidity(message: types.Message, state: FSMContext):
    """Ввод влажности партии"""
    try:
        humidity = float(message.text.strip().replace(",", "."))
        if not 0 <= humidity <= 100:
            raise ValueError

        await state.update_data(humidity=humidity)

        await message.answer(
            "📦 <b>Добавление партии</b>\n\n" "Шаг 6 из 9\n\n" "Введите сорность (%):",
            parse_mode="HTML",
        )
        await AddBatch.impurity.set()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число от 0 до 100:")


@dp.message_handler(state=AddBatch.impurity)
async def add_batch_impurity(message: types.Message, state: FSMContext):
    """Ввод сорности партии"""
    try:
        impurity = float(message.text.strip().replace(",", "."))
        if not 0 <= impurity <= 100:
            raise ValueError

        await state.update_data(impurity=impurity)
        data = await state.get_data()
        quality_class = determine_quality_class(data["humidity"], impurity)
        await state.update_data(quality_class=quality_class)

        await message.answer(
            "📦 <b>Добавление партии</b>\n\n"
            "Шаг 7 из 9\n\n"
            f"Автоматически определен класс качества: <b>{quality_class}</b>\n\n"
            "Выберите тип хранения:",
            reply_markup=storage_type_keyboard(),
            parse_mode="HTML",
        )
        await AddBatch.storage_type.set()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число от 0 до 100:")


@dp.callback_query_handler(
    lambda c: c.data.startswith("storage:"), state=AddBatch.storage_type
)
async def add_batch_storage_type(callback: types.CallbackQuery, state: FSMContext):
    """Выбор типа хранения"""
    storage_type = callback.data.split(":", 1)[1]
    await state.update_data(storage_type=storage_type)

    await callback.message.edit_text(
        "📦 <b>Добавление партии</b>\n\n"
        "Шаг 8 из 9\n\n"
        "Введите дату готовности (в формате ДД.ММ.ГГГГ) или 'сейчас' если готова:",
        parse_mode="HTML",
    )
    await AddBatch.readiness_date.set()
    await callback.answer()


@dp.message_handler(state=AddBatch.readiness_date)
async def add_batch_readiness_date(message: types.Message, state: FSMContext):
    """Завершение добавления расширенной партии"""
    global batch_counter

    readiness_date = message.text.strip()

    if readiness_date.lower() == "сейчас":
        readiness_date = datetime.now().strftime("%d.%m.%Y")
    elif not validate_date(readiness_date):
        await message.answer(
            "❌ Некорректная дата. Используйте формат ДД.ММ.ГГГГ или 'сейчас':"
        )
        return

    user_id = message.from_user.id
    data = await state.get_data()

    # Создаём партию
    batch_counter += 1
    batch = {
        "id": batch_counter,
        "farmer_id": user_id,
        "culture": data["culture"],
        "region": data["region"],
        "volume": data["volume"],
        "price": data["price"],
        "humidity": data["humidity"],
        "impurity": data["impurity"],
        "quality_class": data["quality_class"],
        "storage_type": data["storage_type"],
        "readiness_date": readiness_date,
        "status": "Активна",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "files": [],
        "matches": [],
    }

    # Добавляем партию в базу
    if user_id not in batches:
        batches[user_id] = []
    batches[user_id].append(batch)

    save_batches_to_pickle()

    # ✅ АВТОПРИСОЕДИНЕНИЕ К ПУЛУ (если партия создавалась для пула)
    if "create_batch_for_pull_id" in data:
        pull_id = data["create_batch_for_pull_id"]



                # Проверяем что ещё не присоединились
                already_joined = any(
                )

                if not already_joined:
                    participant = {
                        "farmer_id": user_id,
                        "batch_id": batch["id"],
                        "volume": batch["volume"],
                        "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    pull["current_volume"] += batch["volume"]


                    save_pulls_to_pickle()
                    save_batches_to_pickle()

                    logging.info(
                        f"✅ Партия #{batch['id']} автоматически присоединена к пулу #{pull_id}"
                    )

                    # Уведомление экспортёру о новой партии
                    try:
                        # ✅ ПРОВЕРКА НАЛИЧИЯ ЭКСПОРТЁРА
                        if "exporter_id" in pull and pull["exporter_id"]:
                            farmer_card = format_farmer_card(user_id, batch["id"])

                            await bot.send_message(
                                pull["exporter_id"],
                                f"🎉 <b>Новая партия присоединена к пулу #{pull_id}!</b>\n\n{farmer_card}",
                                parse_mode="HTML",
                            )
                            logging.info(
                                f"✅ Уведомление экспортёру {pull['exporter_id']} отправлено"
                            )
                        else:
                            logging.warning(f"⚠️ В пуле {pull_id} нет exporter_id")
                    except Exception as e:
                        logging.error(f"❌ Ошибка отправки уведомления экспортёру: {e}")

                    # Уведомление фермеру об успешном присоединении
                    await message.answer(
                        f"✅ <b>Партия #{batch['id']} создана и автоматически присоединена к пулу #{pull_id}!</b>\n\n"
                        f"🌾 {batch['culture']}\n"
                        f"📦 Объём: {batch['volume']} т\n"
                        f"💰 Цена: {batch['price']:,.0f} ₽/т\n\n"
                        parse_mode="HTML",
                        reply_markup=get_role_keyboard("farmer"),
                    )

                    await state.finish()
                    return

    # ✅ ОБЫЧНЫЙ ФЛОУ (партия НЕ для конкретного пула)

    # Синхронизация с Google Sheets
    if gs and gs.spreadsheet:
        try:
            gs.sync_batch_to_sheets(batch)
            await publish_batch_to_channel(batch, farmer_name)
        except Exception as e:
            logging.error(f"Ошибка синхронизации с Google Sheets: {e}")

    # Ищем подходящие пулы
    matching_pulls = await find_matching_exporters(batch)

    keyboard = get_role_keyboard("farmer")

    message_text = (
        f"✅ <b>Партия #{batch['id']} добавлена!</b>\n\n"
        f"🌾 Культура: {batch['culture']}\n"
        f"📍 Регион: {batch.get('region', 'Не указан')}\n"
        f"📦 Объём: {batch['volume']} т\n"
        f"💰 Цена: {batch['price']:,.0f} ₽/т\n"
        f"💧 Влажность: {batch.get('humidity', 'Не указано')}%\n"
        f"🌾 Сорность: {batch.get('impurity', 'Не указано')}%\n"
        f"⭐ Класс: {batch.get('quality_class', 'Не указано')}\n"
        f"🏭 Хранение: {batch.get('storage_type', 'Не указано')}\n"
        f"📅 Готовность: {batch.get('readiness_date', 'Не указано')}"
    )

    if matching_pulls:
        message_text += f"\n\n🎯 Найдено подходящих пулов: {len(matching_pulls)}"

        # ✅ СОЗДАЕМ match-объекты И СОХРАНЯЕМ В matches
        match_objs = []
        for pull_dict in matching_pulls:
            pull_id = pull_dict["pull_id"]

            # Создаём match-объект для notify_match()
            match_obj = {
                "pull_id": pull_id,
                "exporter_company": pull_dict.get("exporter_company", ""),
                "exporter_name": pull_dict.get("exporter_name", ""),
                "price": pull_dict.get("price", 0),
                "port": pull_dict.get("port", ""),
                "current_volume": pull_dict.get("current_volume", 0),
                "target_volume": pull_dict.get("target_volume", 0),
            }
            match_objs.append(match_obj)

            # ✅ СОХРАНЯЕМ В matches
            matches[match_id] = {
                "id": match_id,
                "batch_id": batch["id"],
                "pull_id": pull_id,
                "status": "active",
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        await asyncio.sleep(0.5)
        await notify_match(user_id, batch, match_objs)

    await message.answer(message_text, reply_markup=keyboard, parse_mode="HTML")
    await state.finish()


@dp.callback_query_handler(lambda c: c.data.startswith("view_matches:"), state="*")
async def view_batch_matches(callback: types.CallbackQuery):
    """Просмотр совпадений для партии"""
    try:
        batch_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError) as e:
        logging.error(f"❌ Ошибка парсинга batch_id: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    # ✅ ИСПРАВЛЕНО: Поиск партии в структуре {farmer_id: [batch_list]}
    batch = None

    for fid, batch_list in batches.items():
        if isinstance(batch_list, list):
            for b in batch_list:
                    batch = b
                    break
        if batch:
            break

    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return

    # Ищем активные совпадения
    batch_matches = []
    for match in matches.values():
            batch_matches.append(match)

    if not batch_matches:
        await callback.answer("🤷‍♂️ Активных совпадений не найдено", show_alert=True)
        return

    # Формируем текст сообщения
    text = f"🎯 <b>Совпадения для партии #{batch_id}</b>\n\n"
    text += f"🌾 <b>{batch.get('culture', '?')}</b> • {batch.get('volume', 0)} т • {batch.get('price', 0):,.0f} ₽/т\n\n"

    # ✅ Получаем пулы
    all_pulls = pulls.get("pulls", {})

    for i, match in enumerate(batch_matches[:5], 1):
        pull_id = match.get("pull_id")
        pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

        if pull and isinstance(pull, dict):
            target_volume = pull.get("target_volume", 0)
            current_volume = pull.get("current_volume", 0)
            progress = (
                (current_volume / target_volume * 100) if target_volume > 0 else 0
            )

            text += f"<b>{i}. Пул #{pull_id}</b>\n"
            text += (
                f"   📦 <b>Нужно:</b> {target_volume} т ({progress:.0f}% заполнено)\n"
            )
            text += f"   💰 <b>Цена:</b> ₽{pull.get('price', 0):,.0f}/т\n"
            text += f"   🚢 <b>Порт:</b> {pull.get('port', '?')}\n"
            text += f"   👤 <b>Экспортёр:</b> {pull.get('exporter_name', '?')}\n"
            text += f"   📋 <b>Условия:</b> {pull.get('doc_type', 'Не указаны')}\n\n"

    if len(batch_matches) > 5:
        text += f"<i>... и ещё {len(batch_matches) - 5} совпадений</i>\n\n"

    text += "💡 <b>Рекомендация:</b> Свяжитесь с экспортёрами для обсуждения деталей."

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data=f"view_batch:{batch_id}")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.message_handler(lambda m: m.text == "🔧 Мои партии", state="*")
async def view_my_batches(message: types.Message, state: FSMContext):
    """Просмотр всех партий фермера с правильными статусами через статус мап"""

    await state.finish()

    user_id = message.from_user.id
        await message.answer("❌ Эта функция доступна только фермерам")
        return

        await message.answer(
            "📦 У вас пока нет добавленных партий.\n\n"
            "Используйте кнопку '➕ Добавить партию' для создания новой."
        )
        return

    # Статус мап для сопоставления с реальным статусом партии
    status_map = {
        "active": ["активна", "matched"],
        "reserved": ["зарезервирована"],
        "sold": ["продана"],
        "withdrawn": ["снята с продажи"],
    }

    def in_status_group(batch_status, group):
        return batch_status.lower() in status_map.get(group, [])

    active_batches = [
        b for b in user_batches if in_status_group(b.get("status", ""), "active")
    ]
    reserved_batches = [
        b for b in user_batches if in_status_group(b.get("status", ""), "reserved")
    ]
    sold_batches = [
        b for b in user_batches if in_status_group(b.get("status", ""), "sold")
    ]
    withdrawn_batches = [
        b for b in user_batches if in_status_group(b.get("status", ""), "withdrawn")
    ]

    keyboard = InlineKeyboardMarkup(row_width=1)

    for batch in active_batches:
        has_matches = any(
            for m in matches.values()
        )
        match_emoji = "🎯 " if has_matches else ""
        button_text = (
            f"{match_emoji}✅ {batch['culture']} - {batch['volume']} т "
            f"({batch['price']:,.0f} ₽/т)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_batch:{batch['id']}")
        )

    for batch in reserved_batches:
        button_text = (
            f"🔒 {batch['culture']} - {batch['volume']} т "
            f"({batch['price']:,.0f} ₽/т)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_batch:{batch['id']}")
        )

    for batch in sold_batches:
        button_text = (
            f"💰 {batch['culture']} - {batch['volume']} т "
            f"({batch['price']:,.0f} ₽/т)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_batch:{batch['id']}")
        )

    for batch in withdrawn_batches:
        button_text = (
            f"❌ {batch['culture']} - {batch['volume']} т "
            f"({batch['price']:,.0f} ₽/т)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_batch:{batch['id']}")
        )

    keyboard.add(
        InlineKeyboardButton("🔄 Автопоиск экспортёров", callback_data="auto_match_all")
    )

    await message.answer(
        f"📦 <b>Ваши партии</b> ({len(user_batches)} шт.)\n\n"
        f"✅ Активные: {len(active_batches)}\n"
        f"🔒 Зарезервированные: {len(reserved_batches)}\n"
        f"💰 Проданные: {len(sold_batches)}\n"
        f"❌ Снятые с продажи: {len(withdrawn_batches)}\n"
        "Нажмите на партию для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.message_handler(lambda m: m.text == "🎯 Пулы", state="*")
async def view_pools_menu(message: types.Message, state: FSMContext):
    """✅ Просмотр пулов для фермера"""
    await state.finish()

    user_id = message.from_user.id
        await message.answer("❌ Эта функция доступна только фермерам")
        return

    # ✅ ИЗМЕНЕНО: Извлекаем пулы из правильного места
    all_pulls = pulls.get("pulls", {})

    if not all_pulls:
        logging.info(f"📊 Пулов не найдено. pulls.keys() = {list(pulls.keys())}")
        await message.answer(
            "🎯 <b>Активные пулы</b>\n\n"
            "Сейчас нет открытых пулов для участия.\n"
            "Пулы создаются экспортёрами для сбора партий зерна.",
            parse_mode="HTML",
        )
        return

    # ✅ ИЗМЕНЕНО: Итерируем по all_pulls
    open_pulls = []
    for pull_id, pull in all_pulls.items():
        if not isinstance(pull, dict):
            logging.warning(f"⚠️ Пул {pull_id} не dict: {type(pull)}")
            continue

            pull_data = pull.copy()
            pull_data["pull_id"] = pull_id
            open_pulls.append(pull_data)

    logging.info(f"📊 Найдено открытых пулов: {len(open_pulls)} из {len(all_pulls)}")

    if not open_pulls:
        await message.answer(
            "🎯 <b>Активные пулы</b>\n\n"
            "Сейчас нет открытых пулов для участия.\n"
            "Пулы создаются экспортёрами для сбора партий зерна.",
            parse_mode="HTML",
        )
        return

    keyboard = InlineKeyboardMarkup(row_width=1)

    for pull in open_pulls[:10]:
        pull_id = pull.get("pull_id")

        if pull_id is None:
            logging.warning(f"⚠️ pull_id is None для пула: {pull}")
            continue

        progress = (
            pull.get("current_volume", 0) / pull.get("target_volume", 1) * 100
            if pull.get("target_volume", 1) > 0
            else 0
        )

        button_text = (
            f"🌾 {pull.get('culture', '?')} - "
            f"{pull.get('target_volume', 0)}т ({progress:.0f}%)"
        )

        # ✅ ИЗМЕНЕНО: с подчёркиванием
        callback_data = f"view_pull:{pull_id}"

        keyboard.add(InlineKeyboardButton(button_text, callback_data=callback_data))

    await message.answer(
        f"🎯 <b>Активные пулы</b> ({len(open_pulls)} шт.)\n\n"
        "Выберите пул для просмотра деталей и присоединения:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.message_handler(state=JoinPullStatesGroup.volume)
async def join_pull_volume(message: types.Message, state: FSMContext):
    """Ввод объёма для присоединения к пулу"""
    try:
        volume = float(message.text.strip().replace(",", ".").replace(" ", ""))
        if volume <= 0:
            raise ValueError

        data = await state.get_data()
        pull_id = data.get("pull_id")
        batch_id = data.get("batch_id")
        user_id = message.from_user.id

        # ✅ ИСПРАВЛЕНО: Получаем пулы из правильного места
        all_pulls = pulls.get("pulls", {})
        pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

        if not pull:
            await message.answer("❌ Пул не найден")
            await state.finish()
            return

        # Проверка доступного места
        target_volume = pull.get("target_volume", 0)
        current_volume = pull.get("current_volume", 0)
        available = target_volume - current_volume

        if volume > available:
            await message.answer(
                f"Доступно: {available:,.0f} т\n"
                f"Вы указали: {volume:,.0f} т"
            )
            return

        pull_id_str = str(pull_id)


            {
                "farmer_id": user_id,
                "batch_id": batch_id,
                "volume": volume,
                "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        # Обновляем текущий объём пула
        pull["current_volume"] = current_volume + volume

        save_pulls_to_pickle()

        # ✅ КЛЮЧЕВАЯ ПРОВЕРКА - заполненность
        is_full = False
        if pull["current_volume"] >= pull.get("target_volume", 0):
            pull["status"] = "filled"
            is_full = True
            logging.info(f"🎉 Пул #{pull_id} заполнен на 100%!")

        await state.finish()

        if is_full:
            # Пул заполнен на 100%
            await message.answer(
                f"Ваша партия добавлена: {volume:,.0f} т\n\n"
                f"✅ <b>Пул #{pull_id} заполнен на 100%!</b>\n\n"
                parse_mode="HTML",
                reply_markup=farmer_keyboard(),
            )
        else:
            # Обычное добавление
            fill_percent = (pull["current_volume"] / pull.get("target_volume", 1)) * 100
            remaining = pull.get("target_volume", 0) - pull["current_volume"]

            await message.answer(
                f"📦 Ваш объем: {volume:,.0f} т\n"
                f"💵 Цена: ₽{pull.get('price', 0):,.0f}/т\n"
                f"💰 Ваша сумма: ₽{volume * pull.get('price', 0):,.0f}\n\n"
                f"{pull['current_volume']:,.0f} / {pull.get('target_volume', 0):,.0f} т ({fill_percent:.1f}%)\n"
                f"Осталось: {remaining:,.0f} т\n\n"
                parse_mode="HTML",
                reply_markup=farmer_keyboard(),
            )

        logging.info(
            f"Batch {batch_id} → Pull {pull_id}, volume: {volume}, full: {is_full}"
        )

    except ValueError:
        await message.answer("❌ Некорректный объём. Введите положительное число.")


@dp.callback_query_handler(lambda c: c.data == "refresh_prices", state="*")
async def refresh_prices(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

        prices_msg = format_prices_message()
        await callback.message.edit_text(prices_msg, parse_mode="HTML")
    except MessageNotModified:
        await callback.answer("ℹ️ Цены актуальны", show_alert=False)


@dp.callback_query_handler(lambda c: c.data == "refresh_news", state="*")
async def refresh_news(callback: types.CallbackQuery):
    """Обновление новостей"""
    await callback.answer("🔄 Обновляем новости...")
    await update_news_cache()

    news_msg = format_news_message()
    await callback.message.edit_text(
        news_msg, parse_mode="HTML", disable_web_page_preview=True
    )


@dp.callback_query_handler(lambda c: c.data == "auto_match_all", state="*")
async def auto_match_all_batches(callback: types.CallbackQuery):
    """Автопоиск экспортёров для всех активных партий"""
    user_id = callback.from_user.id

        await callback.answer("❌ У вас нет партий", show_alert=True)
        return


    if not active_batches:
        await callback.answer("❌ Нет активных партий", show_alert=True)
        return

    await callback.answer("🔍 Запускаем поиск экспортёров...")

    total_matches = 0
    for batch in active_batches:
        matching_pulls = await find_matching_exporters(batch)
        if matching_pulls:
            total_matches += len(matching_pulls)
            for pull in matching_pulls:
                await notify_match(user_id, batch, [pull])
        await asyncio.sleep(0.5)  # Задержка между запросами

    if total_matches > 0:
        await callback.message.answer(
            f"Найдено совпадений: {total_matches}\n"
        )
    else:
        await callback.message.answer(
            "🤷‍♂️ К сожалению, подходящих экспортёров не найдено.\n\n"
            "Рекомендуем:\n"
            "• Проверить актуальность цен\n"
            "• Уточнить параметры качества\n"
            "• Подождать новых предложений"
        )
        await callback.answer()


@dp.message_handler(lambda m: m.text == "➕ Создать пул", state="*")
async def create_pull_start(message: types.Message, state: FSMContext):
    await state.finish()
    userid = message.from_user.id

        await message.answer("❌ Эта функция доступна только для экспортеров.")
        return

    await CreatePullStatesGroup.culture.set()
    await message.answer(
        "🌾 <b>Создание пула</b>\n\n" "<b>Шаг 1 из 10</b>\n\n" "Выберите культуру:",
        reply_markup=culture_keyboard(),
        parse_mode="HTML",
    )
    logging.info(
        f"User {userid} started pull creation, state set to CreatePullStatesGroup.culture"
    )


# ✅ ИСПРАВЛЕНО: Обработчик выбора культуры
@dp.callback_query_handler(
    lambda c: c.data.startswith("culture:"), state=CreatePullStatesGroup.culture
)
async def create_pull_culture_callback(
    callback: types.CallbackQuery, state: FSMContext
):
    logging.info(
        f"Received callback: {callback.data}, state: {await state.get_state()}"
    )

    try:
        culture = callback.data.split(":", 1)[1]
        logging.info(f"Parsed culture: {culture}")
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка выбора культуры", show_alert=True)
        logging.error(f"Culture selection error: {e}, data: {callback.data}")
        return

    await state.update_data(culture=culture)

    try:
        await callback.message.edit_text(
            f"Выбрана культура: <b>{culture}</b>\n\n"
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"Error editing message: {e}")
        await callback.message.answer(
            f"Выбрана культура: <b>{culture}</b>\n\n"
            parse_mode="HTML",
        )

    await CreatePullStatesGroup.volume.set()
    await callback.answer()
    logging.info("State changed to CreatePullStatesGroup.volume")


# Обработка объема
@dp.message_handler(state=CreatePullStatesGroup.volume)
async def create_pull_volume(message: types.Message, state: FSMContext):
    try:
        volume = float(message.text.strip().replace(",", "."))
        if volume <= 0:
            raise ValueError
        await state.update_data(volume=volume)
        await message.answer(
            f"Объем: <b>{volume:,.0f} тонн</b>\n\n"
            parse_mode="HTML",
        )
        await CreatePullStatesGroup.price.set()
    except ValueError:
        await message.answer("❌ Некорректный объем. Введите положительное число.")


# Обработка цены
@dp.message_handler(state=CreatePullStatesGroup.price)
async def create_pull_price(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.strip().replace(",", "."))
        if price <= 0:
            raise ValueError
        await state.update_data(price=price)
        await message.answer(
            f"Цена: <b>₽{price:,.0f}/тонна</b>\n\n"
            reply_markup=port_keyboard(),
            parse_mode="HTML",
        )
        await CreatePullStatesGroup.port.set()
    except ValueError:
        await message.answer("❌ Некорректная цена. Введите положительное число.")


# Обработка порта
# Обработка порта
@dp.callback_query_handler(
    lambda c: c.data.startswith("selectport_"), state=CreatePullStatesGroup.port
)
async def create_pull_port_callback(callback: types.CallbackQuery, state: FSMContext):
    logging.info(
        f"Received port callback: {callback.data}, state: {await state.get_state()}"
    )

    try:
        port = callback.data.split("_", 1)[1]
        logging.info(f"Parsed port: {port}")
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка выбора порта", show_alert=True)
        logging.error(f"Port selection error: {e}, data: {callback.data}")
        return

    await state.update_data(port=port)

    try:
        await callback.message.edit_text(
            f"Порт: <b>{port}</b>\n\n"
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"Error editing message: {e}")
        await callback.message.answer(
            f"Порт: <b>{port}</b>\n\n"
            parse_mode="HTML",
        )

    await CreatePullStatesGroup.moisture.set()
    await callback.answer()
    logging.info(f"Port set: {port}, state changed to CreatePullStatesGroup.moisture")


# Обработка влажности
@dp.message_handler(state=CreatePullStatesGroup.moisture)
async def create_pull_moisture(message: types.Message, state: FSMContext):
    try:
        moisture = float(message.text.strip().replace(",", "."))
        if not (0 <= moisture <= 100):
            raise ValueError
        await state.update_data(moisture=moisture)
        await message.answer(
            f"Влажность: <b>{moisture}%</b>\n\n"
            parse_mode="HTML",
        )
        await CreatePullStatesGroup.nature.set()
    except ValueError:
        await message.answer("❌ Некорректная влажность. Введите число от 0 до 100.")


# Обработка натуры
@dp.message_handler(state=CreatePullStatesGroup.nature)
async def create_pull_nature(message: types.Message, state: FSMContext):
    try:
        nature = float(message.text.strip().replace(",", "."))
        if nature <= 0:
            raise ValueError
        await state.update_data(nature=nature)
        await message.answer(
            f"Натура: <b>{nature} г/л</b>\n\n"
            parse_mode="HTML",
        )
        await CreatePullStatesGroup.impurity.set()
    except ValueError:
        await message.answer("❌ Некорректная натура. Введите положительное число.")


# Обработка сорной примеси
@dp.message_handler(state=CreatePullStatesGroup.impurity)
async def create_pull_impurity(message: types.Message, state: FSMContext):
    try:
        impurity = float(message.text.strip().replace(",", "."))
        if not (0 <= impurity <= 100):
            raise ValueError
        await state.update_data(impurity=impurity)
        await message.answer(
            f"Сорная примесь: <b>{impurity}%</b>\n\n"
            parse_mode="HTML",
        )
        await CreatePullStatesGroup.weed.set()
    except ValueError:
        await message.answer("❌ Некорректная примесь. Введите число от 0 до 100.")


# Обработка зерновой примеси
@dp.message_handler(state=CreatePullStatesGroup.weed)
async def create_pull_weed(message: types.Message, state: FSMContext):
    try:
        weed = float(message.text.strip().replace(",", "."))
        if not (0 <= weed <= 100):
            raise ValueError
        await state.update_data(weed=weed)
        await message.answer(
            f"Зерновая примесь: <b>{weed}%</b>\n\n"
            parse_mode="HTML",
        )
        await CreatePullStatesGroup.documents.set()
    except ValueError:
        await message.answer("❌ Некорректная примесь. Введите число от 0 до 100.")


@dp.message_handler(state=CreatePullStatesGroup.documents)
async def create_pull_documents(message: types.Message, state: FSMContext):
    documents = message.text.strip()
    await state.update_data(documents=documents)

    # ✅ ИСПРАВЛЕНО: Правильная клавиатура
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("CPT", callback_data="doctype_CPT"),
        InlineKeyboardButton("FOB", callback_data="doctype_FOB"),
    )
    keyboard.add(
        InlineKeyboardButton("CIF", callback_data="doctype_CIF"),
        InlineKeyboardButton("EXW", callback_data="doctype_EXW"),
    )

    await message.answer(
        f"Документы: <b>{documents}</b>\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await CreatePullStatesGroup.doctype.set()
    logging.info(
        f"Documents set: {documents}, state changed to CreatePullStatesGroup.doctype"
    )


@dp.callback_query_handler(
    lambda c: c.data.startswith("doctype_"), state=CreatePullStatesGroup.doctype
)
async def create_pull_finish(callback: types.CallbackQuery, state: FSMContext):

    logging.info(
        f"Received doctype callback: {callback.data}, state: {await state.get_state()}"
    )

    try:
        doctype = callback.data.split("_", 1)[1]
        logging.info(f"Parsed doctype: {doctype}")
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка выбора типа поставки", show_alert=True)
        logging.error(f"Doctype selection error: {e}, data: {callback.data}")
        return

    await state.update_data(doctype=doctype)
    data = await state.get_data()
    userid = callback.from_user.id

    pull_counter += 1
    pull = {
        "id": pull_counter,
        "exporter_id": userid,
        "culture": data["culture"],
        "target_volume": data["volume"],
        "current_volume": 0,
        "port": data["port"],
        "moisture": data.get("moisture", 0),
        "nature": data.get("nature", 0),
        "impurity": data.get("impurity", 0),
        "weed": data.get("weed", 0),
        "documents": data.get("documents", ""),
        "doc_type": doctype,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "participants": [],
        "logist_ids": [],
        "expeditor_ids": [],
    }

    pulls["pulls"][pull_counter] = pull

    try:
        if gs and gs.spreadsheet:
            gs.sync_pull_to_sheets(pull)
    except Exception as e:
        logging.error(f"Error syncing to Google Sheets: {e}")

    logging.info(f"✅ Pull {pull_counter} created by user {userid}")

    await state.finish()

    summary = (
        f"✅ <b>Пул #{pull_counter} успешно создан!</b>\n\n"
        f"🌾 Культура: <b>{pull['culture']}</b>\n"
        f"📦 Объем: <b>{pull['target_volume']:,.0f} тонн</b>\n"
        f"💵 Цена FOB: <b>₽{pull['price']:,.0f}/тонна</b>\n"
        f"🚢 Порт: <b>{pull['port']}</b>\n"
        f"💧 Влажность: <b>≤{pull['moisture']}%</b>\n"
        f"⚖️ Натура: <b>≥{pull['nature']} г/л</b>\n"
        f"🌿 Сорная примесь: <b>≤{pull['impurity']}%</b>\n"
        f"🌾 Зерновая примесь: <b>≤{pull['weed']}%</b>\n"
        f"📋 Документы: <b>{pull['documents']}</b>\n"
        f"📦 Тип поставки: <b>{doctype}</b>\n\n"
    )

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📋 Мои пулы", callback_data="back_to_pulls"))

    await callback.message.edit_text(summary, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "back_to_pools_list", state="*")
async def back_to_pools_list(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку пулов"""
    await state.finish()

    user_id = callback.from_user.id

    # ЛОГИКА: Поддерживаем ОБЕ версии статусов
    open_pulls = [
        pull
    ]

    if not open_pulls:
        await callback.message.edit_text(
            "📦 Сейчас нет открытых пулов.\n\n"
            "Ожидайте новых предложений от экспортёров."
        )
        await callback.answer()
        return


        if relevant_pulls:
            open_pulls = relevant_pulls

    keyboard = InlineKeyboardMarkup(row_width=1)
    for pull in open_pulls[:10]:
        progress = (
            else 0
        )

        keyboard.add(
        )

    await callback.message.edit_text(
        f"📦 <b>Открытые пулы</b> ({len(open_pulls)} шт.)\n\n"
        "Выберите пул для присоединения:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("view_pull_matches:"), state="*")
async def view_pull_matches(callback: types.CallbackQuery):
    try:
        pull_id = parse_callback_id(callback.data)
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

    user_id = callback.from_user.id

        await callback.answer("❌ Пул не найден", show_alert=True)
        return

        await callback.answer("❌ Нет доступа к этому пулу", show_alert=True)
        return
    pull_matches = []
    for match in matches.values():
            pull_matches.append(match)

    if not pull_matches:
        await callback.answer("🤷‍♂️ Активных совпадений не найдено", show_alert=True)
        return

    text = f"🎯 <b>Совпадения для пула #{pull_id}</b>\n\n"

    for i, match in enumerate(pull_matches[:5], 1):

        if batch_info:
            text += f"{i}. <b>Партия #{batch_id}</b>\n"

    if len(pull_matches) > 5:
        text += f"<i>... и ещё {len(pull_matches) - 5} совпадений</i>\n\n"

    text += "💡 <b>Рекомендация:</b> Свяжитесь с фермерами для обсуждения деталей."

    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "back_to_my_pulls", state="*")
async def back_to_my_pulls(callback: types.CallbackQuery, state: FSMContext):
    """✅ Возврат к списку пулов экспортёра"""
    await state.finish()

    user_id = callback.from_user.id
    logging.info(f"🔙 Возврат к пулам: user_id={user_id}")

        await callback.answer(
            "❌ Эта функция доступна только экспортёрам", show_alert=True
        )
        return

    all_pulls = pulls.get("pulls", {})
    my_pulls = {
        pid: pull
        for pid, pull in all_pulls.items()
    }

    if not my_pulls:
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
        )
        await callback.message.edit_text(
            "📋 <b>Мои пулы</b>\n\n"
            "У вас пока нет пулов.\n\n"
            "💡 <i>Создайте первый пул через меню</i>",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        await callback.answer()
        return

    culture_emoji = {
        "пшеница": "🌾",
        "ячмень": "🌾",
        "кукуруза": "🌽",
        "подсолнечник": "🌻",
        "соя": "🫘",
        "рапс": "🌿",
    }

    keyboard = InlineKeyboardMarkup(row_width=1)
    for pull_id, pull in my_pulls.items():
        culture = pull.get("culture", "").lower()
        culture_icon = culture_emoji.get(culture, "🌾")
        status_icon = status_map.get(status, "⚪").split()[0]
        current = pull.get("current_volume", 0)
        target = pull.get("target_volume", 1)
        progress = (current / target * 100) if target > 0 else 0
        button_text = (
            f"{status_icon} {culture_icon} {pull.get('culture', '?')} ({progress:.0f}%)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_pull:{pull_id}")
        )

    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))
    try:
        await callback.message.edit_text(
            f"📋 <b>Мои пулы</b> ({len(my_pulls)} шт.)\n\n"
            "<i>Выберите пул для управления:</i>",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except MessageNotModified:
        pass
    await callback.answer()
    logging.info(f"✅ Показано {len(my_pulls)} пулов")


@dp.message_handler(lambda m: m.text == "📋 Мои пулы", state="*")
async def view_my_pulls(message: types.Message, state: FSMContext):
    """✅ Показывает пулы экспортёра - ЕДИНСТВЕННАЯ ВЕРСИЯ"""
    userid = message.from_user.id
    logging.critical(f"🔔 ВЫЗВАНА ФУНКЦИЯ view_my_pulls: user_id={userid}")

        await message.answer("❌ Эта функция доступна только экспортёрам")
        return

    all_pulls = pulls.get("pulls", {})
    my_pulls = {
        pid: pull
        for pid, pull in all_pulls.items()
    }
    if not my_pulls:
        await message.answer(
            "📋 <b>Мои пулы</b>\n\n"
            "У вас пока нет пулов.\n\n"
            "💡 <i>Создайте первый пул через меню</i>",
            parse_mode="HTML",
        )
        return

    logging.info(f"✅ Экспортер {userid}: показано {len(my_pulls)} пулов")
    culture_emoji = {
        "пшеница": "🌾",
        "ячмень": "🌾",
        "кукуруза": "🌽",
        "подсолнечник": "🌻",
        "соя": "🫘",
        "рапс": "🌿",
    }

    keyboard = InlineKeyboardMarkup(row_width=1)
    for pull_id, pull in my_pulls.items():
        culture = pull.get("culture", "").lower()
        culture_icon = culture_emoji.get(culture, "🌾")
        status_icon = status_map.get(status, "⚪").split()[0]
        current = pull.get("current_volume", 0)
        target = pull.get("target_volume", 1)
        progress = (current / target * 100) if target > 0 else 0
        button_text = (
            f"{status_icon} {culture_icon} {pull.get('culture', '?')} ({progress:.0f}%)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_pull:{pull_id}")
        )

    await message.answer(
        f"📋 <b>Мои пулы</b> ({len(my_pulls)} шт.)\n\n"
        "<i>Выберите пул для управления:</i>",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.callback_query_handler(lambda c: c.data.startswith("view_pull:"), state="*")
async def view_pull_details(callback: types.CallbackQuery):
    """✅ Просмотр деталей пула с учетом роли пользователя и правильной клавиатурой."""
    try:
        pull_id = parse_callback_id(callback.data)
        if pull_id is None:
            await callback.answer("❌ Ошибка обработки данных", show_alert=True)
            return

        user_id = callback.from_user.id
        role = user.get("role") if user else None

        logging.info(
            f"📄 Просмотр пула {pull_id} пользователем {user_id} (роль: {role})"
        )

        all_pulls = pulls.get("pulls", {})
        if not all_pulls:
            await callback.answer("❌ Пулы не найдены в системе", show_alert=True)
            return

        pull = (
            all_pulls.get(pull_id)
            or all_pulls.get(str(pull_id))
            or all_pulls.get(int(pull_id) if str(pull_id).isdigit() else None)
        )
        if not pull:
            logging.warning(
                f"⚠️ Пул {pull_id} не найден. Доступные: {list(all_pulls.keys())}"
            )
            await callback.answer("❌ Пул не найден", show_alert=True)
            return

        # Совпадения (например, для экспортера)
        active_matches = []
        if matches:
            active_matches = [
                m
                for m in matches.values()
            ]

        exporter_id = pull.get("exporter_id")
        exporter_name = pull.get("exporter_name", "Неизвестен")
        exporter_phone = "Не указан"
        exporter_region = "Не указан"
            exporter_phone = exporter_data.get("phone", "Не указан")
            exporter_region = exporter_data.get("region", "Не указан")

        target_volume = float(pull.get("target_volume", 1))
        current_volume = float(pull.get("current_volume", 0))
        progress = (current_volume / target_volume * 100) if target_volume > 0 else 0

        text = f"""📦 <b>Пул #{pull.get('id', pull_id)}</b>

🌾 <b>Культура:</b> {pull.get('culture', '?')}
📦 <b>Объём:</b> {current_volume:.0f}/{target_volume:.0f} т ({progress:.0f}%)
💰 <b>Цена FOB:</b> ₽{pull.get('price', 0):,.0f}/т
🚢 <b>Порт:</b> {pull.get('port', 'Не указан')}

<b>━━━ Требования ━━━</b>
💧 <b>Влажность:</b> до {pull.get('moisture', '?')}%
🏋️ <b>Натура:</b> от {pull.get('nature', '?')} г/л
🌾 <b>Сорность:</b> до {pull.get('impurity', '?')}%

<b>━━━ Экспортер ━━━</b>
👤 <b>Имя:</b> {exporter_name}
📞 <b>Телефон:</b> {exporter_phone}
🏢 <b>Регион:</b> {exporter_region}
📅 <b>Создан:</b> {pull.get('created_at', '?')}
"""

        if active_matches:
            text += f"\n🎯 <b>Совпадения:</b> {len(active_matches)}"

        keyboard = InlineKeyboardMarkup(row_width=2)

        # Клавиатура для экспортера (только если экспортер и владелец)
            keyboard.add(
                InlineKeyboardButton(
                    "✏️ Редактировать", callback_data=f"editpull_{pull_id}"
                ),
                InlineKeyboardButton(
                    "🗑 Удалить", callback_data=f"deletepull_{pull_id}"
                ),
                InlineKeyboardButton(
                    "🔄 Изменить статус", callback_data=f"change_pull_status:{pull_id}"
                ),
            )
            if active_matches:
                keyboard.add(
                    InlineKeyboardButton(
                        f"🎯 Совпадения ({len(active_matches)})",
                        callback_data=f"view_pull_matches:{pull_id}",
                    )
                )
            keyboard.add(
                InlineKeyboardButton(
                    "👥 Участники", callback_data=f"pullparticipants:{pull_id}"
                ),
                InlineKeyboardButton(
                    "🚚 Логистика", callback_data=f"pull_logistics:{pull_id}"
                ),
            )
        elif role == "farmer":
                keyboard.add(
                    InlineKeyboardButton(
                        "✅ Присоединиться", callback_data=f"join_pull:{pull_id}"
                    )
                )


        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        logging.info(f"✅ Пул {pull_id} отображён успешно")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}", exc_info=True)
        await callback.message.reply("❌ <b>Ошибка системы</b>", parse_mode="HTML")


@dp.message_handler(lambda m: m.text == "🔍 Найти партии", state="*")
async def search_batches_for_exporter(message: types.Message, state: FSMContext):
    """Расширенный поиск партий для экспортёра"""
    await state.finish()

    user_id = message.from_user.id
        await message.answer("❌ Эта функция доступна только экспортёрам")
        return

    await message.answer(
        "🔍 <b>Расширенный поиск партий</b>\n\n" "Выберите критерии поиска:",
        reply_markup=search_criteria_keyboard(),
        parse_mode="HTML",
    )


@dp.callback_query_handler(lambda c: c.data.startswith("search_by:"), state="*")
async def handle_search_criteria(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора критерия поиска партий"""
    await state.finish()

    criteria = callback.data.split(":", 1)[1]

    # ════════════════════════════════════════════════════════════════════════════
    # ПОИСК ПО КУЛЬТУРЕ
    # ════════════════════════════════════════════════════════════════════════════

    if criteria == "culture":
        try:
            await callback.message.edit_text(
                "🌾 <b>Поиск по культуре</b>\n\n" "Выберите культуру:",
                reply_markup=culture_keyboard(),
                parse_mode="HTML",
            )
        except Exception as e:
            logging.error(f"Ошибка edit_text: {e}")
            await callback.message.answer(
                "🌾 <b>Поиск по культуре</b>\n\n" "Выберите культуру:",
                reply_markup=culture_keyboard(),
                parse_mode="HTML",
            )
        await SearchByCulture.waiting_culture.set()

    # ════════════════════════════════════════════════════════════════════════════
    # ПОИСК ПО РЕГИОНУ
    # ════════════════════════════════════════════════════════════════════════════

    elif criteria == "region":
        try:
            await callback.message.edit_text(
                "📍 <b>Поиск по региону</b>\n\n" "Выберите регион:",
                parse_mode="HTML",
            )
        except Exception as e:
            logging.error(f"Ошибка edit_text: {e}")
            await callback.message.answer(
                "📍 <b>Поиск по региону</b>\n\n" "Выберите регион:",
                parse_mode="HTML",
            )
        await SearchBatchesStatesGroup.enter_region.set()

    # ════════════════════════════════════════════════════════════════════════════
    # ПОИСК ДОСТУПНЫХ ПАРТИЙ
    # ════════════════════════════════════════════════════════════════════════════

    elif criteria == "available":
        await callback.answer("🔍 Ищем доступные партии...")

        available_batches = []

            if batch.get("status") in [
                "active",
                "Активна",
                "available",
                "доступна",
            ]:

        logging.info(f"✅ Найдено доступных партий: {len(available_batches)}")

        # ────────────────────────────────────────────────────────────────────────
        # ФОРМИРОВАНИЕ ОТВЕТА
        # ────────────────────────────────────────────────────────────────────────

        if available_batches:
            text = f"🌾 <b>Найдено доступных партий: {len(available_batches)}</b>\n\n"

            # Показываем первые 10 партий
            for i, batch in enumerate(available_batches[:10], 1):
                culture = batch.get("culture", "Не указана")
                volume = batch.get("volume", 0)
                price = batch.get("price", 0)
                region = batch.get("region", "Не указан")

                text += f"{i}. <b>{culture}</b> - {volume} т\n"
                text += f"   💰 {price:,.0f} ₽/т | 📍 {region}\n\n"

            if len(available_batches) > 10:
                text += f"... и ещё {len(available_batches) - 10} партий\n"

            # Создаём клавиатуру с первыми 5 партиями
            keyboard = InlineKeyboardMarkup(row_width=1)
            for batch in available_batches[:5]:
                batch_id = batch.get("id", 0)
                culture = batch.get("culture", "Не указана")
                volume = batch.get("volume", 0)

                keyboard.add(
                    InlineKeyboardButton(
                        f"{culture} - {volume} т",
                        callback_data=f"view_batch:{batch_id}",
                    )
                )

            keyboard.add(
                InlineKeyboardButton("◀️ Назад", callback_data="back_to_search")
            )

            try:
                await callback.message.edit_text(
                    text, reply_markup=keyboard, parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"Ошибка edit_text: {e}")
                await callback.message.answer(
                    text, reply_markup=keyboard, parse_mode="HTML"
                )

        else:
            # Партии не найдены
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(
                InlineKeyboardButton("◀️ Назад", callback_data="back_to_search")
            )

            try:
                await callback.message.edit_text(
                    "❌ <b>Доступных партий не найдено</b>\n\n"
                    "Попробуйте изменить критерии поиска.",
                    reply_markup=keyboard,
                    parse_mode="HTML",
                )
            except Exception as e:
                logging.error(f"Ошибка edit_text: {e}")
                await callback.message.answer(
                    "❌ <b>Доступных партий не найдено</b>\n\n"
                    "Попробуйте изменить критерии поиска.",
                    reply_markup=keyboard,
                    parse_mode="HTML",
                )

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("view_batch:"), state="*")
async def view_batch_details(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр детальной информации о партии"""
    logging.info(f"📦 Просмотр партии {callback.data}")

    try:
        batch_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError) as e:
        logging.error(f"❌ Ошибка парсинга batch_id: {e}")
        await callback.answer("❌ Ошибка: некорректный ID партии", show_alert=True)
        return

    user_id = callback.from_user.id
    user_role = user.get("role", "unknown").lower()

    logging.info(f"👤 user_id={user_id}, роль={user_role}")
    logging.info(f"🔍 Ищем партию batch_id={batch_id}")
    logging.info(f"📋 Структура batches: ключей={len(batches)}")

    # ✅ ИСПРАВЛЕНО: Поиск партии в структуре {farmer_id: [batch_list]}
    found_batch = None
    farmer_id = None

    for fid, batch_list in batches.items():
        if isinstance(batch_list, list):
            for batch in batch_list:
                    found_batch = batch
                    farmer_id = fid
                    logging.info(f"✅ Партия {batch_id} найдена у фермера {fid}")
                    break
        elif isinstance(batch_list, dict):
            # На случай если одна партия (словарь)
                found_batch = batch_list
                farmer_id = fid
                logging.info(f"✅ Партия {batch_id} найдена у фермера {fid}")
                break

        if found_batch:
            break

    if not found_batch:
        logging.error(f"❌ Партия {batch_id} НЕ НАЙДЕНА!")
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return

    # Получаем владельца
    logging.info(f"👥 farmer_id={farmer_id}, user_id={user_id}, is_owner={is_owner}")

    # ✅ Формируем сообщение
    text = f"📦 <b>Информация о партии #{batch_id}</b>\n\n"
    text += f"🌾 <b>Культура:</b> {found_batch.get('culture', 'Не указана')}\n"
    text += f"📦 <b>Объём:</b> {found_batch.get('volume', 0)} т\n"
    text += f"💰 <b>Цена:</b> {found_batch.get('price', 0):,.0f} ₽/т\n"
    text += f"📍 <b>Локация:</b> {found_batch.get('location', 'Не указана')}\n"
    text += f"🚢 <b>Порт:</b> {found_batch.get('port', 'Не указан')}\n"
    text += f"🏛 <b>Статус:</b> {found_batch.get('status', 'Активна')}\n"

    # Показатели качества
    if "quality" in found_batch:
        quality = found_batch["quality"]
        text += f"💧 <b>Влажность:</b> {quality.get('moisture', '-')}%\n"
        text += f"🌾 <b>Натура:</b> {quality.get('nature', '-')} г/л\n"
        text += f"🌿 <b>Примесь:</b> {quality.get('impurity', '-')}%\n"
        text += f"🌱 <b>Белок:</b> {quality.get('protein', '-')}%\n"
        if "weed" in quality:
            text += f"🌿 <b>Сорная примесь:</b> {quality.get('weed', '-')}%\n"

    # Дополнительная информация
    if "storage_type" in found_batch:
        text += f"🏭 <b>Хранение:</b> {found_batch['storage_type']}\n"
    if "documents" in found_batch:
        text += f"📄 <b>Документы:</b> {found_batch['documents']}\n"
    if "created_at" in found_batch:
        text += f"📅 <b>Создано:</b> {found_batch['created_at']}\n"

    # ✅ Создаём кнопки В ЗАВИСИМОСТИ ОТ РОЛИ
    keyboard = InlineKeyboardMarkup(row_width=1)

    if is_owner and user_role == "farmer":
        # ✅ CASE 1: Фермер смотрит свою партию
        logging.info(f"✅ CASE 1: Фермер {user_id} смотрит свою партию {batch_id}")
        keyboard.add(
            InlineKeyboardButton(
                "✏️ Редактировать партию", callback_data=f"edit_batch:{batch_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton(
                "🗑 Удалить партию", callback_data=f"delete_batch:{batch_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton(
                "🎯 Посмотреть совпадения", callback_data=f"view_matches:{batch_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton(
                "◀️ Назад к моим партиям", callback_data="back_to_my_batches"
            )
        )

    elif user_role == "exporter":
        # ✅ CASE 2: Экспортёр смотрит партию
        logging.info(f"✅ CASE 2: Экспортёр {user_id} смотрит партию {batch_id}")

        # Получаем контакты фермера
        if farmer_id:
            farmer_name = farmer_info.get("name", "Неизвестно")
            farmer_phone = farmer_info.get("phone", "Не указан")
            farmer_email = farmer_info.get("email", "Не указан")
            farmer_company = farmer_info.get("company_name", farmer_name)

            text += f"🏢 <b>Компания:</b> {farmer_company}\n"
            text += f"👤 <b>Имя:</b> {farmer_name}\n"
            text += f"📱 <b>Телефон:</b> <code>{farmer_phone}</code>\n"
            text += f"📧 <b>Email:</b> {farmer_email}\n"

            keyboard.add(
                InlineKeyboardButton(
                    "📞 Связаться с фермером",
                    callback_data=f"contact_farmer:{batch_id}:{farmer_id}",
                )
            )

        keyboard.add(
            InlineKeyboardButton("◀️ Назад к поиску", callback_data="back_to_search")
        )

        # ✅ CASE 3: Логист смотрит партию
        logging.info(f"✅ CASE 3: Логист {user_id} смотрит партию {batch_id}")
        keyboard.add(
            InlineKeyboardButton(
            )
        )
        keyboard.add(
            InlineKeyboardButton("◀️ Назад к поиску", callback_data="back_to_search")
        )

    else:
        # ✅ CASE 5: Неизвестная роль
        logging.error(
            f"❌ CASE 5: Неизвестная роль '{user_role}' для пользователя {user_id}"
        )
        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_search"))
        text += f"\n\n❌ <b>Ошибка: неизвестная роль '{user_role}'</b>"

    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logging.error(f"❌ Ошибка edit_text: {e}")
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "back_to_search", state="*")
async def back_to_search_menu(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в меню поиска"""
    await state.finish()

    await callback.message.edit_text(
        "🔍 <b>Расширенный поиск партий</b>\n\n" "Выберите критерии поиска:",
        reply_markup=search_criteria_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


# Обработчик выбора культуры


# Обработчик выбора региона
@dp.callback_query_handler(
)
async def search_by_region_selected(callback: types.CallbackQuery, state: FSMContext):
    if ":" in callback.data:
        region = callback.data.split(":", 1)[1]
    else:
        await callback.answer("❌ Ошибка парсинга", show_alert=True)
        return

    # Поиск партий по региону
    found_batches = []
        if batch.get("region") == region and batch.get("status") in [
            "active",
            "Активна",
            "available",
            "доступна",
        ]:

    await state.finish()

    if found_batches:
        text = f"📍 <b>Найдено партий в '{region}': {len(found_batches)}</b>\n\n"

        for i, batch in enumerate(found_batches[:10], 1):
            text += f"{i}. {batch['culture']} - {batch['volume']} т\n"
            text += f"   💰 {batch['price']:,.0f} ₽/т\n\n"

        if len(found_batches) > 10:
            text += f"... и ещё {len(found_batches) - 10} партий"

        keyboard = InlineKeyboardMarkup(row_width=1)
        for batch in found_batches[:5]:
            keyboard.add(
                InlineKeyboardButton(
                    f"{batch['culture']} - {batch['volume']} т",
                    callback_data=f"view_batch:{batch['id']}",
                )
            )
        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_search"))

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    else:
        await callback.message.edit_text(
            f"❌ Партий в '{region}' не найдено", parse_mode="HTML"
        )

    await callback.answer()


# ═══════════════════════════════════════════════════════════════════════════
# РАСШИРЕННЫЙ ФУНКЦИОНАЛ ЭКСПОРТЁРА
# Добавление партий в пулл, выбор логистов и экспедиторов
# ═══════════════════════════════════════════════════════════════════════════


@dp.callback_query_handler(lambda c: c.data.startswith("add_batch_to_pull:"), state="*")
async def add_batch_to_pull_select(callback: types.CallbackQuery):
    """Выбор пулла для добавления партии"""
    try:
        batch_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    user_id = callback.from_user.id

        await callback.answer("❌ Партия не найдена", show_alert=True)
        return

    # Получаем активные пулы экспортёра с той же культурой
    user_pulls = []
        if (
            and p.get("culture", "").lower() == batch.get("culture", "").lower()
        ):
            user_pulls.append((pid, p))

    if not user_pulls:
        await callback.answer(
            f"❌ У вас нет активных пулов для культуры: {batch.get('culture', 'Неизвестно')}\n"
            "Создайте пулл сначала!",
            show_alert=True,
        )
        return

    # Показываем список пулов
    keyboard = InlineKeyboardMarkup(row_width=1)

    for pull_id, pull in user_pulls:
            current_vol = 0


        keyboard.add(
            InlineKeyboardButton(
                f"Пулл #{pull_id}: {current_vol:.1f}/{target_vol:.1f} т",
                callback_data=f"confirm_add_batch:{batch_id}:{pull_id}",
            )
        )

    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))

    await callback.message.edit_text(
        f"🌾 Партия: {batch.get('culture', 'Неизвестно')} • {batch.get('volume', 0):.1f} т\n"
        f"📍 Регион: {batch.get('region', 'Не указан')}\n"
        f"💰 Цена: {batch.get('price', 0):,} ₽/т\n\n"
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    await callback.answer()


# ════════════════════════════════════════════════════════════════════════════════════
# ИСПРАВЛЕННАЯ ВЕРСИЯ
# ════════════════════════════════════════════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data.startswith("confirm_add_batch:"), state="*")
async def confirm_add_batch_to_pull(callback: types.CallbackQuery):
    """✅ Подтверждение добавления партии в пулл - ПОЛНОСТЬЮ ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    try:
        _, batch_id, pull_id = callback.data.split(":")
        batch_id = int(batch_id)
        pull_id = int(pull_id)

        # 🔍 ЛОГИРУЕМ ТИП И ЗНАЧЕНИЕ
        logging.info(f"🔍 batch_id type: {type(batch_id).__name__}, value: {batch_id}")
        logging.info(f"🔍 pull_id type: {type(pull_id).__name__}, value: {pull_id}")

    except (ValueError, IndexError) as e:
        logging.error(f"❌ Ошибка парсинга: {e}")
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    try:
        # 1️⃣ ИЩЕМ ПАРТИЮ И ФЕРМЕРА

        if not batch or not farmer_id:
            logging.error(
                f"❌ Партия не найдена: batch_id={batch_id}, farmer_id={farmer_id}"
            )
            await callback.answer("❌ Партия не найдена", show_alert=True)
            return

        # 2️⃣ ИЩЕМ ПУЛЛ
            logging.error(f"❌ Пулл не найден: pull_id={pull_id}")
            await callback.answer("❌ Пулл не найден", show_alert=True)
            return

        # 3️⃣ ПРОВЕРЯЕМ ПРАВА
            await callback.answer("❌ Это не ваш пулл", show_alert=True)
            return

        # 4️⃣ ИНИЦИАЛИЗИРУЕМ ПОЛЯ ПУЛЛА
        if "batches" not in pull:
            pull["batches"] = []
        if "batch_ids" not in pull:
            pull["batch_ids"] = []
        if "farmer_ids" not in pull:
            pull["farmer_ids"] = []

        # ИНИЦИАЛИЗИРУЕМ pullparticipants

        # 5️⃣ ПРОВЕРЯЕМ ЧТО ПАРТИЯ НЕ ДОБАВЛЕНА
            await callback.answer("⚠️ Партия уже в пуле", show_alert=True)
            return

        # 6️⃣ ДОБАВЛЯЕМ ПАРТИЮ В ПУЛ
        pull["batch_ids"].append(batch_id)
        batch["pull_id"] = pull_id

        # 7️⃣ ДОБАВЛЯЕМ ФЕРМЕРА
            pull["farmer_ids"].append(farmer_id)
            logging.info(f"✅ Фермер {farmer_id} добавлен в пулл {pull_id}")

        # 8️⃣ ДОБАВЛЯЕМ В pullparticipants
        participant_record = {
            "batch_id": batch_id,
            "farmer_id": farmer_id,
            "farmer_name": farmer_info.get("name", "Unknown"),
            "volume": batch.get("volume", 0),
            "quality_class": batch.get("quality_class", ""),
            "culture": batch.get("culture", ""),
            "price_per_ton": batch.get("price", 0),
        }

            logging.info(
                f"✅ Участник добавлен: farmer_id={farmer_id}, batch_id={batch_id}"
            )

        # 9️⃣ СЧИТАЕМ ОБЪЁМ
        current_volume = 0
        for b_id in pull["batch_ids"]:

        target_volume = pull.get("target_volume", 0)
        pull["current_volume"] = current_volume

        # 🔟 ПРОВЕРЯЕМ ЗАПОЛНЕНИЕ
        status_msg = ""
            status_msg = "🎉 Пулл собран!"
            logging.info(f"🎉 Пулл #{pull_id} СОБРАН!")

            try:
                await bot.send_message(
                    callback.from_user.id,
                    f"🎉 <b>Пулл #{pull_id} собран!</b>\n\n"
                    f"🌾 {pull.get('culture', 'Культура')}\n"
                    f"📊 Объём: {current_volume:.1f}/{target_volume:.1f} т\n"
                    f"👥 Фермеров: <b>{len(pull['farmer_ids'])}</b>\n"
                    f"📦 Партий: <b>{len(pull['batch_ids'])}</b>\n"
                    f"📍 Порт: {pull.get('port', 'Не указан')}",
                    parse_mode="HTML",
                )
            except Exception as e:
                logging.error(f"❌ Ошибка уведомления экспортёра: {e}")
        else:
            status_msg = "✅ Партия добавлена"

        # 1️⃣1️⃣ УВЕДОМЛЯЕМ ФЕРМЕРА
        if farmer_id in users:
            try:
                await bot.send_message(
                    farmer_id,
                    f"🌾 {batch.get('culture', 'Культура')} • {batch.get('volume', 0):.1f} т\n"
                    f"📦 Пулл #{pull_id}\n"
                    f"👥 Участников в пуле: <b>{len(pull['farmer_ids'])}</b>\n"
                    f"📊 Собрано: {current_volume:.1f}/{target_volume:.1f} т\n"
                    f"🚢 Порт: {pull.get('port', 'Не указан')}\n"
                    f"💰 Цена: {batch.get('price', 0):,} ₽/т",
                    parse_mode="HTML",
                )
            except Exception as e:
                logging.error(f"❌ Ошибка уведомления фермера: {e}")

        # 🔴 ГЛАВНАЯ ИСПРАВКА - СОХРАНЯЕМ ПУЛЛ В ГЛОБАЛЬНЫЙ СЛОВАРЬ!
        pulls["pulls"][pull_id] = pull

        # 1️⃣2️⃣ СОХРАНЯЕМ ДАННЫЕ
        save_data()

        # ✅ ЛОГИРУЕМ РЕЗУЛЬТАТ
        logging.info(
            f"   pulls['pulls'][{pull_id}]['batch_ids'] = {pulls['pulls'][pull_id].get('batch_ids', [])}"
        )
        logging.info(
        )

        # 1️⃣3️⃣ ОТВЕТ ПОЛЬЗОВАТЕЛЮ
        await callback.message.edit_text(
            f"✅ <b>Партия добавлена в пулл #{pull_id}!</b>\n\n"
            f"📊 Текущий объём: {current_volume:.1f}/{target_volume:.1f} т\n"
            f"👥 Фермеров в пуле: <b>{len(pull['farmer_ids'])}</b>\n"
            f"📦 Партий: <b>{len(pull['batch_ids'])}</b>\n"
            f"{status_msg}",
            parse_mode="HTML",
        )
        await callback.answer("✅ Партия добавлена!")

    except Exception as e:
        logging.error(f"❌ ОШИБКА в confirm_add_batch_to_pull: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)[:100]}", show_alert=True)


# ═══════════════════════════════════════════════════════════════════════════
# 🚚 ФЕРМЕР СОЗДАЕТ ЗАЯВКУ НА ДОСТАВКУ (копия логики экспортера)
# ═══════════════════════════════════════════════════════════════════════════
    await state.finish()
    user_id = message.from_user.id

        return

    logging.info(
    )

    )


        await message.answer(
        )
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
            )
        )

    await message.answer(
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.callback_query_handler(
)
    """Выбор пула для заявки"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    await state.update_data(
        pull_id=pull_id,
        volume=pull.get("current_volume", 0),
        port=pull.get("port", ""),
    )

    await callback.message.edit_text(
        f"Порт: {pull.get('port', '')}\n\n"
        parse_mode="HTML",
    )

    await CreateLogisticRequestStatesGroup.route_from.set()
    await callback.answer()

@dp.callback_query_handler(
)
async def farmer_select_port(callback: types.CallbackQuery, state: FSMContext):
    try:
        port = callback.data.split(":")[-1]
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return


    keyboard = InlineKeyboardMarkup(row_width=1)
    transports = [
        ("🚂 ЖД", "railway"),
        ("🚚 Зерновоз", "grain"),
        ("🚛 Фура", "truck"),
    ]
    for transport_name, transport_code in transports:
        keyboard.add(
            InlineKeyboardButton(
                transport_name,
                callback_data=f"farmer_select_transport:{transport_code}",
            )
        )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="farmer_main_menu"))

    await callback.message.edit_text(
    )

    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("farmer_select_transport:"),
)
async def farmer_select_transport(callback: types.CallbackQuery, state: FSMContext):
    try:
        transport = callback.data.split(":")[-1]
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return


    await callback.message.edit_text(
        "Введите цену в ₽/тонну\n\n"
        "Пример: <code>1500</code>",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("❌ Отмена", callback_data="farmer_main_menu")
        ),
        parse_mode="HTML",
    )

    await callback.answer()


async def farmer_enter_price(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.strip().replace(",", "."))
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            "❌ Неверный формат!\n\nВведите число: <code>1500</code>", parse_mode="HTML"
        )
        return

    data = await state.get_data()
    batch = data["batch"]
    port = data["port"]
    transport = data["transport"]

    ports_dict = {
    }

    transports_dict = {
        "railway": "🚂 ЖД",
        "grain": "🚚 Зерновоз",
        "truck": "🚛 Фура",
    }

    port_name = ports_dict.get(port, port)
    transport_name = transports_dict.get(transport, transport)

    text = (
        f"🌾 <b>Культура:</b> {batch['culture']}\n"
        f"📦 <b>Объём:</b> {batch['volume']} т\n"
        f"💰 <b>Цена партии:</b> {batch['price']:,} ₽/т\n"
        f"💵 <b>Итого:</b> {batch['volume'] * batch['price']:,} ₽\n\n"
        f"🚚 <b>Транспорт:</b> {transport_name}\n"
        f"💳 <b>Ожидаемая цена доставки:</b> {price:,.0f} ₽/т\n"
    )

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            "✅ СОЗДАТЬ ЗАЯВКУ", callback_data="farmer_confirm_logistics_request"
        )
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="farmer_main_menu"))

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await state.update_data(desired_price=price)


@dp.callback_query_handler(text="farmer_confirm_logistics_request", state="*")
async def farmer_confirm_logistics_request(
    callback: types.CallbackQuery, state: FSMContext
):
    user_id = callback.from_user.id
    data = await state.get_data()

    if not data.get("batch"):
        await callback.answer("❌ Ошибка: данные потеряны", show_alert=True)
        return

    batch = data["batch"]
    port = data["port"]
    transport = data["transport"]

    ports_dict = {
        "ariyb": "Ариб",
        "volga_port": "ПКФ «Волга-Порт»",
        "yug_ter": "ПКФ «Юг-Тер»",
        "astr_port": "ПАО «Астр.Порт»",
        "univer_port": "Универ.Порт",
        "yuzh_port": "Юж.Порт",
        "agrofud": "Агрофуд",
        "mosport": "Моспорт",
        "cgp": "ЦГП",
        "amp": "АМП",
        "armada": "Армада",
        "strelec": "Стрелец",
        "alfa": "Альфа",
    }

    transports_dict = {
        "railway": "ЖД",
        "grain": "Зерновоз",
        "truck": "Фура",
    }

    port_name = ports_dict.get(port, port)
    transport_name = transports_dict.get(transport, transport)



    farmer_logistics_requests[request_id] = {
        "id": request_id,
        "farmer_id": user_id,
        "batch_id": data["batch_id"],
        "culture": batch["culture"],
        "volume": batch["volume"],
        "price_per_ton": batch["price"],
        "total_sum": batch["volume"] * batch["price"],
        "port_to": port_name,
        "port_code": port,
        "transport_type": transport_name,
        "transport_code": transport,
        "desired_price": data["desired_price"],
        "status": "active",
        "offers_count": 0,
        "created_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "offers": [],
    }

    save_data()

    logging.info(
        f"✅ Фермер {user_id} создал заявку #{request_id}: "
    )

    logists_count = 0
            try:
                msg = (
                    f"🌾 {batch['culture']} • {batch['volume']} т\n"
                    f"📍 От: {farmer_region}\n"
                    f"🚚 Транспорт: {transport_name}\n"
                )

                keyboard = InlineKeyboardMarkup()
                keyboard.add(
                    InlineKeyboardButton(
                        "✅ ОТКЛИКНУТЬСЯ",
                        callback_data=f"logist_respond_farmer_request:{request_id}",
                    ),
                    InlineKeyboardButton(
                    ),
                )

                await bot.send_message(
                    logist_id, msg, reply_markup=keyboard, parse_mode="HTML"
                )
                logists_count += 1
            except Exception as e:

    success_text = (
        f"🎯 <b>ID:</b> #{request_id}\n"
        f"🌾 {batch['culture']} - {batch['volume']} т\n"
        f"💰 {batch['price']:,} ₽/т • Итого: {batch['volume'] * batch['price']:,} ₽\n\n"
        f"📍 От: {farmer_region}\n"
        f"🚚 Транспорт: {transport_name}\n"
        f"📢 Уведомлено логистов: {logists_count}\n"
    )

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📬 МОИ ЗАЯВКИ", callback_data="farmer_my_requests_menu")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu")
    )

    await callback.message.edit_text(
        success_text, reply_markup=keyboard, parse_mode="HTML"
    )
    await state.finish()
    await callback.answer("✅ Заявка создана!", show_alert=True)


@dp.callback_query_handler(text="farmer_my_requests_menu", state="*")
async def farmer_my_requests_menu(callback: types.CallbackQuery, state: FSMContext):
    """МОИ ЗАЯВКИ - просмотр списка"""
    try:
        await state.finish()
        user_id = callback.from_user.id

        # Ищем заявки этого фермера

        if not my_requests:
            await callback.message.edit_text(
                "📬 <b>МОИ ЗАЯВКИ</b>\n\n" "❌ У вас нет заявок на доставку",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton(
                        "🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu"
                    )
                ),
                parse_mode="HTML",
            )
            await callback.answer()
            return

        # Группируем по статусам

        text += f"🟢 <b>Активных:</b> {len(active)}\n"
        text += f"✅ <b>Завершённых:</b> {len(completed)}\n\n"

        keyboard = InlineKeyboardMarkup(row_width=1)

        if active:
            text += "━━━━━━ АКТИВНЫЕ ━━━━━━\n"
            for r in active:
                keyboard.add(
                    InlineKeyboardButton(
                    )
                )

        if completed:
            text += "━━━━━ ЗАВЕРШЁННЫЕ ━━━━━\n"
            for r in completed:
                keyboard.add(
                    InlineKeyboardButton(
                    )
                )

        keyboard.add(
            InlineKeyboardButton("🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()

    except Exception as e:
        logging.error(f"❌ Ошибка в farmer_my_requests_menu: {e}")
        try:
            await callback.answer(f"⚠️ Ошибка: {str(e)[:50]}", show_alert=True)


# ✅ ДОБАВИТЬ второй обработчик с той же логикой:
@dp.message_handler(text="📬 МОИ ЗАЯВКИ", state="*")
async def farmer_my_requests_text(message: types.Message, state: FSMContext):
    """МОИ ЗАЯВКИ - текстовая кнопка"""
    await state.finish()
    user_id = message.from_user.id


    if not my_requests:
        await message.answer(
            "📬 <b>МОИ ЗАЯВКИ</b>\n\n❌ У вас нет заявок",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton(
                    "🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu"
                )
            ),
            parse_mode="HTML",
        )
        return


    text = f"📬 <b>МОИ ЗАЯВКИ</b>\n\n🟢 Активных: {len(active)}\n✅ Завершённых: {len(completed)}\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

    if active:
        text += "━━━━━━ АКТИВНЫЕ ━━━━━━\n"
        for r in active:
            keyboard.add(
                InlineKeyboardButton(
                )
            )

    if completed:
        text += "━━━━━ ЗАВЕРШЁННЫЕ ━━━━━\n"
        for r in completed:
            keyboard.add(
                InlineKeyboardButton(
                )
            )

    keyboard.add(
        InlineKeyboardButton("🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu")
    )

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query_handler(
    lambda c: c.data.startswith("farmer_request_view:"), state="*"
)
async def farmer_request_view(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей заявки"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if not request:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    user_id = callback.from_user.id

        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return

    text = (
    )

    keyboard = InlineKeyboardMarkup(row_width=2)

    # Если есть отклики
        keyboard.add(
            InlineKeyboardButton(
                )
            )

    # ✅ ДОБАВЛЯЕМ КНОПКИ УПРАВЛЕНИЯ ЗАЯВКОЙ
    keyboard.add(
        InlineKeyboardButton(
        ),
        InlineKeyboardButton(
        ),
    )

    keyboard.add(
        InlineKeyboardButton("📬 МОИ ЗАЯВКИ", callback_data="farmer_my_requests_menu")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


def get_choice_keyboard(field, request_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    if field == "culture":
        for option in CULTURES:
            keyboard.insert(
                InlineKeyboardButton(
                    option,
                    callback_data=f"edit_field_val:{field}:{option}:{request_id}",
                )
            )
    elif field == "transport_type":
        for t, emoji in TRANSPORT_TYPES.items():
            keyboard.insert(
                InlineKeyboardButton(
                    f"{emoji} {t}",
                    callback_data=f"edit_field_val:{field}:{t}:{request_id}",
                )
            )
    elif field == "port_to":
        for port in PORTS:
            keyboard.insert(
                InlineKeyboardButton(
                    port, callback_data=f"edit_field_val:{field}:{port}:{request_id}"
                )
            )
    keyboard.add(
        InlineKeyboardButton(
            "➕ Другое", callback_data=f"edit_field_custom:{field}:{request_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton("⬅️ Назад", callback_data=f"edit_request:{request_id}")
    )
    return keyboard


@dp.callback_query_handler(lambda c: c.data.startswith("edit_request:"), state="*")
async def edit_request_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
        await callback.answer("❌ Заявка не найдена, либо не ваша", show_alert=True)
        return
    await state.update_data(request_id=request_id)
    text = f"✏️ <b>Редактирование заявки #{request_id}</b>\nВыберите поле для изменения:"
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "🌾 Культура", callback_data=f"edit_req_field:culture:{request_id}"
        ),
        InlineKeyboardButton(
            "📦 Объём", callback_data=f"edit_req_field:volume:{request_id}"
        ),
        InlineKeyboardButton(
            "💰 Цена/т", callback_data=f"edit_req_field:price_per_ton:{request_id}"
        ),
        InlineKeyboardButton(
        ),
        InlineKeyboardButton(
            "🚚 Транспорт", callback_data=f"edit_req_field:transport_type:{request_id}"
        ),
        InlineKeyboardButton(
            "🗑️ Отмена", callback_data=f"farmer_request_view:{request_id}"
        ),
    )
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_req_field:"), state="*")
async def edit_req_field_handler(callback: types.CallbackQuery, state: FSMContext):
    _, field, request_id = callback.data.split(":")
    if field in ["culture", "transport_type", "port_to"]:
        await callback.message.edit_text(
            f"📝 Выберите новое значение для <b>{field}</b>:",
            reply_markup=get_choice_keyboard(field, request_id),
            parse_mode="HTML",
        )
        await EditRequestStates.waiting_for_choice.set()
    elif field in ["volume", "price_per_ton"]:
        await callback.message.edit_text(
            f"📝 Введите новое значение для <b>{field}</b> (только число):",
            parse_mode="HTML",
        )
        await EditRequestStates.waiting_for_number.set()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("edit_field_val:"),
    state=EditRequestStates.waiting_for_choice,
)
async def edit_field_value_choice(callback: types.CallbackQuery, state: FSMContext):
    _, field, value, request_id = callback.data.split(":")
    await state.finish()
    user_id = callback.from_user.id
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    request[field] = value
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton(
            "⬅️ Назад к заявке", callback_data=f"farmer_request_view:{request_id}"
        )
    )
    await callback.message.edit_text(
        f"✅ Значение поля <b>{field}</b> обновлено!",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("edit_field_custom:"),
    state=EditRequestStates.waiting_for_choice,
)
async def edit_field_custom_choice(callback: types.CallbackQuery, state: FSMContext):
    _, field, request_id = callback.data.split(":")
    await callback.message.edit_text(
        f"📝 Введите своё значение для <b>{field}</b>:", parse_mode="HTML"
    )
    await EditRequestStates.waiting_for_custom_value.set()
    await callback.answer()


@dp.message_handler(state=EditRequestStates.waiting_for_custom_value)
async def process_custom_value(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    field = user_data.get("edit_field")
    request_id = user_data.get("request_id")
    new_value = message.text.strip()
        await message.answer("❌ Доступ запрещён")
        await state.finish()
        return
    request[field] = new_value
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton(
            "⬅️ Назад к заявке", callback_data=f"farmer_request_view:{request_id}"
        )
    )
    await message.answer(
        f"✅ Значение <b>{field}</b> обновлено!",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await state.finish()


@dp.message_handler(state=EditRequestStates.waiting_for_number)
async def process_number_field(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    field = user_data.get("edit_field")
    request_id = user_data.get("request_id")
    value = message.text.strip().replace(",", ".")
    try:
        value = float(value)
        if value <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите корректное положительное число!")
        return
        await message.answer("❌ Доступ запрещён")
        await state.finish()
        return
    request[field] = value
    if field == "volume":
        request["total_sum"] = value * float(request.get("price_per_ton", 0))
    if field == "price_per_ton":
        request["total_sum"] = float(request.get("volume", 0)) * value
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton(
            "⬅️ Назад к заявке", callback_data=f"farmer_request_view:{request_id}"
        )
    )
    await message.answer(
        f"✅ Значение <b>{field}</b> обновлено!",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await state.finish()


@dp.callback_query_handler(
)
    await state.finish()
    await farmer_request_view(
        callback, state
    )  # должна быть реализована или уже реализована у вас
    await callback.answer()


# ============================================================================
# УДАЛЕНИЕ ЗАЯВКИ ФЕРМЕРА НА ЛОГИСТИКУ
# ============================================================================


@dp.callback_query_handler(lambda c: c.data.startswith("delete_request:"), state="*")
async def confirm_delete_request(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение удаления заявки"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    # Получаем заявку
    if not request:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    user_id = callback.from_user.id

    # Проверка владельца
        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return

    # Сообщение с подтверждением
    text = f"""
⚠️ <b>Подтверждение удаления</b>

Вы действительно хотите удалить заявку?

📋 <b>Заявка #{request['id']}</b>
🌾 {request.get('culture', 'Не указано')} - {request.get('volume', 0)} т
📍 {request.get('farmer_region', 'Не указано')} → {request.get('port_to', 'Не указано')}
💰 Цена доставки: {request.get('desired_price', 0):,} ₽/т

❗️ <b>Это действие нельзя отменить!</b>
"""


    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, удалить", callback_data=f"confirm_delete_request:{request['id']}"
        ),
        InlineKeyboardButton(
            "❌ Отмена", callback_data=f"farmer_request_view:{request['id']}"
        ),
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_delete_request:"), state="*"
)
async def delete_request_final(callback: types.CallbackQuery, state: FSMContext):
    """Окончательное удаление заявки"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    # Получаем заявку
    if not request:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    user_id = callback.from_user.id

    # Проверка владельца
        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return

    # Сохраняем данные для уведомлений
    culture = request.get("culture", "Не указано")
    volume = request.get("volume", 0)



    # ✅ Сохраняем в файл
    try:
        save_farmers_logistics()
        logging.info(f"✅ Заявки сохранены после удаления #{request_id}")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения заявок: {e}")

    # Уведомляем логистов об удалении заявки
    for logist_id in logist_ids:
        try:
            await bot.send_message(
                logist_id,
                f"Заявка #{request_id} ({culture}, {volume}т) была удалена фермером.\n\n"
                parse_mode="HTML",
            )
        except Exception as e:
            logging.error(f"Не удалось уведомить логиста {logist_id}: {e}")

    logging.info(f"🗑️ Заявка #{request_id} удалена пользователем {user_id}")

    # Показываем список оставшихся заявок

    if not my_requests:
        text = "📬 <b>МОИ ЗАЯВКИ</b>\n\n✅ Заявка удалена!\n\n❌ У вас больше нет активных заявок"
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🏠 Главное меню", callback_data="farmer_main_menu")
        )
    else:

        text = f"📬 <b>МОИ ЗАЯВКИ</b>\n\n✅ <b>Заявка #{request_id} удалена!</b>\n\n"
        text += f"🟢 Активных: {len(active)}\n"
        text += f"✅ Завершённых: {len(completed)}\n\n"

        keyboard = InlineKeyboardMarkup(row_width=1)

        if active:
            text += "━━━━━━ АКТИВНЫЕ ━━━━━━\n"
            for r in active:
                keyboard.add(
                    InlineKeyboardButton(
                        label, callback_data=f"farmer_request_view:{r['id']}"
                    )
                )

        keyboard.add(
            InlineKeyboardButton("🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu")
        )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer("✅ Заявка удалена!", show_alert=True)


@dp.callback_query_handler(
    lambda c: c.data.startswith("farmer_view_offers:"), state="*"
)
async def farmer_view_offers(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    user_id = callback.from_user.id
        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return

            InlineKeyboardButton(
        )
        )
        await callback.answer()
        return

    text = f"📞 <b>ОТКЛИКИ НА ЗАЯВКУ #{request_id}</b>\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

        text += (
            f"━━━ ОТКЛИК {i} ━━━\n"
        )

        keyboard.add(
            InlineKeyboardButton(
            )
        )

    keyboard.add(
        InlineKeyboardButton(
        )
    )
    keyboard.add(
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("farmer_contact_logist:"), state="*"
)
async def farmer_contact_logist(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    try:
        parts = callback.data.split(":")
        logist_id = int(parts[2])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

        return

    offer = None
            offer = o
            break

    if not offer:
        await callback.answer("❌ Отклик не найден", show_alert=True)
        return

    text = (

    keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
            )
        )
    keyboard.add(
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# ОБРАБОТЧИК КНОПКИ: СОЗДАТЬ ЗАЯВКУ (мини-клавиатура в подтверждении)
@dp.callback_query_handler(text="farmer_confirm_request", state="*")
async def farmer_confirm_request_button(
    callback: types.CallbackQuery, state: FSMContext
):
    """Кнопка СОЗДАТЬ ЗАЯВКУ в подтверждении"""
    user_id = callback.from_user.id

    # Получаем данные из state
    data = await state.get_data()

    if not data:
        await callback.answer("❌ Ошибка! Данные не найдены", show_alert=True)
        return

    # Создаём заявку в farmer_logistics_requests

    farmer_logistics_requests[request_id] = {
        "id": request_id,
        "user_id": user_id,
        "culture": data.get("culture"),
        "created_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "status": "active",
    }

    await state.finish()

    await callback.message.edit_text(
        f"✅ <b>Заявка #{request_id} успешно создана!</b>\n\n"
        f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        parse_mode="HTML",
    )
    await callback.answer("✅ Заявка создана!", show_alert=False)


# Второй обработчик - оставьте как есть!
@dp.callback_query_handler(text="farmer_cancel_request", state="*")
async def farmer_cancel_request_button(
    callback: types.CallbackQuery, state: FSMContext
):
    """Кнопка ОТМЕНА"""
    await state.finish()

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("🏠 ГЛАВНОЕ МЕНЮ", callback_data="farmer_main_menu")
    )

    await callback.message.edit_text(
        "❌ Создание заявки отменено.", reply_markup=keyboard
    )
    await callback.answer()


# ГЛАВНОЕ МЕНЮ ФЕРМЕРА (добавьте обработчик если его нет)
@dp.callback_query_handler(text="farmer_main_menu", state="*")
async def farmer_main_menu(callback: types.CallbackQuery, state: FSMContext):
    """Главное меню фермера"""
    await state.finish()
    )
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("logist_respond_farmer_request:"), state="*"
)
async def logist_respond_farmer_request(
    callback: types.CallbackQuery, state: FSMContext
):
    await state.finish()
    logist_id = callback.from_user.id

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

        return



# ──────────────────────────────────────────────────────────────────────────
# 2. ПРОСМОТР И ВЫБОР ЛОГИСТА
# ──────────────────────────────────────────────────────────────────────────


@dp.callback_query_handler(
    lambda c: c.data.startswith("select_logistics_for_pull:"), state="*"
)
async def show_logistics_for_pull(callback: types.CallbackQuery):
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Пулл не найден", show_alert=True)
        return

        await callback.answer("⚠️ Пулл ещё не собран", show_alert=True)
        return

    available_logistics = []

    for user_id, user_data in users.items():

    if not available_logistics:
        await callback.answer(
        )
        return

    text = f"🚚 <b>Выбор логиста для пулла #{pull_id}</b>\n\n"
    text += (
        f"🌾 {pull.get('culture', 'Культура')} • {pull.get('target_volume', 0):.1f} т\n"
    )
    text += f"<b>Доступно логистов: {len(available_logistics)}</b>\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

    for log_id, log_card, log_user in available_logistics:


        keyboard.add(
            InlineKeyboardButton(
                btn_text, callback_data=f"view_logistic_card:{pull_id}:{log_id}"
            )
        )

    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_logistic_card:"), state="*"
)
async def view_logistic_card_for_selection(callback: types.CallbackQuery):
    try:
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        return
        return




    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Выбрать этого логиста",
            callback_data=f"confirm_select_logistic:{pull_id}:{log_id}",
        )
    )
    keyboard.add(
        InlineKeyboardButton(
            "◀️ Назад к списку", callback_data=f"select_logistics_for_pull:{pull_id}"
        )
    )

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_select_logistic:"), state="*"
)
async def confirm_select_logistic(callback: types.CallbackQuery):
    try:
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Пулл не найден", show_alert=True)
        return


    pull["selected_logistic"] = log_id
    save_data()

    # Уведомляем логиста
    try:
        await bot.send_message(
            log_id,
                f"📦 Пулл #{pull_id}\n"
                f"🌾 {pull.get('culture', 'Культура')} • {pull.get('target_volume', 0):.1f} т\n"
                f"🚢 Порт: {pull.get('port', 'Не указан')}\n\n"
            parse_mode="HTML",
        )
    except Exception as e:

    await callback.message.edit_text(
            f"📦 Для пулла #{pull_id}\n\n"
        parse_mode="HTML",
    )


# ──────────────────────────────────────────────────────────────────────────
# 3. ПРОСМОТР И ВЫБОР ЭКСПЕДИТОРА
# ──────────────────────────────────────────────────────────────────────────
@dp.callback_query_handler(
    lambda c: c.data.startswith("select_expeditor_for_pull:"), state="*"
)
async def show_expeditors_for_pull(callback: types.CallbackQuery):
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Пулл не найден", show_alert=True)
        return

        await callback.answer("⚠️ Пулл ещё не собран", show_alert=True)
        return

    available_expeditors = []


    if not available_expeditors:
        await callback.answer(
        )
        return

    text = f"📄 <b>Выбор экспедитора для пулла #{pull_id}</b>\n\n"
    text += (
        f"🌾 {pull.get('culture', 'Культура')} • {pull.get('target_volume', 0):.1f} т\n"
    )
    text += f"<b>Доступно экспедиторов: {len(available_expeditors)}</b>\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

    for exp_id, exp_card, exp_user in available_expeditors:

        keyboard.add(
            InlineKeyboardButton(
                btn_text, callback_data=f"view_expeditor_card:{pull_id}:{exp_id}"
            )
        )

    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_expeditor_card:"), state="*"
)
async def view_expeditor_card_for_selection(callback: types.CallbackQuery):
    try:
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if exp_id not in expeditor_cards:
        return

    card = expeditor_cards[exp_id]




    text = (
        f"📝 <b>Описание:</b>\n{description}\n\n"
    )

    # Контакты
    text += "<b>👤 Контакты:</b>\n"
    if card.get("phone") or exp_user.get("phone"):
        phone = card.get("phone") or exp_user.get("phone")
        text += f"📞 {phone}\n"
    if card.get("email") or exp_user.get("email"):
        email = card.get("email") or exp_user.get("email")
        text += f"✉️ {email}\n"

    text += f"\n📅 Создана: {card.get('created_at', 'Н/Д')}\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
        )
    )
    keyboard.add(
        InlineKeyboardButton(
            "◀️ Назад к списку", callback_data=f"select_expeditor_for_pull:{pull_id}"
        )
    )

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(
)
    try:
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Пулл не найден", show_alert=True)
        return



    save_data()

    try:
        await bot.send_message(
            f"🌾 {pull.get('culture','Культура')} • {pull.get('target_volume',0):.1f} т\n"
            f"🚢 Порт: {pull.get('port','Не указан')}\n\n"
            parse_mode="HTML",
        )
    except Exception as e:

    await callback.message.edit_text(
        parse_mode="HTML",
    )


@dp.message_handler(state=SearchBatchesStatesGroup.enter_min_volume)
async def search_min_volume(message: types.Message, state: FSMContext):
    """Ввод минимального объёма при поиске"""
    try:
        min_volume = float(message.text.strip().replace(",", "."))
        if min_volume < 0:
            raise ValueError

        data = await state.get_data()

        if data.get("search_type") == "volume":
            await perform_search(message, {"min_volume": min_volume})
            await state.finish()
        else:
            await state.update_data(min_volume=min_volume)
            await message.answer(
                "Введите максимальный объём (в тоннах, или 0 если не важно):"
            )
            await SearchBatchesStatesGroup.enter_max_volume.set()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число:")


@dp.message_handler(state=SearchBatchesStatesGroup.enter_max_volume)
async def search_max_volume(message: types.Message, state: FSMContext):
    """Ввод максимального объёма при поиске"""
    try:
        max_volume_text = message.text.strip()
        max_volume = (
            float(max_volume_text.replace(",", ".")) if max_volume_text != "0" else 0
        )

        if max_volume < 0:
            raise ValueError

        await state.update_data(max_volume=max_volume)
        await message.answer("Введите минимальную цену (₽/тонна):")
        await SearchBatchesStatesGroup.enter_min_price.set()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число:")


@dp.message_handler(state=SearchBatchesStatesGroup.enter_min_price)
async def search_min_price(message: types.Message, state: FSMContext):
    """Ввод минимальной цены при поиске"""
    try:
        min_price = float(message.text.strip().replace(",", "."))
        if min_price < 0:
            raise ValueError

        await state.update_data(min_price=min_price)
        await message.answer(
            "Введите максимальную цену (₽/тонна, или 0 если не важно):"
        )
        await SearchBatchesStatesGroup.enter_max_price.set()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число:")


@dp.message_handler(state=SearchBatchesStatesGroup.enter_max_price)
async def search_max_price(message: types.Message, state: FSMContext):
    """Завершение комплексного поиска"""
    try:
        max_price_text = message.text.strip()
        max_price = (
            float(max_price_text.replace(",", ".")) if max_price_text != "0" else 0
        )

        if max_price < 0:
            raise ValueError

        data = await state.get_data()
        search_params = {
            "culture": data.get("culture"),
            "region": data.get("region"),
            "min_volume": data.get("min_volume", 0),
            "max_volume": data.get("max_volume", 0),
            "min_price": data.get("min_price", 0),
            "max_price": max_price,
            "status": "Активна",
        }

        await perform_search(message, search_params)
        await state.finish()

    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число:")


@dp.callback_query_handler(
    lambda c: c.data.startswith("quality:"),
    state=SearchBatchesStatesGroup.enter_quality_class,
)
async def search_by_quality(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора класса качества при поиске"""
    quality_class = callback.data.split(":", 1)[1]

    await perform_search(callback.message, {"quality_class": quality_class})
    await state.finish()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("storage:"),
    state=SearchBatchesStatesGroup.enter_storage_type,
)
async def search_by_storage(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора типа хранения при поиске"""
    storage_type = callback.data.split(":", 1)[1]

    await perform_search(callback.message, {"storage_type": storage_type})
    await state.finish()
    await callback.answer()


async def perform_search(message, search_params):
    """Выполнение поиска по заданным параметрам"""
    found_batches = []
        if matches_search_criteria(batch, search_params):

    if not found_batches:
        await message.answer(
            "🔍 <b>Результаты поиска</b>\n\n"
            "По вашему запросу ничего не найдено.\n\n"
            "Попробуйте изменить критерии поиска.",
            parse_mode="HTML",
        )
        return
    found_batches.sort(key=lambda x: x["price"])

    text = "🔍 <b>Результаты поиска</b>\n\n"
    text += f"Найдено партий: {len(found_batches)}\n\n"

    for i, batch in enumerate(found_batches[:10], 1):  # Ограничиваем показ
        text += f"{i}. <b>Партия #{batch['id']}</b>\n"
        text += f"   🌾 {batch['culture']} • {batch['volume']} т\n"
        text += f"   💰 {batch['price']:,.0f} ₽/т\n"
        text += f"   📍 {batch.get('region', 'Не указан')}\n"
        text += f"   ⭐ {batch.get('quality_class', 'Не указано')}\n"
        text += f"   👤 {batch['farmer_name']}\n\n"

    if len(found_batches) > 10:
        text += f"<i>... и ещё {len(found_batches) - 10} партий</i>\n\n"

    text += "💡 <b>Для просмотра деталей свяжитесь с фермером.</b>"

    await message.answer(text, parse_mode="HTML")


def matches_search_criteria(batch, search_params):
    """Проверка соответствия партии критериям поиска"""
        return False
    if search_params.get("culture") and batch["culture"] != search_params["culture"]:
        return False
    if (
        search_params.get("region")
        and batch.get("region", "Не указан") != search_params["region"]
    ):
        return False
    if (
        search_params.get("min_volume", 0) > 0
        and batch["volume"] < search_params["min_volume"]
    ):
        return False
    if (
        search_params.get("max_volume", 0) > 0
        and search_params["max_volume"] < batch["volume"]
    ):
        return False
    if (
        search_params.get("min_price", 0) > 0
        and batch["price"] < search_params["min_price"]
    ):
        return False
    if (
        search_params.get("max_price", 0) > 0
        and search_params["max_price"] < batch["price"]
    ):
        return False
    if search_params.get("quality_class") and batch.get(
        "quality_class"
    ) != search_params.get("quality_class"):
        return False
    if search_params.get("storage_type") and batch.get(
        "storage_type"
    ) != search_params.get("storage_type"):
        return False

    return True


@dp.callback_query_handler(lambda c: c.data.startswith("attach_files:"), state="*")
async def attach_files_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало прикрепления файлов к партии"""
    batch_id = parse_callback_id(callback.data)
    await state.update_data(attach_batch_id=batch_id)

    await callback.message.edit_text(
        f"📎 <b>Прикрепление файлов к партии #{batch_id}</b>\n\n"
        "Отправьте файлы (фото, PDF, документы):\n"
        "• Сертификаты качества\n"
        "• Фото зерна\n"
        "• Документы на партию\n"
        "• Другие relevant файлы\n\n"
        "Когда закончите, нажмите /done",
        parse_mode="HTML",
    )
    await AttachFilesStatesGroup.upload_files.set()
    await callback.answer()


@dp.message_handler(
    content_types=["photo", "document"], state=AttachFilesStatesGroup.upload_files
)
async def attach_files_upload(message: types.Message, state: FSMContext):
    """Обработка загрузки файлов"""
    data = await state.get_data()
    batch_id = data.get("attach_batch_id")

    if not batch:
        await message.answer("❌ Партия не найдена")
        await state.finish()
        return
    if "files" not in batch:
        batch["files"] = []
    file_info = None
    if message.photo:
        file_info = {
            "type": "photo",
            "file_id": message.photo[-1].file_id,
            "caption": message.caption or "",
            "uploaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    elif message.document:
        file_info = {
            "type": "document",
            "file_id": message.document.file_id,
            "file_name": message.document.file_name,
            "mime_type": message.document.mime_type,
            "caption": message.caption or "",
            "uploaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    if file_info:
        batch["files"].append(file_info)
        save_batches_to_pickle()
        if gs and gs.spreadsheet:
            gs.update_batch_in_sheets(batch)

        await message.answer(
            f"✅ Файл добавлен ({len(batch['files'])} всего)\n"
            "Отправьте ещё или нажмите /done для завершения"
        )


@dp.message_handler(commands=["done"], state=AttachFilesStatesGroup.upload_files)
async def attach_files_done(message: types.Message, state: FSMContext):
    """Завершение прикрепления файлов"""
    data = await state.get_data()
    batch_id = data.get("attach_batch_id")
    user_id = message.from_user.id


    await state.finish()

    files_count = len(batch.get("files", [])) if batch else 0

    keyboard = get_role_keyboard(role)

    await message.answer(
        f"Партия #{batch_id}\n"
        f"Всего файлов: {files_count}",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await view_batch_details_direct(message, batch_id, user_id)


@dp.callback_query_handler(lambda c: c.data.startswith("view_files:"), state="*")
async def view_batch_files(callback: types.CallbackQuery):
    """Просмотр файлов партии"""
    batch_id = parse_callback_id(callback.data)
    user_id = callback.from_user.id

    if not batch or not batch.get("files"):
        await callback.answer("📎 Файлов нет", show_alert=True)
        return

    await callback.message.answer(
        f"📎 <b>Файлы партии #{batch_id}</b>\n\n"
        f"Всего файлов: {len(batch['files'])}",
        parse_mode="HTML",
    )
    for file_info in batch["files"]:
        try:
            if file_info["type"] == "photo":
                await callback.message.answer_photo(
                    file_info["file_id"],
                    caption=file_info.get("caption", "")
                    or f"📷 Фото для партии #{batch_id}",
                )
            elif file_info["type"] == "document":
                caption = f"📄 {file_info.get('file_name', 'Документ')}"
                if file_info.get("caption"):
                    caption += f"\n{file_info['caption']}"

                await callback.message.answer_document(
                    file_info["file_id"], caption=caption
                )
        except Exception as e:
            logging.error(f"Ошибка отправки файла: {e}")
            await callback.message.answer(
                f"❌ Не удалось отправить файл: {file_info.get('file_name', 'Неизвестный файл')}"
            )

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "back_to_pulls", state="*")
async def back_to_pulls(callback: types.CallbackQuery):
    """✅ Возврат к списку пулов (УНИВЕРСАЛЬНЫЙ для всех ролей)"""

    user_id = callback.from_user.id

    try:
        # ✅ Проверяем есть ли пользователь в базе
            logging.warning(f"⚠️ Пользователь {user_id} не найден в базе")
            await callback.answer("❌ Пользователь не зарегистрирован", show_alert=True)
            return

        logging.info(
            f"📋 Возврат к спискам. Пользователь {user_id} с ролью: {user_role}"
        )

        # ✅ ЭКСПОРТЕР - показываем его пулы С КНОПКАМИ
        if user_role == "exporter":
            all_pulls = pulls.get("pulls", {})
            my_pulls = {
                pid: pull
                for pid, pull in all_pulls.items()
            }

            if not my_pulls:
                await callback.message.edit_text(
                    "📋 <b>Мои пулы</b>\n\n"
                    "У вас пока нет пулов.\n\n"
                    "💡 <i>Создайте первый пул через меню</i>",
                    parse_mode="HTML",
                )
                await callback.answer()
                logging.info(f"✅ Экспортер {user_id}: пулов не найдено")
                return

            logging.info(f"✅ Экспортер {user_id}: показано {len(my_pulls)} пулов")

            culture_emoji = {
                "пшеница": "🌾",
                "ячмень": "🌾",
                "кукуруза": "🌽",
                "подсолнечник": "🌻",
                "соя": "🫘",
                "рапс": "🌿",
            }

            # СОЗДАЁМ КЛАВИАТУРУ С КНОПКАМИ
            keyboard = InlineKeyboardMarkup(row_width=1)

            for pull_id, pull in my_pulls.items():
                culture = pull.get("culture", "").lower()
                culture_icon = culture_emoji.get(culture, "🌾")
                status_icon = status_map.get(status, "⚪").split()[0]
                current = pull.get("current_volume", 0)
                target = pull.get("target_volume", 1)
                progress = (current / target * 100) if target > 0 else 0

                # Формат кнопки точно как в основном меню
                button_text = f"{status_icon} {culture_icon} {pull.get('culture', '?')} ({progress:.0f}%)"

                # ДОБАВЛЯЕМ КНОПКУ
                keyboard.add(
                    InlineKeyboardButton(
                        button_text, callback_data=f"view_pull:{pull_id}"
                    )
                )

            # ОТПРАВЛЯЕМ С КЛАВИАТУРОЙ
            await callback.message.edit_text(
                f"📋 <b>Мои пулы</b> ({len(my_pulls)} шт.)\n\n"
                "<i>Выберите пул для управления:</i>",
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            await callback.answer()
            logging.info(
                f"✅ Экспортер {user_id}: отправлено меню с {len(my_pulls)} кнопками"
            )

        # ✅ ФЕРМЕР - показываем открытые пулы
        elif user_role == "farmer":
            all_pulls = pulls.get("pulls", {})
            open_pulls = [
                (k, v)
                for k, v in all_pulls.items()
                if isinstance(v, dict)
            ]

            if not open_pulls:
                await callback.message.edit_text(
                    "🎯 <b>Доступные пулы</b>\n\n"
                    "Сейчас нет открытых пулов для участия.",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton(
                            "🔄 Обновить", callback_data="back_to_pulls"
                        )
                    ),
                    parse_mode="HTML",
                )
                await callback.answer()
                logging.info(f"✅ Фермер {user_id}: открытых пулов не найдено")
                return

            keyboard = InlineKeyboardMarkup(row_width=1)

            for pull_id, pull in open_pulls[:10]:
                progress = (
                    pull.get("current_volume", 0) / pull.get("target_volume", 1) * 100
                    if pull.get("target_volume", 1) > 0
                    else 0
                )

                button_text = (
                    f"🌾 {pull.get('culture', '?')} - "
                    f"{pull.get('current_volume', 0):.0f}/{pull.get('target_volume', 0):.0f} т "
                    f"({progress:.0f}%)"
                )
                keyboard.add(
                    InlineKeyboardButton(
                        button_text, callback_data=f"view_pull:{pull_id}"
                    )
                )

            await callback.message.edit_text(
                f"🎯 <b>Доступные пулы</b> ({len(open_pulls)} шт.)\n\n"
                "Выберите пул для просмотра деталей:",
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            await callback.answer()
            logging.info(
                f"✅ Фермер {user_id}: показано {len(open_pulls)} открытых пулов"
            )

        # ✅ ЛОГИСТ и др. - отправляем в главное меню
        else:
            await callback.message.edit_text(
                "🏠 <b>Главное меню</b>\n\n" "Выберите действие:",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("🎯 Пулы", callback_data="view_pools")
                ),
                parse_mode="HTML",
            )
            await callback.answer()
            logging.info(
                f"✅ Пользователь {user_id} ({user_role}): возврат в главное меню"
            )

    except KeyError as e:
        logging.error(f"❌ KeyError: {e}")
        await callback.message.reply("❌ <b>Ошибка данных</b>", parse_mode="HTML")
    except Exception as e:
        logging.error(f"❌ Критичная ошибка в back_to_pulls: {e}", exc_info=True)
        await callback.message.reply(
            "❌ <b>Ошибка системы</b>\nПожалуйста, попробуйте позже",
            parse_mode="HTML",
        )


@dp.message_handler(commands=["stats"], state="*")
@dp.message_handler(commands=["help"], state="*")
async def cmd_help(message: types.Message, state: FSMContext):
    """Справка по боту"""
    await state.finish()

    user_id = message.from_user.id
        await message.answer(
            "ℹ️ <b>Справка по Exportum</b>\n\n"
            "Exportum - платформа для торговли зерном\n\n"
            "Для начала работы:\n"
            "1. Нажмите /start для регистрации\n"
            "2. Выберите вашу роль\n"
            "3. Заполните профиль\n"
            "4. Используйте меню для работы\n\n"
            "Доступные роли:\n"
            "• 🌾 Фермер - продажа зерна\n"
            "• 📦 Экспортёр - покупка и экспорт\n"
            "• 🚚 Логист - перевозки\n"
            "• 🚛 Экспедитор - оформление документов",
            parse_mode="HTML",
        )
        return

    role = user.get("role")

    text = f"ℹ️ <b>Справка для {ROLES.get(role, role)}</b>\n\n"

    if role == "farmer":
        text += (
            "📦 <b>Добавление партий:</b>\n"
            "• Укажите культуру, объём, цену\n"
            "• Добавьте параметры качества\n"
            "• Прикрепите документы и фото\n\n"
            "🔍 <b>Поиск экспортёров:</b>\n"
            "• Автоматический поиск по пулам\n"
            "• Ручной поиск по критериям\n"
            "• Уведомления о совпадениях\n\n"
            "📋 <b>Управление партиями:</b>\n"
            "• Редактирование параметров\n"
            "• Изменение статуса\n"
            "• Просмотр статистики\n\n"
            "💡 <b>Рекомендации:</b>\n"
            "• Поддерживайте актуальные цены\n"
            "• Указывайте точные параметры\n"
            "• Прикрепляйте документы качества"
        )

    elif role == "exporter":
        text += (
            "📦 <b>Создание пулов:</b>\n"
            "• Укажите требования к зерну\n"
            "• Задайте цену FOB\n"
            "• Выберите порт отгрузки\n\n"
            "🔍 <b>Поиск партий:</b>\n"
            "• Расширенный поиск по критериям\n"
            "• Автоматический подбор\n"
            "• Фильтрация по региону и качеству\n\n"
            "🚚 <b>Логистика:</b>\n"
            "• Создание заявок на перевозку\n"
            "• Выбор логистов\n"
            "• Отслеживание доставки\n\n"
            "💡 <b>Рекомендации:</b>\n"
            "• Чётко формулируйте требования\n"
            "• Учитывайте региональные особенности\n"
            "• Своевременно обновляйте пулы"
        )

        text += (
            "🚚 <b>Ваши услуги:</b>\n"
            "• Создание карточки логиста\n"
            "• Указание тарифов и маршрутов\n"
            "• Приём заявок на перевозки\n\n"
            "📋 <b>Работа с заявками:</b>\n"
            "• Просмотр активных заявок\n"
            "• Предложение своих услуг\n"
            "• Общение с экспортёрами\n\n"
            "💼 <b>Ваши предложения:</b>\n"
            "• История предложений\n"
            "• Статусы переговоров\n"
            "• Успешные перевозки\n\n"
            "💡 <b>Рекомендации:</b>\n"
            "• Указывайте реальные тарифы\n"
            "• Оперативно реагируйте на заявки\n"
            "• Поддерживайте актуальность информации"
        )

        text += (
            "🚛 <b>Ваши услуги:</b>\n"
            "• Создание карточки экспедитора\n"
            "• Указание услуг и тарифов\n"
            "• Приём заявок на оформление\n\n"
            "📋 <b>Работа с документами:</b>\n"
            "• Фитосанитарные сертификаты\n"
            "• Ветеринарные свидетельства\n"
            "• Сертификаты качества\n\n"
            "💼 <b>Ваши предложения:</b>\n"
            "• История оформлений\n"
            "• Статусы заявок\n"
            "• Успешные сделки\n\n"
            "💡 <b>Рекомендации:</b>\n"
            "• Чётко описывайте услуги\n"
            "• Указывайте сроки оформления\n"
            "• Поддерживайте репутацию"
        )

    text += "\n\n📞 <b>Поддержка:</b> @exportum_support"

    await message.answer(text, parse_mode="HTML")


@dp.callback_query_handler(lambda c: c.data.startswith("edit_batch:"), state="*")
async def start_edit_batch(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования расширенной партии"""
    batch_id = parse_callback_id(callback.data)


    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return

    await state.update_data(editing_batch_id=batch_id)

    await callback.message.edit_text(
        f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
        "Выберите поле для редактирования:",
        reply_markup=edit_batch_fields_keyboard(batch_id),
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_field:"), state="*")
async def edit_batch_field_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора поля для редактирования партии"""
    field = callback.data.split(":", 1)[1]

    data = await state.get_data()
    batch_id = data.get("editing_batch_id")

    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        return

    await state.update_data(edit_field=field, editing_batch_id=batch_id)
    field_names = {
        "price": "новую цену (₽/тонна)",
        "volume": "новый объём (в тоннах)",
        "humidity": "новую влажность (%)",
        "impurity": "новую сорность (%)",
        "quality_class": "новый класс качества",
        "storage_type": "новый тип хранения",
        "readiness_date": "новую дату готовности (ДД.ММ.ГГГГ)",
        "status": "новый статус",
    }

    if field == "status":
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n" "Выберите новый статус:",
            reply_markup=status_keyboard(),
            parse_mode="HTML",
        )
        await EditBatch.new_value.set()
    elif field == "quality_class":
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
            "Выберите новый класс качества:",
            reply_markup=quality_class_keyboard(),
            parse_mode="HTML",
        )
        await EditBatch.new_value.set()
    elif field == "storage_type":
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
            "Выберите новый тип хранения:",
            reply_markup=storage_type_keyboard(),
            parse_mode="HTML",
        )
        await EditBatch.new_value.set()
    else:
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
            f"Введите {field_names.get(field, 'новое значение')}:"
        )
        await EditBatch.new_value.set()

    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("status:"), state=EditBatch.new_value
)
async def edit_batch_status_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора статуса при редактировании партии"""
    new_status = callback.data.split(":", 1)[1]

    data = await state.get_data()
    batch_id = data.get("editing_batch_id")
    user_id = callback.from_user.id

    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        await state.finish()
        return

    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
    old_value = batch.get("status", "Не указан")
    batch["status"] = new_status
    save_batches_to_pickle()
    if gs and gs.spreadsheet:
        gs.update_batch_in_sheets(batch)

    await state.finish()
    await callback.message.edit_text(
        f"Партия #{batch_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_status}"
    )
    await asyncio.sleep(1)
    await view_batch_details_direct(callback.message, batch_id, user_id)
    await callback.answer("✅ Статус обновлён")


@dp.callback_query_handler(
    lambda c: c.data.startswith("quality:"), state=EditBatch.new_value
)
async def edit_batch_quality_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора класса качества при редактировании партии"""
    new_quality = callback.data.split(":", 1)[1]

    data = await state.get_data()
    batch_id = data.get("editing_batch_id")
    user_id = callback.from_user.id

    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        await state.finish()
        return

    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
    old_value = batch.get("quality_class", "Не указан")
    batch["quality_class"] = new_quality
    save_batches_to_pickle()
    if gs and gs.spreadsheet:
        gs.update_batch_in_sheets(batch)

    await state.finish()

    await callback.message.edit_text(
        f"Партия #{batch_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_quality}"
    )
    await asyncio.sleep(1)
    await view_batch_details_direct(callback.message, batch_id, user_id)
    await callback.answer("✅ Класс качества обновлён")


@dp.callback_query_handler(
    lambda c: c.data.startswith("storage:"), state=EditBatch.new_value
)
async def edit_batch_storage_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора типа хранения при редактировании партии"""
    new_storage = callback.data.split(":", 1)[1]

    data = await state.get_data()
    batch_id = data.get("editing_batch_id")
    user_id = callback.from_user.id

    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        await state.finish()
        return

    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
    old_value = batch.get("storage_type", "Не указан")
    batch["storage_type"] = new_storage
    save_batches_to_pickle()
    if gs and gs.spreadsheet:
        gs.update_batch_in_sheets(batch)

    await state.finish()

    await callback.message.edit_text(
        f"Партия #{batch_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_storage}"
    )
    await asyncio.sleep(1)
    await view_batch_details_direct(callback.message, batch_id, user_id)
    await callback.answer("✅ Тип хранения обновлён")


@dp.message_handler(state=EditBatch.new_value)
async def edit_batch_new_value(message: types.Message, state: FSMContext):
    """Обработка ввода нового значения для редактирования партии"""
    data = await state.get_data()
    batch_id = data.get("editing_batch_id")
    field = data.get("edit_field")
    user_id = message.from_user.id

    if not batch_id or not field:
        await message.answer("❌ Ошибка: данные не найдены")
        await state.finish()
        return

    if not batch:
        await message.answer("❌ Партия не найдена")
        await state.finish()
        return

    new_value = message.text.strip()
    try:
        if field in ["price", "volume", "humidity", "impurity"]:
            new_value_float = float(new_value.replace(",", "."))
            if field == "price" and new_value_float <= 0:
                await message.answer(
                    "❌ Цена должна быть больше 0. Попробуйте ещё раз:"
                )
                return
            elif field == "volume" and new_value_float <= 0:
                await message.answer(
                    "❌ Объём должен быть больше 0. Попробуйте ещё раз:"
                )
                return
            elif field in ["humidity", "impurity"] and not (
                0 <= new_value_float <= 100
            ):
                await message.answer(
                    "❌ Значение должно быть от 0 до 100. Попробуйте ещё раз:"
                )
                return

            old_value = batch.get(field, "Не указано")
            batch[field] = new_value_float
            if field in ["humidity", "impurity"]:
                batch["quality_class"] = determine_quality_class(
                    batch.get("humidity", 0), batch.get("impurity", 0)
                )

        elif field == "readiness_date":
            if new_value.lower() == "сейчас":
                new_value = datetime.now().strftime("%d.%m.%Y")
            elif not validate_date(new_value):
                await message.answer(
                    "❌ Некорректная дата. Используйте формат ДД.ММ.ГГГГ или 'сейчас'. Попробуйте ещё раз:"
                )
                return

            old_value = batch.get(field, "Не указано")
            batch[field] = new_value

        else:
            old_value = batch.get(field, "Не указано")
            batch[field] = new_value
        save_batches_to_pickle()
        if gs and gs.spreadsheet:
            gs.update_batch_in_sheets(batch)

        await state.finish()
        field_names_ru = {
            "price": "Цена",
            "volume": "Объём",
            "humidity": "Влажность",
            "impurity": "Сорность",
            "readiness_date": "Дата готовности",
        }

        keyboard = get_role_keyboard(role)

        await message.answer(
            f"✅ <b>{field_names_ru.get(field, field.capitalize())} обновлена!</b>\n\n"
            f"Партия #{batch_id}\n"
            f"Старое значение: {old_value}\n"
            f"Новое значение: {new_value}",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        await view_batch_details_direct(message, batch_id, user_id)

    except ValueError:
        await message.answer(
            "❌ Некорректное значение. Введите число.\n" "Попробуйте ещё раз:"
        )


@dp.callback_query_handler(lambda c: c.data == "edit_cancel", state="*")
async def edit_cancel(callback: types.CallbackQuery, state: FSMContext):
    """Отмена редактирования"""
    await state.finish()
    await callback.message.edit_text("❌ Редактирование отменено")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("delete_batch:"), state="*")
async def delete_batch_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало удаления партии"""
    batch_id = parse_callback_id(callback.data)
    user_id = callback.from_user.id
    batch_exists = False
    if user_id in batches:
        for b in batches[user_id]:
            if b["id"] == batch_id:
                batch_exists = True
                break

    if not batch_exists:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, удалить", callback_data=f"confirm_delete_batch:{batch_id}"
        ),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete_batch"),
    )

    await callback.message.edit_text(
        f"Вы уверены, что хотите удалить партию #{batch_id}?\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_delete_batch:"), state="*"
)
async def delete_batch_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение удаления партии - улучшенная версия"""
    await state.finish()

    try:
        batch_id = int(callback.data.split(":")[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка!", show_alert=True)
        return

    user_id = callback.from_user.id

    # Находим партию в batches
    batch = None
    if user_id in batches:
        for b in batches[user_id]:
                batch = b
                break

    if not batch:
        await callback.answer("❌ Партия не найдена!", show_alert=True)
        return

    # Удаляем партию из batches
    if user_id in batches:
        save_batches_to_pickle()
        logging.info(f"✅ Партия #{batch_id} удалена из batches")

    # Удаляем партию из пулов
    removed_from_pulls = []
    all_pulls = pulls.get("pulls", {})

    for pull_id, pull in all_pulls.items():
        if not isinstance(pull, dict):
            continue

            logging.info(f"🗑️ Удален batch_id={batch_id} из pull #{pull_id}")

            # Удаляем из pull['batches']
            initial_len = len(pull.get("batches", []))
            pull["batches"] = [
            ]
            if len(pull["batches"]) < initial_len:

            # Пересчитываем current_volume
            new_volume = sum(b.get("volume", 0) for b in pull.get("batches", []))
            pull["current_volume"] = new_volume
            logging.info(f"📉 Обновлен current_volume: {pull.get('current_volume')}")

            # Восстанавливаем статус, если пул больше не заполнен
                "target_volume", 0
            ):
                pull["status"] = "open"
                logging.info(f"✅ Пул #{pull_id} возвращен в статус 'open'")

            # Удаляем farmer_id, если у него больше нет партий
            if "farmer_ids" in pull:
                farmer_batches = [
                ]
                    logging.info(f"🗑️ Удален farmer_id={user_id} из пулла")

            removed_from_pulls.append(pull_id)

    for pull_id in removed_from_pulls:
                ]
                    logging.info(
                    )

    # Сохраняем данные, если пулы были изменены
    if removed_from_pulls:
        save_pulls_to_pickle()

    # Удаляем из Google Sheets, если интегрировано
    if gs and hasattr(gs, "spreadsheet") and gs.spreadsheet:
        try:
            gs.delete_batch_from_sheets(batch_id)
        except Exception as e:
            logging.error(f"Ошибка Google Sheets: {e}")

    # Сообщаем пользователю об успешном удалении
    message = f"✅ Партия <b>#{batch_id}</b> удалена!"
    if removed_from_pulls:
        message += f"\n🔄 Также удалена из пулов: {', '.join(f'#{pid}' for pid in removed_from_pulls)}"

    await callback.message.edit_text(message, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("editfield_crop_"), state="*")
async def edit_crop_field(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик редактирования культуры"""
    await state.finish()

    try:
        batch_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError) as e:
        logger.error(f"Ошибка парсинга batch_id: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    user_id = callback.from_user.id
    load_batches_from_pickle()

    batch = None
    farmer_id = None
    for f_id, user_batches in batches.items():
        for b in user_batches:
                batch = b
                farmer_id = f_id
                break
        if batch:
            break

        await callback.answer("❌ Партия не найдена", show_alert=True)
        return

    await state.update_data(batch_id=batch_id)

    keyboard = InlineKeyboardMarkup(row_width=2)
    cultures = ["Пшеница", "Кукуруза", "Ячмень", "Подсолнечник", "Рапс", "Соя"]

    for culture in cultures:
        keyboard.insert(
            InlineKeyboardButton(culture, callback_data=f"setcrop_{batch_id}_{culture}")
        )

    keyboard.add(
        InlineKeyboardButton("❌ Отмена", callback_data=f"editcancel_{batch_id}")
    )

    await callback.message.edit_text(
        f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("setcrop_"), state="*")
async def set_crop_value(callback: types.CallbackQuery, state: FSMContext):
    """Установка новой культуры"""
    await state.finish()

    try:
        parts = callback.data.split("_")
        batch_id = int(parts[1])
        new_crop = "_".join(parts[2:])
    except (IndexError, ValueError) as e:
        logger.error(f"Ошибка парсинга: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    user_id = callback.from_user.id
    load_batches_from_pickle()

    updated = False
    old_crop = "Неизвестно"

    for farmer_id, user_batches in batches.items():
            continue
        for batch in user_batches:
                old_crop = batch.get("culture", "Неизвестно")
                batch["culture"] = new_crop
                updated = True
                break
        if updated:
            break

    if not updated:
        await callback.answer("❌ Ошибка обновления", show_alert=True)
        return

    await callback.message.edit_text(
        f"Партия #{batch_id}\n"
        f"Было: {old_crop}\n"
        f"Стало: {new_crop}",
        reply_markup=batch_actions_keyboard(batch_id),
        parse_mode="HTML",
    )
    await callback.answer("✅ Культура обновлена!")


@dp.callback_query_handler(lambda c: c.data.startswith("editpull_"), state="*")
async def start_edit_pull(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования пула"""
    try:
        # ✅ ИСПРАВЛЕНО: парсим через подчеркивание
        pull_id = parse_callback_id(callback.data)
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
        return


    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return

        await callback.answer(
            "❌ Нет доступа к редактированию этого пула", show_alert=True
        )
        return

    await state.update_data(editing_pull_id=pull_id)

    try:
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n"
            "Выберите поле для редактирования:",
            reply_markup=edit_pull_fields_keyboard(),
            parse_mode="HTML",
        )
    except MessageNotModified:
        pass  # Сообщение уже в нужном виде
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_pull_field:"), state="*")
async def edit_pull_field_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора поля для редактирования пула"""
    field = callback.data.split(":", 1)[1]

    data = await state.get_data()
    pull_id = data.get("editing_pull_id")

    if not pull_id:
        await callback.answer("❌ Ошибка: пул не найден", show_alert=True)
        return

    await state.update_data(edit_pull_field=field, editing_pull_id=pull_id)

    field_names = {
        "culture": "новую культуру",
        "volume": "новый целевой объём (в тоннах)",
        "price": "новую цену FOB (₽/тонна)",
        "port": "новый порт отгрузки",
        "moisture": "новую максимальную влажность (%)",
        "nature": "новую минимальную натуру (г/л)",
        "impurity": "новую максимальную сорность (%)",
        "weed": "новую максимальную засорённость (%)",
    }

    if field == "culture":
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n" "Выберите новую культуру:",
            reply_markup=culture_keyboard(),
        )
        await EditPullStatesGroup.edit_culture.set()
    elif field == "port":
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n" "Выберите новый порт:",
            reply_markup=port_keyboard(),
        )
        await EditPullStatesGroup.edit_port.set()
    else:
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n"
            f"Введите {field_names.get(field, 'новое значение')}:"
        )
        state_mapping = {
            "volume": EditPullStatesGroup.edit_volume,
            "price": EditPullStatesGroup.edit_price,
            "moisture": EditPullStatesGroup.edit_moisture,
            "nature": EditPullStatesGroup.edit_nature,
            "impurity": EditPullStatesGroup.edit_impurity,
            "weed": EditPullStatesGroup.edit_weed,
        }

        await state_mapping.get(field, EditPullStatesGroup.edit_volume).set()

    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("culture:"), state=EditPullStatesGroup.edit_culture
)
async def edit_pull_culture(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора культуры при редактировании пула"""
    new_culture = callback.data.split(":", 1)[1]

    data = await state.get_data()
    pull_id = data.get("editing_pull_id")

        await callback.answer("❌ Ошибка: пул не найден", show_alert=True)
        await state.finish()
        return


    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        await state.finish()
        return

    old_value = pull.get("culture")
    pull["culture"] = new_culture

    save_pulls_to_pickle()

    if gs and gs.spreadsheet:
        gs.update_pull_in_sheets(pull)

    await state.finish()

    await callback.message.edit_text(
        f"Пул #{pull_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_culture}",
        parse_mode="HTML",
    )
    await asyncio.sleep(1)
    await callback.answer("✅ Культура обновлена")


@dp.callback_query_handler(
    lambda c: c.data.startswith("port:"), state=EditPullStatesGroup.edit_port
)
async def edit_pull_port(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора порта при редактировании пула"""
    port_index = parse_callback_id(callback.data)

    # 📍 Новый список портов
    ports = [
        "Ариб",
        "ПКФ «Волга-Порт»",
        "ПКФ «Юг-Тер»",
        "ПАО «Астр.Порт»",
        "Универ.Порт",
        "Юж.Порт",
        "Агрофуд",
        "Моспорт",
        "ЦГП",
        "АМП",
        "Армада",
        "Стрелец",
        "Альфа",
    ]

    new_port = ports[port_index] if port_index < len(ports) else "Астрахань"

    data = await state.get_data()
    pull_id = data.get("editing_pull_id")

        await callback.answer("❌ Ошибка: пул не найден", show_alert=True)
        await state.finish()
        return


    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        await state.finish()
        return

    old_value = pull.get("port")
    pull["port"] = new_port

    save_pulls_to_pickle()

    if gs and gs.spreadsheet:
        gs.update_pull_in_sheets(pull)

    await state.finish()

    await callback.message.edit_text(
        f"Пул #{pull_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_port}",
        parse_mode="HTML",
    )
    await asyncio.sleep(1)
    await callback.answer("✅ Порт обновлён")


@dp.message_handler(state=EditPullStatesGroup.edit_volume)
async def edit_pull_volume(message: types.Message, state: FSMContext):
    """Обработка ввода объёма при редактировании пула"""
    await edit_pull_numeric_field(message, state, "target_volume", "Объём")


@dp.message_handler(state=EditPullStatesGroup.edit_price)
async def edit_pull_price(message: types.Message, state: FSMContext):
    """Обработка ввода цены при редактировании пула"""
    await edit_pull_numeric_field(message, state, "price", "Цена")


@dp.message_handler(state=EditPullStatesGroup.edit_moisture)
async def edit_pull_moisture(message: types.Message, state: FSMContext):
    """Обработка ввода влажности при редактировании пула"""
    await edit_pull_numeric_field(message, state, "moisture", "Влажность", 0, 100)


@dp.message_handler(state=EditPullStatesGroup.edit_nature)
async def edit_pull_nature(message: types.Message, state: FSMContext):
    """Обработка ввода натуры при редактировании пула"""
    await edit_pull_numeric_field(message, state, "nature", "Натура")


@dp.message_handler(state=EditPullStatesGroup.edit_impurity)
async def edit_pull_impurity(message: types.Message, state: FSMContext):
    """Обработка ввода сорности при редактировании пула"""
    await edit_pull_numeric_field(message, state, "impurity", "Сорность", 0, 100)


@dp.message_handler(state=EditPullStatesGroup.edit_weed)
async def edit_pull_weed(message: types.Message, state: FSMContext):
    """Обработка ввода засорённости при редактировании пула"""
    await edit_pull_numeric_field(message, state, "weed", "Засорённость", 0, 100)


async def edit_pull_numeric_field(
    message: types.Message,
    state: FSMContext,
    field: str,
    field_name: str,
    min_val: float = 0,
    max_val: float = None,
):
    """Универсальная функция для редактирования числовых полей пула"""
    try:
        new_value = float(message.text.strip().replace(",", "."))

        if new_value < min_val:
            await message.answer(f"❌ Значение должно быть не менее {min_val}")
            return

        if max_val is not None and new_value > max_val:
            await message.answer(f"❌ Значение должно быть не более {max_val}")
            return

        data = await state.get_data()
        pull_id = data.get("editing_pull_id")

            await message.answer("❌ Пул не найден")
            await state.finish()
            return


        if not pull:
            await message.answer("❌ Пул не найден")
            await state.finish()
            return

        old_value = pull.get(field, 0)
        pull[field] = new_value

        save_pulls_to_pickle()

        if gs and gs.spreadsheet:
            gs.update_pull_in_sheets(pull)

        await state.finish()

        keyboard = get_role_keyboard("exporter")
        await message.answer(
            f"✅ <b>{field_name} изменена!</b>\n\n"
            f"Пул #{pull_id}\n"
            f"Было: {old_value}\n"
            f"Стало: {new_value}",
            reply_markup=keyboard,
            parse_mode="HTML",
        )

    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректное число")


# ==================== ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ОТЛАДКИ ====================
# ⚠️ ВНИМАНИЕ: Эта функция должна быть ВЫШЕ всех обработчиков!


async def debug_pull_deletion(pullid: int):
    """Отладочная функция для проверки удаления пула"""
    logging.info(f"=== DEBUG: Проверка удаления пула {pullid} ===")

    # Проверка наличия в памяти
    in_memory = pullid in pulls
    logging.info(f"В памяти (pulls dict): {in_memory}")

    # Проверка наличия в pickle
    try:
            saved_pulls = pickle.load(f)
            in_pickle = pullid in saved_pulls.get("pulls", {})
            logging.info(f"В pickle файле: {in_pickle}")
    except Exception as e:
        logging.error(f"Ошибка чтения pickle: {e}")

    # Проверка наличия в Google Sheets
    if gs and gs.spreadsheet:
        try:
            worksheet = gs.spreadsheet.worksheet("Pulls")
            all_values = worksheet.get_all_values()

            found_in_sheets = False
            for row in all_values[1:]:
                if row and len(row) > 0:
                    try:
                        if int(row[0]) == pullid:
                            found_in_sheets = True
                            break
                    except (ValueError, IndexError):
                        continue

            logging.info(f"В Google Sheets: {found_in_sheets}")
        except Exception as e:
            logging.error(f"Ошибка проверки Google Sheets: {e}")

    logging.info("=== DEBUG: Завершено ===")


async def notify_logistic_pull_closed(pullid: int):
    """Уведомить логистов о закрытии пула"""
    try:
            logging.error(f"Пул {pullid} не найден для уведомления логистов")
            return


        # Формируем сообщение
        message = (
            f"🔔 <b>Пул #{pullid} закрыт и готов к логистике</b>\n\n"
            f"🌾 <b>Культура:</b> {pull.get('culture', 'Н/Д')}\n"
            f"💰 <b>Цена FOB:</b> ₽{pull.get('price', 0):,.0f}/тонна\n"
            f"🚢 <b>Порт:</b> {pull.get('port', 'Н/Д')}\n\n"
        )

        # Отправляем уведомления всем логистам
        for logistic_id in logistics:
            try:
                await bot.send_message(logistic_id, message, parse_mode="HTML")
                logging.info(
                    f"Уведомление о закрытии пула {pullid} отправлено логисту {logistic_id}"
                )
            except Exception as e:
                logging.error(f"Не удалось уведомить логиста {logistic_id}: {e}")

        logging.info(
            f"Уведомления о закрытии пула {pullid} отправлены {len(logistics)} логистам"
        )

    except Exception as e:
        logging.error(f"Ошибка в notify_logistic_pull_closed: {e}")


# ==================== НАЧАЛО УДАЛЕНИЯ ПУЛА ====================
@dp.callback_query_handler(lambda c: c.data.startswith("deletepull_"), state="*")
async def deletepullstart_callback(callback: types.CallbackQuery, state: FSMContext):
    """Запрос подтверждения удаления пула"""
    try:
        pullid = parse_callback_id(callback.data)
        await callback.answer("❌ Ошибка: некорректный ID пула", show_alert=True)
        return
        return


    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return

        await callback.answer(
            "❌ Только создатель пула может его удалить", show_alert=True
        )
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, удалить", callback_data=f"confirmdeletepull_{pullid}"
        ),
        InlineKeyboardButton("❌ Отмена", callback_data="canceldeletepull"),
    )

    await callback.message.edit_text(
        f"❓ Вы уверены, что хотите удалить пул №{pullid}?\n\n"
        f"🌾 <b>Культура:</b> {pull.get('culture', 'Н/Д')}\n"
        f"📦 <b>Объём:</b> {pull.get('target_volume', 0)} тонн\n"
        f"💰 <b>Цена FOB:</b> ₽{pull.get('price', 0):,.0f}/тонна\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )

    await callback.answer()


# ==================== ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ ====================
@dp.callback_query_handler(lambda c: c.data.startswith("confirmdeletepull_"), state="*")
async def deletepullconfirmed_callback(
    callback: types.CallbackQuery, state: FSMContext
):
    """Финальное удаление пула после подтверждения"""
    await state.finish()

    try:
        pullid = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка: некорректный ID пула", show_alert=True)
        logging.error(f"Ошибка парсинга при подтверждении: {e}, data: {callback.data}")
        return
        return


    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return

        await callback.answer(
            "❌ У вас нет прав на удаление этого пула", show_alert=True
        )
        return

    # Сохраняем данные для логирования
    pull_culture = pull.get("culture", "Н/Д")
    pull_volume = pull.get("target_volume", 0)
    pull_price = pull.get("price", 0)

    # ========== УДАЛЕНИЕ СВЯЗАННЫХ ДАННЫХ ==========

    # 1. Удаляем все матчи
    matches_to_delete = [
        mid
        for mid, m in matches.items()
    ]
    for mid in matches_to_delete:
        del matches[mid]

    if pullid in pullparticipants:
        del pullparticipants[pullid]
    if str(pullid) in pullparticipants:
        del pullparticipants[str(pullid)]

    # 3. ✅ ИСПРАВЛЕНО: Удаляем сам пул из pulls['pulls']
    if pullid in all_pulls:
        del all_pulls[pullid]
    if str(pullid) in all_pulls:
        del all_pulls[str(pullid)]

    # 4. Сохраняем изменения
    save_pulls_to_pickle()

    # ========== СИНХРОНИЗАЦИЯ С GOOGLE SHEETS ==========
    if gs and gs.spreadsheet:
        try:
            worksheet = gs.spreadsheet.worksheet("Pulls")
            all_values = worksheet.get_all_values()

            rows_to_delete = []
            for i, row in enumerate(all_values[1:], start=2):
                if row and len(row) > 0:
                    try:
                        if int(row[0]) == pullid:
                            rows_to_delete.append(i)
                    except (ValueError, IndexError):
                        continue

            for row_num in reversed(rows_to_delete):
                worksheet.delete_rows(row_num)

            logging.info(f"Пул {pullid} удалён из Google Sheets")
        except Exception as e:
            logging.error(f"Ошибка удаления из Google Sheets: {e}")

    # ========== УВЕДОМЛЕНИЯ ==========

    # Уведомляем участников
    farmer_ids = pull.get("farmer_ids", [])
    for farmer_id in farmer_ids:
        try:
            await bot.send_message(
                farmer_id,
                f"<b>🗑 Пул №{pullid} был удалён</b>\n\n"
                f"🌾 {pull_culture}\n"
                f"📦 {pull_volume:.1f} т\n"
                f"💰 ₽{pull_price:,.0f}/т\n\n"
                parse_mode="HTML",
            )
        except Exception as e:
            logging.warning(f"⚠️ Уведомление фермеру {farmer_id}: {e}")

    # ✅ ДОБАВЛЕНА ОДНА СТРОКА - создание клавиатуры
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton("⬅️ Назад", callback_data="back_to_pulls")
    )

    await callback.message.edit_text(
        f"🌾 Культура: {pull_culture}\n"
        f"📦 Объём: {pull_volume:.1f} т\n"
        f"💰 Цена: ₽{pull_price:,.0f}/т\n"
        f"🆔 ID: {pullid}\n\n"
        f"🔔 Участников уведомлено: {len(farmer_ids)}\n"
        f"🗑️ Матчей удалено: {len(matches_to_delete)}",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    logging.info(
    )

    await callback.answer("✅ Пул удалён")


# ==================== ОТМЕНА УДАЛЕНИЯ ====================
@dp.callback_query_handler(lambda c: c.data == "canceldeletepull", state="*")
async def canceldeletepull_callback(callback: types.CallbackQuery, state: FSMContext):
    """✅ Отмена процесса удаления пула - ИСПРАВЛЕНО"""

    # ✅ Получаем pull_id из state
    user_data = await state.get_data()
    pull_id = user_data.get("delete_pull_id")

    # ✅ Очищаем state
    await state.finish()

    # ✅ Если pull_id есть - возвращаемся к деталям пула
    if pull_id:
        all_pulls = pulls.get("pulls", {})

        if pull:
            # Показываем детали пула
            keyboard = InlineKeyboardMarkup(row_width=2)
            keyboard.add(
                InlineKeyboardButton(
                    "✏️ Редактировать", callback_data=f"editpull_{pull_id}"
                ),
                InlineKeyboardButton(
                    "🗑 Удалить", callback_data=f"deletepull_{pull_id}"
                ),
            )
            keyboard.add(
                InlineKeyboardButton("◀️ Назад", callback_data="back_to_my_pulls")
            )

            text = f"""📦 <b>Пул #{pull_id}</b>

❌ <i>Удаление отменено</i>

🌾 <b>Культура:</b> {pull.get('culture', '?')}
📦 <b>Объём:</b> {pull.get('current_volume', 0):.0f}/{pull.get('target_volume', 0):.0f} т
💰 <b>Цена FOB:</b> ₽{pull.get('price', 0):,.0f}/т
"""

            await callback.message.edit_text(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
        else:
            await callback.message.edit_text("❌ Пул не найден")
    else:
        # Если нет pull_id - просто показываем сообщение
        await callback.message.edit_text("❌ Удаление отменено")

    await callback.answer()


    try:
        return

        return

    # Проверяем право на закрытие
        return



    )



    try:



@dp.callback_query_handler(lambda c: c.data == "cancel_delete_pull", state="*")
async def cancel_delete_pull(callback: types.CallbackQuery):
    """Отмена удаления пула"""
    await callback.message.edit_text("❌ Удаление отменено")
    await callback.answer()


# ================================
# ОБРАБОТЧИК ЗАКРЫТИЯ ПУЛЛА
# ================================
@dp.callback_query_handler(lambda c: c.data == "get_partner_contacts", state="*")
async def get_partner_contacts_handler(callback: types.CallbackQuery):
    """Получение контактов партнёра по сделке"""
    user_id = callback.from_user.id
    partner_info = None

    # Ищем сделку, где пользователь участвует
    for deal_id, deal in deals.items():
            # Логист - получаем контакты экспортёра
            exporter_id = deal.get("exporter_id")
            if exporter:
                partner_info = "📦 <b>Контакты экспортёра:</b>\n\n"
                partner_info += f"📝 {exporter.get('name', 'Неизвестно')}\n"
                partner_info += f"📱 {exporter.get('phone', 'Не указан')}\n"
                partner_info += f"📧 {exporter.get('email', 'Не указан')}\n"
                partner_info += f"📍 {exporter.get('region', 'Не указан')}\n"
                break

            # Экспедитор - получаем контакты экспортёра
            exporter_id = deal.get("exporter_id")
            if exporter:
                partner_info = "📦 <b>Контакты экспортёра:</b>\n\n"
                partner_info += f"📝 {exporter.get('name', 'Неизвестно')}\n"
                partner_info += f"📱 {exporter.get('phone', 'Не указан')}\n"
                partner_info += f"📧 {exporter.get('email', 'Не указан')}\n"
                partner_info += f"📍 {exporter.get('region', 'Не указан')}\n"
                break

    if not partner_info:
        await callback.answer("🤷‍♂️ Контакты не найдены", show_alert=True)
        return

    await callback.message.answer(partner_info, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("complete_deal:"), state="*")
async def complete_deal(callback: types.CallbackQuery):
    """Завершение сделки"""
    deal_id = parse_callback_id(callback.data)

        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id
        await callback.answer(
        )
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, завершить", callback_data=f"confirm_complete_deal:{deal_id}"
        ),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_complete_deal"),
    )

    await callback.message.edit_text(
        f"Вы уверены, что хотите завершить сделку #{deal_id}?\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_complete_deal:"), state="*"
)
async def confirm_complete_deal(callback: types.CallbackQuery):
    """Подтверждение завершения сделки"""
    deal_id = parse_callback_id(callback.data)

        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return

    deal["status"] = "completed"
    deal["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    await notify_deal_participants(deal_id, "✅ Сделка завершена!")

    await callback.message.edit_text(
        f"🎉 <b>Сделка #{deal_id} завершена!</b>\n\n"
        parse_mode="HTML",
    )
    await callback.answer("✅ Сделка завершена")


@dp.callback_query_handler(lambda c: c.data.startswith("cancel_deal:"), state="*")
async def cancel_deal(callback: types.CallbackQuery):
    """Отмена сделки"""
    deal_id = parse_callback_id(callback.data)

        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id
        await callback.answer(
            "❌ Только экспортёр может отменить сделку", show_alert=True
        )
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, отменить", callback_data=f"confirm_cancel_deal:{deal_id}"
        ),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_cancel_deal"),
    )

    await callback.message.edit_text(
        f"Вы уверены, что хотите отменить сделку #{deal_id}?\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_cancel_deal:"), state="*"
)
async def confirm_cancel_deal(callback: types.CallbackQuery):
    """Подтверждение отмены сделки"""
    deal_id = parse_callback_id(callback.data)

        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return

    deal["status"] = "cancelled"

    await notify_deal_participants(deal_id, "❌ Сделка отменена")

    await callback.message.edit_text(
        f"❌ <b>Сделка #{deal_id} отменена!</b>\n\n"
        parse_mode="HTML",
    )
    await callback.answer("✅ Сделка отменена")


@dp.callback_query_handler(
    lambda c: c.data in ["cancel_complete_deal", "cancel_cancel_deal"], state="*"
)
async def cancel_deal_action(callback: types.CallbackQuery):
    """Отмена действия со сделкой"""
    await callback.message.edit_text("❌ Действие отменено")
    await callback.answer()


async def notify_deal_participants(deal_id: int, message: str):
    """Уведомление всех участников сделки"""
    if not deal:
        return

    participants = []
    if deal.get("exporter_id"):
        participants.append(deal["exporter_id"])
    if deal.get("farmer_ids"):
        participants.extend(deal["farmer_ids"])
    if deal.get("logistic_id"):
        participants.append(deal["logistic_id"])
    if deal.get("expeditor_id"):
        participants.append(deal["expeditor_id"])
    for user_id in participants:
        try:
            await bot.send_message(
                user_id,
                f"📋 <b>Уведомление по сделке #{deal_id}</b>\n\n{message}",
                parse_mode="HTML",
            )
            await asyncio.sleep(0.1)  # Задержка между отправками
        except Exception as e:
            logging.error(f"❌ Ошибка уведомления пользователя {user_id}: {e}")


@dp.callback_query_handler(lambda c: c.data.startswith("logistics:"), state="*")
async def deal_logistics(callback: types.CallbackQuery):
    """Управление логистикой для сделки"""
    deal_id = parse_callback_id(callback.data)

        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return

    text = f"🚚 <b>Логистика сделки #{deal_id}</b>\n\n"

    if deal.get("logistic_id"):
        if logistic:
            text += "✅ <b>Логист назначен:</b>\n"
            text += f"👤 {logistic.get('name', 'Неизвестно')}\n"
            text += f"🏢 {logistic.get('company_details', 'Не указана')}\n"
            text += f"📋 ИНН: <code>{logistic.get('inn', 'Не указан')}</code>\n"
            text += f"📋 ОГРН: <code>{logistic.get('ogrn', 'Не указан')}</code>\n"
            text += f"📱 {logistic.get('phone', 'Не указан')}\n"
            text += f"📧 {logistic.get('email', 'Не указан')}\n"
        else:
            text += "❌ Логист не найден в системе\n"
    else:
        text += "🤷‍♂️ <b>Логист не назначен</b>\n\n"
        text += "Для назначения логиста создайте заявку на логистику.\n"

    if deal.get("expeditor_id"):
        if expeditor:
            text += "\n✅ <b>Экспедитор назначен:</b>\n"
            text += f"👤 {expeditor.get('name', 'Неизвестно')}\n"
            text += f"🏢 {expeditor.get('company_details', 'Не указана')}\n"
            text += f"📋 ИНН: <code>{expeditor.get('inn', 'Не указан')}</code>\n"
            text += f"📋 ОГРН: <code>{expeditor.get('ogrn', 'Не указан')}</code>\n"
            text += f"📱 {expeditor.get('phone', 'Не указан')}\n"
            text += f"📧 {expeditor.get('email', 'Не указан')}\n"
        else:
            text += "\n❌ Экспедитор не найден в системе\n"
    else:
        text += "\n🤷‍♂️ <b>Экспедитор не назначен</b>\n\n"
        text += "Для назначения экспедитора создайте заявку на оформление документов.\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("◀️ Назад к сделке", callback_data=f"view_deal:{deal_id}")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("pullparticipants:"), state="*")
async def show_pullparticipants(callback: types.CallbackQuery):
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

        await callback.answer("❌ Пул не найден", show_alert=True)
        return


    if not participants:
        text += "🤷‍♂️ У пула пока нет участников"
    else:
        total_participant_volume = 0
        for i, participant in enumerate(participants, 1):
            farmer_id = participant.get("farmer_id")
            batch_id = participant.get("batch_id")
            volume = participant.get("volume", 0)
            total_participant_volume += volume



        fill_percentage = (
        )

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("◀️ Назад к пулу", callback_data=f"view_pull:{pull_id}")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("pull_logistics:"), state="*")
async def pull_logistics_menu(callback: types.CallbackQuery):
    """Меню логистики для пула"""
    try:
        pull_id = parse_callback_id(callback.data)
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

        await callback.answer("❌ Пул не найден", show_alert=True)
        return


    text = f"🚚 <b>Логистика пула #{pull_id}</b>\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
        InlineKeyboardButton(
            "👀 Активные заявки", callback_data=f"view_shipping_requests:{pull_id}"
        ),
        InlineKeyboardButton(
            "📞 Контакты логистов", callback_data="view_logistics_contacts"
        ),
    )
    keyboard.add(
        InlineKeyboardButton("◀️ Назад к пулу", callback_data=f"view_pull:{pull_id}")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("create_shipping:"), state="*")
async def create_shipping_from_pull(callback: types.CallbackQuery, state: FSMContext):
    """Создание заявки на логистику из пула"""
    try:
        pull_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    await state.update_data(pull_id=pull_id)

    await callback.message.edit_text(
        f"🚚 <b>Заявка на логистику для пула #{pull_id}</b>\n\n"
        "<b>Шаг 1 из 5</b>\n\n"
        "Введите пункт отправки (город/регион):",
        parse_mode="HTML",
    )

    await ShippingRequestStatesGroup.route_from.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "view_logistics_contacts", state="*")
async def view_logistics_contacts(callback: types.CallbackQuery):
    """Показать контакты логистов с реквизитами"""


    if not logistics_users:
        await callback.answer("🤷‍♂️ В системе пока нет логистов", show_alert=True)
        return

    text = "<b>🚚 ЛОГИСТЫ НА ПЛАТФОРМЕ</b>\n\n"

    for i, logistic in enumerate(logistics_users[:10], 1):
        name = logistic.get("name", "Неизвестно")
        phone = logistic.get("phone", "Не указан")
        email = logistic.get("email", "Не указан")
        region = logistic.get("region", "Не указан")

        inn = logistic.get("inn", "Не указан")
        ogrn = logistic.get("ogrn", "Не указан")
        company = logistic.get("company_details", "Не указана")

        text += f"{i}. 👤 <b>{name}</b>\n"
        text += f"   🏢 {company}\n"
        text += f"   📋 ИНН: <code>{inn}</code>\n"
        text += f"   📋 ОГРН: <code>{ogrn}</code>\n"
        text += f"   📱 <code>{phone}</code>\n"

        if email != "Не указан":
            text += f"   📧 <code>{email}</code>\n"

        text += f"   📍 {region}\n\n"

    if len(logistics_users) > 10:
        text += f"\n<i>ℹ️ Ещё {len(logistics_users) - 10} логистов на платформе</i>"

    text += "\n\n💡 <b>Свяжитесь с логистами для обсуждения условий перевозки.</b>"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main"))

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()


@dp.message_handler(
    lambda m: m.text in ["🚚 Моя карточка", "🚛 Моя карточка"], state="*"
)
async def show_logistics_card(message: types.Message):
    """Показать карточку логиста/экспедитора"""
    user_id = message.from_user.id

        await message.answer(
            "❌ Пользователь не найден. Пройдите регистрацию командой /start"
        )
        return

    role = user.get("role")

        await message.answer("❌ Эта функция доступна только логистам и экспедиторам")
        return

    # Формируем карточку

    text = f"{role_emoji} <b>Моя карточка ({role_name})</b>\n\n"
    text += f"👤 Имя: {user.get('name', 'Не указано')}\n"
    text += f"📞 Телефон: <code>{user.get('phone', 'Не указан')}</code>\n"
    text += f"📧 Email: {user.get('email', 'Не указан')}\n"
    text += f"📍 Регион: {user.get('region', 'Не указан')}\n\n"

    if user.get("inn"):
        text += f"🏢 ИНН: <code>{user['inn']}</code>\n"

    if user.get("company_details"):
        text += f"📋 О компании:\n{user['company_details'][:300]}\n\n"

    # Статистика
            ]
        active_requests = [
        ]
        text += "📊 <b>Статистика:</b>\n"
        text += f"   • Активных: {len(active_requests)}\n"
    else:  # expeditor
        # Статистика экспедитора
        text += "📊 <b>Статистика:</b>\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("✏️ Редактировать профиль", callback_data="edit_profile")
    )

    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)


@dp.message_handler(lambda m: m.text == "🚚 Активные заявки", state="*")
async def show_active_requests(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

        await message.answer(
            "❌ Пользователь не найден. Пройдите регистрацию командой /start"
        )
        return

    role = user.get("role")

        all_requests = []

        for req_id, req in shipping_requests.items():
            all_requests.append(
                {
                    "source": "exporter",
                    "culture": req.get("culture", "—"),
                }
            )

        for req_id, req in farmer_logistics_requests.items():
            all_requests.append(
                {
                    "source": "farmer",
                    "culture": req.get("culture", "—"),
                }
            )



        text = (
        )
        for req in all_requests:
                emoji = "🚛"
            else:
                keyboard.add(
                    InlineKeyboardButton(
                    )
                )
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

        title = "📋 <b>Мои заявки на доставку</b>"

        user_requests = []
    else:
        await message.answer("❌ Эта функция недоступна для вашей роли")
        return

    if not user_requests:
        return

    if show_buttons:
        keyboard = InlineKeyboardMarkup(row_width=1)
        for item in user_requests[:20]:
            req = item["req"]
            source = item["source"]

            culture = req.get("culture", "N/A")
            volume = req.get("volume", 0) or 0


                keyboard.add(
                    InlineKeyboardButton(
                        btn_text, callback_data=f"view_request:{source}:{req_id}"
                    )
                )

        await message.answer(
            f"{title}\n\n"
            f"📋 Всего заявок: <b>{len(user_requests)}</b>\n"
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    else:
        text = f"{title}\n\n"
        for idx, item in enumerate(user_requests[:10], 1):
            req = item["req"]
            volume = req.get("volume", 0) or 0
            route_from = req.get("route_from", "Не указано")
            route_to = req.get("route_to", "Не указано")
            culture = req.get("culture", "N/A")
            status = req.get("status", "unknown")

            text += f"{idx}. 📦 Заявка #{req_id}\n"
            text += f"   • Культура: {culture}\n"
            text += f"   • Объём: {volume:.0f} т\n"
            text += f"   • Маршрут: {route_from} → {route_to}\n"
            text += f"   • Статус: {status}\n\n"

        if len(user_requests) > 10:
            text += f"<i>... и ещё {len(user_requests) - 10} заявок</i>\n\n"

        await message.answer(text, parse_mode="HTML")


    await state.finish()

        return

        )
        return

    text = (
    )



        await state.finish()


    await message.answer(
    )



    try:
            raise ValueError
    except ValueError:
        return

    data = await state.get_data()

        await message.answer(
                    parse_mode="HTML",
                )
        await message.answer(
            parse_mode="HTML",
        )




    try:
            raise ValueError
        return

    else:
        return
        await callback.answer(
        )
        return

            else:


    )
            InlineKeyboardButton(
            )
        InlineKeyboardButton(
                )
        )
    )

    await callback.answer()


    try:

        return

    user_id = callback.from_user.id
    else:
        return


            InlineKeyboardButton(
        InlineKeyboardButton(
        InlineKeyboardButton(
                )
        )
    )

    await callback.answer()


    await state.finish()

    try:
        return

    user_id = callback.from_user.id

    ):
        return


            )


    )


        keyboard.add(
            InlineKeyboardButton(
        )
        keyboard.add(
            InlineKeyboardButton(
            )
        keyboard.add(
            InlineKeyboardButton(
            )
        )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")




    try:
        return

    user_id = message.from_user.id
        await state.finish()

    try:
            raise ValueError
    except ValueError:
        return

    data = await state.get_data()

        await state.finish()
        await state.finish()

        await state.finish()


    await state.finish()

    user_id = callback.from_user.id

    card = expeditor_cards.get(user_id)

    if not card:
        return


    text = (
    )

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "🗑 Удалить карточку", callback_data="delete_expeditor_card"
        ),
    )



# ═══════════════════════════════════════════════════════════════════════════
# УДАЛЕНИЕ КАРТОЧКИ
# ═══════════════════════════════════════════════════════════════════════════
@dp.callback_query_handler(lambda c: c.data == "delete_expeditor_card", state="*")
async def delete_expeditor_card(callback: types.CallbackQuery):
    """Удаление карточки экспедитора"""
    user_id = callback.from_user.id

    if user_id in expeditor_cards:
        del expeditor_cards[user_id]
        await callback.answer("✅ Карточка удалена", show_alert=True)
        await callback.message.edit_text(
            "🗑 <b>Карточка удалена</b>\n\n"
            "Вы можете создать новую карточку в любой момент.",
            parse_mode="HTML",
        )
        logging.info(f"🗑 Экспедитор {user_id} удалил свою карточку")
    else:
        await callback.answer("❌ Карточка не найдена", show_alert=True)


    await state.finish()
        await callback.message.edit_text(
            parse_mode="HTML",
        )


@dp.callback_query_handler(lambda c: c.data.startswith("view_request:"), state="*")
async def view_request_details(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    parts = callback.data.split(":")

    if len(parts) == 2:
        source = "exporter"
        try:
            request_id = int(parts[1])
        except ValueError:
            return
    elif len(parts) == 3:
        source = parts[1]
        try:
            request_id = int(parts[2])
        except ValueError:
            return
    else:
        return

    if source == "exporter":
            return
        pull_id = request.get("pull_id")

        price_fob = pull.get("price", 0)
        price_rub = int(price_fob * 95) if price_fob else 0

        text = f"""🚚 <b>Заявка на доставку #{request_id}</b>



<b>Информация о грузе:</b>
📦 Объём: {request.get('volume', 0):.0f} т
💰 Цена FOB: {price_rub:,} ₽/т
🚢 Порт: {pull.get('port', '—')}



<b>Маршрут:</b>



<b>👤 Заказчик (экспортёр):</b>
👤 Имя: {user.get('name', 'Не указано')}
📞 Телефон: <code>{user.get('phone', 'Не указан')}</code>
📧 Email: {user.get('email', 'Не указан')}
📍 Регион: {user.get('region', 'Не указан')}
💬 Username: @{user.get('username', 'нет')}



📅 Создана: {request.get('created_at', '—')}
"""

    # ============ ЗАЯВКА ОТ ФЕРМЕРА ============
    else:
            return


        text = f"""🌾 <b>Логистическая заявка фермера #{request_id}</b>



<b>Информация о грузе:</b>
🌾 Культура: {request.get('culture', '—')}
📦 Объём: {request.get('volume', 0):.0f} т
💰 Цена: {request.get('price_per_ton', 0):,} ₽/т
💵 Итого: {request.get('total_sum', 0):,} ₽



<b>Маршрут:</b>
🚚 Транспорт: {request.get('transport_type', '—')}



<b>Стоимость доставки:</b>
💳 Ожидаемая цена: {request.get('desired_price', 0):,} ₽/т



<b>👤 Заказчик (фермер):</b>
👤 Имя: {user.get('name', 'Не указано')}
📞 Телефон: <code>{user.get('phone', 'Не указан')}</code>
📧 Email: {user.get('email', 'Не указан')}
📍 Регион: {user.get('region', 'Не указан')}
💬 Username: @{user.get('username', 'нет')}



📅 Создана: {request.get('created_at', '—')}
"""

    keyboard = InlineKeyboardMarkup(row_width=1)

        keyboard.add(
        )

    logist_id = callback.from_user.id
            offer
            for offer in logistic_offers.values()
        ]


            )
            keyboard.add(
                InlineKeyboardButton(
                    "💰 Сделать предложение",
                    callback_data=f"make_offer:{source}:{request_id}",
            )

    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error editing message for request {request_id}: {e}")
        try:
            await callback.message.answer(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
        except Exception as e2:
            logging.error(f"Error sending new message: {e2}")


# ═══════════════════════════════════════════════════════════════════════════
# ЛОГИСТ: БАЗА ЭКСПЕДИТОРОВ
# ═══════════════════════════════════════════════════════════════════════════
@dp.message_handler(lambda m: m.text == "🚛 База экспедиторов", state="*")
async def logist_view_expeditors(message: types.Message, state: FSMContext):
    """Логист просматривает базу экспедиторов"""
    await state.finish()

    user_id = message.from_user.id

        await message.answer("❌ Эта функция доступна только логистам")
        return

    if not expeditor_cards:
        await message.answer(
            "🚛 <b>База экспедиторов</b>\n\n"
            "📋 В базе пока нет экспедиторов.\n\n"
            "Как только экспедиторы создадут карточки, вы сможете их увидеть здесь.",
            parse_mode="HTML",
        )
        return

    # Формируем список экспедиторов
    keyboard = InlineKeyboardMarkup(row_width=1)

    text = (
        "🚛 <b>База экспедиторов</b>\n\n"
        f"📋 Всего экспедиторов: <b>{len(expeditor_cards)}</b>\n\n"
        "Выберите экспедитора для просмотра карточки:"
    )

    for exp_id, card in list(expeditor_cards.items())[:20]:  # Максимум 20
        # ИСПРАВЛЕНО: transport_type вместо vehicle_type
        transport = card.get("transport_type", "Не указан")
        capacity = card.get("capacity", 0)
        regions = card.get("regions", [])
        price = card.get("price_per_km", 0)

        # Получаем эмодзи транспорта
        emoji = TRANSPORT_TYPES.get(transport, "🚛")

        # Форматируем регионы (список → строка)
        if isinstance(regions, list):
            regions_text = ", ".join(regions) if regions else "Регион не указан"
        else:
            regions_text = str(regions)

        # Обрезаем если очень длинное (максимум 30 символов)
        if len(regions_text) > 30:
            regions_text = regions_text[:27] + "..."

        btn_text = f"{emoji} {transport} {capacity}т | {regions_text} | {price}₽/км"
        keyboard.add(
            InlineKeyboardButton(btn_text, callback_data=f"view_expeditor:{exp_id}")
        )

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════════════
# ЭКСПЕДИТОР: ДОСТУПНЫЕ ЗАЯВКИ
# ═══════════════════════════════════════════════════════════════════════════


@dp.message_handler(lambda m: m.text == "🚚 Доступные заявки", state="*")
async def expeditor_view_available_requests(message: types.Message, state: FSMContext):
    """Экспедитор просматривает доступные заявки от логистов"""
    await state.finish()

    user_id = message.from_user.id

        await message.answer("❌ Эта функция доступна только экспедиторам")
        return


    if not available_requests:
        await message.answer(
            "🚚 <b>Доступные заявки</b>\n\n"
            "📋 Нет доступных заявок на доставку.\n\n"
            "Как только логисты создадут заявки, они появятся здесь.",
            parse_mode="HTML",
        )
        return

    # Формируем список заявок
    keyboard = InlineKeyboardMarkup(row_width=1)

    text = (
        "🚚 <b>Доступные заявки на доставку</b>\n\n"
        f"📋 Всего заявок: <b>{len(available_requests)}</b>\n\n"
        "Выберите заявку для просмотра деталей:"
    )

        culture = req.get("culture", "N/A")
        volume = req.get("volume", 0) or 0

        btn_text = (
                )
            keyboard.add(
                InlineKeyboardButton(btn_text, callback_data=f"exp_view_req:{req_id}")
            )

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query_handler(lambda c: c.data.startswith("exp_view_req:"), state="*")
async def expeditor_view_request_details(
    callback: types.CallbackQuery, state: FSMContext
):
    """Просмотр деталей заявки экспедитором"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    pull_id = request.get("pull_id")
    exporter_id = request.get("exporter_id")


    # Конвертируем цену
    price_fob = pull.get("price", 0)
    price_rub = int(price_fob * 95) if price_fob else 0

    # Формируем детали заявки
    text = (
        f"🚚 <b>Заявка на доставку #{request_id}</b>\n\n"
        f"🌾 Культура: {request.get('culture', '—')}\n"
        f"📦 Объём: {request.get('volume', 0):.0f} т\n"
        f"💰 Цена FOB: {price_rub:,} ₽/т\n"
        f"🚢 Порт: {pull.get('port', '—')}\n\n"
        f"Компания: {exporter.get('company_name', 'N/A')}\n"
        f"Телефон: <code>{exporter.get('phone', 'Не указан')}</code>\n"
        f"Email: {exporter.get('email', 'Не указан')}\n\n"
    )

    if logist_id:
        text += (
            f"Имя: {logist.get('name', 'N/A')}\n"
            f"Телефон: <code>{logist.get('phone', 'Не указан')}</code>\n"
            f"Email: {logist.get('email', 'Не указан')}\n\n"
        )

    text += f"📅 Создана: {request.get('created_at', '—')}"

    keyboard = InlineKeyboardMarkup(row_width=1)

    # Кнопки связи
        keyboard.add(
            InlineKeyboardButton(
                "📞 Связаться с экспортёром", url=f"tg://user?id={exporter_id}"
            )
        )

    if logist_id:
        keyboard.add(
            InlineKeyboardButton(
                "📞 Связаться с логистом", url=f"tg://user?id={logist_id}"
            )
        )

        keyboard.add(
            InlineKeyboardButton(
            )
        )
    keyboard.add(
        InlineKeyboardButton("◀️ К списку заявок", callback_data="back_to_exp_requests")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("exp_accept:"), state="*")
async def expeditor_accept_request(callback: types.CallbackQuery, state: FSMContext):
    """Экспедитор принимает заявку"""
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    expeditor_id = callback.from_user.id

    # Обновляем статус заявки
    request["status"] = "in_progress"
    request["expeditor_id"] = expeditor_id

    # Уведомляем экспедитора
    await callback.message.edit_text(
        f"✅ <b>Заявка #{request_id} принята!</b>\n\n"
        parse_mode="HTML",
    )

    # Уведомляем экспортёра
    exporter_id = request.get("exporter_id")
    if exporter_id:
        try:
            await bot.send_message(
                exporter_id,
                f"✅ <b>Заявка #{request_id} принята экспедитором!</b>\n\n"
                f"🚛 Экспедитор: {expeditor.get('name', 'N/A')}\n"
                f"📞 Телефон: <code>{expeditor.get('phone', 'Не указан')}</code>\n"
                f"📧 Email: {expeditor.get('email', 'Не указан')}\n\n"
                parse_mode="HTML",
            )

    # Уведомляем логиста (если есть)
    if logist_id:
        try:
            await bot.send_message(
                logist_id,
                f"✅ <b>Заявка #{request_id} принята экспедитором!</b>\n\n"
                f"🚛 Экспедитор: {expeditor.get('name', 'N/A')}\n"
                parse_mode="HTML",
            )

    await callback.answer("✅ Заявка принята!", show_alert=True)
    logging.info(f"✅ Экспедитор {expeditor_id} принял заявку {request_id}")


@dp.callback_query_handler(lambda c: c.data == "back_to_exp_requests", state="*")
async def back_to_exp_requests(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку доступных заявок"""
    await state.finish()


    if not available_requests:
        await callback.message.edit_text(
            "🚚 <b>Доступные заявки</b>\n\n" "📋 Нет доступных заявок на доставку.",
            parse_mode="HTML",
        )
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(row_width=1)

    text = (
        "🚚 <b>Доступные заявки на доставку</b>\n\n"
        f"📋 Всего заявок: <b>{len(available_requests)}</b>\n\n"
        "Выберите заявку для просмотра деталей:"
    )

        culture = req.get("culture", "N/A")
        volume = req.get("volume", 0) or 0

        btn_text = (
        )
            keyboard.add(
                InlineKeyboardButton(btn_text, callback_data=f"exp_view_req:{req_id}")
            )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.message_handler(
    lambda m: m.text == "🚛 Мои доставки"
    state="*",
)
async def my_deliveries_handler(message: types.Message, state: FSMContext):
    await state.finish()

    user_id = message.from_user.id

        await message.answer("❌ Пользователь не найден. Используйте /start")
        return


    # ═══════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════


            await message.answer(
                "🚛 <b>Мои доставки</b>\n\n"
                "У вас пока нет доставок.\n"
                "Откликайтесь на заявки в разделе «🚚 Активные заявки»!\n\n"
                "📊 <b>Статистика:</b>\n"
                "• В процессе: 0\n"
                "• Завершено: 0\n"
                "• Заработано: 0 ₽",
                parse_mode="HTML",
            )
            return


        text += f"• В процессе: {len(in_progress)}\n"
        text += f"• Завершено: {len(completed)}\n"

        if in_progress:
            text += f"<b>🟢 В процессе ({len(in_progress)}):</b>\n\n"


                keyboard.add(
                    InlineKeyboardButton(
                    )
                )

            if len(in_progress) > 5:
                text += f"\n<i>... и ещё {len(in_progress) - 5} доставок</i>\n"

        if completed:
            text += f"\n<b>✅ Завершённые ({len(completed)}):</b>\n"

        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="refresh_deliveries")
        )

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

    # ═══════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════


        if not my_deliveries:
            await message.answer(
                "🚛 <b>Мои доставки</b>\n\n"
                "📋 У вас нет активных доставок.\n\n"
                'Примите заявку из раздела <b>"🚚 Доступные заявки"</b>.',
                parse_mode="HTML",
            )
            return

        keyboard = InlineKeyboardMarkup(row_width=1)

        text = (
            "🚛 <b>Мои активные доставки</b>\n\n"
            f"📋 Всего доставок: <b>{len(my_deliveries)}</b>\n\n"
            "Выберите доставку:"
        )


            keyboard.add(
            )

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

    else:
        await message.answer(
            "❌ Эта функция доступна только для логистов и экспедиторов"
        )


@dp.callback_query_handler(lambda c: c.data.startswith("exp_delivery:"), state="*")
async def expeditor_delivery_details(callback: types.CallbackQuery, state: FSMContext):
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Доставка не найдена", show_alert=True)
        return


    text = (
    )

    keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
            InlineKeyboardButton(
        InlineKeyboardButton(
            "◀️ К списку доставок", callback_data="back_to_exp_deliveries"
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("exp_complete:"), state="*")
async def expeditor_complete_delivery(callback: types.CallbackQuery, state: FSMContext):
    try:
        return

            await callback.answer("❌ Доставка не найдена", show_alert=True)
            return

        request["status"] = "completed"



    if success:
                parse_mode="HTML",
            )
            )

    await callback.answer()


# ═══════════════════════════════════════════════════════════════════════════
# ЭКСПЕДИТОР: ИСТОРИЯ ДОСТАВОК
# ═══════════════════════════════════════════════════════════════════════════
@dp.message_handler(lambda m: m.text == "✔️ История доставок", state="*")
async def expeditor_delivery_history(message: types.Message, state: FSMContext):
    """История завершённых доставок"""
    await state.finish()

    user_id = message.from_user.id

        await message.answer("❌ Эта функция доступна только экспедиторам")
        return


    if not completed:
        await message.answer(
            "✔️ <b>История доставок</b>\n\n" "📋 У вас пока нет завершённых доставок.",
            parse_mode="HTML",
        )
        return

    text = (
        f"📋 Всего завершено: <b>{len(completed)}</b>\n\n"
    )


        text += (
            f"   {culture} | {volume:.0f} т\n"
            f"   ✅ Завершена: {completed_at}\n\n"
        )

    await message.answer(text, parse_mode="HTML")


@dp.callback_query_handler(lambda c: c.data.startswith("view_expeditor:"), state="*")
async def logist_view_expeditor_card(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр карточки экспедитора логистом"""
    await state.finish()

    try:
        expeditor_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if expeditor_id not in expeditor_cards:
        await callback.answer("❌ Карточка не найдена", show_alert=True)
        return

    card = expeditor_cards[expeditor_id]

    # Увеличиваем счётчик просмотров
    card["views"] = card.get("views", 0) + 1

    # СНАЧАЛА ИЗВЛЕКАЕМ transport_type ИЗ card!
    transport_type = card.get("transport_type", "Не указан")

    # ТЕПЕРЬ ПОЛУЧАЕМ ЭМОДЗИ
    transport_emoji = TRANSPORT_TYPES.get(transport_type, "🚛")

    # Форматируем регионы
    regions = card.get("regions", [])
    regions_text = ", ".join(regions) if isinstance(regions, list) else str(regions)

    # Формируем детали карточки
    text = (
        f"{transport_emoji} <b>Транспорт:</b>\n"
        f"Тип: {transport_emoji} {transport_type}\n"
        f"Грузоподъёмность: {card.get('capacity', 0)} т\n\n"
        f"<b>📍 Регионы работы:</b>\n{regions_text}\n\n"
        f"Цена за км: {card.get('price_per_km', 0):.2f} ₽/км\n\n"
        f"<b>📝 Описание:</b>\n{card.get('description', 'Не указано')}\n\n"
        f"Имя: {card.get('user_name', expeditor.get('full_name', 'N/A'))}\n"
        f"Компания: {card.get('company', expeditor.get('company_name', 'N/A'))}\n"
        f"Телефон: <code>{card.get('phone', expeditor.get('phone', 'Не указан'))}</code>\n"
        f"Email: {card.get('email', expeditor.get('email', 'Не указан'))}\n"
        f"Username: @{expeditor.get('username', 'нет')}\n\n"
        f"📅 Создана: {card.get('created_at', 'Н/Д')}\n"
        f"👁 Просмотров: {card.get('views', 0)}"
    )

    keyboard = InlineKeyboardMarkup(row_width=1)

    # ✅ ПРОВЕРЯЕМ VALID ID И ДОБАВЛЯЕМ КНОПКУ БЕЗОПАСНО
    try:
        # Проверяем что ID валидный
            username = expeditor.get("username")

            if username:
                # ✅ ЛУЧШЕ ИСПОЛЬЗОВАТЬ USERNAME
                keyboard.add(
                    InlineKeyboardButton(
                        "📞 Связаться с экспедитором", url=f"https://t.me/{username}"
                    )
                )
            else:
                # Если нет username - используем callback для контакта
                keyboard.add(
                    InlineKeyboardButton(
                        "📞 Отправить сообщение",
                        callback_data=f"send_message_to:{expeditor_id}",
                    )
                )
    except Exception as e:
        logging.warning(f"⚠️ Ошибка добавления кнопки контакта: {e}")

    keyboard.add(
        InlineKeyboardButton(
            "◀️ К списку экспедиторов", callback_data="back_to_expeditors"
        )
    )

    # ✅ БЕЗОПАСНОЕ РЕДАКТИРОВАНИЕ
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logging.warning(f"⚠️ Ошибка edit_text: {e}")
        try:
            # Fallback: отправляем новое сообщение
            await callback.message.answer(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
        except Exception as e2:
            logging.error(f"❌ Ошибка answer: {e2}")
            await callback.answer("❌ Ошибка при отправке", show_alert=True)

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "back_to_expeditors", state="*")
async def back_to_expeditors(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку экспедиторов"""
    await state.finish()

    if not expeditor_cards:
        await callback.message.edit_text(
            "🚛 <b>База экспедиторов</b>\n\n" "📋 В базе пока нет экспедиторов.",
            parse_mode="HTML",
        )
        await callback.answer()
        return

    # Формируем список
    keyboard = InlineKeyboardMarkup(row_width=1)

    text = (
        "🚛 <b>База экспедиторов</b>\n\n"
        f"📋 Всего экспедиторов: <b>{len(expeditor_cards)}</b>\n\n"
        "Выберите экспедитора для просмотра карточки:"
    )

    for exp_id, card in list(expeditor_cards.items())[:20]:
        # ИСПРАВЛЕНО: transport_type вместо vehicle_type
        transport = card.get("transport_type", "Не указан")
        capacity = card.get("capacity", 0)
        regions = card.get("regions", [])
        price = card.get("price_per_km", 0)

        # Получаем эмодзи транспорта
        emoji = TRANSPORT_TYPES.get(transport, "🚛")

        # Форматируем регионы (список → строка)
        if isinstance(regions, list):
            regions_text = ", ".join(regions) if regions else "Регион не указан"
        else:
            regions_text = str(regions)

        # Обрезаем если очень длинное (максимум 30 символов)
        if len(regions_text) > 30:
            regions_text = regions_text[:27] + "..."

        btn_text = f"{emoji} {transport} {capacity}т | {regions_text} | {price}₽/км"
        keyboard.add(
            InlineKeyboardButton(btn_text, callback_data=f"view_expeditor:{exp_id}")
        )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_shipping_requests:"), state="*"
)
async def view_shipping_requests_callback(callback: CallbackQuery):
    """Просмотр заявок на доставку для конкретного пула"""
    pull_id = parse_callback_id(callback.data)

        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    relevant_requests = [
        req
        for req in shipping_requests.values()
    ]

    if not relevant_requests:
        await callback.answer(
        )
        return


    keyboard = InlineKeyboardMarkup(row_width=1)

        request_id = req.get("id")
        keyboard.add(
            InlineKeyboardButton(
            )
        )

    keyboard.add(
        InlineKeyboardButton("🔙 Назад к пулу", callback_data=f"view_pull:{pull_id}")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# ═══════════════════════════════════════════════════════════════════════════
# НОВЫЙ ОБРАБОТЧИК: ПРОСМОТР ДЕТАЛЕЙ ЗАЯВКИ ЛОГИСТА
# ═══════════════════════════════════════════════════════════════════════════


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_logist_request:"), state="*"
)
async def view_logist_request_details(callback: CallbackQuery):
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        return



@dp.callback_query_handler(lambda c: c.data.startswith("select_logist:"), state="*")
async def select_logist_for_pull(callback: CallbackQuery):
    """Экспортёр выбирает логиста для перевозки"""
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return


    # Обновляем статус заявки
        "%d.%m.%Y %H:%M"
    )

    # Уведомляем логиста
    try:
        await bot.send_message(
            logist_id,
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"❌ Ошибка уведомления логиста {logist_id}: {e}")

    await callback.message.edit_text(
        f"👤 Логист: {logist_data.get('name', 'Не указано')}\n"
        f"📞 Телефон: <code>{logist_data.get('phone', 'Не указан')}</code>\n\n"
        parse_mode="HTML",
    )
    await callback.answer("✅ Логист выбран!", show_alert=True)

    logging.info(f"✅ Экспортёр {callback.from_user.id} выбрал логиста {logist_id}")


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================


def edit_pull_fields_keyboard():
    """Клавиатура редактирования полей пула"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 Культура", callback_data="edit_pull_field:culture"),
        InlineKeyboardButton("📦 Объём", callback_data="edit_pull_field:volume"),
        InlineKeyboardButton("💰 Цена", callback_data="edit_pull_field:price"),
        InlineKeyboardButton("🚢 Порт", callback_data="edit_pull_field:port"),
    )
    keyboard.add(
        InlineKeyboardButton("💧 Влажность", callback_data="edit_pull_field:moisture"),
        InlineKeyboardButton("🏋️ Натура", callback_data="edit_pull_field:nature"),
        InlineKeyboardButton("🌾 Сорность", callback_data="edit_pull_field:impurity"),
        InlineKeyboardButton("🌿 Засорённость", callback_data="edit_pull_field:weed"),
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_pulls"))
    return keyboard


async def send_daily_stats():
    """Ежедневная отправка статистики админу"""
    try:
        total_users = len(users)
        role_stats = defaultdict(int)

        # ✅ ПРАВИЛЬНО: итерируемся по user_data
        for user in users.values():
            role = user.get("role", "unknown")
            role_stats[role] += 1

        # ✅ ИСПРАВЛЕНО: правильный подсчёт партий

        # ✅ ИСПРАВЛЕНО: правильный подсчёт активных партий
        active_batches = sum(
            1
        )

        total_deals = len(deals)
        active_deals = len(
            [
                d
                for d in deals.values()
                if d.get("status") in ["pending", "matched", "shipping"]
            ]
        )

        text = "📊 <b>Ежедневная статистика Exportum</b>\n\n"
        text += f"👥 Пользователей: {total_users}\n"
        text += f"📦 Партий: {total_batches} (активных: {active_batches})\n"
        text += f"🎯 Пулов: {total_pulls} (открытых: {open_pulls})\n"
        text += f"📋 Сделок: {total_deals} (активных: {active_deals})\n"
        text += f"🎯 Совпадений: {len(matches)}\n\n"

        text += "<b>Распределение по ролям:</b>\n"
        for role, count in role_stats.items():
            role_name = ROLES.get(role, role)
            text += f"• {role_name}: {count}\n"

        await bot.send_message(ADMIN_ID, text, parse_mode="HTML")
        logging.info("✅ Ежедневная статистика отправлена админу")

    except Exception as e:
        logging.error(f"❌ Ошибка отправки ежедневной статистики: {e}")


async def setup_scheduler():
    """Настройка планировщика задач"""
    try:
        scheduler.add_job(update_prices_cache, "interval", hours=6)
        scheduler.add_job(update_news_cache, "interval", hours=2)
        scheduler.add_job(auto_match_batches_and_pulls, "interval", minutes=30)
        scheduler.add_job(send_daily_stats, "cron", hour=9, minute=0)

        scheduler.start()
        logging.info("✅ Планировщик задач настроен и запущен")
    except Exception as e:
        logging.error(f"❌ Ошибка настройки планировщика: {e}")


# ==================== СОСТОЯНИЯ ДЛЯ ЛОГИСТОВ ====================
class LogisticStatesGroup(StatesGroup):
    """Состояния для создания заявки на перевозку логистом"""

    route_from = State()
    route_to = State()
    volume = State()
    price = State()
    vehicle_type = State()
    notes = State()
    desired_price = State()


# ==================== ОБРАБОТЧИКИ ДЛЯ ЛОГИСТОВ ====================
@dp.message_handler(lambda m: m.text == "➕ Создать заявку на перевозку", state="*")
async def create_shipping_request_start(message: types.Message, state: FSMContext):
    """Начало создания заявки на перевозку"""
    user_id = message.from_user.id

        await message.answer("❌ Эта функция доступна только логистам")
        return

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 1/7: Откуда (регион/город)\n\n"
        "Укажите место погрузки:",
        parse_mode="HTML",
    )
    await LogisticStatesGroup.route_from.set()


@dp.message_handler(state=LogisticStatesGroup.route_from)
async def logistic_route_from(message: types.Message, state: FSMContext):
    """Обработка места погрузки"""
    route_from = message.text.strip()

    await state.update_data(route_from=route_from)

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 2/7: Куда (регион/город/порт)\n\n"
        "Укажите место разгрузки:",
        parse_mode="HTML",
    )
    await LogisticStatesGroup.route_to.set()


@dp.message_handler(state=LogisticStatesGroup.route_to)
async def logistic_route_to(message: types.Message, state: FSMContext):
    """Обработка места разгрузки"""
    route_to = message.text.strip()

    await state.update_data(route_to=route_to)

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 3/7: Максимальный объем\n\n"
        "Укажите максимальный объем перевозки (тонн):",
        parse_mode="HTML",
    )
    await LogisticStatesGroup.volume.set()


@dp.message_handler(state=LogisticStatesGroup.volume)
async def logistic_volume(message: types.Message, state: FSMContext):
    """Обработка объема"""
    try:
        volume = float(message.text.replace(",", "."))
        if volume <= 0:
            raise ValueError

        await state.update_data(volume=volume)

        # ✅ НОВЫЙ ШАГ 4: ОЖИДАЕМАЯ ЦЕНА
        await message.answer(
            "🚚 <b>Создание заявки на перевозку</b>\n\n"
            "Шаг 4/7: Ожидаемая цена (желаемая цена за тонну)\n\n"
            "Укажите ожидаемую цену доставки (₽ за тонну):\n"
            "<i>Например: 500 или 1200.5</i>",
            parse_mode="HTML",
        )
        await LogisticStatesGroup.desired_price.set()

        await message.answer("❌ Неверный формат. Укажите число (например: 1500)")


@dp.message_handler(state=LogisticStatesGroup.desired_price)
async def logistic_desired_price(message: types.Message, state: FSMContext):
    """Обработка ожидаемой цены за тонну"""
    try:
        desired_price = float(message.text.replace(",", ".").replace(" ", ""))
        if desired_price <= 0:
            raise ValueError

        await state.update_data(desired_price=desired_price)

        await message.answer(
            "🚚 <b>Создание заявки на перевозку</b>\n\n"
            "Шаг 5/7: Ваш тариф\n\n"
            "Укажите ваш тариф (₽ за тонну, который вы готовы предложить):",
            parse_mode="HTML",
        )
        await LogisticStatesGroup.price.set()

        await message.answer(
            "❌ Неверный формат цены. Укажите число (например: 700 или 750.5)"
        )


@dp.message_handler(state=LogisticStatesGroup.price)
async def logistic_price(message: types.Message, state: FSMContext):
    """Обработка тарифа логиста"""
    try:
        price = float(message.text.replace(",", ".").replace(" ", ""))
        if price <= 0:
            raise ValueError

        await state.update_data(price=price)
        await message.answer(
            "🚚 <b>Создание заявки на перевозку</b>\n\n"
            "Шаг 6/7: Тип транспорта\n\n"
            "Укажите тип транспорта (например: Фура 20т, Зерновоз, Еврофура):",
            parse_mode="HTML",
        )
        await LogisticStatesGroup.vehicle_type.set()

        await message.answer("❌ Неверный формат. Укажите число (например: 700)")


@dp.message_handler(state=LogisticStatesGroup.vehicle_type)
async def logistic_vehicle_type(message: types.Message, state: FSMContext):
    """Обработка типа транспорта"""
    vehicle_type = message.text.strip()

    await state.update_data(vehicle_type=vehicle_type)

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 7/7: Примечания (необязательно)\n\n"
        "Дополнительная информация или нажмите /skip для пропуска:",
        parse_mode="HTML",
    )
    await LogisticStatesGroup.notes.set()


@dp.message_handler(lambda m: m.text == "/skip", state=LogisticStatesGroup.notes)
@dp.message_handler(state=LogisticStatesGroup.notes)
async def logistic_notes(message: types.Message, state: FSMContext):
    """Завершение создания заявки логистом"""
    user_id = message.from_user.id
    notes = "" if message.text == "/skip" else message.text.strip()
    await state.update_data(notes=notes)

    data = await state.get_data()

    # ✅ ГЕНЕРИРУЕМ ID ПРАВИЛЬНО (без global)

    # ✅ СОЗДАЁМ ЗАЯВКУ С ПОЛНОЙ СТРУКТУРОЙ
    request = {
        "id": request_id,
        "logist_id": user_id,
        "from": data["route_from"],
        "to": data["route_to"],
        "volume": data["volume"],
        "desired_price": data["desired_price"],
        "price": data["price"],
        "vehicle_type": data["vehicle_type"],
        "notes": notes,
        "status": "active",
        "offers_count": 0,  # ← ДОБАВИЛ!
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "offers": [],  # ← ДОБАВИЛ!
    }

    logistics_requests[request_id] = request

    # Формируем сообщение
    logist_name = logist.get("name", "Логист")
    logist_inn = logist.get("inn", "Не указан")
    logist_ogrn = logist.get("ogrn", "Не указан")

    # ✅ ПОКАЗЫВАЕМ СОЗДАНИЕ ЗАЯВКИ
    text = f"✅ <b>Заявка на перевозку #{request_id} создана!</b>\n\n"
    text += f"📍 Маршрут: {data['route_from']} → {data['route_to']}\n"
    text += f"📦 Объем: {data['volume']} т\n"
    text += f"💰 Ожидаемая цена: <code>{data['desired_price']:,.0f}</code> ₽/т\n"
    text += f"💰 Ваш тариф: <code>{data['price']:,.0f}</code> ₽/т\n"
    text += f"🚛 Транспорт: {data['vehicle_type']}\n"
    if notes:
        text += f"📝 Примечания: {notes}\n"

    text += f"\n👤 Контакт: {logist_name}\n"
    text += f"📋 ИНН: <code>{logist_inn}</code>\n"
    text += f"📋 ОГРН: <code>{logist_ogrn}</code>\n"
    text += f"📞 Телефон: <code>{logist.get('phone', 'Не указан')}</code>\n"
    text += f"📧 Email: <code>{logist.get('email', 'Не указан')}</code>"

    await message.answer(text, parse_mode="HTML", reply_markup=logistic_keyboard())

    await state.finish()

    # Сохраняем в JSON
    save_data()  # ← ИСПОЛЬЗУЙ УНИВЕРСАЛЬНУЮ ФУНКЦИЮ СОХРАНЕНИЯ

    logging.info(
        f"✅ Логист {user_id} создал заявку на перевозку #{request_id} "
        f"с ожидаемой ценой {data['desired_price']}"
    )


def get_status_emoji(status: str) -> str:
    """Получить эмодзи для статуса"""
    status_emojis = {
        "active": "🟢",
        "assigned": "🟡",
        "in_progress": "🔵",
        "completed": "✅",
        "cancelled": "❌",
        "pending": "🕐",
        "accepted": "✅",
        "rejected": "❌",
    }
    return status_emojis.get(status, "⚪")


def get_status_name(status: str) -> str:
    """Получить название статуса"""
    status_names = {
        "active": "Активна",
        "assigned": "Назначена",
        "in_progress": "В работе",
        "completed": "Завершена",
        "cancelled": "Отменена",
        "pending": "Ожидает",
        "accepted": "Принято",
        "rejected": "Отклонено",
    }
    return status_names.get(status, status)


@dp.callback_query_handler(text="logistic_requests_list", state="*")
async def show_logistic_requests_list(callback: types.CallbackQuery, state: FSMContext):
    """Список доступных заявок на доставку для логиста"""
    await state.finish()
    user_id = callback.from_user.id

    # ✅ Инициализируем клавиатуру ДО всех условий
    keyboard = InlineKeyboardMarkup(row_width=1)



    if not available_requests:

        keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data="logist_main_menu"))

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return


    text = "🚚 <b>Активные заявки</b>\n\n"
    text += f"📋 Всего заявок: <b>{len(available_requests)}</b>\n\n"

    for req_key, req in available_requests[:10]:
        # ✅ Определяем тип заявки

        if req_type == "farm":
            culture = req.get("culture", "Не указана")
            volume = req.get("volume", 0)

            button_text = f"🌾 {culture} {volume:.0f}т → {port} ({transport})"
        else:
            pull_id = req.get("pull_id")
            pull_info = (
            )
            culture = pull_info.get("culture", "Не указана")
            volume = req.get("volume", 0)

            button_text = f"📦 {culture} {volume:.0f}т | {route_from} → {route_to}"

            keyboard.add(
            )

    if len(available_requests) > 10:
        text += f"\n<i>... и ещё {len(available_requests) - 10} заявок</i>"

    # ✅ Кнопка «Назад»
    keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data="logist_main_menu"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# ============================================================================
# ЛОГИСТ: FSM СОЗДАНИЯ ПРЕДЛОЖЕНИЯ
# ============================================================================
@dp.callback_query_handler(lambda c: c.data.startswith("make_offer:"), state="*")
async def make_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало создания предложения логистом"""
    await state.finish()

    try:
        parts = callback.data.split(":")
        source = parts[1]  # exporter или farmer

        logging.info(f"✅ HANDLER TRIGGERED: source={source}, req_id={req_id}")
    except (IndexError, ValueError) as e:
        logging.error(f"❌ PARSE ERROR: {e}, data={callback.data}")
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

            await callback.answer("❌ Заявка не найдена", show_alert=True)
            return


    existing_offer = any(
        for o in logistic_offers.values()
    )

    if existing_offer:
        logging.warning(
            f"⚠️ DUPLICATE OFFER: user {user_id} already has offer for request {req_id}"
        )
        await callback.answer(
            "❌ Вы уже сделали предложение по этой заявке", show_alert=True
        )
        return


    pull_id = request.get("pull_id")
    expected_price = request.get("desired_price", "Не указана")

    text = f"""✅ <b>СОЗДАНИЕ ПРЕДЛОЖЕНИЯ</b>

📦 Заявка #{req_id}
📊 Объём: {request.get('volume', 0):.1f} т
💰 Ожидаемая цена: <code>{expected_price}</code> ₽/т

━━━━━━━━━━━━━━━━━━━━


    keyboard = InlineKeyboardMarkup(row_width=2)

    vehicles = [
    ]


    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"))

    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        logging.info(f"✅ MESSAGE EDITED for request {req_id}")
    except Exception as e:
        logging.error(f"❌ EDIT ERROR: {e}")
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")

    await LogisticOfferStatesGroup.vehicle_type.set()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("vehicle_"), state=LogisticOfferStatesGroup.vehicle_type
)
async def offer_vehicle_selected(callback: types.CallbackQuery, state: FSMContext):
    """Выбор типа транспорта"""
    vehicle_type = callback.data.replace("vehicle_", "")

    vehicles_display = {
        "grain": "🚚 Зерновоз",
        "truck": "🚛 Фура",
    }
    transport_display = vehicles_display.get(vehicle_type, "Неизвестный транспорт")

    await state.update_data(vehicle_type=vehicle_type)

    data = await state.get_data()
    request_id = data.get("request_id")
    volume = request.get("volume", 0)

    text = f"""✅ <b>СОЗДАНИЕ ПРЕДЛОЖЕНИЯ</b>

✓ Транспорт: <b>{transport_display}</b>

━━━━━━━━━━━━━━━━━━━━

<b>Шаг 2/4: Укажите стоимость доставки</b>

Объём груза: <b>{volume:.1f} т</b>

Введите стоимость доставки в рублях:
<i>(например: 50000)</i>"""

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"))

    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        logging.info(f"✅ VEHICLE SELECTED: {vehicle_type}")
    except Exception as e:
        logging.error(f"❌ EDIT ERROR: {e}")
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")

    await LogisticOfferStatesGroup.price.set()
    await callback.answer()


@dp.message_handler(state=LogisticOfferStatesGroup.price)
async def offer_price_entered(message: types.Message, state: FSMContext):
    """Ввод цены"""
    try:
        price_str = (
            message.text.strip().replace(" ", "").replace(",", "").replace("₽", "")
        )
        price = float(price_str)

        if price <= 0:
            await message.answer(
            )
            return

            await message.answer(
            )
            return

    except ValueError:
        await message.answer(
        )
        return

    await state.update_data(price=price)

    data = await state.get_data()
    vehicle_type = data.get("vehicle_type")

    vehicles_display = {
        "grain": "🚚 Зерновоз",
        "truck": "🚛 Фура",
    }
    transport = vehicles_display.get(vehicle_type, "Неизвестный транспорт")

    text = f"""✅ <b>СОЗДАНИЕ ПРЕДЛОЖЕНИЯ</b>

✓ Транспорт: <b>{transport}</b>
✓ Стоимость: <b>{price:,.0f} ₽</b>

━━━━━━━━━━━━━━━━━━━━

<b>Шаг 3/4: Укажите дату доставки</b>

Введите дату доставки в формате ДД.ММ.ГГГГ:
<i>(например: 15.11.2025)</i>"""

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"))

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await LogisticOfferStatesGroup.delivery_date.set()


@dp.message_handler(state=LogisticOfferStatesGroup.delivery_date)
async def offer_date_entered(message: types.Message, state: FSMContext):
    """Ввод даты доставки"""
    date_str = message.text.strip()

    try:
        delivery_date = datetime.strptime(date_str, "%d.%m.%Y")
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if delivery_date < today:
            await message.answer(
                "❌ Дата доставки не может быть в прошлом!\n\n"
                "Введите дату не ранее сегодняшнего дня:"
            )
            return

        max_date = today + timedelta(days=365)
        if delivery_date > max_date:
            await message.answer(
                "❌ Дата доставки слишком далеко (максимум 1 год)!\n\n"
                "Попробуйте ещё раз:"
            )
            return

    except ValueError:
        await message.answer(
            "❌ Неправильный формат даты!\n\n"
            "Используйте формат ДД.ММ.ГГГГ (например: 15.11.2025):"
        )
        return

    await state.update_data(delivery_date=date_str)

    try:
        await show_offer_confirmation(message, state, user_id=message.from_user.id)
    except Exception as e:
        logging.error(f"❌ Ошибка в show_offer_confirmation: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте ещё раз.")


@dp.callback_query_handler(
    lambda c: c.data == "skip_additional_info",
    state=LogisticOfferStatesGroup.additional_info,
)
async def offer_skip_additional_info(callback: types.CallbackQuery, state: FSMContext):
    """Пропуск дополнительной информации"""
    await state.update_data(additional_info="")
    await show_offer_confirmation(
        callback.message, state, user_id=callback.from_user.id, is_callback=True
    )
    await callback.answer()


@dp.message_handler(state=LogisticOfferStatesGroup.additional_info)
async def offer_additional_info_entered(message: types.Message, state: FSMContext):
    """Ввод дополнительной информации"""
    additional_info = message.text.strip()

    if len(additional_info) > 500:
        await message.answer(
            "❌ Слишком длинное сообщение (максимум 500 символов)!\n\n"
            "Сократите текст:"
        )
        return

    await state.update_data(additional_info=additional_info)
    await show_offer_confirmation(message, state, user_id=message.from_user.id)


async def show_offer_confirmation(
    message_or_callback, state: FSMContext, user_id: int, is_callback: bool = False
):
    """Показать подтверждение предложения"""
    data = await state.get_data()

    request_id = data.get("request_id")
        pull_id = request.get("pull_id")

    vehicle_type = data.get("vehicle_type")
    price = data.get("price")
    delivery_date = data.get("delivery_date")
    additional_info = data.get("additional_info", "")

    vehicles_display = {
        "grain": "🚚 Зерновоз",
    }
    transport = vehicles_display.get(vehicle_type, "Неизвестный транспорт")

    text = f"""📋 <b>ПОДТВЕРЖДЕНИЕ ПРЕДЛОЖЕНИЯ</b>

📦 <b>ЗАЯВКА #{request_id}</b>
📦 Объём: {request.get('volume', 0):.1f} т

━━━━━━━━━━━━━━━━━━━━

<b>ВАШЕ ПРЕДЛОЖЕНИЕ:</b>

🚛 Транспорт: <b>{transport}</b>
💰 Стоимость: <b>{price:,.0f} ₽</b>
📅 Дата доставки: <b>{delivery_date}</b>"""

    if additional_info:
        text += f"\n\nℹ️ Дополнительно:\n{additional_info}"

    text += "\n\n━━━━━━━━━━━━━━━━━━━━\n\n✅ Подтвердите отправку предложения"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Отправить", callback_data="confirm_offer"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"),
    )

    if is_callback:
        await message_or_callback.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    else:
        await message_or_callback.answer(text, reply_markup=keyboard, parse_mode="HTML")

    await LogisticOfferStatesGroup.confirm.set()


@dp.callback_query_handler(
    lambda c: c.data == "confirm_offer", state=LogisticOfferStatesGroup.confirm
)
async def offer_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение и создание предложения"""
    try:
        user_id = callback.from_user.id
        data = await state.get_data()

        request_id = data.get("request_id")
        vehicle_type = data.get("vehicle_type")
        price = data.get("price")
        delivery_date = data.get("delivery_date")
        additional_info = data.get("additional_info", "")

        if not all([request_id, vehicle_type, price, delivery_date]):
            await callback.answer(
                "❌ Ошибка: отсутствуют обязательные данные", show_alert=True
            )
            return

        vehicles_display = {
            "railway": "🚂 Ж/д вагон",
            "grain": "🚚 Зерновоз",
        }
        transport = vehicles_display.get(vehicle_type, "Неизвестный транспорт")


        offer = {
            "id": offer_id,
            "request_id": request_id,
            "logist_id": user_id,
            "vehicle_type": vehicle_type,
            "price": price,
            "delivery_date": delivery_date,
            "additional_info": additional_info,
            "status": "pending",
        }

        logistic_offers[offer_id] = offer

            )

        text = f"""✅ <b>ПРЕДЛОЖЕНИЕ ОТПРАВЛЕНО!</b>


📦 Заявка #{request_id}
🚛 Транспорт: <b>{transport}</b>
💰 Стоимость: <b>{price:,.0f} ₽</b>
📅 Дата: <b>{delivery_date}</b>



        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("💼 Мои предложения", callback_data="my_offers"),
            InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"),
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")

            try:


📦 <b>Заявка #{request_id}</b>
👤 От: <b>{logist_name}</b>
🚛 Транспорт: <b>{transport}</b>
💰 Стоимость: <b>{price:,.0f} ₽</b>
📅 Дата доставки: <b>{delivery_date}</b>"""

                if additional_info:

                        InlineKeyboardButton(
                            "👁 Посмотреть", callback_data=f"view_offer_details_{offer_id}"
                        )
                    )
                    )

                await bot.send_message(
                    parse_mode="HTML",
                )
            except Exception as e:
                logging.error(
                    exc_info=True,
                )
        else:
            )

        await state.finish()
        await callback.answer("✅ Предложение отправлено!")

        logging.info(
        )

    except Exception as e:
        logging.error(f"❌ Критическая ошибка в offer_confirmed: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
        await callback.message.answer(
            "❌ <b>ОШИБКА ПРИ СОЗДАНИИ ПРЕДЛОЖЕНИЯ</b>\n\n"
            "Попробуйте ещё раз или свяжитесь с поддержкой.",
            parse_mode="HTML",
        )


# ============================================================================
# ЛОГИСТ: УПРАВЛЕНИЕ ПРЕДЛОЖЕНИЯМИ
# ============================================================================
async def logistic_my_offers_text_button(message: types.Message, state: FSMContext):
    """Логист нажал на ТЕКСТОВУЮ кнопку '💼 Мои предложения'"""

    logger.info(
        f"✅ ТЕКСТОВАЯ КНОПКА: 💼 Мои предложения | User: {message.from_user.id}"
    )

    user_id = message.from_user.id

    try:
        await state.finish()

        my_offers = [
            (offer_id, offer)
            for offer_id, offer in logistic_offers.items()
        ]

        logger.info(f"[{user_id}] 📦 Найдено предложений: {len(my_offers)}")

        if not my_offers:
            text = "📋 <b>МОИ ПРЕДЛОЖЕНИЯ</b>\n\n❌ У вас нет предложений"
            keyboard = InlineKeyboardMarkup()
            keyboard.add(
                InlineKeyboardButton(
                    "📦 К заявкам", callback_data="logistic_requests_list"
                )
            )
            keyboard.add(InlineKeyboardButton("🔙 Меню", callback_data="back_to_main"))

            await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
            return

        for offer_id, offer in my_offers:
            if status not in by_status:
                by_status[status] = []
            by_status[status].append((offer_id, offer))

        logger.info(
        )

        text = f"📋 <b>МОИ ПРЕДЛОЖЕНИЯ</b> ({len(my_offers)} шт)\n\n"
        text += f"🕐 Ожидают: {len(by_status.get('pending', []))}\n"
        text += f"✅ Приняты: {len(by_status.get('accepted', []))}\n"
        text += f"❌ Отклонены: {len(by_status.get('rejected', []))}\n"
        text += f"⛔ Отменены: {len(by_status.get('cancelled', []))}\n\n"

        keyboard = InlineKeyboardMarkup(row_width=1)
        button_count = 0

        for status_key, emoji in [
            ("pending", "🕐"),
            ("accepted", "✅"),
            ("rejected", "❌"),
            ("cancelled", "⛔"),
        ]:
            for offer_id, offer in by_status.get(status_key, [])[:5]:
                if button_count >= 10:
                    break
                price = offer.get("price", 0)
                keyboard.add(
                    InlineKeyboardButton(
                        button_text, callback_data=f"view_my_offer_{offer_id}"
                    )
                )
                button_count += 1

        keyboard.add(InlineKeyboardButton("🔙 Меню", callback_data="back_to_main"))

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        logger.info(f"[{user_id}] ✅ УСПЕХ! Показано {button_count} кнопок")

    except Exception as e:
        logger.error(f"[{user_id}] ❌ ОШИБКА: {e}", exc_info=True)
        await message.answer("❌ Ошибка загрузки", parse_mode="HTML")


@dp.callback_query_handler(lambda c: c.data == "my_offers")
async def show_my_offers(callback: types.CallbackQuery, state: FSMContext):
    """Показать мои предложения"""

    user_id = callback.from_user.id
    username = callback.from_user.username or "NoUsername"

    logger.info(f"✅ КЛИК ПО 'МОИ ПРЕДЛОЖЕНИЯ' | User: {user_id} | @{username}")

    try:
        try:
            await state.finish()
            logger.debug(f"[{user_id}] FSM state завершён")

        my_offers = [
            (offer_id, offer)
            for offer_id, offer in logistic_offers.items()
        ]

        logger.info(f"[{user_id}] Найдено предложений: {len(my_offers)}")

        if not my_offers:
            text = "📋 <b>МОИ ПРЕДЛОЖЕНИЯ</b>\n\n❌ У вас нет предложений"
            keyboard = InlineKeyboardMarkup()
            keyboard.add(
                InlineKeyboardButton(
                    "📦 К заявкам", callback_data="logistic_requests_list"
                )
            )
            keyboard.add(InlineKeyboardButton("🔙 Меню", callback_data="back_to_main"))

            await callback.message.edit_text(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
            await callback.answer()
            return

        # ✅ НЕ СОЗДАЁМ ПУСТОЙ СЛОВАРЬ! СОБИРАЕМ ВСЕ СТАТУСЫ
        by_status = {}
        for offer_id, offer in my_offers:
            logger.info(f"[{user_id}] 📌 Предложение #{offer_id}: статус = '{status}'")
            if status not in by_status:
                by_status[status] = []
            by_status[status].append((offer_id, offer))

        logger.info(
            f"[{user_id}] 📊 Группировка: {dict((k, len(v)) for k, v in by_status.items())}"
        )

        text = f"📋 <b>МОИ ПРЕДЛОЖЕНИЯ</b> ({len(my_offers)} шт)\n\n"
        text += f"🕐 Ожидают: {len(by_status.get('pending', []))}\n"
        text += f"✅ Приняты: {len(by_status.get('accepted', []))}\n"
        text += f"❌ Отклонены: {len(by_status.get('rejected', []))}\n"
        text += f"⛔ Отменены: {len(by_status.get('cancelled', []))}\n\n"

        keyboard = InlineKeyboardMarkup(row_width=1)
        button_count = 0

        # ✅ ДОБАВЛЯЕМ ВСЕ СТАТУСЫ
        for status_key, emoji in [
            ("pending", "🕐"),
            ("accepted", "✅"),
            ("rejected", "❌"),
            ("cancelled", "⛔"),
        ]:
            for offer_id, offer in by_status.get(status_key, [])[:5]:
                if button_count >= 10:
                    break
                price = offer.get("price", 0)
                button_text = f"{emoji} #{offer_id} | {price:,.0f}₽"
                keyboard.add(
                    InlineKeyboardButton(
                        button_text, callback_data=f"view_my_offer_{offer_id}"
                    )
                )
                button_count += 1

        keyboard.add(InlineKeyboardButton("🔙 Меню", callback_data="back_to_main"))

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()

        logger.info(f"[{user_id}] ✅ УСПЕХ! Показано {button_count} кнопок")

    except Exception as e:
        logger.error(f"[{user_id}] ❌ ОШИБКА: {e}", exc_info=True)
        await callback.answer("❌ Ошибка загрузки", show_alert=True)


@dp.callback_query_handler(lambda c: c.data.startswith("view_my_offer_"), state="*")
async def view_my_offer_details(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID предложения", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    user_id = callback.from_user.id
        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return

    request_id = offer.get("request_id")
        pull_id = request.get("pull_id")
    vehicles_display = {
        "fura": "🚛 Фура",
        "railway": "🚂 Ж/д вагон",
        "grain": "🚚 Зерновоз",
    }

    text = f"📋 <b>МОЁ ПРЕДЛОЖЕНИЕ #{offer_id}</b>\n\n"
    text += f"📦 <b>ЗАЯВКА #{request_id}</b>\n"
    text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
    )
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    transport = vehicles_display.get(offer.get("vehicle_type"), "Не указан")
    text += f"🚛 Транспорт: <b>{transport}</b>\n"
    text += f"💰 Стоимость: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
    text += f"📅 Дата доставки: <b>{offer.get('delivery_date', 'Не указана')}</b>\n"
    if offer.get("additional_info"):
        text += f"\nℹ️ Дополнительно:\n{offer.get('additional_info')}\n"
    text += f"\n📅 Создано: {offer.get('created_at', 'Не указано')}\n"
    if offer.get("updated_at"):
        text += f"✏️ Изменено: {offer.get('updated_at')}\n"
    text += "\n"
    status_icon = status_map.get(status, "⚪").split()[0]
    status_name = get_status_name(status)

    text += f"📊 Статус: <b>{status_icon} {status_name}</b>\n"
    if status == "accepted":
    elif status == "rejected":
        if offer.get("rejection_reason"):
            text += f"<i>Причина: {offer.get('rejection_reason')}</i>"
    elif status == "cancelled":
        if offer.get("cancelled_at"):
            text += f"\n<i>Отменено: {offer.get('cancelled_at')}</i>"

    keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton(
                "✏️ Редактировать цену", callback_data=f"edit_price_{offer_id}"
            ),
            InlineKeyboardButton(
                "❌ Отменить", callback_data=f"cancel_my_offer_{offer_id}"
            ),
        )
    keyboard.add(
        InlineKeyboardButton("🔙 К моим предложениям", callback_data="my_offers")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("cancel_my_offer_"), state="*")
async def cancel_my_offer_confirm(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение отмены предложения"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return

        await callback.answer(
            "❌ Можно отменить только ожидающие предложения", show_alert=True
        )
        return

    vehicles_display = {
        "fura": "🚛 Фура",
        "railway": "🚂 Ж/д вагон",
        "grain": "🚚 Зерновоз",
    }

    transport = vehicles_display.get(offer.get("vehicle_type"), "Неизвестный")

    text = f"❓ <b>ОТМЕНА ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    text += f"🚛 Транспорт: {transport}\n"
    text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n\n"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, отменить", callback_data=f"confirm_cancel_offer_{offer_id}"
        ),
        InlineKeyboardButton("❌ Нет", callback_data=f"view_my_offer_{offer_id}"),
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_cancel_offer_"), state="*"
)
async def cancel_my_offer_confirmed(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return


@dp.callback_query_handler(lambda c: c.data.startswith("edit_offer_"), state="*")
async def edit_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования предложения"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return

        await callback.answer(
            "❌ Можно редактировать только ожидающие предложения", show_alert=True
        )
        return

    text = f"✏️ <b>РЕДАКТИРОВАНИЕ ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
    text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n"
    text += f"📅 Дата: {offer.get('delivery_date')}\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "💰 Изменить цену", callback_data=f"edit_price_{offer_id}"
        ),
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Назад", callback_data=f"view_my_offer_{offer_id}")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# Добавим FSM states для редактирования
class EditOfferStatesGroup(StatesGroup):
    """FSM для редактирования предложения"""

    offer_id = State()
    field = State()
    value = State()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_price_"), state="*")
async def edit_offer_price(callback: types.CallbackQuery, state: FSMContext):
    """Редактирование цены"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if not offer:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return

        await callback.answer(
            "❌ Нельзя редактировать обработанные предложения", show_alert=True
        )
        return

    await state.update_data(offer_id=offer_id, field="price")

    text += f"Заявка #{offer.get('request_id')}\n\n"
    text += f"Текущая цена: <b>{offer.get('price', 0):,.0f} ₽</b>\n\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("❌ Отмена", callback_data=f"view_my_offer_{offer_id}")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await EditOfferStatesGroup.value.set()
    await callback.answer()


@dp.message_handler(state=EditOfferStatesGroup.value)
async def edit_offer_value_entered(message: types.Message, state: FSMContext):
    data = await state.get_data()
    offer_id = data.get("offer_id")
    field = data.get("field")

    if not offer:
        await message.answer("❌ Предложение не найдено")
        await state.finish()
        return

    # Валидация и сохранение
    if field == "price":
        try:
            new_price = float(
                message.text.strip().replace(" ", "").replace(",", "").replace("₽", "")
            )
                await message.answer(
                    "❌ Неправильная цена! Диапазон: 1 - 10 млн ₽\n\nПопробуйте ещё раз:"
                )
                return

            old_price = offer.get("price")
            offer["price"] = new_price
            offer["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


            request_id = offer.get("request_id")
                "company_name", "Логист"
            )

                try:
                    await bot.send_message(
                        f"🔔 <b>Предложение #{offer_id} изменено</b>\n\n"
                        f"📦 Заявка #{request_id}\n"
                        f"👤 Логист: {logist_name}\n"
                        f"💰 Новая цена: <b>{new_price:,.0f} ₽</b>",
                        parse_mode="HTML",
                    )
                except Exception as e:
                    logging.error(f"Ошибка уведомления: {e}")

        except ValueError:
            await message.answer(
                "❌ Неправильный формат! Введите число (без символов)\n\nПопробуйте ещё раз:"
            )
            return

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            "👁 Посмотреть предложение", callback_data=f"view_my_offer_{offer_id}"
        )
    )
    keyboard.add(InlineKeyboardButton("💼 Мои предложения", callback_data="my_offers"))

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await state.finish()

    logging.info(
        f"✏️ Логист {message.from_user.id} изменил цену предложения #{offer_id}"
    )


# ============================================================================
# ЛОГИСТ: СИСТЕМА УВЕДОМЛЕНИЙ
# ============================================================================


async def notify_logistic_offer_accepted(offer_id: int, exporter_id: int):
    """Уведомить логиста о принятии предложения"""
    try:
            return

        if not logist_id:
            return

        request_id = offer.get("request_id")

        text += f"📋 Предложение #{offer_id}\n"
        text += f"📦 Заявка #{request_id}\n"
        text += f"👤 Заказчик: {exporter_company}\n\n"
        text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
        text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n"
        text += f"📅 Дата доставки: {offer.get('delivery_date')}\n\n"

        if exporter_info.get("phone"):
            text += f"📞 Телефон заказчика: {exporter_info.get('phone')}\n"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "📋 Детали предложения", callback_data=f"view_my_offer_{offer_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton("🚛 Мои доставки", callback_data="my_deliveries")
        )

        await bot.send_message(
            logist_id, text, reply_markup=keyboard, parse_mode="HTML"
        )

        logging.info(f"✅ Уведомление о принятии отправлено логисту {logist_id}")

    except Exception as e:
        logging.error(f"❌ Ошибка уведомления логиста о принятии: {e}")


async def notify_logistic_offer_rejected(
    offer_id: int, exporter_id: int, reason: str = None
):
    """Уведомить логиста об отклонении предложения"""
    try:
            return

        if not logist_id:
            return

        request_id = offer.get("request_id")

        text += f"📋 Предложение #{offer_id}\n"
        text += f"📦 Заявка #{request_id}\n"
        text += f"👤 Заказчик: {exporter_company}\n\n"

        if reason:
            text += f"💬 <b>Причина:</b>\n{reason}\n\n"


        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "📦 Посмотреть другие заявки", callback_data="logistic_requests_list"
            )
        )
        keyboard.add(
            InlineKeyboardButton("💼 Мои предложения", callback_data="my_offers")
        )

        await bot.send_message(
            logist_id, text, reply_markup=keyboard, parse_mode="HTML"
        )

        logging.info(f"❌ Уведомление об отклонении отправлено логисту {logist_id}")

    except Exception as e:
        logging.error(f"❌ Ошибка уведомления логиста об отклонении: {e}")


async def notify_logistic_new_request(request_id: int):
    """Уведомить всех логистов о новой заявке"""
    try:
            return
        pull_id = request.get("pull_id")


        if not logistics:
            return

        text += f"📦 Заявка #{request_id}\n"
        text += f"🌾 Культура: {pull_info.get('culture', 'Не указана')}\n"
        text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
        text += f"📍 Маршрут: {request.get('route_from', '')} → {request.get('route_to', '')}\n"


        if request.get("budget"):
            text += f"💰 Бюджет: {request.get('budget'):,.0f} ₽\n"


        keyboard = InlineKeyboardMarkup()
        keyboard.add(
                InlineKeyboardButton(
                    "👀 Посмотреть заявку",
                )
            )
        keyboard.add(
            InlineKeyboardButton(
                "📦 Все заявки", callback_data="logistic_requests_list"
            )
        )

        # Отправляем уведомление каждому логисту
        sent_count = 0
        for logist_id in logistics:
            try:
                await bot.send_message(
                    logist_id, text, reply_markup=keyboard, parse_mode="HTML"
                )
                sent_count += 1
                await asyncio.sleep(0.1)  # Небольшая задержка между отправками
            except Exception as e:
                logging.error(f"Ошибка отправки логисту {logist_id}: {e}")

        logging.info(
            f"🔔 Уведомление о новой заявке #{request_id} отправлено {sent_count} логистам"
        )

    except Exception as e:
        logging.error(f"❌ Ошибка массового уведомления логистов: {e}")


async def notify_logistic_delivery_started(delivery_id: int):
    """Уведомить логиста о начале доставки"""
    try:
            return

        if not logist_id:
            return

        request_id = delivery.get("request_id")

        text += f"📦 Доставка #{delivery_id}\n"
        text += f"📋 Заявка #{request_id}\n\n"
        text += f"📍 Маршрут: {delivery.get('route_from', '')} → {delivery.get('route_to', '')}\n"
        text += f"📅 Плановая дата: {delivery.get('delivery_date', '')}\n\n"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "📋 Детали доставки", callback_data=f"view_delivery_{delivery_id}"
            )
        )

        await bot.send_message(
            logist_id, text, reply_markup=keyboard, parse_mode="HTML"
        )

        logging.info(f"🚚 Уведомление о начале доставки отправлено логисту {logist_id}")

    except Exception as e:
        logging.error(f"❌ Ошибка уведомления о начале доставки: {e}")


async def notify_logistic_delivery_completed(delivery_id: int):
    """Уведомить логиста о завершении доставки"""
    try:
            return

        if not logist_id:
            return

        offer_id = delivery.get("offer_id")
        price = offer.get("price", 0)

        text += f"📦 Доставка #{delivery_id}\n"
        text += f"💰 Сумма: <b>{price:,.0f} ₽</b>\n\n"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "📋 История доставок", callback_data="delivery_history"
            )
        )
        keyboard.add(
            InlineKeyboardButton("📊 Статистика", callback_data="logistic_statistics")
        )

        await bot.send_message(
            logist_id, text, reply_markup=keyboard, parse_mode="HTML"
        )

        logging.info(
            f"✅ Уведомление о завершении доставки отправлено логисту {logist_id}"
        )

    except Exception as e:
        logging.error(f"❌ Ошибка уведомления о завершении доставки: {e}")


async def notify_logistic_request_cancelled(request_id: int, reason: str = None):
    """Уведомить логистов об отмене заявки"""
    try:
            return


            return

        text += f"📦 Заявка #{request_id} была отменена заказчиком.\n\n"

        if reason:
            text += f"💬 Причина: {reason}\n\n"


        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "📦 Другие заявки", callback_data="logistic_requests_list"
            )
        )

        # Уведомляем каждого логиста
            try:
                await bot.send_message(
                    logist_id, text, reply_markup=keyboard, parse_mode="HTML"
                )
                await asyncio.sleep(0.1)
            except Exception as e:
                logging.error(f"Ошибка уведомления логиста {logist_id}: {e}")

        logging.info(f"❌ Уведомления об отмене заявки #{request_id} отправлены")

    except Exception as e:
        logging.error(f"❌ Ошибка уведомления об отмене заявки: {e}")


# ============================================================================
# ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ЛОГИСТА
# ============================================================================


@dp.callback_query_handler(lambda c: c.data == "my_deliveries", state="*")
async def show_my_deliveries(callback: types.CallbackQuery, state: FSMContext):
    """Показать мои доставки"""
    await state.finish()

    user_id = callback.from_user.id


    if not my_deliveries:
        text = "🚚 <b>МОИ ДОСТАВКИ</b>\n\n"
        text += "❌ У вас пока нет активных доставок\n\n"
        text += "<i>Доставки появятся после принятия ваших предложений</i>"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("💼 Мои предложения", callback_data="my_offers")
        )
        keyboard.add(
            InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return

    # Группируем по статусам
    by_status = {"pending": [], "in_progress": [], "completed": [], "cancelled": []}

    for deliv_id, deliv in my_deliveries:
        by_status[status].append((deliv_id, deliv))

    text += f"Всего доставок: <b>{len(my_deliveries)}</b>\n\n"

    active = len(by_status["pending"]) + len(by_status["in_progress"])
    completed = len(by_status["completed"])

    text += f"🟢Активные: <b>{active}</b>\n"
    text += f"✅ Завершённые: <b>{completed}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите доставку:"

    keyboard = InlineKeyboardMarkup(row_width=1)

    # Показываем активные доставки
    for status_key, status_name, emoji in [
        ("pending", "Ожидают", "🕐"),
        ("in_progress", "В пути", "🚚"),
        ("completed", "Завершены", "✅"),
    ]:
        delivs = by_status[status_key]
        if delivs:
            for deliv_id, deliv in delivs[:5]:
                request_id = deliv.get("request_id")

                route = (
                )

                button_text = f"{emoji} #{deliv_id} | {route}"

                keyboard.add(
                    InlineKeyboardButton(
                        button_text, callback_data=f"view_delivery_{deliv_id}"
                    )
                )

    keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="my_deliveries"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "logistic_statistics", state="*")
async def show_logistic_statistics(callback: types.CallbackQuery, state: FSMContext):
    """Показать статистику логиста"""
    await state.finish()

    user_id = callback.from_user.id

    # Собираем статистику

    total_offers = len(my_offers)

    completed_deliveries = len(
    )
    active_deliveries = len(
    )

    # Подсчитываем общий заработок

    # Конверсия
    conversion = (accepted_offers / total_offers * 100) if total_offers > 0 else 0


    text += f"📋 Всего отправлено: <b>{total_offers}</b>\n"
    text += f"✅ Принято: <b>{accepted_offers}</b>\n"
    text += f"❌ Отклонено: <b>{rejected_offers}</b>\n"
    text += f"🕐 Ожидают ответа: <b>{pending_offers}</b>\n"
    text += f"📈 Конверсия: <b>{conversion:.1f}%</b>\n\n"

    text += f"🚚 Активные: <b>{active_deliveries}</b>\n"
    text += f"✅ Завершённые: <b>{completed_deliveries}</b>\n\n"

    text += f"💰 Общий заработок: <b>{total_earnings:,.0f} ₽</b>\n"
    if completed_deliveries > 0:
        avg_earning = total_earnings / completed_deliveries
        text += f"📊 Средняя доставка: <b>{avg_earning:,.0f} ₽</b>\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("💼 Мои предложения", callback_data="my_offers"))
    keyboard.add(InlineKeyboardButton("🚛 Мои доставки", callback_data="my_deliveries"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# ==================== ФУНКЦИИ СОХРАНЕНИЯ ====================
def save_shipping_requests():


def load_shipping_requests():
    try:
                shipping_requests = pickle.load(f)
            logging.info(
            )
    except Exception as e:
        logging.error(f"Ошибка загрузки shipping_requests: {e}")
        shipping_requests = {}


# ═══════════════════════════════════════════════════════════════════════════
# СОЗДАНИЕ ЗАЯВКИ НА ЛОГИСТИКУ (ОБРАБОТЧИКИ)
# ═══════════════════════════════════════════════════════════════════════════
@dp.message_handler(state=ShippingRequestStatesGroup.route_from)
async def shipping_route_from(message: types.Message, state: FSMContext):
    """Шаг 1: Пункт отправки"""
    route_from = message.text.strip()

    if not route_from or len(route_from) < 2:
        await message.answer("❌ Укажите корректное название города/региона")
        return

    await state.update_data(route_from=route_from)

    await message.answer(
        f"📍 Пункт отправки: <b>{route_from}</b>\n\n"
        "<b>Шаг 2 из 7</b>\n\n"
        "Введите пункт назначения (город/порт):",
        parse_mode="HTML",
    )

    await ShippingRequestStatesGroup.route_to.set()


# ===== ШАГ 2: Пункт назначения =====
@dp.message_handler(state=ShippingRequestStatesGroup.route_to)
async def shipping_route_to(message: types.Message, state: FSMContext):
    """Шаг 2: Пункт назначения"""
    route_to = message.text.strip()

    if not route_to or len(route_to) < 2:
        await message.answer("❌ Укажите корректное название пункта назначения")
        return

    await state.update_data(route_to=route_to)
    data = await state.get_data()
    pull_id = data.get("pull_id")

    # ✅ ИСПРАВЛЕНО: получаем пул из правильного места
    all_pulls = pulls.get("pulls", {})
    pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

    if not pull:
        await message.answer("❌ Пул не найден")
        await state.finish()
        return


    await message.answer(
        f"📍 Маршрут: <b>{data['route_from']}</b> → <b>{route_to}</b>\n\n"
        "<b>Шаг 3 из 7</b>\n\n"
        parse_mode="HTML",
    )

    await ShippingRequestStatesGroup.volume.set()


# ===== ШАГ 3: Объём =====
@dp.message_handler(state=ShippingRequestStatesGroup.volume)
async def shipping_volume(message: types.Message, state: FSMContext):
    """Шаг 3: Объём"""
    try:
        volume = float(message.text.replace(",", ".").replace(" ", ""))

        if volume <= 0:
            await message.answer("❌ Объём должен быть больше нуля")
            return

        data = await state.get_data()
        pull_id = data.get("pull_id")

        # ✅ ИСПРАВЛЕНО: Получаем пулы из правильного места
        all_pulls = pulls.get("pulls", {})
        pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

        if not pull:
            await message.answer("❌ Пул не найден")
            await state.finish()
            return

            await message.answer(
            )
            return

        await state.update_data(volume=volume)

        await message.answer(
            f"📦 Объём перевозки: <b>{volume:.0f} т</b>\n\n"
            "<b>Шаг 4 из 7</b>\n\n"
            "Введите культуру:",
            parse_mode="HTML",
        )

        await ShippingRequestStatesGroup.culture.set()

    except ValueError:
        await message.answer(
            "❌ Пожалуйста, введите корректное число\nПример: 100 или 150.5"
        )


# ===== ШАГ 4: Культура =====
@dp.message_handler(state=ShippingRequestStatesGroup.culture)
async def shipping_culture(message: types.Message, state: FSMContext):
    """Шаг 4: Культура"""
    culture = message.text.strip()

    if not culture or len(culture) < 2:
        await message.answer("❌ Укажите корректное название культуры")
        return

    await state.update_data(culture=culture)

    await message.answer(
        f"🌾 Культура: <b>{culture}</b>\n\n"
        "<b>Шаг 5 из 7</b>\n\n"
        "Введите цену перевозки за тонну в рублях\n"
        "Пример: 5500",
        parse_mode="HTML",
    )

    await ShippingRequestStatesGroup.price_rub.set()


# ===== ШАГ 5: Цена в рублях =====
@dp.message_handler(state=ShippingRequestStatesGroup.price_rub)
async def shipping_price_rub(message: types.Message, state: FSMContext):
    """Шаг 5: Цена в рублях за тонну"""

    price_text = message.text.strip()

    try:
        price_rub = float(price_text)
        if price_rub <= 0:
            await message.answer("❌ Цена должна быть больше нуля!")
            return
        if price_rub > 1000000:
            await message.answer("❌ Цена слишком высокая! (макс 1 млн ₽/т)")
            return

        price_rub = int(price_rub)
        await state.update_data(price_rub=price_rub)

        await message.answer(
            f"💰 <b>Цена установлена:</b> <code>{price_rub:,} ₽/т</code>\n\n"
            "<b>Шаг 6 из 7</b>\n\n"
            "Введите желаемую дату отправки (ДД.ММ.ГГГГ)\n"
            "Или нажмите /skip чтобы пропустить:",
            parse_mode="HTML",
        )
        await ShippingRequestStatesGroup.desired_date.set()

    except ValueError:
        await message.answer("❌ Введите корректное число\nПример: 5500")


# ===== ШАГ 6: Дата отправки =====
@dp.message_handler(
    lambda m: m.text == "/skip", state=ShippingRequestStatesGroup.desired_date
)
async def shipping_desired_date_skip(message: types.Message, state: FSMContext):
    """Шаг 6: Пропуск даты"""
    await state.update_data(desired_date="Не указана")
    await shipping_final_confirmation(message, state)


@dp.message_handler(state=ShippingRequestStatesGroup.desired_date)
async def shipping_desired_date(message: types.Message, state: FSMContext):
    """Шаг 6: Желаемая дата отправки"""
    desired_date = message.text.strip()

    if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", desired_date):
        await message.answer(
            "❌ Неверный формат даты!\n"
            "Используйте: ДД.ММ.ГГГГ (например, 15.11.2025)"
        )
        return

    try:
        datetime.strptime(desired_date, "%d.%m.%Y")
    except ValueError:
        await message.answer("❌ Некорректная дата! Проверьте число и месяц")
        return

    await state.update_data(desired_date=desired_date)
    await shipping_final_confirmation(message, state)


# ===== ШАГ 7: Финальное подтверждение и создание заявки =====
async def shipping_final_confirmation(message: types.Message, state: FSMContext):
    """Шаг 7: Финальное создание заявки"""

    data = await state.get_data()
    pull_id = data.get("pull_id")

    all_pulls = pulls.get("pulls", {})
    pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

    if not pull:
        await message.answer("❌ Пул не найден. Операция отменена.")
        await state.finish()
        return

    if not exporter_id:
        await message.answer("❌ Экспортёр не найден. Операция отменена.")
        await state.finish()
        return


    # ✅ СОЗДАЁМ ЗАЯВКУ
    request = {
        "id": request_id,
        "pull_id": pull_id,
        "exporter_id": exporter_id,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "logist_id": None,
    }

    shipping_requests[request_id] = request
    save_shipping_requests()

    await state.finish()

    summary = (
        f"✅ <b>Заявка на логистику #{request_id} создана!</b>\n\n"
        f"📦 Пул: #{pull_id}\n"
        f"👤 Контакт: {exporter.get('name', 'N/A')}\n"
        f"📋 ИНН: <code>{exporter.get('inn', 'Не указан')}</code>\n"
        f"📋 ОГРН: <code>{exporter.get('ogrn', 'Не указан')}</code>\n"
        f"📞 Телефон: <code>{exporter.get('phone', 'N/A')}</code>\n"
        f"📧 Email: <code>{exporter.get('email', 'N/A')}</code>\n\n"
    )

    await message.answer(summary, parse_mode="HTML", reply_markup=exporter_keyboard())


    logging.info(
        f"✅ Заявка #{request_id} создана\n"
    )


# ==================== АВТОМАТИЧЕСКОЕ ПРИКРЕПЛЕНИЕ ПОДРЯДЧИКОВ ====================


async def attach_contractors_to_pull(pull_id):
    """
    Автоматическое прикрепление логистов и экспедиторов к закрытому пуллу
    Вызывается когда пулл достигает нужного тоннажа
    """
        return
    exporter_id = pull.get("exporter_id")
    pull_port = pull.get("port", "")
    pull_region = pull.get("region", "")

    # Находим подходящих логистов
    suitable_logistics = []
    for req_id, request in shipping_requests.items():
            continue

        # Проверяем совпадение по региону/порту
                suitable_logistics.append(
                    {
                        "request_id": req_id,
                        "logist_id": logist_id,
                        "vehicle": request.get("vehicle_type", "Не указан"),
                    }
                )

    # Находим подходящих экспедиторов
    suitable_expeditors = []
    for offer_id, offer in expeditor_offers.items():
            continue

        # Проверяем совпадение по порту
            expeditor_id = offer.get("expeditor_id")
                suitable_expeditors.append(
                    {
                        "offer_id": offer_id,
                        "expeditor_id": expeditor_id,
                    }
                )

    # Отправляем экспортеру карточки подрядчиков
    if exporter_id and (suitable_logistics or suitable_expeditors):
        text = f"🎉 <b>Пулл #{pull_id} собран!</b>\n\n"
        text += f"🚢 Порт: {pull_port}\n\n"

        if suitable_logistics:
            text += f"🚚 <b>Доступные логисты ({len(suitable_logistics)}):</b>\n\n"
            for idx, logist in enumerate(suitable_logistics[:5], 1):
                text += f"{idx}. {logist['logist_name']}\n"
                text += f"   • {logist['route']}\n"
                text += f"   • {logist['price']:,.0f} ₽/т, до {logist['volume']} т\n"
                text += f"   • {logist['vehicle']}\n"
                text += f"   • 📞 {logist['phone']}\n\n"

            if len(suitable_logistics) > 5:
                text += f"<i>... и ещё {len(suitable_logistics) - 5} логистов</i>\n\n"

        if suitable_expeditors:
            text += f"🚛 <b>Доступные экспедиторы ({len(suitable_expeditors)}):</b>\n\n"
            for idx, exp in enumerate(suitable_expeditors[:5], 1):
                text += f"{idx}. {exp['expeditor_name']}\n"
                text += f"   • {exp['service']}\n"
                text += f"   • {exp['price']:,.0f} ₽, срок: {exp['terms']}\n"
                text += f"   • Порты: {exp['ports']}\n"
                text += f"   • 📞 {exp['phone']}\n\n"

            if len(suitable_expeditors) > 5:
                text += (
                    f"<i>... и ещё {len(suitable_expeditors) - 5} экспедиторов</i>\n\n"
                )

        text += "💡 <b>Свяжитесь с подрядчиками для обсуждения условий.</b>"

        try:
            await bot.send_message(exporter_id, text, parse_mode="HTML")
            logging.info(
                f"✅ Экспортеру {exporter_id} отправлены карточки подрядчиков для пулла #{pull_id}"
            )
        except Exception as e:
            logging.error(f"Ошибка отправки карточек подрядчиков: {e}")

    # ← ДОБАВИТЬ: Уведомить фермеров из пулла
    if pull_id in pullparticipants:
        participants = pullparticipants[pull_id]
        for participant in participants:
            farmer_id = participant.get("farmer_id")
            if farmer_id and farmer_id in users:
                try:
                    # Формируем сообщение для фермера
                    farmer_text = f"🎉 <b>Пулл #{pull_id} собран!</b>\n\n"
                    farmer_text += (
                    )
                    farmer_text += f"🚢 Порт: {pull_port}\n\n"

                    # Показать первого логиста (если есть)
                    if suitable_logistics:
                        logist = suitable_logistics[0]
                        farmer_text += "🚚 <b>Доступный логист:</b>\n"
                        farmer_text += f"• {logist['logist_name']}\n"
                        farmer_text += f"• {logist['route']}\n"
                        farmer_text += f"• {logist['price']:,.0f} ₽/т\n"
                        farmer_text += f"• 📞 {logist['phone']}\n\n"

                        if len(suitable_logistics) > 1:
                            farmer_text += f"<i>...и ещё {len(suitable_logistics) - 1} логистов</i>\n\n"

                    # Показать первого экспедитора (если есть)
                    if suitable_expeditors:
                        exp = suitable_expeditors[0]
                        farmer_text += "🚛 <b>Доступный экспедитор:</b>\n"
                        farmer_text += f"• {exp['expeditor_name']}\n"
                        farmer_text += f"• {exp['service']}\n"
                        farmer_text += f"• 📞 {exp['phone']}\n\n"

                        if len(suitable_expeditors) > 1:
                            farmer_text += f"<i>...и ещё {len(suitable_expeditors) - 1} экспедиторов</i>\n\n"

                    farmer_text += "💡 <b>Вы можете связаться с подрядчиками напрямую или через экспортёра.</b>"

                    await bot.send_message(
                        farmer_id,
                    )
                    logging.info(
                        f"✅ Фермеру {farmer_id} отправлено уведомление о сборе пулла #{pull_id}"
                    )
                except Exception as e:
                    logging.error(f"Ошибка уведомления фермера {farmer_id}: {e}")

    # Уведомляем логистов
    for logist in suitable_logistics:
        try:
            text = "📢 <b>Ваш тариф передан экспортеру!</b>\n\n"
            text += f"Пулл #{pull_id}\n"
            text += f"🚢 Порт: {pull_port}\n\n"
            text += "Экспортер может связаться с вами для обсуждения условий перевозки."

            await bot.send_message(logist["logist_id"], text, parse_mode="HTML")
            logging.info(
                f"✅ Логисту {logist['logist_id']} отправлено уведомление о пулле #{pull_id}"
            )
        except Exception as e:
            logging.error(f"Ошибка уведомления логиста: {e}")

    # Уведомляем экспедиторов
    for exp in suitable_expeditors:
        try:
            text = "📢 <b>Ваши услуги переданы экспортеру!</b>\n\n"
            text += f"Пулл #{pull_id}\n"
            text += f"🚢 Порт: {pull_port}\n\n"
            text += "Экспортер может связаться с вами для оформления документов."

            await bot.send_message(exp["expeditor_id"], text, parse_mode="HTML")
            logging.info(
                f"✅ Экспедитору {exp['expeditor_id']} отправлено уведомление о пулле #{pull_id}"
            )
        except Exception as e:
            logging.error(f"Ошибка уведомления экспедитора: {e}")


async def on_startup(dp):
    logging.info("🚀 Бот Exportum запущен")

    load_data()
    load_deliveries()
    load_logistic_ratings()

    # ✅ ЗАГРУЗКА ЗАЯВОК ПРИ СТАРТЕ
    await load_requests_from_file()

    migrate_all_existing_pulls()
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    # ✅ МИГРАЦИЯ СТАРЫХ ПУЛОВ
    logging.info("🔄 Проверка и миграция старых пулов...")
    try:
        migrate_old_pulls()
        logging.info("✅ Миграция пулов завершена")
    except Exception as e:
        logging.error(f"❌ Ошибка миграции пулов: {e}")

    # ============================================================
    # 🔍 ДОБАВИТЬ ДИАГНОСТИКУ ЗДЕСЬ - ПРЯМО ПЕРЕД ПЛАНИРОВЩИКОМ
    # ============================================================
    logging.info("=" * 70)
    logging.info("🔍 ДИАГНОСТИКА ДАННЫХ ПУЛОВ")
    logging.info("=" * 70)

    logging.info(f"📊 Тип данных pulls: {type(pulls)}")
    logging.info(f"📊 Количество пулов: {len(pulls)}")

    if isinstance(pulls, dict):
        logging.info(f"📊 Ключи пулов: {list(pulls.keys())}")
        logging.info(
            f"📊 Тип ключей: {type(list(pulls.keys())[0]) if pulls else 'нет данных'}"
        )
    else:
        logging.info(f"📊 pulls является List, элементов: {len(pulls)}")

    # Показываем первый элемент
    try:
        if isinstance(pulls, dict):
            first_key = list(pulls.keys())[0]
            first_pull = pulls[first_key]
        else:
            first_key = 0
            first_pull = pulls[0]

        logging.info(f"\n📌 ПЕРВЫЙ ЭЛЕМЕНТ (ключ={first_key}):")
        logging.info(f"   Тип: {type(first_pull)}")
        logging.info(
            f"   'id' в данных: {'id' in first_pull if isinstance(first_pull, dict) else 'N/A'}"
        )
        logging.info(
            f"   Ключи: {list(first_pull.keys()) if isinstance(first_pull, dict) else 'N/A'}"
        )
        logging.info(f"   Содержимое: {first_pull}")
    except Exception as e:
        logging.error(f"❌ Ошибка при анализе первого элемента: {e}")

    logging.info("=" * 70 + "\n")
    # ============================================================

    # Настройка планировщика и обновление кэшей
    await setup_scheduler()

    try:
        await update_prices_cache()
        await update_news_cache()
        await schedule_weekly_reports()
        logging.info("✅ Данные обновлены при запуске")
    except Exception as e:
        logging.error(f"❌ Ошибка обновления данных: {e}")

    # Автопоиск совпадений
    try:
        matches_found = await auto_match_batches_and_pulls()
        logging.info(f"✅ Автопоиск при запуске: найдено {matches_found} совпадений")
    except Exception as e:
        logging.error(f"❌ Ошибка автопоиска: {e}")


def validate_integration():
    """Проверка полноты интеграции"""
    required_functions = [
        "load_users_from_json",
        "save_users_to_json",
        "load_batches_from_pickle",
        "save_batches_to_pickle",
        "load_pulls_from_pickle",
        "save_pulls_to_pickle",
        "update_prices_cache",
        "update_news_cache",
        "auto_match_batches_and_pulls",
        "find_matching_batches",
        "find_matching_exporters",
        "notify_match",
    ]

    missing = []
    for func in required_functions:
        if not globals().get(func):
            missing.append(func)

    if missing:
        logging.warning(f"⚠️ Отсутствуют функции: {', '.join(missing)}")
    else:
        logging.info("✅ Все необходимые функции присутствуют")


validate_integration()

logging.info("🎉 Интеграция Exportum завершена!")
logging.info("📋 Доступные функции:")
logging.info("   ✅ Расширенная регистрация с ИНН и реквизитами")
logging.info("   ✅ Расширенные партии с качеством и хранением")
logging.info("   ✅ Автоматический матчинг партий и пулов")
logging.info("   ✅ Уведомления о совпадениях")
logging.info("   ✅ Расширенный поиск по критериям")
logging.info("   ✅ Заявки на логистику")
logging.info("   ✅ Работа с файлами и документами")
logging.info("   ✅ Управление сделками")
logging.info("   ✅ Редактирование и удаление")
logging.info("   ✅ Интеграция с Google Sheets")
logging.info("   ✅ Планировщик задач")


@dp.message_handler(lambda m: m.text == "📦 Доступные партии", state="*")
async def show_available_batches_exporter(message: types.Message, state: FSMContext):
    """Просмотр доступных партий для экспортера"""
    await state.finish()

    user_id = message.from_user.id

    logging.info(f"📦 Обработчик 'Доступные партии' вызван пользователем {user_id}")

    # ✅ ПРОВЕРКА РОЛИ
        logging.warning(f"❌ Пользователь {user_id} не является экспортёром")
        await message.answer("⚠️ Эта функция доступна только экспортёрам.")
        return

    # ✅ ПРАВИЛЬНЫЙ СБОР ДОСТУПНЫХ ПАРТИЙ
    available = []
    for farmer_id, farmer_batches in batches.items():  # ← ПРАВИЛЬНОЕ ИМЯ ПЕРЕМЕННОЙ
        for batch in farmer_batches:  # ← ПЕРЕБИРАЕМ farmer_batches, НЕ user_batches
            # ✅ Проверяем статус партии
            if batch.get("status") in ["active", "Активна", "available", "доступна"]:
                available.append(
                    {"batch": batch, "farmer_id": farmer_id, "farmer_name": farmer_name}
                )

    logging.info(f"📦 Найдено доступных партий: {len(available)}")

    # ✅ ЕСЛИ НЕ НАЙДЕНО ПАРТИЙ
    if not available:
        await message.answer(
            "📦 <b>Доступные партии</b>\n\n"
            "❌ На данный момент нет доступных партий от фермеров.\n\n"
            "💡 Подождите, пока фермеры добавят свои партии.",
            parse_mode="HTML",
        )
        return

    # ✅ ФОРМИРУЕМ СООБЩЕНИЕ
    text = "📦 <b>Доступные партии от фермеров</b>\n\n"
    text += f"Всего: {len(available)} партий\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

    # ✅ ПОКАЗЫВАЕМ ПЕРВЫЕ 10 ПАРТИЙ
    for i, item in enumerate(available[:10], 1):
        batch = item["batch"]
        farmer_name = item["farmer_name"]

        text += f"{i}. <b>{batch['culture']}</b> - {batch['volume']} т\n"
        text += (
            f"   💰 {batch['price']:,.0f} ₽/т | 📍 {batch.get('region', 'Не указан')}\n"
        )
        text += f"   👤 {farmer_name}\n\n"

        keyboard.add(
            InlineKeyboardButton(
                f"🌾 {batch['culture']} - {batch['volume']} т",
                callback_data=f"view_batch:{batch['id']}",
            )
        )

    # ✅ ЕСЛИ ЕЩЕ ЕСТЬ ПАРТИИ
    if len(available) > 10:
        text += f"... и ещё {len(available) - 10} партий\n\n"
        text += "💡 Используйте '🔍 Расширенный поиск' для фильтрации"

    keyboard.add(
        InlineKeyboardButton(
            "🔍 Расширенный поиск", callback_data="advanced_batch_search"
        )
    )
    keyboard.add(InlineKeyboardButton("◀️ Меню", callback_data="back_to_exporter_menu"))

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


async def on_shutdown(dp):
    """Завершение работы бота"""
    logging.info("⏹ Бот Exportum останавливается...")

    # ✅ СОХРАНЯЕМ ВСЕ ДАННЫЕ
    save_users_to_json()
    save_users_to_pickle()
    save_pulls_to_pickle()  # ✅ СОХРАНЯЕМ ОДИН РАЗ!
    save_shipping_requests()
    save_batches_to_pickle()
    save_logistic_offers()
    save_deliveries()
    save_expeditor_offers()

    # ✅ ДОБАВИТЬ ЭТУ СТРОКУ - СОХРАНЕНИЕ ЗАЯВОК:
    save_requests_to_file()

    logging.info("✅ Данные сохранены")

    await bot.close()
    await dp.storage.close()
    await dp.storage.wait_closed()


# ══════════════════════════════════════════════════════════════════════════
# ПРОСМОТР ДЕТАЛЕЙ ДОСТАВКИ
# ═══════════════════════════════════════════════════════════════════════════
async def view_delivery_details(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    try:
    except (IndexError, ValueError):
        return

        await callback.answer("❌ Доставка не найдена", show_alert=True)
        return


    text = (


    keyboard = InlineKeyboardMarkup(row_width=1)
                keyboard.add(
                    InlineKeyboardButton(
                    )
                )
                keyboard.add(
                    InlineKeyboardButton(
                    )
                )
                keyboard.add(
        )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("complete_delivery:"), state="*")
async def complete_delivery(callback: types.CallbackQuery):
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Доставка не найдена", show_alert=True)
        return

    # Обновляем статус
                )


        try:
            await bot.send_message(
                f"Логист завершил доставку по заявке #{request_id}.\n"
                f"Груз доставлен: {request.get('route_to', 'пункт назначения')}.",
                parse_mode="HTML",
            )
            logging.info(
            )
        except Exception as e:

    await callback.answer("✅ Доставка завершена!", show_alert=True)

    # Обновляем сообщение
    text = (
        f"✅ <b>Доставка #{offer_id} завершена!</b>\n\n"
    )

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🔙 К списку доставок", callback_data="back_to_deliveries")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query_handler(
    lambda c: c.data in ["back_to_deliveries", "refresh_deliveries"], state="*"
)
async def back_to_deliveries_handler(callback: types.CallbackQuery, state: FSMContext):
    """Вернуться к списку доставок"""


@dp.message_handler(
    lambda m: m.text in ["💼 Мои логистических услуги", "💼 Мои логистических услуг"],
    state="*",
)
async def logistics_services_stats_handler(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

        await message.answer("❌ Доступно только для логистов")
        return

    await message.answer(
        "💼 <b>Статистика услуг</b>\n\n"
        "📊 <b>Перевозки:</b> 0\n"
        "💰 <b>Заработано:</b> 0 ₽\n"
        "⭐ <b>Рейтинг:</b> нет отзывов\n\n"
        "Данные обновляются автоматически",
        parse_mode="HTML",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ: НОВОСТИ, ЦЕНЫ И ПОИСК
# ═══════════════════════════════════════════════════════════════════════════════


@dp.message_handler(lambda message: message.text == "📊 Новости и цены", state="*")
async def show_news_and_prices(message: types.Message, state: FSMContext):
    """Отображение новостей и цен"""
    user_id = message.from_user.id
        await message.answer("⚠️ Сначала завершите регистрацию.")
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📈 Цены", callback_data="show_prices"),
        InlineKeyboardButton("📰 Новости", callback_data="show_news"),
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu"))

    await message.answer(
        "📊 <b>Новости и цены зернового рынка</b>\n\nВыберите раздел:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.callback_query_handler(lambda c: c.data == "show_prices", state="*")
async def callback_show_prices(callback_query: types.CallbackQuery):
    """Показать цены"""
    await bot.answer_callback_query(callback_query.id)
    try:

        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="show_prices"))
        keyboard.add(
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_news_menu")
        )

        await bot.edit_message_text(
            message_text,
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
        )
    except MessageNotModified:
        pass
    except Exception as e:
        logging.error(f"Ошибка показа цен: {e}")


@dp.callback_query_handler(lambda c: c.data == "show_news", state="*")
async def callback_show_news(callback_query: types.CallbackQuery):
    """Показать новости"""
    await bot.answer_callback_query(callback_query.id)
    try:

        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="show_news"))
        keyboard.add(
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_news_menu")
        )

        await bot.edit_message_text(
            message_text,
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
        )
    except MessageNotModified:
        pass
    except Exception as e:
        logging.error(f"Ошибка показа новостей: {e}")


@dp.callback_query_handler(lambda c: c.data == "back_to_news_menu", state="*")
async def callback_back_to_news_menu(callback_query: types.CallbackQuery):
    """Вернуться в меню новостей"""
    await bot.answer_callback_query(callback_query.id)

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📈 Цены", callback_data="show_prices"),
        InlineKeyboardButton("📰 Новости", callback_data="show_news"),
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu"))

    try:
        await bot.edit_message_text(
            "📊 <b>Новости и цены зернового рынка</b>\n\nВыберите раздел:",
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except MessageNotModified:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# ПРОСМОТР ДЕТАЛЕЙ ПАРТИИ (CALLBACK)
# ═══════════════════════════════════════════════════════════════════════════


async def view_batch_details_direct(
    message: types.Message, batch_id: str, user_id: int
):
    """Показать детали партии"""
    logging.info(f"🔍 Ищем партию batch_id={batch_id}, user_id={user_id}")

    # ✅ ПРОВЕРКА ТИПА ДАННЫХ
    if not isinstance(batches, dict):
        logging.error(f"❌ batches имеет неправильный тип: {type(batches)}")
        await message.answer("❌ Ошибка загрузки партий")
        return

    batch = None

    # ✅ ПРАВИЛЬНЫЙ ПОИСК ПАРТИИ
    for b in user_batches:
            batch = b
            break

    if not batch:
        await message.answer("❌ Партия не найдена")
        return

    active_matches = [
        m
        for m in matches.values()
    ]

    text = f"""
📦 <b>Партия #{batch['id']}</b>

🌾 Культура: {batch['culture']}
📍 Регион: {batch.get('region', 'Не указан')}
📦 Объём: {batch['volume']} т
💰 Цена: {batch['price']:,.0f} ₽/т
💧 Влажность: {batch.get('humidity', 'Не указано')}%
🌾 Сорность: {batch.get('impurity', 'Не указано')}%
⭐ Класс: {batch.get('quality_class', 'Не указано')}
🏭 Хранение: {batch.get('storage_type', 'Не указано')}
📅 Готовность: {batch.get('readiness_date', 'Не указано')}
📊 Статус: {batch.get('status', 'Активна')}
📅 Создано: {batch.get('created_at', 'Неизвестно')}
"""

    if active_matches:
        text += f"\n🎯 <b>Активных совпадений: {len(active_matches)}</b>"

    if batch.get("files"):
        text += f"\n📎 Прикреплено файлов: {len(batch['files'])}"

    keyboard = batch_actions_keyboard(batch_id)

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query_handler(lambda c: c.data == "back_to_my_batches", state="*")
async def back_to_my_batches(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку партий с фильтрацией по статусам"""
    await state.finish()

    user_id = callback.from_user.id

    # Проверка роли
        await callback.answer(
            "❌ Эта функция доступна только фермерам", show_alert=True
        )
        return

    # Получаем партии пользователя

    if not user_batches:
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
        )
        await callback.message.edit_text(
            "📦 У вас пока нет добавленных партий.\n\n"
            "Используйте кнопку '➕ Добавить партию' для создания новой.",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        await callback.answer()
        return

    # Фильтруем партии по статусам
    active_batches = [
    ]
    reserved_batches = [
    ]
    withdrawn_batches = [
    ]

    # Считаем партии с совпадениями
    matched_batches = [
        b
        for b in active_batches
        if any(
            for m in matches.values()
        )
    ]

    # Главное меню со статистикой и кнопками фильтров
    keyboard = InlineKeyboardMarkup(row_width=1)

    keyboard.add(
        InlineKeyboardButton(
            f"✅ Активные ({len(active_batches)})",
            callback_data="filter_batches:active",
        ),
        InlineKeyboardButton(
            f"🔒 Зарезервированные ({len(reserved_batches)})",
            callback_data="filter_batches:reserved",
        ),
        InlineKeyboardButton(
            f"💰 Проданные ({len(sold_batches)})", callback_data="filter_batches:sold"
        ),
        InlineKeyboardButton(
            f"❌ Снятые ({len(withdrawn_batches)})",
            callback_data="filter_batches:withdrawn",
        ),
    )

    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    message_text = (
        f"📦 <b>Ваши партии</b> ({len(user_batches)} шт.)\n\n"
        f"✅ Активные: {len(active_batches)}\n"
        f"🔒 Зарезервированные: {len(reserved_batches)}\n"
        f"💰 Проданные: {len(sold_batches)}\n"
        f"❌ Снятые с продажи: {len(withdrawn_batches)}\n"
        f"🎯 С совпадениями: {len(matched_batches)}\n\n"
        "Выберите статус для просмотра партий:"
    )

    try:
        await callback.message.edit_text(
            message_text, reply_markup=keyboard, parse_mode="HTML"
        )
    except MessageNotModified:
        pass

    await callback.answer()


# Обработчик фильтрации партий по статусам
@dp.callback_query_handler(lambda c: c.data.startswith("filter_batches:"), state="*")
async def filter_batches(callback: types.CallbackQuery, state: FSMContext):
    """Фильтрация и показ партий по статусам"""
    await state.finish()

    user_id = callback.from_user.id

    # Получаем партии пользователя

    status_map = {
    }


    keyboard = InlineKeyboardMarkup(row_width=1)

    if filtered_batches:
        # Добавляем кнопки партий
        for batch in filtered_batches:
            culture = batch.get("culture", "?")
            volume = batch.get("volume", 0)
            price = batch.get("price", 0)
            batch_id = batch.get("id", "unknown")
            button_text = (
                f"{title.split()[0]} {culture} - {volume} т ({price:,.0f} ₽/т)"
            )
            keyboard.add(
                InlineKeyboardButton(
                    button_text, callback_data=f"view_batch:{batch_id}"
                )
            )
    else:
        await callback.message.edit_text(
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("← Назад", callback_data="back_to_my_batches")
            ),
            parse_mode="HTML",
        )
        await callback.answer()
        return

    # Добавляем кнопку возврата
    keyboard.add(InlineKeyboardButton("← Назад", callback_data="back_to_my_batches"))

    message_text = f"📦 <b>{title}</b> ({len(filtered_batches)} шт.)\n\nВыберите партию для просмотра:"

    try:
        await callback.message.edit_text(
            message_text, reply_markup=keyboard, parse_mode="HTML"
        )
    except MessageNotModified:
        pass

    await callback.answer()


@dp.callback_query_handler(text="back_to_requests", state="*")
async def back_to_requests(callback: types.CallbackQuery, state: FSMContext):
    """Вернуться к ПОЛНОМУ списку активных заявок (экспортёры + фермеры)"""
    await state.finish()

    try:
        await callback.answer()
    except Exception as e:
        logging.warning(f"Callback answer error: {e}")

    # 🔄 СОБИРАЕМ ВСЕ АКТИВНЫЕ ЗАЯВКИ
    all_requests = []

    # 📊 DEBUG: Проверяем, что есть в хранилищах
    logging.info(f"🔍 DEBUG: shipping_requests ({len(shipping_requests)} всего)")
    for req_id, req in shipping_requests.items():
        status = req.get("status")
        logging.info(f"   - ID {req_id}: status='{status}'")
            all_requests.append(
                {
                    "source": "exporter",
                    "culture": req.get("culture", "—"),
                    "volume": req.get("volume", 0),
                    "created": req.get("created_at", "—"),
                }
            )

    logging.info(
    )
    for req_id, req in farmer_logistics_requests.items():
        status = req.get("status")
        logging.info(f"   - ID {req_id}: status='{status}'")
            all_requests.append(
                {
                    "source": "farmer",
                    "culture": req.get("culture", "—"),
                    "volume": req.get("volume", 0),
                    "created": req.get("created_at", "—"),
                }
            )

    logging.info(f"✅ ИТОГО активных заявок: {len(all_requests)}")
    for req in all_requests:
        logging.info(f"   - {req['source'].upper()}: #{req['id']} ({req['culture']})")

    # 📊 ФОРМИРУЕМ ТЕКСТ
    text = f"""🚚 <b>Активные заявки</b>


📋 Всего заявок: {len(all_requests)}


Выберите заявку для просмотра деталей и создания предложения:
"""

    # 🎯 КНОПКИ С ЗАЯВКАМИ
    keyboard = InlineKeyboardMarkup(row_width=1)

    if not all_requests:
        text += "\n\n❌ <b>Активных заявок не найдено</b>"
    else:
        for req in all_requests:
                emoji = "🚛"
            else:

            label = f"{emoji} {req['culture']} • {req['volume']:.0f}т"
                keyboard.add(
                    InlineKeyboardButton(
                    )
                )
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error editing message in back_to_requests: {e}")
        try:
            await callback.message.answer(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
        except Exception as e2:
            logging.error(f"Error sending new message: {e2}")


@dp.callback_query_handler(lambda c: c.data == "back_to_exporter_menu", state="*")
async def back_to_exporter_menu(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в меню экспортёра"""
    await state.finish()

    user_id = callback.from_user.id

        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return

    name = user.get("name", "Экспортёр")

    try:
        await callback.message.delete()
    except Exception as e:

    keyboard = exporter_keyboard()

    await callback.message.answer(
        f"👋 С возвращением, {name}!\n\n📦 <b>Меню экспортёра</b>",
        reply_markup=keyboard,
        parse_mode="HTML",
    )

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "advanced_batch_search", state="*")
async def advanced_batch_search_handler(
    callback: types.CallbackQuery, state: FSMContext
):
    """Расширенный поиск партий"""
    await callback.message.edit_text(
        "🔍 <b>Расширенный поиск партий</b>\n\n" "Выберите критерий поиска:",
        reply_markup=search_criteria_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("contact_farmer_"), state="*")
async def contact_farmer_handler(callback: types.CallbackQuery, state: FSMContext):
    """Контакт с фермером"""
    try:
        parts = callback.data.split("_")
        farmer_id = int(parts[2])
        batch_id = int(parts[3])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

    if not farmer:
        await callback.answer("❌ Фермер не найден", show_alert=True)
        return

    farmer_name = farmer.get("name", "Неизвестно")
    farmer_phone = farmer.get("phone", "Не указан")

    await callback.answer(
        f"👤 Фермер: {farmer_name}\n" f"📞 Телефон: {farmer_phone}", show_alert=True
    )

    # Уведомляем фермера о заинтересованности
    try:
        await bot.send_message(
            farmer_id,
            f"👤 {exporter_name} заинтересовался вашей партией #{batch_id}\n\n"
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"Ошибка уведомления фермера: {e}")


@dp.message_handler(lambda message: message.text == "🔍 Поиск", state="*")
async def start_search(message: types.Message, state: FSMContext):
    """Начать поиск партий"""
    user_id = message.from_user.id
        await message.answer("⚠️ Сначала завершите регистрацию.")
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 По культуре", callback_data="search_by_culture"),
        InlineKeyboardButton("📍 По региону", callback_data="search_by_region"),
    )
    keyboard.add(
        InlineKeyboardButton("💰 По цене", callback_data="search_by_price"),
        InlineKeyboardButton("📦 По объему", callback_data="search_by_volume"),
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu"))

    await message.answer(
        "🔍 <b>Поиск партий зерна</b>\n\nВыберите критерий поиска:",
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.callback_query_handler(lambda c: c.data == "search_by_culture")
async def callback_search_by_culture(
    callback_query: types.CallbackQuery, state: FSMContext
):
    """Поиск по культуре"""  # ← DOCSTRING НА ПРАВИЛЬНОМ МЕСТЕ

    # Устанавливаем state ОДИН РАЗ
    await SearchByCulture.waiting_culture.set()

    # Отвечаем на callback
    await bot.answer_callback_query(callback_query.id)

    # Создаём клавиатуру
    keyboard = culture_keyboard()

    try:
        await bot.edit_message_text(
            "🌾 <b>Выберите культуру:</b>",
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except MessageNotModified:
        pass


@dp.callback_query_handler(lambda c: c.data == "search_by_region", state="*")
async def callback_search_by_region(callback_query: types.CallbackQuery):
    """Поиск по региону"""
    await bot.answer_callback_query(callback_query.id)
    try:
        await bot.edit_message_text(
            "📍 <b>Выберите регион:</b>",
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode="HTML",
        )
    except MessageNotModified:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИИ ПУБЛИКАЦИИ В КАНАЛ И ОТЧЕТЫ
# ═══════════════════════════════════════════════════════════════════════════════
async def publish_pull_to_channel(pull_data):
    """Публикация пулла в Telegram канал"""
    try:
        message_text = f"""🌐 <b>НОВЫЙ ПУЛЛ</b>

🌾 Культура: {pull_data.get('culture', 'Не указано')}
📦 Объем: {pull_data.get('target_volume', 0):,.0f} тонн
💰 Цена: {pull_data.get('price', 0):,.0f} ₽/т
🚢 Порт: {pull_data.get('port', 'Не указано')}

📊 Требования к качеству:
• Влажность: до {pull_data.get('moisture', 0)}%
• Натура: от {pull_data.get('nature', 0)} г/л
• Примесь: до {pull_data.get('impurity', 0)}%

🆔 ID пулла: {pull_data.get('id', 'N/A')}
📅 Создан: {pull_data.get('created_at', 'Не указано')}

💬 Для участия напишите боту
"""
        await bot.send_message(CHANNEL_ID, message_text, parse_mode="HTML")
        logging.info(f'✅ Пулл {pull_data.get("id")} опубликован в канал')
    except Exception as e:
        logging.error(f"❌ Ошибка публикации пулла в канал: {e}")


async def publish_batch_to_channel(batch_data, farmer_name):
    """Публикация партии в Telegram канал"""
    try:
        message_text = f"""📦 <b>НОВАЯ ПАРТИЯ</b>

👤 Фермер: {farmer_name}
🌾 Культура: {batch_data.get('culture', 'Не указано')}
📦 Объем: {batch_data.get('volume', 0):,.0f} тонн
💰 Цена: {batch_data.get('price', 0):,.0f} ₽/т
📍 Регион: {batch_data.get('region', 'Не указано')}

📊 Качество:
• Влажность: {batch_data.get('humidity', 0)}%
• Примесь: {batch_data.get('impurity', 0)}%
• Класс: {batch_data.get('quality_class', 'Не указано')}

🆔 ID партии: {batch_data.get('id', 'N/A')}

💬 Для покупки напишите боту
"""
        await bot.send_message(CHANNEL_ID, message_text, parse_mode="HTML")
        logging.info(f'✅ Партия {batch_data.get("id")} опубликована в канал')
    except Exception as e:
        logging.error(f"❌ Ошибка публикации партии в канал: {e}")


async def generate_weekly_report():
    """Генерация еженедельного отчета"""
    try:
        farmers_count = len([u for u in users.values() if u.get("role") == "farmer"])
        exporters_count = len(
            [u for u in users.values() if u.get("role") == "exporter"]
        )
        logistics_count = len(
        )
        expeditors_count = len(
        )

        total_deals = len(deals)

        total_batch_volume = 0
            total_batch_volume += batch.get("volume", 0)

        prices = []
            if batch.get("price"):
                prices.append(batch["price"])

        avg_price = sum(prices) / len(prices) if prices else 0

        report_text = f"""📊 <b>ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ</b>
{'='*40}

👥 <b>ПОЛЬЗОВАТЕЛИ:</b>
• Фермеры: {farmers_count}
• Экспортеры: {exporters_count}
• Логисты: {logistics_count}
• Экспедиторы: {expeditors_count}
• <b>Всего: {len(users)}</b>

📦 <b>ПАРТИИ:</b>
• Всего партий: {total_batches}
• Общий объем: {total_batch_volume:,.0f} тонн
• Средняя цена: {avg_price:,.0f} ₽/т

🌐 <b>ПУЛЛЫ:</b>
• Всего пуллов: {total_pulls}

🤝 <b>СДЕЛКИ:</b>
• Завершено сделок: {total_deals}

📅 Дата отчета: {datetime.now().strftime('%d.%m.%Y %H:%M')}
"""


        try:
            await bot.send_message(CHANNEL_ID, report_text, parse_mode="HTML")
        except Exception as e:

        logging.info("✅ Еженедельный отчет отправлен")
    except Exception as e:
        logging.error(f"❌ Ошибка генерации отчета: {e}")


async def schedule_weekly_reports():
    """Запуск scheduler для еженедельных отчетов"""
    try:
        scheduler.add_job(
            generate_weekly_report, "cron", day_of_week="mon", hour=9, minute=0
        )
        logging.info("✅ Scheduler запущен: еженедельные отчеты активны")
    except Exception as e:
        logging.error(f"❌ Ошибка запуска scheduler: {e}")


@dp.callback_query_handler(lambda c: c.data == "reload_data", state="*")
async def reload_data_callback(callback: CallbackQuery, state: FSMContext):
    """Перезагрузка данных из файлов"""
    await state.finish()

    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    try:
        load_data()
        await callback.answer("✅ Данные успешно перезагружены", show_alert=True)
        logging.info("Данные перезагружены администратором")
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
        logging.error(f"Ошибка перезагрузки данных: {e}")


# === HANDLER: Присоединение партий к пуллу ===
@dp.callback_query_handler(
    lambda c: c.data and c.data.startswith("selectbatch_"), state="*"
)
async def process_batch_selection_for_pull(
    callback_query: CallbackQuery, state: FSMContext
):
    """Обработка выбора партии для добавления в пулл"""
    try:
        batch_id = callback_query.data.split("_")[1]
        user_id = callback_query.from_user.id

        logging.info(f"Выбор партии {batch_id}")

        data = await state.get_data()
        pull_id = data.get("pull_id")

        if not pull_id:
            await callback_query.answer("❌ Пулл не найден", show_alert=True)
            return

        load_batches_from_pickle()

        if not batch:
            await callback_query.answer("❌ Партия не найдена", show_alert=True)
            return

            await callback_query.answer("❌ Не ваша партия", show_alert=True)
            return

        if not pull:
            await callback_query.answer("❌ Пулл не найден", show_alert=True)
            return

        if "batches" not in pull:
            pull["batches"] = []

            await callback_query.answer("⚠️ Уже добавлена", show_alert=True)
            return

        current_volume = pull.get("current_volume", 0)
        pull["current_volume"] = current_volume + batch.get("volume", 0)


        await callback_query.answer("✅ Партия добавлена!", show_alert=True)

        logging.info(f"✅ Партия {batch_id} добавлена в пулл {pull_id}")

        try:
                gs.sync_pull_to_sheets(pull_id, pull)
        except Exception as e:
            logging.debug(f"Синхронизация: {e}")

    except Exception as e:
        logging.error(f"Ошибка: {e}")
        await callback_query.answer("❌ Ошибка", show_alert=True)


# === БЕЗОПАСНЫЕ ФУНКЦИИ ===


async def safe_notify_exporter(pull, batch):
    """Безопасное уведомление экспортёра о новой партии"""
    try:
        if not pull or not isinstance(pull, dict):
            logging.debug("Пулл не указан")
            return

        exporter_id = pull.get("exporter_id")
        if not exporter_id:
            logging.debug("У пулла нет exporter_id")
            return

            logging.debug(f"Экспортёр {exporter_id} не найден")
            return

        message = f"🔔 Новая партия!\n{batch.get('culture')} - {batch.get('volume')} т"
        await bot.send_message(exporter_id, message)
        logging.info(f"✅ Экспортёр {exporter_id} уведомлён")

    except Exception as e:
        logging.debug(f"Уведомление: {e}")


async def safe_publish_to_channel(batch):
    """Безопасная публикация в канал"""
    try:
        channel_id = os.getenv("CHANNEL_ID")
        if not channel_id:
            logging.debug("CHANNEL_ID не настроен")
            return

        message = f"🌾 Новая партия!\n{batch.get('culture')} - {batch.get('volume')} т"
        await bot.send_message(channel_id, message)
        logging.info("✅ Опубликовано в канале")

    except Exception as e:
        if "Chat not found" in str(e):
            logging.debug("Канал не найден (норма)")
        else:
            logging.debug(f"Публикация: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ ОСНОВНЫХ КНОПОК МЕНЮ (ДОБАВЛЕНЫ)
# ═══════════════════════════════════════════════════════════════════════════
@dp.message_handler(lambda m: m.text == "📋 Мои пуллы", state="*")
async def show_my_pulls_farmer(message: types.Message, state: FSMContext):
    """Показать пуллы в которых участвует фермер"""
    user_id = message.from_user.id

        await message.answer("❌ Пользователь не найден")
        return

    my_pulls = []

        )

    if not my_pulls:
        await message.answer(
            "📋 Вы пока не участвуете ни в одном пулле\n\n"
            "Используйте '🔍 Найти пулл' чтобы найти подходящие пуллы",
            parse_mode="Markdown",
        )
        return

    msg = "📋 *Пуллы в которых вы участвуете:*\n\n"

    for i, item in enumerate(my_pulls, 1):
        pull = item["pull"]
        batch = item["batch"]

        status_emoji = {"open": "🟢", "filling": "🟡", "closed": "🔴"}.get(
        )
        msg += f"   Порт: {pull.get('port', 'не указан')}\n"

    await message.answer(msg, parse_mode="Markdown")


@dp.message_handler(lambda m: m.text == "➕ Создать партию", state="*")
async def create_batch_start(message: types.Message, state: FSMContext):
    """Начать создание партии"""
    user_id = message.from_user.id

        await message.answer("❌ Эта функция доступна только фермерам")
        return

    await message.answer(
        "🌾 *Создание новой партии*\n\n" "Укажите культуру:",
        reply_markup=culture_keyboard(),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════════════════
# ДОПОЛНИТЕЛЬНАЯ ФУНКЦИЯ: /debug для проверки аккаунта
# ═══════════════════════════════════════════════════════════════════════════


@dp.message_handler(commands=["debug"], state="*")
async def debug_account(message: types.Message):
    """Показать информацию о своём аккаунте для отладки"""
    user_id = message.from_user.id

    info = []
    info.append("👤 *Информация об аккаунте*\n")
    info.append(f"User ID: `{user_id}`\n")

    # Проверяем users
    if user_id in users:
        info.append("✅ Найден в памяти (users)")
        info.append(f"   Роль: {user_data.get('role', 'не указана')}")
        info.append(f"   Телефон: {user_data.get('phone', 'не указан')}")
        info.append(f"   Регион: {user_data.get('region', 'не указан')}\n")
    else:
        info.append("❌ Не найден в памяти (users)\n")

    # Проверяем batches
    if user_id in batches:
        batch_count = len(batches[user_id])
        info.append(f"📦 Партий фермера: {batch_count}\n")


    # Проверяем Google Sheets
    try:  # ← ПРАВИЛЬНЫЙ ОТСТУП!
        if gs and gs.spreadsheet:
            worksheet = gs.spreadsheet.worksheet("Users")
            cell = worksheet.find(str(user_id))
            if cell:
                info.append(f"✅ Найден в Google Sheets (строка {cell.row})")
            else:
                info.append("❌ Не найден в Google Sheets")
        else:
            info.append("⚠️ Google Sheets не подключён")
    except Exception as e:
        info.append(f"❌ Ошибка проверки Google Sheets: {e}")

    await message.answer("\n".join(info), parse_mode="Markdown")


# ============================================================================
# СИСТЕМА ЛОГИСТИЧЕСКИХ ЗАЯВОК
# ============================================================================
# -------------------- ЭКСПОРТЁР: СОЗДАНИЕ ЗАЯВКИ --------------------
async def create_logistics_request_start(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

        await message.answer("❌ Эта функция доступна только экспортёрам")
        return

    true_pulls = None
    if (
        isinstance(pulls, dict)
        and "pulls" in pulls
        and isinstance(pulls["pulls"], dict)
    ):
        true_pulls = pulls["pulls"]
    elif isinstance(pulls, dict):
        true_pulls = pulls
    else:
        true_pulls = {}

    logging.info(f"USER {user_id} requested logistics")
    logging.info(f"ALL PULLS IN SYSTEM: {len(true_pulls)}")

    exporter_pulls = {}
    for pid, p in true_pulls.items():
        exp_id = p.get("exporter_id")
                logging.info(
                )
                exporter_pulls[pid] = p


    if not exporter_pulls:
        )
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    for pull_id, pull in list(exporter_pulls.items())[:10]:
        culture = pull.get("culture", "Неизвестно")
        current_vol = pull.get("current_volume", 0) or 0
        keyboard.add(
        )

    await message.answer(
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@dp.callback_query_handler(
    lambda c: c.data.startswith("create_logistic_req:"), state="*"
)
async def select_pull_for_logistics(callback: types.CallbackQuery, state: FSMContext):
    """Выбор пула для заявки"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    await state.update_data(
        pull_id=pull_id,
        volume=pull.get("current_volume", 0),
        port=pull.get("port", ""),
    )

    await callback.message.edit_text(
        f"Порт: {pull.get('port', '')}\n\n"
        parse_mode="HTML",
    )

    await CreateLogisticRequestStatesGroup.route_from.set()
    await callback.answer()


@dp.message_handler(state=CreateLogisticRequestStatesGroup.route_from)
async def logistics_request_from(message: types.Message, state: FSMContext):
    """Место погрузки"""
    route_from = message.text.strip()
    await state.update_data(route_from=route_from)

    data = await state.get_data()

    await message.answer(
        f"Откуда: <b>{route_from}</b>\n"
        f"Куда: <b>{data.get('port', '')}</b>\n\n"
        parse_mode="HTML",
    )

    await CreateLogisticRequestStatesGroup.loading_date.set()


@dp.message_handler(state=CreateLogisticRequestStatesGroup.loading_date)
async def logistics_request_date(message: types.Message, state: FSMContext):
    """Дата погрузки"""
    loading_date = message.text.strip()

    if not validate_date(loading_date):
        await message.answer(
            "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ (например: 15.12.2025)"
        )
        return

    await state.update_data(loading_date=loading_date)

    # ✅ НОВЫЙ ШАГ: ОЖИДАЕМАЯ ЦЕНА
    data = await state.get_data()

    await message.answer(
        f"Откуда: <b>{data.get('route_from', '')}</b>\n"
        f"Куда: <b>{data.get('port', '')}</b>\n"
        f"Дата: <b>{loading_date}</b>\n\n"
        parse_mode="HTML",
    )

    await CreateLogisticRequestStatesGroup.desired_price.set()


@dp.message_handler(state=CreateLogisticRequestStatesGroup.desired_price)
async def logistics_request_desired_price(message: types.Message, state: FSMContext):
    """Ожидаемая цена доставки"""
    try:
        desired_price = float(message.text.replace(",", ".").replace(" ", ""))

        if desired_price <= 0:
            raise ValueError("Цена должна быть больше 0")

        await state.update_data(desired_price=desired_price)

        await message.answer(
            "🚚 <b>Заявка на логистику</b>\n\n"
            "<b>Шаг 4 из 4</b>\n\n"
            "Дополнительные требования (или /skip для пропуска):",
            parse_mode="HTML",
        )

        await CreateLogisticRequestStatesGroup.notes.set()

        await message.answer(
            "❌ Неверный формат цены. Укажите число (например: 700 или 750.5)"
        )


@dp.message_handler(
    lambda m: m.text == "/skip", state=CreateLogisticRequestStatesGroup.notes
)
@dp.message_handler(state=CreateLogisticRequestStatesGroup.notes)
async def logistics_request_finish(message: types.Message, state: FSMContext):
    """Завершение создания заявки"""
    global logistics_request_counter

    notes = "" if message.text == "/skip" else message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id

    pull_id = data["pull_id"]

    logistics_request_counter += 1

    # ✅ СОЗДАЁМ ЗАЯВКУ С ПОЛЕМ desired_price
    request = {
        "id": logistics_request_counter,
        "pull_id": pull_id,
        "culture": data["culture"],
        "volume": data["volume"],
        "route_from": data["route_from"],
        "route_to": data.get("port", ""),
        "loading_date": data["loading_date"],
        "desired_price": data.get("desired_price", 0),  # ← НОВОЕ!
        "notes": notes,
        "status": "active",
        "offers_count": 0,
        "selected_offer_id": None,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    logistics_requests[logistics_request_counter] = request
    save_logistics_requests_to_pickle()

    await state.finish()

    # ✅ ПОКАЗЫВАЕМ ОЖИДАЕМУЮ ЦЕНУ В ИТОГЕ
    exporter_inn = exporter.get("inn", "Не указан")
    exporter_ogrn = exporter.get("ogrn", "Не указан")
    exporter_phone = exporter.get("phone", "Не указан")
    exporter_email = exporter.get("email", "Не указан")

    summary = (
        f"✅ <b>Заявка на логистику #{logistics_request_counter} создана!</b>\n\n"
        f"📦 Пул: #{pull_id}\n"
        f"🌾 Культура: {data['culture']}\n"
        f"📦 Объем: {data['volume']:.0f} т\n"
        f"📍 Маршрут: {data['route_from']} → {request['route_to']}\n"
        f"📅 Дата: {data['loading_date']}\n"
        f"💰 Ожидаемая цена: <code>{data.get('desired_price', 0):,.0f}</code> ₽/т\n"
    )

    if notes:
        summary += f"📝 Примечания: {notes}\n"

    summary += (
        f"\n👤 Контакт: {exporter.get('name', '')}\n"
        f"📋 ИНН: <code>{exporter_inn}</code>\n"
        f"📋 ОГРН: <code>{exporter_ogrn}</code>\n"
        f"📞 Телефон: <code>{exporter_phone}</code>\n"
        f"📧 Email: <code>{exporter_email}</code>\n\n"
    )

    await message.answer(summary, parse_mode="HTML", reply_markup=exporter_keyboard())

    # Уведомляем логистов
    await notify_logistics_about_new_request(request)

    logging.info(
        f"✅ Logistics request {logistics_request_counter} created by exporter {user_id} "
        f"with desired_price {data.get('desired_price', 0)}"
    )

@dp.callback_query_handler(
    lambda c: c.data.startswith("view_logistics_req:"), state="*"
)
async def view_logistics_request_details(
    callback: types.CallbackQuery, state: FSMContext
):
    """Просмотр деталей заявки логиста"""

    await state.finish()

    try:
        req_id = parse_callback_id(callback.data)
    except ValueError as e:
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    user_id = callback.from_user.id

    already_offered = any(
    )

    # ✅ ПОЛНАЯ ИНФОРМАЦИЯ О ЗАЯВКЕ
    msg = f"""
📋 <b>ЗАЯВКА #{req_id}</b>

<b>🌾 Информация о грузе:</b>
Культура: {req.get('culture', 'Не указана')}
Объем: {req.get('volume', 0):.0f} т

<b>📍 Маршрут доставки:</b>
От: {req.get('route_from', 'Не указано')}
До: {req.get('route_to', 'Не указано')}

<b>📅 Сроки:</b>
Дата погрузки: {req.get('loading_date', 'Не указана')}
Срок доставки: {req.get('delivery_date', 'Не указан')}

<b>💼 Статистика:</b>
Статус: {req.get('status', 'active')}
"""

    if req.get("notes"):
        msg += f"\n<b>📝 Дополнительно:</b>\n{req['notes']}"

    # Информация о заказчике
        msg += f"""

<b>👨‍🌾 Заказчик:</b>
"""

    keyboard = InlineKeyboardMarkup(row_width=1)

        keyboard.add(
            InlineKeyboardButton(
            )
        )
        keyboard.add(
            InlineKeyboardButton("✔️ Вы уже откликнулись", callback_data="noop")
        )

    # Кнопка контакта (если это не ваша заявка)
        keyboard.add(
            InlineKeyboardButton(
                "📞 Позвонить заказчику",
        )
            )
            keyboard.add(
                InlineKeyboardButton(
                )
            )
                    keyboard.add(
                        InlineKeyboardButton(
                        )
                    )

    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("respond_logistics:"), state="*")
async def respond_to_logistics_request(
    callback: types.CallbackQuery, state: FSMContext
):

    try:
        req_id = parse_callback_id(callback.data)
    except ValueError:
        await callback.answer("❌ Ошибка", show_alert=True)
        return



        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
        return



async def logistics_offer_price(message: types.Message, state: FSMContext):
    """Цена за перевозку"""
    try:
        price = float(message.text.strip().replace(",", ".").replace(" ", ""))
        if price <= 0:
            raise ValueError

        await state.update_data(price=price)

        await message.answer(
            f"Цена: <b>{price:,.0f} ₽/т</b>\n\n"
            parse_mode="HTML",
        )


    except ValueError:
        await message.answer("❌ Некорректная цена. Введите положительное число.")


async def logistics_offer_vehicle(message: types.Message, state: FSMContext):
    """Тип транспорта"""
    vehicle_type = message.text.strip()
    await state.update_data(vehicle_type=vehicle_type)

    await message.answer(
        parse_mode="HTML",
    )



async def logistics_offer_days(message: types.Message, state: FSMContext):
    """Срок доставки"""
    try:
        delivery_days = int(message.text.strip())
        if delivery_days <= 0:
            raise ValueError

        await state.update_data(delivery_days=delivery_days)

        await message.answer(
            f"Срок: <b>{delivery_days} дней</b>\n\n"
            parse_mode="HTML",
        )


    except ValueError:
        await message.answer("❌ Некорректный срок. Введите целое число дней.")


# ============================================================================
# 🚛 ЗАВЕРШЕНИЕ ОТКЛИКА ЛОГИСТА НА ЗАЯВКУ
# ============================================================================


@dp.message_handler(
)
async def logistics_offer_finish(message: types.Message, state: FSMContext):
    """Завершение отклика логиста"""
    global logistics_offer_counter

    notes = "" if message.text == "/skip" else message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id


        await message.answer("❌ Заявка не найдена. Попробуйте ещё раз.")
        await state.finish()
        return


    offer = {
        "request_id": req_id,
        "logist_id": user_id,
        "price": data["price"],
        "vehicle_type": data["vehicle_type"],
        "delivery_days": data["delivery_days"],
        "notes": notes,
        "status": "pending",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


    # Обновляем счётчик откликов в заявке
        req["status"] = "has_offers"

    save_logistics_requests_to_pickle()
    save_logistics_offers_to_pickle()

    await state.finish()

    # РАСЧЁТ ИТОГОВОЙ СУММЫ
    total_price = data["price"] * req["volume"]

    # ИТОГОВОЕ СООБЩЕНИЕ ЛОГИСТУ
    summary = (
        f"📋 Заявка: #{req_id}\n"
        f"🌾 {req['culture']} • {req['volume']:.0f} т\n"
        f"📍 {req['route_from']} → {req['route_to']}\n\n"
        f"💰 <b>Ваша цена:</b> <code>{data['price']:,.0f}</code> ₽/т\n"
        f"💵 <b>Общая сумма:</b> <code>{total_price:,.0f}</code> ₽\n"
        f"🚛 <b>Транспорт:</b> {data['vehicle_type']}\n"
        f"⏱ <b>Срок:</b> {data['delivery_days']} дн.\n\n"
    )

    await message.answer(summary, parse_mode="HTML", reply_markup=logistic_keyboard())

    # Уведомляем экспортёра
    await notify_exporter_about_offer(req, offer)

    logging.info(
    )


# ============================================================================
# 🔔 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ УВЕДОМЛЕНИЙ
# ============================================================================


    msg = (
    )

    for logist_id in logistics_users:
        try:
            await bot.send_message(logist_id, msg, parse_mode="HTML")
            logging.info(
            )
        except Exception as e:
            logging.error(f"❌ Error notifying logist {logist_id}: {e}")


    exporter_id = request.get("exporter_id")
    msg = (
    )

    try:
        await bot.send_message(exporter_id, msg, parse_mode="HTML")
        logging.info(
            f"✅ Notified exporter {exporter_id} about offer #{offer.get('id','-')}"
        )
    except Exception as e:
        logging.error(f"❌ Error notifying exporter {exporter_id}: {e}")


# ============================================================================
# 🔙 НАВИГАЦИОННЫЕ ОБРАБОТЧИКИ
# ===========================================================================
@dp.callback_query_handler(lambda c: c.data == "back_to_logistics_requests", state="*")
async def back_to_logistics_requests_handler(
    callback: types.CallbackQuery, state: FSMContext
):
    """Возврат к списку активных заявок"""
    await state.finish()


@dp.callback_query_handler(lambda c: c.data == "noop", state="*")
async def noop_handler(callback: types.CallbackQuery):
    """Пустой обработчик для кнопок без действия"""
    await callback.answer()


# ====================================================================
# ПОИСК ПО КУЛЬТУРЕ
# ====================================================================
@dp.message_handler(Text(equals="🔍 Поиск по культуре"), state="*")
async def start_search_by_culture(message: types.Message, state: FSMContext):
    """Начало поиска партий по культуре"""

    # 1. Очищаем предыдущий state (если был)
    await state.finish()

    # 2. СНАЧАЛА устанавливаем новый state!
    await SearchByCulture.waiting_culture.set()

    # 3. ПОТОМ показываем кнопки
    await message.answer(
        "🔍 <b>Поиск по культуре</b>\n\n" "Выберите культуру для поиска партий:",
        reply_markup=culture_keyboard(),
        parse_mode="HTML",
    )


if "logistics_cards" not in globals():
    logistics_cards = {}
if "expeditor_cards" not in globals():
    expeditor_cards = {}


@dp.message_handler(Text(equals="📋 Моя карточка"), state="*")
async def show_my_card_menu(message: types.Message, state: FSMContext):
    await state.finish()

    user_id = message.from_user.id
        await message.answer("❌ Вы не зарегистрированы. Используйте /start")
        return

    role = user.get("role")

            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(
                InlineKeyboardButton(
                    "✏️ Редактировать", callback_data="edit_logistic_card"
                ),
                InlineKeyboardButton("🗑 Удалить", callback_data="delete_logistic_card"),
            )
        else:
            keyboard.add(
                InlineKeyboardButton(
                    "➕ Создать карточку", callback_data="create_logistic_card"
                )
            )


            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(
                InlineKeyboardButton(
                ),
                InlineKeyboardButton(
                    "🗑 Удалить", callback_data="delete_expeditor_card"
                ),
            )
        else:
            keyboard.add(
                InlineKeyboardButton(
                    "➕ Создать карточку", callback_data="create_expeditor_card"
                )
            )
    else:
        await message.answer("❌ Карточки доступны только логистам и экспедиторам")
        return

    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


# ====================================================================
# СОЗДАНИЕ КАРТОЧКИ ЛОГИСТА
# ====================================================================


@dp.callback_query_handler(lambda c: c.data == "create_logistic_card", state="*")
async def start_create_logistic_card(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.edit_text(
        "🚚 <b>Создание карточки логиста</b>\n\n"
        "Шаг 1/7\n\n"
        "Введите ваши маршруты (например: Краснодар-Новороссийск, Ростов-Азов):",
        parse_mode="HTML",
    )
    await CreateLogisticCardStates.routes.set()
    await callback.answer()


@dp.message_handler(state=CreateLogisticCardStates.routes)
async def process_logistic_routes(message: types.Message, state: FSMContext):
    await message.answer(
        "💰 Шаг 2/7\n\nУкажите цену за километр (руб.):", parse_mode="HTML"
    )
    await CreateLogisticCardStates.price_per_km.set()


@dp.message_handler(state=CreateLogisticCardStates.price_per_km)
async def process_price_per_km(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        await state.update_data(price_per_km=price)
        await message.answer(
        )
        await CreateLogisticCardStates.price_per_ton.set()
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")


@dp.message_handler(state=CreateLogisticCardStates.price_per_ton)
async def process_price_per_ton(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        await state.update_data(price_per_ton=price)
        await message.answer(
            "📦 Шаг 4/7\n\nУкажите минимальный объём перевозки (тонн):",
            parse_mode="HTML",
        )
        await CreateLogisticCardStates.min_volume.set()
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")


@dp.message_handler(state=CreateLogisticCardStates.min_volume)
async def process_min_volume(message: types.Message, state: FSMContext):
    try:
        volume = float(message.text.replace(",", "."))
        await state.update_data(min_volume=volume)
        await message.answer(
            "🚛 Шаг 5/7\n\nУкажите тип транспорта (например: Фура 20т):",
            parse_mode="HTML",
        )
        await CreateLogisticCardStates.transport_type.set()
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")


@dp.message_handler(state=CreateLogisticCardStates.transport_type)
async def process_transport_type(message: types.Message, state: FSMContext):

    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "ПКФ «Волга-Порт»",
        "ПКФ «Юг-Тер»",
        "ПАО «Астр.Порт»",
        "Универ.Порт",
        "Юж.Порт",
        "Агрофуд",
        "Моспорт",
        "ЦГП",
        "АМП",
        "Армада",
        "Стрелец",
        "Альфа",
    ]

    for port in ports:
        keyboard.insert(InlineKeyboardButton(port, callback_data=f"selectport_{port}"))
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="ports_selected"))

    await message.answer(
        "🏢 Шаг 6/7\n\nВыберите порты, в которых работаете (можно несколько):",
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await CreateLogisticCardStates.ports.set()


@dp.callback_query_handler(
    lambda c: c.data.startswith("selectport_"), state=CreateLogisticCardStates.ports
)
async def toggle_port_selection(callback: types.CallbackQuery, state: FSMContext):
    port = callback.data.replace("selectport_", "")

    data = await state.get_data()
    selected_ports = data.get("selected_ports", [])

    if port in selected_ports:
        selected_ports.remove(port)
    else:
        selected_ports.append(port)

    await state.update_data(selected_ports=selected_ports)

    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "ПКФ «Волга-Порт»",
        "ПКФ «Юг-Тер»",
        "ПАО «Астр.Порт»",
        "Универ.Порт",
        "Юж.Порт",
        "Агрофуд",
        "Моспорт",
        "ЦГП",
        "АМП",
        "Армада",
        "Стрелец",
        "Альфа",
    ]
    for p in ports:
        mark = "✅ " if p in selected_ports else ""
        keyboard.insert(
            InlineKeyboardButton(f"{mark}{p}", callback_data=f"selectport_{p}")
        )
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="ports_selected"))

    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data == "ports_selected", state=CreateLogisticCardStates.ports
)
async def ports_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_ports = data.get("selected_ports", [])

    if not selected_ports:
        await callback.answer("❌ Выберите хотя бы один порт", show_alert=True)
        return

    await callback.message.edit_text(
        "📝 Шаг 7/7\n\nВведите дополнительную информацию (или напишите 'нет'):",
        parse_mode="HTML",
    )
    await CreateLogisticCardStates.additional_info.set()
    await callback.answer()


@dp.message_handler(state=CreateLogisticCardStates.additional_info)
async def save_logistic_card(message: types.Message, state: FSMContext):
    await state.update_data(additional_info=additional)
    data = await state.get_data()
    user_id = message.from_user.id

    logistics_cards[user_id] = {
        # Основные данные карточки
        "user_id": user_id,
        # Метаданные карточки
        "status": "active",
        "created_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "views": 0,
        "name": user_data.get("name", "Не указано"),
        "phone": user_data.get("phone", "Не указан"),
        "email": user_data.get("email", "Не указан"),
        "company": user_data.get("company_details", "Не указана"),
        "inn": user_data.get("inn", "Не указан"),
        "ogrn": user_data.get("ogrn", "Не указан"),
        "region": user_data.get("region", "Не указан"),
    }

    save_logistics_cards_to_pickle()

    await state.finish()
    await message.answer(
        "✅ <b>Карточка логиста создана!</b>\n\n"
        "Теперь экспортёры будут получать вашу карточку при закрытии пулов.",
        parse_mode="HTML",
    )

    logging.info(f"✅ Logistic card created for user {user_id}")


# ====================================================================
# СОЗДАНИЕ КАРТОЧКИ ЭКСПЕДИТОРА (АНАЛОГИЧНО)
# ====================================================================
@dp.callback_query_handler(lambda c: c.data == "create_expeditor_card", state="*")
async def start_create_expeditor_card(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.edit_text(
        parse_mode="HTML",
    )
    await callback.answer()



    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "ПКФ «Волга-Порт»",
        "ПКФ «Юг-Тер»",
        "ПАО «Астр.Порт»",
        "Универ.Порт",
        "Юж.Порт",
        "Агрофуд",
        "Моспорт",
        "ЦГП",
        "АМП",
        "Армада",
        "Стрелец",
        "Альфа",
    ]
    for port in ports:
        keyboard.insert(
            InlineKeyboardButton(port, callback_data=f"selectexpport_{port}")
        )
    keyboard.add(
        InlineKeyboardButton("✅ Готово", callback_data="expeditor_ports_selected")
    )

    await message.answer(
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await CreateExpeditorCardStates.ports.set()


@dp.callback_query_handler(
    lambda c: c.data.startswith("selectexpport_"), state=CreateExpeditorCardStates.ports
)
async def toggle_expeditor_port(callback: types.CallbackQuery, state: FSMContext):
    port = callback.data.replace("selectexpport_", "")

    data = await state.get_data()
    selected_ports = data.get("selected_ports", [])

    if port in selected_ports:
        selected_ports.remove(port)
    else:
        selected_ports.append(port)

    await state.update_data(selected_ports=selected_ports)

    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "ПКФ «Волга-Порт»",
        "ПКФ «Юг-Тер»",
        "ПАО «Астр.Порт»",
        "Универ.Порт",
        "Юж.Порт",
        "Агрофуд",
        "Моспорт",
        "ЦГП",
        "АМП",
        "Армада",
        "Стрелец",
        "Альфа",
    ]
    for p in ports:
        mark = "✅ " if p in selected_ports else ""
        keyboard.insert(
            InlineKeyboardButton(f"{mark}{p}", callback_data=f"selectexpport_{p}")
        )
    keyboard.add(
        InlineKeyboardButton("✅ Готово", callback_data="expeditor_ports_selected")
    )

    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data == "expeditor_ports_selected",
    state=CreateExpeditorCardStates.ports,
)
async def expeditor_ports_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_ports = data.get("selected_ports", [])

    if not selected_ports:
        await callback.answer("❌ Выберите хотя бы один порт", show_alert=True)
        return

    await callback.message.edit_text(
    )
    await callback.answer()


    await message.answer(
        parse_mode="HTML",
    )



    data = await state.get_data()
    user_id = message.from_user.id

    expeditor_cards[user_id] = {
        "user_id": user_id,
        "status": "active",
        "created_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
    }

    save_expeditor_cards_to_pickle()

    await state.finish()
    await message.answer(
        "✅ <b>Карточка экспедитора создана!</b>\n\n"
        parse_mode="HTML",
    )

    logging.info(f"✅ Expeditor card created for user {user_id}")


# ====================================================================
# ВЫБОР ЛОГИСТА ЭКСПОРТЁРОМ
# ====================================================================
@dp.callback_query_handler(lambda c: c.data.startswith("select_logistic_"))
async def select_logistic_handler(callback: types.CallbackQuery):
    try:
        parts = callback.data.split("_")
        logistic_id = int(parts[2])
        deal_id = int(parts[3])

            await callback.answer("❌ Сделка не найдена", show_alert=True)
            return

            "%Y-%m-%d %H:%M:%S"
        )
        save_deals_to_pickle()

        inn = logistic.get("inn", "Не указано")
        ogrn = logistic.get("ogrn", "Не указано")

            f"👤 Контакт: {logistic.get('name', 'Не указано')}\n"
            f"🏢 Компания: {company}\n"
            f"📋 ИНН: <code>{inn}</code>\n"
            f"📋 ОГРН: <code>{ogrn}</code>\n"
        )

        pull_id = deal.get("pull_id")

        try:
                f"📦 Сделка #{deal_id}\n"
                f"🎯 Объём: {pull.get('current_volume', 0)} т\n"
            )
            logging.info(
            )
        except Exception as e:

        logging.info(f"✅ Логист {logistic_id} выбран для сделки {deal_id}")

    except Exception as e:
        logging.error(f"Ошибка при выборе логиста: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)


# ====================================================================
# ВЫБОР ЭКСПЕДИТОРА ЭКСПОРТЁРОМ
# ====================================================================
async def select_expeditor_handler(callback: types.CallbackQuery):
    try:
        parts = callback.data.split("_")
        expeditor_id = int(parts[2])
        deal_id = int(parts[3])

            await callback.answer("❌ Сделка не найдена", show_alert=True)
            return

            "%Y-%m-%d %H:%M:%S"
        )
        save_deals_to_pickle()

            f"👤 Контакт: {expeditor.get('name', 'Не указано')}\n"
        )

        pull_id = deal.get("pull_id")

        try:
                f"📦 Сделка #{deal_id}\n"
                f"🎯 Объём: {pull.get('current_volume', 0)} т\n"
            )
        except Exception as e:

        logging.info(f"✅ Expeditor {expeditor_id} selected for deal {deal_id}")

    except Exception as e:
        logging.error(f"Error selecting expeditor: {e}")


# ============================================================================
# НЕДОСТАЮЩИЕ ОБРАБОТЧИКИ (добавлены при исправлении)
# ============================================================================


@dp.callback_query_handler(lambda c: c.data == "back_to_menu", state="*")
async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.finish()
    user_id = callback.from_user.id

    # ✅ ИСПРАВЛЕНИЕ: используем get_role_keyboard
    if role == "farmer":
        await callback.message.answer(
            "🌾 <b>Главное меню фермера</b>\n\n" "Выберите действие:",
            reply_markup=get_role_keyboard("farmer"),
            parse_mode="HTML",
        )
    elif role == "exporter":
        await callback.message.answer(
            "🚢 <b>Главное меню экспортёра</b>\n\n" "Выберите действие:",
            reply_markup=get_role_keyboard("exporter"),
            parse_mode="HTML",
        )
        await callback.message.answer(
            "🚚 <b>Главное меню логиста</b>\n\n" "Выберите действие:",
            parse_mode="HTML",
        )
        await callback.message.answer(
            "📋 <b>Главное меню экспедитора</b>\n\n" "Выберите действие:",
            reply_markup=get_role_keyboard("expeditor"),
            parse_mode="HTML",
        )
    else:
        await callback.message.answer("⚠️ Роль не определена. Используйте /start")

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "cancel", state="*")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    await state.finish()
    await callback.message.answer("❌ Действие отменено")
    await back_to_menu_handler(callback, state)


@dp.callback_query_handler(lambda c: c.data == "cancel_action", state="*")
async def cancel_action_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отмена действия"""
    await cancel_handler(callback, state)


@dp.callback_query_handler(lambda c: c.data == "transport_type", state="*")
async def transport_type_handler(callback: types.CallbackQuery):
    """Выбор типа транспорта"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚛 Автомобильный", callback_data="transport:auto"),
        InlineKeyboardButton("🚂 Железнодорожный", callback_data="transport:rail"),
        InlineKeyboardButton("🚢 Морской", callback_data="transport:sea"),
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu"),
    )
    await callback.message.edit_text(
        "🚚 Выберите тип транспорта:", reply_markup=keyboard
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "view_my_batches", state="*")
async def view_my_batches_handler(callback: types.CallbackQuery):
    """Просмотр моих партий"""
    user_id = callback.from_user.id

    if not user_batches:
        await callback.message.answer("📦 У вас пока нет созданных партий")
        await callback.answer()
        return

    text = "<b>📦 Ваши партии:</b>\n\n"
    keyboard = InlineKeyboardMarkup(row_width=1)

    for i, batch in enumerate(user_batches[:10], 1):
        crop = batch.get("culture", "Н/Д")
        volume = batch.get("volume", 0)
        text += f"{i}. {crop} - {volume} тонн\n"
        keyboard.add(
            InlineKeyboardButton(
                f"📦 {crop} ({volume}т)", callback_data=f"view_batch:{batch.get('id')}"
            )
        )

    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "startsearch", state="*")
async def startsearch_handler(callback: types.CallbackQuery):
    """Начать поиск"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 По культуре", callback_data="search_by_culture"),
        InlineKeyboardButton("📍 По региону", callback_data="search_by_region"),
        InlineKeyboardButton("💰 По цене", callback_data="search_by:price"),
        InlineKeyboardButton("📊 По объёму", callback_data="search_by:volume"),
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu"),
    )
    await callback.message.edit_text(
        "🔍 Выберите критерий поиска:", reply_markup=keyboard
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "broadcast_confirm", state="*")
async def broadcast_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение рассылки"""
    data = await state.get_data()
    message_text = data.get("broadcast_message")

    if not message_text:
        await callback.message.answer("❌ Сообщение для рассылки не найдено")
        await callback.answer()
        return

    # Отправляем всем пользователям
    sent = 0
    failed = 0

    for user_id in users.keys():
        try:
            await bot.send_message(
                user_id, f"📢 <b>Рассылка:</b>\n\n{message_text}", parse_mode="HTML"
            )
            sent += 1
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка отправки рассылки {user_id}: {e}")

    await callback.message.answer(
        f"✅ Рассылка завершена\n📤 Отправлено: {sent}\n❌ Ошибок: {failed}"
    )
    await state.finish()
    await callback.answer()




    )
    await callback.answer()



        return

        return
        return

    await callback.message.answer(
    )
    await callback.answer()


    try:
    user_id = callback.from_user.id
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

        return



    await state.finish()
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    request_id = offer.get("request_id")

        return



# ═══════════════════════════════════════════════════════════════════════════
# ОТМЕНА ПРЕДЛОЖЕНИЯ
# ═══════════════════════════════════════════════════════════════════════════


@dp.callback_query_handler(lambda c: c.data.startswith("cancel_offer:"), state="*")
async def cancel_offer_handler(callback: types.CallbackQuery):
    """Отмена предложения логистом"""
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

    # Проверяем что предложение ещё в статусе pending
        await callback.answer(
            "❌ Можно отменить только предложения в ожидании ответа", show_alert=True
        )
        return

    # Подтверждение отмены
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, отменить", callback_data=f"confirm_cancel_offer:{offer_id}"
        ),
    )

    await callback.message.edit_text(
        f"🚫 <b>Отмена предложения #{offer_id}</b>\n\n"
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_cancel_offer:"), state="*"
)
async def confirm_cancel_offer(callback: types.CallbackQuery):
    """Подтверждение и финальная обработка отмены предложения"""
    try:
        # Парсим offer_id
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return

    request_id = offer.get("request_id")

    # Обновляем статус предложения

    # Получаем данные для уведомления
    price = offer.get("price", 0)
    vehicle_type = offer.get("vehicle_type", "Не указан")
    delivery_date = offer.get("delivery_date", "Не указана")

        try:
            await bot.send_message(
                text=f"""🚫 <b>Логист отменил предложение</b>

📋 Предложение: #{offer_id}
📦 Заявка: #{request_id}
🚛 Транспорт: {vehicle_type}
💰 Цена: {price:,} ₽
📅 Дата доставки: {delivery_date}

Вы можете рассмотреть другие предложения от других логистов.""",
                parse_mode="HTML",
            )
            logging.info(
            )
        except Exception as e:

    # ✅ Показываем сообщение об успешной отмене
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            "📋 К моим предложениям", callback_data="back_to_offers_list"
        )
    )

    await callback.message.edit_text(
        f"""✅ <b>Предложение успешно отменено</b>

🚫 Предложение: #{offer_id}
📝 Статус: ОТМЕНЕНО

        reply_markup=keyboard,
        parse_mode="HTML",
    )

    # Уведомляем логиста
    await callback.answer("✅ Предложение отменено", show_alert=True)

    # Логируем действие
    logging.info(f"🚫 Предложение #{offer_id} отменено логистом")


# ============================================================================
# 🔙 ВОЗВРАТ К СПИСКУ ПРЕДЛОЖЕНИЙ - ПОЛНАЯ ВЕРСИЯ
# ============================================================================


@dp.callback_query_handler(lambda c: c.data == "back_to_offers_list", state="*")
async def back_to_offers_list(callback: types.CallbackQuery, state: FSMContext):
    """Вернуться к списку предложений логиста"""
    await state.finish()

    user_id = callback.from_user.id

    # Проверяем доступ
        await callback.answer("❌ Доступ запрещен", show_alert=True)
        return

    try:
        # Получаем все предложения логиста
        my_offers = {
            oid: o
            for oid, o in logistic_offers.items()
        }

        # Если предложений нет
        if not my_offers:
            keyboard = InlineKeyboardMarkup()
            keyboard.add(
                InlineKeyboardButton(
                    "🚚 К активным заявкам", callback_data="logistic_requests_list"
                )
            )

            await callback.message.edit_text(
                "💼 <b>Мои предложения</b>\n\n"
                "У вас пока нет предложений.\n"
                "Откликайтесь на заявки в разделе «🚚 Активные заявки»!",
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            await callback.answer()
            return

        # Подсчёт по статусам
        in_progress = sum(
        )

        text = (
            f"📊 <b>Всего:</b> {len(my_offers)}\n"
            f"⏳ Ожидают ответа: {pending}\n"
            f"✅ Приняты: {accepted}\n"
            f"🚚 В работе: {in_progress}\n"
            f"✔️ Завершены: {completed}\n"
            f"❌ Отклонены: {rejected}\n"
            f"🚫 Отменены: {cancelled}\n\n"
        )

        keyboard = InlineKeyboardMarkup(row_width=1)

        # Сортируем предложения (активные первыми)
        sorted_offers = sorted(
            my_offers.items(),
            key=lambda x: {
                "pending": 0,
                "accepted": 1,
                "in_progress": 2,
                "completed": 3,
                "rejected": 4,
                "cancelled": 5,
        )

        # Показываем первые 10 предложений
        for idx, (offer_id, offer) in enumerate(sorted_offers[:10], 1):
            req_id = offer.get("request_id")
            price = offer.get("price", 0)
            # Эмодзи статуса
            status_emoji = {
                "pending": "⏳",
                "accepted": "✅",
                "in_progress": "🚚",
                "completed": "✔️",
                "rejected": "❌",
                "cancelled": "🚫",
            }.get(status, "❓")

            # Получаем культуру из заявки
                culture = request.get("culture", "Н/Д")

            text += f"{idx}. {status_emoji} <b>Предложение #{offer_id}</b>\n"
            text += f"   📋 Заявка #{req_id} | {culture}\n"
            text += f"   💰 {price:,} ₽\n\n"

            # Добавляем кнопку
            keyboard.add(
                InlineKeyboardButton(
                    f"{status_emoji} Предложение #{offer_id}",
                )
            )

        # Если предложений больше чем показываем
        if len(my_offers) > 10:
            text += f"<i>...и ещё {len(my_offers) - 10} предложений</i>\n"

        # Редактируем сообщение
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")

        await callback.answer()

    except Exception as e:
        logging.error(f"Ошибка в back_to_offers_list: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


# ============================================================================
# FSM СОСТОЯНИЯ ДЛЯ РЕДАКТИРОВАНИЯ КАРТОЧКИ
# ============================================================================


# ============================================================================
# ✏️ МЕНЮ РЕДАКТИРОВАНИЯ КАРТОЧКИ
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "edit_logistic_card", state="*")
async def edit_logistic_card_menu(callback: types.CallbackQuery, state: FSMContext):
    """Меню редактирования карточки логиста"""
    await state.finish()

    user_id = callback.from_user.id

    # Проверяем наличие карточки
    if user_id not in logistics_cards:
        await callback.answer("❌ Карточка не найдена", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "🚛 Изменить тип транспорта", callback_data="edit_card_vehicle"
        ),
        InlineKeyboardButton(
            "📦 Изменить грузоподъёмность", callback_data="edit_card_capacity"
        ),
        InlineKeyboardButton("📍 Изменить регионы", callback_data="edit_card_regions"),
        InlineKeyboardButton(
            "💰 Изменить цену (₽/км)", callback_data="edit_card_price"
        ),
        InlineKeyboardButton(
            "📝 Изменить описание", callback_data="edit_card_description"
        ),
        InlineKeyboardButton("🔙 Назад к карточке", callback_data="back_to_card"),
    )

    await callback.message.edit_text(
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_card_vehicle", state="*")
async def edit_card_vehicle(callback: types.CallbackQuery, state: FSMContext):
    """Изменить тип транспорта в карточке"""
    await state.finish()

    user_id = callback.from_user.id

    if user_id in logistics_cards:

    vehicle_display = {
        "grain": "🚚 Зерновоз",
    }

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚚 Зерновоз", callback_data="edit_vtype_grain"),
        InlineKeyboardButton("🚛 Фура", callback_data="edit_vtype_truck"),
    )

    await callback.message.edit_text(
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_vtype_"), state="*")
async def save_edit_vehicle(callback: types.CallbackQuery, state: FSMContext):
    """Сохранить выбранный тип транспорта и показать обновленную карточку"""
    await state.finish()

    user_id = callback.from_user.id

    # Извлекаем тип транспорта из callback_data
    try:
    except Exception as e:
        logging.error(f"Ошибка парсинга callback_data: {e}")
        await callback.answer("❌ Ошибка при обработке выбора", show_alert=True)
        return

    if user_id not in logistics_cards:
        await callback.answer("❌ Ошибка: Карточка не найдена", show_alert=True)
        return

    try:
        old_vehicle = logistics_cards[user_id].get("vehicle_type", "не установлен")

        logistics_cards[user_id]["vehicle_type"] = vehicle_type
        logistics_cards[user_id]["updated_at"] = datetime.now().strftime(
            "%d.%m.%Y %H:%M:%S"
        )

        vehicle_names = {
            "wagon": "🚂 Ж/д вагон",
        }

        await callback.answer("✅ Тип транспорта обновлён", show_alert=False)

        await show_card_after_edit(callback.message, user_id)

        logging.info(
            f"Логист {user_id} обновил тип транспорта: "
            f"'{old_vehicle}' → '{vehicle_type}' ({vehicle_names.get(vehicle_type, 'Unknown')})"
        )

    except Exception as e:
        logging.error(f"Ошибка при сохранении типа транспорта: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


# ============================================================================
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "edit_card_capacity", state="*")
async def edit_card_capacity(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование грузоподъёмности"""
    await state.finish()

    user_id = callback.from_user.id

    current_capacity = "не установлена"
    if user_id in logistics_cards:

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📦 1-5 т", callback_data="edit_cap_1_5"),
        InlineKeyboardButton("📦 5-10 т", callback_data="edit_cap_5_10"),
        InlineKeyboardButton("📦 10-20 т", callback_data="edit_cap_10_20"),
        InlineKeyboardButton("📦 20+ т", callback_data="edit_cap_20_plus"),
        InlineKeyboardButton("✍️ Ввести вручную", callback_data="edit_cap_custom"),
        InlineKeyboardButton("❌ Отмена", callback_data="edit_logistic_card"),
    )

    text = (
        f"<i>Текущее значение:</i> <b>{current_capacity}</b>\n\n"
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.message_handler(state=EditCardStates.capacity, content_types=["text"])
async def save_edit_capacity(message: types.Message, state: FSMContext):
    """Сохранить новую грузоподъёмность из текстового ввода"""
    user_id = message.from_user.id
    capacity_text = message.text.strip()

    if user_id not in logistics_cards:
        await message.answer(
            parse_mode="HTML",
        )
        await state.finish()
        return

    try:
        capacity_text = capacity_text.replace(",", ".").strip()
        capacity = float(capacity_text)

        if capacity <= 0:
            raise ValueError("Значение должно быть положительным")
        if capacity > 10000:
            raise ValueError("Значение слишком велико (максимум 10000 тонн)")

        old_capacity = logistics_cards[user_id].get("capacity", "не установлена")

        logistics_cards[user_id]["capacity"] = capacity
        logistics_cards[user_id]["updated_at"] = datetime.now().strftime(
            "%d.%m.%Y %H:%M:%S"
        )

        await message.answer(
            f"📦 <b>Новое значение:</b> {capacity} тонн\n"
            f"📦 <b>Старое значение:</b> {old_capacity}",
            parse_mode="HTML",
        )

        logging.info(
            f"Логист {user_id} обновил грузоподъёмность: "
            f"'{old_capacity}' → '{capacity}' тонн"
        )

        await show_card_after_edit(message, user_id)
        await state.finish()

    except ValueError as ve:
        error_msg = str(ve)
        await message.answer(
            f"<i>{error_msg}</i>\n\n"
            parse_mode="HTML",
        )
        logging.warning(
        )

    except Exception as e:
        logging.error(f"Ошибка при сохранении грузоподъёмности: {e}", exc_info=True)
        await message.answer(
            parse_mode="HTML",
        )


# ============================================================================
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "edit_card_regions", state="*")
async def edit_card_regions(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    user_id = callback.from_user.id

    current_regions = "не установлены"
    if user_id in logistics_cards:
        regions_list = logistics_cards[user_id].get("regions", [])
        if regions_list:
            current_regions = ", ".join(regions_list)

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="edit_logistic_card"))

    text = (
        f"<i>Текущие регионы:</i> <b>{current_regions}</b>\n\n"
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await state.set_state(EditCardStates.regions)
    await callback.answer()


@dp.message_handler(state=EditCardStates.regions, content_types=["text"])
async def save_edit_regions(message: types.Message, state: FSMContext):
    """Сохранить новые регионы доставки"""
    user_id = message.from_user.id
    regions_text = message.text.strip()

    if user_id not in logistics_cards:
        await message.answer(
            parse_mode="HTML",
        )
        await state.finish()
        return

    try:

        if not regions_list:
            await message.answer(
            )
            return

        if len(regions_list) > 20:
            await message.answer(
                parse_mode="HTML",
            )
            return

        for region in regions_list:
            if len(region) < 2:
                await message.answer(
                    f"Название региона слишком короткое: <b>{region}</b>\n"
                    parse_mode="HTML",
                )
                return
            if len(region) > 50:
                await message.answer(
                    f"Название региона слишком длинное: <b>{region}</b>\n"
                    parse_mode="HTML",
                )
                return

        old_regions = logistics_cards[user_id].get("regions", [])

        logistics_cards[user_id]["regions"] = regions_list
        logistics_cards[user_id]["updated_at"] = datetime.now().strftime(
            "%d.%m.%Y %H:%M:%S"
        )

        regions_display = "\n".join([f"• {r}" for r in regions_list])

        text = (
            f"<b>Новые регионы:</b>\n{regions_display}\n\n"
            f"<b>Количество регионов:</b> {len(regions_list)}\n\n"
            f"⏰ Обновлено: <i>{logistics_cards[user_id]['updated_at']}</i>"
        )

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "✏️ Редактировать ещё", callback_data="edit_logistic_card"
            ),
            InlineKeyboardButton("📋 К карточке", callback_data="refresh_card"),
        )

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

        logging.info(
            f"Логист {user_id} обновил регионы: "
            f"'{', '.join(old_regions)}' → '{', '.join(regions_list)}'"
        )

        await state.finish()

    except Exception as e:
        logging.error(f"Ошибка при сохранении регионов: {e}", exc_info=True)
        await message.answer(
            parse_mode="HTML",
        )


# ============================================================================
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "edit_card_price", state="*")
async def edit_card_price(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование цены за км"""
    await state.finish()

    user_id = callback.from_user.id

    current_price = "не установлена"
    if user_id in logistics_cards:
        if price:
            current_price = f"{price} ₽/км"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="edit_logistic_card"))

    text = (
        f"<i>Текущая цена:</i> <b>{current_price}</b>\n\n"
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await state.set_state(EditCardStates.price_per_km)
    await callback.answer()


@dp.message_handler(state=EditCardStates.price_per_km, content_types=["text"])
async def save_edit_price(message: types.Message, state: FSMContext):
    """Сохранить новую цену за км"""
    user_id = message.from_user.id
    price_text = message.text.strip()

    if user_id not in logistics_cards:
        await message.answer(
            parse_mode="HTML",
        )
        await state.finish()
        return

    try:
        price_text = price_text.replace(",", ".").replace(" ", "").strip()
        price = float(price_text)

        if price < 0.01:
            await message.answer(
                "❌ <b>Минимальная цена:</b> 0.01 ₽/км", parse_mode="HTML"
            )
            return
        if price > 10000:
            await message.answer(
                "❌ <b>Максимальная цена:</b> 10000 ₽/км", parse_mode="HTML"
            )
            return

        price = round(price, 2)

        old_price = logistics_cards[user_id].get("price_per_km", "не установлена")

        logistics_cards[user_id]["price_per_km"] = price
        logistics_cards[user_id]["updated_at"] = datetime.now().strftime(
            "%d.%m.%Y %H:%M:%S"
        )

        text = (
            f"💰 <b>Новая цена:</b> {price} ₽/км\n"
            f"💰 <b>Старая цена:</b> {old_price}\n\n"
            f"• 100 км: {price * 100:.2f} ₽\n"
            f"• 500 км: {price * 500:.2f} ₽\n"
            f"• 1000 км: {price * 1000:.2f} ₽\n\n"
            f"⏰ Обновлено: <i>{logistics_cards[user_id]['updated_at']}</i>"
        )

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "✏️ Редактировать ещё", callback_data="edit_logistic_card"
            ),
            InlineKeyboardButton("📋 К карточке", callback_data="refresh_card"),
        )

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

        logging.info(
        )

        await state.finish()

        await message.answer(
            "❌ <b>Ошибка валидации</b>\n\n"
            "Введите корректное число\n\n"
            "<b>Примеры правильного ввода:</b>\n"
            "• <code>15</code>\n"
            "• <code>25.50</code>\n"
            "• <code>30,99</code>",
            parse_mode="HTML",
        )

    except Exception as e:
        logging.error(f"Ошибка при сохранении цены: {e}", exc_info=True)
        await message.answer(
            parse_mode="HTML",
        )


# ============================================================================
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "edit_card_description", state="*")
async def edit_card_description(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование описания компании"""
    await state.finish()

    user_id = callback.from_user.id

    current_description = "не установлено"
    if user_id in logistics_cards:
        if desc:
            current_description = desc[:100] + "..." if len(desc) > 100 else desc

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="edit_logistic_card"))

    text = (
        f"<b>{current_description}</b>\n\n"
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await state.set_state(EditCardStates.description)
    await callback.answer()


@dp.message_handler(state=EditCardStates.description, content_types=["text"])
async def save_edit_description(message: types.Message, state: FSMContext):
    """Сохранить новое описание компании"""
    user_id = message.from_user.id
    description_text = message.text.strip()

    if user_id not in logistics_cards:
        await message.answer(
            parse_mode="HTML",
        )
        await state.finish()
        return

    try:
        if len(description_text) < 10:
            await message.answer(
                f"Введено: <b>{len(description_text)}</b> символов\n"
                parse_mode="HTML",
            )
            return

        if len(description_text) > 1000:
            await message.answer(
                f"Введено: <b>{len(description_text)}</b> символов\n"
                parse_mode="HTML",
            )
            return

        forbidden_chars = ["<script", "<?php", "javascript:", "onclick", "onerror"]
                await message.answer(
                    "Пожалуйста, используйте только текст",
                    parse_mode="HTML",
                )
                return


        description_cleaned = " ".join(description_text.split())

        logistics_cards[user_id]["description"] = description_cleaned
        logistics_cards[user_id]["updated_at"] = datetime.now().strftime(
            "%d.%m.%Y %H:%M:%S"
        )

        preview_old = (
            old_description[:80] + "..."
            if len(str(old_description)) > 80
            else old_description
        )
        preview_new = (
            description_cleaned[:80] + "..."
            if len(description_cleaned) > 80
            else description_cleaned
        )

        text = (
            f"📝 Символов: <b>{len(description_cleaned)}</b>\n"
            f"📄 Слов: <b>{len(description_cleaned.split())}</b>\n\n"
            f"<i>{preview_new}</i>\n\n"
            f"⏰ Обновлено: <i>{logistics_cards[user_id]['updated_at']}</i>"
        )

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "✏️ Редактировать ещё", callback_data="edit_logistic_card"
            ),
            InlineKeyboardButton("📋 К карточке", callback_data="refresh_card"),
        )

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

        logging.info(
        )

        await state.finish()

    except Exception as e:
        logging.error(f"Ошибка при сохранении описания: {e}", exc_info=True)
        await message.answer(
            parse_mode="HTML",
        )


# ============================================================================
# ============================================================================
async def show_card_after_edit(message: types.Message, user_id: int):
    """
    """
    if user_id not in logistics_cards:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
        )

        await message.answer(
            "❌ Карточка не найдена", reply_markup=keyboard, parse_mode="HTML"
        )
        return

    card = logistics_cards[user_id]

    vehicle_map = {
        "wagon": ("🚂", "Ж/д вагон"),
    }


    capacity = card.get("capacity", 0)


    price_text = f"{price:.2f} ₽/км" if price > 0 else "❓"


    text = (
        f"{emoji} <b>{vehicle_name}</b>\n"
        f"📦 {capacity_text}\n"
        f"💰 {price_text}\n\n"
        f"{description}\n\n"
        f"👁 Просмотров: {card.get('views', 0)}\n"
        f"✅ Завершено: {card.get('completed_deals', 0)}"
    )

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Редактировать", callback_data="edit_logistic_card"),
        InlineKeyboardButton("🗑 Удалить", callback_data="delete_logistic_card"),
    )

    try:
        if hasattr(message, "edit_text"):
            await message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        else:
            await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


# ============================================================================
# 🔙 ОБРАБОТЧИК ВОЗВРАТА К КАРТОЧКЕ
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "back_to_card", state="*")
async def back_to_card(callback: types.CallbackQuery, state: FSMContext):
    """Вернуться к карточке логиста"""
    user_id = callback.from_user.id

    try:
        await state.finish()
        await show_card_after_edit(callback.message, user_id)
        await callback.answer()
        logging.info(f"Логист {user_id} вернулся к карточке")
    except Exception as e:
        logging.error(f"Ошибка при возврате к карточке: {e}", exc_info=True)
        try:
            await callback.answer("❌ Ошибка", show_alert=True)


@dp.message_handler(lambda m: m.text == "📋 Создать предложение", state="*")
async def expeditor_create_offer_handler(message: types.Message, state: FSMContext):
    """Начать создание предложения экспедитора"""
    await state.finish()
    user_id = message.from_user.id

        await message.answer("❌ Доступ запрещен.")
        return

    await ExpeditorOfferStates.service_type.set()
    await message.answer(
        "<b>📋 Создание предложения</b>\n\n<b>Выберите тип услуги:</b>",
        reply_markup=expeditor_service_keyboard(),
        parse_mode="HTML",
    )


@dp.callback_query_handler(
    lambda c: c.data.startswith("service:"), state=ExpeditorOfferStates.service_type
)
async def set_service_type(callback: types.CallbackQuery, state: FSMContext):
    """Установить тип услуги"""
    service_names = {
        "docs": "📄 Оформление документов",
        "customs": "🏢 Таможенное оформление",
        "freight": "🚢 Фрахтование",
        "full": "📦 Полный комплекс услуг",
    }

    await state.update_data(service_type=service_names.get(service_type, service_type))
    await ExpeditorOfferStates.ports.set()

    await callback.message.edit_text(
        f"Выбрана услуга: {service_names.get(service_type, service_type)}\n\n"
        parse_mode="HTML",
    )
    await callback.answer()


@dp.message_handler(state=ExpeditorOfferStates.ports)
async def set_expeditor_ports(message: types.Message, state: FSMContext):
    """Установить порты обслуживания"""
    ports = message.text.strip()

    if len(ports) < 3:
        await message.answer("❌ Слишком короткое название портов")
        return

    await state.update_data(ports=ports)
    await ExpeditorOfferStates.price.set()
    await message.answer(
        "<b>💰 Укажите стоимость услуг (₽):</b>\n\nВведите цену: <code>50000</code>",
        parse_mode="HTML",
    )


@dp.message_handler(state=ExpeditorOfferStates.price)
async def set_expeditor_price(message: types.Message, state: FSMContext):
    """Установить цену услуги"""
    try:
        price = float(message.text.replace(",", ".").replace(" ", ""))
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            "❌ Неверный формат. Введите число: <code>50000</code>", parse_mode="HTML"
        )
        return

    await state.update_data(price=price)
    await ExpeditorOfferStates.terms.set()
    await message.answer(
        "<b>📝 Укажите условия:</b>\n\nОпишите условия работы, сроки, гарантии:\n<code>Срок: 3-5 дней. Гарантия возврата.</code>",
        parse_mode="HTML",
    )


@dp.message_handler(state=ExpeditorOfferStates.terms)
async def set_expeditor_terms(message: types.Message, state: FSMContext):
    """Установить условия"""
    terms = message.text.strip()

    if len(terms) < 10:
        await message.answer("❌ Слишком короткое описание условий.")
        return

    await state.update_data(terms=terms)
    await ExpeditorOfferStates.confirm.set()

    data = await state.get_data()
    text += f"📋 {data['service_type']}\n"
    text += f"🏢 {data['ports']}\n"
    text += f"💰 {data['price']:,.0f} ₽\n"
    text += f"📝 {data['terms']}\n\nВсё верно?"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_expeditor_offer"),
        InlineKeyboardButton("❌ Отменить", callback_data="cancel"),
    )
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query_handler(
    lambda c: c.data == "confirm_expeditor_offer", state=ExpeditorOfferStates.confirm
)
async def confirm_expeditor_offer(callback: types.CallbackQuery, state: FSMContext):
    """Подтвердить создание предложения экспедитора"""
    data = await state.get_data()
    user_id = callback.from_user.id

    offer = {
        "id": offer_id,
        "expeditor_id": user_id,
        "service_type": data["service_type"],
        "ports": data["ports"],
        "terms": data["terms"],
        "status": "active",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    expeditor_offers[offer_id] = offer
    save_expeditor_offers()

    await callback.message.edit_text(
        f"<b>✅ Предложение создано!</b>\n\nПредложение #{offer_id}\n"
        f"📋 {data['service_type']}\n"
        f"🏢 {data['ports']}\n"
        parse_mode="HTML",
    )
    await state.finish()
    await callback.answer()


@dp.message_handler(
    lambda m: m.text == "💼 Мои предложения"
    state="*",
)
async def expeditor_my_offers_handler(message: types.Message, state: FSMContext):
    """Показать предложения экспедитора"""
    await state.finish()
    user_id = message.from_user.id

    my_offers = {
        oid: o
        for oid, o in expeditor_offers.items()
    }

    if not my_offers:
        await message.answer(
            "<b>💼 Мои предложения</b>\n\nУ вас пока нет предложений.\n\nСоздайте через <b>📋 Создать предложение</b>",
            parse_mode="HTML",
        )
        return

    text = f"<b>💼 Мои предложения</b>\n\nВсего: <b>{len(my_offers)}</b>\nАктивных: <b>{active}</b>\n\n"

    for idx, (offer_id, offer) in enumerate(list(my_offers.items())[:10], 1):
        text += f"{idx}. <b>Предложение #{offer_id}</b>\n"
        text += f"   📋 {offer.get('service_type', '')}\n"
        text += f"   🏢 {offer.get('ports', '')}\n"
        text += f"   💰 {offer.get('price', 0):,.0f} ₽\n"
        text += f"   Статус: {offer.get('status', 'active')}\n\n"

    if len(my_offers) > 10:
        text += f"\n...и ещё {len(my_offers) - 10}\n"

    await message.answer(text, parse_mode="HTML")


# ========== ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ ==========
# ============================================================================
# ЭКСПОРТЁР: ПРОСМОТР ПРЕДЛОЖЕНИЙ ЛОГИСТОВ
# ============================================================================


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_request_offers_"), state="*"
)
async def view_request_offers(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр предложений логистов по заявке"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID заявки", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id

        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return

    # Получаем все предложения по заявке
    offers = [
        (offer_id, offer)
        for offer_id, offer in logistic_offers.items()
    ]

    if not offers:

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🔙 К заявкам", callback_data="my_shipping_requests")
        )
        keyboard.add(
            InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return

    # Группируем по статусам
    by_status = {"pending": [], "accepted": [], "rejected": []}
    for offer_id, offer in offers:
        if status in by_status:
            by_status[status].append((offer_id, offer))

    for status in by_status:

    pull_id = request.get("pull_id")


    # Статистика
    total = len(offers)
    pending = len(by_status["pending"])
    accepted = len(by_status["accepted"])
    rejected = len(by_status["rejected"])


    if pending > 0:
        best_offer_id, best_offer = by_status["pending"][0]
        text += f"💵 Цена: <b>{best_offer.get('price', 0):,.0f} ₽</b>\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите предложение для просмотра:"

    keyboard = InlineKeyboardMarkup(row_width=1)


        price = offer.get("price", 0)
        vehicle = offer.get("vehicle_type", "Не указан")


        keyboard.add(
            InlineKeyboardButton(
                button_text, callback_data=f"view_offer_details_{offer_id}"
            )
        )

        keyboard.add(
            InlineKeyboardButton(
                f"➕ Показать ещё {len(by_status['pending']) - 5}",
                callback_data=f"show_all_offers_{request_id}",
            )
        )

    # Кнопка сравнения
    if pending >= 2:
        keyboard.add(
            InlineKeyboardButton(
                "⚖️ Сравнить предложения", callback_data=f"compare_offers_{request_id}"
            )
        )

    keyboard.add(
        InlineKeyboardButton(
            "🔄 Обновить", callback_data=f"view_request_offers_{request_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton("🔙 К заявкам", callback_data="my_shipping_requests")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_offer_details_"), state="*"
)
    """Детальный просмотр предложения"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    request_id = offer.get("request_id")

    user_id = callback.from_user.id

    # Проверяем права
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    # Информация о логисте
    logist_name = logist_info.get("company_name", "Не указана")
    logist_phone = logist_info.get("phone", "Не указан")

    # Статистика логиста
    logist_offers = [
    ]
    logist_deliveries = [
    ]
    total_offers = len(logist_offers)

    text = f"📋 <b>ДЕТАЛИ ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"

    # Информация о предложении
    text += f"💵 Стоимость: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
    text += f"🚛 Транспорт: <b>{offer.get('vehicle_type', 'Не указан')}</b>\n"
    text += f"📅 Дата доставки: <b>{offer.get('delivery_date', 'Не указана')}</b>\n"

    if offer.get("additional_info"):
        text += f"\nℹ️ Дополнительно:\n<i>{offer.get('additional_info')}</i>\n"

    text += f"\n📅 Создано: {offer.get('created_at', 'Не указано')}\n\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"

    # Информация о логисте
    text += f"🏢 Название: <b>{logist_name}</b>\n"
    text += f"📞 Телефон: {logist_phone}\n\n"

    # Статистика
    text += f"✅ Завершённых доставок: <b>{completed}</b>\n"
    text += f"📋 Всего предложений: <b>{total_offers}</b>\n"

    if completed > 0:
        # Средняя стоимость
        if completed_deliveries:
            avg_price = sum(completed_deliveries) / len(completed_deliveries)
            text += f"💰 Средняя стоимость: <b>{avg_price:,.0f} ₽</b>\n"

    text += "\n━━━━━━━━━━━━━━━━━━━━\n\n"

    # Статус предложения

        text += "⏳ <b>Ожидает вашего решения</b>"
    elif status == "accepted":
        text += "✅ <b>Предложение принято</b>"
    elif status == "rejected":
        text += "❌ <b>Предложение отклонено</b>"
        if offer.get("rejection_reason"):
            text += f"\n<i>Причина: {offer.get('rejection_reason')}</i>"

    keyboard = InlineKeyboardMarkup(row_width=2)

        keyboard.add(
            InlineKeyboardButton(
                "✅ Принять", callback_data=f"accept_offer_{offer_id}"
            ),
            InlineKeyboardButton(
                "❌ Отклонить", callback_data=f"reject_offer_{offer_id}"
            ),
        )

        keyboard.add(
            InlineKeyboardButton(
                "🔙 К предложениям", callback_data=f"view_request_offers_{request_id}"
            )
        )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("compare_offers_"), state="*")
async def compare_offers(callback: types.CallbackQuery, state: FSMContext):
    """Сравнение предложений"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id

        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    # Получаем ожидающие предложения
    offers = [
        (offer_id, offer)
        for offer_id, offer in logistic_offers.items()
    ]

    if len(offers) < 2:
        await callback.answer(
            "❌ Недостаточно предложений для сравнения", show_alert=True
        )
        return

    # Сортируем по цене
    offers.sort(key=lambda x: x[1].get("price", 999999))

    text += f"📦 Заявка #{request_id}\n"
    text += f"📊 Сравниваем {len(offers)} предложений\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"

    # Показываем топ-3
    for i, (offer_id, offer) in enumerate(offers[:3], 1):
        logist_name = logist_info.get("company_name", f"Логист #{logist_id}")

        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"

        text += f"{medal} <b>#{i} - {logist_name}</b>\n"
        text += f"💰 Цена: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
        text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
        text += f"📅 Дата: {offer.get('delivery_date')}\n"

        # Статистика логиста
        logist_deliveries = [
            d
            for d in deliveries.values()
        ]
        completed = len(logist_deliveries)

        text += f"✅ Доставок: {completed}\n"

    if len(offers) > 3:
        text += f"<i>... и ещё {len(offers) - 3} предложений</i>\n\n"

    # Анализ
    prices = [o[1].get("price", 0) for o in offers]
    min_price = min(prices)
    max_price = max(prices)
    avg_price = sum(prices) / len(prices)

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"💵 Минимальная: <b>{min_price:,.0f} ₽</b>\n"
    text += f"💰 Средняя: <b>{avg_price:,.0f} ₽</b>\n"
    text += f"💸 Максимальная: <b>{max_price:,.0f} ₽</b>\n"
    text += f"📈 Разброс: <b>{max_price - min_price:,.0f} ₽</b>\n\n"

    # Рекомендация
    best_offer_id, best_offer = offers[0]
    text += f"Самое выгодное предложение #{best_offer_id}\n"
    text += f"Экономия: <b>{avg_price - min_price:,.0f} ₽</b>"

    keyboard = InlineKeyboardMarkup(row_width=1)

    # Кнопки для топ-3
    for i, (offer_id, offer) in enumerate(offers[:3], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"
        price = offer.get("price", 0)

        keyboard.add(
            InlineKeyboardButton(
                f"{medal} Выбрать #{i} ({price:,.0f} ₽)",
                callback_data=f"view_offer_details_{offer_id}",
            )
        )

    keyboard.add(
        InlineKeyboardButton(
            "📋 Все предложения", callback_data=f"view_request_offers_{request_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton(
            "🔙 Назад", callback_data=f"view_request_offers_{request_id}"
        )
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# ============================================================================
# ЭКСПОРТЁР: ПРИНЯТИЕ И ОТКЛОНЕНИЕ ПРЕДЛОЖЕНИЙ
# ============================================================================


class AcceptOfferStatesGroup(StatesGroup):
    """FSM для принятия предложения"""

    offer_id = State()
    confirm = State()


class RejectOfferStatesGroup(StatesGroup):
    """FSM для отклонения предложения"""

    offer_id = State()
    reason = State()


@dp.callback_query_handler(lambda c: c.data.startswith("accept_offer_"), state="*")
async def accept_offer_start(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    request_id = offer.get("request_id")
    user_id = callback.from_user.id

        await callback.answer("❌ Нет доступа", show_alert=True)
        return

        await callback.answer("❌ Предложение уже обработано", show_alert=True)
        return

    accepted_offers = [
        o
        for o in logistic_offers.values()
    ]

    if accepted_offers:
        await callback.answer(
            "❌ По этой заявке уже принято другое предложение", show_alert=True
        )
        return

    # Информация о предложении
    logist_name = logist_info.get("company_name", "Не указана")

    pull_id = request.get("pull_id")

    text = f"✅ <b>ПРИНЯТИЕ ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    text += f"📦 <b>ЗАЯВКА #{request_id}</b>\n"
    text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"

    text += f"🚚 Логист: <b>{logist_name}</b>\n"
    text += f"💰 Стоимость: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
    text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
    text += f"📅 Дата доставки: {offer.get('delivery_date')}\n\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"

    text += "⚠️ <b>ВАЖНО:</b>\n"
    text += "• Будет создана доставка\n"
    text += "• Остальные предложения будут отклонены\n"
    text += "• Логист получит уведомление\n"
    text += "• Отменить действие будет невозможно\n\n"

    text += "✅ Подтвердите принятие предложения"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Подтверждаю", callback_data=f"confirm_accept_{offer_id}"
        ),
        InlineKeyboardButton(
            "❌ Отмена", callback_data=f"view_offer_details_{offer_id}"
        ),
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("confirm_accept_"), state="*")
async def accept_offer_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение принятия предложения"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    request_id = offer.get("request_id")

    user_id = callback.from_user.id

        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    # Принимаем предложение
    offer["status"] = "accepted"
    offer["accepted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    offer["accepted_by"] = user_id

    # Обновляем статус заявки
    request["status"] = "assigned"
    request["assigned_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        delivery = {
            "id": delivery_id,
            "request_id": request_id,
            "offer_id": offer_id,
            "pull_id": request.get("pull_id"),
            "volume": request.get("volume"),
            "price": offer.get("price"),
            "vehicle_type": offer.get("vehicle_type"),
            "delivery_date": offer.get("delivery_date"),
            "status": "pending",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        deliveries[delivery_id] = delivery

    rejected_count = 0
    for other_offer_id, other_offer in logistic_offers.items():
        if (
            and other_offer_id != offer_id
        ):
            other_offer["status"] = "rejected"
            other_offer["rejected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            other_offer["rejection_reason"] = "Принято другое предложение"
            rejected_count += 1

            # Уведомляем логистов
            if other_logist_id:
                asyncio.create_task(
                    notify_logistic_offer_rejected(
                    )
                )

    # Сохраняем данные
        save_shipping_requests()
    save_logistic_offers()
    save_deliveries()

    # Уведомляем принятого логиста
    if logist_id:

    # Сообщение пользователю
    logist_name = logist_info.get("company_name", "Не указана")

    text += f"✅ Предложение #{offer_id} успешно принято\n"
    text += f"📦 Доставка #{delivery_id} создана\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"🚚 Логист: {logist_name}\n"
    text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n"
    text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
    text += f"📅 Дата: {offer.get('delivery_date')}\n\n"

    if rejected_count > 0:
        text += f"ℹ️ Отклонено предложений: {rejected_count}\n\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(
            callback_data=f"view_delivery_{delivery_id}",
        )
    )
        keyboard.add(
            InlineKeyboardButton("📋 Мои доставки", callback_data="exporter_deliveries")
        )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer("✅ Предложение принято!")

    logging.info(
        f"✅ Экспортёр {user_id} принял предложение #{offer_id}, создана доставка #{delivery_id}"
    )


@dp.callback_query_handler(lambda c: c.data.startswith("reject_offer_"), state="*")
async def reject_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало отклонения предложения"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    request_id = offer.get("request_id")

    user_id = callback.from_user.id

        await callback.answer("❌ Нет доступа", show_alert=True)
        return

        await callback.answer("❌ Предложение уже обработано", show_alert=True)
        return

    await state.update_data(offer_id=offer_id)

    logist_name = logist_info.get("company_name", "Не указана")

    text = f"❌ <b>ОТКЛОНЕНИЕ ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    text += f"🚚 Логист: {logist_name}\n"
    text += f"💰 Цена: {offer.get('price', 0):,.0f} ₽\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>Укажите причину отклонения</b>\n"
    text += "<i>(необязательно, но рекомендуется)</i>\n\n"
    text += "Возможные причины:\n"
    text += "• Высокая цена\n"
    text += "• Несоответствие транспорта\n"
    text += "• Неподходящие сроки\n"
    text += "• Принято другое предложение\n\n"
    text += "Или нажмите 'Без причины'"

    keyboard = InlineKeyboardMarkup(row_width=1)

    reasons = [
    ]

    for reason_text, callback_data in reasons:
        keyboard.add(InlineKeyboardButton(reason_text, callback_data=callback_data))

    keyboard.add(
        InlineKeyboardButton(
            "✍️ Указать свою причину", callback_data="reject_reason_custom"
        )
    )
    keyboard.add(
        InlineKeyboardButton("➡️ Без причины", callback_data="reject_reason_none")
    )
    keyboard.add(
        InlineKeyboardButton(
            "🔙 Отмена", callback_data=f"view_offer_details_{offer_id}"
        )
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await RejectOfferStatesGroup.reason.set()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("reject_reason_"), state=RejectOfferStatesGroup.reason
)
async def reject_offer_reason_selected(
    callback: types.CallbackQuery, state: FSMContext
):
    """Выбор причины отклонения"""
    reason_key = callback.data.replace("reject_reason_", "")

    data = await state.get_data()
    offer_id = data.get("offer_id")

    if reason_key == "custom":
        text = "✍️ <b>УКАЖИТЕ ПРИЧИНУ ОТКЛОНЕНИЯ</b>\n\n"
        text += "Введите текст причины (до 200 символов):"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("❌ Отмена", callback_data=f"reject_offer_{offer_id}")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return


    # Отклоняем предложение
    await reject_offer_execute(callback, state, offer_id, reason)


@dp.message_handler(state=RejectOfferStatesGroup.reason)
async def reject_offer_custom_reason(message: types.Message, state: FSMContext):
    """Ввод своей причины отклонения"""
    reason = message.text.strip()

    if len(reason) > 200:
        await message.answer(
            "❌ Слишком длинная причина (максимум 200 символов)!\n\nВведите короче:"
        )
        return

    data = await state.get_data()
    offer_id = data.get("offer_id")


    await reject_offer_execute(fake_callback, state, offer_id, reason)


async def reject_offer_execute(
    callback_or_fake, state: FSMContext, offer_id: int, reason: str = None
):
    """Выполнение отклонения предложения"""

        if hasattr(callback_or_fake, "answer"):
            await callback_or_fake.answer("❌ Предложение не найдено", show_alert=True)
        return
    request_id = offer.get("request_id")

    user_id = callback_or_fake.from_user.id

    # Отклоняем предложение
    offer["status"] = "rejected"
    offer["rejected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    offer["rejected_by"] = user_id
    if reason:
        offer["rejection_reason"] = reason

    save_logistic_offers()

    # Уведомляем логиста
    if logist_id:

    # Сообщение
    text += f"✅ Предложение #{offer_id} отклонено\n"
    text += f"📋 Заявка #{request_id}\n\n"

    if reason:
        text += f"💬 Причина: <i>{reason}</i>\n\n"

    text += "ℹ️ Логист получил уведомление об отклонении"

    keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "📋 Другие предложения", callback_data=f"view_request_offers_{request_id}"
            )
        )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    if hasattr(callback_or_fake.message, "edit_text"):
        await callback_or_fake.message.edit_text(
            text, reply_markup=keyboard, parse_mode="HTML"
        )
    else:
        await callback_or_fake.message.answer(
            text, reply_markup=keyboard, parse_mode="HTML"
        )

    await state.finish()

    logging.info(f"❌ Экспортёр {user_id} отклонил предложение #{offer_id}")


# ============================================================================
# ЭКСПОРТЁР: УПРАВЛЕНИЕ ЗАЯВКАМИ НА ДОСТАВКУ
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "my_shipping_requests", state="*")
async def show_my_shipping_requests(callback: types.CallbackQuery, state: FSMContext):
    """Список заявок экспортёра на доставку"""
    await state.finish()

    user_id = callback.from_user.id


    if not my_requests:
        text += "<i>Создайте заявку из раздела 'Мои пулы'</i>"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("➕ Создать пул", callback_data="create_pull")
        )
        keyboard.add(
            InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return

    # Группируем по статусам
    by_status = {
        "active": [],
        "assigned": [],
        "in_progress": [],
        "completed": [],
        "cancelled": [],
    }


    # Сортируем по дате (новые первые)
    for status in by_status:
        by_status[status].sort(key=lambda x: x[1].get("created_at", ""), reverse=True)

    text += f"Всего заявок: <b>{len(my_requests)}</b>\n\n"

    # Статистика
    assigned = len(by_status["assigned"])
    in_progress = len(by_status["in_progress"])
    completed = len(by_status["completed"])

    text += f"🆕 Активные: <b>{active}</b>\n"
    text += f"👤 Назначены: <b>{assigned}</b>\n"
    text += f"🚚 В пути: <b>{in_progress}</b>\n"
    text += f"✅ Завершены: <b>{completed}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите заявку:"

    keyboard = InlineKeyboardMarkup(row_width=1)

    # Показываем заявки
    for status_key, status_name, emoji in [
        ("active", "Активные", "🆕"),
        ("assigned", "Назначены", "👤"),
        ("in_progress", "В пути", "🚚"),
        ("completed", "Завершены", "✅"),
    ]:
        requests = by_status[status_key]
        if requests:
                pull_id = req.get("pull_id")
                volume = req.get("volume", 0)

                # Подсчёт предложений
                offers_count = len(
                    [
                        o
                        for o in logistic_offers.values()
                    ]
                )

                    button_text += f" | 📬 {offers_count}"

                    keyboard.add(
                        InlineKeyboardButton(
                            button_text, callback_data=f"view_my_request_{req_id}"
                        )
                    )

    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="my_shipping_requests")
    )
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("view_my_request_"), state="*")
async def view_my_request_details(callback: types.CallbackQuery, state: FSMContext):
    """Детальный просмотр своей заявки"""
    await state.finish()
    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id

        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return

    pull_id = request.get("pull_id")
    text = f"📦 <b>ЗАЯВКА #{request_id}</b>\n\n"
    text += f"🌾 Культура: <b>{pull_info.get('culture', 'Не указана')}</b>\n"
    text += f"📦 Объём: <b>{request.get('volume', 0):.1f} т</b>\n"
    if request.get("budget"):
        text += f"💰 Бюджет: {request.get('budget'):,.0f} ₽\n"
    if request.get("requirements"):
        text += f"\n📋 Требования:\n{request.get('requirements')}\n"
    text += f"\n📅 Создана: {request.get('created_at', 'Не указано')}\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"

    status_icon = status_map.get(status, "⚪").split()[0]
    status_name = get_status_name(status)
    text += f"📊 Статус: <b>{status_icon} {status_name}</b>\n\n"

    all_offers = [
    ]

    text += f"  • Ожидают решения: <b>{len(pending_offers)}</b>\n"
    text += f"  • Принято: <b>{len(accepted_offers)}</b>\n"
    text += f"  • Всего: <b>{len(all_offers)}</b>\n\n"

        if logist_info.get("phone"):
            text += f"📞 Телефон: {logist_info.get('phone')}\n"

    keyboard = InlineKeyboardMarkup(row_width=2)
        if len(pending_offers) > 0:
            keyboard.add(
                InlineKeyboardButton(
                    f"📬 Предложения ({len(pending_offers)})",
                    callback_data=f"view_request_offers_{request_id}",
                )
            )
        keyboard.add(
            InlineKeyboardButton(
            ),
            InlineKeyboardButton(
                "❌ Отменить", callback_data=f"cancel_request_{request_id}"
            ),
        )
        keyboard.add(
            InlineKeyboardButton(
                "📦 Доставка", callback_data=f"view_delivery_by_request_{request_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton(
                "📬 Предложения", callback_data=f"view_request_offers_{request_id}"
            )
        )
    elif status in ["in_progress", "completed"]:
        keyboard.add(
            InlineKeyboardButton(
                "📦 Доставка", callback_data=f"view_delivery_by_request_{request_id}"
            )
        )

    keyboard.add(
        InlineKeyboardButton("🔙 К заявкам", callback_data="my_shipping_requests")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("cancel_request_"), state="*")
async def cancel_request_confirm(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение отмены заявки"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id

        await callback.answer("❌ Нет доступа", show_alert=True)
        return

        return

    # Подсчёт предложений
    pending_offers = [
        o
        for o in logistic_offers.values()
    ]

    text = f"❓ <b>ОТМЕНА ЗАЯВКИ #{request_id}</b>\n\n"

    if len(pending_offers) > 0:
        text += f"⚠️ У вас есть <b>{len(pending_offers)}</b> ожидающих предложений!\n"


    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Да, отменить", callback_data=f"confirm_cancel_request_{request_id}"
        ),
        InlineKeyboardButton("❌ Нет", callback_data=f"view_my_request_{request_id}"),
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("confirm_cancel_request_"), state="*"
)
async def cancel_request_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Отмена заявки подтверждена"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id

        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    # Отменяем заявку
    request["status"] = "cancelled"
    request["cancelled_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cancelled_offers = 0
    for offer_id, offer in logistic_offers.items():
            offer["status"] = "rejected"
            offer["rejection_reason"] = "Заявка отменена заказчиком"
            cancelled_offers += 1

        asyncio.create_task(
        )
            )

    save_shipping_requests()
    save_logistic_offers()

    text = f"✅ <b>ЗАЯВКА #{request_id} ОТМЕНЕНА</b>\n\n"

    if cancelled_offers > 0:
        text += f"\n📬 Отклонено предложений: {cancelled_offers}\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📦 Мои заявки", callback_data="my_shipping_requests")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer("✅ Заявка отменена")

    logging.info(f"❌ Экспортёр {user_id} отменил заявку #{request_id}")


# ============================================================================
# ЭКСПОРТЁР: ПРОСМОТР ДОСТАВОК
# ============================================================================
@dp.callback_query_handler(lambda c: c.data == "exporter_deliveries", state="*")
async def show_exporter_deliveries(callback: types.CallbackQuery, state: FSMContext):
    """Список доставок экспортёра"""
    await state.finish()

    user_id = callback.from_user.id


    if not my_deliveries:
        text = "📦 <b>МОИ ДОСТАВКИ</b>\n\n"
        text += "❌ У вас пока нет доставок\n\n"
        text += "<i>Доставки появятся после принятия предложений логистов</i>"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("📋 Мои заявки", callback_data="my_shipping_requests")
        )
        keyboard.add(
            InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return

    # Группируем по статусам
    by_status = {"pending": [], "in_progress": [], "completed": [], "cancelled": []}

    for deliv_id, deliv in my_deliveries:
        by_status[status].append((deliv_id, deliv))

    text += f"Всего доставок: <b>{len(my_deliveries)}</b>\n\n"

    pending = len(by_status["pending"])
    in_progress = len(by_status["in_progress"])
    completed = len(by_status["completed"])

    text += f"🕐 Ожидают начала: <b>{pending}</b>\n"
    text += f"🚚 В пути: <b>{in_progress}</b>\n"
    text += f"✅ Завершены: <b>{completed}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите доставку:"

    keyboard = InlineKeyboardMarkup(row_width=1)

    # Показываем доставки
    for status_key, status_name, emoji in [
        ("pending", "Ожидают", "🕐"),
        ("in_progress", "В пути", "🚚"),
        ("completed", "Завершены", "✅"),
    ]:
        delivs = by_status[status_key]
        if delivs:
            for deliv_id, deliv in delivs[:5]:
                route = f"{deliv.get('route_from', '')} → {deliv.get('route_to', '')}"
                volume = deliv.get("volume", 0)

                button_text = f"{emoji} #{deliv_id} | {route[:20]} | {volume:.0f}т"

                keyboard.add(
                    InlineKeyboardButton(
                        button_text, callback_data=f"view_delivery_{deliv_id}"
                    )
                )

    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="exporter_deliveries")
    )
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_delivery_by_request_"), state="*"
)
async def view_delivery_by_request(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

    try:
    except (IndexError, ValueError):
        return

    delivery_id = None
    for deliv_id, deliv in deliveries.items():
        delivery_id = deliv_id
        break

        return



# ============================================================================
# ЭКСПОРТЁР: ОЦЕНКА И ОТЗЫВЫ ЛОГИСТОВ
# ============================================================================

# Глобальная структура для хранения рейтингов
logistic_ratings = {}  # {logist_id: {'total_rating': 0, 'count': 0, 'reviews': []}}


def save_logistic_ratings():
    """Сохранить рейтинги логистов"""
    try:
        with open(os.path.join(DATA_DIR, "logistic_ratings.pkl"), "wb") as f:
            pickle.dump(logistic_ratings, f)
        logging.info("✅ Logistic ratings saved")
    except Exception as e:
        logging.error(f"❌ Error saving logistic ratings: {e}")


def load_logistic_ratings():
    """Загрузить рейтинги логистов"""
    global logistic_ratings
    try:
        filepath = os.path.join(DATA_DIR, "logistic_ratings.pkl")
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                logistic_ratings = pickle.load(f)
            logging.info(f"✅ Loaded {len(logistic_ratings)} logistic ratings")
        else:
            logistic_ratings = {}
    except Exception as e:
        logging.error(f"❌ Error loading logistic ratings: {e}")
        logistic_ratings = {}


class RateLogisticStatesGroup(StatesGroup):
    """FSM для оценки логиста"""

    delivery_id = State()
    rating = State()
    review = State()


@dp.callback_query_handler(lambda c: c.data.startswith("rate_logistic_"), state="*")
async def rate_logistic_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало оценки логиста"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Доставка не найдена", show_alert=True)
        return
    user_id = callback.from_user.id

    # Проверки
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

        await callback.answer(
            "❌ Можно оценить только завершённую доставку", show_alert=True
        )
        return

    if delivery.get("rated"):
        await callback.answer("❌ Вы уже оценили эту доставку", show_alert=True)
        return

    await state.update_data(delivery_id=delivery_id)

    logist_name = logist_info.get("company_name", "Не указана")

    text += f"🚚 Логист: <b>{logist_name}</b>\n"
    text += f"📦 Доставка #{delivery_id}\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>Поставьте оценку работе логиста:</b>\n\n"
    text += "⭐⭐⭐⭐⭐ — Отлично\n"
    text += "⭐⭐⭐⭐ — Хорошо\n"
    text += "⭐⭐⭐ — Нормально\n"
    text += "⭐⭐ — Плохо\n"
    text += "⭐ — Очень плохо"

    keyboard = InlineKeyboardMarkup(row_width=5)
    keyboard.add(
        InlineKeyboardButton("⭐", callback_data="rate_1"),
        InlineKeyboardButton("⭐⭐", callback_data="rate_2"),
        InlineKeyboardButton("⭐⭐⭐", callback_data="rate_3"),
        InlineKeyboardButton("⭐⭐⭐⭐", callback_data="rate_4"),
        InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="rate_5"),
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="exporter_deliveries"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await RateLogisticStatesGroup.rating.set()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("rate_"), state=RateLogisticStatesGroup.rating
)
async def rate_logistic_rating_selected(
    callback: types.CallbackQuery, state: FSMContext
):
    """Выбор оценки"""

    try:
        rating = int(callback.data.split("_")[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if rating < 1 or rating > 5:
        await callback.answer("❌ Некорректная оценка", show_alert=True)
        return

    await state.update_data(rating=rating)

    stars = "⭐" * rating

    text = f"{stars} <b>ОЦЕНКА: {rating}/5</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>Напишите отзыв о работе логиста</b>\n"
    text += "<i>(необязательно, до 500 символов)</i>\n\n"
    text += "Например:\n"
    text += "• Быстрая доставка\n"
    text += "• Профессиональный подход\n"
    text += "• Груз доставлен в целости\n\n"
    text += "Или нажмите 'Пропустить'"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("➡️ Пропустить", callback_data="skip_review"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="exporter_deliveries"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await RateLogisticStatesGroup.review.set()
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data == "skip_review", state=RateLogisticStatesGroup.review
)
async def rate_logistic_skip_review(callback: types.CallbackQuery, state: FSMContext):
    """Пропустить отзыв"""
    await rate_logistic_save(callback, state, None)


@dp.message_handler(state=RateLogisticStatesGroup.review)
async def rate_logistic_review_entered(message: types.Message, state: FSMContext):
    """Ввод отзыва"""
    review = message.text.strip()

    if len(review) > 500:
        await message.answer(
            "❌ Слишком длинный отзыв (максимум 500 символов)!\n\nВведите короче:"
        )
        return

    await rate_logistic_save(fake_callback, state, review)


async def rate_logistic_save(callback_or_fake, state: FSMContext, review: str = None):
    """Сохранение оценки и отзыва"""

    data = await state.get_data()
    delivery_id = data.get("delivery_id")
    rating = data.get("rating")

    user_id = callback_or_fake.from_user.id
        if hasattr(callback_or_fake, "answer"):
            await callback_or_fake.answer("❌ Доставка не найдена", show_alert=True)
        return

    # Сохраняем оценку в доставке
    delivery["rated"] = True
    delivery["rating"] = rating
    if review:
        delivery["review"] = review
    delivery["rated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Обновляем рейтинг логиста
    if logist_id not in logistic_ratings:
        logistic_ratings[logist_id] = {"total_rating": 0, "count": 0, "reviews": []}

    logistic_ratings[logist_id]["total_rating"] += rating
    logistic_ratings[logist_id]["count"] += 1

    if review:
        logistic_ratings[logist_id]["reviews"].append(
            {
                "delivery_id": delivery_id,
                "rating": rating,
                "review": review,
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    # Вычисляем средний рейтинг
    avg_rating = (
        logistic_ratings[logist_id]["total_rating"]
        / logistic_ratings[logist_id]["count"]
    )

    save_deliveries()
    save_logistic_ratings()

    logist_name = logist_info.get("company_name", "Не указана")

    stars = "⭐" * rating

    text += f"{stars} <b>{rating}/5</b>\n\n"
    text += f"🚚 Логист: {logist_name}\n"
    text += f"📦 Доставка #{delivery_id}\n\n"

    if review:
        text += f"💬 Ваш отзыв:\n<i>{review}</i>\n\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"📊 Средний рейтинг логиста: <b>{avg_rating:.1f}/5</b>\n"
    text += f"📋 Всего оценок: {logistic_ratings[logist_id]['count']}\n\n"
    text += "Ваша оценка поможет другим пользователям!"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📦 Мои доставки", callback_data="exporter_deliveries")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    if hasattr(callback_or_fake.message, "edit_text"):
        try:
            await callback_or_fake.message.edit_text(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
            await callback_or_fake.message.answer(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
    else:
        await callback_or_fake.message.answer(
            text, reply_markup=keyboard, parse_mode="HTML"
        )

    await state.finish()

    logging.info(f"⭐ Экспортёр {user_id} оценил логиста {logist_id} на {rating}/5")


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_logistic_profile_"), state="*"
)
async def view_logistic_profile(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр профиля и рейтинга логиста"""
    await state.finish()

    try:
        logist_id = int(callback.data.split("_")[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Логист не найден", show_alert=True)
        return

    text += f"🏢 Компания: <b>{logist_info.get('company_name', 'Не указана')}</b>\n"
    text += f"📞 Телефон: {logist_info.get('phone', 'Не указан')}\n"

    if logist_info.get("email"):
        text += f"📧 Email: {logist_info.get('email')}\n"


    # Рейтинг
    if logist_id in logistic_ratings:
        rating_data = logistic_ratings[logist_id]
        avg_rating = rating_data["total_rating"] / rating_data["count"]
        stars = "⭐" * int(round(avg_rating))

        text += f"{stars} <b>{avg_rating:.1f}/5</b>\n"
        text += f"📋 Оценок: {rating_data['count']}\n\n"
    else:

    # Статистика доставок
    logist_deliveries = [
    ]

    in_progress = len(
    )

    text += f"✅ Завершено доставок: {completed}\n"
    text += f"🚚 В процессе: {in_progress}\n"
    text += f"📋 Всего: {len(logist_deliveries)}\n\n"

    # Последние отзывы
    if logist_id in logistic_ratings and logistic_ratings[logist_id]["reviews"]:
        text += "━━━━━━━━━━━━━━━━━━━━\n\n"

        reviews = logistic_ratings[logist_id]["reviews"][-3:]  # Последние 3
        for r in reversed(reviews):
            stars = "⭐" * r["rating"]
            text += f"{stars} {r['rating']}/5\n"
            text += f"<i>{r['review']}</i>\n"
            text += f"<code>{r['date']}</code>\n\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# ============================================================================
# ЭКСПЕДИТОР: УПРАВЛЕНИЕ ПРЕДЛОЖЕНИЯМИ И ЗАПРОСАМИ
# ============================================================================


@dp.callback_query_handler(lambda c: c.data == "expeditor_my_offers", state="*")
async def show_expeditor_my_offers(callback: types.CallbackQuery, state: FSMContext):
    """Список предложений экспедитора"""
    await state.finish()

    user_id = callback.from_user.id


    if not my_offers:
        text += "❌ У вас пока нет предложений\n\n"
        text += "<i>Создайте предложение в разделе 'Создать предложение'</i>"

        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton(
                "➕ Создать предложение", callback_data="create_expeditor_offer"
            )
        )
        keyboard.add(
            InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return

    # Группируем по статусам

    for offer_id, offer in my_offers:
        if status in by_status:
            by_status[status].append((offer_id, offer))

    text += f"Всего предложений: <b>{len(my_offers)}</b>\n\n"

    active = len(by_status["active"])
    selected = len(by_status["selected"])

    text += f"🆕 Активные: <b>{active}</b>\n"
    text += f"✅ Выбраны: <b>{selected}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите предложение:"

    keyboard = InlineKeyboardMarkup(row_width=1)

    # Показываем предложения
    for status_key, status_name, emoji in [
        ("active", "Активные", "🆕"),
        ("selected", "Выбраны", "✅"),
    ]:
        offers = by_status[status_key]
        if offers:
            for offer_id, offer in offers[:5]:
                service = offer.get("service_type", "Услуга")
                price = offer.get("price", 0)

                button_text = f"{emoji} #{offer_id} | {service[:20]} | {price:,.0f}₽"

                keyboard.add(
                    InlineKeyboardButton(
                        button_text, callback_data=f"view_expeditor_offer_{offer_id}"
                    )
                )

    keyboard.add(
        InlineKeyboardButton(
            "➕ Новое предложение", callback_data="create_expeditor_offer"
        )
    )
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="expeditor_my_offers")
    )
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
)
async def view_expeditor_offer_details(
    callback: types.CallbackQuery, state: FSMContext
):
    """Детальный просмотр предложения экспедитора"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    user_id = callback.from_user.id

        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return

    text = f"📋 <b>ПРЕДЛОЖЕНИЕ #{offer_id}</b>\n\n"
    text += f"📦 Услуга: <b>{offer.get('service_type', 'Не указана')}</b>\n"
    text += f"🚢 Порты: {offer.get('ports', 'Не указаны')}\n"
    text += f"💰 Цена: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
    text += f"📅 Сроки: {offer.get('terms', 'Не указаны')}\n\n"

    if offer.get("description"):
        text += f"📝 Описание:\n<i>{offer.get('description')}</i>\n\n"

    text += f"📅 Создано: {offer.get('created_at', 'Не указано')}\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"

    # Статус

    if status == "active":
        text += "📊 Статус: <b>🆕 Активно</b>\n"
        text += "Ваше предложение видно экспортёрам"
    elif status == "selected":
        text += "📊 Статус: <b>✅ Выбрано экспортёром</b>\n"
        if offer.get("exporter_id"):
            text += (
                f"\n🏢 Экспортёр: {exporter_info.get('company_name', 'Не указано')}\n"
            )
            text += f"📞 Телефон: {exporter_info.get('phone', 'Не указан')}"
    elif status == "cancelled":
        text += "📊 Статус: <b>❌ Отменено</b>"

    keyboard = InlineKeyboardMarkup(row_width=2)

    if status == "active":
        keyboard.add(
            InlineKeyboardButton(
                "✏️ Редактировать", callback_data=f"edit_expeditor_offer_{offer_id}"
            ),
            InlineKeyboardButton(
                "❌ Отменить", callback_data=f"cancel_expeditor_offer_{offer_id}"
            ),
        )

    keyboard.add(
        InlineKeyboardButton("🔙 К предложениям", callback_data="expeditor_my_offers")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("cancel_expeditor_offer_"), state="*"
)
async def cancel_expeditor_offer(callback: types.CallbackQuery, state: FSMContext):
    """Отмена предложения экспедитора"""
    await state.finish()

    try:
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    user_id = callback.from_user.id

        await callback.answer("❌ Нет доступа", show_alert=True)
        return

        await callback.answer(
            "❌ Можно отменить только активные предложения", show_alert=True
        )
        return

    # Отменяем
    offer["status"] = "cancelled"
    offer["cancelled_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    save_expeditor_offers()

    text = f"✅ <b>ПРЕДЛОЖЕНИЕ #{offer_id} ОТМЕНЕНО</b>\n\n"
    text += "Предложение больше не будет показываться экспортёрам"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("💼 Мои предложения", callback_data="expeditor_my_offers")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer("✅ Предложение отменено")

    logging.info(f"❌ Экспедитор {user_id} отменил предложение #{offer_id}")


@dp.callback_query_handler(lambda c: c.data == "expeditor_available_pulls", state="*")
async def show_expeditor_available_pulls(
    callback: types.CallbackQuery, state: FSMContext
):
    """Просмотр доступных пулов для экспедитора"""
    await state.finish()

    user_id = callback.from_user.id

    # Получаем предложения экспедитора для определения портов
    my_offers = [
        offer
        for offer in expeditor_offers.values()
    ]

    # Собираем порты из предложений
    my_ports = set()
    for offer in my_offers:
        ports_str = offer.get("ports", "")
        if ports_str:
            ports_list = [p.strip() for p in ports_str.split(",")]
            my_ports.update([p.lower() for p in ports_list])

    # ✅ ИСПРАВЛЕНО: Ищем подходящие пулы
    suitable_pulls = []
    all_pulls = pulls.get("pulls", {})  # ✅ ДОБАВЛЕНО

    for pull_id, pull in all_pulls.items():  # ✅ ИСПРАВЛЕНО
        if not isinstance(pull, dict):  # ✅ ДОБАВЛЕНО
            continue

            continue

        pull_port = pull.get("port", "").lower()

        # Проверяем совпадение портов
        if not my_ports or any(port in pull_port for port in my_ports):
            suitable_pulls.append((pull_id, pull))

    if not suitable_pulls:
        text = "🚢 <b>ДОСТУПНЫЕ ПУЛЫ</b>\n\n"

        keyboard = InlineKeyboardMarkup()
            keyboard.add(
                InlineKeyboardButton(
                    "➕ Создать предложение", callback_data="create_expeditor_offer"
                )
            )
        else:

        keyboard.add(
            InlineKeyboardButton(
                "🔄 Обновить", callback_data="expeditor_available_pulls"
            )
        )

        keyboard.add(
            InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        return

    # Сортируем по дате (новые первые)
    suitable_pulls.sort(key=lambda x: x[1].get("created_at", ""), reverse=True)

    text += f"Найдено пулов: <b>{len(suitable_pulls)}</b>\n"

    if my_ports:
        text += f"Ваши порты: <i>{', '.join(my_ports)}</i>\n"

    text += "\n━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите пул:"

    keyboard = InlineKeyboardMarkup(row_width=1)

    for pull_id, pull in suitable_pulls[:10]:
        culture = pull.get("culture", "Не указана")
        volume = pull.get("current_volume", 0)
        port = pull.get("port", "Не указан")

        button_text = f"📦 #{pull_id} | {culture} {volume:.0f}т | {port}"

        keyboard.add(
            InlineKeyboardButton(
                button_text, callback_data=f"view_pull_for_expeditor_{pull_id}"
            )
        )

    if len(suitable_pulls) > 10:
        keyboard.add(
            InlineKeyboardButton(
                f"➕ Показать ещё {len(suitable_pulls) - 10}",
                callback_data="show_more_pulls_expeditor",
            )
        )

    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="expeditor_available_pulls")
    )
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(
    lambda c: c.data.startswith("view_pull_for_expeditor_"), state="*"
)
async def view_pull_for_expeditor(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр пула для экспедитора"""
    await state.finish()

    try:
        pull_id = int(callback.data.split("_")[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return


    if not pull:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    text = f"📦 <b>ПУЛ #{pull_id}</b>\n\n"
    text += f"🌾 Культура: <b>{pull.get('culture', 'Не указана')}</b>\n"
    text += f"📦 Объём: <b>{pull.get('current_volume', 0):.1f} т</b>\n"
    text += f"🚢 Порт отгрузки: {pull.get('port', 'Не указан')}\n"
    text += f"📅 Дата отгрузки: {pull.get('shipment_date', 'Не указана')}\n\n"

    # Информация об экспортёре
    exporter_id = pull.get("exporter_id")
    if exporter_id:
        text += f"Компания: {exporter_info.get('company_name', 'Не указана')}\n"
        text += f"Телефон: {exporter_info.get('phone', 'Не указан')}\n"
        if exporter_info.get("email"):
            text += f"Email: {exporter_info.get('email')}\n"
        text += "\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "💡 <b>Для сотрудничества:</b>\n"
    text += "Свяжитесь с экспортёром напрямую\n"
    text += "или отправьте предложение услуг"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("💼 Мои предложения", callback_data="expeditor_my_offers")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 К пулам", callback_data="expeditor_available_pulls")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "expeditor_statistics", state="*")
async def show_expeditor_statistics(callback: types.CallbackQuery, state: FSMContext):
    """Статистика экспедитора"""
    await state.finish()

    user_id = callback.from_user.id


    total_offers = len(my_offers)

    # Подсчёт пулов по портам
    ports_dict = {}
    for offer in my_offers:
        ports_str = offer.get("ports", "")
        if ports_str:
            for port in ports_str.split(","):
                port = port.strip()
                ports_dict[port] = ports_dict.get(port, 0) + 1

    text += f"📋 Всего предложений: <b>{total_offers}</b>\n"
    text += f"🆕 Активных: <b>{active_offers}</b>\n"
    text += f"✅ Выбрано: <b>{selected_offers}</b>\n\n"

    if selected_offers > 0 and total_offers > 0:
        success_rate = (selected_offers / total_offers) * 100
        text += f"📈 Процент успеха: <b>{success_rate:.1f}%</b>\n\n"

    if ports_dict:
        for port, count in sorted(ports_dict.items(), key=lambda x: x[1], reverse=True)[
            :5
        ]:
            text += f"🚢 {port}: {count} предложений\n"
        text += "\n"

    # Средняя цена услуг
    prices = [o.get("price", 0) for o in my_offers if o.get("price")]
    if prices:
        avg_price = sum(prices) / len(prices)
        text += f"💰 Средняя цена: <b>{avg_price:,.0f} ₽</b>\n"
        text += f"💵 Мин. цена: {min(prices):,.0f} ₽\n"
        text += f"💸 Макс. цена: {max(prices):,.0f} ₽\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("💼 Мои предложения", callback_data="expeditor_my_offers")
    )
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="expeditor_statistics")
    )
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()


# ============= ИНТЕРАКТИВНЫЙ ОБРАБОТЧИК: МОИ СДЕЛКИ С ФИЛЬТРАЦИЕЙ И ДЕТАЛЯМИ =============


# FSM состояния для просмотра сделок
class DealView(StatesGroup):
    viewing_deals = State()
    viewing_detail = State()


deals_cache = {}


# ==========================@==================================================
@dp.message_handler(lambda msg: msg.text in ["📋 Мои сделки"])
async def show_user_deals(message: types.Message, state: FSMContext):
    """✅ ГЛАВНЫЙ ОБРАБОТЧИК - показывает ВСЕ статусы, кнопки ТОЛЬКО если > 0"""
    await state.finish()

    user_id = message.from_user.id
    user_role = user_data.get("role", "unknown")
    user_name = user_data.get("name", "Пользователь")

    logging.critical(f"📋 МОИ СДЕЛКИ/ПАРТИИ: user_id={user_id}, role={user_role}")

    try:
        keyboard = InlineKeyboardMarkup(row_width=1)
        text = ""

        # ============================================================================
        # 👨‍🌾 ФЕРМЕР
        # ============================================================================
        if user_role == "farmer":

            # Нормализуем статусы - приводим к нижнему регистру
            active = len(
                [
                    d
                    for d in user_batches
                ]
            )
            reserved = len(
                [
                    d
                    for d in user_batches
                    or d.get("reserved_volume", 0) > 0
                ]
            )
            sold = len(
                [
                    d
                    for d in user_batches
                    or d.get("sold")
                ]
            )
            canceled = len(
                [
                    d
                    for d in user_batches
                ]
            )
            matches = len([d for d in user_batches if len(d.get("matches", [])) > 0])

            total = active + reserved + sold + canceled
            total_volume = sum([b.get("volume", 0) for b in user_batches])
            total_price = sum(
                [b.get("volume", 0) * b.get("price", 0) for b in user_batches]
            )

            text = (
                f"👤 {user_name}\n"
                f"📊 Всего: <b>{total}</b>\n"
                f"📦 Объём: {total_volume:,.0f}т\n"
                f"💰 Сумма: {total_price:,.0f}₽\n\n"
                f"✅ Активные: {active}\n"
                f"🔒 Зарезервированные: {reserved}\n"
                f"💰 Проданные: {sold}\n"
                f"❌ Снятые с продажи: {canceled}\n"
                f"🎯 С совпадениями: {matches}\n\n"
            )

            # ТОЛЬКО кнопки если COUNT > 0
            if active > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"✅ Активные ({active})",
                        callback_data="deals_status:farmer:active",
                    )
                )
            if reserved > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"🔒 Зарезервированные ({reserved})",
                        callback_data="deals_status:farmer:reserved",
                    )
                )
            if sold > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"💰 Проданные ({sold})",
                        callback_data="deals_status:farmer:sold",
                    )
                )
            if canceled > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"❌ Снятые с продажи ({canceled})",
                        callback_data="deals_status:farmer:canceled",
                    )
                )
            if matches > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"🎯 С совпадениями ({matches})",
                        callback_data="deals_status:farmer:matches",
                    )
                )

            if total == 0:
                text = (
                    f"👤 {user_name}\n"
                    "📭 <b>Партий не найдено</b>\n\n"
                    "💡 <b>Подсказка:</b> Создайте новую партию через '➕ Добавить партию'"
                )

        # ============================================================================
        # 📤 ЭКСПОРТЕР
        # ============================================================================
        elif user_role == "exporter":
            # ✅ ИСПРАВЛЕНО: Получаем пулы из правильного места
            all_pulls_dict = pulls.get("pulls", {})

            active = len(
                [
                    d
                ]
            )
            filled = len(
                [
                    d
                ]
            )
            closed = len(
                [
                    d
                ]
            )
            completed = len(
                [
                    d
                ]
            )
            cancelled = len(
                [
                    d
                ]
            )

            total_price = sum(
                [
                ]
            )

            text = (
                f"👤 {user_name}\n"
                f"📊 Всего: <b>{len(all_pulls)}</b>\n"
                f"📦 Объём: {total_volume:,.0f}т\n"
                f"💰 Сумма: {total_price:,.0f}₽\n\n"
                f"✅ Активные: {active}\n"
                f"🔒 Заполненные: {filled}\n"
                f"💰 Закрытые: {closed}\n"
                f"❌ Отмененные: {cancelled}\n"
                f"🎯 Завершенные: {completed}\n\n"
            )

            # ТОЛЬКО кнопки если COUNT > 0
            if active > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"✅ Активные ({active})",
                        callback_data="deals_status:exporter:active",
                    )
                )
            if filled > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"🔒 Заполненные ({filled})",
                        callback_data="deals_status:exporter:filled",
                    )
                )
            if closed > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"💰 Закрытые ({closed})",
                        callback_data="deals_status:exporter:closed",
                    )
                )
            if cancelled > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"❌ Отмененные ({cancelled})",
                        callback_data="deals_status:exporter:cancelled",
                    )
                )
            if completed > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"🎯 Завершенные ({completed})",
                        callback_data="deals_status:exporter:completed",
                    )
                )

            if len(all_pulls) == 0:
                text = (
                    f"👤 {user_name}\n"
                    "📭 <b>Пулов не найдено</b>\n\n"
                    "💡 <b>Подсказка:</b> Создайте новый пул через '➕ Создать пул'"
                )

        # ============================================================================
        # 🚚 ЛОГИСТ
        # ============================================================================

            pending = len(
                [
                    d
                ]
            )
            in_progress = len(
                [
                    d
                ]
            )
            completed = len(
                [
                    d
                ]
            )


            text = (
                f"👤 {user_name}\n"
                f"📊 Всего: <b>{len(all_orders)}</b>\n"
                f"📦 Объём: {total_volume:,.0f}т\n"
                f"💰 Сумма: {total_price:,.0f}₽\n\n"
                f"⏳ Ожидающие: {pending}\n"
                f"🚗 В пути: {in_progress}\n"
                f"✅ Доставлено: {completed}\n\n"
            )

            # ТОЛЬКО кнопки если COUNT > 0
            if pending > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"⏳ Ожидающие ({pending})",
                        callback_data="deals_status:logist:pending",
                    )
                )
            if in_progress > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"🚗 В пути ({in_progress})",
                        callback_data="deals_status:logist:in_progress",
                    )
                )
            if completed > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"✅ Доставлено ({completed})",
                        callback_data="deals_status:logist:completed",
                    )
                )

            if len(all_orders) == 0:
                text = (
                    f"👤 {user_name}\n"
                    "📭 <b>Доставок не найдено</b>"
                )

        # ============================================================================
        # ✈️ ЭКСПЕДИТОР
        # ============================================================================
                    }

            active = len(
                [
                    d
                ]
            )
            in_progress = len(
                [
                    d
                ]
            )
            delivered = len(
                [
                    d
                ]
            )


            text = (
                f"👤 {user_name}\n"
                f"📦 Объём: {total_volume:,.0f}т\n"
                f"💰 Сумма: {total_price:,.0f}₽\n\n"
                f"🟢 Активные: {active}\n"
                f"⏳ В пути: {in_progress}\n"
                f"✅ Доставлено: {delivered}\n\n"
            )

            # ТОЛЬКО кнопки если COUNT > 0
            if active > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"🟢 Активные ({active})",
                        callback_data="deals_status:expeditor:active",
                    )
                )
            if in_progress > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"⏳ В пути ({in_progress})",
                        callback_data="deals_status:expeditor:in_progress",
                    )
                )
            if delivered > 0:
                keyboard.add(
                    InlineKeyboardButton(
                        f"✅ Доставлено ({delivered})",
                    )
                )

                text = (
                    f"👤 {user_name}\n"
                    "📭 <b>Маршрутов не найдено</b>"
                )

        else:
            text = f"❓ Неизвестная роль: {user_role}"

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
        logging.info(f"✅ Главное меню отправлено: {user_role}")

    except Exception as e:
        logging.error(f"❌ ОШИБКА: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {str(e)}")


@dp.callback_query_handler(
)
    """✅ Возврат в главное меню для ВСЕХ кнопок назад"""
    user_id = callback.from_user.id
    current_time = time.time()
    last_click = last_back_click.get(user_id, 0)

    # ✅ ANTISPAM: игнорируем клики быстрее 0.5 секунды
    if current_time - last_click < 0.5:
        logging.info(f"⏱️ Защита от спама: {user_id} нажимает слишком быстро")
        await callback.answer("⏱️ Подождите немного...", show_alert=False)
        return

    last_back_click[user_id] = current_time
    await callback.answer()

    try:
        if state:
            try:
                await state.finish()

        role = user_data.get("role", "unknown").lower()

        logging.info(f"⬅️ Возврат в главное меню: role={role}, user_id={user_id}")

        # ✅ ВСЕГДА ИНИЦИАЛИЗИРУЕМ VARIABLES В НАЧАЛЕ!
        text += f"👤 Роль: <b>{role}</b>\n"
        text += f"👤 Пользователь: {user_data.get('name', 'Unknown')}\n\n"

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(InlineKeyboardButton("📋 Мои сделки", callback_data="my_deals"))
        keyboard.add(InlineKeyboardButton("📊 Статистика", callback_data="stats"))
        keyboard.add(InlineKeyboardButton("⚙️ Настройки", callback_data="settings"))

        # ✅ TRY EDIT
        try:
            await callback.message.edit_text(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
        except Exception as e:
            error_str = str(e).lower()
            if "not modified" in error_str:
            else:
                logging.warning(f"⚠️ edit_text ошибка: {error_str}")
                try:
                    await callback.message.answer(
                        text, reply_markup=keyboard, parse_mode="HTML"
                    )
                except Exception as e2:
                    logging.error(f"❌ answer ошибка: {e2}")

    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            recovery_kb = InlineKeyboardMarkup()
            recovery_kb.add(
                InlineKeyboardButton("🏠 Повторить", callback_data="back_to_main")
            )
            await callback.message.edit_text(
                f"❌ Ошибка: {str(e)[:100]}",
                reply_markup=recovery_kb,
                parse_mode="HTML",
            )


# ============================================================================
# ФИЛЬТР ПО СТАТУСУ
# ============================================================================
@dp.callback_query_handler(lambda q: q.data.startswith("deals_status:"))
async def filter_deals_by_status(callback: types.CallbackQuery):
    await callback.answer()

    try:
        parts = callback.data.split(":")
        status = parts[2]
        user_id = callback.from_user.id

        logging.info(f"📊 Фильтр: role={role}, status={status}, user_id={user_id}")

        filtered_items = []
        logging.info(f"   🔍 Ищу статус: '{search_status}'")

        # ✅ ЭМОДЗИ КУЛЬТУР
        culture_emoji = {
            "пшеница": "🌾",
            "ячмень": "🌾",
            "кукуруза": "🌽",
            "подсолнечник": "🌻",
            "соя": "🫘",
            "рапс": "🌿",
        }

        # ════════════════════════════════════════════════════════════════════
        # 👨‍🌾 ФЕРМЕР - БАТЧИ
        # ════════════════════════════════════════════════════════════════════
        if role == "farmer":
            }

            logging.info(f"   📦 Батчей у фермера: {len(user_batches)}")

            for idx, batch in enumerate(user_batches):
                batch_status = batch.get("status", "Активна")
                logging.info(
                    f"      Батч #{idx}: '{batch.get('culture', 'N/A')}' → статус='{batch_status}'"
                )

                    filtered_items.append(
                        {
                            "index": idx,
                            "culture": batch.get("culture", "N/A"),
                            "volume": batch.get("volume", 0),
                            "price": batch.get("price", 0),
                            "status": batch_status,
                        }
                    )

            logging.info(
                f"   ✅ Всего найдено батчей: {len(filtered_items)} из {len(user_batches)}"
            )

        # ════════════════════════════════════════════════════════════════════
        # 📤 ЭКСПОРТЕР - ПУЛЫ
        # ════════════════════════════════════════════════════════════════════
        elif role == "exporter":
            pull_count = 0
            all_pulls = pulls.get("pulls", {})

            for pull_id, pull in all_pulls.items():
                if not isinstance(pull, dict):
                    continue

                if (
                    pull_count += 1

                    logging.info(
                        f"      Пул #{pull.get('id')}: '{pull.get('culture', 'N/A')}' → статус='{pull_status}'"
                    )

                        filtered_items.append(
                            {
                                "index": pull.get("id"),
                                "culture": pull.get("culture", "N/A"),
                                "volume": pull.get("current_volume", 0),
                                "target": pull.get("target_volume", 0),
                                "status": pull_status,
                            }
                        )

            logging.info(
                f"   ✅ Всего найдено пулов: {len(filtered_items)} из {pull_count}"
            )

        # ════════════════════════════════════════════════════════════════════
        # 🚚 ЛОГИСТ - ДОСТАВКИ
        # ════════════════════════════════════════════════════════════════════
            ship_count = 0
                    ship_count += 1

                    logging.info(
                    )

                        filtered_items.append(
                            {
                                "status": ship_status,
                            }
                        )

            logging.info(
                f"   ✅ Всего найдено доставок: {len(filtered_items)} из {ship_count}"
            )

        # ════════════════════════════════════════════════════════════════════
        # ✈️ ЭКСПЕДИТОР - МАРШРУТЫ
        # ════════════════════════════════════════════════════════════════════
            offer_count = 0
            for offer_id, offer in expeditor_offers.items():
                    offer_count += 1
                    offer_status = offer.get("status", "Открыт")

                    logging.info(
                        f"      Маршрут #{offer.get('id')}: {offer.get('from_port')}→{offer.get('to_port')} → статус='{offer_status}'"
                    )

                        filtered_items.append(
                            {
                                "index": offer.get("id"),
                                "volume": offer.get("max_volume", 0),
                                "price": offer.get("price", 0),
                                "status": offer_status,
                    }
                )

            logging.info(
            )

        # ════════════════════════════════════════════════════════════════════
        # ✅ КОМПАКТНЫЙ ТЕКСТ
        # ════════════════════════════════════════════════════════════════════
        status_titles = {
            "active": "АКТИВНЫЕ",
            "filled": "ЗАПОЛНЕННЫЕ",
            "closed": "ЗАКРЫТЫЕ",
            "cancelled": "ОТМЕНЕННЫЕ",
            "completed": "ЗАВЕРШЕННЫЕ",
        }

        title = status_titles.get(search_status, search_status.upper())

        if filtered_items:
            text = f"<b>✅ {title}</b> ({len(filtered_items)} шт.)\n\n"
            text += "<i>Выберите элемент для просмотра деталей:</i>"
        else:
            text = f"📭 <b>Нет {title.lower()} элементов</b>"
            logging.warning(f"   ⚠️ НЕ НАЙДЕНО элементов со статусом '{search_status}'")

        # ════════════════════════════════════════════════════════════════════
        # ✅ КНОПКИ С ЭМОДЗИ КУЛЬТУР
        # ════════════════════════════════════════════════════════════════════
        keyboard = InlineKeyboardMarkup(row_width=1)

        if filtered_items:
            for item in filtered_items:

                if role == "farmer":
                    culture = item.get("culture", "").lower()
                    emoji = culture_emoji.get(culture, "🌾")
                    button_text = f"{emoji} {item['culture']}"

                elif role == "exporter":
                    culture = item.get("culture", "").lower()
                    emoji = culture_emoji.get(culture, "🌾")
                    progress = 0
                    if item["target"] > 0:
                        progress = (item["volume"] / item["target"]) * 100

                    button_text = f"🚚 {item['from']} → {item['to']}"

                keyboard.add(
                    InlineKeyboardButton(button_text, callback_data=callback_data)
                )

        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        logging.info(f"   ✅ Показано {len(filtered_items)} элементов пользователю")

    except Exception as e:
        logging.error(f"❌ Ошибка в фильтре: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)[:50]}", show_alert=True)


# ============================================================================
# ОБРАБОТЧИК ДЕТАЛЕЙ СДЕЛКИ - ПОЛНАЯ ИНФОРМАЦИЯ ОБО ВСЕЙ ЦЕПОЧКЕ
# ============================================================================
# ============================================================================
# 🏠 ГЛАВНОЕ МЕНЮ - обработчик
# ============================================================================


# ════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════════════════════
# ГЛОБАЛЬНЫЕ ФУНКЦИИ
# ════════════════════════════════════════════════════════════════════════════════════


def get_safe_float(value, default=0):
    """Безопасно преобразует значение в float"""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def get_pull_by_batch_v3(batch_id):
    """✅ ОКОНЧАТЕЛЬНАЯ ВЕРСИЯ: Поиск пула по batch_id"""
    try:
        # Нормализуем batch_id к int
        try:
            search_batch_id = int(batch_id)
        except (ValueError, TypeError):
            logging.error(f"❌ Невозможно преобразовать batch_id={batch_id} в int")
            return None, None

        logging.info(
            f"🔍 Ищу пул для batch_id={search_batch_id} (type={type(search_batch_id).__name__})"
        )

        logging.info(f"📋 pullparticipants.keys() = {list(pullparticipants.keys())}")

        if not pullparticipants:
            return None, None

        # Ищем через ВСЕ пулы
        for pull_id, participants_list in pullparticipants.items():
            logging.info(
                f"   🔎 Проверяю pull_id={pull_id} (type={type(pull_id).__name__})"
            )

            # participants_list должен быть список
            if not isinstance(participants_list, list):
                logging.warning(
                    f"      ⚠️ participants для pull_id={pull_id} НЕ список! type={type(participants_list)}"
                )
                continue

            logging.info(
                f"      📍 Участников в pull_id={pull_id}: {len(participants_list)}"
            )

            # Проверяем каждого участника
            for idx, participant in enumerate(participants_list):
                if not isinstance(participant, dict):
                    logging.warning(f"         ⚠️ Участник {idx} НЕ dict!")
                    continue

                participant_batch_id = participant.get("batch_id")
                logging.info(
                    f"         👤 Участник {idx}: batch_id={participant_batch_id} (type={type(participant_batch_id).__name__})"
                )

                # ✅ КЛЮЧЕВОЕ СРАВНЕНИЕ
                    logging.info(
                        f"         🎯 МАТЧ! {participant_batch_id} == {search_batch_id}"
                    )

                    # Теперь ищем сам пул в pulls['pulls']
                    pulls_dict = pulls.get("pulls", {})
                    logging.info(
                        f"         📦 pulls['pulls'].keys() = {list(pulls_dict.keys())}"
                    )

                    # Пул может быть с ключом str или int
                    pull = (
                        pulls_dict.get(pull_id)
                        or pulls_dict.get(
                            int(pull_id)
                            if isinstance(pull_id, str) and pull_id.isdigit()
                            else None
                        )
                    )

                    if pull:
                        logging.info(
                            f"         ✅✅✅ УСПЕХ! Найден пул pull_id={pull_id}"
                        )
                        logging.info(
                            f"              Пул данные: culture={pull.get('culture')}, volume={pull.get('current_volume')}/{pull.get('target_volume')}"
                        )
                        return pull, str(pull_id)
                    else:
                        logging.warning(
                            f"         ⚠️ Пул pull_id={pull_id} НЕ найден в pulls['pulls']!"
                        )

        logging.warning(f"❌ batch_id={search_batch_id} не найдена ни в одном пуле")
        return None, None

    except Exception as e:
        logging.error(f"❌ ОШИБКА в get_pull_by_batch_v3: {e}", exc_info=True)
        return None, None


def get_batches_by_pull(pull_id):
    """Находит все партии пула"""
    try:
        pulls_dict = pulls.get("pulls", {})
    return []


def get_farmers_by_pull(pull_id):
    """Находит всех фермеров пула"""
    try:
        pulls_dict = pulls.get("pulls", {})
    return []


def get_logistics_for_pull(pull_id):
    """Находит логистику для пула"""
    try:
        for logistic_id, logistic in logistics_requests.items():
                return logistic, logistic_id
    return None, None


def get_expeditors_data(logistics_id=None):
    """Находит данные экспедиторов"""
    expeditors_list = []
    try:
        for exp_id, expeditor in expeditor_cards.items():
                expeditors_list.append((expeditor, exp_id))
    return expeditors_list


# ════════════════════════════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ - ДЛЯ ВСЕХ РОЛЕЙ
# ════════════════════════════════════════════════════════════════════════════════════
@dp.callback_query_handler(lambda q: q.data.startswith("deal_detail:"))
async def show_deal_detail(callback: types.CallbackQuery):
    """✅ ФИНАЛЬНЫЙ ОБРАБОТЧИК: Показывает партию/пул/логистику со ВСЕМИ УЧАСТНИКАМИ"""
    await callback.answer()

    try:
        parts = callback.data.split(":")
        item_id = parts[2]
        user_id = callback.from_user.id

        logging.info(f"🔍 Сделка: role={role}, item_id={item_id}, user_id={user_id}")

        text = ""
        keyboard = InlineKeyboardMarkup()

        def get_batch_participants_in_pull(pull_id):
            """Получает всех фермеров и их партии в пуле"""
            try:
                if isinstance(participants, list):
                    return participants
                return []
            except Exception as e:
                logging.error(f"❌ get_batch_participants_in_pull: {e}")
                return []

        def get_logistics_by_pull(pull_id):
            """Находит логистику по ID пула"""
            try:
                for logistic_id, logistic in logistics_requests.items():
                    pull_ref = logistic.get("pull_id")
                        return logistic, logistic_id
            return None, None

        def get_expeditors_by_logistics(logistics_id):
            """Получает экспедиторов для логистики"""
            expeditors_list = []
            try:
                for exp_id, expeditor in expeditor_cards.items():
                        expeditors_list.append((expeditor, exp_id))
            return expeditors_list

        # ════════════════════════════════════════════════════════════════════
        # 👨‍🌾 ФЕРМЕР - ПАРТИЯ + ПУЛ + УЧАСТНИКИ
        # ════════════════════════════════════════════════════════════════════
        if role == "farmer":
            try:

                batch = None
                for idx, b in enumerate(user_batches):
                        batch = b
                        break

                if not batch:
                    text = "❌ <b>Партия не найдена</b>"
                else:
                    logging.info(f"📊 Фермер просматривает: batch_id={batch_id}")

                    volume = get_safe_float(batch.get("volume"), 0)
                    price_per_ton = get_safe_float(
                        batch.get("price"), 0
                    ) or get_safe_float(batch.get("price_per_ton"), 0)
                    total_price = volume * price_per_ton

                    humidity = batch.get("humidity", "—")
                    if humidity and humidity != "—":
                        humidity = f"{get_safe_float(humidity):.1f}%"

                    impurity = batch.get("impurity", "—")
                    if impurity and impurity != "—":
                        impurity = f"{get_safe_float(impurity):.1f}%"

                    text += (
                        f"🌱 Культура: <b>{batch.get('culture', 'Не указана')}</b>\n"
                    )
                    text += f"📦 Объём: <b>{volume:,.1f} т</b>\n"
                    text += f"💰 Цена/тонна: <b>{price_per_ton:,.1f} ₽</b>\n"
                    text += f"💵 Сумма: <b>{total_price:,.0f} ₽</b>\n"
                    text += f"💧 Влажность: <b>{humidity}</b>\n"
                    text += f"🗑️ Примеси: <b>{impurity}</b>\n"
                    text += f"⭐ Класс: <b>{batch.get('quality_class', '—')}</b>\n"
                    text += f"📅 Дата: <b>{batch.get('created_at', 'N/A')}</b>\n"

                    files = batch.get("files", [])
                    if files:
                        text += f"\n<b>📄 ФАЙЛЫ ({len(files)})</b>\n"
                        for f_item in files:
                            size_kb = f_item.get("size", 0) / 1024
                            text += (
                                f"📎 {f_item.get('name', 'Файл')} ({size_kb:.1f} KB)\n"
                            )
                    else:


                    text += f"👤 Имя: <b>{farmer.get('name', 'Не указано')}</b>\n"
                    text += f"☎️ Телефон: <b>{farmer.get('phone', '—')}</b>\n"
                    text += f"📍 Регион: <b>{farmer.get('region', '—')}</b>\n"

                    pull, pull_id = get_pull_by_batch_v3(batch_id)
                    if pull and pull_id:

                        text += f"🌾 Культура: <b>{pull.get('culture', '—')}</b>\n"
                        text += f"🏗️ Порт: <b>{pull.get('port', '—')}</b>\n"
                        text += (
                            f"💰 Цена/т: <b>{pull.get('price_per_ton', 0):,.1f} ₽</b>\n"
                        )

                        exporter_id = pull.get("exporter_id") or pull.get("creator_id")
                        if exporter_id and exporter_id in users:
                            text += (
                                f"👤 Имя: <b>{exporter.get('name', 'Не указано')}</b>\n"
                            )
                            text += f"☎️ Телефон: <b>{exporter.get('phone', '—')}</b>\n"
                            text += f"📍 Регион: <b>{exporter.get('region', '—')}</b>\n"

                        participants = get_batch_participants_in_pull(pull_id)
                        if participants:
                            text += (
                                f"\n<b>👨‍🌾 ФЕРМЕРЫ В ПУЛЕ ({len(participants)})</b>\n"
                            )
                            for idx_p, p in enumerate(participants, 1):
                                text += f"{idx_p}. <b>{p.get('farmer_name', 'Unknown')}</b>\n"
                                text += f"   📦 {p.get('volume', 0):,.1f} т\n"

                        logistics, logistics_id = get_logistics_by_pull(pull_id)
                        if logistics and logistics.get("logist_id") in users:
                            text += (
                                f"👤 Имя: <b>{logist.get('name', 'Не указано')}</b>\n"
                            )
                            text += f"☎️ Телефон: <b>{logist.get('phone', '—')}</b>\n"
                            text += (
                                f"📤 От: <b>{logistics.get('route_from', '—')}</b>\n"
                            )
                            text += (
                                f"📥 Куда: <b>{logistics.get('route_to', '—')}</b>\n"
                            )

                            expeditors_list = get_expeditors_by_logistics(logistics_id)
                            if expeditors_list:
                                text += (
                                    f"\n<b>✈️ ЭКСПЕДИТОРЫ ({len(expeditors_list)})</b>\n"
                                )
                                for idx_exp, (expeditor, exp_id) in enumerate(
                                    expeditors_list, 1
                                ):
                                    exp_user_id = expeditor.get("user_id")
                                    if exp_user_id and exp_user_id in users:
                                        text += f"{idx_exp}. <b>{expeditor_user.get('name', 'Не указано')}</b>\n"
                                        text += (
                                            f"   ☎️ {expeditor_user.get('phone', '—')}\n"
                                        )
                                        text += f"   🚢 {expeditor.get('vehicle_type', '—')}\n"
                        else:

            except (ValueError, IndexError) as e:
                logging.error(f"❌ Ошибка фермер: {e}")
                text = "❌ <b>Ошибка: неверный ID партии</b>"

        # ════════════════════════════════════════════════════════════════════
        # 📤 ЭКСПОРТЕР
        # ════════════════════════════════════════════════════════════════════
        elif role == "exporter":
            try:
                pull_id = str(item_id)
                pulls_dict = pulls.get("pulls", {})
                pull = pulls_dict.get(pull_id) or pulls_dict.get(
                    int(pull_id) if str(pull_id).isdigit() else None
                )

                if not pull:
                    text = "❌ <b>Пул не найден</b>"
                    else:
                        target_volume = get_safe_float(pull.get("target_volume"), 0)
                        current_volume = get_safe_float(pull.get("current_volume"), 0)
                        price_per_ton = get_safe_float(
                            pull.get("price_per_ton"), 0
                        ) or get_safe_float(pull.get("price"), 0)
                        progress = (
                            (current_volume / target_volume * 100)
                            if target_volume > 0
                            else 0
                        )
                        total_price = current_volume * price_per_ton

                        text += f"💰 Цена/т: <b>{price_per_ton:,.1f} ₽</b>\n"
                        text += f"💵 Сумма: <b>{total_price:,.0f} ₽</b>\n"
                        text += f"🏗️ Порт: <b>{pull.get('port', 'Не указан')}</b>\n"

                        exporter_id = pull.get("exporter_id") or pull.get("creator_id")

                        text += f"☎️ Телефон: <b>{exporter.get('phone', '—')}</b>\n"
                        text += f"📍 Регион: <b>{exporter.get('region', '—')}</b>\n"

                        participants = get_batch_participants_in_pull(pull_id)
                        if participants:
                            text += (
                                f"\n<b>👨‍🌾 ФЕРМЕРЫ И ИХ ПАРТИИ ({len(participants)})</b>\n"
                            )
                            for idx_p, p in enumerate(participants, 1):
                                farmer_id = p.get("farmer_id")
                                farmer_name = p.get("farmer_name", "Unknown")
                                volume = p.get("volume", 0)

                                text += f"{idx_p}. <b>{farmer_name}</b>\n"
                                text += f"   📦 Партия: {volume:,.1f} т\n"
                                text += f"   ⭐ Класс: {p.get('quality_class', '—')}\n"

                                if farmer_id and farmer_id in users:
                                    text += f"   ☎️ {farmer.get('phone', '—')}\n"
                        else:

                        logistics, logistics_id = get_logistics_by_pull(pull_id)
                        if logistics and logistics.get("logist_id") in users:
                            text += f"☎️ Телефон: <b>{logist.get('phone', '—')}</b>\n"

                            expeditors_list = get_expeditors_by_logistics(logistics_id)
                            if expeditors_list:
                                for idx_e, (expeditor, exp_id) in enumerate(
                                    expeditors_list, 1
                                ):
                                    exp_user_id = expeditor.get("user_id")
                                    if exp_user_id and exp_user_id in users:
                                        text += f"{idx_e}. <b>{exp_user.get('name', 'Не указано')}</b>\n"
                                        text += (
                                            f"   🚢 {expeditor.get('vehicle_type', '—')}\n"
                                        )
                        else:

            except (ValueError, KeyError) as e:
                logging.error(f"❌ Ошибка exporter: {e}")
                text = "❌ <b>Ошибка: неверный ID пула</b>"

        else:
            text = "❌ <b>Неизвестная роль!</b>"

        keyboard.add(
            )
        try:
            await callback.message.edit_text(
                text, reply_markup=keyboard, parse_mode="HTML"
            )
        except MessageNotModified:
            pass
            try:
                await callback.message.answer(
                    text, reply_markup=keyboard, parse_mode="HTML"
                )
                await callback.answer("❌ Ошибка при загрузке", show_alert=True)

    except Exception as e:
        logging.error(f"❌ ОШИБКА show_deal_detail: {e}", exc_info=True)
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("📊 Меню", callback_data="back_to_main"))
        try:
            await callback.message.edit_text(
                f"❌ <b>Ошибка:</b>\n<code>{str(e)[:100]}</code>",
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            await callback.answer(f"❌ {str(e)[:50]}", show_alert=True)


# ============================================================================
# СМЕНА СТАТУСОВ: ФЕРМЕР
# ============================================================================
@dp.callback_query_handler(lambda q: q.data.startswith("change_farmer_status:"))
async def change_farmer_batch_status(callback: types.CallbackQuery):
    """✅ Фермер меняет статус партии"""
    await callback.answer()
    try:
        parts = callback.data.split(":")
        deal_id = parts[1]
        new_status = parts[2]
        user_id = callback.from_user.id


        try:
            await callback.answer("❌ Батч не найден", show_alert=True)
            return


        valid = {
            "in_progress": ["completed", "active"],
            "completed": [],
            "canceled": ["active"],
        }

        if new_status not in valid.get(old_status, []):
            await callback.answer(
                f"❌ Переход {old_status} → {new_status} не допущен", show_alert=True
            )
            return

        batch["status"] = new_status
        batch["status_changed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        batch["status_changed_by"] = user_id

        save_batches_to_pickle()

        logging.info(
            f"✅ Фермер {user_id}: партия {batch_idx} {old_status} → {new_status}"
        )

        await callback.message.edit_text(
            f"📦 {batch.get('volume', 0)}т\n"
            f"⏱️ {old_status} → <b>{new_status}</b>",
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"❌ ОШИБКА: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


# ============================================================================
# СМЕНА СТАТУСОВ: ЭКСПОРТЕР
# ============================================================================


@dp.callback_query_handler(lambda q: q.data.startswith("change_exporter_status:"))
async def change_exporter_pool_status(callback: types.CallbackQuery):
    try:
        parts = callback.data.split(":")
            return
            return

    except Exception as e:
        logging.error(f"❌ ОШИБКА: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


# ============================================================================
# СМЕНА СТАТУСОВ: ЛОГИСТ
# ============================================================================
@dp.callback_query_handler(lambda q: q.data.startswith("change_logist_status:"))
async def change_logist_delivery_status(callback: types.CallbackQuery):
    try:
        parts = callback.data.split(":")
        new_status = parts[2]
        user_id = callback.from_user.id

            await callback.answer("❌ Доставка не найдена", show_alert=True)
            return

        valid = {
            "in_progress": ["completed", "pending"],
            "completed": [],
        }

        if new_status not in valid.get(old_status, []):
            await callback.answer(
                f"❌ Переход {old_status} → {new_status} не допущен", show_alert=True
            )
            return

        delivery["status"] = new_status
        delivery["status_changed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        delivery["status_changed_by"] = user_id


        logging.info(
        )

        await callback.message.edit_text(
            f"⏱️ {old_status} → <b>{new_status}</b>",
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"❌ ОШИБКА: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


# ============================================================================
# СМЕНА СТАТУСОВ: ЭКСПЕДИТОР
# ============================================================================


@dp.callback_query_handler(lambda q: q.data.startswith("change_expeditor_status:"))
async def change_expeditor_freight_status(callback: types.CallbackQuery):
    """✅ Экспедитор меняет статус маршрута + ЗАКРЫВАЕТ СДЕЛКИ"""
    try:
        parts = callback.data.split(":")
        new_status = parts[2]
        user_id = callback.from_user.id

            await callback.answer("❌ Маршрут не найден", show_alert=True)
            return


        valid = {
            "active": ["in_progress"],
        }

        if new_status not in valid.get(old_status, []):
            await callback.answer(
                f"❌ Переход {old_status} → {new_status} не допущен", show_alert=True
            )
            return


            logging.info(
            )

                all_pulls = pulls.get("pulls", {})

                                continue


                        )

                    delivery["status"] = "completed"
                    )

                save_pulls_to_pickle()

                await callback.message.edit_text(
                    f"⏱️ {old_status} → <b>{new_status}</b>\n\n"
                    parse_mode="HTML",
                )
        else:
                await callback.message.edit_text(
                        f"⏱️ {old_status} → <b>{new_status}</b>",
                        parse_mode="HTML",
                    )

        logging.info(
        )

    except Exception as e:
        logging.error(f"❌ ОШИБКА: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@dp.callback_query_handler(lambda q: q.data.startswith("change_pull_status:"))
async def change_pull_status(callback: types.CallbackQuery):
    try:
        parts = callback.data.split(":")
        pull_id = int(parts[1])
        user_id = callback.from_user.id

        all_pulls = pulls.get("pulls", {})
        pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

        if not pull:
            await callback.answer("❌ Пул не найден", show_alert=True)
            return

        creator_id = pull.get("creator_id") or pull.get("exporter_id")
            await callback.answer(
                "❌ Вы не можете менять статус этого пула", show_alert=True
            )
            return

        text = (
            f"<b>Пул:</b> #{pull_id}\n"
            f"🌾 <b>Культура:</b> {pull.get('culture', '?')}\n"
            f"<b>Текущий статус:</b> {status_map.get(current_status, current_status)}\n\n"
        )

        keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(
                InlineKeyboardButton(
                )
            )

        keyboard.add(
            InlineKeyboardButton("🔙 Назад", callback_data=f"view_pull:{pull_id}")
        )

        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        logging.info(f"✅ Показаны опции смены статуса для пула {pull_id}")

    except Exception as e:
        logging.error(f"❌ Ошибка в change_pull_status: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)[:50]}", show_alert=True)


@dp.callback_query_handler(lambda q: q.data.startswith("confirm_pull_status:"))
async def confirm_pull_status(callback: types.CallbackQuery):
    """Применить новый статус к пулу, уведомить ВСЕХ присоединившихся фермеров и ВСЕХ логистов"""
    try:
        parts = callback.data.split(":")
        pull_id = int(parts[1])
        user_id = callback.from_user.id

        all_pulls = pulls.get("pulls", {})
        pull = all_pulls.get(pull_id) or all_pulls.get(str(pull_id))

        if not pull:
            await callback.answer("❌ Пул не найден", show_alert=True)
            return

        creator_id = pull.get("creator_id") or pull.get("exporter_id")
            await callback.answer(
                "❌ Вы не можете менять статус этого пула", show_alert=True
            )
            return


            await callback.answer(
            )
            return

            current = pull.get("current_volume", 0)
            target = pull.get("target_volume", 0)
            if current < target:
                await callback.answer(
                    f"⚠️ Пул не полностью заполнен!\n{current:.0f}/{target:.0f}т",
                    show_alert=True,
                )
                return

        pull["status"] = new_status
        pull["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_pulls_to_pickle()

            p.get("farmer_id")
            if p.get("farmer_id")
        logging.info(
            f"[CONFIRM_PULL_STATUS] pull_id={pull_id} logist_ids={logist_ids} farmer_ids={farmer_ids} all_notify_ids={list(all_notify_ids)}"
        )

        notification_text = (
            f"📤 Пул: <b>#{pull_id}</b>\n"
            f"🌾 Культура: <b>{pull.get('culture', 'Не указана')}</b>\n"
            f"📊 Объём: <b>{pull.get('current_volume', 0):,.1f} т</b>\n"
            f"🔄 Было: {status_map.get(old_status, old_status)}\n"
            f"➡️ Стало: <b>{status_map.get(new_status, new_status)}</b>\n"
            f"📅 Время: {pull['updated_at']}"
        )

        keyboard_notification = InlineKeyboardMarkup()
        keyboard_notification.add(
            InlineKeyboardButton(
                "📊 Посмотреть пул", callback_data=f"view_pull:{pull_id}"
            )
        )

        sent_count = 0
        failed_ids = []
            try:
                await bot.send_message(
                    notify_id,
                    notification_text,
                    parse_mode="HTML",
                    reply_markup=keyboard_notification,
                )
                logging.info(f"[NOTIFY] Уведомление отправлено {notify_id}")
                sent_count += 1
            except Exception as e:
                logging.warning(
                    f"[NOTIFY] Ошибка отправки уведомления {notify_id}: {e}"
                )
                failed_ids.append(notify_id)

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
                "📊 Назад к пулу", callback_data=f"view_pull:{pull_id}"
            )
        )
        keyboard.add(
            InlineKeyboardButton("🏠 В главное меню", callback_data="back_to_main")
        )

        confirmation_text = (
            f"📤 Пул: <b>#{pull_id}</b>\n"
            f"🌾 Культура: <b>{pull.get('culture', 'Не указана')}</b>\n"
            f"🔄 Было: <b>{status_map.get(old_status, old_status)}</b>\n"
            f"➡️ Стало: <b>{status_map.get(new_status, new_status)}</b>\n"
            f"📅 Время: {pull['updated_at']}\n\n"
            f"🔔 Отправлено уведомлений: {sent_count} (логисты: {len(logist_ids)}, фермеры: {len(farmer_ids)})"
        )

        await callback.message.edit_text(
            confirmation_text, reply_markup=keyboard, parse_mode="HTML"
        )

        logging.info(
            f"✅ Статус пула {pull_id}: "
            f"{status_map.get(old_status, old_status)} → "
            f"{status_map.get(new_status, new_status)}"
        )

    except Exception as e:
        logging.error(f"❌ Ошибка в confirm_pull_status: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {str(e)[:50]}", show_alert=True)


# ============================================================================
# ЗАПУСК БОТА
# ============================================================================
if __name__ == "__main__":
    logging.info("🚀 Запуск бота...")
    try:
        os.makedirs("data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    except Exception as e:
        logging.error(f"❌ Ошибка создания директорий: {e}")

    executor.start_polling(
        dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown
    )
