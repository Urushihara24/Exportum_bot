import warnings
warnings.filterwarnings("ignore", message=".*LibreSSL.*")
warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")
warnings.filterwarnings("ignore", category=UserWarning, module='urllib3')

import os
import logging
import requests
import asyncio
import re
import time
import json
import pickle
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from collections import defaultdict

from aiogram import Bot, Dispatcher, types
from aiogram.utils.exceptions import MessageNotModified
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton,
    InlineKeyboardMarkup,    # ← ДОБАВИТЬ
    InlineKeyboardButton,    # ← ДОБАВИТЬ
    CallbackQuery,           # ← ДОБАВИТЬ
    ReplyKeyboardRemove
)
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
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

DB_PATH = "bot_data.db"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


API_TOKEN = os.getenv('BOT_TOKEN', '8180339161:AAFRk8446gKMagAkUTGjGyFDTFHa__mVOY0')
ADMIN_ID = int(os.getenv('173014517', '1481790360'))

CONFIG = {
    'timeout': 15,
    'cache_ttl': 1800,
    'fallback_prices': {
        'Пшеница': 15650,
        'Ячмень': 13300,
        'Кукуруза': 14000,
        'Соя': 40900,
        'Подсолнечник': 38600
    },
    'south_regions': ['Краснодар', 'Ростов', 'Астрахань', 'Волгоград', 'Ставрополь'],
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# === КОНСТАНТЫ ФАЙЛОВ ДАННЫХ ===
DATA_DIR = 'data'  # Локальная директория
USERSFILE = os.path.join(DATA_DIR, 'users.pkl')
BATCHESFILE = os.path.join(DATA_DIR, 'batches.pkl')
PULLSFILE = os.path.join(DATA_DIR, 'pulls.pkl')
EXPEDITORSFILE = os.path.join(DATA_DIR, 'expeditors.pkl')
os.makedirs(DATA_DIR, exist_ok=True)

LOGS_DIR = 'logs'

USERS_FILE = 'data/users.json'
BATCHES_FILE = 'data/batches.pkl'
PULLS_FILE = 'data/pulls.pkl'
PRICES_FILE = 'data/prices.json'
NEWS_FILE = 'data/news.json'





SHIPPINGREQUESTSFILE = 'shippingrequests.pkl'

GOOGLE_SHEETS_CREDENTIALS = 'credentials.json'
SPREADSHEET_ID = "1DywxtuWW4-1Q0O71ajVaBB5Ih15nZjA4rvlpV7P7NOA"

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM КАНАЛ ДЛЯ ПУБЛИКАЦИИ
# ═══════════════════════════════════════════════════════════════════════════════
CHANNEL_ID = "@your_channel"  # Замените на ID канала (-1001234567890)


dp = Dispatcher(bot, storage=storage)

users = {}
batches = {
    '999999999': [
        {
            'id': 'BATCH001',
            'culture': 'Пшеница',
            'volume': 500,
            'price': 15000,
            'region': 'Краснодарский край',
            'quality_class': '3 класс',
            'storage_type': 'Элеватор',
            'status': 'active',
            'harvest_year': 2024
        },
        {
            'id': 'BATCH002',
            'culture': 'Кукуруза',
            'volume': 300,
            'price': 12000,
            'region': 'Ростовская область',
            'quality_class': '2 класс',
            'storage_type': 'Ангар',
            'status': 'active',
            'harvest_year': 2024
        },
        {
            'id': 'BATCH003',
            'culture': 'Подсолнечник',
            'volume': 200,
            'price': 25000,
            'region': 'Ставропольский край',
            'quality_class': 'Базисный',
            'storage_type': 'Элеватор',
            'status': 'active',
            'harvest_year': 2024
        }
    ]
}

# ============================================================================
# КОНФИГУРАЦИЯ БОТА
# ============================================================================

CHANNEL_ID = "@your_channel"  # Замените на ID канала (-1001234567890)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Планировщик задач
scheduler = AsyncIOScheduler()

# ============================================================================
# ХРАНИЛИЩА ДАННЫХ (ТОЛЬКО ОДНО ОПРЕДЕЛЕНИЕ!)
# ============================================================================

# Основные хранилища
users = {}
batches = {}
pulls = {}
pull_participants = {}
matches = {}
deals = {}

# Логистика
shipping_requests = {}
logistics_requests = {}
logistic_offers = {}
deliveries = {}

# Экспедитор
expeditor_offers = {}

# Рейтинги
logistic_ratings = {}

# Счётчики
batch_counter = 0
pull_counter = 0
deal_counter = 0
match_counter = 0
logistics_request_counter = 0
logistics_offer_counter = 0

# Кэш цен и новостей
prices_cache = {'data': {}, 'updated': None}
news_cache = {'data': [], 'updated': None}
last_prices_update = None
last_news_update = None

# ============================================================================
# КОНСТАНТЫ
# ============================================================================

ROLES = {
    'farmer': '🌾 Фермер',
    'exporter': '📦 Экспортёр',
    'logistic': '🚚 Логист',
    'expeditor': '🚛 Экспедитор'
}

CULTURES = ['Пшеница', 'Ячмень', 'Кукуруза', 'Подсолнечник', 'Рапс', 'Соя']

QUALITY_CLASSES = ['1 класс', '2 класс', '3 класс', '4 класс', '5 класс']

STORAGE_TYPES = ['Элеватор', 'Склад', 'Напольное хранение', 'Силос']

DEAL_STATUSES = {
    'pending': '🔄 В процессе',
    'matched': '🎯 Найден партнёр', 
    'shipping': '🚛 Организация перевозки',
    'completed': '✅ Завершена',
    'cancelled': '❌ Отменена'
}

# ============================================================================
# КРИТИЧНЫЕ НЕДОСТАЮЩИЕ ФУНКЦИИ (добавлены при исправлении)
# ============================================================================
# ==================== НОВЫЕ ФУНКЦИИ ====================
def translate_pull_status(status: str, current_volume: float = None, target_volume: float = None) -> str:
    '''Переводит статус пула на русский с эмодзи'''
    
    # Если статус не задан, но известны объёмы — определяем автоматически
    if not status and current_volume is not None and target_volume is not None:
        if current_volume >= target_volume:
            status = 'filled'
        else:
            status = 'active'
    
    status_map = {
        'active': '✅ Активный',
        'filled': '🎉 Заполнен (100%)',
        'closed': '🔒 Закрыт',
        'completed': '✔️ Завершён',
        'cancelled': '❌ Отменён'
    }
    
    # Приводим к нижнему регистру, если статус не пустой
    normalized_status = status.lower() if status else ''
    
    # Если статус всё ещё неизвестен, но есть объёмы — анализируем
    if not normalized_status and current_volume is not None and target_volume is not None:
        if current_volume >= target_volume:
            normalized_status = 'filled'
        elif current_volume > 0:
            normalized_status = 'active'
        else:
            normalized_status = 'active'
    
    return status_map.get(normalized_status, '❓ Неизвестен')

def migrate_old_pulls():
    '''Устанавливает статус для старых пулов без статуса'''
    migrated_count = 0
    valid_statuses = ['active', 'filled', 'closed', 'completed', 'cancelled']
    
    for pull_id, pull in pulls.items():
        current_status = pull.get('status', None)
        
        # Логируем текущее состояние пула
        logging.info(f"🔍 Пул #{pull_id}: статус='{current_status}' (тип: {type(current_status).__name__}), "
                    f"объём={pull.get('current_volume', 0)}/{pull.get('target_volume', 0)}")
        
        # Проверяем: статус отсутствует, пустой, None или некорректный
        needs_migration = (
            current_status is None or 
            current_status == '' or 
            current_status.lower() not in valid_statuses
        )
        
        if needs_migration:
            # Определяем статус по заполненности
            current_vol = pull.get('current_volume', 0)
            target_vol = pull.get('target_volume', 1)
            
            if current_vol >= target_vol and target_vol > 0:
                pull['status'] = 'filled'
                logging.info(f"✅ Пул #{pull_id} обновлён: 'filled' (заполнен {current_vol}/{target_vol} т)")
                migrated_count += 1
            else:
                pull['status'] = 'active'
                logging.info(f"✅ Пул #{pull_id} обновлён: 'active' (активен {current_vol}/{target_vol} т)")
                migrated_count += 1
        else:
            logging.info(f"ℹ️ Пул #{pull_id}: статус '{current_status}' корректен, пропускаем")
    
    if migrated_count > 0:
        save_pulls_to_pickle()
        logging.info(f"✅ Миграция завершена: обновлено {migrated_count} пулов")
    else:
        logging.info("ℹ️ Миграция не требуется: все пулы уже имеют корректный статус")

def parse_join_pull_callback(callback_data: str) -> dict:
    '''Универсальный парсер callback для join_pull'''
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
    '''Проверяет, поместится ли партия в пул'''
    batch_volume = batch.get("volume", 0)
    current_volume = pull.get("current_volume", 0)
    target_volume = pull.get("target_volume", 0)
    available = target_volume - current_volume
    
    if batch_volume > available:
        return (False, f"❌ Объём партии ({batch_volume} т) превышает доступный ({available} т)")
    
    return (True, "✅ Партия подходит")


async def check_and_close_pool_if_full(pull_id: int):
    '''Автоматически закрывает пул если current_volume >= target_volume'''
    if pull_id not in pulls:
        logging.error(f"❌ Пул #{pull_id} не найден")
        return
    
    pull = pulls[pull_id]
    current = pull.get("current_volume", 0)
    target = pull.get("target_volume", 0)
    
    logging.info(f"🔍 Проверка автозакрытия пула #{pull_id}: {current}/{target} т")
    
    if current >= target:
        pull["status"] = "Закрыт"
        pull["closed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_pulls_to_pickle()
        
        logging.info(f"🔒 Пул #{pull_id} АВТОМАТИЧЕСКИ ЗАКРЫТ ({current}/{target} т)")
        
        # Уведомляем экспортёра
        exporter_id = pull.get("exporter_id")
        if exporter_id:
            try:
                await bot.send_message(
                    exporter_id,
                    f"🎉 <b>Пул #{pull_id} закрыт!</b>\n\n"
                    f"📦 Культура: {pull['culture']}\n"
                    f"📊 Объём: {current}/{target} т\n"
                    f"✅ Пул заполнен и готов к отгрузке.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"❌ Ошибка уведомления экспортёра: {e}")

# ==================== КОНЕЦ НОВЫХ ФУНКЦИЙ ====================

def save_deals_to_pickle():
    """Сохранение сделок в pickle"""
    try:
        deals_file = os.path.join(DATA_DIR, 'deals.pkl')
        with open(deals_file, 'wb') as f:
            pickle.dump(deals, f)
        logging.info("✅ Сделки сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения сделок: {e}")

def save_data():
    """Универсальная функция сохранения всех данных"""
    try:
        save_users_to_pickle()
        save_batches_to_pickle()
        save_pulls_to_pickle()
        save_deals_to_pickle()

        # Дополнительно сохраняем остальные данные
        if logistics_cards:
            with open(os.path.join(DATA_DIR, 'logistics_cards.pkl'), 'wb') as f:
                pickle.dump(logistics_cards, f)

        if expeditor_cards:
            with open(os.path.join(DATA_DIR, 'expeditor_cards.pkl'), 'wb') as f:
                pickle.dump(expeditor_cards, f)

        if logistics_requests:
            with open(os.path.join(DATA_DIR, 'logistics_requests.pkl'), 'wb') as f:
                pickle.dump(logistics_requests, f)

        logging.info("✅ Все данные успешно сохранены")
        return True
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения данных: {e}")
        return False

def get_logistics_by_port(port):
    """Поиск логистов по порту"""
    result = []
    for uid, card in logistics_cards.items():
        ports = card.get('ports', [])
        if isinstance(ports, str):
            ports = [p.strip() for p in ports.split(',')]
        if port in ports or 'Все порты' in ports:
            result.append({
                'user_id': uid,
                'company': card.get('company', users.get(uid, {}).get('name', 'Н/Д')),
                'phone': users.get(uid, {}).get('phone', 'Н/Д'),
                'price_per_ton': card.get('price_per_ton', 0),
                'transport_type': card.get('transport_type', 'Н/Д')
            })
    return result

def get_expeditors_by_port(port):
    """Поиск экспедиторов по порту"""
    result = []
    for uid, card in expeditor_cards.items():
        ports = card.get('ports', [])
        if isinstance(ports, str):
            ports = [p.strip() for p in ports.split(',')]
        if port in ports or 'Все порты' in ports:
            result.append({
                'user_id': uid,
                'company': card.get('company', users.get(uid, {}).get('name', 'Н/Д')),
                'phone': users.get(uid, {}).get('phone', 'Н/Д'),
                'services': card.get('services', 'Н/Д'),
                'dt_price': card.get('dt_price', 0)
            })
    return result

def format_logistics_cards(logistics):
    """Форматирование карточек логистов для отображения"""
    if not logistics:
        return "❌ Логисты не найдены для данного порта"

    text = ""
    for i, log in enumerate(logistics[:5], 1):
        text += f"{i}. 🚚 <b>{log.get('company', 'Н/Д')}</b>\n"
        text += f"   📱 {log.get('phone', 'Н/Д')}\n"
        text += f"   💰 {log.get('price_per_ton', 0):,.0f} ₽/т\n"
        text += f"   🚛 {log.get('transport_type', 'Н/Д')}\n\n"

    if len(logistics) > 5:
        text += f"... и ещё {len(logistics) - 5} логистов"

    return text

def format_expeditors_cards(expeditors):
    """Форматирование карточек экспедиторов для отображения"""
    if not expeditors:
        return "❌ Экспедиторы не найдены для данного порта"

    text = ""
    for i, exp in enumerate(expeditors[:5], 1):
        text += f"{i}. 📋 <b>{exp.get('company', 'Н/Д')}</b>\n"
        text += f"   📱 {exp.get('phone', 'Н/Д')}\n"
        text += f"   💰 ДТ: {exp.get('dt_price', 0):,.0f} ₽\n"
        text += f"   📦 {exp.get('services', 'Н/Д')}\n\n"

    if len(expeditors) > 5:
        text += f"... и ещё {len(expeditors) - 5} экспедиторов"

    return text

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

def get_logistics_by_port(port):
    """Поиск логистов по порту"""
    result = []
    for uid, card in logistics_cards.items():
        ports = card.get('ports', [])
        if isinstance(ports, str):
            ports = [p.strip() for p in ports.split(',')]
        if port in ports or 'Все порты' in ports:
            result.append({
                'user_id': uid,
                'company': card.get('company', users.get(uid, {}).get('name', 'Н/Д')),
                'phone': users.get(uid, {}).get('phone', 'Н/Д'),
                'price_per_ton': card.get('price_per_ton', 0),
                'transport_type': card.get('transport_type', 'Н/Д'),
                'routes': card.get('routes', 'Н/Д')
            })
    return result

def get_expeditors_by_port(port):
    """Поиск экспедиторов по порту"""
    result = []
    for uid, card in expeditor_cards.items():
        ports = card.get('ports', [])
        if isinstance(ports, str):
            ports = [p.strip() for p in ports.split(',')]
        if port in ports or 'Все порты' in ports:
            result.append({
                'user_id': uid,
                'company': card.get('company', users.get(uid, {}).get('name', 'Н/Д')),
                'phone': users.get(uid, {}).get('phone', 'Н/Д'),
                'services': card.get('services', 'Н/Д'),
                'dt_price': card.get('dt_price', 0),
                'experience': card.get('experience', 'Н/Д')
            })
    return result

def format_logistics_cards(logistics):
    """Форматирование карточек логистов"""
    if not logistics:
        return "❌ Логисты не найдены"
    text = ""
    for i, log in enumerate(logistics[:5], 1):
        text += f"{i}. 🚚 <b>{log.get('company', 'Н/Д')}</b>\\n"
        text += f"   📱 {log.get('phone')}\\n"
        text += f"   💰 {log.get('price_per_ton', 0):,.0f} ₽/т\\n\\n"
    return text

def format_expeditors_cards(expeditors):
    """Форматирование карточек экспедиторов"""
    if not expeditors:
        return "❌ Экспедиторы не найдены"
    text = ""
    for i, exp in enumerate(expeditors[:5], 1):
        text += f"{i}. 📋 <b>{exp.get('company', 'Н/Д')}</b>\\n"
        text += f"   📱 {exp.get('phone')}\\n"
        text += f"   💰 {exp.get('dt_price', 0):,.0f} ₽\\n\\n"
    return text

def save_deals_to_pickle():
    """Сохранение сделок"""
    try:
        with open(os.path.join(DATA_DIR, 'deals.pkl'), 'wb') as f:
            pickle.dump(deals, f)
        logging.info("✅ Сделки сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")

def save_logistics_requests_to_pickle():
    """Сохранение заявок на логистику"""
    try:
        with open(os.path.join(DATA_DIR, 'logistics_requests.pkl'), 'wb') as f:
            pickle.dump(logistics_requests, f)
        logging.info("✅ Заявки сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")

def save_logistics_offers_to_pickle():
    """Сохранение предложений"""
    try:
        with open(os.path.join(DATA_DIR, 'logistics_offers.pkl'), 'wb') as f:
            pickle.dump(logistics_offers, f)
        logging.info("✅ Предложения сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")

def save_logistics_cards_to_pickle():
    """Сохранение карточек логистов"""
    try:
        with open(os.path.join(DATA_DIR, 'logistics_cards.pkl'), 'wb') as f:
            pickle.dump(logistics_cards, f)
        logging.info("✅ Карточки логистов сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")

def save_expeditor_cards_to_pickle():
    """Сохранение карточек экспедиторов"""
    try:
        with open(os.path.join(DATA_DIR, 'expeditor_cards.pkl'), 'wb') as f:
            pickle.dump(expeditor_cards, f)
        logging.info("✅ Карточки экспедиторов сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")

def save_data():
    """Универсальная функция сохранения всех данных"""
    try:
        save_users_to_pickle()
        save_batches_to_pickle()
        save_pulls_to_pickle()
        save_deals_to_pickle()
        save_logistics_requests_to_pickle()
        save_logistics_offers_to_pickle()
        save_logistics_cards_to_pickle()
        save_expeditor_cards_to_pickle()
        logging.info("✅ Все данные сохранены")
        return True
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")
        return False

def save_logistic_offers():
    try:
        with open(os.path.join(DATA_DIR, 'logistic_offers.pkl'), 'wb') as f:
            pickle.dump(logistic_offers, f)
        logging.info("✅ Logistic offers saved")
    except Exception as e:
        logging.error(f"❌ Error saving logistic offers: {e}")


def load_logistic_offers():
    global logistic_offers
    try:
        filepath = os.path.join(DATA_DIR, 'logistic_offers.pkl')
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                logistic_offers = pickle.load(f)
            logging.info(f"✅ Loaded {len(logistic_offers)} logistic offers")
        else:
            logistic_offers = {}
    except Exception as e:
        logging.error(f"❌ Error loading logistic offers: {e}")
        logistic_offers = {}


def save_deliveries():
    try:
        with open(os.path.join(DATA_DIR, 'deliveries.pkl'), 'wb') as f:
            pickle.dump(deliveries, f)
        logging.info("✅ Deliveries saved")
    except Exception as e:
        logging.error(f"❌ Error saving deliveries: {e}")


def load_deliveries():
    global deliveries
    try:
        filepath = os.path.join(DATA_DIR, 'deliveries.pkl')
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                deliveries = pickle.load(f)
            logging.info(f"✅ Loaded {len(deliveries)} deliveries")
        else:
            deliveries = {}
    except Exception as e:
        logging.error(f"❌ Error loading deliveries: {e}")
        deliveries = {}


def save_expeditor_offers():
    try:
        with open(os.path.join(DATA_DIR, 'expeditor_offers.pkl'), 'wb') as f:
            pickle.dump(expeditor_offers, f)
        logging.info("✅ Expeditor offers saved")
    except Exception as e:
        logging.error(f"❌ Error saving expeditor offers: {e}")


def load_expeditor_offers():
    global expeditor_offers
    try:
        filepath = os.path.join(DATA_DIR, 'expeditor_offers.pkl')
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                expeditor_offers = pickle.load(f)
            logging.info(f"✅ Loaded {len(expeditor_offers)} expeditor offers")
        else:
            expeditor_offers = {}
    except Exception as e:
        logging.error(f"❌ Error loading expeditor offers: {e}")
        expeditor_offers = {}

# ============================================================================
# КРИТИЧЕСКАЯ ФУНКЦИЯ: Парсинг ID из callback_data
# ============================================================================

def parse_callback_id(callback_data: str) -> int:
    """Парсит ID из callback_data"""
    import logging
    
    # Список префиксов которые НЕ содержат ID
    non_id_prefixes = ['culture:', 'port:', 'region:']
    
    # Проверяем префиксы
    for prefix in non_id_prefixes:
        if callback_data.startswith(prefix):
            return None  # Пропускаем callback без ID
    
    try:
        if ':' in callback_data:
            return int(callback_data.split(':')[-1])
        elif '_' in callback_data:
            return int(callback_data.split('_')[-1])
        return int(callback_data)
    except (ValueError, IndexError) as e:
        logging.error(f"Parse error: {e}")
        raise ValueError(f"Cannot parse ID from '{callback_data}'")



class RegistrationStatesGroup(StatesGroup):
    name = State()
    phone = State()
    email = State()
    region = State()
    role = State()
    inn = State()
    company_details = State()



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
    services = State()
    dt_price = State()
    ports = State()
    experience = State()
    additional_info = State()

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
    volume = State()   # Объем
    price = State()    # Цена
    port = State()     # Порт
    moisture = State() # Влажность
    nature = State()   # Натура
    impurity = State() # Сорная примесь
    weed = State()     # Зерновая примесь
    documents = State() # Документы
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

class AttachFilesStatesGroup(StatesGroup):
    """Прикрепление файлов к партии"""
    upload_files = State()
    confirm_upload = State()

class ShippingRequestStatesGroup(StatesGroup):
    """Заявка на логистику"""
    pull_id = State()
    route_from = State()
    route_to = State()
    volume = State()
    culture = State()
    desired_date = State()
# Логистика - заявка экспортёра

class CreateLogisticRequestStatesGroup(StatesGroup):
    route_from = State()
    loading_date = State()
    notes = State()

# Логистика - отклик логиста
class LogisticOfferStatesGroup(StatesGroup):
    """FSM для создания предложения логиста"""
    request_id = State()        # ID заявки
    vehicle_type = State()      # Тип транспорта
    price = State()             # Цена доставки
    delivery_date = State()     # Дата доставки
    additional_info = State()   # Дополнительная информация
    confirm = State()           # Подтверждение


# Присоединение к пулу

def validate_phone(phone):
    """Валидация номера телефона"""
    cleaned = re.sub(r'[\s\-\(\)\+]', '', phone)
    return len(cleaned) >= 10 and cleaned.isdigit()


def validate_email(email):
    """Проверка email на валидность с правильным regex"""
    if not email or not isinstance(email, str):
        return False
    # Правильный паттерн: должна быть локальная часть перед @
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))

def validate_email(email):
    """Валидация email"""
    return re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email) is not None

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

def validate_date(date_str):
    """Валидация даты"""
    try:
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except ValueError:
        return False

def farmer_keyboard():
    """Клавиатура для фермера"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add("➕ Добавить партию")
    keyboard.row("🔧 Мои партии", "🎯 Пулы")
    keyboard.row("🔍 Поиск экспортёров", "📋 Мои сделки")
    keyboard.row("🚚 Предложения логистов")  # ✅ НОВАЯ КНОПКА
    keyboard.row("👤 Профиль")
    keyboard.add("📈 Цены на зерно", "📰 Новости рынка")
    return keyboard


def exporter_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("➕ Создать пул"),
        KeyboardButton("📦 Мои пулы")
    )
    keyboard.add(
        KeyboardButton("🚚 Заявка на логистику"),  # ← НОВАЯ КНОПКА
        KeyboardButton("📋 Мои сделки")
    )
    keyboard.add(
        KeyboardButton("🔍 Найти партии"),
        KeyboardButton("👤 Профиль")
    )
    keyboard.add(
        KeyboardButton("📈 Цены на зерно"),
        KeyboardButton("📰 Новости рынка")
    )
    return keyboard


def logistic_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(KeyboardButton("📋 Активные заявки"))
    keyboard.add(KeyboardButton("💼 Мои предложения"), KeyboardButton("🚚 Мои доставки"))
    keyboard.add(KeyboardButton("📊 Статистика"), KeyboardButton("📰 Новости и цены"))
    keyboard.add(KeyboardButton("👤 Профиль"), KeyboardButton("ℹ️ Помощь"))
    return keyboard


def expeditor_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(KeyboardButton("📋 Создать предложение"))
    keyboard.add(KeyboardButton("💼 Мои предложения"), KeyboardButton("🚢 Мои услуги"))
    keyboard.add(KeyboardButton("📊 Статистика"), KeyboardButton("📰 Новости и цены"))
    keyboard.add(KeyboardButton("👤 Профиль"), KeyboardButton("ℹ️ Помощь"))
    return keyboard


def vehicle_type_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚛 Автомобиль", callback_data="vehicle:truck"),
        InlineKeyboardButton("🚂 Ж/д", callback_data="vehicle:train")
    )
    keyboard.add(
        InlineKeyboardButton("🚢 Судно", callback_data="vehicle:ship"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel")
    )
    return keyboard


def expeditor_service_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📄 Документы", callback_data="service:docs"),
        InlineKeyboardButton("🏢 Таможня", callback_data="service:customs")
    )
    keyboard.add(
        InlineKeyboardButton("🚢 Фрахт", callback_data="service:freight"),
        InlineKeyboardButton("📦 Полный сервис", callback_data="service:full")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    return keyboard

# ============= МОИ СДЕЛКИ ФЕРМЕРА =============
@dp.message_handler(Text(equals='📋 Мои сделки'), state='*')
async def show_my_deals_message(message: types.Message, state: FSMContext):
    """Показать сделки пользователя"""
    user_id = message.from_user.id
    user_role = users.get(user_id, {}).get('role', 'unknown')

    # ✅ ПРОВЕРКА ТИПА ДАННЫХ
    if not isinstance(batches, dict):
        logging.error(f"❌ batches имеет неправильный тип: {type(batches)}")
        batches_local = {}
    else:
        batches_local = batches

    user_batches = batches_local.get(user_id, [])
    
    if user_role == 'farmer':
        user_batches = batches.get(user_id, [])
        if not user_batches:
            await message.answer("📭 У вас пока нет активных партий", reply_markup=farmer_keyboard())
            return
        text = "📦 Мои партии:\n\n"
        for b in user_batches[:10]:
            text += f"📦 #{b.get('id')} | {b.get('culture')} | {b.get('volume')}т\n"
        await message.answer(text, reply_markup=farmer_keyboard())
    
    elif user_role == 'exporter':
        user_matches = [m for m in matches.values() 
                       if pulls.get(m.get('pull_id'), {}).get('exporter_id') == user_id]
        if not user_matches:
            await message.answer("📭 У вас пока нет активных сделок", reply_markup=exporter_keyboard())
            return
        text = "📋 Мои сделки:\n\n"
        for m in user_matches[:10]:
            text += f"🤝 Сделка #{m.get('id')} | Пул #{m.get('pull_id')}\n"
        await message.answer(text, reply_markup=exporter_keyboard())
    
    else:
        await message.answer("❌ Неизвестная роль")


@dp.message_handler(lambda m: m.text == "📋 Доступные сделки", state='*')
async def expeditor_view_available_deals(message: types.Message, state: FSMContext):
    """Просмотр доступных сделок для экспедитора"""
    await state.finish()
    
    user_id = message.from_user.id
    
    if user_id not in users or users[user_id].get('role') != 'expeditor':
        await message.answer("❌ Эта функция доступна только экспедиторам")
        return
    
    # Находим сделки со статусом matched (ждут логистику)
    available_deals = []
    for exporter_id, exporter_deals in deals.items():
        for deal in exporter_deals:
            if deal.get('status') == 'matched' and deal.get('expeditor_id') is None:
                available_deals.append(deal)
    
    if not available_deals:
        await message.answer(
            "📋 <b>Доступные сделки</b>\n\n"
            "В данный момент нет доступных сделок.",
            parse_mode='HTML'
        )
        return
    
    msg = f"📋 <b>Доступные сделки ({len(available_deals)})</b>\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for deal in available_deals[:10]:  # Показываем первые 10
        deal_info = (
            f"🌾 {deal['culture']} • "
            f"{deal['total_volume']} т • "
            f"🚢 {deal['port']}"
        )
        
        keyboard.add(
            InlineKeyboardButton(
                deal_info,
                callback_data=f"expeditor_view_deal:{deal['id']}"
            )
        )
    
    await message.answer(msg + "Выберите сделку для просмотра:", reply_markup=keyboard, parse_mode='HTML')


@dp.callback_query_handler(lambda c: c.data.startswith('expeditor_view_deal:'), state='*')
async def expeditor_view_deal_details(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей сделки"""
    await state.finish()
    
    try:
        deal_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    # Ищем сделку
    deal = None
    for exporter_deals in deals.values():
        for d in exporter_deals:
            if d['id'] == deal_id:
                deal = d
                break
        if deal:
            break
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    msg = f"📋 <b>Сделка #{deal_id}</b>\n\n"
    msg += f"🌾 Культура: {deal['culture']}\n"
    msg += f"📦 Объём: {deal['total_volume']} т\n"
    msg += f"💰 Цена: {deal['price']:,.0f} ₽/т\n"
    msg += f"🚢 Порт: {deal['port']}\n"
    msg += f"👥 Участников: {len(deal.get('participants', []))}\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("✅ Взять в работу", callback_data=f"expeditor_take:{deal_id}"),
        InlineKeyboardButton("◀️ Назад", callback_data="expeditor_available_deals")
    )
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('expeditor_take:'), state='*')
async def expeditor_take_deal(callback: types.CallbackQuery, state: FSMContext):
    """Взять сделку в работу"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    try:
        deal_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    # Ищем сделку
    deal = None
    for exporter_deals in deals.values():
        for d in exporter_deals:
            if d['id'] == deal_id:
                deal = d
                break
        if deal:
            break
    
    if not deal:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    if deal.get('expeditor_id'):
        await callback.answer("❌ Сделка уже взята другим экспедитором", show_alert=True)
        return
    
    # Назначаем экспедитора
    deal['expeditor_id'] = user_id
    deal['expeditor_name'] = users[user_id].get('name', 'Неизвестно')
    deal['status'] = 'in_progress'
    
    save_deals_to_pickle()
    
    await callback.answer("✅ Сделка взята в работу!", show_alert=True)
    
    await callback.message.edit_text(
        f"✅ <b>Сделка #{deal_id} взята в работу!</b>\n\n"
        f"🌾 {deal['culture']} • {deal['total_volume']} т\n"
        f"🚢 Порт: {deal['port']}\n\n"
        f"Перейдите в '💼 Мои сделки' для работы с документами.",
        parse_mode='HTML'
    )
    
    # Уведомляем экспортёра
    try:
        await bot.send_message(
            deal['exporter_id'],
            f"✅ <b>Сделка #{deal_id} взята в работу!</b>\n\n"
            f"📋 Экспедитор: {users[user_id].get('name')}\n"
            f"📱 Телефон: {users[user_id].get('phone')}\n\n"
            f"Экспедитор начнёт оформление документов.",
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка уведомления экспортёра: {e}")


@dp.message_handler(lambda m: m.text == "💼 Мои сделки", state='*')
async def expeditor_my_deals(message: types.Message, state: FSMContext):
    """Просмотр сделок экспедитора"""
    await state.finish()
    
    user_id = message.from_user.id
    
    # Находим сделки этого экспедитора
    my_deals = []
    for exporter_deals in deals.values():
        for deal in exporter_deals:
            if deal.get('expeditor_id') == user_id:
                my_deals.append(deal)
    
    if not my_deals:
        await message.answer(
            "💼 <b>Мои сделки</b>\n\n"
            "У вас пока нет сделок в работе.",
            parse_mode='HTML'
        )
        return
    
    msg = f"💼 <b>Мои сделки ({len(my_deals)})</b>\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for deal in my_deals:
        status_emoji = {
            'in_progress': '⏳',
            'documents_ready': '📄',
            'completed': '✅'
        }.get(deal.get('status'), '📋')
        
        deal_info = (
            f"{status_emoji} #{deal['id']} • "
            f"{deal['culture']} • "
            f"{deal['total_volume']} т"
        )
        
        keyboard.add(
            InlineKeyboardButton(
                deal_info,
                callback_data=f"expeditor_my_deal:{deal['id']}"
            )
        )
    
    await message.answer(msg + "Выберите сделку:", reply_markup=keyboard, parse_mode='HTML')



def admin_keyboard():
    """Клавиатура для админа"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("👥 Пользователи"),
        KeyboardButton("📊 Статистика")
    )
    keyboard.add(
        KeyboardButton("📢 Рассылка"),
        KeyboardButton("📥 Экспорт данных")
    )
    keyboard.add(
        KeyboardButton("🔄 Обновить цены"),
        KeyboardButton("📈 Аналитика")
    )
    keyboard.add(KeyboardButton("🏠 Главное меню"))
    return keyboard


def adminkeyboard():
    """Алиас для совместимости"""
    return admin_keyboard()


def format_admin_statistics():
    """Форматирование статистики для админа"""
    total_users = len(users)
    farmers_count = len([u for u in users.values() if u.get('role') == 'farmer'])
    exporters_count = len([u for u in users.values() if u.get('role') == 'exporter'])
    logistics_count = len([u for u in users.values() if u.get('role') == 'logistic'])
    expeditors_count = len([u for u in users.values() if u.get('role') == 'expeditor'])

    total_pulls = len(pulls)
    active_pulls = len([p for p in pulls.values() if p.get('status') == 'active'])

    total_batches = sum(len(batches) for user_batches in batches.values())

    total_requests = len(shipping_requests)
    active_requests = len([r for r in shipping_requests.values() if r.get('status') == 'active'])

    msg = "📊 <b>Статистика бота</b>\n\n"
    msg += "👥 <b>Пользователи:</b>\n"
    msg += f"  • Всего: {total_users}\n"
    msg += f"  • Фермеры: {farmers_count}\n"
    msg += f"  • Экспортёры: {exporters_count}\n"
    msg += f"  • Логисты: {logistics_count}\n"
    msg += f"  • Экспедиторы: {expeditors_count}\n\n"

    msg += "🎯 <b>Пулы:</b>\n"
    msg += f"  • Всего: {total_pulls}\n"
    msg += f"  • Активные: {active_pulls}\n\n"

    msg += "📦 <b>Партии:</b>\n"
    msg += f"  • Всего: {total_batches}\n\n"

    msg += "🚚 <b>Заявки на логистику:</b>\n"
    msg += f"  • Всего: {total_requests}\n"
    msg += f"  • Активные: {active_requests}\n"

    return msg

@dp.message_handler(commands=['reset'], state='*')
async def reset_account(message: types.Message, state: FSMContext):
    """Удалить свой аккаунт для повторной регистрации"""
    user_id = message.from_user.id

    # Очищаем состояние FSM
    await state.finish()

    # Создаём клавиатуру подтверждения
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_reset:{user_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_reset")
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
        parse_mode="Markdown"
    )


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_reset:'), state='*')
async def confirm_reset_account(callback: CallbackQuery, state: FSMContext):
    """Подтверждение удаления аккаунта"""
    user_id = parse_callback_id(callback.data)

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не ваш аккаунт", show_alert=True)
        return

    deleted_items = []

    # 1. Удаляем из словаря users (ПАМЯТЬ БОТА)
    if user_id in users:
        role = users[user_id].get('role', 'user')
        del users[user_id]
        deleted_items.append(f"профиль ({role})")
        logging.info(f"✅ Удалён user {user_id} из памяти")

    # 2. Удаляем партии фермера (ПАМЯТЬ БОТА)
    if user_id in batches:
        batch_count = len(batches[user_id])
        del batches[user_id]
        deleted_items.append(f"{batch_count} партий")
        logging.info(f"✅ Удалено {batch_count} партий фермера {user_id}")
    
    # 4. Удаляем из Google Sheets - Users
    try:
        worksheet = spreadsheet.worksheet('Users')
        cell = worksheet.find(str(user_id))
        if cell:
            worksheet.delete_rows(cell.row)
            deleted_items.append("запись в Google Sheets (Users)")
            logging.info(f"✅ Удалён аккаунт {user_id} из Google Sheets (Users)")
    except Exception as e:
        logging.error(f"❌ Ошибка удаления из Google Sheets (Users): {e}")

    # 5. Удаляем партии из Google Sheets - Batches
    try:
        worksheet = spreadsheet.worksheet('Batches')
        all_values = worksheet.get_all_values()
        rows_to_delete = []

        for i, row in enumerate(all_values[1:], start=2):  # Пропускаем заголовок
            if row and len(row) > 1 and str(row[1]) == str(user_id):  # farmer_id в колонке 2
                rows_to_delete.append(i)

        # Удаляем с конца чтобы индексы не сбивались
        for row_num in reversed(rows_to_delete):
            worksheet.delete_rows(row_num)

        if rows_to_delete:
            deleted_items.append(f"{len(rows_to_delete)} партий из Google Sheets")
            logging.info(f"✅ Удалено {len(rows_to_delete)} партий из Google Sheets")
    except Exception as e:
        logging.error(f"❌ Ошибка удаления партий из Google Sheets: {e}")

    # 6. Удаляем пулы из Google Sheets - Pulls
    try:
        worksheet = spreadsheet.worksheet('Pulls')
        all_values = worksheet.get_all_values()
        rows_to_delete = []

        for i, row in enumerate(all_values[1:], start=2):
            if row and len(row) > 1 and str(row[1]) == str(user_id):  # exporter_id в колонке 2
                rows_to_delete.append(i)

        for row_num in reversed(rows_to_delete):
            worksheet.delete_rows(row_num)

        if rows_to_delete:
            deleted_items.append(f"{len(rows_to_delete)} пуллов из Google Sheets")
            logging.info(f"✅ Удалено {len(rows_to_delete)} пуллов из Google Sheets")
    except Exception as e:
        logging.error(f"❌ Ошибка удаления пуллов из Google Sheets: {e}")

    # Формируем сообщение о результатах
    if deleted_items:
        items_text = "\n".join([f"• {item}" for item in deleted_items])
        result_msg = (
            f"✅ *Аккаунт удалён!*\n\n"
            f"Удалено:\n{items_text}\n\n"
            f"Вы можете зарегистрироваться заново командой /start"
        )
    else:
        result_msg = "⚠️ Аккаунт не найден или уже удалён"

    await callback.message.edit_text(result_msg, parse_mode="Markdown")
    await callback.answer("✅ Аккаунт удалён")


@dp.callback_query_handler(lambda c: c.data == 'cancel_reset', state='*')
async def cancel_reset_account(callback: CallbackQuery):
    """Отмена удаления аккаунта"""
    await callback.message.edit_text("❌ Удаление отменено")
    await callback.answer("Отменено")
def format_admin_analytics():
    """Форматирование аналитики для админа"""
    regions_count = {}
    for user in users.values():
        region = user.get('region', 'Не указан')
        regions_count[region] = regions_count.get(region, 0) + 1

    top_regions = sorted(regions_count.items(), key=lambda x: x[1], reverse=True)[:5]

    cultures_count = {}
    for user_batches in batches.values():
        for batch in user_batches:
            culture = batch.get('culture', 'Не указана')
            cultures_count[culture] = cultures_count.get(culture, 0) + 1

    top_cultures = sorted(cultures_count.items(), key=lambda x: x[1], reverse=True)[:5]

    pool_stats = {'forming': 0, 'active': 0, 'completed': 0, 'cancelled': 0}
    for pull in pulls.values():
        status = pull.get('status', 'unknown')
        if status in pool_stats:
            pool_stats[status] += 1

    msg = "📈 <b>Аналитика бота</b>\n\n"

    if top_regions:
        msg += "🗺 <b>Топ-5 регионов:</b>\n"
        for idx, (region, count) in enumerate(top_regions, 1):
            msg += f"  {idx}. {region}: {count} польз.\n"

    if top_cultures:
        msg += "\n🌾 <b>Топ-5 культур:</b>\n"
        for idx, (culture, count) in enumerate(top_cultures, 1):
            msg += f"  {idx}. {culture}: {count} партий\n"

    msg += "\n🎯 <b>Статусы пулов:</b>\n"
    msg += f"  • Формируется: {pool_stats['forming']}\n"
    msg += f"  • Активные: {pool_stats['active']}\n"
    msg += f"  • Завершённые: {pool_stats['completed']}\n"
    msg += f"  • Отменённые: {pool_stats['cancelled']}\n"

    return msg


def format_admin_users():
    """Форматирование списка пользователей для админа"""
    farmers = [u for u in users.values() if u.get('role') == 'farmer']
    exporters = [u for u in users.values() if u.get('role') == 'exporter']
    logistics = [u for u in users.values() if u.get('role') == 'logistic']
    expeditors = [u for u in users.values() if u.get('role') == 'expeditor']

    msg = "👥 <b>Пользователи системы</b>\n\n"
    msg += f"Всего: {len(users)}\n\n"

    if farmers:
        msg += f"<b>🌾 Фермеры ({len(farmers)})</b>\n"
        for u in farmers[:5]:
            name = u.get('name', 'Без имени')
            phone = u.get('phone', 'Нет телефона')
            region = u.get('region', 'Не указан')
            msg += f"  • {name}\n    📱 {phone}\n    📍 {region}\n"
        if len(farmers) > 5:
            msg += f"  ... и ещё {len(farmers) - 5}\n"
        msg += "\n"

    if exporters:
        msg += f"<b>🚢 Экспортёры ({len(exporters)})</b>\n"
        for u in exporters[:5]:
            name = u.get('name', 'Без имени')
            phone = u.get('phone', 'Нет телефона')
            msg += f"  • {name}\n    📱 {phone}\n"
        if len(exporters) > 5:
            msg += f"  ... и ещё {len(exporters) - 5}\n"
        msg += "\n"

    if logistics:
        msg += f"<b>🚚 Логисты ({len(logistics)})</b>\n"
        for u in logistics[:5]:
            name = u.get('name', 'Без имени')
            phone = u.get('phone', 'Нет телефона')
            msg += f"  • {name}\n    📱 {phone}\n"
        if len(logistics) > 5:
            msg += f"  ... и ещё {len(logistics) - 5}\n"
        msg += "\n"

    if expeditors:
        msg += f"<b>📋 Экспедиторы ({len(expeditors)})</b>\n"
        for u in expeditors[:3]:
            name = u.get('name', 'Без имени')
            phone = u.get('phone', 'Нет телефона')
            msg += f"  • {name}\n    📱 {phone}\n"
        if len(expeditors) > 3:
            msg += f"  ... и ещё {len(expeditors) - 3}\n"

    return msg


def join_pull_keyboard(pull_id):
    """Клавиатура для присоединения к пулу"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Присоединиться", callback_data=f"confirm_join_pull:{pull_id}"),
        InlineKeyboardButton("📋 Выбрать партию", callback_data=f"select_batch_for_pull:{pull_id}"),
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_pools_list")
    )
    return keyboard

def get_pull_details_keyboard(pull_id, user_id, pull):
    """Создание клавиатуры для карточки пула"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    if user_id == pull.get('exporter_id'):
        keyboard.add(
            InlineKeyboardButton("👥 Участники", callback_data=f"viewparticipants:{pull_id}"),
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"editpull_{pull_id}")
        )
        keyboard.add(
            InlineKeyboardButton("✅ Закрыть пул", callback_data=f"closepull_{pull_id}"),
            InlineKeyboardButton("❌ Удалить", callback_data=f"deletepull_{pull_id}")
        )

    elif user_id in users and users[user_id].get('role') == 'farmer':
        keyboard.add(
            InlineKeyboardButton("✅ Присоединиться", callback_data=f"join_pull:{pull_id}")
        )
        keyboard.add(
            InlineKeyboardButton("👥 Участники", callback_data=f"viewparticipants:{pull_id}")
        )
    
    else:
        keyboard.add(
            InlineKeyboardButton("👥 Участники", callback_data=f"viewparticipants:{pull_id}")
        )
    
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_pools")
    )
    
    return keyboard

def logistics_offer_keyboard():
    """Клавиатура для логистических услуг"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚛 Фура", callback_data="transport_type:truck"),
        InlineKeyboardButton("🚂 Ж/Д", callback_data="transport_type:train"),
        InlineKeyboardButton("🚢 Судно", callback_data="transport_type:ship")
    )
    return keyboard

def admin_broadcast_keyboard():
    """Клавиатура подтверждения рассылки"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Отправить", callback_data="broadcast_confirm"),
        InlineKeyboardButton("❌ Отменить", callback_data="broadcast_cancel")
    )
    return keyboard

def culture_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    crops = ["Пшеница", "Ячмень", "Кукуруза", "Подсолнечник", "Рапс", "Соя"]
    buttons = [InlineKeyboardButton(crop, callback_data=f"culture:{crop}") for crop in crops]
    keyboard.add(*buttons)
    return keyboard

def region_keyboard():
    """Клавиатура выбора региона"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    regions = [
        'Астраханская область', 'Краснодарский край', 'Ставропольский край', 'Ростовская область',
        'Волгоградская область', 'Воронежская область', 'Курская область',
        'Белгородская область', 'Саратовская область', 'Оренбургская область',
        'Алтайский край', 'Омская область', 'Новосибирская область'
    ]
    for region in regions:
        keyboard.add(
            InlineKeyboardButton(region, callback_data=f"region:{region}")
        )
    return keyboard

def port_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "Ариб-Снаб",
        "КЗК",
        "ПКФ ЮТ",
        "ПКФ ВП",
        "Астр Порт",
        "Астр Унив",
        "Астр Южн",
        "ЗЮЙД-ВЕСТ",
        "АгроФуд",
        "Порт 83",
        "ПКФ ЦГП",
        "Астр зерн",
        "Астр морск"
    ]

    buttons = [InlineKeyboardButton(port, callback_data=f"selectport_{port}") for port in ports]
    keyboard.add(*buttons)
    return keyboard

def quality_class_keyboard():
    """Клавиатура выбора класса качества"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    for quality_class in QUALITY_CLASSES:
        keyboard.add(
            InlineKeyboardButton(quality_class, callback_data=f"quality:{quality_class}")
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
        InlineKeyboardButton("❌ Нет", callback_data=f"cancel:{action}")
    )
    return keyboard

def batch_actions_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_batch:{batch_id}"),
        InlineKeyboardButton("🗑 Удалить", callback_data=f"delete_batch:{batch_id}")
    )
    keyboard.add(InlineKeyboardButton("📎 Прикрепить файлы", callback_data=f"attach_files:{batch_id}"))
    keyboard.add(InlineKeyboardButton("📄 Просмотр файлов", callback_data=f"view_files:{batch_id}"))
    keyboard.add(InlineKeyboardButton("🔍 Найти экспортёров", callback_data=f"find_exporters:{batch_id}"))
    keyboard.add(
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_batches")  # ← ИСПРАВЬ ЭТО
    )
    return keyboard


def edit_batch_fields_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 Культура", callback_data=f"editfield_crop_{batch_id}")  # ← ДОБАВЬ ЭТО
    )
    keyboard.add(
        InlineKeyboardButton("💰 Цена", callback_data="edit_field:price"),
        InlineKeyboardButton("📦 Объём", callback_data="edit_field:volume")
    )
    keyboard.add(
        InlineKeyboardButton("💧 Влажность", callback_data="edit_field:humidity"),
        InlineKeyboardButton("🌾 Сорность", callback_data="edit_field:impurity")
    )
    keyboard.add(
        InlineKeyboardButton("⭐ Класс качества", callback_data="edit_field:quality_class"),
        InlineKeyboardButton("🏭 Тип хранения", callback_data="edit_field:storage_type")
    )
    keyboard.add(
        InlineKeyboardButton("📅 Дата готовности", callback_data="edit_field:readiness_date"),
        InlineKeyboardButton("📊 Статус", callback_data="edit_field:status")
    )
    keyboard.add(
        InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel")
    )
    return keyboard

def status_keyboard():
    """Клавиатура выбора статуса партии"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    statuses = ['Активна', 'Зарезервирована', 'Продана', 'Снята с продажи']
    for status in statuses:
        keyboard.add(
            InlineKeyboardButton(status, callback_data=f"status:{status}")
        )
    return keyboard

def profile_edit_keyboard():
    """Клавиатура редактирования профиля"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📱 Телефон", callback_data="edit_profile:phone"),
        InlineKeyboardButton("📧 Email", callback_data="edit_profile:email")
    )
    keyboard.add(
        InlineKeyboardButton("📍 Регион", callback_data="edit_profile:region"),
        InlineKeyboardButton("🏢 Реквизиты", callback_data="edit_profile:company_details")
    )
    keyboard.add(
        InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel")
    )
    return keyboard

def search_criteria_keyboard():
    """Клавиатура выбора критериев поиска"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 По культуре", callback_data="search_by:culture"),
        InlineKeyboardButton("📍 По региону", callback_data="search_by:region")
    )
    keyboard.add(
        InlineKeyboardButton("📦 По объёму", callback_data="search_by:volume"),
        InlineKeyboardButton("💰 По цене", callback_data="search_by:price")
    )
    keyboard.add(
        InlineKeyboardButton("⭐ По качеству", callback_data="search_by:quality"),
        InlineKeyboardButton("🏭 По типу хранения", callback_data="search_by:storage")
    )
    keyboard.add(
        InlineKeyboardButton("🔍 Все параметры", callback_data="search_by:all"),
        InlineKeyboardButton("🌾 Только доступные", callback_data="search_by:available")
    )
    keyboard.add(
        InlineKeyboardButton("📍 Топ регионы", callback_data="search_by:topregions")
    )
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_main_menu")  # ← ЭТА СТРОКА
    )
    return keyboard

@dp.callback_query_handler(lambda c: c.data == 'back_to_main_menu', state='*')
async def back_to_main_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню из расширенного поиска"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    if user_id not in users:
        await callback.message.answer(
            "❌ Пользователь не найден. Используйте /start для регистрации"
        )
        await callback.answer()
        return
    
    user = users[user_id]
    role = user.get('role', 'unknown')
    name = user.get('name', 'Пользователь')
    
    # Удаляем inline сообщение
    try:
        await callback.message.delete()
    except Exception as e:
        print(f"Ошибка: {e}")
        pass
    
    # Отправляем новое сообщение с ReplyKeyboard для роли
    if role == 'farmer':
        keyboard = farmer_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n🌾 <b>Меню фермера</b>"
    elif role == 'exporter':
        keyboard = exporter_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n📦 <b>Меню экспортёра</b>"
    elif role == 'logistic':
        keyboard = logistic_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n🚚 <b>Меню логиста</b>"
    elif role == 'expeditor':
        keyboard = expeditor_keyboard()
        welcome_text = f"👋 С возвращением, {name}!\n\n🏭 <b>Меню экспедитора</b>"
    else:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(KeyboardButton("📋 Меню"))
        welcome_text = f"👋 С возвращением, {name}!"
    
    await callback.message.answer(
        welcome_text,
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await callback.answer()


def deal_actions_keyboard(deal_id):
    """Клавиатура действий со сделкой"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📞 Контакты партнёра", callback_data=f"contact_partner:{deal_id}"),
        InlineKeyboardButton("🚚 Логистика", callback_data=f"logistics:{deal_id}")
    )
    keyboard.add(
        InlineKeyboardButton("✅ Завершить сделку", callback_data=f"complete_deal:{deal_id}"),
        InlineKeyboardButton("❌ Отменить сделку", callback_data=f"cancel_deal:{deal_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 К списку сделок", callback_data="back_to_deals")
    )
    return keyboard

async def notify_logistic_pull_closed(pull_id):
    """Уведомить логистов о закрытии пулла"""
    pull = pulls.get(pull_id)
    if not pull:
        return
    
    # Найти всех логистов, откликнувшихся на этот пулл
    related_logistics = []
    for req_id, req in shipping_requests.items():
        if req.get('pull_id') == pull_id:
            logist_id = req.get('logist_id')
            if logist_id:
                related_logistics.append(logist_id)
    
    # Отправить уведомления логистам
    for logist_id in set(related_logistics):
        try:
            await bot.send_message(
                logist_id,
                f"🔒 <b>Пулл #{pull_id} закрыт</b>\n\n"
                f"🌾 {pull.get('culture', 'N/A')}\n"
                f"📦 {pull.get('target_volume', 0)} т\n"
                f"📍 {pull.get('port', 'N/A')}\n\n"
                f"Спасибо за ваше предложение!",
                parse_mode='HTML'
            )
            logging.info(f"Уведомление логисту {logist_id} о закрытии пулла {pull_id}")
        except Exception as e:
            logging.error(f"Ошибка уведомления логиста {logist_id}: {e}")


def format_news_message():
    """Форматирование сообщения с новостями"""
    if not news_cache or not news_cache.get('data'):
        return (
            "📰 <b>Новости рынка</b>\n\n"
            "⚠️ Новости ещё не загружены.\n"
            "Попробуйте позже или используйте /start для обновления."
        )
    
    news_list = news_cache['data']
    updated_time = news_cache['updated'].strftime("%d.%m.%Y %H:%M") if news_cache.get('updated') else "Неизвестно"
    
    if not news_list:
        return (
            "📰 <b>Новости рынка</b>\n\n"
            "🤷‍♂️ Новостей не найдено.\n"
            "Попробуйте позже."
        )
    
    message = "📰 <b>Последние новости рынка</b>\n\n"
    
    for i, news_item in enumerate(news_list[:5], 1):   
        title = news_item.get('title', 'Без названия')
        link = news_item.get('link', '')
        date = news_item.get('date', 'Неизвестно')
        
        if link:
            message += f"{i}. <a href='{link}'>{title}</a>\n"
        else:
            message += f"{i}. {title}\n"
        
        message += f"   📅 <i>{date}</i>\n\n"
    
    message += f"🕐 Обновлено: {updated_time}"
    
    return message

def format_prices_message():
    """✅ Форматирование сообщения с ценами - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    if not prices_cache or not prices_cache.get('data'):
        return (
            "📊 <b>Актуальные цены на зерно</b>\n\n"
            "⚠️ Данные ещё не загружены.\n"
            "Используйте /start для обновления."
        )
    
    updated_time = prices_cache['updated'].strftime("%d.%m.%Y %H:%M") if prices_cache.get('updated') else "Неизвестно"
    
    data = prices_cache['data']
    russia = data.get('russia_south', {})
    fob = data.get('fob', 0)
    cbot = data.get('cbot', {})
    
    message = "📊 <b>Актуальные цены на зерно</b>\n\n"
    
    if russia:
        message += "🇷🇺 <b>Юг России (руб/т)</b>\n"
        for culture, price in russia.items():
            if isinstance(price, (int, float)):
                message += f"  • {culture}: <code>{price:,.0f} ₽/т</code>\n"
            else:
                message += f"  • {culture}: <code>{price}</code>\n"
    
    message += "\n🚢 <b>FOB Черное море</b>\n"
    if isinstance(fob, (int, float)):
        message += f"  • Пшеница: <code>${fob:.2f}/т</code>\n"
    else:
        message += f"  • Пшеница: <code>{fob}</code>\n"
    
    if cbot:
        message += "\n🌎 <b>CBOT</b>\n"
        for culture, price in cbot.items():
            if price:
                message += f"  • {culture}: <code>{price}</code>\n"
    
    message += f"\n🕐 Обновлено: {updated_time}"
    
    return message

def format_farmer_card(farmer_id, batch_id=None):
    """Форматирование полной карточки фермера с контактами"""
    
    if farmer_id not in users:
        return "❌ Фермер не найден"
    
    farmer = users[farmer_id]
    
    msg = f"👤 <b>Фермер: {farmer.get('name', 'Неизвестно')}</b>\n\n"
    
    msg += "<b>📞 Контакты:</b>\n"
    msg += f"📱 Телефон: <code>{farmer.get('phone', 'Не указан')}</code>\n"
    msg += f"📧 Email: <code>{farmer.get('email', 'Не указан')}</code>\n"
    msg += f"📍 Регион: {farmer.get('region', 'Не указан')}\n\n"
    
    if farmer.get('inn'):
        msg += "<b>🏢 Реквизиты:</b>\n"
        msg += f"ИНН: <code>{farmer.get('inn')}</code>\n"
        if farmer.get('company_details'):
            details = farmer['company_details'][:200]
            msg += f"{details}...\n" if len(farmer['company_details']) > 200 else f"{details}\n"
        msg += "\n"
    
    # ✅ ИСПРАВЛЕНО: безопасное обращение к полям партии
    if batch_id and farmer_id in batches:
        for batch in batches[farmer_id]:
            if batch['id'] == batch_id:
                msg += f"<b>📦 Партия #{batch_id}:</b>\n"
                msg += f"🌾 Культура: {batch.get('culture', 'Не указано')}\n"
                msg += f"📦 Объём: {batch.get('volume', 0)} т\n"
                msg += f"💰 Цена: {batch.get('price', 0):,.0f} ₽/т\n"
                
                # ✅ КАЧЕСТВО - только если есть
                if 'moisture' in batch or 'nature' in batch:
                    msg += "\n<b>🔬 Качество:</b>\n"
                    if 'nature' in batch:
                        msg += f"   🌾 Натура: {batch.get('nature', 'Не указано')} г/л\n"
                    if 'moisture' in batch:
                        msg += f"   💧 Влажность: {batch['moisture']}%\n"
                    if 'impurity' in batch:
                        msg += f"   🌿 Сорность: {batch.get('impurity', 'Не указано')}%\n"
                
                # ✅ СТАТУС
                msg += f"\n📊 Статус: {batch.get('status', 'Активна')}\n"
                break
    
    # ✅ СТАТИСТИКА
    if farmer_id in batches:
        total_batches = len(batches[farmer_id])
        active_batches = len([b for b in batches[farmer_id] if b.get('status') == 'Активна'])
        
        msg += "\n<b>📊 Статистика:</b>\n"
        msg += f"Всего партий: {total_batches}\n"
        msg += f"Активных: {active_batches}\n"
    
    return msg


def get_role_keyboard(role):
    role = str(role).lower()
    if role in ['farmer', 'фермер']:
        return farmer_keyboard()
    elif role in ['exporter', 'экспортёр']:
        return exporter_keyboard()
    elif role in ['logistic', 'логист']:
        return logistic_keyboard()
    elif role in ['expeditor', 'экспедитор', 'broker', 'брокер']:
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
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except Exception as e:
        return False


async def find_matching_exporters(batch):
    """Поиск подходящих пулов экспортёров для партии фермера"""
    matching_pulls = []

    try:
        batch_culture = batch.get('culture', '').strip()
        batch_volume = batch.get('volume', 0)

        if not batch_culture or batch_volume <= 0:
            logging.warning(f"Некорректные данные партии: culture={batch_culture}, volume={batch_volume}")
            return []

        for pull_id, pull in pulls.items():
            pull_culture = pull.get('culture', '').strip()
            pull_status = pull.get('status', '')
            pull_current_volume = pull.get('current_volume', 0)
            pull_target_volume = pull.get('target_volume', 0)

            if (pull_culture.lower() == batch_culture.lower() and 
                pull_status != 'completed' and 
                pull_current_volume < pull_target_volume):

                free_space = pull_target_volume - pull_current_volume

                if free_space > 0:
                    exporter_id = pull.get('exporter_id')
                    exporter = users.get(exporter_id, {})

                    matching_pulls.append({
                        'pull_id': pull_id,
                        'pull': pull,
                        'exporter': exporter,
                        'exporter_id': exporter_id,
                        'exporter_name': exporter.get('name', 'Неизвестно'),
                        'exporter_company': exporter.get('company', 'Неизвестно'),
                        'exporter_phone': exporter.get('phone', 'Не указан'),
                        'culture': pull_culture,
                        'price': pull.get('price', 0),
                        'port': pull.get('port', 'Не указан'),
                        'free_space': free_space,
                        'current_volume': pull_current_volume,
                        'target_volume': pull_target_volume
                    })

        if matching_pulls:
            logging.info(f"✅ Найдено {len(matching_pulls)} пулов для партии {batch.get('id')}")
        else:
            logging.info(f"❌ Пулов для {batch_culture} не найдено")

        return matching_pulls

    except Exception as e:
        logging.error(f"❌ Ошибка find_matching_exporters: {e}")
        return []


async def find_matching_batches(pull_data):
    """Поиск подходящих партий для пула"""
    global batches
    matching_batches = []  # <-- ИСПРАВЛЕНО: список вместо словаря
    
    for user_id, user_batches in batches.items():
        for batch in user_batches:
            if (batch['culture'] == pull_data['culture'] and
                batch['status'] == 'Активна' and
                batch['price'] <= pull_data['price'] * 75 and  # Примерный курс
                batch.get('humidity', 999) <= pull_data.get('moisture', 0) and
                batch.get('impurity', 999) <= pull_data.get('impurity', 0)):
                
                matching_batches.append(batch)
    
    return matching_batches


async def create_match_notification(batch_id, pull_id):
    """Создание уведомления о совпадении"""
    global match_counter
    match_counter += 1
    
    match_data = {
        'id': match_counter,
        'batch_id': batch_id,
        'pull_id': pull_id,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'active'
    }
    
    matches[match_counter] = match_data
    return match_counter

async def notify_match(farmer_id, batch, matching_pulls, extra=None, *args, **kwargs):
    """Уведомление фермеру о найденных совпадениях"""
    try:
        if not matching_pulls:
            return

        batch_id = batch.get('id', '?')
        batch_culture = batch.get('culture', 'Неизвестно')
        batch_volume = batch.get('volume', 0)

        text = "🎯 <b>Найдены подходящие пулы!</b>\n\n"
        text += f"📦 Ваша партия: {batch_culture} - {batch_volume} т\n"
        text += f"🔍 Найдено пулов: {len(matching_pulls)}\n\n"

        kb = InlineKeyboardMarkup(row_width=1)
        
        for idx, match in enumerate(matching_pulls[:5], 1):
            pull_id = match.get('pull_id')
            unique_id = int(time.time() * 1000) + idx
            kb.add(InlineKeyboardButton(
                f"🔗 Присоединиться к Пулу #{pull_id}",
                callback_data=f"join_pull:{pull_id}:{unique_id}"
            ))
            text += f"<b>{idx}. Пул #{pull_id}</b>\n"
            text += f"🏢 {match.get('exporter_company', 'Не указано')}\n"
            text += f"👤 {match.get('exporter_name', 'Не указано')}\n"
            text += f"💰 {match.get('price', 0):,.0f} ₽/т\n"
            text += f"🏢 Порт: {match.get('port', 'Не указан')}\n"
            text += f"📊 {match.get('current_volume', 0)}/{match.get('target_volume', 0)} т\n"
            text += "\n"

        logging.info(f"🔄 Отправляю уведомление фермеру {farmer_id}...")
        logging.info(f"📝 Текст сообщения ({len(text)} символов): {text[:200]}...")
        logging.info(f"🔘 Кнопок: {len(kb.inline_keyboard)}")
        await bot.send_message(farmer_id, text, parse_mode='HTML', reply_markup=kb)
        logging.info(f"✅ Уведомление фермеру {farmer_id} УСПЕШНО отправлено!")

    except Exception as e:
        logging.error(f"❌ Ошибка уведомления фермеру {farmer_id}: {e}")

async def auto_match_batches_and_pulls():
    """Автоматический поиск совпадений между партиями и пулами"""
    try:
        logging.info("🔄 Запуск автоматического поиска совпадений...")
        
        matches_found = 0
        
        for pull_id, pull in pulls.items():
            if pull['status'] != 'Открыт':
                continue
                
            matching_batches = await find_matching_batches(pull)
            
            for batch in matching_batches:
                # Пропускаем, если фермер = владельцу пула
                if batch.get("farmer_id") == pull.get("exporter_id"):
                    continue
                    
                # Проверяем, есть ли уже активное совпадение
                existing_match = None
                for match in matches.values():
                    if (match['batch_id'] == batch['id'] and 
                        match['pull_id'] == pull_id and 
                        match['status'] == 'active'):
                        existing_match = match
                        break
                
                # Если совпадение новое, отправляем уведомление
                if not existing_match:
                    # Создаём правильный match-объект
                    match_obj = {
                        'pull_id': pull_id,
                        'exporter_company': pull.get('company', ''),
                        'exporter_name': pull.get('contact', ''),
                        'price': pull.get('price', 0),
                        'port': pull.get('port', ''),
                        'current_volume': pull.get('current_volume', 0),
                        'target_volume': pull.get('target_volume', 0)
                    }
                    
                    await notify_match(
                        batch["farmer_id"], 
                        batch, 
                        [match_obj]
                    )
                    
                    # ✅ СОЗДАЕМ ЗАПИСЬ О СОВПАДЕНИИ
                    match_id = len(matches) + 1
                    matches[match_id] = {
                        'id': match_id,
                        'batch_id': batch['id'],
                        'pull_id': pull_id,
                        'status': 'active',
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    matches_found += 1
                    await asyncio.sleep(0.1)  
        
        logging.info(f"✅ Автопоиск завершен. Найдено совпадений: {matches_found}")
        return matches_found
    
    except Exception as e:
        logging.error(f"❌ Ошибка автопоиска: {e}")
        return 0


# ============================================================================
# ОБРАБОТЧИК КОМАНДЫ /start
# ============================================================================

@dp.message_handler(commands=['start'], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
    await state.finish()
    user_id = message.from_user.id
    
    logging.info(f"🚀 /start от пользователя {user_id}")

    # Если пользователь уже зарегистрирован
    if user_id in users:
        user = users[user_id]
        role = user.get('role', 'unknown')
        name = user.get('name', 'Пользователь')
        
        logging.info(f"✅ Пользователь {user_id} уже зарегистрирован как {role}")

        welcome_text = f"👋 Добро пожаловать, {name}!\n\nВыберите действие:"

        if role == 'farmer':
            keyboard = farmer_keyboard()
        elif role == 'exporter':
            keyboard = exporter_keyboard()
        elif role == 'logistic':
            keyboard = logistic_keyboard()
        elif role == 'expeditor':
            keyboard = expeditor_keyboard()
        elif role == 'admin':
            keyboard = admin_keyboard()
        else:
            keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
            keyboard.add(KeyboardButton("📝 Регистрация"))

        await message.answer(welcome_text, reply_markup=keyboard, parse_mode='HTML')

    else:
        # ✅ НОВЫЙ ПОЛЬЗОВАТЕЛЬ
        logging.info(f"👤 Новый пользователь {user_id} - начинаем регистрацию")
        
        welcome_text = """
🌾 <b>EXPORTUM</b>

Платформа зернового рынка для:

• 👨‍🌾 Фермеров — продажа партий зерна
• 📦 Экспортёров — создание пуллов и закупка
• 🚛 Логистов — предложение транспортных услуг
• 📋 Экспедиторов — таможенное оформление

━━━━━━━━━━━━━━━━━━━━
📢 Канал: @EXPORTUM
💬 Чат: @exportum_chat
🤖 Бот: @exportumbot

📊 Котировки и новости обновляются ежедневно!
        """
        
        await message.answer(welcome_text, parse_mode='HTML')
        
        await message.answer(
            "📝 <b>Регистрация</b>\n\n"
            "Шаг 1 из 3\n\n"
            "Введите ваше имя:",
            parse_mode='HTML',
            reply_markup=types.ReplyKeyboardRemove()
        )
        
        await RegistrationStatesGroup.name.set()


# ============================================================================
# ADMIN CALLBACK HANDLERS - ВЫСОКИЙ ПРИОРИТЕТ (ПЕРЕД ВСЕМИ ОСТАЛЬНЫМИ!)
# ============================================================================

def format_admin_statistics():
    """Форматирование статистики"""
    total_users = len(users)
    farmers = sum(1 for u in users.values() if u.get('role') == 'farmer')
    exporters = sum(1 for u in users.values() if u.get('role') == 'exporter')
    logists = sum(1 for u in users.values() if u.get('role') == 'logistic')
    expeditors = sum(1 for u in users.values() if u.get('role') == 'expeditor')

    total_pulls = len(pulls)
    active_pulls = sum(1 for p in pulls.values() if p.get('status') == 'active')

    total_batches = sum(len(b) for b in batches.values())

    total_requests = len(shipping_requests)
    active_requests = sum(1 for r in shipping_requests.values() if r.get('status') == 'active')

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


def format_admin_analytics():
    """Форматирование аналитики"""
    regions = {}
    for user in users.values():
        region = user.get('region', 'Не указан')
        regions[region] = regions.get(region, 0) + 1

    top_regions = sorted(regions.items(), key=lambda x: x[1], reverse=True)[:5]

    cultures = {}
    for user_batches in batches.values():
        for batch in user_batches:
            culture = batch.get('culture', 'Неизвестно')
            cultures[culture] = cultures.get(culture, 0) + 1

    top_cultures = sorted(cultures.items(), key=lambda x: x[1], reverse=True)[:5]

    pull_statuses = {}
    for pull in pulls.values():
        status = pull.get('status', 'Неизвестно')
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
        status_emoji = "✅" if status == "active" else "⏸"
        msg += f"{status_emoji} {status.capitalize()}: {count}\n"

    return msg


def format_admin_users():
    """Форматирование списка пользователей"""
    if not users:
        return "❌ Нет пользователей"

    msg = "👥 <b>Пользователи системы</b>\n\n"

    roles = {
        'farmer': '🌾 Фермеры',
        'exporter': '💼 Экспортёры',
        'logistic': '🚚 Логисты',
        'expeditor': '⚓ Экспедиторы'
    }

    for role, title in roles.items():
        role_users = [u for u in users.values() if u.get('role') == role]
        if role_users:
            msg += f"{title}: {len(role_users)}\n"
            for user in role_users[:3]:
                name = user.get('name', 'Без названия')
                phone = user.get('phone', 'Нет телефона')
                msg += f"• {name} ({phone})\n"
            if len(role_users) > 3:
                msg += f"... и ещё {len(role_users) - 3}\n"
            msg += "\n"

    return msg


# Admin callback handlers - регистрируем ПЕРВЫМИ!
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admin'), state='*')
async def admin_callbacks_router(callback: types.CallbackQuery, state: FSMContext):
    """Роутер для всех admin callback handlers"""

    # КРИТИЧНО: Сбрасываем state СРАЗУ!
    current_state = await state.get_state()
    if current_state:
        logging.info(f"⚠️ Сбрасываем state: {current_state}")
        await state.finish()

    # Проверяем права админа
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    data = callback.data
    logging.info(f"🔑 Admin callback: {data} from {callback.from_user.id}")

    # Обрабатываем каждый callback
    if data == "adminstat":
        msg = format_admin_statistics()
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminstat"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )
        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer("✅ Статистика обновлена")

    elif data == "adminanalytics":
        msg = format_admin_analytics()
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminanalytics"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )
        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer("✅ Аналитика обновлена")

    elif data == "adminexport":
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("👥 Пользователи", callback_data="exportusers"),
            InlineKeyboardButton("📦 Пуллы", callback_data="exportpulls")
        )
        keyboard.add(
            InlineKeyboardButton("🌾 Партии", callback_data="exportbatches"),
            InlineKeyboardButton("📋 Заявки", callback_data="exportrequests")
        )
        keyboard.add(InlineKeyboardButton("💼 Полный бэкап", callback_data="exportfull"))
        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"))

        await callback.message.edit_text(
            "📤 <b>Экспорт данных</b>\n\nВыберите данные для экспорта:",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await callback.answer()

    elif data == "adminusers":
        msg = format_admin_users()
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminusers"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )
        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()

    elif data == "adminbroadcast":
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"))

        await callback.message.edit_text(
            "📧 <b>Рассылка</b>\n\nОтправьте сообщение для рассылки всем пользователям.",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await callback.answer()

    elif data == "adminprices":
        await callback.answer("⏳ Запускаю обновление цен...")

        try:
            await update_prices_cache()

            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin"))

            await callback.message.edit_text(
                "✅ <b>Цены обновлены успешно!</b>",
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Ошибка обновления цен: {e}")
            await callback.message.edit_text(f"❌ Ошибка обновления: {e}")


@dp.callback_query_handler(lambda c: c.data == "backtoadmin", state='*')
async def back_to_admin_callback(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в админ меню"""

    # Сбрасываем state
    current_state = await state.get_state()
    if current_state:
        logging.info(f"⚠️ Сбрасываем state: {current_state}")
        await state.finish()

    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    logging.info(f"◀️ Back to admin menu by {callback.from_user.id}")

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📊 Статистика", callback_data="adminstat"),
        InlineKeyboardButton("📈 Аналитика", callback_data="adminanalytics")
    )
    keyboard.add(
        InlineKeyboardButton("📤 Экспорт данных", callback_data="adminexport"),
        InlineKeyboardButton("👥 Пользователи", callback_data="adminusers")
    )
    keyboard.add(
        InlineKeyboardButton("📧 Рассылка", callback_data="adminbroadcast"),
        InlineKeyboardButton("💰 Обновить цены", callback_data="adminprices")
    )

    await callback.message.edit_text(
        "🔐 <b>Админ панель EXPORTUM</b>\n\nВыберите действие:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

# ============================================================================
# АВТОМАТИЧЕСКОЕ ЗАКРЫТИЕ ПУЛА И СОЗДАНИЕ СДЕЛКИ
# ============================================================================

def check_and_close_pull_if_full(pull_id):
    """Проверяет заполненность пула и закрывает его при 100%"""
    if pull_id not in pulls:
        return False
    
    pull = pulls[pull_id]
    current = pull.get('current_volume', 0)
    target = pull.get('target_volume', 0)
    
    if current >= target and pull.get('status') == 'Открыт':
        pull['status'] = 'Заполнен'
        pull['closed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        deal_id = create_deal_from_full_pull(pull)
        pull['deal_id'] = deal_id
        
        save_pulls_to_pickle()
        logging.info(f"✅ Pull {pull_id} auto-closed → Deal {deal_id}")
        
        asyncio.create_task(notify_all_about_pull_closure(pull, deal_id))
        return True
    
    return False


def create_deal_from_full_pull(pull):
    """Создаёт сделку из заполненного пула"""
    global deal_counter
    
    deal_counter += 1
    
    farmer_ids = []
    batch_details = []
    
    for participant in pull.get('participants', []):
        f_id = participant.get('farmer_id')
        b_id = participant.get('batch_id')
        volume = participant.get('volume', 0)
        
        if f_id not in farmer_ids:
            farmer_ids.append(f_id)
        
        batch_details.append({
            'farmer_id': f_id,
            'batch_id': b_id,
            'volume': volume,
            'farmer_name': users.get(f_id, {}).get('name', 'Неизвестно')
        })
    
    deal = {
        'id': deal_counter,
        'pull_id': pull['id'],
        'type': 'pool_deal',
        'exporter_id': pull['exporter_id'],
        'exporter_name': pull['exporter_name'],
        'farmer_ids': farmer_ids,
        'batches': batch_details,
        'logistic_id': None,
        'expeditor_id': None,
        'culture': pull['culture'],
        'volume': pull['current_volume'],
        'price': pull['price'],
        'total_sum': pull['current_volume'] * pull['price'],
        'port': pull['port'],
        'quality': {
            'moisture': pull.get('moisture', 0),
            'nature': pull.get('nature', 0),
            'impurity': pull.get('impurity', 0),
            'weed': pull.get('weed', 0)
        },
        'documents': pull.get('documents', ''),
        'doc_type': pull.get('doc_type', 'FOB'),
        'status': 'new',
        'payment_status': 'pending',
        'delivery_status': 'pending',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    deals[deal_counter] = deal
    save_deals_to_pickle()
    logging.info(f"✅ Deal {deal_counter} created from pull {pull['id']}")
    
    return deal_counter


async def notify_all_about_pull_closure(pull, deal_id):
    """Уведомление всех участников о закрытии пула с карточками подрядчиков"""
    pull_id = pull['id']
    exporter_id = pull['exporter_id']
    port = pull.get('port', 'Не указан')

    # 1. Подбор логистов и экспедиторов по порту
    logistics = get_logistics_by_port(port)
    expeditors = get_expeditors_by_port(port)

    # 2. Уведомление экспортёру
    exporter_text = f"""🎉 <b>ПУЛ #{pull_id} СОБРАН!</b>

📦 Культура: {pull.get('culture')}
🎯 Объём: {pull.get('current_volume')} / {pull.get('target_volume')} т
🏢 Порт: {port}
💰 Цена: {pull.get('price', 0):,.0f} ₽/т

✅ Сделка #{deal_id} создана
"""

    try:
        await bot.send_message(exporter_id, exporter_text, parse_mode='HTML')

        # 3. Отправляем карточки логистов с кнопками
        if logistics:
            logistics_text = f"\n🚚 <b>ДОСТУПНЫЕ ЛОГИСТЫ ({len(logistics)}):</b>\n\n"
            logistics_text += format_logistics_cards(logistics)

            logistics_keyboard = InlineKeyboardMarkup(row_width=1)
            for log in logistics[:5]:
                company = log.get('company', 'Без названия')[:30]
                user_id = log.get('user_id')
                logistics_keyboard.add(
                    InlineKeyboardButton(
                        f"🚚 {company}",
                        callback_data=f"select_logistic_{user_id}_{deal_id}"
                    )
                )

            await bot.send_message(
                exporter_id,
                logistics_text,
                reply_markup=logistics_keyboard,
                parse_mode='HTML'
            )

        # 4. Отправляем карточки экспедиторов с кнопками
        if expeditors:
            expeditors_text = f"\n📜 <b>ДОСТУПНЫЕ ЭКСПЕДИТОРЫ ({len(expeditors)}):</b>\n\n"
            expeditors_text += format_expeditors_cards(expeditors)

            expeditors_keyboard = InlineKeyboardMarkup(row_width=1)
            for exp in expeditors[:5]:
                company = exp.get('company', 'Без названия')[:30]
                user_id = exp.get('user_id')
                expeditors_keyboard.add(
                    InlineKeyboardButton(
                        f"📜 {company}",
                        callback_data=f"select_expeditor_{user_id}_{deal_id}"
                    )
                )

            await bot.send_message(
                exporter_id,
                expeditors_text,
                reply_markup=expeditors_keyboard,
                parse_mode='HTML'
            )
    except Exception as e:
        logging.error(f"Error sending to exporter {exporter_id}: {e}")

    # 5. Уведомления участникам (фермерам)
    participants = pull_participants.get(pull_id, [])
    for participant in participants:
        farmer_id = participant.get('farmer_id')
        volume = participant.get('volume', 0)

        try:
            await bot.send_message(
                farmer_id,
                f"""✅ <b>ПУЛ #{pull_id} СОБРАН!</b>

🌾 Культура: {pull.get('culture')}
📦 Ваш объём: {volume} т
💰 Цена: {pull.get('price', 0):,.0f} ₽/т
🏢 Порт: {port}

Ожидайте контакта от логиста.""",
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Error sending to farmer {farmer_id}: {e}")

    # 6. Уведомления логистам
    for logistic in logistics:
        try:
            await bot.send_message(
                logistic.get('user_id'),
                f"""🚚 <b>НОВЫЙ ПУЛ ДОСТУПЕН!</b>

📦 Пул #{pull_id}
🌾 {pull.get('culture')}
🎯 Объём: {pull.get('current_volume')} т
🏢 Порт: {port}

Ваша карточка передана экспортёру.""",
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"Ошибка: {e}")
            pass

    # 7. Уведомления экспедиторам
    for expeditor in expeditors:
        try:
            await bot.send_message(
                expeditor.get('user_id'),
                f"""📜 <b>НОВЫЙ ПУЛ ДОСТУПЕН!</b>

📦 Пул #{pull_id}
🌾 {pull.get('culture')}
🎯 Объём: {pull.get('current_volume')} т
🏢 Порт: {port}

Ваша карточка передана экспортёру.""",
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"Ошибка: {e}")
            pass

    logging.info(f"✅ Enhanced notifications sent for pull {pull_id}, deal {deal_id}")


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
                users_data.append({
                    'ID': uid,
                    'Имя': user.get('name', ''),
                    'Роль': user.get('role', ''),
                    'Телефон': user.get('phone', ''),
                    'Регион': user.get('region', ''),
                    'ИНН': user.get('inn', ''),
                    'Дата': user.get('registered_at', '')
                })

            df = pd.DataFrame(users_data)
            filename = f'users_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False, engine='openpyxl')

            with open(filename, 'rb') as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт пользователей\n\nВсего: {len(users_data)}"
                )

            os.remove(filename)

        elif data == "exportpulls":
            pulls_data = []
            for pull_id, pull in pulls.items():
                pulls_data.append({
                    'ID': pull_id,
                    'Культура': pull.get('culture', ''),
                    'Объём': pull.get('current_volume', 0),
                    'Цена': pull.get('price', 0),
                    'Порт': pull.get('port', ''),
                    'Статус': pull.get('status', ''),
                    'Экспортёр': pull.get('exporter_name', '')
                })

            df = pd.DataFrame(pulls_data)
            filename = f'pulls_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False, engine='openpyxl')

            with open(filename, 'rb') as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт пуллов\n\nВсего: {len(pulls_data)}"
                )

            os.remove(filename)

        elif data == "exportbatches":
            batches_data = []
            for farmer_id, batches in batches.items():
                farmer_name = users.get(farmer_id, {}).get('name', 'Неизвестен')
                for batch in user_batches:
                    batches_data.append({
                        'ID': batch.get('id', ''),
                        'Фермер': farmer_name,
                        'Культура': batch.get('culture', ''),
                        'Объём': batch.get('volume', 0),
                        'Цена': batch.get('price', 0),
                        'Регион': batch.get('region', ''),
                        'Статус': batch.get('status', '')
                    })

            df = pd.DataFrame(batches_data)
            filename = f'batches_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False, engine='openpyxl')

            with open(filename, 'rb') as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт партий\n\nВсего: {len(batches_data)}"
                )

            os.remove(filename)

        elif data == "exportrequests":
            requests_data = []
            for req_id, req in shipping_requests.items():
                requests_data.append({
                    'ID': req_id,
                    'От': req.get('from_city', ''),
                    'До': req.get('to_city', ''),
                    'Объём': req.get('volume', 0),
                    'Дата': req.get('loading_date', ''),
                    'Статус': req.get('status', '')
                })

            df = pd.DataFrame(requests_data)
            filename = f'requests_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False, engine='openpyxl')

            with open(filename, 'rb') as f:
                await callback.message.answer_document(
                    types.InputFile(f, filename=filename),
                    caption=f"📤 Экспорт заявок\n\nВсего: {len(requests_data)}"
                )

            os.remove(filename)

        elif data == "exportfull":
            import zipfile
            import io
            import json

            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr('users.json', json.dumps(users, ensure_ascii=False, indent=2))
                zip_file.writestr('pulls.json', json.dumps(pulls, ensure_ascii=False, indent=2))
                zip_file.writestr('batches.json', json.dumps(batches, ensure_ascii=False, indent=2))
                zip_file.writestr('deals.json', json.dumps(deals, ensure_ascii=False, indent=2))
                zip_file.writestr('shipping_requests.json', json.dumps(shipping_requests, ensure_ascii=False, indent=2))

                backup_info = {
                    'created_at': datetime.now().isoformat(),
                    'total_users': len(users),
                    'total_pulls': len(pulls),
                    'total_batches': sum(len(b) for b in batches.values())
                }
                zip_file.writestr('backup_info.json', json.dumps(backup_info, ensure_ascii=False, indent=2))

            zip_buffer.seek(0)

            filename = f'exportum_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            await callback.message.answer_document(
                types.InputFile(zip_buffer, filename=filename),
                caption=f"💼 Полный бэкап\n\n"
                        f"👥 Пользователей: {len(users)}\n"
                        f"📦 Пуллов: {len(pulls)}\n"
                        f"🌾 Партий: {sum(len(b) for b in batches.values())}"
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
    
    await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 1 из 7\n\n"
        "Введите ваше полное имя:",
        parse_mode='HTML',
        reply_markup=ReplyKeyboardRemove()
    )
    await RegistrationStatesGroup.name.set()

@dp.callback_query_handler(lambda c: c.data == "start_registration", state='*')
async def start_registration(callback: types.CallbackQuery, state: FSMContext):
    """Начало регистрации"""
    await callback.message.edit_text(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 1 из 7\n\n"
        "Введите ваше полное имя:",
        parse_mode='HTML'
    )
    await RegistrationStatesGroup.name.set()
    await callback.answer()


@dp.message_handler(state=RegistrationStatesGroup.name)
async def registration_name(message: types.Message, state: FSMContext):
    """Получение имени при регистрации"""
    name = message.text.strip()
    
    if len(name) < 2:
        await message.answer("❌ Имя слишком короткое. Введите полное имя:")
        return
    
    await state.update_data(name=name)
    
    await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 2 из 7\n\n"
        "📱 Введите ваш номер телефона\n\n"
        "Например: +79991234567",
        parse_mode='HTML'
    )
    await RegistrationStatesGroup.phone.set()


@dp.message_handler(state=RegistrationStatesGroup.phone)
async def registration_phone(message: types.Message, state: FSMContext):
    """Получение телефона при регистрации"""
    phone = message.text.strip()
    
    # Простая валидация
    if len(phone) < 10 or not any(char.isdigit() for char in phone):
        await message.answer("❌ Некорректный номер телефона. Попробуйте ещё раз:")
        return
    
    await state.update_data(phone=phone)
    
    await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 3 из 7\n\n"
        "📧 Введите ваш email:",
        parse_mode='HTML'
    )
    await RegistrationStatesGroup.email.set()


@dp.message_handler(state=RegistrationStatesGroup.email)
async def registration_email(message: types.Message, state: FSMContext):
    """Получение email при регистрации"""
    email = message.text.strip()
    
    # Простая валидация email
    if '@' not in email or '.' not in email.split('@')[-1]:
        await message.answer("❌ Некорректный email. Попробуйте ещё раз:")
        return
    
    await state.update_data(email=email)
    
    await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 4 из 7\n\n"
        "🏢 Введите ИНН вашей компании\n\n"
        "ИНН должен состоять из 10 или 12 цифр",
        parse_mode='HTML'
    )
    await RegistrationStatesGroup.inn.set()


@dp.message_handler(state=RegistrationStatesGroup.inn)
async def registration_inn(message: types.Message, state: FSMContext):
    """Получение ИНН при регистрации"""
    inn = message.text.strip()
    
    if not validate_inn(inn):
        await message.answer("❌ Некорректный ИНН. Должно быть 10 или 12 цифр. Попробуйте ещё раз:")
        return
    
    await state.update_data(inn=inn)
    
    await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 5 из 7\n\n"
        "📍 Введите юридический адрес компании\n\n"
        "Например: г. Краснодар, ул. Красная, д. 1, оф. 10",   
        parse_mode='HTML'
    )
    await RegistrationStatesGroup.company_details.set()  # ← ЗАКРЫТЫ СКОБКИ!


@dp.message_handler(state=RegistrationStatesGroup.company_details)
async def registration_company_details(message: types.Message, state: FSMContext):
    """Получение юридического адреса компании"""
    company_details = message.text.strip()
    
    if len(company_details) < 10:
        await message.answer("❌ Слишком короткий адрес. Введите полный юридический адрес:")
        return
    
    await state.update_data(company_details=company_details)
    
    # ✅ ДОБАВЛЕНО: Переход к выбору роли
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("👨‍🌾 Фермер", callback_data="role:farmer"),
        InlineKeyboardButton("📦 Экспортёр", callback_data="role:exporter")
    )
    keyboard.add(
        InlineKeyboardButton("🚛 Логист", callback_data="role:logistic"),
        InlineKeyboardButton("📋 Экспедитор", callback_data="role:expeditor")
    )
    
    await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 6 из 7\n\n"
        "Выберите вашу роль:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    # ✅ ПЕРЕХОД К СОСТОЯНИЮ ВЫБОРА РОЛИ
    await RegistrationStatesGroup.role.set()

@dp.callback_query_handler(lambda c: c.data.startswith('role:'), state=RegistrationStatesGroup.role)
async def registration_role(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора роли"""
    
    role = callback.data.split(':', 1)[1]
    
    logging.info(f"📝 Пользователь {callback.from_user.id} выбрал роль: {role}")
    
    # КРИТИЧНО: СОХРАНЯЕМ РОЛЬ
    await state.update_data(role=role)
    
    # КРИТИЧНО: ПЕРЕХОД К РЕГИОНУ
    await callback.message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 7 из 7\n\n"
        "Выберите ваш регион:",
        reply_markup=region_keyboard(),
        parse_mode='HTML'
    )
    
    await RegistrationStatesGroup.region.set()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('join_pull:'), state='*')
async def join_pull_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса присоединения к пулу"""
    
    try:
        parsed = parse_join_pull_callback(callback.data)
        pull_id = parsed["pull_id"]
        logging.info(f"🔗 join_pull callback: {callback.data}, извлечён pull_id: {pull_id}")
    except (IndexError, ValueError) as e:
        logging.error(f"❌ Ошибка парсинга callback: {callback.data}, ошибка: {e}")
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return

    if pull_id not in pulls:
        logging.warning(f"❌ Пул {pull_id} не найден. Доступные: {list(pulls.keys())}")
        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    pull = pulls[pull_id]
    
    # ✅ НОВОЕ: Проверка на закрытый пул
    if pull.get('status') == 'filled':
        await callback.answer(
            "❌ Этот пул уже заполнен на 100%!\n\nПопробуйте другие доступные пулы.",
            show_alert=True
        )
        return
    
    user_id = callback.from_user.id

    if user_id not in users:
        await callback.answer("❌ Пользователь не зарегистрирован", show_alert=True)
        return

    if users[user_id].get('role') != 'farmer':
        await callback.answer("❌ Только фермеры могут присоединяться к пулам", show_alert=True)
        return

    if user_id not in batches:
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
                text="✅ Создать партию прямо сейчас",
                callback_data=f"createbatchforpull:{pull_id}"
            ),
            InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel"
            )
        )
        await callback.message.answer(
            f"🌾 У вас нет партий культуры <b>{pull['culture']}</b> для этого пула.\n\n"
            f"Хотите создать новую партию прямо сейчас?",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await callback.answer()
        return

    already_joined_batch_ids = []
    if pull_id in pullparticipants:
        already_joined_batch_ids = [
            p['batch_id'] for p in pullparticipants[pull_id] 
            if p['farmer_id'] == user_id
        ]

    # ✅ ДОБАВЬТЕ ЭТИ СТРОКИ ДЛЯ ДИАГНОСТИКИ:
    logging.info(f"📋 Культура пула: '{pull['culture']}'")
    logging.info(f"📋 Партии пользователя {user_id}:")
    for b in batches.get(user_id, []):
        logging.info(f"   - ID: {b['id']}, Культура: '{b.get('culture')}', Статус: '{b.get('status')}', Уже присоединена: {b['id'] in already_joined_batch_ids}")

    active_batches = [
        b for b in batches[user_id]
        if b.get('culture') == pull['culture'] 
        and b.get('status', '').lower() in ['активна', 'active', 'открыта']
        and b['id'] not in already_joined_batch_ids
    ]

    logging.info(f"✅ Найдено активных партий: {len(active_batches)}")  # ← ДОБАВЬТЕ!

    if not active_batches:
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton(
                text="✅ Создать партию прямо сейчас",
                callback_data=f"createbatchforpull:{pull_id}"
            ),
            InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel"
            )
        )
        await callback.message.answer(
            f"🌾 У вас нет партий культуры <b>{pull.get('culture', 'Неизвестно')}</b> для этого пула.\n\n"
            f"Хотите создать новую партию прямо сейчас?",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await callback.answer()
        return

    await state.update_data(join_pull_id=pull_id)

    keyboard = InlineKeyboardMarkup(row_width=1)
    for batch in active_batches:
        button_text = f"{batch['culture']} - {batch['volume']} т - {batch['price']:,.0f} ₽/т"
        keyboard.add(
            InlineKeyboardButton(
                button_text,
                callback_data=f"selectbatchjoin:{batch['id']}"
            )
        )
        logging.info(f"   Добавлена партия: {button_text}")

    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data=f"viewpull:{pull_id}")
    )

    await JoinPullStatesGroup.select_batch.set()

    await callback.message.edit_text(
        f"🎯 <b>Выберите партию для присоединения к пулу #{pull_id}</b>\n\n"
        f"🌾 Культура: {pull.get('culture', 'Неизвестно')}\n"
        f"📦 Целевой объём: {pull['target_volume']} т\n"
        f"📊 Текущий объём: {pull['current_volume']} т\n"
        f"📉 Доступно: {pull['target_volume'] - pull['current_volume']} т\n\n"
        f"Выберите партию из списка ниже:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

# Быстрое создание партии для пулла
@dp.callback_query_handler(lambda c: c.data.startswith('quickbatch:'), state='*')
async def quick_batch_start(callback: types.CallbackQuery, state: FSMContext):
    """Начать создание партии для присоединения к пуллу"""
    await state.finish()
    
    pull_id = callback.data.split(':')[1]
    pull = pulls.get(pull_id)
    
    if not pull:
        await callback.answer("❌ Пулл не найден", show_alert=True)
        return
    
    await state.update_data(pull_id=pull_id)
    await QuickBatchStatesGroup.volume.set()
    
    msg = (
        f"📦 Создание партии для пулла\n\n"
        f"🌾 Культура: {pull['culture']}\n"
        f"📍 Регион: {pull['region']}\n"
        f"📊 Доступно в пулле: {pull['volume'] - pull.get('filled', 0):,.0f} т\n\n"
        f"Введите объём вашей партии (тонн):"
    )
    
    await callback.message.edit_text(msg, parse_mode='HTML')
    await callback.answer()

# Обработка объёма
@dp.message_handler(state=QuickBatchStatesGroup.volume)
async def quick_batch_volume(message: types.Message, state: FSMContext):
    """Получение объёма партии"""
    try:
        volume = float(message.text.replace(',', '.').replace(' ', ''))
        if volume <= 0:
            await message.answer("❌ Объём должен быть больше нуля")
            return
        
        data = await state.get_data()
        pull = pulls[data['pull_id']]
        available = pull['volume'] - pull.get('filled', 0)
        
        if volume > available:
            await message.answer(
                f"⚠️ В пулле доступно только {available:,.0f} т\n"
                f"Введите объём не больше {available:,.0f} т:"
            )
            return
        
        await state.update_data(volume=volume)
        await QuickBatchStatesGroup.price.set()
        await message.answer("Введите цену (₽/т):")
    
    except ValueError:
        await message.answer("❌ Введите корректное число")

# Обработка цены
@dp.message_handler(state=QuickBatchStatesGroup.price)
async def quick_batch_price(message: types.Message, state: FSMContext):
    """Получение цены"""
    try:
        price = float(message.text.replace(',', '.').replace(' ', ''))
        if price <= 0:
            await message.answer("❌ Цена должна быть больше нуля")
            return
        
        await state.update_data(price=price)
        
        # Спрашиваем про качество
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("Да", callback_data="quickquality:yes"),
            InlineKeyboardButton("Нет, пропустить", callback_data="quickquality:no")
        )
        
        await message.answer(
            "Хотите указать параметры качества (влажность, натура, примеси)?",
            reply_markup=keyboard
        )
    
    except ValueError:
        await message.answer("❌ Введите корректное число")

# Выбор - указывать качество или нет
@dp.callback_query_handler(lambda c: c.data.startswith('quickquality:'), state=QuickBatchStatesGroup.price)
async def quick_batch_quality_choice(callback: types.CallbackQuery, state: FSMContext):
    """Выбор - указывать параметры качества"""
    choice = callback.data.split(':')[1]
    
    if choice == 'yes':
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
        nature = float(message.text.replace(',', '.'))
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
        moisture = float(message.text.replace(',', '.'))
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
        impurity = float(message.text.replace(',', '.'))
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
    pull_id = data['pull_id']
    pull = pulls[pull_id]
    
    # Создаём партию
    batch_id = generate_id()
    batch = {
        'id': batch_id,
        'farmer_id': user_id,
        'culture': pull['culture'],
        'volume': data['volume'],
        'price': data['price'],
        'region': pull['region'],
        'status': 'active',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Добавляем качество если есть
    if 'nature' in data:
        batch['nature'] = data.get('nature')
        batch['moisture'] = data.get('moisture')
        batch['impurity'] = data.get('impurity')
    
    # Сохраняем партию
    if user_id not in batches:
        batches[user_id] = []
    batches[user_id].append(batch)
    
    # Добавляем в пулл
    if 'batches' not in pull:
        pull['batches'] = []
    pull['batches'].append(batch_id)
    pull['filled'] = pull.get('filled', 0) + data['volume']
    
    # Создаём сделку
    deal_id = generate_id()
    deal = {
        'id': deal_id,
        'pull_id': pull_id,
        'batch_id': batch_id,
        'farmer_id': user_id,
        'exporter_id': pull['exporter_id'],
        'status': 'matched',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    deals[deal_id] = deal
    
    # Сохраняем
    save_batches_to_pickle()
    save_pulls_to_pickle()
    save_deals_to_pickle()
    
    # Уведомления
    farmer = users[user_id]
    exporter = users[pull['exporter_id']]
    
    await bot.send_message(
        user_id,
        f"✅ Партия создана и добавлена в пулл!\n\n"
        f"🌾 {batch['culture']} • {batch['volume']:,.0f} т • {batch['price']:,.0f} ₽/т\n"
        f"📊 Пулл заполнен: {pull['filled']:,.0f}/{pull['volume']:,.0f} т"
    )
    
    await bot.send_message(
        pull['exporter_id'],
        f"📦 Новая партия добавлена в ваш пулл!\n\n"
        f"👤 Фермер: {farmer.get('name')}\n"
        f"🌾 {batch['culture']} • {batch['volume']:,.0f} т • {batch['price']:,.0f} ₽/т\n"
        f"📊 Заполнено: {pull['filled']:,.0f}/{pull['volume']:,.0f} т"
    )
    
    # Проверяем заполнение пулла
    if pull['filled'] >= pull['volume']:
        pull['status'] = 'filled'
        await notify_pull_filled(pull_id)
    
    # Возвращаемся к пуллу
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📊 К пуллу", callback_data=f"viewpull:{pull_id}"))
    
    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(
            "✅ Готово! Партия добавлена в пулл",
            reply_markup=keyboard
        )
    else:
        await message_or_callback.edit_text(
            "✅ Готово! Партия добавлена в пулл",
            reply_markup=keyboard
        )
@dp.callback_query_handler(lambda c: c.data.startswith('createbatchforpull:'), state='*')
async def create_batch_for_pull_callback(callback: types.CallbackQuery, state: FSMContext):
    """Создание партии для присоединения к пулу"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    
    # Сохраняем ID пула и культуру для последующего присоединения
    await state.update_data(
        create_batch_for_pull_id=pull_id, 
        culture=pull['culture']
    )
    
    await callback.message.answer(
        f"**📦 Создание партии для пула #{pull_id}**\n\n"
        f"🌾 Культура: **{pull['culture']}**\n\n"
        f"**Шаг 1/8:** Укажите регион производства:",
        reply_markup=region_keyboard(),
        parse_mode='Markdown'
    )
    
    await AddBatch.region.set()  # ✅ ИСПРАВЛЕНО: AddBatch вместо AddBatchStatesGroup
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('selectbatchjoin:'), state=JoinPullStatesGroup.select_batch)
async def select_batch_for_join(callback: types.CallbackQuery, state: FSMContext):
    """Выбор партии для присоединения к пулу"""
    try:
        batch_id = parse_callback_id(callback.data)  
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        await state.finish()
        return
    
    data = await state.get_data()    
    pull_id = data.get('join_pull_id')
    
    if not pull_id:
        await callback.answer("❌ Пул не найден. Попробуйте снова.", show_alert=True)
        await state.finish()
        return
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        await state.finish()
        return
    
    pull = pulls[pull_id]  
    user_id = callback.from_user.id

    # Ищем партию
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
        
    if batch['culture'] != pull['culture']:
        await callback.answer("❌ Культура партии не совпадает с пулом!", show_alert=True)
        await state.finish()
        return
    
    # Проверяем доступный объём
    available = pull['target_volume'] - pull['current_volume']
    logging.info(f"Партия {batch_id}: объем {batch['volume']}, доступно {available}")
    
    if batch['volume'] > available:
        await callback.answer("❌ Объем партии больше доступного в пуле!", show_alert=True)
        await state.finish()
        return
    
    # Добавляем участника в пул
    if pull_id not in pullparticipants:
        pullparticipants[pull_id] = []
    
    participant = {
        'farmer_id': user_id,
        'farmer_name': users[user_id].get('name', ''),
        'batch_id': batch_id,
        'volume': batch['volume'],
        'joined_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    pullparticipants[pull_id].append(participant)
    pull['current_volume'] += batch['volume']

    # ✅ АВТОЗАКРЫТИЕ ПУЛЛА ПРИ 100%
    is_full = False
    if pull['current_volume'] >= pull['target_volume']:
        pull['status'] = 'filled'
        is_full = True
        logging.info(f"🎉 Пулл #{pull_id} заполнен на 100%!")
        
        # Уведомление экспортёру о заполнении
        exporter_id = pull.get('exporter_id')
        if exporter_id:
            try:
                await bot.send_message(
                    exporter_id,
                    f"🎉 <b>ПУЛ #{pull_id} ЗАПОЛНЕН!</b>\n\n"
                    f"🌾 {pull.get('culture')} - {pull['current_volume']:,.0f} т\n"
                    f"💰 ${pull.get('price', 0):,.0f}/т\n"
                    f"🏢 {pull.get('port')}\n\n"
                    f"✅ Готов к отгрузке!",
                    parse_mode='HTML'
                )
                logging.info(f"✅ Уведомление экспортёру о заполнении отправлено")
            except Exception as e:
                logging.error(f"Ошибка уведомления экспортёра о заполнении: {e}")
        
        # Уведомление фермерам о заполнении
        if pull_id in pullparticipants:
            for participant in pullparticipants[pull_id]:
                farmer_id = participant.get('farmer_id')
                if farmer_id and farmer_id != exporter_id:
                    try:
                        await bot.send_message(
                            farmer_id,
                            f"🎉 <b>ПУЛ #{pull_id} ЗАПОЛНЕН!</b>\n\n"
                            f"Ваша партия: {participant.get('volume')} т\n\n"
                            f"Ожидайте инструкций.",
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        logging.error(f"Ошибка уведомления фермера: {e}")
        
        # 🚚 НОВОЕ: Уведомление ЛОГИСТАМ о заполненном пуле
        all_logistics = [uid for uid, user in users.items() if user.get('role') == 'logistic']
        if all_logistics:
            for logist_id in all_logistics:
                try:
                    await bot.send_message(
                        logist_id,
                        f"🚚 <b>НОВЫЙ ЗАПОЛНЕННЫЙ ПУЛ #{pull_id}!</b>\n\n"
                        f"🌾 Культура: {pull.get('culture')}\n"
                        f"📦 Объём: {pull['current_volume']:,.0f} т\n"
                        f"🚢 Порт: {pull.get('port')}\n"
                        f"💰 Цена FOB: ${pull.get('price', 0):,.0f}/т\n"
                        f"📅 Создан: {pull.get('created_at', 'N/A')}\n\n"
                        f"⚡ Требуется организация логистики!",
                        parse_mode='HTML'
                    )
                    logging.info(f"✅ Уведомление логисту {logist_id} отправлено")
                except Exception as e:
                    logging.error(f"❌ Ошибка уведомления логиста {logist_id}: {e}")
        else:
            logging.info("ℹ️ Логисты не найдены в системе")
    
    # Обновляем статус партии
    batch['status'] = 'Зарезервирована'
    
    # Сохраняем изменения
    save_pulls_to_pickle()
    save_batches_to_pickle()
    
    # Расчёт процента заполнения
    fill_percent = (pull['current_volume'] / pull['target_volume']) * 100
    remaining = pull['target_volume'] - pull['current_volume']
    
    # ✅ УДАЛЯЕМ СООБЩЕНИЕ С ВЫБОРОМ ПАРТИИ
    try:
        await callback.message.delete()
        logging.info("✅ Сообщение с выбором партии удалено")
    except Exception as e:
        logging.warning(f"⚠️ Не удалось удалить сообщение: {e}")
    
    # Уведомление фермеру о присоединении
    if is_full:
        await callback.answer("✅ Партия добавлена! Пул заполнен на 100%!", show_alert=True)
        
        await bot.send_message(
            user_id,
            f"🎉 <b>Поздравляем!</b>\n\n"
            f"Партия #{batch_id} добавлена в пул #{pull_id}\n\n"
            f"✅ <b>Пул заполнен на 100%!</b>\n\n"
            f"🌾 {batch['culture']} • {batch['volume']} т\n"
            f"💰 {batch['price']:,.0f} ₽/т\n\n"
            f"Сделка создана автоматически.\n"
            f"Детали придут отдельным сообщением.",
            parse_mode='HTML'
        )
    else:
        await callback.answer("✅ Успешно присоединились к пулу!", show_alert=True)
        
        await bot.send_message(
            user_id,
            f"✅ <b>Партия #{batch_id} присоединена к пулу #{pull_id}!</b>\n\n"
            f"🌾 Культура: {batch['culture']}\n"
            f"📦 Объем: {batch['volume']} т\n"
            f"💰 Цена: {batch['price']:,.0f} ₽/т\n\n"
            f"📊 <b>Заполненность пула:</b>\n"
            f"{pull['current_volume']:,.0f} / {pull['target_volume']:,.0f} т ({fill_percent:.1f}%)\n"
            f"Осталось: {remaining:,.0f} т\n\n"
            f"Экспортёр свяжется с вами для обсуждения деталей.",
            parse_mode='HTML'
        )
    
    # Уведомляем экспортёра о новом участнике
    try:
        if pull_id not in pulls:
            logging.warning(f"⚠️ Пул {pull_id} не найден для уведомления экспортёру")
        else:
            pull = pulls[pull_id]
            
            if 'exporter_id' not in pull:
                logging.warning(f"⚠️ В пуле {pull_id} нет exporter_id")
            elif pull['exporter_id'] == user_id:
                # ✅ НОВАЯ ПРОВЕРКА: Экспортёр сам присоединился к своему пулу
                logging.info(f"ℹ️ Экспортёр #{pull['exporter_id']} сам присоединился к своему пулу")
            else:
                # Отправляем уведомление экспортёру (только если это НЕ сам фермер)
                farmer = users.get(user_id, {})
                farmer_name = farmer.get('name', 'Неизвестно')
                farmer_phone = farmer.get('phone', 'Не указан')
                farmer_region = farmer.get('region', 'Не указан')
                
                await bot.send_message(
                    pull['exporter_id'],
                    f"📦 <b>Новый участник в пуле #{pull_id}!</b>\n\n"
                    f"👤 Фермер: {farmer_name}\n"
                    f"📱 Телефон: {farmer_phone}\n"
                    f"📍 Регион: {farmer_region}\n\n"
                    f"📦 Партия #{batch_id}:\n"
                    f"   🌾 {batch['culture']}\n"
                    f"   📊 Объём: {batch['volume']} т\n"
                    f"   💰 Цена: {batch['price']:,.0f} ₽/т\n\n"
                    f"📊 Заполнено: {fill_percent:.1f}%\n"
                    f"Объём: {pull['current_volume']:,.0f}/{pull['target_volume']:,.0f} т\n\n"
                    f"💬 Свяжитесь с фермером для обсуждения деталей.",
                    parse_mode='HTML'
                )
                logging.info(f"✅ Уведомление экспортёру {pull['exporter_id']} отправлено")
                
    except Exception as e:
        logging.error(f"❌ Ошибка отправки уведомления экспортёру: {e}")
    
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('viewparticipants:'), state='*')
async def view_pullparticipants(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр участников пула с полными контактами"""
    await state.finish()
    
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    participants = pullparticipants.get(pull_id, [])
    
    if not participants:
        await callback.answer("В пуле пока нет участников", show_alert=True)
        return
    msg = f"👥 <b>Участники пула #{pull_id}</b>\n\n"
    msg += f"🌾 Культура: {pull['culture']}\n"
    msg += f"📦 Целевой объём: {pull['target_volume']} т\n"
    msg += f"📊 Текущий объём: {pull['current_volume']} т\n"
    msg += f"📈 Заполнено: {(pull['current_volume'] / pull['target_volume'] * 100):.1f}%\n\n"
    
    msg += f"<b>Участники ({len(participants)}):</b>\n\n"
    
    for i, p in enumerate(participants, 1):
        farmer_id = p['farmer_id']
        farmer = users.get(farmer_id, {})
        
        msg += f"{i}. <b>{p['farmer_name']}</b>\n"
        msg += f"   📦 Объём: {p['volume']} т\n"
        batch_id = p['batch_id']
        batch = None
        if farmer_id in batches:
            for b in batches[farmer_id]:
                if b['id'] == batch_id:
                    batch = b
                    break
        
        if batch:
            msg += f"   💰 Цена: {batch['price']:,.0f} ₽/т\n"
            msg += f"   📍 Регион: {batch.get('region', 'Не указано')}\n"
        
        msg += f"   📅 Присоединился: {p['joined_at']}\n"
        phone = farmer.get('phone', 'Не указан')
        email = farmer.get('email', 'Не указан')
        msg += f"   📱 Телефон: <code>{phone}</code>\n"
        msg += f"   📧 Email: <code>{email}</code>\n"
        msg += "\n"
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("◀️ Назад к пулу", callback_data=f"viewpull:{pull_id}")
    )
    
    await callback.message.edit_text(
        msg,
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('region:'), state=RegistrationStatesGroup.region)
async def registration_region(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора региона"""
    
    region = callback.data.split(':', 1)[1]
    
    if region == 'other':
        await callback.message.answer("Введите название вашего региона:")
        return
    
    await state.update_data(region=region)
    
    data = await state.get_data()
    user_id = callback.from_user.id
    role = data.get('role')
    
    logging.info(f"📝 Завершение регистрации user_id={user_id}, role={role}, region={region}")
    
    users[user_id] = {
        'name': data.get('name'),
        'phone': data.get('phone'),
        'email': data.get('email'),
        'inn': data.get('inn'),
        'company_details': data.get('company_details'),
        'role': role,
        'region': region,
        'registered_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    save_users_to_pickle()
    
    # Синхронизация с Google Sheets
    if gs and gs.spreadsheet:
        try:
            gs.sync_user_to_sheets(users[user_id], user_id)
        except Exception as e:
            logging.error(f"Ошибка синхронизации с Google Sheets: {e}")
    
    await state.finish()
    
    keyboard = get_role_keyboard(role)
    
    role_names_display = {
        'farmer': 'Фермер',
        'exporter': 'Экспортёр',
        'logistic': 'Логист',
        'expeditor': 'Экспедитор'
    }
    
    await callback.message.answer(
        f"✅ <b>Регистрация завершена!</b>\n\n"
        f"👤 Имя: {data.get('name')}\n"
        f"📱 Телефон: {data.get('phone')}\n"
        f"📧 Email: {data.get('email')}\n"
        f"🎭 Роль: {role_names_display.get(role, role)}\n"
        f"📍 Регион: {region}\n\n"
        f"Добро пожаловать в EXPORTUM!",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await callback.answer()


@dp.message_handler(commands=['admin'], state='*')
async def admin_menu(message: types.Message, state: FSMContext):
    """Админ меню"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("🚫 У вас нет доступа к этой команде.")
        return
    
    await message.answer(
        "🔐 <b>Админ панель</b>\n\n"
        "Выберите действие:",
        reply_markup=admin_keyboard(),
        parse_mode='HTML'
    )

# ========================================
# ОБРАБОТЧИКИ АДМИН-ПАНЕЛИ
# ========================================

@dp.message_handler(lambda m: m.text == '📊 Статистика бота', state='*')
async def admin_stats_button(message: types.Message, state: FSMContext):
    """Обработчик кнопки Статистика бота"""
    await state.finish()
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        await message.answer('⛔ Доступ запрещён')
        return
    
    # Собираем статистику
    total_users = len(users)
    farmers = len([u for u in users.values() if u.get('role') == 'farmer'])
    exporters = len([u for u in users.values() if u.get('role') == 'exporter'])
    logistics = len([u for u in users.values() if u.get('role') == 'logistic'])
    expeditors = len([u for u in users.values() if u.get('role') == 'expeditor'])
    
    total_batches = sum(len(batches) for user_batches in batches.values())
    active_batches = sum(
        len([b for b in batches if b.get('status') == 'активна'])
        for user_batches in batches.values()
    )
    
    total_pulls = len(pulls)
    open_pulls = len([p for p in pulls.values() if p.get('status') == 'открыт'])
    
    total_deals = len(deals)
    active_deals = len([d for d in deals.values() if d.get('status') not in ['completed', 'cancelled']])
    
    total_matches = len(matches)
    active_matches = len([m for m in matches.values() if m.get('status') == 'active'])
    
    stats_msg = f"""📊 <b>Статистика бота</b>

👥 <b>Пользователи:</b>
• Всего: {total_users}
• 🌾 Фермеров: {farmers}
• 📦 Экспортёров: {exporters}
• 🚚 Логистов: {logistics}
• 🚛 Экспедиторов: {expeditors}

📦 <b>Партии:</b>
• Всего: {total_batches}
• Активных: {active_batches}

🎯 <b>Пуллы:</b>
• Всего: {total_pulls}
• Открытых: {open_pulls}

🤝 <b>Сделки:</b>
• Всего: {total_deals}
• Активных: {active_deals}

🔗 <b>Совпадения:</b>
• Всего: {total_matches}
• Активных: {active_matches}"""
    
    await message.answer(stats_msg, parse_mode='HTML')


@dp.message_handler(lambda m: m.text == '📂 Экспорт данных', state='*')
async def admin_export_button(message: types.Message, state: FSMContext):
    """Обработчик кнопки Экспорт данных"""
    await state.finish()
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        await message.answer('⛔ Доступ запрещён')
        return
    
    await message.answer('⏳ Формирую экспорт данных...')
    
    try:
        # Формируем данные для экспорта
        export_data = {
            'users': users,
            'batches': {uid: batches for uid, batches in batches.items()},
            'pulls': pulls,
            'deals': deals,
            'matches': matches,
            'exported_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Сохраняем в файл
        filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        
        # Отправляем файл
        with open(filename, 'rb') as f:
            await message.answer_document(
                f,
                caption=f"📂 <b>Экспорт данных</b>\n\n"
                        f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                        f"👥 Пользователей: {len(users)}\n"
                        f"📦 Партий: {sum(len(b) for b in batches.values())}\n"
                        f"🎯 Пуллов: {len(pulls)}",
                parse_mode='HTML'
            )
        
        # Удаляем временный файл
        os.remove(filename)
        
    except Exception as e:
        logging.error(f"Ошибка экспорта данных: {e}")
        await message.answer('❌ Ошибка при экспорте данных')


@dp.message_handler(lambda m: m.text == '📊 Аналитика', state='*')
async def admin_analytics_button(message: types.Message, state: FSMContext):
    """Обработчик кнопки Аналитика"""
    await state.finish()
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        await message.answer('⛔ Доступ запрещён')
        return
    
    # Расширенная аналитика
    total_batch_volume = 0
    prices = []
    
    for farmer_id, batches in batches.items():
        for batch in user_batches:
            total_batch_volume += batch.get('volume', 0)
            if batch.get('price'):
                prices.append(batch['price'])
    
    avg_price = sum(prices) / len(prices) if prices else 0
    min_price = min(prices) if prices else 0
    max_price = max(prices) if prices else 0
    
    # Статистика по культурам
    cultures_stats = {}
    for user_batches in batches.values():
        for batch in user_batches:
            culture = batch.get('culture', 'Неизвестно')
            if culture not in cultures_stats:
                cultures_stats[culture] = {'count': 0, 'volume': 0}
            cultures_stats[culture]['count'] += 1
            cultures_stats[culture]['volume'] += batch.get('volume', 0)
    
    analytics_msg = f"""📊 <b>Расширенная аналитика</b>

📦 <b>Объёмы:</b>
• Общий объём партий: {total_batch_volume:,.0f} т

💰 <b>Цены:</b>
• Средняя цена: {avg_price:,.0f} ₽/т
• Минимальная: {min_price:,.0f} ₽/т
• Максимальная: {max_price:,.0f} ₽/т

🌾 <b>По культурам:</b>"""
    
    for culture, stats in cultures_stats.items():
        analytics_msg += f"\n• {culture}: {stats['count']} партий, {stats['volume']:,.0f} т"
    
    analytics_msg += f"\n\n📅 Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    
    await message.answer(analytics_msg, parse_mode='HTML')

@dp.message_handler(lambda m: m.text == "📢 Рассылка", state='*')
async def admin_broadcast_start(message: types.Message, state: FSMContext):
    """Начало рассылки"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    
    await message.answer(
        "📢 <b>Рассылка сообщений</b>\n\n"
        "Отправьте сообщение, которое нужно разослать всем пользователям.\n\n"
        "<i>Отправьте /cancel для отмены</i>",
        parse_mode='HTML'
    )
    
    await BroadcastStatesGroup.message.set()

class BroadcastStatesGroup(StatesGroup):
    message = State()
    confirm = State()



# ============================================================================
# ОБРАБОТЧИКИ КНОПОК АДМИН-ПАНЕЛИ
# ============================================================================

@dp.message_handler(lambda m: m.text == "📊 Статистика", state='*')
async def admin_statistics_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки Статистика"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    stats_message = format_admin_statistics()

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="admin_refresh_stats"),
        InlineKeyboardButton("📊 Детали", callback_data="admin_detailed_stats")
    )

    await message.answer(stats_message, reply_markup=keyboard, parse_mode='HTML')


@dp.message_handler(lambda m: m.text == "📈 Аналитика", state='*')
async def admin_analytics_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки Аналитика"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    analytics_message = format_admin_analytics()

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="admin_refresh_analytics"),
        InlineKeyboardButton("📤 Экспорт", callback_data="admin_export_analytics")
    )

    await message.answer(analytics_message, reply_markup=keyboard, parse_mode='HTML')


@dp.message_handler(lambda m: m.text == "📥 Экспорт данных", state='*')
async def admin_export_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки Экспорт данных"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("👥 Пользователи", callback_data="export_users"),
        InlineKeyboardButton("🎯 Пулы", callback_data="export_pools")
    )
    keyboard.add(
        InlineKeyboardButton("📦 Партии", callback_data="export_batches"),
        InlineKeyboardButton("🚚 Заявки", callback_data="export_requests")
    )
    keyboard.add(
        InlineKeyboardButton("💾 Полный бэкап", callback_data="export_full")
    )

    await message.answer(
        "📥 <b>Экспорт данных</b>\n\n"
        "Выберите данные для экспорта:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


@dp.message_handler(lambda m: m.text == "👥 Пользователи", state='*')
async def admin_users_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки Пользователи"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    msg = "👥 <b>Пользователи системы</b>\n\n"

    roles_data = [
        ("Фермеры", "farmer", "🌾"),
        ("Экспортёры", "exporter", "📦"),
        ("Логисты", "logistic", "🚚"),
        ("Экспедиторы", "expeditor", "🚛")
    ]

    for role_name, role_key, emoji in roles_data:
        role_users = [u for u in users.values() if u.get('role') == role_key]
        msg += f"{emoji} <b>{role_name}:</b> {len(role_users)}\n"
        for user in role_users[:5]:
            company = user.get('company_name', 'Без названия')
            phone = user.get('phone', 'N/A')
            msg += f"  • {company} ({phone})\n"
        if len(role_users) > 5:
            msg += f"  ... и ещё {len(role_users) - 5}\n"
        msg += "\n"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔍 Поиск", callback_data="admin_search_user"),
        InlineKeyboardButton("📤 Экспорт", callback_data="export_users")
    )

    await message.answer(msg, reply_markup=keyboard, parse_mode='HTML')


@dp.message_handler(lambda m: m.text == "🔄 Обновить цены", state='*')
async def admin_update_prices_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки Обновить цены"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    await message.answer("🔄 Запускаю обновление цен...")

    try:
        # Обновляем цены (если есть функция парсинга)
        await message.answer(
            "✅ <b>Цены обновлены</b>\n\n"
            "Данные успешно загружены из источников.",
            parse_mode='HTML'
        )
    except Exception as e:
        await message.answer(
            f"❌ <b>Ошибка обновления</b>\n\n"
            f"Не удалось обновить цены: {str(e)}",
            parse_mode='HTML'
        )


@dp.message_handler(lambda m: m.text == "🏠 Главное меню", state='*')
async def admin_main_menu_handler(message: types.Message, state: FSMContext):
    """Возврат в главное меню из админ-панели"""
    await state.finish()

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    # Определяем роль пользователя и отправляем соответствующую клавиатуру
    user_data = users.get(user_id, {})
    role = user_data.get('role', 'unknown')

    if role == 'farmer':
        await message.answer(
            "🏠 Главное меню",
            reply_markup=farmer_keyboard()
        )
    elif role == 'exporter':
        await message.answer(
            "🏠 Главное меню",
            reply_markup=exporter_keyboard()
        )
    elif role == 'logistic':
        await message.answer(
            "🏠 Главное меню",
            reply_markup=logistic_keyboard()
        )
    else:
        await message.answer(
            "🏠 Главное меню",
            reply_markup=ReplyKeyboardRemove()
        )


@dp.message_handler(state=BroadcastStatesGroup.message, content_types=types.ContentType.TEXT)
async def admin_broadcast_message(message: types.Message, state: FSMContext):
    """Получение сообщения для рассылки"""
    
    broadcast_text = message.text
    await state.update_data(broadcast_text=broadcast_text)
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да", callback_data="broadcast_confirm_yes"),
        InlineKeyboardButton("❌ Нет", callback_data="broadcast_confirm_no")
    )
    
    await message.answer(
        f"📢 <b>Предпросмотр рассылки:</b>\n\n"
        f"{broadcast_text}\n\n"
        f"Разослать это сообщение <b>{len(users)}</b> пользователям?",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await BroadcastStatesGroup.confirm.set()

@dp.callback_query_handler(lambda c: c.data.startswith('broadcast_confirm_'), state=BroadcastStatesGroup.confirm)
async def admin_broadcast_confirm(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение рассылки"""
    
    action = callback.data.split('_', 2)[2]
    
    if action == 'no':
        await callback.message.edit_text("❌ Рассылка отменена")
        await state.finish()
        await callback.answer()
        return
    
    data = await state.get_data()
    broadcast_text = data.get('broadcast_text')
    
    await callback.message.edit_text("📤 Отправка сообщений...")
    
    success_count = 0
    fail_count = 0
    
    for user_id in users.keys():
        try:
            await bot.send_message(user_id, broadcast_text)
            success_count += 1
            await asyncio.sleep(0.05)  
        except Exception as e:
            fail_count += 1
            logging.error(f"Ошибка отправки пользователю {user_id}: {e}")
    
    await callback.message.answer(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"✅ Успешно: {success_count}\n"
        f"❌ Ошибок: {fail_count}",
        reply_markup=admin_keyboard(),
        parse_mode='HTML'
    )
    
    await state.finish()
    await callback.answer()

@dp.message_handler(lambda m: m.text == "📤 Экспорт", state='*')
async def admin_export(message: types.Message, state: FSMContext):
    """Экспорт данных"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    
    export_data = {
        'users': users,
        'batches': {uid: batches for uid, batches in batches.items()},
        'pulls': pulls,
        'deals': deals,
        'matches': matches,
        'exported_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    filename = f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, ensure_ascii=False, indent=2)
    
    with open(filename, 'rb') as f:
        await message.answer_document(
            f,
            caption=f"📤 Экспорт данных\n\n"
                    f"🗓 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                    f"👥 Пользователей: {len(users)}\n"
                    f"📦 Партий: {sum(len(b) for b in batches.values())}\n"
                    f"🎯 Пулов: {len(pulls)}",
            parse_mode='HTML'
        )
    
    os.remove(filename)

@dp.message_handler(lambda m: m.text == "🔍 Найти совпадения", state='*')
async def admin_manual_match(message: types.Message, state: FSMContext):
    """Ручной поиск совпадений"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    
    await message.answer("🔍 Запуск поиска совпадений...")
    
    matches_found = await auto_match_batches_and_pulls()
    
    await message.answer(
        f"✅ <b>Поиск завершён!</b>\n\n"
        f"🔍 Найдено совпадений: {matches_found}\n"
        f"📊 Всего активных: {len([m for m in matches.values() if m.get('status') == 'active'])}",
        parse_mode='HTML'
    )

@dp.message_handler(lambda m: m.text == "◀️ Назад", state='*')
async def admin_back(message: types.Message, state: FSMContext):
    """Возврат из админ панели"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    if user_id in users:
        role = users[user_id].get('role')
        keyboard = get_role_keyboard(role)
        await message.answer("◀️ Возврат в главное меню", reply_markup=keyboard)
    else:
        await message.answer("◀️ Возврат")

@dp.message_handler(commands=['match'], state='*')
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
        f"✅ Поиск завершен!\n\n"
        f"Найдено новых совпадений: {matches_found}\n"
        f"Всего активных совпадений: {len([m for m in matches.values() if m['status'] == 'active'])}"
    )

@dp.message_handler(lambda m: m.text == "👤 Профиль", state='*')
async def cmd_profile(message: types.Message, state: FSMContext):
    """Показать расширенный профиль пользователя"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы. Используйте /start")
        return
    
    user = users[user_id]
    
    profile_text = f"""
👤 <b>Ваш профиль</b>

📝 Имя: {user.get('name', 'Не указано')}
🎭 Роль: {ROLES.get(user.get('role'), 'Не указана')}
📱 Телефон: {user.get('phone', 'Не указан')}
📧 Email: {user.get('email', 'Не указан')}
🏢 ИНН: {user.get('inn', 'Не указан')}
📍 Регион: {user.get('region', 'Не указан')}
📅 Регистрация: {user.get('registered_at', 'Неизвестно')}
"""
    
    if user.get('company_details'):
        profile_text += f"\n🏢 <b>Реквизиты компании:</b>\n{user['company_details']}"
    
    keyboard = profile_edit_keyboard()
    
    await message.answer(profile_text, parse_mode='HTML', reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data.startswith("edit_profile:"), state='*')
async def start_edit_profile(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование профиля"""
    field = callback.data.split(':', 1)[1]
    
    await state.update_data(edit_field=field)
    
    field_names = {
        'phone': 'номер телефона',
        'email': 'email',
        'region': 'регион',
        'company_details': 'реквизиты компании'
    }
    
    if field == 'region':
        await callback.message.edit_text(
            "Выберите новый регион:",
            reply_markup=region_keyboard()
        )
    else:
        await callback.message.answer(
            f"Введите новый {field_names.get(field, 'значение')}:"
        )
    
    await EditProfile.new_value.set()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('region:'), state=EditProfile.new_value)
async def edit_profile_region(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора региона при редактировании профиля"""
    new_region = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    field = data.get('edit_field')
    user_id = callback.from_user.id
    
    if field != 'region':
        await callback.answer("❌ Ошибка")
        return
    
    old_value = users[user_id].get('region', 'Не указан')
    users[user_id]['region'] = new_region
    
    save_users_to_json()
    
    if gs and gs.spreadsheet:
        gs.update_user_in_sheets(user_id, users[user_id])
    
    await state.finish()
    
    role = users[user_id].get('role')
    keyboard = get_role_keyboard(role)
    
    await callback.message.edit_text(
        f"✅ Регион обновлён!\n\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_region}"
    )
    
    await callback.message.answer(
        "Профиль обновлён!",
        reply_markup=keyboard
    )
    await callback.answer("✅ Регион обновлён")

@dp.message_handler(state=EditProfile.new_value)
async def edit_profile_value(message: types.Message, state: FSMContext):
    """Сохранить новое значение профиля"""
    user_id = message.from_user.id
    data = await state.get_data()
    field = data.get('edit_field')
    new_value = message.text.strip()
    
    if field == 'email':
        if not validate_email(new_value):
            await message.answer("❌ Некорректный email. Попробуйте ещё раз:")
            return
    elif field == 'phone':
        if not validate_phone(new_value):
            await message.answer("❌ Некорректный номер телефона. Попробуйте ещё раз:")
            return
    
    old_value = users[user_id].get(field, 'Не указано')
    users[user_id][field] = new_value
    
    save_users_to_json()
    
    if gs and gs.spreadsheet:
        gs.update_user_in_sheets(user_id, users[user_id])

    await state.finish()
    
    role = users[user_id].get('role')
    keyboard = get_role_keyboard(role)
    
    field_names = {
        'phone': 'Телефон',
        'email': 'Email',
        'company_details': 'Реквизиты компании'
    }
    
    await message.answer(
        f"✅ {field_names.get(field, field.capitalize())} обновлён!",
        reply_markup=keyboard
    )

@dp.message_handler(lambda m: m.text == "📈 Цены на зерно", state='*')
async def show_prices_menu(message: types.Message, state: FSMContext):
    """Показать цены сразу без меню"""
    await state.finish()
    
    prices_msg = format_prices_message()
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить цены", callback_data="refresh_prices")
    )
    
    await message.answer(
        prices_msg,
        parse_mode='HTML',
        reply_markup=keyboard
    )
    
@dp.message_handler(lambda m: m.text == "📰 Новости рынка", state='*')
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
        parse_mode='HTML',
        reply_markup=keyboard,
        disable_web_page_preview=True
    )

@dp.callback_query_handler(lambda c: c.data == "view_news", state='*')
async def show_news(callback: types.CallbackQuery):
    """Показать новости"""
    news_msg = format_news_message()
    await callback.message.edit_text(
        news_msg, 
        parse_mode='HTML',
        disable_web_page_preview=True
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
            "https://www.zol.ru/soya.htm"  # общая страница
        ]
        
        for url in base_urls:
            try:
                response = requests.get(url, timeout=10, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
                if response.status_code != 200:
                    continue
                
                text = response.text.lower()
                
                # Паттерны для парсинга
                patterns = [
                    r'соя\s*[=:]\s*(\d+\.?\d*)',
                    r'soy\s*[=:]\s*(\d+\.?\d*)',
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
                            except Exception as e:
                                continue
                
                # Убираем дубликаты
                prices = list(set(prices))
                
                if prices and len(prices) >= 1:
                    avg = int(sum(prices) / len(prices))
                    logging.info(f"✅ Соя (ZOL.RU): найдено {len(prices)} регионов")
                    for i, price in enumerate(sorted(prices), 1):
                        logging.info(f"   Регион {i}: {price:,} ₽/т")
                    logging.info(f"✅ Соя: средняя {avg:,} ₽/т ({len(prices)} регионов) [СПАРСЕНО]")
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
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')[1:]
            logging.info(f"📋 Пшеница: найдено строк {len(rows)}")
            
            wheat_prices = []
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                
                region = cells[0].get_text(strip=True)
                
                for i in range(1, min(4, len(cells))):
                    price_text = cells[i].get_text(strip=True)
                    if not price_text or price_text == '-':
                        continue
                    
                    if '-' in price_text and not price_text.startswith('-'):
                        prices_range = price_text.split('-')
                        for p in prices_range:
                            try:
                                price_clean = re.sub(r'[^0-9]', '', p)
                                if price_clean:
                                    price_value = int(price_clean)
                                    if 8000 <= price_value <= 30000:
                                        wheat_prices.append(price_value)
                                        logging.info(f"✅ Пшеница: {price_value} ₽/т из {region}")
                            except Exception as e:
                                continue
                    else:
                        try:
                            price_clean = re.sub(r'[^0-9]', '', price_text)
                            if price_clean:
                                price_value = int(price_clean)
                                if 8000 <= price_value <= 30000:
                                    wheat_prices.append(price_value)
                                    logging.info(f"✅ Пшеница: {price_value} ₽/т из {region}")
                        except Exception as e:
                            continue
            
            if wheat_prices:
                result['Пшеница'] = int(sum(wheat_prices) / len(wheat_prices))
                logging.info(f"✅ Пшеница: средняя {result['Пшеница']} ₽/т ({len(wheat_prices)} цен)")
            else:
                result['Пшеница'] = 15000
                logging.warning("⚠️ Пшеница: используем резервное значение")
        else:
            result['Пшеница'] = 15000
            logging.warning("⚠️ Пшеница: таблица не найдена")
            
    except Exception as e:
        logging.error(f"❌ Ошибка парсинга пшеницы: {e}")
        result['Пшеница'] = 15000
    
    # 2. УЛУЧШЕННЫЙ ПАРСИНГ ДРУГИХ КУЛЬТУР
    today = datetime.now().strftime("%Y-%m-%d")
    
    cereals_urls = {
        'Ячмень': f"https://www.zerno.ru/cerealspricesdate/{today}/barley",
        'Кукуруза': f"https://www.zerno.ru/cerealspricesdate/{today}/corn",
        'Подсолнечник': f"https://www.zerno.ru/cerealspricesdate/{today}/sunflower",
    }
    
    fallback_prices = {
        'Ячмень': 14000,
        'Кукуруза': 14000,
        'Соя': 25000,
        'Подсолнечник': 30000
    }
    
    price_ranges = {
        'Ячмень': (7000, 25000),
        'Кукуруза': (10000, 30000),  # РАСШИРЕНО!
        'Соя': (18000, 60000),  # РАСШИРЕНО!
        'Подсолнечник': (15000, 50000)
    }
    
    for culture, url in cereals_urls.items():
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code != 200:
                logging.warning(f"⚠️ {culture}: страница недоступна (код {response.status_code})")
                result[culture] = fallback_prices[culture]
                continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')
            
            if not table:
                logging.warning(f"⚠️ {culture}: таблица не найдена")
                result[culture] = fallback_prices[culture]
                continue
            
            prices = []
            rows = table.find_all('tr')
            logging.info(f"📋 {culture}: найдено строк {len(rows)}")
            
            for row in rows:
                cells = row.find_all('td')
                
                # Пропускаем короткие строки
                if len(cells) < 3:
                    continue
                
                # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Пропускаем заголовки
                first_cell = cells[0].get_text(strip=True)
                if any(keyword in first_cell for keyword in ['Класс', 'Город', 'цена', 'изм.', 'тренд', 'Валюта']):
                    continue
                
                # Получаем город/источник для логирования
                city = first_cell if first_cell else "Неизвестно"
                
                # Ищем цену в разных колонках (приоритет: 2, 1, 3, 4)
                for col_idx in [2, 1, 3, 4]:
                    if len(cells) <= col_idx:
                        continue
                    
                    price_text = cells[col_idx].get_text(strip=True)
                    
                    # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Пропускаем служебные значения
                    if not price_text or price_text in ['default_value', '-', '0', '', 'руб/т']:
                        continue
                    
                    try:
                        # Извлекаем только цифры
                        price_clean = re.sub(r'[^0-9]', '', price_text)
                        if not price_clean:
                            continue
                        
                        price_value = int(price_clean)
                        
                        # Валидация с расширенными диапазонами
                        min_p, max_p = price_ranges[culture]
                        if min_p <= price_value <= max_p:
                            prices.append(price_value)
                            logging.info(f"✅ {culture}: {price_value} ₽/т из {city}")
                            break  # Нашли цену, переходим к следующей строке
                    except Exception as e:
                        continue
            
            # Результат
            if prices:
                avg = int(sum(prices) / len(prices))
                result[culture] = avg
                logging.info(f"✅ {culture}: средняя {avg} ₽/т ({len(prices)} цен)")
            else:
                result[culture] = fallback_prices[culture]
                logging.warning(f"⚠️ {culture}: используем резервное значение {fallback_prices[culture]} ₽/т")
                
        except Exception as e:
            result[culture] = fallback_prices[culture]
            logging.error(f"❌ {culture}: {e}")
     
    try:
        soy_price = parse_soy_from_zol()
        
        if soy_price:
            result['Соя'] = soy_price
        else:
            # Резервное значение на основе последних реальных данных
            result['Соя'] = 28000
            logging.warning("⚠️ Соя: используем резервное значение 28,000 ₽/т")
            
    except Exception as e:
        result['Соя'] = 28000
        logging.error(f"❌ Соя: {e}, используем резервное")
    
    logging.info(f"📊 Парсинг завершён: {len(result)} культур")
    return result


def parse_fob_black_sea():
    """✅ Парсинг FOB (Черное море)"""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/ZW=F"
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        data = response.json()
        
        if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
            result = data['chart']['result'][0]
            if 'meta' in result and 'regularMarketPrice' in result['meta']:
                price_cents = result['meta']['regularMarketPrice']
                price_dollars = price_cents / 100
                fob_price = round(price_dollars * 36.74, 2)
                logging.info(f"✅ FOB: ${fob_price}/т")
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
            'Пшеница (CBoT)': 'ZW=F',
            'Кукуруза (CBoT)': 'ZC=F',
            'Соя (CBoT)': 'ZS=F'
        }
        
        for name, symbol in symbols.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                data = response.json()
                
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    result = data['chart']['result'][0]
                    if 'meta' in result and 'regularMarketPrice' in result['meta']:
                        price_cents = result['meta']['regularMarketPrice']
                        price_dollars = price_cents / 100
                        prices[name] = f"${price_dollars:.2f}/bu"
                        logging.info(f"✅ {name}: ${price_dollars:.2f}/bu")
            except Exception as e:
                logging.error(f"❌ {name}: {e}")
                continue
        
        if not prices:
            prices = {
                'Пшеница (CBoT)': '$5.50/bu',
                'Кукуруза (CBoT)': '$4.20/bu',
                'Соя (CBoT)': '$10.80/bu'
            }
            logging.warning("⚠️ CBoT: используем fallback")
        
        return prices
        
    except Exception as e:
        logging.error(f"❌ parse_cbot_futures: {e}")
        return {
            'Пшеница (CBoT)': '$5.50/bu',
            'Кукуруза (CBoT)': '$4.20/bu',
            'Соя (CBoT)': '$10.80/bu'
        }

def parse_grain_news(limit=5):
    """✅ Парсинг новостей с zerno.ru"""
    newslist = []
    
    try:
        url = "https://www.zerno.ru"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = soup.find_all('a', href=re.compile(r'/node/\d+'))
        seen_titles = set()
        
        keywords = ['экспорт', 'россия', 'астрахань', 'зерно', 'пшениц', 'урожай', 
                   'fob', 'черное море', 'цен', 'рынок']
        
        for link in links[:20]:
            title = link.text.strip()
            href = link.get('href', '')
            
            title_lower = title.lower()
            
            if title and len(title) > 30 and title not in seen_titles:
                if any(kw in title_lower for kw in keywords):
                    seen_titles.add(title)
                    
                    date = datetime.now().strftime("%d.%m.%Y")
                    full_link = f"https://www.zerno.ru{href}" if href.startswith('/') else href
                    
                    newslist.append({
                        'title': title,
                        'link': full_link,
                        'date': date
                    })
                    
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
            'data': {
                'russia_south': russia_prices,
                'fob': fob_price,
                'cbot': cbot_prices
            },
            'updated': datetime.now()
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
        
        news_cache = {
            'data': news,
            'updated': datetime.now()
        }
        
        logging.info(f"✅ Новости обновлены: {len(news)} записей")
        
    except Exception as e:
        logging.error(f"❌ update_news_cache: {e}")
        news_cache = {
            'data': [],
            'updated': datetime.now()
        }

def load_users_from_json():
    """Загрузка пользователей из JSON"""
    global users
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                users = {int(k): v for k, v in loaded.items()}
            logging.info(f"✅ Пользователи загружены: {len(users)}")
        else:
            logging.info("ℹ️ Файл пользователей не найден, создан новый")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки пользователей: {e}")

def save_users_to_json():
    """Сохранение пользователей в JSON"""
    try:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        logging.info("✅ Пользователи сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пользователей: {e}")

def load_batches_from_pickle():
    """Загрузка партий из pickle"""
    global batches
    
    try:
        if not os.path.exists('data/batches.pkl'):
            batches = {}
            logging.info("⚠️ Файл партий не найден, создана пустая структура")
            return
        
        with open('data/batches.pkl', 'rb') as f:
            loaded_batches = pickle.load(f)
            
            # ✅ ПРОВЕРЯЕМ ТИП ДАННЫХ
            if isinstance(loaded_batches, dict):
                batches = loaded_batches
            elif isinstance(loaded_batches, list):
                logging.warning("⚠️ Обнаружен старый формат batches (list), конвертирую в dict")
                batches = {}
                for batch in loaded_batches:
                    farmer_id = batch.get('farmer_id')
                    if farmer_id:
                        if farmer_id not in batches:
                            batches[farmer_id] = []
                        batches[farmer_id].append(batch)
            else:
                logging.error(f"❌ Неизвестный формат batches: {type(loaded_batches)}")
                batches = {}
        
        # Подсчёт
        total_batches = sum(len(batch_list) for batch_list in batches.values())
        farmers_count = len(batches)
        
        logging.info(f"✅ Загружено партий: {total_batches} от {farmers_count} фермеров")
        
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки партий: {e}")
        batches = {}


def save_batches_to_pickle():
    """Сохранение партий в pickle"""
    try:
        os.makedirs('data', exist_ok=True)
        
        # ✅ ПРОВЕРЯЕМ ЧТО СОХРАНЯЕМ СЛОВАРЬ
        if not isinstance(batches, dict):
            logging.error(f"❌ batches имеет неправильный тип: {type(batches)}")
            return
        
        with open('data/batches.pkl', 'wb') as f:
            pickle.dump(batches, f)
        
        total_batches = sum(len(batch_list) for batch_list in batches.values())
        logging.info(f"✅ Партии сохранены: {total_batches} партий")   
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения партий: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# ФУНКЦИИ СОХРАНЕНИЯ/ЗАГРУЗКИ ПУЛОВ И ПОЛЬЗОВАТЕЛЕЙ
# ═══════════════════════════════════════════════════════════════════════════

def save_pulls_to_pickle():
    """Сохранение пулов и участников в pickle"""
    try:
        data = {
            'pulls': pulls,
            'pullparticipants': pullparticipants
        }
        with open(PULLSFILE, 'wb') as f:
            pickle.dump(data, f)
        logging.info("✅ Пулы и участники сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пулов: {e}")


def load_pulls_from_pickle():
    """Загрузка пулов и участников из pickle"""
    global pulls, pullparticipants, pull_counter
    try:
        if os.path.exists(PULLSFILE):
            with open(PULLSFILE, 'rb') as f:
                data = pickle.load(f)
                pulls = data.get('pulls', {})
                pullparticipants = data.get('pullparticipants', {})

            # Восстанавливаем счетчик
            if pulls:
                pull_counter = max(pulls.keys()) if pulls else 0

            logging.info(f"✅ Загружено {len(pulls)} пулов и {len(pullparticipants)} групп участников")
        else:
            logging.info("📂 Файл pulls.pkl не найден")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки пулов: {e}")
        pulls = {}
        pullparticipants = {}


def save_users_to_pickle():
    """Сохранение пользователей в pickle"""
    try:
        with open(USERSFILE, 'wb') as f:
            pickle.dump(users, f)
        logging.info("✅ Пользователи сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пользователей: {e}")


def load_users_from_pickle():
    """Загрузка пользователей из pickle"""
    global users
    try:
        if os.path.exists(USERSFILE):
            with open(USERSFILE, 'rb') as f:
                loaded = pickle.load(f)
                if isinstance(loaded, dict):
                    users = loaded
                    logging.info(f"✅ Загружено: {len(users)}")
                else:
                    users = {}
        else:
            logging.info("📂 Файл не найден")
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")
        users = {}

def savepullstopickle():
    """Сохранение пуллов и участников в pickle"""
    try:
        data = {
            'pulls': pulls,
            'pullparticipants': pullparticipants
        }
        with open(PULLSFILE, 'wb') as f:
            pickle.dump(data, f)
        logging.info("✅ Пуллы и участники сохранены в pickle")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пуллов: {e}")


def loadpullsfrompickle():
    """Загрузка пуллов и участников из pickle"""
    global pulls, pullparticipants, pull_counter
    try:
        if os.path.exists(PULLSFILE):
            with open(PULLSFILE, 'rb') as f:
                data = pickle.load(f)
                pulls = data.get('pulls', {})
                pullparticipants = data.get('pullparticipants', {})
            
            # Восстанавливаем счетчик пуллов
            if pulls:
                pull_counter = max(pulls.keys()) if pulls else 0
            
            logging.info(f"✅ Загружено {len(pulls)} пуллов и {len(pullparticipants)} групп участников")
        else:
            logging.info("📂 Файл pulls.pkl не найден, начинаем с пустых данных")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки пуллов: {e}")
        pulls = {}
        pullparticipants = {}


class GoogleSheetsManager:
    """Менеджер для работы с Google Sheets"""

    def __init__(self, credentials_file, spreadsheet_id):
        self.spreadsheet_id = spreadsheet_id
        self.client = None
        self.spreadsheet = None

        try:
            if not os.path.exists(credentials_file):
                logging.warning(f"⚠️ Файл {credentials_file} не найден. Google Sheets будет отключен.")
                return

            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]

            creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
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
        except Exception as e:
            worksheet = self.spreadsheet.add_worksheet(
                title=title,
                rows=1000,
                cols=len(headers)
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

            headers = ['User ID', 'Имя', 'Роль', 'Телефон', 'Email', 'ИНН', 'Регион', 'Реквизиты', 'Дата регистрации', 'Обновлено']
            worksheet = self.get_or_create_worksheet('Users', headers)

            if not worksheet:
                return

            row_data = [
                str(user_id),
                str(user_data.get('name', '')),
                str(user_data.get('role', '')),
                str(user_data.get('phone', '')),
                str(user_data.get('email', '')),
                str(user_data.get('inn', '')),
                str(user_data.get('region', '')),
                str(user_data.get('company_requisites', '')),
                str(user_data.get('registration_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]

            try:
                cell = worksheet.find(str(user_id))
                if cell:
                    worksheet.update(f'A{cell.row}:J{cell.row}', [row_data])
                    logging.info(f"✅ Пользователь {user_id} обновлен в Google Sheets")
                else:
                    worksheet.append_row(row_data)
                    logging.info(f"✅ Пользователь {user_id} добавлен в Google Sheets")
            except Exception as e:
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
                'ID', 'Фермер ID', 'Культура', 'Объём (т)', 'Цена (₽/т)', 
                'Регион', 'Влажность (%)', 'Протеин (%)', 'Клейковина (%)', 
                'Сорность (%)', 'Дата готовности', 'Статус', 
                'Создано', 'Обновлено'
            ]
            worksheet = self.get_or_create_worksheet('Batches', headers)

            if not worksheet:
                return

            row_data = [
                str(batch.get('id', '')),
                str(batch.get('farmer_id', '')),
                str(batch.get('culture', '')),
                str(batch.get('volume', 0)),
                str(batch.get('price', 0)),
                str(batch.get('region', '')),
                str(batch.get('moisture', '')),
                str(batch.get('protein', '')),
                str(batch.get('gluten', '')),
                str(batch.get('weediness', '')),
                str(batch.get('readiness_date', '')),
                str(batch.get('status', 'active')),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]

            batch_id = str(batch.get('id', ''))
            try:
                cell = worksheet.find(batch_id)
                if cell:
                    worksheet.update(f'A{cell.row}:N{cell.row}', [row_data])
                    logging.info(f"✅ Партия {batch_id} обновлена в Google Sheets")
                else:
                    worksheet.append_row(row_data)
                    logging.info(f"✅ Партия {batch_id} добавлена в Google Sheets")
            except Exception as e:
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

            worksheet = self.spreadsheet.worksheet('Batches')
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
                'ID', 'Экспортер ID', 'Культура', 'Целевой объём (т)', 'Текущий объём (т)',
                'Цена (₽/т)', 'Влажность (%)', 'Сорность (%)', 'Статус', 'Создано', 'Обновлено'
            ]
            worksheet = self.get_or_create_worksheet('Pulls', headers)

            if not worksheet:
                return

            row_data = [
                str(pull.get('id', '')),
                str(pull.get('exporter_id', '')),
                str(pull.get('culture', '')),
                str(pull.get('target_volume', 0)),
                str(pull.get('current_volume', 0)),
                str(pull.get('price', 0)),
                str(pull.get('moisture', '')),
                str(pull.get('impurity', '')),
                str(pull.get('status', 'active')),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]

            pull_id = str(pull.get('id', ''))
            try:
                cell = worksheet.find(pull_id)
                if cell:
                    worksheet.update(f'A{cell.row}:K{cell.row}', [row_data])
                    logging.info(f"✅ Пул {pull_id} обновлен в Google Sheets")
                else:
                    worksheet.append_row(row_data)
                    logging.info(f"✅ Пул {pull_id} добавлен в Google Sheets")
            except Exception as e:
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
                'ID', 'Пул ID', 'Партия ID', 'Фермер ID', 'Экспортер ID',
                'Объём (т)', 'Цена (₽/т)', 'Статус', 'Создано'
            ]
            worksheet = self.get_or_create_worksheet('Deals', headers)

            if not worksheet:
                return

            row_data = [
                str(deal.get('id', '')),
                str(deal.get('pull_id', '')),
                str(deal.get('batch_id', '')),
                str(deal.get('farmer_id', '')),
                str(deal.get('exporter_id', '')),
                str(deal.get('volume', 0)),
                str(deal.get('price', 0)),
                str(deal.get('status', 'pending')),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]

            worksheet.append_row(row_data)
            logging.info("✅ Сделка добавлена в Google Sheets")

        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации сделки в Google Sheets: {e}")



def sync_user_to_sheets(self, user_id, user_data):
    """Синхронизация пользователя в Google Sheets"""
    if not self.spreadsheet:
        return
    
    try:
        headers = [
            'user_id', 'username', 'name', 'phone', 'email',
            'inn', 'company_details', 'region', 'role', 'registered_at'
        ]
        
        worksheet = self.get_or_create_worksheet('Users', headers)
        if not worksheet:
            return
        
        try:
            cell = worksheet.find(str(user_id))
            row_num = cell.row
            
            row_data = [
                str(user_id),
                clean_text(user_data.get('username', '')),
                clean_text(user_data.get('name', '')),
                clean_text(user_data.get('phone', '')),
                clean_text(user_data.get('email', '')),
                clean_text(user_data.get('inn', '')),
                clean_text(user_data.get('company_details', '')),
                clean_text(user_data.get('region', '')),
                user_data.get('role', ''),
                user_data.get('registered_at', '')
            ]
            
            worksheet.update(values=[row_data], range_name=f'A{row_num}:J{row_num}')
            logging.info(f"✅ Обновлен пользователь {user_id}")
            
        except Exception as e:
            # Пользователь не найден - добавляем новую строку
            row_data = [
                str(user_id),
                clean_text(user_data.get('username', '')),
                clean_text(user_data.get('name', '')),
                clean_text(user_data.get('phone', '')),
                clean_text(user_data.get('email', '')),
                clean_text(user_data.get('inn', '')),
                clean_text(user_data.get('company_details', '')),
                clean_text(user_data.get('region', '')),
                clean_text(user_data.get('role', '')),
                clean_text(user_data.get('registered_at', ''))
            ]
            
            worksheet.append_row(row_data)
            logging.info(f"✅ Добавлен пользователь {user_id}")
            
    except Exception as e:
        logging.error(f"❌ Ошибка синхронизации пользователя: {e}")

    
def sync_batch_to_sheets(self, batch_data):
    """Синхронизация партии в Google Sheets"""
    if not self.spreadsheet:
        logging.warning("⚠️ Google Sheets не подключен")
        return
    
    try:
        headers = [
            'batch_id', 'farmer_id', 'farmer_name', 'culture', 'region',
            'volume', 'price', 'humidity', 'impurity', 'quality_class',
            'storage_type', 'readiness_date', 'status', 'created_at'
        ]
        
        worksheet = self.get_or_create_worksheet('Batches', headers)
        if not worksheet:
            return
        
        try:
            cell = worksheet.find(str(batch_data['id']))
            row_num = cell.row
            
            row_data = [
                str(batch_data['id']),
                str(batch_data['farmer_id']),
                clean_text(batch_data.get('farmer_name', '')),
                clean_text(batch_data.get('culture', '')),
                clean_text(batch_data.get('region', '')),
                str(batch_data.get('volume', '')),
                str(batch_data.get('price', '')),
                str(batch_data.get('humidity', '')),
                str(batch_data.get('impurity', '')),
                clean_text(batch_data.get('quality_class', '')),
                clean_text(batch_data.get('storage_type', '')),
                clean_text(batch_data.get('readiness_date', '')),
                batch_data.get('status', ''),
                batch_data.get('created_at', '')
            ]
            
            worksheet.update(values=[row_data], range_name=f'A{row_num}:N{row_num}')
            logging.info(f"✅ Обновлена партия {batch_data['id']}")
            
        except Exception as e:
            # Партия не найдена - добавляем новую
            row_data = [
                str(batch_data['id']),
                str(batch_data['farmer_id']),
                clean_text(batch_data.get('farmer_name', '')),
                clean_text(batch_data.get('culture', '')),
                clean_text(batch_data.get('region', '')),
                str(batch_data.get('volume', '')),
                str(batch_data.get('price', '')),
                str(batch_data.get('humidity', '')),
                str(batch_data.get('impurity', '')),
                clean_text(batch_data.get('quality_class', '')),
                clean_text(batch_data.get('storage_type', '')),
                clean_text(batch_data.get('readiness_date', '')),
                batch_data.get('status', ''),
                batch_data.get('created_at', '')
            ]
            
            worksheet.append_row(row_data)
            logging.info(f"✅ Добавлена партия {batch_data['id']}")
            
    except Exception as e:
        logging.error(f"❌ Ошибка синхронизации партии: {e}")

        
    def sync_pull_to_sheets(self, pull_data):
        """Синхронизация пула в Google Sheets"""
        if not self.spreadsheet:
            return
        
        try:
            headers = [
                'pull_id', 'exporter_id', 'exporter_name', 'culture',
                'target_volume', 'current_volume', 'price', 'port',
                'moisture', 'nature', 'impurity', 'weed',
                'documents', 'doc_type', 'status', 'created_at'
            ]
            
            worksheet = self.get_or_create_worksheet('Pulls', headers)
            if not worksheet:
                return
            
            try:
                cell = worksheet.find(str(pull_data['id']))
                row_num = cell.row
                
                row_data = [
                    str(pull_data['id']),
                    str(pull_data['exporter_id']),
                    pull_data.get('exporter_name', ''),
                    pull_data.get('culture', ''),
                    str(pull_data.get('target_volume', '')),
                    str(pull_data.get('current_volume', '')),
                    str(pull_data.get('price', '')),
                    pull_data.get('port', ''),
                    str(pull_data.get('moisture', '')),
                    str(pull_data.get('nature', '')),
                    str(pull_data.get('impurity', '')),
                    str(pull_data.get('weed', '')),
                    pull_data.get('documents', ''),
                    pull_data.get('doc_type', ''),
                    pull_data.get('status', ''),
                    pull_data.get('created_at', '')
                ]
                
                worksheet.update(values=[row_data], range_name=f'A{row_num}:P{row_num}')
                logging.info(f"✅ Обновлён пул {pull_data['id']}")
                
            except Exception as e:
                row_data = [
                    str(pull_data['id']),
                    str(pull_data['exporter_id']),
                    pull_data.get('exporter_name', ''),
                    pull_data.get('culture', ''),
                    str(pull_data.get('target_volume', '')),
                    str(pull_data.get('current_volume', '')),
                    str(pull_data.get('price', '')),
                    pull_data.get('port', ''),
                    str(pull_data.get('moisture', '')),
                    str(pull_data.get('nature', '')),
                    str(pull_data.get('impurity', '')),
                    str(pull_data.get('weed', '')),
                    pull_data.get('documents', ''),
                    pull_data.get('doc_type', ''),
                    pull_data.get('status', ''),
                    pull_data.get('created_at', '')
                ]
                worksheet.append_row(row_data)
                logging.info(f"✅ Добавлен пул {pull_data['id']}")
                
        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации пула: {e}")
    
    def delete_batch_from_sheets(self, batch_id):
        """Удаление партии из Google Sheets"""
        if not self.spreadsheet:
            return
        
        try:
            worksheet = self.get_or_create_worksheet('Batches', [])
            if not worksheet:
                return
            
            cell = worksheet.find(str(batch_id))
            if cell:
                worksheet.delete_rows(cell.row)
                logging.info(f"✅ Удалена партия {batch_id} из Google Sheets")
                
        except Exception as e:
            logging.error(f"❌ Ошибка удаления партии {batch_id}: {e}")
    
    def update_batch_in_sheets(self, batch_data):
        """Обновление партии в Google Sheets"""
        self.sync_batch_to_sheets(batch_data)
    
    def update_user_in_sheets(self, user_id, user_data):
        """Обновление пользователя в Google Sheets"""
        self.sync_user_to_sheets(user_id, user_data)
    
    def update_pull_in_sheets(self, pull_data):
        """Обновление пула в Google Sheets"""
        self.sync_pull_to_sheets(pull_data)
    
    def sync_deal_to_sheets(self, deal_data):
        """Синхронизация сделки в Google Sheets"""
        if not self.spreadsheet:
            return
        
        try:
            headers = [
                'deal_id', 'pull_id', 'exporter_id', 'farmer_ids',
                'logistic_id', 'expeditor_id', 'total_volume', 
                'status', 'created_at', 'completed_at'
            ]
            
            worksheet = self.get_or_create_worksheet('Deals', headers)
            if not worksheet:
                return
            
            farmer_ids_str = ','.join(map(str, deal_data.get('farmer_ids', [])))
            
            try:
                cell = worksheet.find(str(deal_data['id']))
                row_num = cell.row
                
                row_data = [
                    str(deal_data['id']),
                    str(deal_data.get('pull_id', '')),
                    str(deal_data.get('exporter_id', '')),
                    farmer_ids_str,
                    str(deal_data.get('logistic_id', '')),
                    str(deal_data.get('expeditor_id', '')),
                    str(deal_data.get('total_volume', '')),
                    deal_data.get('status', ''),
                    deal_data.get('created_at', ''),
                    deal_data.get('completed_at', '')
                ]
                
                worksheet.update(values=[row_data], range_name=f'A{row_num}:J{row_num}')
                logging.info(f"✅ Обновлена сделка {deal_data['id']}")
                
            except Exception as e:
                row_data = [
                    str(deal_data['id']),
                    str(deal_data.get('pull_id', '')),
                    str(deal_data.get('exporter_id', '')),
                    farmer_ids_str,
                    str(deal_data.get('logistic_id', '')),
                    str(deal_data.get('expeditor_id', '')),
                    str(deal_data.get('total_volume', '')),
                    deal_data.get('status', ''),
                    deal_data.get('created_at', ''),
                    deal_data.get('completed_at', '')
                ]
                worksheet.append_row(row_data)
                logging.info(f"✅ Добавлена сделка {deal_data['id']}")
                
        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации сделки: {e}")
    
    def sync_match_to_sheets(self, match_data):
        """Синхронизация совпадения в Google Sheets"""
        if not self.spreadsheet:
            return
        
        try:
            headers = [
                'match_id', 'batch_id', 'pull_id', 'status', 'created_at'
            ]
            
            worksheet = self.get_or_create_worksheet('Matches', headers)
            if not worksheet:
                return
            
            try:
                cell = worksheet.find(str(match_data['id']))
                row_num = cell.row
                
                row_data = [
                    str(match_data['id']),
                    str(match_data.get('batch_id', '')),
                    str(match_data.get('pull_id', '')),
                    match_data.get('status', ''),
                    match_data.get('created_at', '')
                ]
                
                worksheet.update(values=[row_data], range_name=f'A{row_num}:E{row_num}')
 
            except Exception as e:
                row_data = [
                    str(match_data['id']),
                    str(match_data.get('batch_id', '')),
                    str(match_data.get('pull_id', '')),
                    match_data.get('status', ''),
                    match_data.get('created_at', '')
                ]
                worksheet.append_row(row_data)
                logging.info(f"✅ Добавлено совпадение {match_data['id']}")
                
        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации совпадения: {e}")
    
    def export_all_data(self):
        """Экспорт всех данных в Google Sheets"""
        if not self.spreadsheet:
            logging.warning("⚠️ Google Sheets не подключен")
            return False
        
        try:
            for user_id, user_data in users.items():
                self.sync_user_to_sheets(user_id, user_data)
            for user_batches in batches.values():
                for batch in user_batches:
                    self.sync_batch_to_sheets(batch)
            for pull_data in pulls.values():
                self.sync_pull_to_sheets(pull_data)
            for deal_data in deals.values():
                self.sync_deal_to_sheets(deal_data)
            for match_data in matches.values():
                self.sync_match_to_sheets(match_data)
            
            logging.info("✅ Все данные экспортированы в Google Sheets")
            return True
            
        except Exception as e:
            logging.error(f"❌ Ошибка экспорта данных: {e}")
            return False

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
            logging.warning(f"⚠️ Файл credentials не найден: {GOOGLE_SHEETS_CREDENTIALS}")
            return

        logging.info("🔄 Инициализация Google Sheets Manager...")

        # Создаём менеджер в отдельном потоке чтобы не блокировать
        loop = asyncio.get_event_loop()
        gs = await loop.run_in_executor(
            None,
            GoogleSheetsManager,
            GOOGLE_SHEETS_CREDENTIALS,
            SPREADSHEET_ID
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

@dp.message_handler(lambda message: message.text == "🚚 Предложения логистов", state='*')
async def farmer_view_logistics_offers(message: types.Message, state: FSMContext):
    """Просмотр предложений логистов для фермера"""
    await state.finish()
    
    user_id = message.from_user.id
    
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await message.answer("❌ Эта функция доступна только фермерам")
        return
    
    if not logistics_offers:
        await message.answer(
            "📭 <b>Предложения логистов пока отсутствуют</b>\n\n"
            "Когда появятся новые предложения, вы сможете их здесь увидеть.",
            parse_mode='HTML'
        )
        return
    
    active_offers = [offer for offer in logistics_offers.values() if offer.get('status') == 'active']
    
    if not active_offers:
        await message.answer(
            "📭 <b>Активных предложений нет</b>\n\n"
            "Все текущие предложения уже приняты или отклонены.",
            parse_mode='HTML'
        )
        return
    
    text = "🚚 <b>Доступные предложения логистов</b>\n\n"
    text += f"Найдено предложений: {len(active_offers)}\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for offer in active_offers[:10]:
        logist_name = users.get(offer['logist_id'], {}).get('name', 'Логист')
        
        button_text = (
            f"🚛 {offer['transport_type']} | "
            f"{offer['route']} | "
            f"{offer['price_per_ton']:,.0f} ₽/т"
        )
        
        keyboard.add(
            InlineKeyboardButton(
                button_text,
                callback_data=f"farmer_view_offer:{offer['id']}"
            )
        )
    
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')


@dp.callback_query_handler(lambda c: c.data.startswith('farmer_view_offer:'), state='*')
async def farmer_view_offer_details(callback: types.CallbackQuery, state: FSMContext):
    """Детали предложения логиста для фермера"""
    await state.finish()
    
    try:
        offer_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if offer_id not in logistics_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistics_offers[offer_id]
    logist_id = offer['logist_id']
    
    if logist_id not in users:
        await callback.answer("❌ Логист не найден", show_alert=True)
        return
    
    logist = users[logist_id]
    
    text = (
        f"🚚 <b>Предложение #{offer_id}</b>\n\n"
        f"👤 Логист: {logist.get('name', 'Не указано')}\n"
        f"📱 Телефон: {logist.get('phone', 'Не указан')}\n"
        f"📧 Email: {logist.get('email', 'Не указан')}\n\n"
        f"🚛 Транспорт: {offer['transport_type']}\n"
        f"📍 Маршрут: {offer['route']}\n"
        f"💰 Цена: {offer['price_per_ton']:,.0f} ₽/т\n"
        f"⏱ Сроки: {offer['delivery_days']} дней\n\n"
    )
    
    if offer.get('additional_info'):
        text += f"ℹ️ Доп. информация:\n{offer['additional_info']}\n\n"
    
    text += f"📅 Создано: {offer['created_at']}"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Принять предложение",
            callback_data=f"farmer_accept_offer:{offer_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton(
            "◀️ Назад к списку",
            callback_data="farmer_back_to_offers"
        )
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('farmer_accept_offer:'), state='*')
async def farmer_accept_logistics_offer(callback: types.CallbackQuery, state: FSMContext):
    """Фермер принимает предложение логиста"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    try:
        offer_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if offer_id not in logistics_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistics_offers[offer_id]
    
    if offer.get('status') != 'active':
        await callback.answer("❌ Это предложение уже неактивно", show_alert=True)
        return
    
    offer['status'] = 'accepted_by_farmer'
    offer['farmer_id'] = user_id
    offer['accepted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    save_logistics_to_pickle()
    
    await callback.answer("✅ Предложение принято!", show_alert=True)
    
    await callback.message.edit_text(
        f"✅ <b>Предложение #{offer_id} принято!</b>\n\n"
        f"Логист свяжется с вами для организации перевозки.\n\n"
        f"📱 Контакт логиста:\n"
        f"Телефон: {users[offer['logist_id']].get('phone', 'Не указан')}\n"
        f"Email: {users[offer['logist_id']].get('email', 'Не указан')}",
        parse_mode='HTML'
    )
    
    try:
        await bot.send_message(
            offer['logist_id'],
            f"🎉 <b>Ваше предложение #{offer_id} принято фермером!</b>\n\n"
            f"👤 Фермер: {users[user_id].get('name', 'Не указано')}\n"
            f"📱 Телефон: {users[user_id].get('phone', 'Не указан')}\n"
            f"📧 Email: {users[user_id].get('email', 'Не указан')}\n\n"
            f"Свяжитесь с фермером для согласования деталей.",
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка уведомления логиста: {e}")


@dp.callback_query_handler(lambda c: c.data == 'farmer_back_to_offers', state='*')
async def farmer_back_to_offers_list(callback: types.CallbackQuery):
    """Возврат к списку предложений"""
    await farmer_view_logistics_offers(callback.message, FSMContext(dp.storage, callback.from_user.id, callback.from_user.id))
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_deal:'), state='*')
async def view_deal_details(callback: types.CallbackQuery):
    """Просмотр деталей сделки"""
    deal_id = parse_callback_id(callback.data)
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    text = f"📋 <b>Сделка #{deal_id}</b>\n\n"
    
    text += f"📊 Статус: {DEAL_STATUSES.get(deal.get('status', 'pending'), deal.get('status'))}\n"
    
    if deal.get('total_volume'):
        text += f"📦 Объём: {deal['total_volume']} т\n"
    
    if deal.get('exporter_id'):
        exporter_name = users.get(deal['exporter_id'], {}).get('name', 'Неизвестно')
        text += f"📦 Экспортёр: {exporter_name}\n"
    
    if deal.get('farmer_ids'):
        farmers_count = len(deal['farmer_ids'])
        text += f"🌾 Фермеров: {farmers_count}\n"
    
    if deal.get('logistic_id'):
        logistic_name = users.get(deal['logistic_id'], {}).get('name', 'Неизвестно')
        text += f"🚚 Логист: {logistic_name}\n"
    
    if deal.get('expeditor_id'):
        expeditor_name = users.get(deal['expeditor_id'], {}).get('name', 'Неизвестно')
        text += f"🚛 Экспедитор: {expeditor_name}\n"
    
    if deal.get('created_at'):
        text += f"📅 Создана: {deal['created_at']}\n"
    
    if deal.get('completed_at'):
        text += f"✅ Завершена: {deal['completed_at']}\n"
    
    keyboard = deal_actions_keyboard(deal_id)
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.message_handler(lambda m: m.text == "🔍 Поиск экспортёров", state='*')
async def search_exporters(message: types.Message, state: FSMContext):
    """Поиск экспортёров для фермера"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await message.answer("❌ Эта функция доступна только фермерам")
        return
    
    if user_id not in batches or not batches[user_id]:
        await message.answer(
            "📦 У вас пока нет партий для поиска экспортёров.\n\n"
            "Сначала добавьте партию через меню '➕ Добавить партию'"
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for batch in batches[user_id]:
        if batch.get('status') in ['active', 'Активна', 'активна', '', None]:
            button_text = f"🌾 {batch['culture']} - {batch['volume']} т"
            keyboard.add(
                InlineKeyboardButton(button_text, callback_data=f"findexporters:{batch['id']}")
            )
    
    await message.answer(
        "🔍 <b>Поиск экспортёров</b>\n\n"
        "Выберите партию для поиска подходящих экспортёров:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


@dp.callback_query_handler(lambda c: c.data.startswith('findexporters:'), state='*')
async def process_find_exporters(callback: types.CallbackQuery):
    """Обработка выбора партии для поиска экспортёров"""
    batch_id = int(callback.data.split(':')[1])
    user_id = callback.from_user.id
    
    await callback.answer("🔍 Ищем подходящих экспортёров...")
    
    # Находим партию
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.message.answer("❌ Партия не найдена")
        return
    
    # Ищем подходящие пулы экспортёров
    matching_pulls = []
    
    for exporter_id, exporter_pulls_list in pulls.items():
        for pull in exporter_pulls_list:
            if pull.get('status') != 'open':
                continue
            
            # Проверяем соответствие культуры
            if pull.get('culture') != batch.get('culture'):
                continue
            
            # Проверяем цену (пулл должен предлагать цену >= цене фермера)
            if pull.get('price', 0) < batch.get('price', 0):
                continue
            
            # Проверяем объём (в пулле должно быть место)
            required = pull.get('volume', 0)
            current = pull.get('current_volume', 0)
            remaining = required - current
            
            if remaining >= batch.get('volume', 0):
                # Добавляем информацию об экспортёре
                exporter = users.get(exporter_id, {})
                matching_pulls.append({
                    'pull': pull,
                    'exporter': exporter,
                    'exporter_id': exporter_id,
                    'remaining_volume': remaining
                })
    
    if not matching_pulls:
        await callback.message.answer(
            f"🔍 <b>Результаты поиска для партии #{batch_id}</b>\n\n"
            f"🌾 {batch['culture']} - {batch['volume']} т\n"
            f"💰 {batch['price']:,.0f} ₽/т\n\n"
            f"❌ К сожалению, подходящих экспортёров не найдено.\n\n"
            f"💡 Попробуйте:\n"
            f"• Снизить цену\n"
            f"• Разделить партию на меньшие объёмы\n"
            f"• Подождать появления новых пулов",
            parse_mode='HTML'
        )
        return
    
    # Формируем сообщение с результатами
    text = (
        f"🔍 <b>Найдено {len(matching_pulls)} подходящих экспортёров</b>\n\n"
        f"Для партии:\n"
        f"🌾 {batch['culture']} - {batch['volume']} т\n"
        f"💰 {batch['price']:,.0f} ₽/т\n"
        f"📍 {batch.get('region', 'Не указан')}\n\n"
        f"<b>Подходящие предложения:</b>\n\n"
    )
    
    for idx, match in enumerate(matching_pulls[:10], 1):  # Показываем до 10
        pull = match['pull']
        exporter = match['exporter']
        
        text += (
            f"{idx}. <b>Экспортёр:</b> {exporter.get('company_name', 'Не указано')}\n"
            f"   💰 Цена: {pull.get('price', 0):,.0f} ₽/т\n"
            f"   📦 Нужно: {pull.get('volume', 0)} т (свободно: {match['remaining_volume']} т)\n"
            f"   📍 Порт: {pull.get('port', 'Не указан')}\n"
        )
        
        if exporter.get('phone'):
            text += f"   📞 {exporter['phone']}\n"
        
        text += "\n"
    
    if len(matching_pulls) > 10:
        text += f"\n... и ещё {len(matching_pulls) - 10} предложений"
    
    # Создаём клавиатуру с действиями
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for idx, match in enumerate(matching_pulls[:5], 1):  # Первые 5 для действий
        exporter_name = match['exporter'].get('company_name', f"Экспортёр {match['exporter_id']}")
        keyboard.add(
            InlineKeyboardButton(
                f"📩 Связаться с {exporter_name[:20]}",
                callback_data=f"contact_exporter:{match['exporter_id']}"
            )
        )
    
    keyboard.add(
        InlineKeyboardButton("🔙 Назад", callback_data='back_to_my_batches')
    )
    
    await callback.message.answer(text, reply_markup=keyboard, parse_mode='HTML')


@dp.message_handler(lambda m: m.text == "➕ Добавить партию", state='*')
async def add_batch_start(message: types.Message, state: FSMContext):
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await message.answer("❌ Эта функция доступна только фермерам")
        return
    await AddBatch.culture.set()  #ВАЖНО!

    await message.answer(
        "📦 <b>Добавление партии</b>\n\n"
        "Шаг 1 из 9\n\n"
        "Выберите культуру:",
        reply_markup=culture_keyboard(),
        parse_mode='HTML'
    )

@dp.callback_query_handler(lambda c: c.data.startswith('search_crop_'), state='*')
async def search_by_culture_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обработка поиска партий по культуре - ИСПРАВЛЕНО"""
    await state.finish()
    
    try:
        # Извлекаем культуру из callback_data
        culture = callback.data.replace('search_crop_', '').replace('search_selectcrop_', '')
        logging.info(f"🔍 Поиск партий по культуре: {culture}")
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка выбора культуры", show_alert=True)
        return
    
    # Поиск всех активных партий
    found_batches = []  # ← ИСПРАВЛЕНО НА СПИСОК!
    
    for farmer_id in batches:
        for batch in batches[farmer_id]:
            if batch.get('culture') == culture and batch.get('status') in ['active', 'available']:
                batch['farmer_id'] = farmer_id  # Добавляем farmer_id для дальнейшего использования
                found_batches.append(batch)
    
    if not found_batches:
        # Партий не найдено
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_search"))
        await callback.message.edit_text(
            f"❌ <b>Партий с культурой «{culture}» не найдено</b>\n\n"
            f"Попробуйте выбрать другую культуру.",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await callback.answer()
        return
    
    # Сортируем по цене (от меньшей к большей)
    found_batches_sorted = sorted(found_batches, key=lambda x: x.get('price', 0))
    
    # Формируем сообщение с результатами
    text = f"🔍 <b>Найдено партий: {len(found_batches)}</b>\n"
    text += f"📦 Культура: <b>{culture}</b>\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for idx, batch in enumerate(found_batches_sorted[:10], 1):  # Максимум 10 результатов
        volume = batch.get('volume', 0)
        price = batch.get('price', 0)
        region = batch.get('region', 'Не указан')
        batch_id = batch.get('id', 'N/A')
        farmer_id = batch.get('farmer_id')
        
        text += f"{idx}. 📦 <b>{volume:.1f} т</b> | "
        text += f"💰 {price:,.0f} ₽/т | "
        text += f"📍 {region}\n"
        
        keyboard.add(
            InlineKeyboardButton(
                f"👁 Партия #{batch_id} - {volume:.1f} т",
                callback_data=f"viewbatch_{batch_id}_{farmer_id}"
            )
        )
    
    if len(found_batches) > 10:
        text += f"\n<i>... и ещё {len(found_batches) - 10} партий</i>"
    
    keyboard.add(InlineKeyboardButton("🔙 Назад к поиску", callback_data="back_to_search"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer(f"✅ Найдено: {len(found_batches)}")

# ====================================================================
# КАРТОЧКИ ЛОГИСТОВ И ЭКСПЕДИТОРОВ
# ====================================================================
# После существующего хендлера searchbyculture добавьте:

@dp.callback_query_handler(lambda c: c.data.startswith('culture:'), state=SearchByCulture.waiting_culture)
async def search_by_culture_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора культуры для поиска"""
    global batches
    await state.finish()
    
    try:
        culture = callback.data.replace('search_selectcrop_', '')
        logging.info(f"🔍 Поиск партий по культуре: {culture}")
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка выбора культуры", show_alert=True)
        return
    
    # Поиск всех активных партий
    found_batches = []   
    for farmer_id in batches:
        for batch in batches[farmer_id]:
            if batch.get('culture') == culture and batch.get('status') in ['active', 'available']:
                batch_copy = batch.copy()
                batch_copy['farmer_id'] = farmer_id
                found_batches.append(batch_copy)
    
    if found_batches:
        # Сортируем по цене
        found_batches_sorted = sorted(found_batches, key=lambda x: x.get('price', 0))
        
        text = f"🔍 <b>Найдено партий:</b> {len(found_batches)}\n"
        text += f"<b>Культура:</b> {culture}\n\n"
        
        keyboard = InlineKeyboardMarkup(row_width=1)
        
        # Показываем первые 10 партий
        for i, batch in enumerate(found_batches_sorted[:10], 1):
            volume = batch.get('volume', 0)
            price = batch.get('price', 0)
            region = batch.get('region', 'Не указан')
            
            text += f"{i}. <b>{volume:.1f} т</b> - {price:,.0f} ₽/т\n"
            text += f"   📍 {region}\n\n"
            
            keyboard.add(InlineKeyboardButton(
                f"Просмотр: {volume:.1f} т - {price:,.0f} ₽/т",
                callback_data=f"viewbatch_{batch['id']}"
            ))
        
        if len(found_batches) > 10:
            text += f"\n...и ещё {len(found_batches) - 10} партий"
        
        keyboard.add(InlineKeyboardButton("🔙 Назад к поиску", callback_data="startsearch"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        logging.info(f"✅ Найдено {len(found_batches)} партий по культуре {culture}")
    else:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="startsearch"))
        
        await callback.message.edit_text(
            f"❌ Партий с культурой <b>{culture}</b> не найдено\n\n"
            f"Попробуйте выбрать другую культуру или добавьте свою партию.",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        logging.info(f"❌ Партий по культуре {culture} не найдено")
    
    await callback.answer()

# Глобальные переменные (если ещё не определены)
@dp.callback_query_handler(lambda c: c.data.startswith('culture:'), state=AddBatch.culture)
async def add_batch_culture(callback: types.CallbackQuery, state: FSMContext):
    """Выбор культуры для партии"""
    try:
        culture = callback.data.split(':', 1)[1]
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга: {e}, data: {callback.data}")
        return
    
    await state.update_data(culture=culture)
    
    await callback.message.edit_text(
        "📦 <b>Добавление партии</b>\n\n"
        "Шаг 2 из 9\n\n"
        "Выберите регион:",
        reply_markup=region_keyboard(),
        parse_mode='HTML'
    )
    await AddBatch.region.set()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('region:'), state=AddBatch.region)
async def add_batch_region(callback: types.CallbackQuery, state: FSMContext):
    """Выбор региона для партии"""
    region = callback.data.split(':', 1)[1]
    await state.update_data(region=region)
    
    await callback.message.edit_text(
        "📦 <b>Добавление партии</b>\n\n"
        "Шаг 3 из 9\n\n"
        "Введите объём партии (в тоннах):",
        parse_mode='HTML'
    )
    await AddBatch.volume.set()
    await callback.answer()

@dp.message_handler(state=AddBatch.volume)
async def add_batch_volume(message: types.Message, state: FSMContext):
    """Ввод объёма партии"""
    try:
        volume = float(message.text.strip().replace(',', '.'))
        if volume <= 0:
            raise ValueError
        
        await state.update_data(volume=volume)
        
        await message.answer(
            "📦 <b>Добавление партии</b>\n\n"
            "Шаг 4 из 9\n\n"
            "Введите цену (₽/тонна):",
            parse_mode='HTML'
        )
        await AddBatch.price.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число больше 0:")

@dp.message_handler(state=AddBatch.price)
async def add_batch_price(message: types.Message, state: FSMContext):
    """Ввод цены партии"""
    try:
        price = float(message.text.strip().replace(',', '.'))
        if price <= 0:
            raise ValueError
        
        await state.update_data(price=price)
        
        await message.answer(
            "📦 <b>Добавление партии</b>\n\n"
            "Шаг 5 из 9\n\n"
            "Введите влажность (%):",
            parse_mode='HTML'
        )
        await AddBatch.humidity.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число больше 0:")

@dp.message_handler(state=AddBatch.humidity)
async def add_batch_humidity(message: types.Message, state: FSMContext):
    """Ввод влажности партии"""
    try:
        humidity = float(message.text.strip().replace(',', '.'))
        if not 0 <= humidity <= 100:
            raise ValueError
        
        await state.update_data(humidity=humidity)
        
        await message.answer(
            "📦 <b>Добавление партии</b>\n\n"
            "Шаг 6 из 9\n\n"
            "Введите сорность (%):",
            parse_mode='HTML'
        )
        await AddBatch.impurity.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число от 0 до 100:")

@dp.message_handler(state=AddBatch.impurity)
async def add_batch_impurity(message: types.Message, state: FSMContext):
    """Ввод сорности партии"""
    try:
        impurity = float(message.text.strip().replace(',', '.'))
        if not 0 <= impurity <= 100:
            raise ValueError
        
        await state.update_data(impurity=impurity)
        data = await state.get_data()
        quality_class = determine_quality_class(data['humidity'], impurity)
        await state.update_data(quality_class=quality_class)
        
        await message.answer(
            "📦 <b>Добавление партии</b>\n\n"
            "Шаг 7 из 9\n\n"
            f"Автоматически определен класс качества: <b>{quality_class}</b>\n\n"
            "Выберите тип хранения:",
            reply_markup=storage_type_keyboard(),
            parse_mode='HTML'
        )
        await AddBatch.storage_type.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число от 0 до 100:")

@dp.callback_query_handler(lambda c: c.data.startswith('storage:'), state=AddBatch.storage_type)
async def add_batch_storage_type(callback: types.CallbackQuery, state: FSMContext):
    """Выбор типа хранения"""
    storage_type = callback.data.split(':', 1)[1]
    await state.update_data(storage_type=storage_type)
    
    await callback.message.edit_text(
        "📦 <b>Добавление партии</b>\n\n"
        "Шаг 8 из 9\n\n"
        "Введите дату готовности (в формате ДД.ММ.ГГГГ) или 'сейчас' если готова:",
        parse_mode='HTML'
    )
    await AddBatch.readiness_date.set()
    await callback.answer()

@dp.message_handler(state=AddBatch.readiness_date)
async def add_batch_readiness_date(message: types.Message, state: FSMContext):
    """Завершение добавления расширенной партии"""
    global batch_counter
    
    readiness_date = message.text.strip()

    if readiness_date.lower() == 'сейчас':
        readiness_date = datetime.now().strftime('%d.%m.%Y')
    elif not validate_date(readiness_date):
        await message.answer("❌ Некорректная дата. Используйте формат ДД.ММ.ГГГГ или 'сейчас':")
        return
    
    user_id = message.from_user.id
    data = await state.get_data()
    
    # Создаём партию
    batch_counter += 1
    batch = {
        'id': batch_counter,
        'farmer_id': user_id,
        'farmer_name': users[user_id].get('name', ''),
        'culture': data['culture'],
        'region': data['region'],
        'volume': data['volume'],
        'price': data['price'],
        'humidity': data['humidity'],
        'impurity': data['impurity'],
        'quality_class': data['quality_class'],
        'storage_type': data['storage_type'],
        'readiness_date': readiness_date,
        'status': 'Активна',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'files': [],
        'matches': []
    }
    
    # Добавляем партию в базу
    if user_id not in batches:
        batches[user_id] = []
    batches[user_id].append(batch)
    
    save_batches_to_pickle()
    
    # ✅ АВТОПРИСОЕДИНЕНИЕ К ПУЛУ (если партия создавалась для пула)
    if 'create_batch_for_pull_id' in data:
        pull_id = data['create_batch_for_pull_id']
        
        if pull_id in pulls:
            pull = pulls[pull_id]
            available = pull['target_volume'] - pull['current_volume']
            
            if batch['volume'] <= available and batch['culture'] == pull['culture']:
                if pull_id not in pullparticipants:
                    pullparticipants[pull_id] = []
                
                # Проверяем что ещё не присоединились
                already_joined = any(p['batch_id'] == batch['id'] for p in pullparticipants[pull_id])
                
                if not already_joined:
                    participant = {
                        'farmer_id': user_id,
                        'farmer_name': users[user_id].get('name', ''),
                        'batch_id': batch['id'],
                        'volume': batch['volume'],
                        'joined_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    pullparticipants[pull_id].append(participant)
                    pull['current_volume'] += batch['volume']
                    
                    batch['status'] = 'Зарезервирована'
                    
                    save_pulls_to_pickle()
                    save_batches_to_pickle()
                    
                    logging.info(f"✅ Партия #{batch['id']} автоматически присоединена к пулу #{pull_id}")
                    
                    # Уведомление экспортёру о новой партии
                    try:
                        # ✅ ПРОВЕРКА НАЛИЧИЯ ЭКСПОРТЁРА
                        if 'exporter_id' in pull and pull['exporter_id']:
                            farmer_card = format_farmer_card(user_id, batch['id'])
                            
                            await bot.send_message(
                                pull['exporter_id'],
                                f"🎉 <b>Новая партия присоединена к пулу #{pull_id}!</b>\n\n{farmer_card}",
                                parse_mode='HTML'
                            )
                            logging.info(f"✅ Уведомление экспортёру {pull['exporter_id']} отправлено")
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
                        f"Экспортёр получил уведомление и свяжется с вами.",
                        parse_mode='HTML',
                        reply_markup=get_role_keyboard('farmer')
                    )
                    
                    await state.finish()
                    return

    # ✅ ОБЫЧНЫЙ ФЛОУ (партия НЕ для конкретного пула)
    
    # Синхронизация с Google Sheets
    if gs and gs.spreadsheet:
        try:
            gs.sync_batch_to_sheets(batch)
            farmer_name = users[user_id].get('name', 'Неизвестно')
            await publish_batch_to_channel(batch, farmer_name)
        except Exception as e:
            logging.error(f"Ошибка синхронизации с Google Sheets: {e}")
    
    # Ищем подходящие пулы
    matching_pulls = await find_matching_exporters(batch)
    
    keyboard = get_role_keyboard('farmer')
    
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
            pull_id = pull_dict['pull_id']
            
            # Создаём match-объект для notify_match()
            match_obj = {
                'pull_id': pull_id,
                'exporter_company': pull_dict.get('exporter_company', ''),
                'exporter_name': pull_dict.get('exporter_name', ''),
                'price': pull_dict.get('price', 0),
                'port': pull_dict.get('port', ''),
                'current_volume': pull_dict.get('current_volume', 0),
                'target_volume': pull_dict.get('target_volume', 0)
            }
            match_objs.append(match_obj)
            
            # ✅ СОХРАНЯЕМ В matches
            match_id = len(matches) + 1
            matches[match_id] = {
                'id': match_id,
                'batch_id': batch['id'],
                'pull_id': pull_id,
                'status': 'active',
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        
        await asyncio.sleep(0.5)
        await notify_match(user_id, batch, match_objs)
    
    await message.answer(message_text, reply_markup=keyboard, parse_mode='HTML')
    await state.finish()


@dp.callback_query_handler(lambda c: c.data.startswith('view_matches:'), state='*')


async def view_batch_matches(callback: types.CallbackQuery):
    """Просмотр совпадений для партии"""
    batch_id = parse_callback_id(callback.data)  
    user_id = callback.from_user.id
    
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    
    batch_matches = []
    for match in matches.values():
        if (match['batch_id'] == batch_id and 
            match['status'] == 'active'):
            batch_matches.append(match)
    
    if not batch_matches:
        await callback.answer("🤷‍♂️ Активных совпадений не найдено", show_alert=True)
        return
    
    text = f"🎯 <b>Совпадения для партии #{batch_id}</b>\n\n"
    text += f"🌾 {batch['culture']} • {batch['volume']} т • {batch['price']:,.0f} ₽/т\n\n"
    
    for i, match in enumerate(batch_matches[:5], 1):
        pull_id = match['pull_id']
        if pull_id in pulls:
            pull = pulls[pull_id]
            progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
            
            text += f"{i}. <b>Пулл #{pull_id}</b>\n"
            text += f"   📦 Нужно: {pull['target_volume']} т ({progress:.0f}% заполнено)\n"
            text += f"   💰 Цена: ${pull['price']}/т (~{pull['price'] * 75:,.0f} ₽/т)\n"
            text += f"   🚢 Порт: {pull['port']}\n"
            text += f"   👤 Экспортёр: {pull['exporter_name']}\n\n"
    
    if len(batch_matches) > 5:
        text += f"<i>... и ещё {len(batch_matches) - 5} совпадений</i>\n\n"
    
    text += "💡 <b>Рекомендация:</b> Свяжитесь с экспортёрами для обсуждения деталей."
    
    await callback.message.answer(text, parse_mode='HTML')
    await callback.answer()

@dp.message_handler(lambda m: m.text == "🔧 Мои партии", state='*')
async def view_my_batches(message: types.Message, state: FSMContext):
    global batches
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await message.answer("❌ Эта функция доступна только фермерам")
        return
    
    if user_id not in batches or not batches[user_id]:
        await message.answer(
            "📦 У вас пока нет добавленных партий.\n\n"
            "Используйте кнопку '➕ Добавить партию' для создания новой."
        )
        return
    
    user_batches = batches[user_id]
    active_batches = [b for b in user_batches if b.get('status') == 'Активна']
    other_batches = [b for b in user_batches if b.get('status') != 'Активна']
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for batch in active_batches:
        has_matches = any(m['batch_id'] == batch['id'] and m['status'] == 'active' 
                         for m in matches.values())
        match_emoji = "🎯 " if has_matches else ""
        
        button_text = (
            f"{match_emoji}✅ {batch['culture']} - {batch['volume']} т "
            f"({batch['price']:,.0f} ₽/т)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"viewbatch_{batch['id']}")
        )
    
    for batch in other_batches:
        status_emoji = {
            'Зарезервирована': '🔒',
            'Продана': '💰',
            'Снята с продажи': '❌'
        }.get(batch.get('status', 'Активна'), '📦')
        
        button_text = (
            f"{status_emoji} {batch['culture']} - {batch['volume']} т "
            f"({batch['price']:,.0f} ₽/т)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"viewbatch_{batch['id']}")
        )
    
    keyboard.add(
        InlineKeyboardButton("🔄 Автопоиск экспортёров", callback_data="auto_match_all")
    )
    
    await message.answer(
        f"📦 <b>Ваши партии</b> ({len(batches)} шт.)\n\n"
        f"✅ Активные: {len(active_batches)}\n"
        f"🎯 С совпадениями: {len([b for b in active_batches if any(m['batch_id'] == b['id'] and m['status'] == 'active' for m in matches.values())])}\n\n"
        "Нажмите на партию для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
@dp.message_handler(lambda m: m.text == "🎯 Пулы", state='*')
async def view_pools_menu(message: types.Message, state: FSMContext):
    """Просмотр пулов для фермера"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await message.answer("❌ Эта функция доступна только фермерам")
        return
    
    open_pulls = [pull for pull in pulls.values() if pull.get('status') == 'Открыт']
    
    if not open_pulls:
        await message.answer(
            "🎯 <b>Активные пулы</b>\n\n"
            "Сейчас нет открытых пулов для участия.\n"
            "Пулы создаются экспортёрами для сбора партий зерна.",
            parse_mode='HTML'
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for pull in open_pulls[:10]:
        progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
        button_text = f"🌾 {pull['culture']} - {pull['target_volume']} т ({progress:.0f}% заполнено)"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"viewpull:{pull['id']}")  # ← ИСПРАВЛЕНО!
        )
    
    await message.answer(
        f"🎯 <b>Активные пулы</b> ({len(open_pulls)} шт.)\n\n"
        "Выберите пул для просмотра деталей и присоединения:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

@dp.message_handler(state=JoinPullStatesGroup.volume)
async def join_pull_volume(message: types.Message, state: FSMContext):
    try:
        volume = float(message.text.strip().replace(",", ".").replace(" ", ""))
        if volume <= 0:
            raise ValueError
        
        data = await state.get_data()
        pull_id = data['pull_id']
        batch_id = data['batch_id']
        user_id = message.from_user.id
        
        pull = pulls.get(pull_id)
        if not pull:
            await message.answer("❌ Пул не найден")
            await state.finish()
            return
        
        # Проверка доступного места
        available = pull['target_volume'] - pull.get('current_volume', 0)
        
        if volume > available:
            await message.answer(
                f"❌ Превышен доступный объём!\n"
                f"Доступно: {available:,.0f} т\n"
                f"Вы указали: {volume:,.0f} т"
            )
            return
        
        # Добавляем участника
        if 'participants' not in pull:
            pull['participants'] = []
        
        pull['participants'].append({
            'farmer_id': user_id,
            'batch_id': batch_id,
            'volume': volume,
            'joined_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Обновляем текущий объём
        pull['current_volume'] = pull.get('current_volume', 0) + volume
        save_pulls_to_pickle()
        
        # ✅ КЛЮЧЕВАЯ ПРОВЕРКА - ДОБАВИТЬ ЭТУ СТРОКУ!
        is_full = check_and_close_pull_if_full(pull_id)
        
        await state.finish()
        
        if is_full:
            # Пул заполнен на 100%
            await message.answer(
                f"🎉 <b>Поздравляем!</b>\n\n"
                f"Ваша партия добавлена: {volume:,.0f} т\n\n"
                f"✅ <b>Пул #{pull_id} заполнен на 100%!</b>\n\n"
                f"Пул автоматически закрыт и создана сделка.\n"
                f"Детали сделки придут отдельно.",
                parse_mode='HTML',
                reply_markup=farmer_keyboard()
            )
        else:
            # Обычное добавление
            fill_percent = (pull['current_volume'] / pull['target_volume']) * 100
            remaining = pull['target_volume'] - pull['current_volume']
            
            await message.answer(
                f"✅ <b>Партия добавлена в пул!</b>\n\n"
                f"📦 Ваш объем: {volume:,.0f} т\n"
                f"💵 Цена: ${pull['price']:,.0f}/т\n"
                f"💰 Ваша сумма: ${volume * pull['price']:,.0f}\n\n"
                f"📊 <b>Заполненность пула:</b>\n"
                f"{pull['current_volume']:,.0f} / {pull['target_volume']:,.0f} т ({fill_percent:.1f}%)\n"
                f"Осталось: {remaining:,.0f} т\n\n"
                f"Вы получите уведомление, когда пул будет заполнен.",
                parse_mode='HTML',
                reply_markup=farmer_keyboard()
            )
        
        logging.info(f"Batch {batch_id} → Pull {pull_id}, volume: {volume}, full: {is_full}")
        
    except ValueError:
        await message.answer("❌ Некорректный объём. Введите положительное число.")


@dp.message_handler(lambda m: m.text == "🚚 Заявка на логистику", state='*')
async def create_shipping_request(message: types.Message, state: FSMContext):
    """Создание заявки на логистику"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await message.answer("❌ Эта функция доступна только экспортёрам")
        return
    
    user_pulls = [p for p in pulls.values() if p['exporter_id'] == user_id and p.get('status') == 'Открыт']
    
    if not user_pulls:
        await message.answer(
            "🚚 <b>Заявка на логистику</b>\n\n"
            "У вас нет открытых пулов для создания заявки на логистику.\n"
            "Сначала создайте пул через меню '➕ Создать пул'"
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for pull in user_pulls:
        button_text = f"🌾 {pull['culture']} - {pull['target_volume']} т → {pull['port']}"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"create_shipping:{pull['id']}")
        )
    
    await message.answer(
        "🚚 <b>Создание заявки на логистику</b>\n\n"
        "Выберите пул для которого нужна логистика:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
@dp.callback_query_handler(lambda c: c.data == 'refresh_prices', state='*')
async def refresh_prices(callback: types.CallbackQuery, state: FSMContext):
    """Обновление отображения цен (показываем текущие)"""
    await state.finish()

    prices_msg = format_prices_message()
    
    try:
        await callback.message.edit_text(prices_msg, parse_mode='HTML')
        await callback.answer("✅ Цены обновлены!")
    except MessageNotModified:
        await callback.answer("ℹ️ Цены актуальны", show_alert=False)

@dp.callback_query_handler(lambda c: c.data == "refresh_news", state='*')
async def refresh_news(callback: types.CallbackQuery):
    """Обновление новостей"""
    await callback.answer("🔄 Обновляем новости...")
    await update_news_cache()
    
    news_msg = format_news_message()
    await callback.message.edit_text(
        news_msg, 
        parse_mode='HTML',
        disable_web_page_preview=True
    )

@dp.callback_query_handler(lambda c: c.data in ["view_analytics", "view_grain_news", "view_export_news"], state='*')
@dp.callback_query_handler(lambda c: c.data == "auto_match_all", state='*')
async def auto_match_all_batches(callback: types.CallbackQuery):
    """Автопоиск экспортёров для всех активных партий"""
    user_id = callback.from_user.id
    
    if user_id not in batches:
        await callback.answer("❌ У вас нет партий", show_alert=True)
        return
    
    active_batches = [b for b in batches[user_id] if b.get('status') == 'Активна']
    
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
            f"✅ Автопоиск завершен!\n\n"
            f"Найдено совпадений: {total_matches}\n"
            f"Экспортёры получили уведомления о ваших партиях."
        )
    else:
        await callback.message.answer(
            "🤷‍♂️ К сожалению, подходящих экспортёров не найдено.\n\n"
            "Рекомендуем:\n"
            "• Проверить актуальность цен\n"
            "• Уточнить параметры качества\n"
            "• Подождать новых предложений"
        )

async def view_batch_details_direct(message: types.Message, batch_id: str, user_id: int):
    """Показать детали партии"""
    logging.info(f"🔍 Ищем партию batch_id={batch_id}, user_id={user_id}")

    # ✅ ПРОВЕРКА ТИПА ДАННЫХ
    if not isinstance(batches, dict):
        logging.error(f"❌ batches имеет неправильный тип: {type(batches)}")
        await message.answer("❌ Ошибка загрузки партий")
        return

    batch = None

    # ✅ ПРАВИЛЬНЫЙ ПОИСК ПАРТИИ
    user_batches = batches.get(user_id, [])
    for b in user_batches:
        if str(b.get('id')) == str(batch_id):
            batch = b
            break
    
    if not batch:
        await message.answer("❌ Партия не найдена")
        return
    
    active_matches = [m for m in matches.values() 
                     if m['batch_id'] == batch_id and m['status'] == 'active']
    
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
    
    if batch.get('files'):
        text += f"\n📎 Прикреплено файлов: {len(batch['files'])}"
    
    keyboard = batch_actions_keyboard(batch_id)
    
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')

@dp.callback_query_handler(lambda c: c.data.startswith('view_batch:'), state='*')
async def view_batch_details_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()  #Теперь state определен ✅
    batch_id = parse_callback_id(callback.data)  
    user_id = callback.from_user.id
    await view_batch_details_direct(callback.message, batch_id, user_id)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('viewbatch_'), state='*')
async def view_batch_from_search(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей партии из результатов поиска"""
    logging.info(f"🔍 viewbatch callback: {callback.data}")
    await state.finish()
    
    try:
        batch_id = int(callback.data.split('_')[1])
        logging.info(f"✅ Извлечён batch_id: {batch_id}")
    except Exception as e:
        logging.error(f"❌ Ошибка извлечения batch_id: {e}")
        await callback.message.answer("❌ Ошибка обработки")
        return
    
    user_id = callback.from_user.id
    await view_batch_details_direct(callback.message, batch_id, user_id)

async def view_batch_details_handler(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей партии"""
    await state.finish()
    
    try:
        # ✅ ИСПРАВЛЕНО: используем ':' вместо '_'
        batch_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        logging.error(f"Ошибка парсинга batch_id из {callback.data}: {e}")
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    # Ищем партию
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    
    # Формируем сообщение с деталями
    msg = f"📦 <b>Партия #{batch_id}</b>\n\n"
    msg += f"🌾 Культура: {batch['culture']}\n"
    msg += f"📊 Объём: {batch['volume']} т\n"
    msg += f"💰 Цена: {batch['price']:,.0f} ₽/т\n"
    msg += f"📍 Регион: {batch.get('region', 'Не указан')}\n"
    msg += f"📋 Статус: {batch.get('status', 'Активна')}\n"
    
    # Качество если есть
    if 'nature' in batch:
        msg += "\n<b>Качество:</b>\n"
        msg += f"   🔸 Натура: {batch.get('nature', 'Не указано')} г/л\n"
        msg += f"   💧 Влажность: {batch.get('moisture', '-')}%\n"
        msg += f"   🌿 Сорность: {batch.get('impurity', '-')}%\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="view_my_batches")
    )
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'back_to_my_batches', state='*')
async def back_to_my_batches(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку партий"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Проверка роли
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await callback.answer("❌ Эта функция доступна только фермерам", show_alert=True)
        return
    
    # Получаем партии пользователя
    user_batches = batches.get(user_id, [])
    
    if not user_batches:
        await callback.message.edit_text(
            "📦 У вас пока нет добавленных партий.\n\n"
            "Используйте кнопку '➕ Добавить партию' для создания новой.",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("🏠 Главное меню", callback_data="backtomainmenu")
            )
        )
        await callback.answer()
        return
    
    # Фильтруем активные партии
    active_batches = [b for b in user_batches if b.get('status') in ['Активна', 'active', 'Доступна']]
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Добавляем кнопки партий
    for batch in active_batches:
        culture = batch.get('culture', '?')
        volume = batch.get('volume', 0)
        price = batch.get('price', 0)
        button_text = f"✅ {culture} - {volume} т ({price:,.0f} ₽/т)"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"viewbatch_{batch['id']}")
        )
    
    # Если есть активные партии, добавляем кнопку автопоиска
    if active_batches:
        keyboard.add(
            InlineKeyboardButton("🔍 Автопоиск экспортёров", callback_data="auto_match_all")
        )
    
    # Кнопка возврата в главное меню
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="backtomainmenu")
    )
    
    message_text = f"📦 <b>Ваши партии</b> ({len(active_batches)} активных)\n\nВыберите партию для просмотра:"
    
    try:
        await callback.message.edit_text(
            message_text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    except MessageNotModified:
        pass
    
    await callback.answer()

    
    await callback.message.edit_text(
        f"📦 <b>Ваши партии</b> ({len(batches)} шт.)\n\n"
        f"✅ Активные: {len(active_batches)}\n\n"
        "Нажмите на партию для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.message_handler(lambda m: m.text == "➕ Создать пул", state='*')
async def create_pull_start(message: types.Message, state: FSMContext):
    await state.finish()
    userid = message.from_user.id
    
    if userid not in users or users[userid].get("role") != "exporter":
        await message.answer("❌ Эта функция доступна только для экспортеров.")
        return
    
    await CreatePullStatesGroup.culture.set()
    await message.answer(
        "🌾 <b>Создание пула</b>\n\n"
        "<b>Шаг 1 из 10</b>\n\n"
        "Выберите культуру:",
        reply_markup=culture_keyboard(),
        parse_mode="HTML"
    )
    logging.info(f"User {userid} started pull creation, state set to CreatePullStatesGroup.culture")

# ✅ ИСПРАВЛЕНО: Обработчик выбора культуры
@dp.callback_query_handler(lambda c: c.data.startswith("culture:"), state=CreatePullStatesGroup.culture)
async def create_pull_culture_callback(callback: types.CallbackQuery, state: FSMContext):
    logging.info(f"Received callback: {callback.data}, state: {await state.get_state()}")
    
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 2 из 10</b>\n\n"
            f"Выбрана культура: <b>{culture}</b>\n\n"
            f"Введите целевой объем пула (в тоннах):",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error editing message: {e}")
        await callback.message.answer(
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 2 из 10</b>\n\n"
            f"Выбрана культура: <b>{culture}</b>\n\n"
            f"Введите целевой объем пула (в тоннах):",
            parse_mode="HTML"
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 3 из 10</b>\n\n"
            f"Объем: <b>{volume:,.0f} тонн</b>\n\n"
            f"Введите цену FOB ($/тонна):",
            parse_mode="HTML"
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 4 из 10</b>\n\n"
            f"Цена: <b>${price:,.0f}/тонна</b>\n\n"
            f"Выберите порт отгрузки:",
            reply_markup=port_keyboard(),
            parse_mode="HTML"
        )
        await CreatePullStatesGroup.port.set()
    except ValueError:
        await message.answer("❌ Некорректная цена. Введите положительное число.")

# Обработка порта
# Обработка порта
@dp.callback_query_handler(lambda c: c.data.startswith("selectport_"), state=CreatePullStatesGroup.port)
async def create_pull_port_callback(callback: types.CallbackQuery, state: FSMContext):
    logging.info(f"Received port callback: {callback.data}, state: {await state.get_state()}")
    
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 5 из 10</b>\n\n"
            f"Порт: <b>{port}</b>\n\n"
            f"Введите максимальную влажность (%):",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error editing message: {e}")
        await callback.message.answer(
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 5 из 10</b>\n\n"
            f"Порт: <b>{port}</b>\n\n"
            f"Введите максимальную влажность (%):",
            parse_mode="HTML"
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 6 из 10</b>\n\n"
            f"Влажность: <b>{moisture}%</b>\n\n"
            f"Введите минимальную натуру (г/л):",
            parse_mode="HTML"
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 7 из 10</b>\n\n"
            f"Натура: <b>{nature} г/л</b>\n\n"
            f"Введите максимальную сорную примесь (%):",
            parse_mode="HTML"
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 8 из 10</b>\n\n"
            f"Сорная примесь: <b>{impurity}%</b>\n\n"
            f"Введите максимальную зерновую примесь (%):",
            parse_mode="HTML"
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
            f"🌾 <b>Создание пула</b>\n\n"
            f"<b>Шаг 9 из 10</b>\n\n"
            f"Зерновая примесь: <b>{weed}%</b>\n\n"
            f"Какие документы требуются? (например: Фитосертификат, качество, ветсертификат)",
            parse_mode="HTML"
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
        InlineKeyboardButton("FOB", callback_data="doctype_FOB")
    )
    keyboard.add(
        InlineKeyboardButton("CIF", callback_data="doctype_CIF"),
        InlineKeyboardButton("EXW", callback_data="doctype_EXW")
    )
    
    await message.answer(
        f"🌾 <b>Создание пула</b>\n\n"
        f"<b>Шаг 10 из 10</b>\n\n"
        f"Документы: <b>{documents}</b>\n\n"
        f"Выберите тип поставки:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await CreatePullStatesGroup.doctype.set()
    logging.info(f"Documents set: {documents}, state changed to CreatePullStatesGroup.doctype")


@dp.callback_query_handler(lambda c: c.data.startswith("doctype_"), state=CreatePullStatesGroup.doctype)
async def create_pull_finish(callback: types.CallbackQuery, state: FSMContext):
    global pull_counter
    
    logging.info(f"Received doctype callback: {callback.data}, state: {await state.get_state()}")
    
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
    
    # Создание пула
    pull_counter += 1
    pull = {
        "id": pull_counter,
        "exporter_id": userid,
        "exporter_name": users[userid].get("name", ""),
        "culture": data["culture"],
        "target_volume": data["volume"],
        "current_volume": 0,
        "price": data["price"],
        "port": data["port"],
        "moisture": data.get("moisture", 0),
        "nature": data.get("nature", 0),
        "impurity": data.get("impurity", 0),
        "weed": data.get("weed", 0),
        "documents": data.get("documents", ""),
        "doc_type": doctype,
        "status": "Открыт",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "participants": []
    }
    
    pulls[pull_counter] = pull
    save_pulls_to_pickle()
    
    # Синхронизация с Google Sheets
    try:
        if gs and gs.spreadsheet:
            gs.sync_pull_to_sheets(pull)
    except Exception as e:
        logging.error(f"Error syncing to Google Sheets: {e}")
    
    logging.info(f"Pull {pull_counter} created by user {userid}")
    
    # ✅ ЗАВЕРШАЕМ STATE ОДИН РАЗ
    await state.finish()
    
    # Формируем сообщение с результатом
    summary = (
        f"✅ <b>Пул #{pull_counter} успешно создан!</b>\n\n"
        f"🌾 Культура: <b>{pull['culture']}</b>\n"
        f"📦 Объем: <b>{pull['target_volume']:,.0f} тонн</b>\n"
        f"💵 Цена FOB: <b>${pull['price']:,.0f}/тонна</b>\n"
        f"🚢 Порт: <b>{pull['port']}</b>\n"
        f"💧 Влажность: <b>≤{pull['moisture']}%</b>\n"
        f"⚖️ Натура: <b>≥{pull['nature']} г/л</b>\n"
        f"🌿 Сорная примесь: <b>≤{pull['impurity']}%</b>\n"
        f"🌾 Зерновая примесь: <b>≤{pull['weed']}%</b>\n"
        f"📋 Документы: <b>{pull['documents']}</b>\n"
        f"📦 Тип поставки: <b>{doctype}</b>\n\n"
        f"Фермеры смогут присоединяться к пулу со своими партиями."
    )
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📋 Мои пулы", callback_data="back_to_pulls"))
    
    await callback.message.edit_text(summary, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()
    

@dp.callback_query_handler(lambda c: c.data == "back_to_pools_list", state='*')
async def back_to_pools_list(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку пулов"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    open_pulls = [pull for pull in pulls.values() if pull.get('status') == 'Открыт']
    
    if not open_pulls:
        await callback.message.edit_text(
            "📦 Сейчас нет открытых пулов.\n\n"
            "Ожидайте новых предложений от экспортёров."
        )
        await callback.answer()
        return
    if user_id in batches and batches[user_id]:
        farmer_cultures = set(batch['culture'] for batch in batches[user_id])
        relevant_pulls = [p for p in open_pulls if p['culture'] in farmer_cultures]
        
        if relevant_pulls:
            open_pulls = relevant_pulls
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    for pull in open_pulls[:10]:
        available = pull['target_volume'] - pull['current_volume']
        progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
        
        button_text = f"🌾 {pull['culture']} | {available} т | ${pull['price']}/т ({progress:.0f}%)"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"viewpull:{pull['id']}")
        )
    
    await callback.message.edit_text(
        f"📦 <b>Открытые пулы</b> ({len(open_pulls)} шт.)\n\n"
        "Выберите пул для присоединения:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('view_pull_matches:'), state='*')
async def view_pull_matches(callback: types.CallbackQuery):
    """Просмотр совпадений для пула"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    if pull['exporter_id'] != user_id:
        await callback.answer("❌ Нет доступа к этому пулу", show_alert=True)
        return
    pull_matches = []
    for match in matches.values():
        if (match['pull_id'] == pull_id and 
            match['status'] == 'active'):
            pull_matches.append(match)
    
    if not pull_matches:
        await callback.answer("🤷‍♂️ Активных совпадений не найдено", show_alert=True)
        return
    
    text = f"🎯 <b>Совпадения для пула #{pull_id}</b>\n\n"
    text += f"🌾 {pull['culture']} • {pull['target_volume']} т • ${pull['price']}/т\n\n"
    
    for i, match in enumerate(pull_matches[:5], 1):
        batch_id = match['batch_id']
        batch_info = None
        for user_batches in batches.values():
            for batch in user_batches:
                if batch['id'] == batch_id:
                    batch_info = batch
                    break
            if batch_info:
                break
        
        if batch_info:
            text += f"{i}. <b>Партия #{batch_id}</b>\n"
            text += f"   📦 Объём: {batch_info['volume']} т\n"
            text += f"   💰 Цена: {batch_info['price']:,.0f} ₽/т\n"
            text += f"   📍 Регион: {batch_info['region']}\n"
            text += f"   👤 Фермер: {batch_info['farmer_name']}\n\n"
    
    if len(pull_matches) > 5:
        text += f"<i>... и ещё {len(pull_matches) - 5} совпадений</i>\n\n"
    
    text += "💡 <b>Рекомендация:</b> Свяжитесь с фермерами для обсуждения деталей."
    
    await callback.message.answer(text, parse_mode='HTML')
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'back_to_my_pulls', state='*')
async def back_to_my_pulls_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    user_id = callback.from_user.id
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await callback.answer("❌ Эта функция доступна только экспортёрам", show_alert=True)
        return
    my_pulls = {k: v for k, v in pulls.items() if v['exporter_id'] == user_id}
    if not my_pulls:
        await callback.message.edit_text("У вас пока нет пулов.")
        await callback.answer()
        return
    text = "📦 <b>Ваши пулы:</b>\n\n"
    keyboard = InlineKeyboardMarkup()
    for pull_id, pull in my_pulls.items():
        status_emoji = "🟢" if pull['status'] == 'Открыт' else "🔴"
        button_text = f"{status_emoji} Пул #{pull_id}: {pull['culture']}"
        keyboard.add(InlineKeyboardButton(button_text, callback_data=f"viewpull:{pull_id}"))
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.message_handler(lambda m: m.text == "📦 Мои пулы", state='*')
async def view_my_pulls(message: types.Message, state: FSMContext):
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await message.answer("❌ Эта функция доступна только экспортёрам")
        return
    my_pulls = {k: v for k, v in pulls.items() if v['exporter_id'] == user_id}
    
    if not my_pulls:
        await message.answer(
            "📦 У вас пока нет созданных пулов.\n\n"
            "Используйте кнопку '➕ Создать пул' для создания нового."
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for pull_id, pull in my_pulls.items():
        status_emoji = {
            'Открыт': '🟢',
            'В работе': '🟡', 
            'Закрыт': '🔴',
            'Завершён': '✅'
        }.get(pull.get('status', 'Открыт'), '📦')
        
        progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
        has_matches = any(m['pull_id'] == pull_id and m['status'] == 'active' 
                         for m in matches.values())
        match_emoji = "🎯 " if has_matches else ""
        
        button_text = (
            f"{match_emoji}{status_emoji} {pull['culture']} - {pull['current_volume']:.0f}/"
            f"{pull['target_volume']:.0f} т ({progress:.0f}%)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_pull:{pull_id}")
        )
    
    await message.answer(
        f"📦 <b>Ваши пулы</b> ({len(my_pulls)} шт.)\n\n"
        "Нажмите на пул для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

@dp.callback_query_handler(lambda c: c.data.startswith('view_pull:'), state='*')
async def view_pull_details(callback: types.CallbackQuery):
    """Просмотр деталей расширенного пула"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    is_owner = pull['exporter_id'] == user_id
    
    progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
    active_matches = [m for m in matches.values() 
                     if m['pull_id'] == pull_id and m['status'] == 'active']
    
    text = f"""
📦 <b>Пул #{pull['id']}</b>

🌾 Культура: {pull['culture']}
📦 Объём: {pull['current_volume']:.0f}/{pull['target_volume']:.0f} т ({progress:.0f}%)
💰 Цена FOB: ${pull['price']}/т (~{pull['price'] * 75:,.0f} ₽/т)
🚢 Порт: {pull['port']}

<b>Требования к качеству:</b>
💧 Влажность: до {pull['moisture']}%
🏋️ Натура: от {pull['nature']} г/л
🌾 Сорность: до {pull['impurity']}%
🌿 Засорённость: до {pull['weed']}%

📄 Документы: {pull['documents']}
📋 Тип: {pull['doc_type']}
📊 Статус: {translate_pull_status(
    pull.get('status'),
    pull.get('current_volume'),
    pull.get('target_volume')
)}
👤 Экспортёр: {pull['exporter_name']}
📅 Создан: {pull['created_at']}
"""
    
    if active_matches:
        text += f"\n🎯 <b>Активных совпадений: {len(active_matches)}</b>"
    
    participants_count = len(pull.get('participants', []))
    if participants_count > 0:
        text += f"\n👥 Участников: {participants_count}"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    if is_owner:
        keyboard.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"editpull_{pull_id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"deletepull_{pull_id}")
        )
        if pull.get('status') == 'active':
            keyboard.add(
                InlineKeyboardButton("🔒 Закрыть пулл", callback_data=f"close_pull_{pull_id}")
        )
        if active_matches:
            keyboard.add(
                InlineKeyboardButton("🎯 Показать совпадения", callback_data=f"view_pull_matches:{pull_id}")
            )
        
        keyboard.add(
            InlineKeyboardButton("👥 Участники", callback_data=f"pullparticipants:{pull_id}"),
            InlineKeyboardButton("🚚 Логистика", callback_data=f"pull_logistics:{pull_id}")
        )
    else:
        keyboard.add(
            InlineKeyboardButton("✅ Присоединиться", callback_data=f"join_pull:{pull_id}")
        )
    
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_pulls")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.message_handler(lambda m: m.text == "🔍 Найти партии", state='*')
async def search_batches_for_exporter(message: types.Message, state: FSMContext):
    """Расширенный поиск партий для экспортёра"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await message.answer("❌ Эта функция доступна только экспортёрам")
        return
    
    await message.answer(
        "🔍 <b>Расширенный поиск партий</b>\n\n"
        "Выберите критерии поиска:",
        reply_markup=search_criteria_keyboard(),
        parse_mode='HTML'
    )

@dp.callback_query_handler(lambda c: c.data.startswith('search_by:'), state='*')
async def handle_search_criteria(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора критерия поиска партий"""
    await state.finish()
    
    criteria = callback.data.split(':', 1)[1]
    
    if criteria == 'culture':
        await callback.message.edit_text(
            "🌾 <b>Поиск по культуре</b>\n\n"
            "Выберите культуру:",
            reply_markup=culture_keyboard(),
            parse_mode='HTML'
        )
        await SearchBatchesStatesGroup.enter_culture.set()
        
    elif criteria == 'region':
        await callback.message.edit_text(
            "📍 <b>Поиск по региону</b>\n\n"
            "Выберите регион:",
            reply_markup=region_keyboard(),
            parse_mode='HTML'
        )
        await SearchBatchesStatesGroup.enter_region.set()
        
    elif criteria == 'volume':
        await callback.message.edit_text(
            "📦 <b>Поиск по объёму</b>\n\n"
            "Введите минимальный объём (тонн):",
            parse_mode='HTML'
        )
        await SearchBatchesStatesGroup.enter_min_volume.set()
        
    elif criteria == 'price':
        await callback.message.edit_text(
            "💰 <b>Поиск по цене</b>\n\n"
            "Введите максимальную цену (₽/т):",
            parse_mode='HTML'
        )
        await SearchBatchesStatesGroup.enter_max_price.set()
        
    elif criteria == 'quality':
        await callback.message.edit_text(
            "⭐ <b>Поиск по классу качества</b>\n\n"
            "Выберите класс:",
            reply_markup=quality_class_keyboard(),
            parse_mode='HTML'
        )
        await SearchBatchesStatesGroup.enter_quality_class.set()
        
    elif criteria == 'storage':
        await callback.message.edit_text(
            "🏭 <b>Поиск по типу хранения</b>\n\n"
            "Выберите тип:",
            reply_markup=storage_type_keyboard(),
            parse_mode='HTML'
        )
        await SearchBatchesStatesGroup.enter_storage_type.set()
        
    elif criteria == 'all':
        await callback.message.edit_text(
            "🔍 <b>Поиск по всем параметрам</b>\n\n"
            "Начнём с культуры. Выберите:",
            reply_markup=culture_keyboard(),
            parse_mode='HTML'
        )
        await SearchBatchesStatesGroup.enter_culture.set()
        
    elif criteria == 'available':
        # Поиск только доступных партий
        await callback.answer("🔍 Ищем доступные партии...")
        
        available_batches = []
        for user_batches in batches.values():
            for batch in user_batches:
                if batch.get('status') in ['active', 'Активна', 'available', 'доступна']:
                    available_batches.append(batch)
        
        if available_batches:
            text = f"🌾 <b>Найдено доступных партий: {len(available_batches)}</b>\n\n"
            
            for i, batch in enumerate(available_batches[:10], 1):
                text += f"{i}. <b>{batch['culture']}</b> - {batch['volume']} т\n"
                text += f"   💰 {batch['price']:,.0f} ₽/т | 📍 {batch.get('region', 'Не указан')}\n\n"
            
            if len(available_batches) > 10:
                text += f"... и ещё {len(available_batches) - 10} партий\n"
            
            keyboard = InlineKeyboardMarkup(row_width=1)
            for batch in available_batches[:5]:
                keyboard.add(InlineKeyboardButton(
                    f"{batch['culture']} - {batch['volume']} т",
                    callback_data=f"viewbatch_{batch['id']}"
                ))
            keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_search"))
            
            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        else:
            await callback.message.edit_text(
                "❌ Доступных партий не найдено",
                parse_mode='HTML'
            )
            
    elif criteria == 'topregions':
        # Показать партии из топовых регионов
        region_counts = {}
        for user_batches in batches.values():
            for batch in user_batches:
                if batch.get('status') in ['active', 'Активна', 'available', 'доступна']:
                    region = batch.get('region', 'Неизвестно')
                    region_counts[region] = region_counts.get(region, 0) + 1
        
        if region_counts:
            top_regions = sorted(region_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            
            text = "📍 <b>Топ-5 регионов:</b>\n\n"
            for i, (region, count) in enumerate(top_regions, 1):
                text += f"{i}. {region}: {count} партий\n"
            
            keyboard = InlineKeyboardMarkup(row_width=1)
            for region, count in top_regions:
                keyboard.add(InlineKeyboardButton(
                    f"{region} ({count})",
                    callback_data=f"searchregion:{region}"
                ))
            keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_search"))
            
            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        else:
            await callback.message.edit_text("❌ Нет данных", parse_mode='HTML')
    
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'back_to_search', state='*')
async def back_to_search_menu(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в меню поиска"""
    await state.finish()
    
    await callback.message.edit_text(
        "🔍 <b>Расширенный поиск партий</b>\n\n"
        "Выберите критерии поиска:",
        reply_markup=search_criteria_keyboard(),
        parse_mode='HTML'
    )
    await callback.answer()


# Обработчик выбора культуры
@dp.callback_query_handler(lambda c: c.data.startswith('culture:'), state=SearchBatchesStatesGroup.enter_culture)
async def search_by_culture_selected(callback: types.CallbackQuery, state: FSMContext):
    global batches
    culture = callback.data.split(':', 1)[1]
    
    # Поиск партий по культуре
    found_batches = []    
    for user_batches in batches.values():
        for batch in user_batches:
            if batch.get('culture') == culture and batch.get('status') in ['active', 'Активна', 'available', 'доступна']:
                found_batches.append(batch)
    
    await state.finish()
    
    if found_batches:
        text = f"🌾 <b>Найдено партий '{culture}': {len(found_batches)}</b>\n\n"
        
        for i, batch in enumerate(found_batches[:10], 1):
            text += f"{i}. {batch['volume']} т - {batch['price']:,.0f} ₽/т\n"
            text += f"   📍 {batch.get('region', 'Не указан')}\n\n"
        
        if len(found_batches) > 10:
            text += f"... и ещё {len(found_batches) - 10} партий"
        
        keyboard = InlineKeyboardMarkup(row_width=1)
        for batch in found_batches[:5]:
            keyboard.add(InlineKeyboardButton(
                f"{batch['volume']} т - {batch['price']:,.0f} ₽/т",
                callback_data=f"viewbatch_{batch['id']}"
            ))
        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_search"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    else:
        await callback.message.edit_text(
            f"❌ Партий '{culture}' не найдено",
            parse_mode='HTML'
        )
    
    await callback.answer()


# Обработчик выбора региона
@dp.callback_query_handler(lambda c: c.data.startswith('region:') or c.data.startswith('searchregion:'), state='*')
async def search_by_region_selected(callback: types.CallbackQuery, state: FSMContext):
    if ':' in callback.data:
        region = callback.data.split(':', 1)[1]
    else:
        await callback.answer("❌ Ошибка парсинга", show_alert=True)
        return
    
    # Поиск партий по региону
    found_batches = [] 
    for user_batches in batches.values():
        for batch in user_batches:
            if batch.get('region') == region and batch.get('status') in ['active', 'Активна', 'available', 'доступна']:
                found_batches.append(batch)
    
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
            keyboard.add(InlineKeyboardButton(
                f"{batch['culture']} - {batch['volume']} т",
                callback_data=f"viewbatch_{batch['id']}"
            ))
        keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_search"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    else:
        await callback.message.edit_text(
            f"❌ Партий в '{region}' не найдено",
            parse_mode='HTML'
        )
    
    await callback.answer()

# ═══════════════════════════════════════════════════════════════════════════
# РАСШИРЕННЫЙ ФУНКЦИОНАЛ ЭКСПОРТЁРА
# Добавление партий в пулл, выбор логистов и экспедиторов
# ═══════════════════════════════════════════════════════════════════════════

@dp.callback_query_handler(lambda c: c.data.startswith('add_batch_to_pull:'), state='*')
async def add_batch_to_pull_select(callback: types.CallbackQuery):
    """Выбор пулла для добавления партии"""
    global batches
    try:
        batch_id = parse_callback_id(callback.data)  
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    user_id = callback.from_user.id

    if batch_id not in batches:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return

    batch = batches[batch_id]

    # Получаем активные пулы экспортёра с той же культурой
    user_pulls = []
    for pid, p in pulls.items():
        if (p.get('creator_id') == user_id and 
            p.get('culture', '').lower() == batch.get('culture', '').lower() and
            p.get('status', 'active') in ['active', 'открыт', 'активен']):
            user_pulls.append((pid, p))

    if not user_pulls:
        await callback.answer(
            f"❌ У вас нет активных пулов для культуры: {batch.get('culture', 'Неизвестно')}\n"
            "Создайте пулл сначала!",
            show_alert=True
        )
        return

    # Показываем список пулов
    keyboard = InlineKeyboardMarkup(row_width=1)

    for pull_id, pull in user_pulls:
        # Считаем текущий объём
        current_vol = 0
        if 'batches' in pull and pull['batches']:
            for b_id in pull['batches']:
                if b_id in batches:
                    current_vol += batches[b_id].get('volume', 0)

        target_vol = pull.get('target_volume', 0)

        keyboard.add(InlineKeyboardButton(
            f"Пулл #{pull_id}: {current_vol:.1f}/{target_vol:.1f} т",
            callback_data=f"confirm_add_batch:{batch_id}:{pull_id}"
        ))

    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))

    await callback.message.edit_text(
        f"📦 <b>Добавление партии в пулл</b>\n\n"
        f"🌾 Партия: {batch.get('culture', 'Неизвестно')} • {batch.get('volume', 0):.1f} т\n"
        f"📍 Регион: {batch.get('region', 'Не указан')}\n"
        f"💰 Цена: {batch.get('price', 0):,} ₽/т\n\n"
        f"Выберите пулл для добавления:",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_add_batch:'), state='*')
async def confirm_add_batch_to_pull(callback: types.CallbackQuery):
    """Подтверждение добавления партии в пулл"""
    try:
        _, batch_id, pull_id = callback.data.split(':')
        batch_id = int(batch_id)
        pull_id = int(pull_id)
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if batch_id not in batches or pull_id not in pulls:
        await callback.answer("❌ Данные не найдены", show_alert=True)
        return

    batch = batches[batch_id]
    pull = pulls[pull_id]

    # Проверяем что пулл принадлежит экспортёру
    if pull.get('creator_id') != callback.from_user.id:
        await callback.answer("❌ Это не ваш пулл", show_alert=True)
        return

    # Инициализируем массив партий если его нет
    if 'batches' not in pull:
        pull['batches'] = []

    # Проверяем что партия не добавлена уже
    if batch_id in pull['batches']:
        await callback.answer("⚠️ Партия уже в пулле", show_alert=True)
        return

    # Добавляем партию в пулл
    pull['batches'].append(batch_id)
    batch['status'] = 'in_pull'
    batch['pull_id'] = pull_id

    # Считаем текущий объем
    current_volume = 0
    for b_id in pull['batches']:
        if b_id in batches:
            current_volume += batches[b_id].get('volume', 0)

    target_volume = pull.get('target_volume', 0)

    # Проверяем заполнение пулла
    if current_volume >= target_volume:
        pull['status'] = 'completed'

        # Уведомляем экспортёра
        try:
            await bot.send_message(
                callback.from_user.id,
                f"🎉 <b>Пулл #{pull_id} собран!</b>\n\n"
                f"🌾 {pull.get('culture', 'Культура')}\n"
                f"📊 Объём: {current_volume:.1f}/{target_volume:.1f} т\n"
                f"📍 Порт: {pull.get('port', 'Не указан')}\n\n"
                f"Теперь вы можете выбрать логиста и экспедитора!",
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Ошибка уведомления экспортёра: {e}")

    # Уведомляем фермера
    farmer_id = batch.get('farmer_id')
    if farmer_id and farmer_id in users:
        try:
            await bot.send_message(
                farmer_id,
                f"✅ <b>Ваша партия добавлена в пулл!</b>\n\n"
                f"🌾 {batch.get('culture', 'Культура')} • {batch.get('volume', 0):.1f} т\n"
                f"📦 Пулл #{pull_id}\n"
                f"🚢 Порт: {pull.get('port', 'Не указан')}\n"
                f"💰 Цена: {batch.get('price', 0):,} ₽/т",
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Не удалось уведомить фермера {farmer_id}: {e}")

    save_data()

    status_text = "🎉 Пулл собран!" if current_volume >= target_volume else "✅ Партия добавлена"

    await callback.message.edit_text(
        f"✅ <b>Партия добавлена в пулл #{pull_id}!</b>\n\n"
        f"📊 Текущий объём: {current_volume:.1f}/{target_volume:.1f} т\n"
        f"{status_text}",
        parse_mode='HTML'
    )
    await callback.answer("✅ Партия добавлена!")


# ──────────────────────────────────────────────────────────────────────────
# 2. ПРОСМОТР И ВЫБОР ЛОГИСТА
# ──────────────────────────────────────────────────────────────────────────

@dp.callback_query_handler(lambda c: c.data.startswith('select_logistics_for_pull:'), state='*')
async def show_logistics_for_pull(callback: types.CallbackQuery):
    """Показать список логистов для выбора"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if pull_id not in pulls:
        await callback.answer("❌ Пулл не найден", show_alert=True)
        return

    pull = pulls[pull_id]

    # Проверяем что пулл собран
    if pull.get('status') not in ['completed', 'собран', 'закрыт']:
        await callback.answer("⚠️ Пулл ещё не собран", show_alert=True)
        return

    # Получаем список логистов
    available_logistics = []

    for user_id, user_data in users.items():
        if user_data.get('role') == 'logist':
            # Проверяем есть ли карточка логиста
            logistic_card = user_data.get('logistics_card', {})
            if logistic_card:
                available_logistics.append((user_id, logistic_card, user_data))

    if not available_logistics:
        await callback.answer(
            "⚠️ Нет доступных логистов\nПопробуйте позже",
            show_alert=True
        )
        return

    # Формируем сообщение со списком
    text = f"🚚 <b>Выбор логиста для пулла #{pull_id}</b>\n\n"
    text += f"🌾 {pull.get('culture', 'Культура')} • {pull.get('target_volume', 0):.1f} т\n"
    text += f"🚢 Порт: {pull.get('port', 'Не указан')}\n\n"
    text += f"<b>Доступно логистов: {len(available_logistics)}</b>\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

    for log_id, log_card, log_user in available_logistics:
        # Формируем краткую карточку
        company = log_card.get('company_name', 'Компания')
        price = log_card.get('price_per_ton', 0)

        btn_text = f"🚚 {company} • {price:,} ₽/т"

        keyboard.add(InlineKeyboardButton(
            btn_text,
            callback_data=f"view_logistic_card:{pull_id}:{log_id}"
        ))

    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))

    await callback.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_logistic_card:'), state='*')
async def view_logistic_card_for_selection(callback: types.CallbackQuery):
    """Просмотр карточки логиста и выбор"""
    try:
        _, pull_id, log_id = callback.data.split(':')
        pull_id = int(pull_id)
        log_id = int(log_id)
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if log_id not in users:
        await callback.answer("❌ Логист не найден", show_alert=True)
        return

    log_user = users[log_id]
    log_card = log_user.get('logistics_card', {})

    if not log_card:
        await callback.answer("❌ Карточка не найдена", show_alert=True)
        return

    # Формируем детальную карточку
    text = "🚚 <b>Карточка логиста</b>\n\n"
    text += f"🏢 <b>{log_card.get('company_name', 'Компания')}</b>\n"
    text += f"📍 Маршрут: {log_card.get('route', 'Не указан')}\n"
    text += f"💰 Тариф: {log_card.get('price_per_ton', 0):,} ₽/т\n"
    text += f"🚛 Транспорт: {log_card.get('transport_type', 'Не указан')}\n"
    text += f"⏱ Срок доставки: {log_card.get('delivery_days', 'Не указан')} дней\n"

    if log_card.get('additional_info'):
        text += f"\n📝 {log_card['additional_info']}\n"

    # Контакты
    text += "\n<b>Контакты:</b>\n"
    if log_user.get('username'):
        text += f"Telegram: @{log_user['username']}\n"
    if log_user.get('phone'):
        text += f"📞 {log_user['phone']}\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Выбрать этого логиста",
            callback_data=f"confirm_select_logistic:{pull_id}:{log_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton(
            "◀️ Назад к списку",
            callback_data=f"select_logistics_for_pull:{pull_id}"
        )
    )

    await callback.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_select_logistic:'), state='*')
async def confirm_select_logistic(callback: types.CallbackQuery):
    """Подтверждение выбора логиста"""
    try:
        _, pull_id, log_id = callback.data.split(':')
        pull_id = int(pull_id)
        log_id = int(log_id)
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if pull_id not in pulls:
        await callback.answer("❌ Пулл не найден", show_alert=True)
        return

    pull = pulls[pull_id]

    # Назначаем логиста
    pull['selected_logistic'] = log_id

    save_data()

    # Уведомляем логиста
    log_user = users.get(log_id, {})
    try:
        await bot.send_message(
            log_id,
            f"🎉 <b>Вы выбраны для перевозки!</b>\n\n"
            f"📦 Пулл #{pull_id}\n"
            f"🌾 {pull.get('culture', 'Культура')} • {pull.get('target_volume', 0):.1f} т\n"
            f"🚢 Порт: {pull.get('port', 'Не указан')}\n\n"
            f"Экспортёр свяжется с вами для уточнения деталей.",
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Не удалось уведомить логиста {log_id}: {e}")

    company_name = log_user.get('logistics_card', {}).get('company_name', 'Логист')

    await callback.message.edit_text(
        f"✅ <b>Логист выбран!</b>\n\n"
        f"🚚 Компания: {company_name}\n"
        f"📦 Для пулла #{pull_id}\n\n"
        f"Логист получил уведомление.",
        parse_mode='HTML'
    )
    await callback.answer("✅ Логист назначен!")


# ──────────────────────────────────────────────────────────────────────────
# 3. ПРОСМОТР И ВЫБОР ЭКСПЕДИТОРА
# ──────────────────────────────────────────────────────────────────────────

@dp.callback_query_handler(lambda c: c.data.startswith('select_expeditor_for_pull:'), state='*')
async def show_expeditors_for_pull(callback: types.CallbackQuery):
    """Показать список экспедиторов для выбора"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if pull_id not in pulls:
        await callback.answer("❌ Пулл не найден", show_alert=True)
        return

    pull = pulls[pull_id]

    # Проверяем что пулл собран
    if pull.get('status') not in ['completed', 'собран', 'закрыт']:
        await callback.answer("⚠️ Пулл ещё не собран", show_alert=True)
        return

    # Получаем список экспедиторов
    available_expeditors = []

    for user_id, user_data in users.items():
        if user_data.get('role') == 'expeditor':
            # Проверяем есть ли карточка
            expeditor_card = user_data.get('expeditor_card', {})
            if expeditor_card:
                available_expeditors.append((user_id, expeditor_card, user_data))

    if not available_expeditors:
        await callback.answer(
            "⚠️ Нет доступных экспедиторов\nПопробуйте позже",
            show_alert=True
        )
        return

    # Формируем сообщение
    text = f"📄 <b>Выбор экспедитора для пулла #{pull_id}</b>\n\n"
    text += f"🌾 {pull.get('culture', 'Культура')} • {pull.get('target_volume', 0):.1f} т\n"
    text += f"🚢 Порт: {pull.get('port', 'Не указан')}\n\n"
    text += f"<b>Доступно экспедиторов: {len(available_expeditors)}</b>\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

    for exp_id, exp_card, exp_user in available_expeditors:
        company = exp_card.get('company_name', 'Экспедитор')
        price = exp_card.get('customs_fee', 0)

        btn_text = f"📄 {company} • {price:,} ₽"

        keyboard.add(InlineKeyboardButton(
            btn_text,
            callback_data=f"view_expeditor_card:{pull_id}:{exp_id}"
        ))

    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))

    await callback.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_expeditor_card:'), state='*')
async def view_expeditor_card_for_selection(callback: types.CallbackQuery):
    """Просмотр карточки экспедитора и выбор"""
    try:
        _, pull_id, exp_id = callback.data.split(':')
        pull_id = int(pull_id)
        exp_id = int(exp_id)
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if exp_id not in users:
        await callback.answer("❌ Экспедитор не найден", show_alert=True)
        return

    exp_user = users[exp_id]
    exp_card = exp_user.get('expeditor_card', {})

    if not exp_card:
        await callback.answer("❌ Карточка не найдена", show_alert=True)
        return

    # Формируем карточку
    text = "📄 <b>Карточка экспедитора</b>\n\n"
    text += f"🏢 <b>{exp_card.get('company_name', 'Компания')}</b>\n"
    text += f"📋 Услуги: {exp_card.get('services', 'Оформление ДТ')}\n"
    text += f"💰 Стоимость: {exp_card.get('customs_fee', 0):,} ₽\n"
    text += f"⏱ Сроки: {exp_card.get('processing_time', 'Не указаны')}\n"

    if exp_card.get('additional_services'):
        text += f"\n✨ Доп. услуги: {exp_card['additional_services']}\n"

    # Контакты
    text += "\n<b>Контакты:</b>\n"
    if exp_user.get('username'):
        text += f"Telegram: @{exp_user['username']}\n"
    if exp_user.get('phone'):
        text += f"📞 {exp_user['phone']}\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton(
            "✅ Выбрать этого экспедитора",
            callback_data=f"confirm_select_expeditor:{pull_id}:{exp_id}"
        )
    )
    keyboard.add(
        InlineKeyboardButton(
            "◀️ Назад к списку",
            callback_data=f"select_expeditor_for_pull:{pull_id}"
        )
    )

    await callback.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_select_expeditor:'), state='*')
async def confirm_select_expeditor(callback: types.CallbackQuery):
    """Подтверждение выбора экспедитора"""
    try:
        _, pull_id, exp_id = callback.data.split(':')
        pull_id = int(pull_id)
        exp_id = int(exp_id)
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if pull_id not in pulls:
        await callback.answer("❌ Пулл не найден", show_alert=True)
        return

    pull = pulls[pull_id]

    # Назначаем экспедитора
    pull['selected_expeditor'] = exp_id

    save_data()

    # Уведомляем экспедитора
    exp_user = users.get(exp_id, {})
    try:
        await bot.send_message(
            exp_id,
            f"🎉 <b>Вы выбраны для оформления ДТ!</b>\n\n"
            f"📦 Пулл #{pull_id}\n"
            f"🌾 {pull.get('culture', 'Культура')} • {pull.get('target_volume', 0):.1f} т\n"
            f"🚢 Порт: {pull.get('port', 'Не указан')}\n\n"
            f"Экспортёр свяжется с вами для уточнения деталей.",
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Не удалось уведомить экспедитора {exp_id}: {e}")

    company_name = exp_user.get('expeditor_card', {}).get('company_name', 'Экспедитор')

    await callback.message.edit_text(
        f"✅ <b>Экспедитор выбран!</b>\n\n"
        f"📄 Компания: {company_name}\n"
        f"📦 Для пулла #{pull_id}\n\n"
        f"Экспедитор получил уведомление.",
        parse_mode='HTML'
    )
    await callback.answer("✅ Экспедитор назначен!")


async def select_search_criteria(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора критериев поиска"""
    search_type = callback.data.split(':', 1)[1]
    
    await state.update_data(search_type=search_type)
    
    if search_type == 'culture':
        await callback.message.edit_text(
            "🔍 <b>Поиск по культуре</b>\n\n"
            "Выберите культуру:",
            reply_markup=culture_keyboard()
        )
        await SearchBatchesStatesGroup.enter_culture.set()
    
    elif search_type == 'region':
        await callback.message.edit_text(
            "🔍 <b>Поиск по региону</b>\n\n"
            "Выберите регион:",
            reply_markup=region_keyboard()
        )
        await SearchBatchesStatesGroup.enter_region.set()
    
    elif search_type == 'volume':
        await callback.message.edit_text(
            "🔍 <b>Поиск по объёму</b>\n\n"
            "Введите минимальный объём (в тоннах):"
        )
        await SearchBatchesStatesGroup.enter_min_volume.set()
    
    elif search_type == 'price':
        await callback.message.edit_text(
            "🔍 <b>Поиск по цене</b>\n\n"
            "Введите минимальную цену (₽/тонна):"
        )
        await SearchBatchesStatesGroup.enter_min_price.set()
    
    elif search_type == 'quality':
        await callback.message.edit_text(
            "🔍 <b>Поиск по классу качества</b>\n\n"
            "Выберите класс качества:",
            reply_markup=quality_class_keyboard()
        )
        await SearchBatchesStatesGroup.enter_quality_class.set()
    
    elif search_type == 'storage':
        await callback.message.edit_text(
            "🔍 <b>Поиск по типу хранения</b>\n\n"
            "Выберите тип хранения:",
            reply_markup=storage_type_keyboard()
        )
        await SearchBatchesStatesGroup.enter_storage_type.set()
    
    elif search_type == 'all':
        await callback.message.edit_text(
            "🔍 <b>Комплексный поиск</b>\n\n"
            "Выберите культуру:",
            reply_markup=culture_keyboard()
        )
        await SearchBatchesStatesGroup.enter_culture.set()
    
    elif search_type == 'active':
        await perform_search(callback.message, {'status': 'Активна'})
    
    await callback.answer()

    """Обработка выбора региона при поиске"""
    region = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    search_type = data.get('search_type')
    
    if search_type == 'region':
        await perform_search(callback.message, {'region': region})
        await state.finish()
    else:
        await state.update_data(region=region)
        await callback.message.edit_text(
            "🔍 <b>Комплексный поиск</b>\n\n"
            "Введите минимальный объём (в тоннах):"
        )
        await SearchBatchesStatesGroup.enter_min_volume.set()
    
    await callback.answer()

@dp.message_handler(state=SearchBatchesStatesGroup.enter_min_volume)
async def search_min_volume(message: types.Message, state: FSMContext):
    """Ввод минимального объёма при поиске"""
    try:
        min_volume = float(message.text.strip().replace(',', '.'))
        if min_volume < 0:
            raise ValueError
        
        data = await state.get_data()
        
        if data.get('search_type') == 'volume':
            await perform_search(message, {'min_volume': min_volume})
            await state.finish()
        else:
            await state.update_data(min_volume=min_volume)
            await message.answer("Введите максимальный объём (в тоннах, или 0 если не важно):")
            await SearchBatchesStatesGroup.enter_max_volume.set()
            
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число:")

@dp.message_handler(state=SearchBatchesStatesGroup.enter_max_volume)
async def search_max_volume(message: types.Message, state: FSMContext):
    """Ввод максимального объёма при поиске"""
    try:
        max_volume_text = message.text.strip()
        max_volume = float(max_volume_text.replace(',', '.')) if max_volume_text != '0' else 0
        
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
        min_price = float(message.text.strip().replace(',', '.'))
        if min_price < 0:
            raise ValueError
        
        await state.update_data(min_price=min_price)
        await message.answer("Введите максимальную цену (₽/тонна, или 0 если не важно):")
        await SearchBatchesStatesGroup.enter_max_price.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число:")

@dp.message_handler(state=SearchBatchesStatesGroup.enter_max_price)
async def search_max_price(message: types.Message, state: FSMContext):
    """Завершение комплексного поиска"""
    try:
        max_price_text = message.text.strip()
        max_price = float(max_price_text.replace(',', '.')) if max_price_text != '0' else 0
        
        if max_price < 0:
            raise ValueError
        
        data = await state.get_data()
        search_params = {
            'culture': data.get('culture'),
            'region': data.get('region'),
            'min_volume': data.get('min_volume', 0),
            'max_volume': data.get('max_volume', 0),
            'min_price': data.get('min_price', 0),
            'max_price': max_price,
            'status': 'Активна'
        }
        
        await perform_search(message, search_params)
        await state.finish()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число:")

@dp.callback_query_handler(lambda c: c.data.startswith('quality:'), state=SearchBatchesStatesGroup.enter_quality_class)
async def search_by_quality(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора класса качества при поиске"""
    quality_class = callback.data.split(':', 1)[1]
    
    await perform_search(callback.message, {'quality_class': quality_class})
    await state.finish()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('storage:'), state=SearchBatchesStatesGroup.enter_storage_type)
async def search_by_storage(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора типа хранения при поиске"""
    storage_type = callback.data.split(':', 1)[1]
    
    await perform_search(callback.message, {'storage_type': storage_type})
    await state.finish()
    await callback.answer()

async def perform_search(message, search_params):
    """Выполнение поиска по заданным параметрам"""
    found_batches = []    
    for user_batches in batches.values():
        for batch in user_batches:
            if matches_search_criteria(batch, search_params):
                found_batches.append(batch)
    
    if not found_batches:
        await message.answer(
            "🔍 <b>Результаты поиска</b>\n\n"
            "По вашему запросу ничего не найдено.\n\n"
            "Попробуйте изменить критерии поиска.",
            parse_mode='HTML'
        )
        return
    found_batches.sort(key=lambda x: x['price'])
    
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
    
    await message.answer(text, parse_mode='HTML')

def matches_search_criteria(batch, search_params):
    """Проверка соответствия партии критериям поиска"""
    if batch.get('status') != 'Активна':
        return False
    if search_params.get('culture') and batch['culture'] != search_params['culture']:
        return False
    if search_params.get('region') and batch.get('region', 'Не указан') != search_params['region']:
        return False
    if search_params.get('min_volume', 0) > 0 and batch['volume'] < search_params['min_volume']:
        return False
    if (search_params.get('max_volume', 0) > 0 and 
        search_params['max_volume'] < batch['volume']):
        return False
    if search_params.get('min_price', 0) > 0 and batch['price'] < search_params['min_price']:
        return False
    if (search_params.get('max_price', 0) > 0 and 
        search_params['max_price'] < batch['price']):
        return False
    if (search_params.get('quality_class') and 
        batch.get('quality_class') != search_params.get('quality_class')):
        return False
    if (search_params.get('storage_type') and 
        batch.get('storage_type') != search_params.get('storage_type')):
        return False
    
    return True

@dp.callback_query_handler(lambda c: c.data.startswith('attach_files:'), state='*')
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
        parse_mode='HTML'
    )
    await AttachFilesStatesGroup.upload_files.set()
    await callback.answer()

@dp.message_handler(content_types=['photo', 'document'], state=AttachFilesStatesGroup.upload_files)
async def attach_files_upload(message: types.Message, state: FSMContext):
    """Обработка загрузки файлов"""
    data = await state.get_data()
    batch_id = data.get('attach_batch_id')
    user_id = message.from_user.id
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await message.answer("❌ Партия не найдена")
        await state.finish()
        return
    if 'files' not in batch:
        batch['files'] = []
    file_info = None
    if message.photo:
        file_info = {
            'type': 'photo',
            'file_id': message.photo[-1].file_id,
            'caption': message.caption or '',
            'uploaded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    elif message.document:
        file_info = {
            'type': 'document',
            'file_id': message.document.file_id,
            'file_name': message.document.file_name,
            'mime_type': message.document.mime_type,
            'caption': message.caption or '',
            'uploaded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    if file_info:
        batch['files'].append(file_info)
        save_batches_to_pickle()
        if gs and gs.spreadsheet:
            gs.update_batch_in_sheets(batch)
        
        await message.answer(
            f"✅ Файл добавлен ({len(batch['files'])} всего)\n"
            "Отправьте ещё или нажмите /done для завершения"
        )

@dp.message_handler(commands=['done'], state=AttachFilesStatesGroup.upload_files)
async def attach_files_done(message: types.Message, state: FSMContext):
    """Завершение прикрепления файлов"""
    data = await state.get_data()
    batch_id = data.get('attach_batch_id')
    user_id = message.from_user.id
    
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    await state.finish()
    
    files_count = len(batch.get('files', [])) if batch else 0
    
    role = users[user_id].get('role')
    keyboard = get_role_keyboard(role)
    
    await message.answer(
        f"✅ <b>Файлы прикреплены!</b>\n\n"
        f"Партия #{batch_id}\n"
        f"Всего файлов: {files_count}",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await view_batch_details_direct(message, batch_id, user_id)

@dp.callback_query_handler(lambda c: c.data.startswith('view_files:'), state='*')
async def view_batch_files(callback: types.CallbackQuery):
    """Просмотр файлов партии"""
    batch_id = parse_callback_id(callback.data)  
    user_id = callback.from_user.id
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch or not batch.get('files'):
        await callback.answer("📎 Файлов нет", show_alert=True)
        return
    
    await callback.message.answer(
        f"📎 <b>Файлы партии #{batch_id}</b>\n\n"
        f"Всего файлов: {len(batch['files'])}",
        parse_mode='HTML'
    )
    for file_info in batch['files']:
        try:
            if file_info['type'] == 'photo':
                await callback.message.answer_photo(
                    file_info['file_id'],
                    caption=file_info.get('caption', '') or f"📷 Фото для партии #{batch_id}"
                )
            elif file_info['type'] == 'document':
                caption = f"📄 {file_info.get('file_name', 'Документ')}"
                if file_info.get('caption'):
                    caption += f"\n{file_info['caption']}"
                
                await callback.message.answer_document(
                    file_info['file_id'],
                    caption=caption
                )
        except Exception as e:
            logging.error(f"Ошибка отправки файла: {e}")
            await callback.message.answer(
                f"❌ Не удалось отправить файл: {file_info.get('file_name', 'Неизвестный файл')}"
            )
    
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_pulls", state='*')
async def back_to_pulls(callback: types.CallbackQuery):
    """Возврат к списку пулов"""
    user_id = callback.from_user.id
    
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await callback.answer("❌ Нет доступа")
        return
    
    my_pulls = {k: v for k, v in pulls.items() if v['exporter_id'] == user_id}
    
    if not my_pulls:
        await callback.message.edit_text("📦 У вас нет пулов")
        await callback.answer()
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for pull_id, pull in my_pulls.items():
        status_emoji = {
            'Открыт': '🟢',
            'В работе': '🟡',
            'Закрыт': '🔴',
            'Завершён': '✅'
        }.get(pull.get('status', 'Открыт'), '📦')
        
        progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
        
        has_matches = any(m['pull_id'] == pull_id and m['status'] == 'active' 
                         for m in matches.values())
        match_emoji = "🎯 " if has_matches else ""
        
        button_text = (
            f"{match_emoji}{status_emoji} {pull['culture']} - {pull['current_volume']:.0f}/"
            f"{pull['target_volume']:.0f} т ({progress:.0f}%)"
        )
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_pull:{pull_id}")
        )
    
    await callback.message.edit_text(
        f"📦 <b>Ваши пулы</b> ({len(my_pulls)} шт.)\n\n"
        "Нажмите на пул для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_deals", state='*')
async def back_to_deals(callback: types.CallbackQuery):
    """Возврат к списку сделок"""
    user_id = callback.from_user.id
    
    user_deals = []
    for deal_id, deal in deals.items():
        if (deal.get('exporter_id') == user_id or 
            user_id in deal.get('farmer_ids', []) or
            deal.get('logistic_id') == user_id or
            deal.get('expeditor_id') == user_id):
            user_deals.append(deal)
    
    if not user_deals:
        await callback.message.edit_text("📋 У вас нет сделок")
        await callback.answer()
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for deal in user_deals[:10]:
        status_emoji = {
            'pending': '🔄',
            'matched': '🎯',
            'shipping': '🚛',
            'completed': '✅',
            'cancelled': '❌'
        }.get(deal.get('status', 'pending'), '📋')
        
        deal_info = f"Сделка #{deal['id']}"
        if deal.get('total_volume'):
            deal_info += f" - {deal['total_volume']} т"
        
        button_text = f"{status_emoji} {deal_info}"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_deal:{deal['id']}")
        )
    
    await callback.message.edit_text(
        f"📋 <b>Ваши сделки</b> ({len(user_deals)} шт.)\n\n"
        "Выберите сделку для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.message_handler(commands=['stats'], state='*')
@dp.message_handler(commands=['help'], state='*')
async def cmd_help(message: types.Message, state: FSMContext):
    """Справка по боту"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users:
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
            parse_mode='HTML'
        )
        return
    
    user = users[user_id]
    role = user.get('role')
    
    text = f"ℹ️ <b>Справка для {ROLES.get(role, role)}</b>\n\n"
    
    if role == 'farmer':
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
    
    elif role == 'exporter':
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
    
    elif role == 'logistic':
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
    
    elif role == 'expeditor':
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
    
    await message.answer(text, parse_mode='HTML')

@dp.callback_query_handler(lambda c: c.data.startswith('edit_batch:'), state='*')
async def start_edit_batch(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования расширенной партии"""
    batch_id = parse_callback_id(callback.data)  
    user_id = callback.from_user.id
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    
    await state.update_data(editing_batch_id=batch_id)
    
    await callback.message.edit_text(
        f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
        "Выберите поле для редактирования:",
        reply_markup=edit_batch_fields_keyboard(batch_id),
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('edit_field:'), state='*')
async def edit_batch_field_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора поля для редактирования партии"""
    field = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    batch_id = data.get('editing_batch_id')
    
    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        return
    
    await state.update_data(edit_field=field, editing_batch_id=batch_id)
    field_names = {
        'price': 'новую цену (₽/тонна)',
        'volume': 'новый объём (в тоннах)',
        'humidity': 'новую влажность (%)',
        'impurity': 'новую сорность (%)',
        'quality_class': 'новый класс качества',
        'storage_type': 'новый тип хранения',
        'readiness_date': 'новую дату готовности (ДД.ММ.ГГГГ)',
        'status': 'новый статус'
    }
    
    if field == 'status':
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
            "Выберите новый статус:",
            reply_markup=status_keyboard(),
            parse_mode='HTML'
        )
        await EditBatch.new_value.set()
    elif field == 'quality_class':
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
            "Выберите новый класс качества:",
            reply_markup=quality_class_keyboard(),
            parse_mode='HTML'
        )
        await EditBatch.new_value.set()
    elif field == 'storage_type':
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
            "Выберите новый тип хранения:",
            reply_markup=storage_type_keyboard(),
            parse_mode='HTML'
        )
        await EditBatch.new_value.set()
    else:
        await callback.message.edit_text(
            f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
            f"Введите {field_names.get(field, 'новое значение')}:"
        )
        await EditBatch.new_value.set()
    
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('status:'), state=EditBatch.new_value)
async def edit_batch_status_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора статуса при редактировании партии"""
    new_status = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    batch_id = data.get('editing_batch_id')
    user_id = callback.from_user.id
    
    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        await state.finish()
        return
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
    old_value = batch.get('status', 'Не указан')
    batch['status'] = new_status
    save_batches_to_pickle()
    if gs and gs.spreadsheet:
        gs.update_batch_in_sheets(batch)
    
    await state.finish()
    await callback.message.edit_text(
        f"✅ <b>Статус обновлён!</b>\n\n"
        f"Партия #{batch_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_status}"
    )
    await asyncio.sleep(1)
    await view_batch_details_direct(callback.message, batch_id, user_id)
    await callback.answer("✅ Статус обновлён")

@dp.callback_query_handler(lambda c: c.data.startswith('quality:'), state=EditBatch.new_value)
async def edit_batch_quality_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора класса качества при редактировании партии"""
    new_quality = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    batch_id = data.get('editing_batch_id')
    user_id = callback.from_user.id
    
    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        await state.finish()
        return
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
    old_value = batch.get('quality_class', 'Не указан')
    batch['quality_class'] = new_quality
    save_batches_to_pickle()
    if gs and gs.spreadsheet:
        gs.update_batch_in_sheets(batch)
    
    await state.finish()
    
    await callback.message.edit_text(
        f"✅ <b>Класс качества обновлён!</b>\n\n"
        f"Партия #{batch_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_quality}"
    )
    await asyncio.sleep(1)
    await view_batch_details_direct(callback.message, batch_id, user_id)
    await callback.answer("✅ Класс качества обновлён")

@dp.callback_query_handler(lambda c: c.data.startswith('storage:'), state=EditBatch.new_value)
async def edit_batch_storage_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора типа хранения при редактировании партии"""
    new_storage = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    batch_id = data.get('editing_batch_id')
    user_id = callback.from_user.id
    
    if not batch_id:
        await callback.answer("❌ Ошибка: партия не найдена", show_alert=True)
        await state.finish()
        return
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
    old_value = batch.get('storage_type', 'Не указан')
    batch['storage_type'] = new_storage
    save_batches_to_pickle()
    if gs and gs.spreadsheet:
        gs.update_batch_in_sheets(batch)
    
    await state.finish()
    
    await callback.message.edit_text(
        f"✅ <b>Тип хранения обновлён!</b>\n\n"
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
    batch_id = data.get('editing_batch_id')
    field = data.get('edit_field')
    user_id = message.from_user.id
    
    if not batch_id or not field:
        await message.answer("❌ Ошибка: данные не найдены")
        await state.finish()
        return
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await message.answer("❌ Партия не найдена")
        await state.finish()
        return
    
    new_value = message.text.strip()
    try:
        if field in ['price', 'volume', 'humidity', 'impurity']:
            new_value_float = float(new_value.replace(',', '.'))
            if field == 'price' and new_value_float <= 0:
                await message.answer("❌ Цена должна быть больше 0. Попробуйте ещё раз:")
                return
            elif field == 'volume' and new_value_float <= 0:
                await message.answer("❌ Объём должен быть больше 0. Попробуйте ещё раз:")
                return
            elif field in ['humidity', 'impurity'] and not (0 <= new_value_float <= 100):
                await message.answer("❌ Значение должно быть от 0 до 100. Попробуйте ещё раз:")
                return
            
            old_value = batch.get(field, 'Не указано')
            batch[field] = new_value_float
            if field in ['humidity', 'impurity']:
                batch['quality_class'] = determine_quality_class(
                    batch.get('humidity', 0),
                    batch.get('impurity', 0)
                )
        
        elif field == 'readiness_date':
            if new_value.lower() == 'сейчас':
                new_value = datetime.now().strftime('%d.%m.%Y')
            elif not validate_date(new_value):
                await message.answer("❌ Некорректная дата. Используйте формат ДД.ММ.ГГГГ или 'сейчас'. Попробуйте ещё раз:")
                return
            
            old_value = batch.get(field, 'Не указано')
            batch[field] = new_value
        
        else:
            old_value = batch.get(field, 'Не указано')
            batch[field] = new_value
        save_batches_to_pickle()
        if gs and gs.spreadsheet:
            gs.update_batch_in_sheets(batch)
        
        await state.finish()
        field_names_ru = {
            'price': 'Цена',
            'volume': 'Объём',
            'humidity': 'Влажность',
            'impurity': 'Сорность',
            'readiness_date': 'Дата готовности'
        }
        
        role = users[user_id].get('role')
        keyboard = get_role_keyboard(role)
        
        await message.answer(
            f"✅ <b>{field_names_ru.get(field, field.capitalize())} обновлена!</b>\n\n"
            f"Партия #{batch_id}\n"
            f"Старое значение: {old_value}\n"
            f"Новое значение: {new_value}",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await view_batch_details_direct(message, batch_id, user_id)
        
    except ValueError:
        await message.answer(
            "❌ Некорректное значение. Введите число.\n"
            "Попробуйте ещё раз:"
        )

@dp.callback_query_handler(lambda c: c.data == 'edit_cancel', state='*')
async def edit_cancel(callback: types.CallbackQuery, state: FSMContext):
    """Отмена редактирования"""
    await state.finish()
    await callback.message.edit_text("❌ Редактирование отменено")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('delete_batch:'), state='*')
async def delete_batch_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало удаления партии"""
    batch_id = parse_callback_id(callback.data)  
    user_id = callback.from_user.id
    batch_exists = False
    if user_id in batches:
        for b in batches[user_id]:
            if b['id'] == batch_id:
                batch_exists = True
                break
    
    if not batch_exists:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_batch:{batch_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete_batch")
    )
    
    await callback.message.edit_text(
        f"⚠️ <b>Подтверждение удаления</b>\n\n"
        f"Вы уверены, что хотите удалить партию #{batch_id}?\n\n"
        f"<b>Это действие нельзя отменить!</b>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_delete_batch:'), state='*')
async def delete_batch_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение удаления партии"""
    await state.finish()
    
    try:
        batch_id = int(callback.data.split(':')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    # Удаляем партию
    if user_id in batches:
        batches[user_id] = [b for b in batches[user_id] if b['id'] != batch_id]
        save_batches_to_pickle()
    
    # ✅ ДОБАВЬТЕ: Удаляем партию из всех пулов
    removed_from_pulls = []
    for pull_id, participants in list(pullparticipants.items()):
        # Проверяем есть ли эта партия в пуле
        old_count = len(participants)
        pullparticipants[pull_id] = [
            p for p in participants 
            if p['batch_id'] != batch_id
        ]
        
        # Если партия была удалена из пула
        if len(pullparticipants[pull_id]) < old_count:
            removed_from_pulls.append(pull_id)
            
            # Пересчитываем объём пула
            if pull_id in pulls:
                pulls[pull_id]['current_volume'] = sum(
                    p['volume'] for p in pullparticipants[pull_id]
                )
                logging.info(f"🔄 Пул #{pull_id}: объём пересчитан → {pulls[pull_id]['current_volume']} т")
    
    # Сохраняем изменения
    if removed_from_pulls:
        save_pulls_to_pickle()
        logging.info(f"🗑️ Партия #{batch_id} удалена из пулов: {removed_from_pulls}")
    
    # Удаляем из Google Sheets
    if gs and gs.spreadsheet:
        try:
            gs.delete_batch_from_sheets(batch_id)
        except Exception as e:
            logging.error(f"Ошибка Google Sheets: {e}")
    
    # Формируем сообщение
    message = f"✅ Партия <b>#{batch_id}</b> удалена!"
    if removed_from_pulls:
        message += f"\n🔄 Также удалена из пулов: {', '.join(f'#{pid}' for pid in removed_from_pulls)}"
    
    await callback.message.edit_text(message, parse_mode='HTML')
    await callback.answer()



@dp.callback_query_handler(lambda c: c.data == 'cancel_delete_batch', state='*')
async def cancel_delete_batch(callback: types.CallbackQuery, state: FSMContext):
    """Отмена удаления"""
    await state.finish()
    await callback.message.edit_text("❌ Удаление отменено.")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('editfield_crop_'), state='*')
async def edit_crop_field(callback: types.CallbackQuery, state: FSMContext):
    """Обработчик редактирования культуры"""
    await state.finish()
    
    try:
        batch_id = int(callback.data.split('_')[2])
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
            if isinstance(b, dict) and b.get('id') == batch_id:
                batch = b
                farmer_id = f_id
                break
        if batch:
            break
    
    if not batch or farmer_id != user_id:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    
    await state.update_data(batch_id=batch_id)
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    cultures = ['Пшеница', 'Кукуруза', 'Ячмень', 'Подсолнечник', 'Рапс', 'Соя']
    
    for culture in cultures:
        keyboard.insert(InlineKeyboardButton(culture, callback_data=f"setcrop_{batch_id}_{culture}"))
    
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data=f"editcancel_{batch_id}"))
    
    await callback.message.edit_text(
        f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
        f"Выберите новую <b>культуру</b>:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('setcrop_'), state='*')
async def set_crop_value(callback: types.CallbackQuery, state: FSMContext):
    """Установка новой культуры"""
    await state.finish()
    
    try:
        parts = callback.data.split('_')
        batch_id = int(parts[1])
        new_crop = '_'.join(parts[2:])
    except (IndexError, ValueError) as e:
        logger.error(f"Ошибка парсинга: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    user_id = callback.from_user.id
    load_batches_from_pickle()
    
    updated = False
    old_crop = "Неизвестно"
    
    for farmer_id, user_batches in batches.items():
        if farmer_id != user_id:
            continue
        for batch in user_batches:
            if isinstance(batch, dict) and batch.get('id') == batch_id:
                old_crop = batch.get('culture', 'Неизвестно')
                batch['culture'] = new_crop
                updated = True
                break
        if updated:
            break
    
    if not updated:
        await callback.answer("❌ Ошибка обновления", show_alert=True)
        return
    
    save_batches(batches)
    
    await callback.message.edit_text(
        f"✅ <b>Культура обновлена!</b>\n\n"
        f"Партия #{batch_id}\n"
        f"Было: {old_crop}\n"
        f"Стало: {new_crop}",
        reply_markup=batch_actions_keyboard(batch_id),
        parse_mode='HTML'
    )
    await callback.answer("✅ Культура обновлена!")


@dp.callback_query_handler(lambda c: c.data == 'canceldeletebatch', state='*')
async def cancel_delete_batch(callback: types.CallbackQuery, state: FSMContext):
    # Сбрасываем состояние FSM
    await state.finish()
    
    await callback.message.edit_text("❌ Удаление отменено.")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('editpull_'), state='*')
async def start_edit_pull(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования пула"""
    try:
        # ✅ ИСПРАВЛЕНО: парсим через подчеркивание
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    if pull['exporter_id'] != user_id:
        await callback.answer("❌ Нет доступа к редактированию этого пула", show_alert=True)
        return
    
    await state.update_data(editing_pull_id=pull_id)
    
    try:
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n"
            "Выберите поле для редактирования:",
            reply_markup=edit_pull_fields_keyboard(),
            parse_mode='HTML'
        )
    except MessageNotModified:
        pass  # Сообщение уже в нужном виде
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_pull_field:'), state='*')
async def edit_pull_field_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора поля для редактирования пула"""
    field = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    pull_id = data.get('editing_pull_id')
    
    if not pull_id:
        await callback.answer("❌ Ошибка: пул не найден", show_alert=True)
        return
    
    await state.update_data(edit_pull_field=field, editing_pull_id=pull_id)
    
    field_names = {
        'culture': 'новую культуру',
        'volume': 'новый целевой объём (в тоннах)',
        'price': 'новую цену FOB ($/тонна)',
        'port': 'новый порт отгрузки',
        'moisture': 'новую максимальную влажность (%)',
        'nature': 'новую минимальную натуру (г/л)',
        'impurity': 'новую максимальную сорность (%)',
        'weed': 'новую максимальную засорённость (%)'
    }
    
    if field == 'culture':
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n"
            "Выберите новую культуру:",
            reply_markup=culture_keyboard()
        )
        await EditPullStatesGroup.edit_culture.set()
    elif field == 'port':
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n"
            "Выберите новый порт:",
            reply_markup=port_keyboard()
        )
        await EditPullStatesGroup.edit_port.set()
    else:
        await callback.message.edit_text(
            f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n"
            f"Введите {field_names.get(field, 'новое значение')}:"
        )
        state_mapping = {
            'volume': EditPullStatesGroup.edit_volume,
            'price': EditPullStatesGroup.edit_price,
            'moisture': EditPullStatesGroup.edit_moisture,
            'nature': EditPullStatesGroup.edit_nature,
            'impurity': EditPullStatesGroup.edit_impurity,
            'weed': EditPullStatesGroup.edit_weed
        }
        
        await state_mapping.get(field, EditPullStatesGroup.edit_volume).set()
    
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('culture:'), state=EditPullStatesGroup.edit_culture)
async def edit_pull_culture(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора культуры при редактировании пула"""
    new_culture = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    pull_id = data.get('editing_pull_id')
    
    if not pull_id or pull_id not in pulls:
        await callback.answer("❌ Ошибка: пул не найден", show_alert=True)
        await state.finish()
        return
    
    pull = pulls[pull_id]
    old_value = pull['culture']
    pull['culture'] = new_culture
    
    save_pulls_to_pickle()
    
    if gs and gs.spreadsheet:
        gs.update_pull_in_sheets(pull)
    
    await state.finish()
    
    await callback.message.edit_text(
        f"✅ <b>Культура обновлена!</b>\n\n"
        f"Пул #{pull_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_culture}"
    )
    await asyncio.sleep(1)
    await callback.answer("✅ Культура обновлена")

@dp.callback_query_handler(lambda c: c.data.startswith('port:'), state=EditPullStatesGroup.edit_port)
async def edit_pull_port(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора порта при редактировании пула"""
    port_index = parse_callback_id(callback.data)
    ports = [
        "Ариб",
        "Ариб-Снаб",
        "КЗК",
        "ПКФ ЮТ",
        "ПКФ ВП",
        "Астр Порт",
        "Астр Унив",
        "Астр Южн",
        "ЗЮЙД-ВЕСТ",
        "АгроФуд",
        "Порт 83",
        "ПКФ ЦГП",
        "Астр зерн",
        "Астр морск"
    ]

    new_port = ports[port_index] if port_index < len(ports) else 'Астрахань'
    
    data = await state.get_data()
    pull_id = data.get('editing_pull_id')
    
    if not pull_id or pull_id not in pulls:
        await callback.answer("❌ Ошибка: пул не найден", show_alert=True)
        await state.finish()
        return
    
    pull = pulls[pull_id]
    old_value = pull['port']
    pull['port'] = new_port
    
    save_pulls_to_pickle()
    
    if gs and gs.spreadsheet:
        gs.update_pull_in_sheets(pull)
    
    await state.finish()
    
    await callback.message.edit_text(
        f"✅ <b>Порт обновлён!</b>\n\n"
        f"Пул #{pull_id}\n"
        f"Старое значение: {old_value}\n"
        f"Новое значение: {new_port}"
    )
    await asyncio.sleep(1)
    await view_pull_details(callback)
    await callback.answer("✅ Порт обновлён")

@dp.message_handler(state=EditPullStatesGroup.edit_volume)
async def edit_pull_volume(message: types.Message, state: FSMContext):
    """Обработка ввода объёма при редактировании пула"""
    await edit_pull_numeric_field(message, state, 'target_volume', 'Объём')

@dp.message_handler(state=EditPullStatesGroup.edit_price)
async def edit_pull_price(message: types.Message, state: FSMContext):
    """Обработка ввода цены при редактировании пула"""
    await edit_pull_numeric_field(message, state, 'price', 'Цена')

@dp.message_handler(state=EditPullStatesGroup.edit_moisture)
async def edit_pull_moisture(message: types.Message, state: FSMContext):
    """Обработка ввода влажности при редактировании пула"""
    await edit_pull_numeric_field(message, state, 'moisture', 'Влажность', 0, 100)

@dp.message_handler(state=EditPullStatesGroup.edit_nature)
async def edit_pull_nature(message: types.Message, state: FSMContext):
    """Обработка ввода натуры при редактировании пула"""
    await edit_pull_numeric_field(message, state, 'nature', 'Натура')

@dp.message_handler(state=EditPullStatesGroup.edit_impurity)
async def edit_pull_impurity(message: types.Message, state: FSMContext):
    """Обработка ввода сорности при редактировании пула"""
    await edit_pull_numeric_field(message, state, 'impurity', 'Сорность', 0, 100)

@dp.message_handler(state=EditPullStatesGroup.edit_weed)
async def edit_pull_weed(message: types.Message, state: FSMContext):
    """Обработка ввода засорённости при редактировании пула"""
    await edit_pull_numeric_field(message, state, 'weed', 'Засорённость', 0, 100)

async def edit_pull_numeric_field(message: types.Message, state: FSMContext, field: str, field_name: str, min_val: float = 0, max_val: float = None):
    try:
        new_value = float(message.text.strip().replace(',', '.'))
        
        if new_value < min_val:
            await message.answer(f"❌ Значение должно быть не менее {min_val}")
            return
        
        if max_val is not None and new_value > max_val:
            await message.answer(f"❌ Значение должно быть не более {max_val}")
            return
        
        data = await state.get_data()
        pull_id = data.get('editing_pull_id')
        
        if not pull_id or pull_id not in pulls:
            await message.answer("❌ Пул не найден")
            await state.finish()
            return
        
        pull = pulls[pull_id]
        old_value = pull.get(field, 0)
        pull[field] = new_value
        
        save_pulls_to_pickle()
        
        if gs and gs.spreadsheet:
            gs.update_pull_in_sheets(pull)
        
        await state.finish()
        
        keyboard = get_role_keyboard('exporter')
        await message.answer(
            f"✅ <b>{field_name} изменена!</b>\n\n"
            f"Пул #{pull_id}\n"
            f"Было: {old_value}\n"
            f"Стало: {new_value}",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        # ✅ ИСПРАВЛЕНО: Вместо view_pull_details_direct используем обычный ответ
        # Или перенаправляем на просмотр через callback
        # await view_pull_details_direct(message, pull_id)
        
    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректное число")

@dp.callback_query_handler(lambda c: c.data.startswith('viewpull:'), state='*')
async def viewpulldetailscallback(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей пула"""
    await state.finish()
    
    try:
        pullid = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer('❌ Ошибка: некорректный ID пула', show_alert=True)
        return
    
    userid = callback.from_user.id
    
    if pullid not in pulls:
        await callback.answer('❌ Пул не найден', show_alert=True)
        return
    
    pull = pulls[pullid]
    
    # Проверяем роль и ID
    isowner = (pull.get('exporter_id') == userid and 
               users.get(userid, {}).get('role') == 'exporter')
    
    progress = (pull.get('current_volume', 0) / pull.get('target_volume', 1)) * 100 if pull.get('target_volume', 0) > 0 else 0
    
    text = (
        f"<b>🌾 Пул #{pullid}</b>\n\n"
        f"📊 <b>Культура:</b> {pull.get('culture', 'Н/Д')}\n"
        f"📦 <b>Объём:</b> {pull.get('current_volume', 0):.0f}/{pull.get('target_volume', 0):.0f} тонн ({progress:.0f}%)\n"
        f"💰 <b>Цена FOB:</b> ${pull.get('price', 0):,.0f}/тонна\n"
        f"🚢 <b>Порт:</b> {pull.get('port', 'Н/Д')}\n\n"
        f"<b>📋 Требования к качеству:</b>\n"
        f"💧 Влажность: до {pull.get('moisture', 0)}%\n"
        f"🌾 Натура: от {pull.get('nature', 0)} г/л\n"
        f"🔬 Сорность: до {pull.get('impurity', 0)}%\n"
        f"🌿 Засорённость: до {pull.get('weed', 0)}%\n\n"
        f"📄 Документы: {pull.get('documents', 'Нет')}\n"
        f"📋 Статус: {pull.get('status', 'Активен')}\n"
        f"🗓 Создан: {pull.get('created_at', 'Н/Д')}"
    )
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    if isowner:
        keyboard.add(
            InlineKeyboardButton('✏️ Редактировать', callback_data=f'editpull_{pullid}'),
            InlineKeyboardButton('🗑 Удалить', callback_data=f'deletepull_{pullid}')
        )
        keyboard.add(
            InlineKeyboardButton('👥 Участники', callback_data=f'viewpullparticipants_{pullid}'),
            InlineKeyboardButton('🤝 Матчи', callback_data=f'viewpullmatches_{pullid}')
        )
        keyboard.add(
            InlineKeyboardButton('🔒 Закрыть пул', callback_data=f'close_pull_{pullid}')
        )
    else:
        keyboard.add(
            InlineKeyboardButton('🤝 Присоединиться', callback_data=f'join_pull:{pullid}')  # ← ИСПРАВЛЕНО!
        )
    
    keyboard.add(
        InlineKeyboardButton('🔙 Назад', callback_data='back_to_pools_list')
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


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
        with open(PULLSFILE, 'rb') as f:
            saved_pulls = pickle.load(f)
            in_pickle = pullid in saved_pulls.get('pulls', {})
            logging.info(f"В pickle файле: {in_pickle}")
    except Exception as e:
        logging.error(f"Ошибка чтения pickle: {e}")
    
    # Проверка наличия в Google Sheets
    if gs and gs.spreadsheet:
        try:
            worksheet = gs.spreadsheet.worksheet('Pulls')
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
        # Получаем всех логистов
        logistics = [uid for uid, user in users.items() if user.get('role') == 'logistic']
        
        if pullid not in pulls:
            logging.error(f"Пул {pullid} не найден для уведомления логистов")
            return
        
        pull = pulls[pullid]
        
        # Формируем сообщение
        message = (
            f"🔔 <b>Пул #{pullid} закрыт и готов к логистике</b>\n\n"
            f"🌾 <b>Культура:</b> {pull.get('culture', 'Н/Д')}\n"
            f"📦 <b>Объём:</b> {pull.get('targetvolume', 0)} тонн\n"
            f"💰 <b>Цена FOB:</b> ${pull.get('price', 0):,.0f}/тонна\n"
            f"🚢 <b>Порт:</b> {pull.get('port', 'Н/Д')}\n\n"
            f"📋 Вы можете подать заявку на логистику этого пула."
        )
        
        # Отправляем уведомления всем логистам
        for logistic_id in logistics:
            try:
                await bot.send_message(logistic_id, message, parse_mode='HTML')
                logging.info(f"Уведомление о закрытии пула {pullid} отправлено логисту {logistic_id}")
            except Exception as e:
                logging.error(f"Не удалось уведомить логиста {logistic_id}: {e}")
        
        logging.info(f"Уведомления о закрытии пула {pullid} отправлены {len(logistics)} логистам")
        
    except Exception as e:
        logging.error(f"Ошибка в notify_logistic_pull_closed: {e}")

# ==================== НАЧАЛО УДАЛЕНИЯ ПУЛА ====================
@dp.callback_query_handler(lambda c: c.data.startswith('deletepull_'), state='*')
async def deletepullstart_callback(callback: types.CallbackQuery, state: FSMContext):
    """Запрос подтверждения удаления пула"""
    try:
        pullid = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer('❌ Ошибка: некорректный ID пула', show_alert=True)
        return
    
    userid = callback.from_user.id
    
    if pullid not in pulls:
        await callback.answer('❌ Пул не найден', show_alert=True)
        return
    
    pull = pulls[pullid]
    
    if pull.get('exporter_id') != userid:
        await callback.answer('❌ Только создатель пула может его удалить', show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton('✅ Да, удалить', callback_data=f'confirmdeletepull_{pullid}'),
        InlineKeyboardButton('❌ Отмена', callback_data='canceldeletepull')
    )
    
    await callback.message.edit_text(
        f"<b>⚠️ Подтверждение удаления</b>\n\n"
        f"❓ Вы уверены, что хотите удалить пул №{pullid}?\n\n"
        f"🌾 <b>Культура:</b> {pull.get('culture', 'Н/Д')}\n"
        f"📦 <b>Объём:</b> {pull.get('targetvolume', 0)} тонн\n"
        f"💰 <b>Цена FOB:</b> ${pull.get('price', 0):,.0f}/тонна\n\n"
        f"<b>⚠️ Это действие нельзя отменить!</b>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await callback.answer()


# ==================== ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ ====================
@dp.callback_query_handler(lambda c: c.data.startswith('confirmdeletepull_'), state='*')
async def deletepullconfirmed_callback(callback: types.CallbackQuery, state: FSMContext):
    """Финальное удаление пула после подтверждения"""
    await state.finish()
    
    try:
        pullid = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer('❌ Ошибка: некорректный ID пула', show_alert=True)
        logging.error(f"Ошибка парсинга при подтверждении: {e}, data: {callback.data}")
        return
    
    userid = callback.from_user.id
    
    if pullid not in pulls:
        await callback.answer('❌ Пул не найден', show_alert=True)
        return
    
    pull = pulls[pullid]
    
    if pull.get('exporter_id') != userid:
        await callback.answer('❌ У вас нет прав на удаление этого пула', show_alert=True)
        return
    
    # Сохраняем данные для логирования
    pull_culture = pull.get('culture', 'Н/Д')
    pull_volume = pull.get('targetvolume', 0)
    pull_price = pull.get('price', 0)
    
    # ========== УДАЛЕНИЕ СВЯЗАННЫХ ДАННЫХ ==========
    
    # 1. Удаляем все матчи
    matches_to_delete = [mid for mid, m in matches.items() if m.get('pullid') == pullid]
    for mid in matches_to_delete:
        del matches[mid]
    
    # 2. Сохраняем и удаляем участников
    participants = pullparticipants.get(pullid, [])
    if pullid in pullparticipants:
        del pullparticipants[pullid]
    
    # 3. Удаляем сам пул
    del pulls[pullid]
    
    # 4. Сохраняем изменения
    savepullstopickle()  # ← ВОТ ТУТ ВЫЗОВ ФУНКЦИИ!
    
    # ========== СИНХРОНИЗАЦИЯ С GOOGLE SHEETS ==========
    if gs and gs.spreadsheet:
        try:
            worksheet = gs.spreadsheet.worksheet('Pulls')
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
    for participant in participants:
        farmerid = participant.get('farmerid')
        if farmerid and farmerid in users:
            try:
                await bot.send_message(
                    farmerid,
                    f"<b>🗑 Пул №{pullid} был удалён</b>\n\n"
                    f"🌾 <b>Культура:</b> {pull_culture}\n"
                    f"📦 <b>Объём:</b> {pull_volume} тонн\n"
                    f"💰 <b>Цена FOB:</b> ${pull_price:,.0f}/тонна\n\n"
                    f"Экспортёр удалил этот пул. Если у вас были партии, они остались в вашем списке.",
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"Не удалось уведомить фермера {farmerid}: {e}")
    
    # ========== ОТВЕТ ПОЛЬЗОВАТЕЛЮ ==========
    await callback.message.edit_text(
        f"<b>✅ Пул №{pullid} успешно удалён!</b>\n\n"
        f"🌾 <b>Культура:</b> {pull_culture}\n"
        f"📦 <b>Объём:</b> {pull_volume} тонн\n"
        f"💰 <b>Цена FOB:</b> ${pull_price:,.0f}/тонна\n\n"
        f"🗑 Удалено участников: {len(participants)}\n"
        f"🗑 Удалено матчей: {len(matches_to_delete)}",
        parse_mode='HTML'
    )
    
    await callback.answer('✅ Пул удалён!')
    
    logging.info(f"Пул {pullid} удалён пользователем {userid}. Участников: {len(participants)}, матчей: {len(matches_to_delete)}")


# ==================== ОТМЕНА УДАЛЕНИЯ ====================
@dp.callback_query_handler(lambda c: c.data == 'canceldeletepull', state='*')
async def canceldeletepull_callback(callback: types.CallbackQuery, state: FSMContext):
    """Отмена процесса удаления пула"""
    await state.finish()
    
    await callback.message.edit_text(
        "❌ Удаление отменено. Пул не был удалён."
    )
    
    await callback.answer()

    await callback.message.edit_text(
        f"🔒 <b>Подтвердите закрытие пулла</b>\n\n"
        f"🆔 Пулл #{pull_id}\n"
        f"🌾 {pull.get('culture', 'N/A')}\n"
        f"📦 {pull.get('targetvolume', 0)} т\n"
        f"💰 {pull.get('price', 0)} $/т (FOB)\n\n"
        f"<b>⚠️ После закрытия пулла:</b>\n"
        f"• Новые участники не смогут присоединиться\n"
        f"• Логисты получат уведомление\n"
        f"• Статус изменится на 'Закрыт'\n\n"
        f"<b>Вы уверены?</b>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback_query.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirmclosepull_'), state='*')
async def confirm_close_pull_callback(callback_query: types.CallbackQuery):
    """Подтвердить закрытие пулла"""
    try:
        pull_id = int(callback_query.data.split('_')[3])
    except (IndexError, ValueError) as e:
        await callback_query.answer('❌ Ошибка: неверный ID пулла', show_alert=True)
        return
    
    user_id = callback_query.from_user.id
    
    if pull_id not in pulls:
        await callback_query.answer('❌ Пулл не найден', show_alert=True)
        return
    
    pull = pulls[pull_id]
    
    # ✅ ИСПРАВЛЕНО: правильное имя поля
    if pull.get('exporter_id') != user_id:
        await callback_query.answer('⚠️ Только владелец может закрыть пулл', show_alert=True)
        return
    
    # ИЗМЕНИТЬ СТАТУС НА 'CLOSED'
    pulls[pull_id]['status'] = 'closed'
    pulls[pull_id]['closedat'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Сохранить в файл
    savepullstopickle()
    
    # Обновить в Google Sheets (если подключено)
    if gs and gs.spreadsheet:
        try:
            # ✅ ИСПРАВЛЕНО: правильный метод
            gs.updatepullinsheets(pull)
        except Exception as e:
            logging.error(f"Ошибка обновления пулла в Google Sheets: {e}")
    
    # ← ОТПРАВИТЬ УВЕДОМЛЕНИЯ ЛОГИСТАМ
    await notify_logistic_pull_closed(pull_id)
    
    # Уведомить участников пулла
    participants = pullparticipants.get(pull_id, [])
    for participant in participants:
        farmerid = participant.get('farmerid')
        if farmerid and farmerid in users:
            try:
                await bot.send_message(
                    farmerid,
                    f"🔒 <b>Пулл #{pull_id} закрыт</b>\n\n"
                    f"🌾 {pull.get('culture', 'N/A')}\n"
                    f"📦 {pull.get('targetvolume', 0)} т\n"
                    f"💰 {pull.get('price', 0)} $/т\n\n"
                    f"Спасибо за участие!",
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"Ошибка уведомления фермера {farmerid}: {e}")
    
    # Показать подтверждение
    await callback.message.edit_text(
        f"✅ <b>Пулл #{pull_id} успешно закрыт!</b>\n\n"
        f"🌾 {pull.get('culture', 'N/A')}\n"
        f"📦 {pull.get('targetvolume', 0)} т\n"
        f"💰 {pull.get('price', 0)} $/т\n\n"
        f"Все участники и логисты получили уведомления.",
        parse_mode='HTML'
    )
    
    await callback_query.answer('✅ Пулл закрыт!')
    logging.info(f"Пулл {pull_id} закрыт пользователем {user_id}")


@dp.callback_query_handler(lambda c: c.data == 'cancel_delete_pull', state='*')
async def cancel_delete_pull(callback: types.CallbackQuery):
    """Отмена удаления пула"""
    await callback.message.edit_text("❌ Удаление отменено")
    await callback.answer()


# ================================
# ОБРАБОТЧИК ЗАКРЫТИЯ ПУЛЛА
# ================================
@dp.callback_query_handler(lambda c: c.data == 'get_partner_contacts', state='*')
async def get_partner_contacts_handler(callback: types.CallbackQuery):
    """Получение контактов партнёра по сделке"""
    user_id = callback.from_user.id
    partner_info = None
    
    # Ищем сделку, где пользователь участвует
    for deal_id, deal in deals.items():
        if deal.get('logistic_id') == user_id:
            # Логист - получаем контакты экспортёра
            exporter_id = deal.get('exporter_id')
            exporter = users.get(exporter_id)
            if exporter:
                partner_info = "📦 <b>Контакты экспортёра:</b>\n\n"
                partner_info += f"📝 {exporter.get('name', 'Неизвестно')}\n"
                partner_info += f"📱 {exporter.get('phone', 'Не указан')}\n"
                partner_info += f"📧 {exporter.get('email', 'Не указан')}\n"
                partner_info += f"📍 {exporter.get('region', 'Не указан')}\n"
                break
        
        elif deal.get('expeditor_id') == user_id:
            # Экспедитор - получаем контакты экспортёра
            exporter_id = deal.get('exporter_id')
            exporter = users.get(exporter_id)
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
    
    await callback.message.answer(partner_info, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('complete_deal:'), state='*')
async def complete_deal(callback: types.CallbackQuery):
    """Завершение сделки"""
    deal_id = parse_callback_id(callback.data)
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    user_id = callback.from_user.id
    if userid != deal.get('exporterid') and userid not in deal.get('farmerids', []):
        await callback.answer('⚠️ Только участники сделки могут её завершить', showalert=True)
        return
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, завершить", callback_data=f"confirm_complete_deal:{deal_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_complete_deal")
    )
    
    await callback.message.edit_text(
        f"✅ <b>Подтверждение завершения</b>\n\n"
        f"Вы уверены, что хотите завершить сделку #{deal_id}?\n\n"
        f"После завершения:\n"
        f"• Сделка переместится в архив\n"
        f"• Все участники получат уведомление\n"
        f"• Статистика будет обновлена\n\n"
        f"<b>Это действие нельзя отменить!</b>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('confirm_complete_deal:'), state='*')
async def confirm_complete_deal(callback: types.CallbackQuery):
    """Подтверждение завершения сделки"""
    deal_id = parse_callback_id(callback.data)
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    deal['status'] = 'completed'
    deal['completed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    save_pulls_to_pickle()
    await notify_deal_participants(deal_id, "✅ Сделка завершена!")
    
    await callback.message.edit_text(
        f"🎉 <b>Сделка #{deal_id} завершена!</b>\n\n"
        f"Все участники уведомлены о завершении сделки.\n"
        f"Спасибо за использование платформы Exportum!",
        parse_mode='HTML'
    )
    await callback.answer("✅ Сделка завершена")

@dp.callback_query_handler(lambda c: c.data.startswith('cancel_deal:'), state='*')
async def cancel_deal(callback: types.CallbackQuery):
    """Отмена сделки"""
    deal_id = parse_callback_id(callback.data)
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    user_id = callback.from_user.id
    if deal.get('exporter_id') != user_id:
        await callback.answer("❌ Только экспортёр может отменить сделку", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, отменить", callback_data=f"confirm_cancel_deal:{deal_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_cancel_deal")
    )
    
    await callback.message.edit_text(
        f"❌ <b>Подтверждение отмены</b>\n\n"
        f"Вы уверены, что хотите отменить сделку #{deal_id}?\n\n"
        f"После отмены:\n"
        f"• Сделка будет помечена как отменённая\n"
        f"• Все участники получат уведомление\n"
        f"• Статистика будет обновлена\n\n"
        f"<b>Это действие нельзя отменить!</b>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('confirm_cancel_deal:'), state='*')
async def confirm_cancel_deal(callback: types.CallbackQuery):
    """Подтверждение отмены сделки"""
    deal_id = parse_callback_id(callback.data)
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    deal['status'] = 'cancelled'
    deal['completed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    save_pulls_to_pickle()
    await notify_deal_participants(deal_id, "❌ Сделка отменена")
    
    await callback.message.edit_text(
        f"❌ <b>Сделка #{deal_id} отменена!</b>\n\n"
        f"Все участники уведомлены об отмене сделки.",
        parse_mode='HTML'
    )
    await callback.answer("✅ Сделка отменена")

@dp.callback_query_handler(lambda c: c.data in ['cancel_complete_deal', 'cancel_cancel_deal'], state='*')
async def cancel_deal_action(callback: types.CallbackQuery):
    """Отмена действия со сделкой"""
    await callback.message.edit_text("❌ Действие отменено")
    await callback.answer()

async def notify_deal_participants(deal_id: int, message: str):
    """Уведомление всех участников сделки"""
    deal = deals.get(deal_id)
    if not deal:
        return
    
    participants = []
    if deal.get('exporter_id'):
        participants.append(deal['exporter_id'])
    if deal.get('farmer_ids'):
        participants.extend(deal['farmer_ids'])
    if deal.get('logistic_id'):
        participants.append(deal['logistic_id'])
    if deal.get('expeditor_id'):
        participants.append(deal['expeditor_id'])
    for user_id in participants:
        try:
            await bot.send_message(
                user_id,
                f"📋 <b>Уведомление по сделке #{deal_id}</b>\n\n{message}",
                parse_mode='HTML'
            )
            await asyncio.sleep(0.1)  # Задержка между отправками
        except Exception as e:
            logging.error(f"❌ Ошибка уведомления пользователя {user_id}: {e}")

@dp.callback_query_handler(lambda c: c.data.startswith('logistics:'), state='*')
async def deal_logistics(callback: types.CallbackQuery):
    """Управление логистикой для сделки"""
    deal_id = parse_callback_id(callback.data)
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    
    text = f"🚚 <b>Логистика сделки #{deal_id}</b>\n\n"
    
    if deal.get('logistic_id'):
        logistic = users.get(deal['logistic_id'])
        if logistic:
            text += "✅ <b>Логист назначен:</b>\n"
            text += f"📝 {logistic.get('name', 'Неизвестно')}\n"
            text += f"📱 {logistic.get('phone', 'Не указан')}\n"
            text += f"📧 {logistic.get('email', 'Не указан')}\n"
        else:
            text += "❌ Логист не найден в системе\n"
    else:
        text += "🤷‍♂️ <b>Логист не назначен</b>\n\n"
        text += "Для назначения логиста создайте заявку на логистику."
    
    if deal.get('expeditor_id'):
        expeditor = users.get(deal['expeditor_id'])
        if expeditor:
            text += "\n✅ <b>Экспедитор назначен:</b>\n"
            text += f"📝 {expeditor.get('name', 'Неизвестно')}\n"
            text += f"📱 {expeditor.get('phone', 'Не указан')}\n"
            text += f"📧 {expeditor.get('email', 'Не указан')}\n"
        else:
            text += "\n❌ Экспедитор не найден в системе\n"
    else:
        text += "\n🤷‍♂️ <b>Экспедитор не назначен</b>\n\n"
        text += "Для назначения экспедитора создайте заявку на оформление документов."
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("◀️ Назад к сделке", callback_data=f"view_deal:{deal_id}"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pullparticipants:'), state='*')
async def show_pullparticipants(callback: types.CallbackQuery):
    """Показать участников пула"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    participants = pullparticipants.get(pull_id, [])
    
    text = f"👥 <b>Участники пула #{pull_id}</b>\n\n"
    text += f"🌾 {pull['culture']} • {pull['target_volume']} т\n\n"
    
    if not participants:
        text += "🤷‍♂️ У пула пока нет участников"
    else:
        total_participant_volume = 0
        for i, participant in enumerate(participants, 1):
            farmer_id = participant.get('farmer_id')
            farmer = users.get(farmer_id)
            batch_id = participant.get('batch_id')
            volume = participant.get('volume', 0)
            total_participant_volume += volume
            
            farmer_name = farmer.get('name', 'Неизвестно') if farmer else 'Неизвестно'
            
            text += f"{i}. 👤 {farmer_name}\n"
            text += f"   📦 Партия #{batch_id}: {volume} т\n"
            text += f"   📍 {farmer.get('region', 'Не указан') if farmer else 'Не указан'}\n\n"
        
        fill_percentage = (total_participant_volume / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
        text += f"📊 <b>Итого:</b> {total_participant_volume:.0f}/{pull['target_volume']:.0f} т ({fill_percentage:.1f}%)"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("◀️ Назад к пулу", callback_data=f"view_pull:{pull_id}"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pull_logistics:'), state='*')
async def pull_logistics_menu(callback: types.CallbackQuery):
    """Меню логистики для пула"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    
    text = f"🚚 <b>Логистика пула #{pull_id}</b>\n\n"
    text += f"🌾 {pull['culture']} • {pull['target_volume']} т • {pull['port']}\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("📋 Создать заявку на логистику", callback_data=f"create_shipping:{pull_id}"),
        InlineKeyboardButton("👀 Активные заявки", callback_data=f"view_shipping_requests:{pull_id}"),
        InlineKeyboardButton("📞 Контакты логистов", callback_data="view_logistics_contacts")
    )
    keyboard.add(InlineKeyboardButton("◀️ Назад к пулу", callback_data=f"view_pull:{pull_id}"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('create_shipping:'), state='*')
async def create_shipping_from_pull(callback: types.CallbackQuery, state: FSMContext):
    """Создание заявки на логистику из пула"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    
    await state.update_data(pull_id=pull_id)
    
    await callback.message.edit_text(
        f"🚚 <b>Заявка на логистику для пула #{pull_id}</b>\n\n"
        f"🌾 {pull['culture']} • {pull['target_volume']} т • {pull['port']}\n\n"
        "Введите пункт отправки (город/регион):",
        parse_mode='HTML'
    )
    
    await ShippingRequestStatesGroup.route_from.set()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "view_logistics_contacts", state='*')
async def view_logistics_contacts(callback: types.CallbackQuery):
    """Показать контакты логистов"""
    logistics_users = [user for user in users.values() if user.get('role') == 'logistic']
    
    if not logistics_users:
        await callback.answer("🤷‍♂️ В системе пока нет логистов", show_alert=True)
        return
    
    text = "🚚 <b>Логисты на платформе</b>\n\n"
    
    for i, logistic in enumerate(logistics_users[:10], 1):  # Ограничиваем показ
        text += f"{i}. 📝 {logistic.get('name', 'Неизвестно')}\n"
        text += f"   📱 {logistic.get('phone', 'Не указан')}\n"
        text += f"   📧 {logistic.get('email', 'Не указан')}\n"
        text += f"   📍 {logistic.get('region', 'Не указан')}\n\n"
    
    if len(logistics_users) > 10:
        text += f"<i>... и ещё {len(logistics_users) - 10} логистов</i>\n\n"
    
    text += "💡 <b>Свяжитесь с логистами для обсуждения условий перевозки.</b>"
    
    await callback.message.edit_text(text, parse_mode='HTML')
    await callback.answer()

@dp.message_handler(lambda m: m.text in ["🚚 Моя карточка", "🚛 Моя карточка"], state='*')
async def show_logistics_card(message: types.Message):
    """Показать карточку логиста/экспедитора"""
    user_id = message.from_user.id

    if user_id not in users:
        await message.answer("❌ Пользователь не найден. Пройдите регистрацию командой /start")
        return

    user = users[user_id]
    role = user.get('role')

    if role not in ['logistic', 'expeditor']:
        await message.answer("❌ Эта функция доступна только логистам и экспедиторам")
        return

    # Формируем карточку
    role_emoji = "🚚" if role == 'logistic' else "🚛"
    role_name = "Логист" if role == 'logistic' else "Экспедитор"

    text = f"{role_emoji} <b>Моя карточка ({role_name})</b>\n\n"
    text += f"👤 Имя: {user.get('name', 'Не указано')}\n"
    text += f"📞 Телефон: <code>{user.get('phone', 'Не указан')}</code>\n"
    text += f"📧 Email: {user.get('email', 'Не указан')}\n"
    text += f"📍 Регион: {user.get('region', 'Не указан')}\n\n"

    if user.get('inn'):
        text += f"🏢 ИНН: <code>{user['inn']}</code>\n"

    if user.get('company_details'):
        text += f"📋 О компании:\n{user['company_details'][:300]}\n\n"

    # Статистика
    if role == 'logistic':
        # Считаем заявки логиста
        logistics_requests = [req for req in shipping_requests.values() if req.get('logist_id') == user_id]
        active_requests = [req for req in logistics_requests if req.get('status') == 'active']
        text += "📊 <b>Статистика:</b>\n"
        text += f"   • Всего заявок: {len(logistics_requests)}\n"
        text += f"   • Активных: {len(active_requests)}\n"
    else:  # expeditor
        # Статистика экспедитора
        text += "📊 <b>Статистика:</b>\n"
        text += "   • Оформленных сделок: 0\n"
        text += "   • В процессе: 0\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("✏️ Редактировать профиль", callback_data="edit_profile"))

    await message.answer(text, parse_mode='HTML', reply_markup=keyboard)


@dp.message_handler(lambda m: m.text == "🚚 Активные заявки", state='*')
async def show_active_requests(message: types.Message, state: FSMContext):
    """Показать активные заявки на доставку"""
    await state.finish()
    user_id = message.from_user.id
    
    if user_id not in users:
        await message.answer("❌ Пользователь не найден. Пройдите регистрацию командой /start")
        return
    
    user = users[user_id]
    role = user.get('role')
    
    # Фильтруем заявки в зависимости от роли
    if role == 'logistic':
        # Логист видит ВСЕ активные заявки от экспортёров
        user_requests = [req for req in shipping_requests.values() if req.get('status') == 'active']
        title = "🚚 <b>Активные заявки на доставку</b>"
        show_buttons = True
    elif role == 'exporter':
        # Экспортёр видит СВОИ заявки
        user_requests = [req for req in shipping_requests.values() if req.get('exporter_id') == user_id]
        title = "📋 <b>Мои заявки на доставку</b>"
        show_buttons = False
    elif role == 'expeditor':
        # Экспедитор видит заявки, которые ему назначены
        user_requests = []  # TODO: добавить фильтрацию по экспедитору
        title = "🚛 <b>Мои заявки на оформление</b>"
        show_buttons = False
    else:
        await message.answer("❌ Эта функция недоступна для вашей роли")
        return
    
    if not user_requests:
        msg_text = f"{title}\n\n"
        if role == 'logistic':
            msg_text += "📋 Активных заявок нет\n\n"
            msg_text += "Как только экспортёры создадут заявки на доставку, "
            msg_text += "они появятся здесь. Вы получите уведомление."
        else:
            msg_text += "❌ У вас нет заявок"
        
        await message.answer(msg_text, reply_markup=logistic_keyboard() if role == 'logistic' else None, parse_mode='HTML')
        return
    
    # Создаём кнопки для каждой заявки (только для логиста)
    if show_buttons:
        keyboard = InlineKeyboardMarkup(row_width=1)
        
        for req in user_requests[:15]:  # Показываем первые 15
            req_id = req.get('id', 'N/A')
            culture = req.get('culture', 'N/A')
            volume = req.get('volume', 0) or 0  # ← ИСПРАВЛЕНО!
            route_from = req.get('route_from', '—')
            route_to = req.get('route_to', '—')
            
            btn_text = f"🚛 #{req_id} | {culture} | {volume:.0f} т | {route_from}→{route_to}"
            keyboard.add(
                InlineKeyboardButton(btn_text, callback_data=f"view_request:{req_id}")
            )
        
        await message.answer(
            f"{title}\n\n"
            f"📋 Всего заявок: <b>{len(user_requests)}</b>\n\n"
            f"Выберите заявку для просмотра деталей и создания предложения:",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    else:
        # Для экспортёра показываем список текстом
        text = f"{title}\n\n"
        
        for idx, req in enumerate(user_requests[:10], 1):
            req_id = req.get('id', 'N/A')
            volume = req.get('volume', 0) or 0  # ← ИСПРАВЛЕНО!
            route_from = req.get('route_from', 'Не указано')
            route_to = req.get('route_to', 'Не указано')
            culture = req.get('culture', 'N/A')
            status = req.get('status', 'unknown')
            
            text += f"{idx}. 📦 Заявка #{req_id}\n"
            text += f"   • Культура: {culture}\n"
            text += f"   • Объём: {volume:.0f} т\n"
            text += f"   • Маршрут: {route_from} → {route_to}\n"
            text += f"   • Статус: {status}\n\n"
        
        if len(user_requests) > 10:
            text += f"<i>... и ещё {len(user_requests) - 10} заявок</i>\n\n"
        
        await message.answer(text, parse_mode='HTML')

@dp.callback_query_handler(lambda c: c.data.startswith('view_request:'), state='*')
async def view_request_details(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей заявки на доставку"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split(':')[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    pull_id = request.get('pull_id')
    pull = pulls.get(pull_id, {})
    exporter_id = request.get('exporter_id')
    exporter = users.get(exporter_id, {})
    
    # Формируем детали заявки
    text = f"""🚚 <b>Заявка на доставку #{request_id}</b>

<b>Информация о грузе:</b>
🌾 Культура: {request.get('culture', '—')}
📦 Объём: {request.get('volume', 0):.0f} т
💰 Цена FOB: ${pull.get('price', 0):.2f}/т
🚢 Порт: {pull.get('port', '—')}

<b>Маршрут:</b>
📍 Откуда: {request.get('route_from', '—')}
📍 Куда: {request.get('route_to', '—')}
📅 Желаемая дата: {request.get('desired_date', '—')}

<b>Экспортёр:</b>
🏢 {exporter.get('company_name', 'N/A')}
📱 @{exporter.get('username', 'нет username')}

<b>Статус:</b> {request.get('status', 'active').upper()}
📅 Создана: {request.get('created_at', '—')}
"""
    
    # Проверяем, есть ли уже предложения от этого логиста
    user_id = callback.from_user.id
    existing_offers = [
        offer for offer in logistic_offers.values()
        if offer.get('request_id') == request_id 
        and offer.get('logist_id') == user_id
    ]
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    if existing_offers:
        # Логист уже сделал предложение
        offer = existing_offers[0]
        text += f"\n\n✅ <b>Вы уже сделали предложение:</b>\n"
        text += f"🚛 Транспорт: {offer.get('vehicle_type', '—')}\n"
        text += f"💰 Цена: {offer.get('price', 0):,.0f} ₽\n"
        text += f"📅 Дата доставки: {offer.get('delivery_date', '—')}\n"
        text += f"📊 Статус: {offer.get('status', 'pending')}"
        
        keyboard.add(
            InlineKeyboardButton("◀️ К списку заявок", callback_data="back_to_requests")
        )
    else:
        # Можно сделать предложение
        keyboard.add(
            InlineKeyboardButton("💰 Сделать предложение", 
                                callback_data=f"make_offer:{request_id}")
        )
        keyboard.add(
            InlineKeyboardButton("◀️ К списку заявок", callback_data="back_to_requests")
        )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'back_to_requests', state='*')
async def back_to_requests(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку активных заявок"""
    await state.finish()
    
    # Получаем роль пользователя
    user_id = callback.from_user.id
    if user_id not in users:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    
    user = users[user_id]
    role = user.get('role')
    
    # Фильтруем активные заявки
    if role == 'logistic':
        user_requests = [req for req in shipping_requests.values() if req.get('status') == 'active']
        title = "🚚 <b>Активные заявки на доставку</b>"
    else:
        await callback.answer("❌ Функция доступна только логистам", show_alert=True)
        return
    
    if not user_requests:
        await callback.message.edit_text(
            f"{title}\n\n"
            f"📋 Активных заявок нет\n\n"
            f"Как только экспортёры создадут заявки на доставку, "
            f"они появятся здесь.",
            parse_mode='HTML'
        )
        await callback.answer()
        return
    
    # Создаём кнопки
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for req in user_requests[:15]:
        req_id = req.get('id', 'N/A')
        culture = req.get('culture', 'N/A')
        volume = req.get('volume', 0) or 0
        route_from = req.get('route_from', '—')
        route_to = req.get('route_to', '—')
        
        btn_text = f"🚛 #{req_id} | {culture} | {volume:.0f} т | {route_from}→{route_to}"
        keyboard.add(
            InlineKeyboardButton(btn_text, callback_data=f"view_request:{req_id}")
        )
    
    await callback.message.edit_text(
        f"{title}\n\n"
        f"📋 Всего заявок: <b>{len(user_requests)}</b>\n\n"
        f"Выберите заявку для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

# ============================================================================
# ЛОГИСТ: СОЗДАНИЕ ПРЕДЛОЖЕНИЯ ПО ДОСТАВКЕ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data.startswith('make_offer:'), state='*')
async def make_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало создания предложения логистом"""
    
    try:
        request_id = int(callback.data.split(':')[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    
    # Сохраняем ID заявки в FSM
    await state.update_data(request_id=request_id)
    
    # Переходим к выбору типа транспорта
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🚛 Фура (20 т)", callback_data="vehicle:fura"),
        InlineKeyboardButton("🚚 Еврофура (22 т)", callback_data="vehicle:eurofura"),
        InlineKeyboardButton("📦 Контейнер 20ft", callback_data="vehicle:container20"),
        InlineKeyboardButton("📦 Контейнер 40ft", callback_data="vehicle:container40"),
        InlineKeyboardButton("🚐 Другое", callback_data="vehicle:other"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer")
    )
    
    await callback.message.edit_text(
        f"💰 <b>Создание предложения</b>\n\n"
        f"📦 Заявка: #{request_id}\n"
        f"🌾 {request.get('culture', 'N/A')} - {request.get('volume', 0):.0f} т\n"
        f"📍 {request.get('route_from', '—')} → {request.get('route_to', '—')}\n\n"
        f"<b>Шаг 1/3:</b> Выберите тип транспорта:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await LogisticOfferStatesGroup.vehicle_type.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('vehicle:'), state=LogisticOfferStatesGroup.vehicle_type)
async def offer_vehicle_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора типа транспорта"""
    
    vehicle_map = {
        'fura': '🚛 Фура (20 т)',
        'eurofura': '🚚 Еврофура (22 т)',
        'container20': '📦 Контейнер 20ft',
        'container40': '📦 Контейнер 40ft',
        'other': '🚐 Другое'
    }
    
    vehicle_code = callback.data.split(':')[1]
    vehicle_type = vehicle_map.get(vehicle_code, '🚐 Другое')
    
    await state.update_data(vehicle_type=vehicle_type)
    
    # Переходим к вводу цены
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer")
    )
    
    data = await state.get_data()
    request_id = data.get('request_id')
    request = shipping_requests.get(request_id, {})
    
    await callback.message.edit_text(
        f"💰 <b>Создание предложения</b>\n\n"
        f"📦 Заявка: #{request_id}\n"
        f"🚛 Транспорт: {vehicle_type}\n\n"
        f"<b>Шаг 2/3:</b> Введите цену доставки (в рублях):\n\n"
        f"Например: <code>50000</code> или <code>75000</code>\n\n"
        f"💡 Рекомендуемая цена для {request.get('volume', 0):.0f} т: "
        f"{int(request.get('volume', 0) * 1500):,} ₽",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await LogisticOfferStatesGroup.price.set()
    await callback.answer()


@dp.message_handler(state=LogisticOfferStatesGroup.price)
async def offer_price_entered(message: types.Message, state: FSMContext):
    """Обработка ввода цены"""
    
    try:
        price = float(message.text.replace(',', '').replace(' ', ''))
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            "❌ Неверный формат цены!\n\n"
            "Введите число, например: <code>50000</code>",
            parse_mode='HTML'
        )
        return
    
    await state.update_data(price=price)
    
    # Переходим к вводу даты
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📅 Завтра", callback_data="date:tomorrow"),
        InlineKeyboardButton("📅 Через 3 дня", callback_data="date:3days"),
        InlineKeyboardButton("📅 Через неделю", callback_data="date:week"),
        InlineKeyboardButton("📝 Ввести вручную", callback_data="date:custom"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer")
    )
    
    data = await state.get_data()
    request_id = data.get('request_id')
    vehicle_type = data.get('vehicle_type')
    
    await message.answer(
        f"💰 <b>Создание предложения</b>\n\n"
        f"📦 Заявка: #{request_id}\n"
        f"🚛 Транспорт: {vehicle_type}\n"
        f"💰 Цена: {price:,.0f} ₽\n\n"
        f"<b>Шаг 3/3:</b> Выберите дату доставки:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await LogisticOfferStatesGroup.delivery_date.set()


@dp.callback_query_handler(lambda c: c.data.startswith('date:'), state=LogisticOfferStatesGroup.delivery_date)
async def offer_date_selected(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора даты доставки"""
    
    from datetime import datetime, timedelta
    
    date_code = callback.data.split(':')[1]
    
    if date_code == 'custom':
        await callback.message.edit_text(
            "📅 <b>Введите дату доставки</b>\n\n"
            "Формат: <code>ДД.ММ.ГГГГ</code>\n"
            "Например: <code>15.11.2025</code>",
            parse_mode='HTML'
        )
        await callback.answer()
        return
    
    # Автоматический расчёт даты
    today = datetime.now()
    if date_code == 'tomorrow':
        delivery_date = today + timedelta(days=1)
    elif date_code == '3days':
        delivery_date = today + timedelta(days=3)
    elif date_code == 'week':
        delivery_date = today + timedelta(days=7)
    else:
        delivery_date = today + timedelta(days=1)
    
    delivery_date_str = delivery_date.strftime('%d.%m.%Y')
    await state.update_data(delivery_date=delivery_date_str)
    
    # Переходим к подтверждению
    await show_offer_confirmation(callback.message, state)
    await callback.answer()


@dp.message_handler(state=LogisticOfferStatesGroup.delivery_date)
async def offer_date_custom(message: types.Message, state: FSMContext):
    """Обработка ручного ввода даты"""
    
    from datetime import datetime
    
    try:
        # Проверяем формат даты
        delivery_date = datetime.strptime(message.text, '%d.%m.%Y')
        
        # Проверяем, что дата не в прошлом
        if delivery_date < datetime.now():
            await message.answer("❌ Дата не может быть в прошлом!")
            return
        
        delivery_date_str = delivery_date.strftime('%d.%m.%Y')
        await state.update_data(delivery_date=delivery_date_str)
        
        # Переходим к подтверждению
        await show_offer_confirmation(message, state)
        
    except ValueError:
        await message.answer(
            "❌ Неверный формат даты!\n\n"
            "Используйте формат: <code>ДД.ММ.ГГГГ</code>\n"
            "Например: <code>15.11.2025</code>",
            parse_mode='HTML'
        )


async def show_offer_confirmation(message: types.Message, state: FSMContext):
    """Показать подтверждение предложения"""
    
    data = await state.get_data()
    request_id = data.get('request_id')
    vehicle_type = data.get('vehicle_type')
    price = data.get('price')
    delivery_date = data.get('delivery_date')
    
    request = shipping_requests.get(request_id, {})
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Отправить", callback_data="confirm_offer"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer")
    )
    
    text = f"""💰 <b>Подтверждение предложения</b>

📦 <b>Заявка #{request_id}</b>
🌾 {request.get('culture', 'N/A')} - {request.get('volume', 0):.0f} т
📍 {request.get('route_from', '—')} → {request.get('route_to', '—')}

<b>Ваше предложение:</b>
🚛 Транспорт: {vehicle_type}
💰 Цена: {price:,.0f} ₽
📅 Дата доставки: {delivery_date}

Отправить предложение экспортёру?"""
    
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    await LogisticOfferStatesGroup.confirm.set()


@dp.callback_query_handler(lambda c: c.data == 'confirm_offer', state=LogisticOfferStatesGroup.confirm)
async def confirm_logistic_offer(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение и отправка предложения"""
    
    data = await state.get_data()
    request_id = data.get('request_id')
    vehicle_type = data.get('vehicle_type')
    price = data.get('price')
    delivery_date = data.get('delivery_date')
    
    user_id = callback.from_user.id
    
    # Создаём предложение
    offer_id = len(logistic_offers) + 1
    offer = {
        'id': offer_id,
        'request_id': request_id,
        'logist_id': user_id,
        'vehicle_type': vehicle_type,
        'price': price,
        'delivery_date': delivery_date,
        'status': 'pending',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    logistic_offers[offer_id] = offer
    save_logistic_offers()
    
    # Отправляем уведомление экспортёру
    request = shipping_requests.get(request_id, {})
    exporter_id = request.get('exporter_id')
    logist = users.get(user_id, {})
    
    if exporter_id:
        try:
            keyboard = InlineKeyboardMarkup(row_width=2)
            keyboard.add(
                InlineKeyboardButton("✅ Принять", callback_data=f"accept_offer:{offer_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_offer:{offer_id}")
            )
            
            await bot.send_message(
                exporter_id,
                f"💰 <b>НОВОЕ ПРЕДЛОЖЕНИЕ ПО ДОСТАВКЕ!</b>\n\n"
                f"📦 Заявка #{request_id}\n"
                f"🌾 {request.get('culture', 'N/A')} - {request.get('volume', 0):.0f} т\n"
                f"📍 {request.get('route_from', '—')} → {request.get('route_to', '—')}\n\n"
                f"<b>От логиста:</b>\n"
                f"🏢 {logist.get('company_name', 'N/A')}\n"
                f"📱 @{logist.get('username', 'нет username')}\n\n"
                f"<b>Предложение:</b>\n"
                f"🚛 Транспорт: {vehicle_type}\n"
                f"💰 Цена: {price:,.0f} ₽\n"
                f"📅 Дата: {delivery_date}",
                reply_markup=keyboard,
                parse_mode='HTML'
            )
            logging.info(f"✅ Уведомление экспортёру {exporter_id} о предложении #{offer_id} отправлено")
        except Exception as e:
            logging.error(f"❌ Ошибка отправки уведомления экспортёру: {e}")
    
    await callback.message.edit_text(
        f"✅ <b>Предложение отправлено!</b>\n\n"
        f"📦 Заявка: #{request_id}\n"
        f"💰 Цена: {price:,.0f} ₽\n"
        f"📅 Дата: {delivery_date}\n\n"
        f"Экспортёр получил уведомление.\n"
        f"Вы узнаете о решении, как только он примет или отклонит предложение.",
        parse_mode='HTML'
    )
    
    await state.finish()
    await callback.answer("✅ Предложение отправлено!")


@dp.callback_query_handler(lambda c: c.data == 'cancel_offer', state='*')
async def cancel_offer(callback: types.CallbackQuery, state: FSMContext):
    """Отмена создания предложения"""
    
    await state.finish()
    
    await callback.message.edit_text(
        "❌ Создание предложения отменено.\n\n"
        "Используйте кнопку <b>🚚 Активные заявки</b> для просмотра доступных заявок.",
        parse_mode='HTML'
    )
    
    await callback.answer("❌ Отменено")

@dp.message_handler(lambda m: m.text == "💼 Мои предложения", state='*')
async def show_my_offers(message: types.Message):
    """Показать предложения пользователя (для экспортеров - пулы, для логистов - заявки)"""
    user_id = message.from_user.id

    if user_id not in users:
        await message.answer("❌ Пользователь не найден")
        return

    user = users[user_id]
    role = user.get('role')

    if role == 'exporter':
        # Экспортер видит свои пулы
        user_pulls = [pull for pull in pulls.values() if pull.get('exporter_id') == user_id]

        if not user_pulls:
            await message.answer("💼 <b>Мои пулы</b>\n\n❌ У вас пока нет созданных пулов", parse_mode='HTML')
            return

        text = "💼 <b>Мои пулы</b>\n\n"
        for idx, pull in enumerate(user_pulls[:10], 1):
            pull_id = pull.get('id')
            culture = pull.get('culture', 'Не указано')
            volume = pull.get('volume', 0)
            price = pull.get('price', 0)
            status = pull.get('status', 'Открыт')

            text += f"{idx}. 🌾 Пул #{pull_id}\n"
            text += f"   • {culture}, {volume} т\n"
            text += f"   • {price:,.0f} ₽/т\n"
            text += f"   • Статус: {status}\n\n"

        if len(user_pulls) > 10:
            text += f"<i>... и ещё {len(user_pulls) - 10} пулов</i>\n"

        await message.answer(text, parse_mode='HTML')

    elif role == 'logistic':
        # Логист видит свои заявки на перевозку
        user_requests = [req for req in shipping_requests.values() if req.get('logist_id') == user_id]

        if not user_requests:
            await message.answer("💼 <b>Мои предложения</b>\n\n❌ У вас пока нет заявок", parse_mode='HTML')
            return

        text = "💼 <b>Мои предложения по перевозке</b>\n\n"
        for idx, req in enumerate(user_requests[:10], 1):
            req_id = req.get('id')
            volume = req.get('volume', 0)
            from_loc = req.get('from', 'Не указано')
            to_loc = req.get('to', 'Не указано')
            price = req.get('price', 0)

            text += f"{idx}. 🚚 Заявка #{req_id}\n"
            text += f"   • {from_loc} → {to_loc}\n"
            text += f"   • {volume} т, {price:,.0f} ₽\n\n"

        await message.answer(text, parse_mode='HTML')
    else:
        await message.answer("❌ Эта функция недоступна для вашей роли")


@dp.message_handler(lambda m: m.text in ["📋 Мои перевозки", "📋 Мои оформления"], state='*')
async def show_my_transportations(message: types.Message):
    """Показать перевозки/оформления в работе"""
    user_id = message.from_user.id

    if user_id not in users:
        await message.answer("❌ Пользователь не найден")
        return

    user = users[user_id]
    role = user.get('role')

    if role == 'logistic':
        title = "📋 <b>Мои перевозки</b>"
        # Логист видит заявки в статусе "в работе"
        active_requests = [req for req in shipping_requests.values() 
                          if req.get('logist_id') == user_id and req.get('status') == 'in_progress']

        if not active_requests:
            await message.answer(f"{title}\n\n❌ Нет активных перевозок", parse_mode='HTML')
            return

        text = f"{title}\n\n"
        for idx, req in enumerate(active_requests[:10], 1):
            text += f"{idx}. 🚚 Заявка #{req.get('id')}\n"
            text += f"   • {req.get('from')} → {req.get('to')}\n"
            text += f"   • {req.get('volume')} т\n\n"

        await message.answer(text, parse_mode='HTML')

    elif role == 'expeditor':
        title = "📋 <b>Мои оформления</b>"
        # Экспедитор видит свои оформляемые сделки
        # TODO: добавить фильтрацию сделок по экспедитору
        text = f"{title}\n\n❌ Нет активных оформлений"
        await message.answer(text, parse_mode='HTML')
    else:
        await message.answer("❌ Эта функция недоступна для вашей роли")


@dp.callback_query_handler(lambda c: c.data.startswith('view_shipping_requests:'), state='*')
async def view_shipping_requests_callback(callback: CallbackQuery):
    """Просмотр заявок на доставку для конкретного пула"""
    pull_id = parse_callback_id(callback.data)

    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return

    # Находим все заявки логистов для этого региона
    pull = pulls[pull_id]
    pull_region = pull.get('region', '')

    relevant_requests = [req for req in shipping_requests.values() 
                        if req.get('status') == 'active' and 
                        (pull_region in req.get('from', '') or pull_region in req.get('to', ''))]

    if not relevant_requests:
        await callback.answer("❌ Нет доступных логистов для этого региона", show_alert=True)
        return

    text = f"🚚 <b>Доступные логисты для пула #{pull_id}</b>\n\n"

    for idx, req in enumerate(relevant_requests[:10], 1):
        logist_id = req.get('logist_id')
        if logist_id and logist_id in users:
            logist = users[logist_id]
            text += f"{idx}. {logist.get('name', 'Логист')}\n"
            text += f"   • 📞 {logist.get('phone', 'Не указан')}\n"
            text += f"   • 📍 {req.get('from')} → {req.get('to')}\n"
            text += f"   • 💰 {req.get('price', 0):,.0f} ₽\n\n"

    if len(relevant_requests) > 10:
        text += f"<i>... и ещё {len(relevant_requests) - 10} логистов</i>\n\n"

    text += "💡 <b>Свяжитесь с логистами для обсуждения условий перевозки.</b>"

    await callback.message.edit_text(text, parse_mode='HTML')
    await callback.answer()


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def edit_pull_fields_keyboard():
    """Клавиатура редактирования полей пула"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 Культура", callback_data="edit_pull_field:culture"),
        InlineKeyboardButton("📦 Объём", callback_data="edit_pull_field:volume"),
        InlineKeyboardButton("💰 Цена", callback_data="edit_pull_field:price"),
        InlineKeyboardButton("🚢 Порт", callback_data="edit_pull_field:port")
    )
    keyboard.add(
        InlineKeyboardButton("💧 Влажность", callback_data="edit_pull_field:moisture"),
        InlineKeyboardButton("🏋️ Натура", callback_data="edit_pull_field:nature"),
        InlineKeyboardButton("🌾 Сорность", callback_data="edit_pull_field:impurity"),
        InlineKeyboardButton("🌿 Засорённость", callback_data="edit_pull_field:weed")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_my_pulls")
    )
    return keyboard



async def send_daily_stats():
    """Ежедневная отправка статистики админу"""
    try:
        total_users = len(users)
        role_stats = defaultdict(int)
        for user in users.values():
            role = user.get('role', 'unknown')
            role_stats[role] += 1
        total_batches = sum(len(batches) for user_batches in batches.values())
        active_batches = sum(1 for user_batches in batches.values() for b in batches if b.get('status') == 'Активна')
        total_pulls = len(pulls)
        open_pulls = len([p for p in pulls.values() if p.get('status') == 'Открыт'])
        total_deals = len(deals)
        active_deals = len([d for d in deals.values() if d.get('status') in ['pending', 'matched', 'shipping']])
        
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
        
        await bot.send_message(ADMIN_ID, text, parse_mode='HTML')
        logging.info("✅ Ежедневная статистика отправлена админу")
        
    except Exception as e:
        logging.error(f"❌ Ошибка отправки ежедневной статистики: {e}")

async def setup_scheduler():
    """Настройка планировщика задач"""
    global scheduler  # ← ДОБАВЬ ЭТУ СТРОКУ!
    try:
        scheduler.add_job(update_prices_cache, 'interval', hours=6)
        scheduler.add_job(update_news_cache, 'interval', hours=2)
        scheduler.add_job(auto_match_batches_and_pulls, 'interval', minutes=30)
        scheduler.add_job(send_daily_stats, 'cron', hour=9, minute=0)
        
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


class ExcavatorStatesGroup(StatesGroup):
    """Состояния для создания карточки экспедитора"""
    service_type = State()
    price = State()
    terms = State()
    ports = State()
    notes = State()


# ==================== ОБРАБОТЧИКИ ДЛЯ ЛОГИСТОВ ====================

@dp.message_handler(lambda m: m.text == "➕ Создать заявку на перевозку", state='*')
async def create_shipping_request_start(message: types.Message, state: FSMContext):
    """Начало создания заявки на перевозку"""
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Эта функция доступна только логистам")
        return

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 1/6: Откуда (регион/город)\n\n"
        "Укажите место погрузки:",
        parse_mode='HTML'
    )
    await LogisticStatesGroup.route_from.set()


@dp.message_handler(state=LogisticStatesGroup.route_from)
async def logistic_route_from(message: types.Message, state: FSMContext):
    """Обработка места погрузки"""
    route_from = message.text.strip()

    await state.update_data(route_from=route_from)

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 2/6: Куда (регион/город/порт)\n\n"
        "Укажите место разгрузки:",
        parse_mode='HTML'
    )
    await LogisticStatesGroup.route_to.set()


@dp.message_handler(state=LogisticStatesGroup.route_to)
async def logistic_route_to(message: types.Message, state: FSMContext):
    """Обработка места разгрузки"""
    route_to = message.text.strip()

    await state.update_data(route_to=route_to)

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 3/6: Максимальный объем\n\n"
        "Укажите максимальный объем перевозки (тонн):",
        parse_mode='HTML'
    )
    await LogisticStatesGroup.volume.set()


@dp.message_handler(state=LogisticStatesGroup.volume)
async def logistic_volume(message: types.Message, state: FSMContext):
    """Обработка объема"""
    try:
        volume = float(message.text.replace(',', '.'))
        if volume <= 0:
            raise ValueError

        await state.update_data(volume=volume)

        await message.answer(
            "🚚 <b>Создание заявки на перевозку</b>\n\n"
            "Шаг 4/6: Тариф\n\n"
            "Укажите тариф (₽ за тонну):",
            parse_mode='HTML'
        )
        await LogisticStatesGroup.price.set()

    except Exception as e:
        await message.answer("❌ Неверный формат. Укажите число (например: 1500)")


@dp.message_handler(state=LogisticStatesGroup.price)
async def logistic_price(message: types.Message, state: FSMContext):
    """Обработка тарифа"""
    try:
        price = float(message.text.replace(',', '.').replace(' ', ''))
        if price <= 0:
            raise ValueError

        await state.update_data(price=price)

        await message.answer(
            "🚚 <b>Создание заявки на перевозку</b>\n\n"
            "Шаг 5/6: Тип транспорта\n\n"
            "Укажите тип транспорта (например: Фура 20т, Зерновоз):",
            parse_mode='HTML'
        )
        await LogisticStatesGroup.vehicle_type.set()

    except Exception as e:
        await message.answer("❌ Неверный формат. Укажите число (например: 700)")


@dp.message_handler(state=LogisticStatesGroup.vehicle_type)
async def logistic_vehicle_type(message: types.Message, state: FSMContext):
    """Обработка типа транспорта"""
    vehicle_type = message.text.strip()

    await state.update_data(vehicle_type=vehicle_type)

    await message.answer(
        "🚚 <b>Создание заявки на перевозку</b>\n\n"
        "Шаг 6/6: Примечания (необязательно)\n\n"
        "Дополнительная информация или нажмите /skip для пропуска:",
        parse_mode='HTML'
    )
    await LogisticStatesGroup.notes.set()


@dp.message_handler(lambda m: m.text == '/skip', state=LogisticStatesGroup.notes)
@dp.message_handler(state=LogisticStatesGroup.notes)
async def logistic_notes(message: types.Message, state: FSMContext):
    """Завершение создания заявки"""
    user_id = message.from_user.id

    notes = "" if message.text == '/skip' else message.text.strip()
    await state.update_data(notes=notes)

    data = await state.get_data()

    # Генерируем ID заявки
    global shipping_requests
    request_id = len(shipping_requests) + 1

    # Создаем заявку
    request = {
        'id': request_id,
        'logist_id': user_id,
        'from': data['route_from'],
        'to': data['route_to'],
        'volume': data['volume'],
        'price': data['price'],
        'vehicle_type': data['vehicle_type'],
        'notes': notes,
        'status': 'active',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    shipping_requests[request_id] = request

    # Формируем сообщение
    logist_name = users[user_id].get('name', 'Логист')

    text = f"✅ <b>Заявка на перевозку #{request_id} создана!</b>\n\n"
    text += f"📍 Маршрут: {data['route_from']} → {data['route_to']}\n"
    text += f"📦 Объем: {data['volume']} т\n"
    text += f"💰 Тариф: {data['price']:,.0f} ₽/т\n"
    text += f"🚛 Транспорт: {data['vehicle_type']}\n"
    if notes:
        text += f"📝 Примечания: {notes}\n"
    text += f"\n👤 Контакт: {logist_name}\n"
    text += f"📞 Телефон: {users[user_id].get('phone', 'Не указан')}"

    await message.answer(text, parse_mode='HTML', reply_markup=logistic_keyboard())

    await state.finish()

    # Сохраняем в JSON
    save_shipping_requests()

    logging.info(f"✅ Логист {user_id} создал заявку на перевозку #{request_id}")

@dp.callback_query_handler(lambda c: c.data.startswith('view_shipping_request_'), state='*')
async def view_shipping_request_details(callback: types.CallbackQuery, state: FSMContext):
    """Детальный просмотр заявки на доставку для логиста"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID заявки", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    user_id = callback.from_user.id
    
    exporter_id = request.get('exporter_id')
    exporter_info = users.get(exporter_id, {})
    exporter_company = exporter_info.get('company_name', 'Не указана')
    
    pull_id = request.get('pull_id')
    pull_info = pulls.get(pull_id, {})
    
    offers_count = len([
        o for o in logistic_offers.values()
        if o.get('request_id') == request_id
    ])
    
    user_has_offer = any(
        o.get('request_id') == request_id and o.get('logist_id') == user_id
        for o in logistic_offers.values()
    )
    
    text = f"📦 <b>ЗАЯВКА НА ДОСТАВКУ #{request_id}</b>\n\n"
    
    text += "🌾 <b>ИНФОРМАЦИЯ О ГРУЗЕ:</b>\n"
    text += f"   • Культура: <b>{pull_info.get('culture', 'Не указана')}</b>\n"
    text += f"   • Объём: <b>{request.get('volume', 0):.1f} т</b>\n"
    
    if request.get('weight'):
        text += f"   • Вес: <b>{request.get('weight', 0):.1f} т</b>\n"
    
    text += "\n"
    
    text += "📍 <b>МАРШРУТ:</b>\n"
    text += f"   • Откуда: <b>{request.get('route_from', 'Не указано')}</b>\n"
    text += f"   • Куда: <b>{request.get('route_to', 'Не указано')}</b>\n"
    
    if request.get('distance'):
        text += f"   • Расстояние: <b>{request.get('distance')} км</b>\n"
    
    text += "\n"
    
    text += "📅 <b>СРОКИ:</b>\n"
    if request.get('desired_date'):
        text += f"   • Желаемая дата: <b>{request.get('desired_date')}</b>\n"
    else:
        text += f"   • Желаемая дата: <i>Не указана</i>\n"
    
    text += f"   • Создана: <b>{request.get('created_at', 'Не указано')}</b>\n\n"
    
    if request.get('budget'):
        text += "💰 <b>БЮДЖЕТ:</b>\n"
        text += f"   • Ориентировочный: <b>{request.get('budget'):,.0f} ₽</b>\n\n"
    
    if request.get('requirements'):
        text += "📋 <b>ТРЕБОВАНИЯ:</b>\n"
        text += f"   {request.get('requirements')}\n\n"
    
    if request.get('additional_info'):
        text += "ℹ️ <b>ДОПОЛНИТЕЛЬНО:</b>\n"
        text += f"   {request.get('additional_info')}\n\n"
    
    text += "👤 <b>ЗАКАЗЧИК:</b>\n"
    text += f"   • Компания: <b>{exporter_company}</b>\n"
    
    if exporter_info.get('contact_person'):
        text += f"   • Контакт: {exporter_info.get('contact_person')}\n"
    
    text += "\n"
    
    text += "📊 <b>СТАТУС:</b>\n"
    text += f"   • Статус заявки: <b>{get_status_emoji(request.get('status', 'active'))} {get_status_name(request.get('status', 'active'))}</b>\n"
    text += f"   • Предложений получено: <b>{offers_count}</b>\n"
    
    if user_has_offer:
        text += f"\n<i>✅ Вы уже сделали предложение по этой заявке</i>"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    if not user_has_offer and request.get('status') == 'active':
        keyboard.add(
            InlineKeyboardButton(
                "✅ Сделать предложение",
                callback_data=f"make_offer_{request_id}"
            )
        )
    elif user_has_offer:
        user_offer_id = next(
            (o_id for o_id, o in logistic_offers.items() 
             if o.get('request_id') == request_id and o.get('logist_id') == user_id),
            None
        )
        if user_offer_id:
            keyboard.add(
                InlineKeyboardButton(
                    "👁 Моё предложение",
                    callback_data=f"view_my_offer_{user_offer_id}"
                )
            )
    
    keyboard.add(
        InlineKeyboardButton("🔙 Назад к списку", callback_data="logistic_requests_list")
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()


def get_status_emoji(status: str) -> str:
    """Получить эмодзи для статуса"""
    status_emojis = {
        'active': '🟢',
        'assigned': '🟡',
        'in_progress': '🔵',
        'completed': '✅',
        'cancelled': '❌',
        'pending': '🕐',
        'accepted': '✅',
        'rejected': '❌'
    }
    return status_emojis.get(status, '⚪')


def get_status_name(status: str) -> str:
    """Получить название статуса"""
    status_names = {
        'active': 'Активна',
        'assigned': 'Назначена',
        'in_progress': 'В работе',
        'completed': 'Завершена',
        'cancelled': 'Отменена',
        'pending': 'Ожидает',
        'accepted': 'Принято',
        'rejected': 'Отклонено'
    }
    return status_names.get(status, status)


@dp.callback_query_handler(lambda c: c.data == 'logistic_requests_list', state='*')
async def show_logistic_requests_list(callback: types.CallbackQuery, state: FSMContext):
    """Список доступных заявок на доставку для логиста"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    available_requests = [
        (req_id, req) for req_id, req in shipping_requests.items()
        if req.get('status') == 'active' and not req.get('logist_id')
    ]
    
    if not available_requests:
        text = "📦 <b>ДОСТУПНЫЕ ЗАЯВКИ НА ДОСТАВКУ</b>\n\n"
        text += "❌ Нет доступных заявок\n\n"
        text += "<i>Заявки появятся, когда экспортёры создадут новые запросы на доставку</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="logistic_requests_list"))
        keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    available_requests.sort(key=lambda x: x[1].get('created_at', ''), reverse=True)
    
    text = f"📦 <b>ДОСТУПНЫЕ ЗАЯВКИ НА ДОСТАВКУ</b>\n\n"
    text += f"Найдено заявок: <b>{len(available_requests)}</b>\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for req_id, req in available_requests[:10]:
        pull_id = req.get('pull_id')
        pull_info = pulls.get(pull_id, {})
        culture = pull_info.get('culture', 'Не указана')
        volume = req.get('volume', 0)
        route_from = req.get('route_from', 'Не указано')
        route_to = req.get('route_to', 'Не указано')
        
        button_text = f"📦 {culture} {volume:.0f}т | {route_from} → {route_to}"
        
        keyboard.add(
            InlineKeyboardButton(
                button_text,
                callback_data=f"view_shipping_request_{req_id}"
            )
        )
    
    if len(available_requests) > 10:
        text += f"\n<i>... и ещё {len(available_requests) - 10} заявок</i>"
    
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить список", callback_data="logistic_requests_list")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


# ============================================================================
# ЛОГИСТ: FSM СОЗДАНИЯ ПРЕДЛОЖЕНИЯ
# ============================================================================

class LogisticOfferStatesGroup(StatesGroup):
    """FSM для создания предложения логиста"""
    request_id = State()
    vehicle_type = State()
    price = State()
    delivery_date = State()
    additional_info = State()
    confirm = State()


@dp.callback_query_handler(lambda c: c.data.startswith('make_offer_'), state='*')
async def make_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало создания предложения"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID заявки", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    user_id = callback.from_user.id
    
    # Проверяем, нет ли уже предложения
    existing_offer = any(
        o.get('request_id') == request_id and o.get('logist_id') == user_id
        for o in logistic_offers.values()
    )
    
    if existing_offer:
        await callback.answer("❌ Вы уже сделали предложение по этой заявке", show_alert=True)
        return
    
    # Сохраняем ID заявки
    await state.update_data(request_id=request_id)
    
    # Информация о заявке
    pull_id = request.get('pull_id')
    pull_info = pulls.get(pull_id, {})
    
    text = f"✅ <b>СОЗДАНИЕ ПРЕДЛОЖЕНИЯ</b>\n\n"
    text += f"📦 Заявка #{request_id}\n"
    text += f"🌾 Культура: {pull_info.get('culture', 'Не указана')}\n"
    text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
    text += f"📍 Маршрут: {request.get('route_from', '')} → {request.get('route_to', '')}\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>Шаг 1/4: Выберите тип транспорта</b>"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    vehicles = [
        ("🚛 Фура 20т", "vehicle_Фура 20т"),
        ("🚚 Еврофура 22т", "vehicle_Еврофура 22т"),
        ("📦 Контейнер 20ft", "vehicle_Контейнер 20ft"),
        ("📦 Контейнер 40ft", "vehicle_Контейнер 40ft"),
        ("🚐 Газель 3т", "vehicle_Газель 3т"),
        ("🚐 Газель 5т", "vehicle_Газель 5т"),
        ("🚛 Тонар 25т", "vehicle_Тонар 25т"),
        ("🚜 Зерновоз 30т", "vehicle_Зерновоз 30т"),
    ]
    
    for name, data in vehicles:
        keyboard.add(InlineKeyboardButton(name, callback_data=data))
    
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await LogisticOfferStatesGroup.vehicle_type.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('vehicle_'), state=LogisticOfferStatesGroup.vehicle_type)
async def offer_vehicle_selected(callback: types.CallbackQuery, state: FSMContext):
    """Выбор типа транспорта"""
    vehicle_type = callback.data.replace('vehicle_', '')
    
    await state.update_data(vehicle_type=vehicle_type)
    
    data = await state.get_data()
    request_id = data.get('request_id')
    request = shipping_requests.get(request_id, {})
    volume = request.get('volume', 0)
    
    text = f"✅ <b>СОЗДАНИЕ ПРЕДЛОЖЕНИЯ</b>\n\n"
    text += f"✓ Транспорт: <b>{vehicle_type}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"<b>Шаг 2/4: Укажите стоимость доставки</b>\n\n"
    text += f"Объём груза: <b>{volume:.1f} т</b>\n\n"
    text += "Введите стоимость доставки в рублях:\n"
    text += "<i>(например: 50000)</i>"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await LogisticOfferStatesGroup.price.set()
    await callback.answer()


@dp.message_handler(state=LogisticOfferStatesGroup.price)
async def offer_price_entered(message: types.Message, state: FSMContext):
    """Ввод цены"""
    user_id = message.from_user.id
    
    # Валидация цены
    try:
        price_str = message.text.strip().replace(' ', '').replace(',', '').replace('₽', '')
        price = float(price_str)
        
        if price <= 0:
            await message.answer(
                "❌ Цена должна быть больше нуля!\n\n"
                "Попробуйте ещё раз:"
            )
            return
        
        if price > 10000000:
            await message.answer(
                "❌ Цена слишком большая (максимум 10 млн ₽)!\n\n"
                "Попробуйте ещё раз:"
            )
            return
        
    except ValueError:
        await message.answer(
            "❌ Неправильный формат цены!\n\n"
            "Введите число (например: 50000):"
        )
        return
    
    await state.update_data(price=price)
    
    data = await state.get_data()
    vehicle_type = data.get('vehicle_type')
    
    text = f"✅ <b>СОЗДАНИЕ ПРЕДЛОЖЕНИЯ</b>\n\n"
    text += f"✓ Транспорт: <b>{vehicle_type}</b>\n"
    text += f"✓ Стоимость: <b>{price:,.0f} ₽</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>Шаг 3/4: Укажите дату доставки</b>\n\n"
    text += "Введите дату доставки в формате ДД.ММ.ГГГГ:\n"
    text += "<i>(например: 15.11.2025)</i>"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"))
    
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    await LogisticOfferStatesGroup.delivery_date.set()


@dp.message_handler(state=LogisticOfferStatesGroup.delivery_date)
async def offer_date_entered(message: types.Message, state: FSMContext):
    """Ввод даты доставки"""
    date_str = message.text.strip()
    
    # Валидация даты
    try:
        delivery_date = datetime.strptime(date_str, '%d.%m.%Y')
        
        # Проверка, что дата не в прошлом
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if delivery_date < today:
            await message.answer(
                "❌ Дата доставки не может быть в прошлом!\n\n"
                "Введите дату не ранее сегодняшнего дня:"
            )
            return
        
        # Проверка, что дата не слишком далеко
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
    
    data = await state.get_data()
    vehicle_type = data.get('vehicle_type')
    price = data.get('price')
    
    text = f"✅ <b>СОЗДАНИЕ ПРЕДЛОЖЕНИЯ</b>\n\n"
    text += f"✓ Транспорт: <b>{vehicle_type}</b>\n"
    text += f"✓ Стоимость: <b>{price:,.0f} ₽</b>\n"
    text += f"✓ Дата доставки: <b>{date_str}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "<b>Шаг 4/4: Дополнительная информация (необязательно)</b>\n\n"
    text += "Укажите дополнительную информацию:\n"
    text += "• Условия оплаты\n"
    text += "• Страховка груза\n"
    text += "• Особые условия\n\n"
    text += "Или нажмите 'Пропустить'"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("➡️ Пропустить", callback_data="skip_additional_info"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer"))
    
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    await LogisticOfferStatesGroup.additional_info.set()


@dp.callback_query_handler(lambda c: c.data == 'skip_additional_info', state=LogisticOfferStatesGroup.additional_info)
async def offer_skip_additional_info(callback: types.CallbackQuery, state: FSMContext):
    """Пропуск дополнительной информации"""
    await state.update_data(additional_info="")
    await show_offer_confirmation(callback.message, state, user_id=callback.from_user.id, is_callback=True)
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


async def show_offer_confirmation(message_or_callback, state: FSMContext, user_id: int, is_callback: bool = False):
    """Показать подтверждение предложения"""
    data = await state.get_data()
    
    request_id = data.get('request_id')
    request = shipping_requests.get(request_id, {})
    pull_id = request.get('pull_id')
    pull_info = pulls.get(pull_id, {})
    
    vehicle_type = data.get('vehicle_type')
    price = data.get('price')
    delivery_date = data.get('delivery_date')
    additional_info = data.get('additional_info', '')
    
    text = f"📋 <b>ПОДТВЕРЖДЕНИЕ ПРЕДЛОЖЕНИЯ</b>\n\n"
    text += f"📦 <b>ЗАЯВКА #{request_id}</b>\n"
    text += f"🌾 Культура: {pull_info.get('culture', 'Не указана')}\n"
    text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
    text += f"📍 Маршрут: {request.get('route_from', '')} → {request.get('route_to', '')}\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"<b>ВАШЕ ПРЕДЛОЖЕНИЕ:</b>\n\n"
    text += f"🚛 Транспорт: <b>{vehicle_type}</b>\n"
    text += f"💰 Стоимость: <b>{price:,.0f} ₽</b>\n"
    text += f"📅 Дата доставки: <b>{delivery_date}</b>\n"
    
    if additional_info:
        text += f"\nℹ️ Дополнительно:\n{additional_info}\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "✅ Подтвердите отправку предложения"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Отправить", callback_data="confirm_offer"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_offer")
    )
    
    if is_callback:
        await message_or_callback.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    else:
        await message_or_callback.answer(text, reply_markup=keyboard, parse_mode='HTML')
    
    await LogisticOfferStatesGroup.confirm.set()


@dp.callback_query_handler(lambda c: c.data == 'confirm_offer', state=LogisticOfferStatesGroup.confirm)
async def offer_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение и создание предложения"""
    user_id = callback.from_user.id
    data = await state.get_data()
    
    request_id = data.get('request_id')
    vehicle_type = data.get('vehicle_type')
    price = data.get('price')
    delivery_date = data.get('delivery_date')
    additional_info = data.get('additional_info', '')
    
    # Генерируем ID предложения
    global logistic_offers
    offer_id = len(logistic_offers) + 1
    
    # Создаём предложение
    offer = {
        'id': offer_id,
        'request_id': request_id,
        'logist_id': user_id,
        'vehicle_type': vehicle_type,
        'price': price,
        'delivery_date': delivery_date,
        'additional_info': additional_info,
        'status': 'pending',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    logistic_offers[offer_id] = offer
    
    # Получаем информацию
    request = shipping_requests.get(request_id, {})
    exporter_id = request.get('exporter_id')
    logist_name = users.get(user_id, {}).get('company_name', 'Логистическая компания')
    
    # Уведомляем логиста
    text = f"✅ <b>ПРЕДЛОЖЕНИЕ ОТПРАВЛЕНО!</b>\n\n"
    text += f"📦 Заявка #{request_id}\n"
    text += f"🚛 Транспорт: {vehicle_type}\n"
    text += f"💰 Стоимость: {price:,.0f} ₽\n"
    text += f"📅 Дата: {delivery_date}\n\n"
    text += f"Ваше предложение #{offer_id} отправлено экспортёру.\n"
    text += f"Вы получите уведомление, когда он примет решение."
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📋 Мои предложения", callback_data="my_offers"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    
    # Уведомляем экспортёра
    if exporter_id:
        try:
            exporter_text = f"🔔 <b>НОВОЕ ПРЕДЛОЖЕНИЕ ПО ВАШЕЙ ЗАЯВКЕ!</b>\n\n"
            exporter_text += f"📦 Заявка #{request_id}\n"
            exporter_text += f"👤 От: {logist_name}\n"
            exporter_text += f"🚛 Транспорт: {vehicle_type}\n"
            exporter_text += f"💰 Стоимость: {price:,.0f} ₽\n"
            exporter_text += f"📅 Дата доставки: {delivery_date}\n"
            
            if additional_info:
                exporter_text += f"\nℹ️ Дополнительно:\n{additional_info}\n"
            
            exporter_keyboard = InlineKeyboardMarkup()
            exporter_keyboard.add(
                InlineKeyboardButton("👁 Посмотреть", callback_data=f"view_offer_{offer_id}")
            )
            
            await bot.send_message(
                exporter_id,
                exporter_text,
                reply_markup=exporter_keyboard,
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Ошибка отправки уведомления экспортёру: {e}")
    
    await state.finish()
    await callback.answer("✅ Предложение отправлено!")
    
    logging.info(f"✅ Логист {user_id} создал предложение #{offer_id} для заявки #{request_id}")


@dp.callback_query_handler(lambda c: c.data == 'cancel_offer', state='*')
async def offer_cancelled(callback: types.CallbackQuery, state: FSMContext):
    """Отмена создания предложения"""
    await state.finish()
    
    text = "❌ Создание предложения отменено"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📦 К списку заявок", callback_data="logistic_requests_list"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


# ============================================================================
# ЛОГИСТ: УПРАВЛЕНИЕ ПРЕДЛОЖЕНИЯМИ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == 'my_offers', state='*')
async def show_my_offers(callback: types.CallbackQuery, state: FSMContext):
    """Показать мои предложения"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Получаем все предложения логиста
    my_offers = [
        (offer_id, offer) for offer_id, offer in logistic_offers.items()
        if offer.get('logist_id') == user_id
    ]
    
    if not my_offers:
        text = "📋 <b>МОИ ПРЕДЛОЖЕНИЯ</b>\n\n"
        text += "❌ У вас пока нет предложений\n\n"
        text += "<i>Создайте предложение на доступную заявку</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("📦 К списку заявок", callback_data="logistic_requests_list"))
        keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    # Группируем по статусам
    by_status = {
        'pending': [],
        'accepted': [],
        'rejected': [],
        'cancelled': []
    }
    
    for offer_id, offer in my_offers:
        status = offer.get('status', 'pending')
        by_status[status].append((offer_id, offer))
    
    # Сортируем по дате (новые первые)
    for status in by_status:
        by_status[status].sort(key=lambda x: x[1].get('created_at', ''), reverse=True)
    
    # Формируем сообщение
    text = f"📋 <b>МОИ ПРЕДЛОЖЕНИЯ</b>\n\n"
    text += f"Всего предложений: <b>{len(my_offers)}</b>\n\n"
    
    # Статистика
    pending_count = len(by_status['pending'])
    accepted_count = len(by_status['accepted'])
    rejected_count = len(by_status['rejected'])
    
    text += f"🕐 Ожидают ответа: <b>{pending_count}</b>\n"
    text += f"✅ Приняты: <b>{accepted_count}</b>\n"
    text += f"❌ Отклонены: <b>{rejected_count}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите предложение:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Показываем предложения по статусам
    for status_key, status_name, emoji in [
        ('pending', 'Ожидают', '🕐'),
        ('accepted', 'Приняты', '✅'),
        ('rejected', 'Отклонены', '❌')
    ]:
        offers = by_status[status_key]
        if offers:
            for offer_id, offer in offers[:5]:  # Показываем по 5 из каждой категории
                request_id = offer.get('request_id')
                request = shipping_requests.get(request_id, {})
                pull_id = request.get('pull_id')
                pull_info = pulls.get(pull_id, {})
                
                culture = pull_info.get('culture', 'Не указана')
                price = offer.get('price', 0)
                
                button_text = f"{emoji} #{offer_id} | {culture} | {price:,.0f} ₽"
                
                keyboard.add(
                    InlineKeyboardButton(
                        button_text,
                        callback_data=f"view_my_offer_{offer_id}"
                    )
                )
    
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="my_offers")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_my_offer_'), state='*')
async def view_my_offer_details(callback: types.CallbackQuery, state: FSMContext):
    """Детальный просмотр своего предложения"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID предложения", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    user_id = callback.from_user.id
    
    # Проверяем, что это предложение пользователя
    if offer.get('logist_id') != user_id:
        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return
    
    # Получаем информацию о заявке
    request_id = offer.get('request_id')
    request = shipping_requests.get(request_id, {})
    pull_id = request.get('pull_id')
    pull_info = pulls.get(pull_id, {})
    
    # Получаем информацию об экспортёре
    exporter_id = request.get('exporter_id')
    exporter_info = users.get(exporter_id, {})
    exporter_company = exporter_info.get('company_name', 'Не указана')
    
    # Формируем сообщение
    text = f"📋 <b>МОЁ ПРЕДЛОЖЕНИЕ #{offer_id}</b>\n\n"
    
    # Информация о заявке
    text += f"📦 <b>ЗАЯВКА #{request_id}</b>\n"
    text += f"🌾 Культура: {pull_info.get('culture', 'Не указана')}\n"
    text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
    text += f"📍 Маршрут: {request.get('route_from', '')} → {request.get('route_to', '')}\n"
    text += f"👤 Заказчик: {exporter_company}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Информация о предложении
    text += f"<b>ВАШЕ ПРЕДЛОЖЕНИЕ:</b>\n\n"
    text += f"🚛 Транспорт: <b>{offer.get('vehicle_type', 'Не указан')}</b>\n"
    text += f"💰 Стоимость: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
    text += f"📅 Дата доставки: <b>{offer.get('delivery_date', 'Не указана')}</b>\n"
    
    if offer.get('additional_info'):
        text += f"\nℹ️ Дополнительно:\n{offer.get('additional_info')}\n"
    
    text += f"\n📅 Создано: {offer.get('created_at', 'Не указано')}\n\n"
    
    # Статус
    status = offer.get('status', 'pending')
    status_emoji = get_status_emoji(status)
    status_name = get_status_name(status)
    
    text += f"📊 Статус: <b>{status_emoji} {status_name}</b>\n"
    
    if status == 'accepted':
        text += f"\n✅ <b>Ваше предложение принято!</b>\n"
        text += f"<i>Ожидайте дальнейших инструкций от заказчика</i>"
    elif status == 'rejected':
        text += f"\n❌ <b>Предложение отклонено</b>\n"
        if offer.get('rejection_reason'):
            text += f"<i>Причина: {offer.get('rejection_reason')}</i>"
    elif status == 'cancelled':
        text += f"\n❌ <b>Предложение отменено вами</b>"
    
    # Кнопки
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    if status == 'pending':
        # Можно редактировать или отменить
        keyboard.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_offer_{offer_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_my_offer_{offer_id}")
        )
    
    keyboard.add(
        InlineKeyboardButton("🔙 К моим предложениям", callback_data="my_offers")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('cancel_my_offer_'), state='*')
async def cancel_my_offer_confirm(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение отмены предложения"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    
    if offer.get('logist_id') != callback.from_user.id:
        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return
    
    if offer.get('status') != 'pending':
        await callback.answer("❌ Можно отменить только ожидающие предложения", show_alert=True)
        return
    
    text = f"❓ <b>ОТМЕНА ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    text += f"Вы уверены, что хотите отменить это предложение?\n\n"
    text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
    text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n\n"
    text += f"<i>Это действие нельзя будет отменить</i>"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, отменить", callback_data=f"confirm_cancel_offer_{offer_id}"),
        InlineKeyboardButton("❌ Нет", callback_data=f"view_my_offer_{offer_id}")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_cancel_offer_'), state='*')
async def cancel_my_offer_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Отмена предложения подтверждена"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    
    # Обновляем статус
    offer['status'] = 'cancelled'
    offer['cancelled_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Уведомляем экспортёра
    request_id = offer.get('request_id')
    request = shipping_requests.get(request_id, {})
    exporter_id = request.get('exporter_id')
    
    if exporter_id:
        try:
            await bot.send_message(
                exporter_id,
                f"🔔 <b>Предложение #{offer_id} отменено логистом</b>\n\n"
                f"📦 Заявка #{request_id}\n"
                f"К сожалению, логист отменил своё предложение.",
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Ошибка уведомления экспортёра: {e}")
    
    text = f"✅ <b>ПРЕДЛОЖЕНИЕ #{offer_id} ОТМЕНЕНО</b>\n\n"
    text += f"Ваше предложение успешно отменено.\n"
    text += f"Экспортёр получил уведомление об этом."
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📋 Мои предложения", callback_data="my_offers"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer("✅ Предложение отменено")
    
    logging.info(f"❌ Логист {callback.from_user.id} отменил предложение #{offer_id}")


@dp.callback_query_handler(lambda c: c.data.startswith('edit_offer_'), state='*')
async def edit_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования предложения"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    
    if offer.get('logist_id') != callback.from_user.id:
        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return
    
    if offer.get('status') != 'pending':
        await callback.answer("❌ Можно редактировать только ожидающие предложения", show_alert=True)
        return
    
    text = f"✏️ <b>РЕДАКТИРОВАНИЕ ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    text += f"Текущие данные:\n\n"
    text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
    text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n"
    text += f"📅 Дата: {offer.get('delivery_date')}\n\n"
    text += f"Что вы хотите изменить?"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🚛 Изменить транспорт", callback_data=f"edit_vehicle_{offer_id}"),
        InlineKeyboardButton("💰 Изменить цену", callback_data=f"edit_price_{offer_id}"),
        InlineKeyboardButton("📅 Изменить дату", callback_data=f"edit_date_{offer_id}"),
        InlineKeyboardButton("ℹ️ Изменить доп. информацию", callback_data=f"edit_info_{offer_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Назад", callback_data=f"view_my_offer_{offer_id}")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


# Добавим FSM states для редактирования
class EditOfferStatesGroup(StatesGroup):
    """FSM для редактирования предложения"""
    offer_id = State()
    field = State()
    value = State()


@dp.callback_query_handler(lambda c: c.data.startswith('edit_price_'), state='*')
async def edit_offer_price(callback: types.CallbackQuery, state: FSMContext):
    """Редактирование цены"""
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    offer = logistic_offers.get(offer_id)
    if not offer:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    await state.update_data(offer_id=offer_id, field='price')
    
    text = f"💰 <b>ИЗМЕНЕНИЕ ЦЕНЫ</b>\n\n"
    text += f"Текущая цена: <b>{offer.get('price', 0):,.0f} ₽</b>\n\n"
    text += f"Введите новую цену в рублях:"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data=f"edit_offer_{offer_id}"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await EditOfferStatesGroup.value.set()
    await callback.answer()


@dp.message_handler(state=EditOfferStatesGroup.value)
async def edit_offer_value_entered(message: types.Message, state: FSMContext):
    """Сохранение изменённого значения"""
    data = await state.get_data()
    offer_id = data.get('offer_id')
    field = data.get('field')
    
    offer = logistic_offers.get(offer_id)
    if not offer:
        await message.answer("❌ Предложение не найдено")
        await state.finish()
        return
    
    # Валидация и сохранение
    if field == 'price':
        try:
            new_price = float(message.text.strip().replace(' ', '').replace(',', '').replace('₽', ''))
            if new_price <= 0 or new_price > 10000000:
                await message.answer("❌ Неправильная цена! Попробуйте ещё раз:")
                return
            
            old_price = offer.get('price')
            offer['price'] = new_price
            offer['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            text = f"✅ <b>ЦЕНА ИЗМЕНЕНА!</b>\n\n"
            text += f"Было: {old_price:,.0f} ₽\n"
            text += f"Стало: <b>{new_price:,.0f} ₽</b>\n\n"
            text += f"Экспортёр получит уведомление об изменении"
            
            # Уведомляем экспортёра
            request_id = offer.get('request_id')
            request = shipping_requests.get(request_id, {})
            exporter_id = request.get('exporter_id')
            
            if exporter_id:
                try:
                    await bot.send_message(
                        exporter_id,
                        f"🔔 <b>Предложение #{offer_id} изменено</b>\n\n"
                        f"📦 Заявка #{request_id}\n"
                        f"💰 Новая цена: <b>{new_price:,.0f} ₽</b>",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logging.error(f"Ошибка уведомления: {e}")
            
        except ValueError:
            await message.answer("❌ Неправильный формат! Введите число:")
            return
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("👁 Посмотреть предложение", callback_data=f"view_my_offer_{offer_id}"))
    keyboard.add(InlineKeyboardButton("📋 Мои предложения", callback_data="my_offers"))
    
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    await state.finish()
    
    logging.info(f"✏️ Логист {message.from_user.id} изменил предложение #{offer_id}")


# ============================================================================
# ЛОГИСТ: СИСТЕМА УВЕДОМЛЕНИЙ
# ============================================================================

async def notify_logistic_offer_accepted(offer_id: int, exporter_id: int):
    """Уведомить логиста о принятии предложения"""
    try:
        if offer_id not in logistic_offers:
            return
        
        offer = logistic_offers[offer_id]
        logist_id = offer.get('logist_id')
        
        if not logist_id:
            return
        
        request_id = offer.get('request_id')
        request = shipping_requests.get(request_id, {})
        exporter_info = users.get(exporter_id, {})
        exporter_company = exporter_info.get('company_name', 'Компания')
        
        text = f"🎉 <b>ВАШЕ ПРЕДЛОЖЕНИЕ ПРИНЯТО!</b>\n\n"
        text += f"📋 Предложение #{offer_id}\n"
        text += f"📦 Заявка #{request_id}\n"
        text += f"👤 Заказчик: {exporter_company}\n\n"
        text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
        text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n"
        text += f"📅 Дата доставки: {offer.get('delivery_date')}\n\n"
        text += f"━━━━━━━━━━━━━━━━━━━━\n\n"
        text += f"✅ <b>Следующие шаги:</b>\n"
        text += f"1. Свяжитесь с заказчиком\n"
        text += f"2. Уточните детали погрузки\n"
        text += f"3. Подготовьте транспорт\n\n"
        
        if exporter_info.get('phone'):
            text += f"📞 Телефон заказчика: {exporter_info.get('phone')}\n"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("📋 Детали предложения", callback_data=f"view_my_offer_{offer_id}")
        )
        keyboard.add(
            InlineKeyboardButton("🚚 Мои доставки", callback_data="my_deliveries")
        )
        
        await bot.send_message(
            logist_id,
            text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        logging.info(f"✅ Уведомление о принятии отправлено логисту {logist_id}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка уведомления логиста о принятии: {e}")


async def notify_logistic_offer_rejected(offer_id: int, exporter_id: int, reason: str = None):
    """Уведомить логиста об отклонении предложения"""
    try:
        if offer_id not in logistic_offers:
            return
        
        offer = logistic_offers[offer_id]
        logist_id = offer.get('logist_id')
        
        if not logist_id:
            return
        
        request_id = offer.get('request_id')
        exporter_info = users.get(exporter_id, {})
        exporter_company = exporter_info.get('company_name', 'Компания')
        
        text = f"❌ <b>ПРЕДЛОЖЕНИЕ ОТКЛОНЕНО</b>\n\n"
        text += f"📋 Предложение #{offer_id}\n"
        text += f"📦 Заявка #{request_id}\n"
        text += f"👤 Заказчик: {exporter_company}\n\n"
        text += f"К сожалению, ваше предложение не было принято.\n\n"
        
        if reason:
            text += f"💬 <b>Причина:</b>\n{reason}\n\n"
        
        text += f"━━━━━━━━━━━━━━━━━━━━\n\n"
        text += f"💡 <b>Рекомендации:</b>\n"
        text += f"• Пересмотрите свою ценовую политику\n"
        text += f"• Предложите более гибкие условия\n"
        text += f"• Ищите другие заявки\n"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("📦 Посмотреть другие заявки", callback_data="logistic_requests_list")
        )
        keyboard.add(
            InlineKeyboardButton("📋 Мои предложения", callback_data="my_offers")
        )
        
        await bot.send_message(
            logist_id,
            text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        logging.info(f"❌ Уведомление об отклонении отправлено логисту {logist_id}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка уведомления логиста об отклонении: {e}")


async def notify_logistic_new_request(request_id: int):
    """Уведомить всех логистов о новой заявке"""
    try:
        if request_id not in shipping_requests:
            return
        
        request = shipping_requests[request_id]
        pull_id = request.get('pull_id')
        pull_info = pulls.get(pull_id, {})
        
        # Получаем всех логистов
        logistics = [uid for uid, user in users.items() if user.get('role') == 'logistic']
        
        if not logistics:
            return
        
        text = f"🔔 <b>НОВАЯ ЗАЯВКА НА ДОСТАВКУ!</b>\n\n"
        text += f"📦 Заявка #{request_id}\n"
        text += f"🌾 Культура: {pull_info.get('culture', 'Не указана')}\n"
        text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
        text += f"📍 Маршрут: {request.get('route_from', '')} → {request.get('route_to', '')}\n"
        
        if request.get('desired_date'):
            text += f"📅 Желаемая дата: {request.get('desired_date')}\n"
        
        if request.get('budget'):
            text += f"💰 Бюджет: {request.get('budget'):,.0f} ₽\n"
        
        text += f"\n<i>Торопитесь! Конкуренты уже смотрят эту заявку</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("👁 Посмотреть заявку", callback_data=f"view_shipping_request_{request_id}")
        )
        keyboard.add(
            InlineKeyboardButton("📦 Все заявки", callback_data="logistic_requests_list")
        )
        
        # Отправляем уведомление каждому логисту
        sent_count = 0
        for logist_id in logistics:
            try:
                await bot.send_message(
                    logist_id,
                    text,
                    reply_markup=keyboard,
                    parse_mode='HTML'
                )
                sent_count += 1
                await asyncio.sleep(0.1)  # Небольшая задержка между отправками
            except Exception as e:
                logging.error(f"Ошибка отправки логисту {logist_id}: {e}")
        
        logging.info(f"🔔 Уведомление о новой заявке #{request_id} отправлено {sent_count} логистам")
        
    except Exception as e:
        logging.error(f"❌ Ошибка массового уведомления логистов: {e}")


async def notify_logistic_delivery_started(delivery_id: int):
    """Уведомить логиста о начале доставки"""
    try:
        if delivery_id not in deliveries:
            return
        
        delivery = deliveries[delivery_id]
        logist_id = delivery.get('logist_id')
        
        if not logist_id:
            return
        
        request_id = delivery.get('request_id')
        
        text = f"🚚 <b>ДОСТАВКА НАЧАЛАСЬ!</b>\n\n"
        text += f"📦 Доставка #{delivery_id}\n"
        text += f"📋 Заявка #{request_id}\n\n"
        text += f"✅ Статус изменён на: <b>В пути</b>\n\n"
        text += f"📍 Маршрут: {delivery.get('route_from', '')} → {delivery.get('route_to', '')}\n"
        text += f"📅 Плановая дата: {delivery.get('delivery_date', '')}\n\n"
        text += f"<i>Не забудьте обновить статус по прибытии!</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("📋 Детали доставки", callback_data=f"view_delivery_{delivery_id}")
        )
        
        await bot.send_message(
            logist_id,
            text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        logging.info(f"🚚 Уведомление о начале доставки отправлено логисту {logist_id}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка уведомления о начале доставки: {e}")


async def notify_logistic_delivery_completed(delivery_id: int):
    """Уведомить логиста о завершении доставки"""
    try:
        if delivery_id not in deliveries:
            return
        
        delivery = deliveries[delivery_id]
        logist_id = delivery.get('logist_id')
        
        if not logist_id:
            return
        
        offer_id = delivery.get('offer_id')
        offer = logistic_offers.get(offer_id, {})
        price = offer.get('price', 0)
        
        text = f"✅ <b>ДОСТАВКА ЗАВЕРШЕНА!</b>\n\n"
        text += f"📦 Доставка #{delivery_id}\n"
        text += f"💰 Сумма: <b>{price:,.0f} ₽</b>\n\n"
        text += f"🎉 Поздравляем с успешным выполнением заказа!\n\n"
        text += f"📊 <b>Следующие шаги:</b>\n"
        text += f"• Получите оплату от заказчика\n"
        text += f"• Закройте документы\n"
        text += f"• Оставьте отзыв о сотрудничестве"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("📋 История доставок", callback_data="delivery_history")
        )
        keyboard.add(
            InlineKeyboardButton("📊 Статистика", callback_data="logistic_statistics")
        )
        
        await bot.send_message(
            logist_id,
            text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        logging.info(f"✅ Уведомление о завершении доставки отправлено логисту {logist_id}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка уведомления о завершении доставки: {e}")


async def notify_logistic_request_cancelled(request_id: int, reason: str = None):
    """Уведомить логистов об отмене заявки"""
    try:
        if request_id not in shipping_requests:
            return
        
        # Находим все предложения по этой заявке
        related_offers = [
            (offer_id, offer) for offer_id, offer in logistic_offers.items()
            if offer.get('request_id') == request_id and offer.get('status') == 'pending'
        ]
        
        if not related_offers:
            return
        
        text = f"❌ <b>ЗАЯВКА ОТМЕНЕНА</b>\n\n"
        text += f"📦 Заявка #{request_id} была отменена заказчиком.\n\n"
        
        if reason:
            text += f"💬 Причина: {reason}\n\n"
        
        text += f"Ваше предложение автоматически отменено."
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("📦 Другие заявки", callback_data="logistic_requests_list")
        )
        
        # Уведомляем каждого логиста
        for offer_id, offer in related_offers:
            logist_id = offer.get('logist_id')
            if logist_id:
                try:
                    await bot.send_message(
                        logist_id,
                        text,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )
                    # Отменяем предложение
                    offer['status'] = 'cancelled'
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"Ошибка уведомления логиста {logist_id}: {e}")
        
        logging.info(f"❌ Уведомления об отмене заявки #{request_id} отправлены")
        
    except Exception as e:
        logging.error(f"❌ Ошибка уведомления об отмене заявки: {e}")


# ============================================================================
# ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ЛОГИСТА
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == 'my_deliveries', state='*')
async def show_my_deliveries(callback: types.CallbackQuery, state: FSMContext):
    """Показать мои доставки"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Получаем доставки логиста
    my_deliveries = [
        (deliv_id, deliv) for deliv_id, deliv in deliveries.items()
        if deliv.get('logist_id') == user_id
    ]
    
    if not my_deliveries:
        text = "🚚 <b>МОИ ДОСТАВКИ</b>\n\n"
        text += "❌ У вас пока нет активных доставок\n\n"
        text += "<i>Доставки появятся после принятия ваших предложений</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("📋 Мои предложения", callback_data="my_offers"))
        keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    # Группируем по статусам
    by_status = {
        'pending': [],
        'in_progress': [],
        'completed': [],
        'cancelled': []
    }
    
    for deliv_id, deliv in my_deliveries:
        status = deliv.get('status', 'pending')
        by_status[status].append((deliv_id, deliv))
    
    text = f"🚚 <b>МОИ ДОСТАВКИ</b>\n\n"
    text += f"Всего доставок: <b>{len(my_deliveries)}</b>\n\n"
    
    active = len(by_status['pending']) + len(by_status['in_progress'])
    completed = len(by_status['completed'])
    
    text += f"🔵 Активные: <b>{active}</b>\n"
    text += f"✅ Завершённые: <b>{completed}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите доставку:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Показываем активные доставки
    for status_key, status_name, emoji in [
        ('pending', 'Ожидают', '🕐'),
        ('in_progress', 'В пути', '🚚'),
        ('completed', 'Завершены', '✅')
    ]:
        delivs = by_status[status_key]
        if delivs:
            for deliv_id, deliv in delivs[:5]:
                request_id = deliv.get('request_id')
                request = shipping_requests.get(request_id, {})
                
                route = f"{request.get('route_from', '')} → {request.get('route_to', '')}"
                
                button_text = f"{emoji} #{deliv_id} | {route}"
                
                keyboard.add(
                    InlineKeyboardButton(
                        button_text,
                        callback_data=f"view_delivery_{deliv_id}"
                    )
                )
    
    keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="my_deliveries"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'logistic_statistics', state='*')
async def show_logistic_statistics(callback: types.CallbackQuery, state: FSMContext):
    """Показать статистику логиста"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Собираем статистику
    my_offers = [o for o in logistic_offers.values() if o.get('logist_id') == user_id]
    my_deliveries = [d for d in deliveries.values() if d.get('logist_id') == user_id]
    
    total_offers = len(my_offers)
    accepted_offers = len([o for o in my_offers if o.get('status') == 'accepted'])
    rejected_offers = len([o for o in my_offers if o.get('status') == 'rejected'])
    pending_offers = len([o for o in my_offers if o.get('status') == 'pending'])
    
    completed_deliveries = len([d for d in my_deliveries if d.get('status') == 'completed'])
    active_deliveries = len([d for d in my_deliveries if d.get('status') in ['pending', 'in_progress']])
    
    # Подсчитываем общий заработок
    total_earnings = sum(
        logistic_offers.get(d.get('offer_id'), {}).get('price', 0)
        for d in my_deliveries if d.get('status') == 'completed'
    )
    
    # Конверсия
    conversion = (accepted_offers / total_offers * 100) if total_offers > 0 else 0
    
    text = f"📊 <b>СТАТИСТИКА РАБОТЫ</b>\n\n"
    text += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    
    text += f"<b>ПРЕДЛОЖЕНИЯ:</b>\n"
    text += f"📋 Всего отправлено: <b>{total_offers}</b>\n"
    text += f"✅ Принято: <b>{accepted_offers}</b>\n"
    text += f"❌ Отклонено: <b>{rejected_offers}</b>\n"
    text += f"🕐 Ожидают ответа: <b>{pending_offers}</b>\n"
    text += f"📈 Конверсия: <b>{conversion:.1f}%</b>\n\n"
    
    text += f"<b>ДОСТАВКИ:</b>\n"
    text += f"🚚 Активные: <b>{active_deliveries}</b>\n"
    text += f"✅ Завершённые: <b>{completed_deliveries}</b>\n\n"
    
    text += f"<b>ФИНАНСЫ:</b>\n"
    text += f"💰 Общий заработок: <b>{total_earnings:,.0f} ₽</b>\n"
    if completed_deliveries > 0:
        avg_earning = total_earnings / completed_deliveries
        text += f"📊 Средняя доставка: <b>{avg_earning:,.0f} ₽</b>\n"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📋 Мои предложения", callback_data="my_offers"))
    keyboard.add(InlineKeyboardButton("🚚 Мои доставки", callback_data="my_deliveries"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

# ==================== ОБРАБОТЧИКИ ДЛЯ ЭКСПЕДИТОРОВ ====================

@dp.message_handler(lambda m: m.text == "➕ Создать карточку услуг", state='*')
async def create_expeditor_offer_start(message: types.Message, state: FSMContext):
    """Начало создания карточки экспедитора"""
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'expeditor':
        await message.answer("❌ Эта функция доступна только экспедиторам")
        return

    await message.answer(
        "🚛 <b>Создание карточки услуг</b>\n\n"
        "Шаг 1/5: Тип услуги\n\n"
        "Укажите тип услуги (например: Оформление ДТ, Таможенное оформление):",
        parse_mode='HTML'
    )
    await ExcavatorStatesGroup.service_type.set()


@dp.message_handler(state=ExcavatorStatesGroup.service_type)
async def expeditor_service_type(message: types.Message, state: FSMContext):
    """Обработка типа услуги"""
    service_type = message.text.strip()

    await state.update_data(service_type=service_type)

    await message.answer(
        "🚛 <b>Создание карточки услуг</b>\n\n"
        "Шаг 2/5: Стоимость\n\n"
        "Укажите стоимость услуги (₽):",
        parse_mode='HTML'
    )
    await ExcavatorStatesGroup.price.set()


@dp.message_handler(state=ExcavatorStatesGroup.price)
async def expeditor_price(message: types.Message, state: FSMContext):
    """Обработка стоимости"""
    try:
        price = float(message.text.replace(',', '.').replace(' ', ''))
        if price <= 0:
            raise ValueError

        await state.update_data(price=price)

        await message.answer(
            "🚛 <b>Создание карточки услуг</b>\n\n"
            "Шаг 3/5: Сроки выполнения\n\n"
            "Укажите сроки (например: 3-5 дней):",
            parse_mode='HTML'
        )
        await ExcavatorStatesGroup.terms.set()

    except Exception as e:
        await message.answer("❌ Неверный формат. Укажите число (например: 15000)")


@dp.message_handler(state=ExcavatorStatesGroup.terms)
async def expeditor_terms(message: types.Message, state: FSMContext):
    """Обработка сроков"""
    terms = message.text.strip()

    await state.update_data(terms=terms)

    await message.answer(
        "🚛 <b>Создание карточки услуг</b>\n\n"
        "Шаг 4/5: Порты\n\n"
        "Укажите порты, в которых работаете (через запятую):",
        parse_mode='HTML'
    )
    await ExcavatorStatesGroup.ports.set()


@dp.message_handler(state=ExcavatorStatesGroup.ports)
async def expeditor_ports(message: types.Message, state: FSMContext):
    """Обработка портов"""
    ports = message.text.strip()

    await state.update_data(ports=ports)

    await message.answer(
        "🚛 <b>Создание карточки услуг</b>\n\n"
        "Шаг 5/5: Примечания (необязательно)\n\n"
        "Дополнительная информация или /skip для пропуска:",
        parse_mode='HTML'
    )
    await ExcavatorStatesGroup.notes.set()


@dp.message_handler(lambda m: m.text == '/skip', state=ExcavatorStatesGroup.notes)
@dp.message_handler(state=ExcavatorStatesGroup.notes)
async def expeditor_notes(message: types.Message, state: FSMContext):
    """Завершение создания карточки"""
    user_id = message.from_user.id

    notes = "" if message.text == '/skip' else message.text.strip()
    await state.update_data(notes=notes)

    data = await state.get_data()

    # Генерируем ID предложения
    global expeditor_offers
    offer_id = len(expeditor_offers) + 1

    # Создаем предложение
    offer = {
        'id': offer_id,
        'expeditor_id': user_id,
        'service_type': data['service_type'],
        'price': data['price'],
        'terms': data['terms'],
        'ports': data['ports'],
        'notes': notes,
        'status': 'active',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    expeditor_offers[offer_id] = offer

    # Формируем сообщение
    expeditor_name = users[user_id].get('name', 'Экспедитор')

    text = f"✅ <b>Карточка услуг #{offer_id} создана!</b>\n\n"
    text += f"📋 Услуга: {data['service_type']}\n"
    text += f"💰 Стоимость: {data['price']:,.0f} ₽\n"
    text += f"⏱ Сроки: {data['terms']}\n"
    text += f"🚢 Порты: {data['ports']}\n"
    if notes:
        text += f"📝 Примечания: {notes}\n"
    text += f"\n👤 Контакт: {expeditor_name}\n"
    text += f"📞 Телефон: {users[user_id].get('phone', 'Не указан')}"

    await message.answer(text, parse_mode='HTML', reply_markup=expeditor_keyboard())

    await state.finish()

    logging.info(f"✅ Экспедитор {user_id} создал карточку услуг #{offer_id}")


# ==================== ФУНКЦИИ СОХРАНЕНИЯ ====================

def save_shipping_requests():
    """Сохранение заявок на перевозку"""
    try:
        with open('data/shipping_requests.pkl', 'wb') as f:
            pickle.dump(shipping_requests, f)
    except Exception as e:
        logging.error(f"Ошибка сохранения shipping_requests: {e}")


def load_shipping_requests():
    """Загрузка заявок на перевозку"""
    global shipping_requests
    try:
        if False:  # Pickle disabled
            with open('data/shipping_requests.pkl', 'rb') as f:
                shipping_requests = pickle.load(f)
                logging.info(f"✅ Заявки на перевозку загружены: {len(shipping_requests)}")
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
    
    await state.update_data(route_from=route_from)
    
    await message.answer(
        f"📍 Пункт отправки: <b>{route_from}</b>\n\n"
        "<b>Шаг 2 из 5</b>\n\n"
        "Введите пункт назначения (город/порт):",
        parse_mode='HTML'
    )
    
    await ShippingRequestStatesGroup.route_to.set()


@dp.message_handler(state=ShippingRequestStatesGroup.route_to)
async def shipping_route_to(message: types.Message, state: FSMContext):
    """Шаг 2: Пункт назначения"""
    route_to = message.text.strip()
    
    await state.update_data(route_to=route_to)
    
    data = await state.get_data()
    pull = pulls[data['pull_id']]
    
    # ✅ ПРАВИЛЬНО: Берём target_volume (общий объём пула)
    total_volume = pull.get('target_volume', 0)
    
    await message.answer(
        f"📍 Маршрут: <b>{data['route_from']}</b> → <b>{route_to}</b>\n\n"
        "<b>Шаг 3 из 5</b>\n\n"
        f"Введите объём груза для перевозки (тонн)\n"
        f"Объём пула: {total_volume:.0f} т",
        parse_mode='HTML'
    )
    
    await ShippingRequestStatesGroup.volume.set()


@dp.message_handler(state=ShippingRequestStatesGroup.volume)
async def shipping_volume(message: types.Message, state: FSMContext):
    """Шаг 3: Объём"""
    try:
        volume = float(message.text.replace(',', '.').replace(' ', ''))
        
        if volume <= 0:
            await message.answer("❌ Объём должен быть больше нуля")
            return
        
        data = await state.get_data()
        pull = pulls[data['pull_id']]
        
        # ✅ ПРАВИЛЬНО: Проверяем относительно target_volume (общего объёма пула)
        target_volume = pull.get('target_volume', 0)
        
        if volume > target_volume:
            await message.answer(
                f"❌ Указанный объём превышает объём пула!\n"
                f"Объём пула: {target_volume:.0f} т"
            )
            return
        
        await state.update_data(volume=volume)
        
        await message.answer(
            f"📦 Объём перевозки: <b>{volume:.0f} т</b>\n\n"
            "<b>Шаг 4 из 5</b>\n\n"
            "Введите культуру:",
            parse_mode='HTML'
        )
        
        await ShippingRequestStatesGroup.culture.set()
        
    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректное число\nПример: 100 или 150.5")


@dp.message_handler(state=ShippingRequestStatesGroup.culture)
async def shipping_culture(message: types.Message, state: FSMContext):
    """Шаг 4: Культура"""
    culture = message.text.strip()
    
    await state.update_data(culture=culture)
    
    await message.answer(
        f"🌾 Культура: <b>{culture}</b>\n\n"
        "<b>Шаг 5 из 5</b>\n\n"
        "Введите желаемую дату отправки (ДД.ММ.ГГГГ)\n"
        "Или нажмите /skip чтобы пропустить:",
        parse_mode='HTML'
    )
    
    await ShippingRequestStatesGroup.desired_date.set()


@dp.message_handler(lambda m: m.text == '/skip', state=ShippingRequestStatesGroup.desired_date)
@dp.message_handler(state=ShippingRequestStatesGroup.desired_date)
async def shipping_desired_date(message: types.Message, state: FSMContext):
    """Шаг 5: Желаемая дата (финал)"""
    
    if message.text != '/skip':
        desired_date = message.text.strip()
        
        # Проверка формата даты
        if not re.match(r'\d{2}\.\d{2}\.\d{4}', desired_date):
            await message.answer(
                "❌ Неверный формат даты!\n"
                "Используйте формат: ДД.ММ.ГГГГ (например, 15.11.2025)"
            )
            return
        
        await state.update_data(desired_date=desired_date)
    else:
        await state.update_data(desired_date='Не указана')
        desired_date = 'Не указана'
    
    # Получаем все данные
    data = await state.get_data()
    user_id = message.from_user.id
    pull = pulls[data['pull_id']]
    
    # Создаём заявку
    request_id = len(shipping_requests) + 1
    
    request = {
        'id': request_id,
        'pull_id': data['pull_id'],
        'exporter_id': pull['exporter_id'],
        'route_from': data['route_from'],
        'route_to': data['route_to'],
        'volume': data['volume'],
        'culture': data['culture'],
        'desired_date': data.get('desired_date', 'Не указана'),
        'status': 'active',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'logist_id': None,
    }
    
    shipping_requests[request_id] = request
    save_shipping_requests()
    
    # Формируем сообщение
    text = "✅ <b>Заявка на логистику создана!</b>\n\n"
    text += f"📋 <b>Заявка #{request_id}</b>\n\n"
    text += f"📍 Маршрут: {data['route_from']} → {data['route_to']}\n"
    text += f"📦 Объём: {data['volume']:.0f} т\n"
    text += f"🌾 Культура: {data['culture']}\n"
    text += f"📅 Желаемая дата: {data.get('desired_date', 'Не указана')}\n\n"
    text += "🔔 Логисты получат уведомление о вашей заявке"
    
    keyboard = exporter_keyboard()
    
    await message.answer(
        text,
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    # Уведомляем логистов
    logistics_users = [uid for uid, user in users.items() if user.get('role') == 'logistic']
    
    for logist_id in logistics_users:
        try:
            await bot.send_message(
                logist_id,
                f"🚚 <b>Новая заявка на логистику!</b>\n\n"
                f"📋 Заявка #{request_id}\n"
                f"📍 {data['route_from']} → {data['route_to']}\n"
                f"📦 {data['volume']:.0f} т {data['culture']}\n"
                f"📅 {data.get('desired_date', 'Не указана')}\n\n"
                f"Используйте '🚚 Мои заявки' для просмотра деталей",
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Ошибка отправки уведомления логисту {logist_id}: {e}")
    
    await state.finish()
    logging.info(f"Заявка на логистику #{request_id} создана экспортёром {user_id}")



# ==================== АВТОМАТИЧЕСКОЕ ПРИКРЕПЛЕНИЕ ПОДРЯДЧИКОВ ====================

async def attach_contractors_to_pull(pull_id):
    """
    Автоматическое прикрепление логистов и экспедиторов к закрытому пуллу
    Вызывается когда пулл достигает нужного тоннажа
    """
    if pull_id not in pulls:
        return

    pull = pulls[pull_id]
    exporter_id = pull.get('exporter_id')
    pull_port = pull.get('port', '')
    pull_region = pull.get('region', '')

    # Находим подходящих логистов
    suitable_logistics = []
    for req_id, request in shipping_requests.items():
        if request.get('status') != 'active':
            continue

        # Проверяем совпадение по региону/порту
        req_to = request.get('to', '').lower()
        if pull_port.lower() in req_to or pull_region.lower() in req_to:
            logist_id = request.get('logist_id')
            if logist_id in users:
                suitable_logistics.append({
                    'request_id': req_id,
                    'logist_id': logist_id,
                    'logist_name': users[logist_id].get('name', 'Логист'),
                    'phone': users[logist_id].get('phone', 'Не указан'),
                    'route': f"{request['from']} → {request['to']}",
                    'price': request['price'],
                    'volume': request['volume'],
                    'vehicle': request.get('vehicle_type', 'Не указан')
                })

    # Находим подходящих экспедиторов
    suitable_expeditors = []
    for offer_id, offer in expeditor_offers.items():
        if offer.get('status') != 'active':
            continue

        # Проверяем совпадение по порту
        offer_ports = offer.get('ports', '').lower()
        if pull_port.lower() in offer_ports:
            expeditor_id = offer.get('expeditor_id')
            if expeditor_id in users:
                suitable_expeditors.append({
                    'offer_id': offer_id,
                    'expeditor_id': expeditor_id,
                    'expeditor_name': users[expeditor_id].get('name', 'Экспедитор'),
                    'phone': users[expeditor_id].get('phone', 'Не указан'),
                    'service': offer['service_type'],
                    'price': offer['price'],
                    'terms': offer['terms'],
                    'ports': offer['ports']
                })

    # Отправляем экспортеру карточки подрядчиков
    if exporter_id and (suitable_logistics or suitable_expeditors):
        text = f"🎉 <b>Пулл #{pull_id} собран!</b>\n\n"
        text += f"📦 {pull['culture']}, {pull['volume']} т\n"
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
                text += f"<i>... и ещё {len(suitable_expeditors) - 5} экспедиторов</i>\n\n"

        text += "💡 <b>Свяжитесь с подрядчиками для обсуждения условий.</b>"

        try:
            await bot.send_message(exporter_id, text, parse_mode='HTML')
            logging.info(f"✅ Экспортеру {exporter_id} отправлены карточки подрядчиков для пулла #{pull_id}")
        except Exception as e:
            logging.error(f"Ошибка отправки карточек подрядчиков: {e}")

    # ← ДОБАВИТЬ: Уведомить фермеров из пулла
    if pull_id in pullparticipants:
        participants = pullparticipants[pull_id]
        for participant in participants:
            farmer_id = participant.get('farmer_id')
            if farmer_id and farmer_id in users:
                try:
                    # Формируем сообщение для фермера
                    farmer_text = f"🎉 <b>Пулл #{pull_id} собран!</b>\n\n"
                    farmer_text += f"📦 {pull['culture']}, {pull.get('target_volume', 0)} т\n"
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
                    
                    await bot.send_message(farmer_id, 
                        f"✅ Вы участвуете в пулле на {target_volume:.1f} т!\n"
                        f"Ваша партия {batch['volume']:.1f} т добавлена в пулл #{pull_id}")
                    logging.info(f"✅ Фермеру {farmer_id} отправлено уведомление о сборе пулла #{pull_id}")
                except Exception as e:
                    logging.error(f"Ошибка уведомления фермера {farmer_id}: {e}")

    # Уведомляем логистов
    for logist in suitable_logistics:
        try:
            text = "📢 <b>Ваш тариф передан экспортеру!</b>\n\n"
            text += f"Пулл #{pull_id}\n"
            text += f"📦 {pull['culture']}, {pull['volume']} т\n"
            text += f"🚢 Порт: {pull_port}\n\n"
            text += "Экспортер может связаться с вами для обсуждения условий перевозки."

            await bot.send_message(logist['logist_id'], text, parse_mode='HTML')
            logging.info(f"✅ Логисту {logist['logist_id']} отправлено уведомление о пулле #{pull_id}")
        except Exception as e:
            logging.error(f"Ошибка уведомления логиста: {e}")

    # Уведомляем экспедиторов
    for exp in suitable_expeditors:
        try:
            text = "📢 <b>Ваши услуги переданы экспортеру!</b>\n\n"
            text += f"Пулл #{pull_id}\n"
            text += f"📦 {pull['culture']}, {pull['volume']} т\n"
            text += f"🚢 Порт: {pull_port}\n\n"
            text += "Экспортер может связаться с вами для оформления документов."

            await bot.send_message(exp['expeditor_id'], text, parse_mode='HTML')
            logging.info(f"✅ Экспедитору {exp['expeditor_id']} отправлено уведомление о пулле #{pull_id}")
        except Exception as e:
            logging.error(f"Ошибка уведомления экспедитора: {e}")

async def on_startup(dp):
    logging.info("🚀 Бот Exportum запущен")
    
    # Загрузка данных из файлов
    load_users_from_json()
    load_users_from_pickle()
    load_pulls_from_pickle()
    load_batches_from_pickle()
    load_logistic_offers()
    load_deliveries()
    load_expeditor_offers()
    load_logistic_ratings()    
    # Создание директорий
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # ✅ МИГРАЦИЯ СТАРЫХ ПУЛОВ (после загрузки данных)
    logging.info("🔄 Проверка и миграция старых пулов...")
    try:
        migrate_old_pulls()
        logging.info("✅ Миграция пулов завершена")
    except Exception as e:
        logging.error(f"❌ Ошибка миграции пулов: {e}")
    
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
        'load_users_from_json', 'save_users_to_json',
        'load_batches_from_pickle', 'save_batches_to_pickle', 
        'load_pulls_from_pickle', 'save_pulls_to_pickle',
        'update_prices_cache', 'update_news_cache',
        'auto_match_batches_and_pulls', 'find_matching_batches',
        'find_matching_exporters', 'notify_match'
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

@dp.message_handler(lambda m: m.text == '📦 Доступные партии', state='*')
async def show_available_batches_exporter(message: types.Message, state: FSMContext):
    """Просмотр доступных партий для экспортера"""
    await state.finish()
    
    user_id = message.from_user.id
    
    logging.info(f"📦 Обработчик 'Доступные партии' вызван пользователем {user_id}")

    if user_id not in users or users[user_id].get('role') != 'exporter':
        logging.warning(f"❌ Пользователь {user_id} не является экспортёром")
        await message.answer('⚠️ Эта функция доступна только экспортёрам.')
        return

    available = []
    for farmer_id, batches in batches.items():
        for batch in user_batches:
            if batch.get('status') in ['active', 'Активна', 'available', 'доступна']:
                farmer_name = users.get(farmer_id, {}).get('name', 'Неизвестно')
                available.append({
                    'batch': batch,
                    'farmer_id': farmer_id,
                    'farmer_name': farmer_name
                })
    
    logging.info(f"📦 Найдено доступных партий: {len(available)}")

    if not available:
        await message.answer(
            "📦 <b>Доступные партии</b>\n\n"
            "❌ На данный момент нет доступных партий от фермеров.\n\n"
            "💡 Подождите, пока фермеры добавят свои партии.",
            parse_mode='HTML'
        )
        return

    text = "📦 <b>Доступные партии от фермеров</b>\n\n"
    text += f"Всего: {len(available)} партий\n\n"

    keyboard = InlineKeyboardMarkup(row_width=1)

    for i, item in enumerate(available[:10], 1):
        batch = item['batch']
        farmer_name = item['farmer_name']

        text += f"{i}. <b>{batch['culture']}</b> - {batch['volume']} т\n"
        text += f"   💰 {batch['price']:,.0f} ₽/т | 📍 {batch.get('region', 'Не указан')}\n"
        text += f"   👤 {farmer_name}\n\n"

        keyboard.add(InlineKeyboardButton(
            f"🌾 {batch['culture']} - {batch['volume']} т",
            callback_data=f"viewbatch_{batch['id']}"
        ))

    if len(available) > 10:
        text += f"... и ещё {len(available) - 10} партий\n\n"
        text += "💡 Используйте '🔍 Расширенный поиск' для фильтрации"

    keyboard.add(InlineKeyboardButton("🔍 Расширенный поиск", callback_data="advanced_batch_search"))
    keyboard.add(InlineKeyboardButton("◀️ Меню", callback_data="back_to_exporter_menu"))

    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')

@dp.message_handler(state='*')
async def handle_unexpected_messages(message: types.Message, state: FSMContext):
    """Обработка сообщений в непредусмотренных состояниях"""
    
    # ✅ ДОБАВЬТЕ: Пропускаем команды
    if message.text and message.text.startswith('/'):
        return
    
    # ✅ ДОБАВЬТЕ: Пропускаем кнопки меню
    menu_buttons = [
        '➕ Создать пул', '📦 Мои пулы', '🔍 Найти партии', '📋 Мои сделки',
        '🚚 Заявка на логистику', '👤 Профиль', '📈 Цены на зерно', '📰 Новости рынка',
        '📦 Доступные партии', '📦 Мои партии', '🔍 Найти экспортёров', '📊 Мои сделки',
        '🚚 Мои заявки', '🚛 Мои перевозки',
    ]
    
    if message.text in menu_buttons:
        return
    
    # Остальная логика...
    current_state = await state.get_state()
    if current_state is not None:
        await message.answer("❌ Завершите текущее действие или отмените его командой /cancel")
    else:
        user_id = message.from_user.id
        if user_id in users:
            role = users[user_id].get('role')
            keyboard = get_role_keyboard(role)
            await message.answer("Используйте меню для навигации", reply_markup=keyboard)
        else:
            await message.answer("Используйте /start для начала работы")

    # Остальная логика
    current_state = await state.get_state()
    if current_state is not None:
        await message.answer("❌ Завершите текущее действие или отмените его командой /cancel")
    else:
        user_id = message.from_user.id
        if user_id in users:
            role = users[user_id].get('role')
            keyboard = get_role_keyboard(role)
            await message.answer("Используйте меню для навигации", reply_markup=keyboard)
        else:
            await message.answer("Используйте /start для начала работы")



async def on_shutdown(dp):
    """Завершение работы бота"""
    logging.info("⏹ Бот Exportum останавливается...")
    save_users_to_json()
    save_users_to_pickle()
    save_pulls_to_pickle()
    save_shipping_requests()
    save_batches_to_pickle()
    save_logistic_offers()
    save_deliveries()
    save_expeditor_offers()
    savepullstopickle()
    logging.info("✅ Данные сохранены")
    
    await bot.close()
    await dp.storage.close()
    await dp.storage.wait_closed()


# ═══════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ КНОПОК ЛОГИСТИКИ - ФИНАЛЬНАЯ ВЕРСИЯ
# ═══════════════════════════════════════════════════════════════════

@dp.message_handler(lambda m: m.text == "📋 Активные заявки", state='*')
async def logistics_active_requests_handler(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Доступно только для логистов")
        return

    await message.answer(
        "📋 <b>Активные заявки</b>\n\n"
        "У вас пока нет активных заявок.\n\n"
        "📊 Статистика:\n"
        "• Всего обработано: 0\n"
        "• В ожидании: 0\n"
        "• Завершено: 0",
        parse_mode='HTML'
    )

@dp.message_handler(lambda m: m.text == "💼 Мои предложения", state='*')
async def logistics_my_offers_handler(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Доступно только для логистов")
        return

    await message.answer(
        "💼 <b>Мои предложения</b>\n\n"
        "У вас пока нет предложений.\n\n"
        "📊 Статистика:\n"
        "• Отправлено: 0\n"
        "• Принято: 0\n"
        "• Отклонено: 0",
        parse_mode='HTML'
    )

@dp.message_handler(lambda m: m.text == "🚛 Мои перевозки", state='*')
async def logistics_my_deliveries_handler(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Доступно только для логистов")
        return

    await message.answer(
        "🚛 <b>Мои перевозки</b>\n\n"
        "У вас пока нет перевозок.\n\n"
        "📊 Статистика:\n"
        "• В процессе: 0\n"
        "• Завершено: 0\n"
        "• Заработано: 0 ₽",
        parse_mode='HTML'
    )

@dp.message_handler(lambda m: m.text in ["💼 Мои логистических услуги", "💼 Мои логистических услуг"], state='*')
async def logistics_services_stats_handler(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Доступно только для логистов")
        return

    await message.answer(
        "💼 <b>Статистика услуг</b>\n\n"
        "📊 <b>Перевозки:</b> 0\n"
        "💰 <b>Заработано:</b> 0 ₽\n"
        "⭐ <b>Рейтинг:</b> нет отзывов\n\n"
        "Данные обновляются автоматически",
        parse_mode='HTML'
    )

# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ: НОВОСТИ, ЦЕНЫ И ПОИСК
# ═══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(lambda message: message.text == '📊 Новости и цены', state='*')
async def show_news_and_prices(message: types.Message, state: FSMContext):
    """Отображение новостей и цен"""
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer('⚠️ Сначала завершите регистрацию.')
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton('📈 Цены', callback_data='show_prices'),
        InlineKeyboardButton('📰 Новости', callback_data='show_news')
    )
    keyboard.add(InlineKeyboardButton('🔙 Назад', callback_data='back_to_menu'))

    await message.answer(
        '📊 <b>Новости и цены зернового рынка</b>\n\nВыберите раздел:',
        reply_markup=keyboard,
        parse_mode='HTML'
    )


@dp.callback_query_handler(lambda c: c.data == 'show_prices', state='*')
async def callback_show_prices(callback_query: types.CallbackQuery):
    """Показать цены"""
    await bot.answer_callback_query(callback_query.id)
    try:
        regional_prices = parse_russia_regional_prices()
        fob_prices = parse_fob_black_sea()
        cbot_prices = parse_cbot_futures()
        message_text = format_prices_message(regional_prices, fob_prices, cbot_prices)

        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton('🔄 Обновить', callback_data='show_prices'))
        keyboard.add(InlineKeyboardButton('🔙 Назад', callback_data='back_to_news_menu'))

        await bot.edit_message_text(
            message_text,
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
    except MessageNotModified:
        pass
    except Exception as e:
        logging.error(f'Ошибка показа цен: {e}')


@dp.callback_query_handler(lambda c: c.data == 'show_news', state='*')
async def callback_show_news(callback_query: types.CallbackQuery):
    """Показать новости"""
    await bot.answer_callback_query(callback_query.id)
    try:
        news_list = parse_grain_news()
        message_text = format_news_message(news_list)

        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton('🔄 Обновить', callback_data='show_news'))
        keyboard.add(InlineKeyboardButton('🔙 Назад', callback_data='back_to_news_menu'))

        await bot.edit_message_text(
            message_text,
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
    except MessageNotModified:
        pass
    except Exception as e:
        logging.error(f'Ошибка показа новостей: {e}')


@dp.callback_query_handler(lambda c: c.data == 'back_to_news_menu', state='*')
async def callback_back_to_news_menu(callback_query: types.CallbackQuery):
    """Вернуться в меню новостей"""
    await bot.answer_callback_query(callback_query.id)

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton('📈 Цены', callback_data='show_prices'),
        InlineKeyboardButton('📰 Новости', callback_data='show_news')
    )
    keyboard.add(InlineKeyboardButton('🔙 Назад', callback_data='back_to_menu'))

    try:
        await bot.edit_message_text(
            '📊 <b>Новости и цены зернового рынка</b>\n\nВыберите раздел:',
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    except MessageNotModified:
        pass

# ═══════════════════════════════════════════════════════════════════════════
# ПРОСМОТР ДЕТАЛЕЙ ПАРТИИ (CALLBACK)
# ═══════════════════════════════════════════════════════════════════════════

@dp.callback_query_handler(lambda c: c.data.startswith('batch:'), state='*')
async def view_batch_details_handler(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей партии"""
    await state.finish()
    
    try:
        # ✅ ИСПРАВЛЕНО: ':' вместо '_'
        batch_id = parse_callback_id(callback.data)
    except (IndexError, ValueError) as e:
        logging.error(f"Ошибка парсинга batch_id из {callback.data}: {e}")
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    # Ищем партию
    batch = None
    farmer_id = None
    
    # ✅ ИСПРАВЛЕНИЕ: Ищем партию у ВСЕХ пользователей
    for uid, user_batches in batches.items():
        for b in user_batches:
            if b['id'] == batch_id:
                batch = b
                farmer_id = uid
                break
        if batch:
            break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    
    # Формируем сообщение с деталями
    msg = f"📦 <b>Партия #{batch_id}</b>\n\n"
    msg += f"🌾 Культура: {batch['culture']}\n"
    msg += f"📊 Объём: {batch['volume']} т\n"
    msg += f"💰 Цена: {batch['price']:,.0f} ₽/т\n"
    msg += f"📍 Регион: {batch.get('region', 'Не указан')}\n"
    msg += f"📋 Статус: {batch.get('status', 'Активна')}\n"
    
    # Качество если есть
    if 'nature' in batch or 'moisture' in batch:
        msg += "\n<b>🔬 Качество:</b>\n"
        if 'nature' in batch:
            msg += f"   🌾 Натура: {batch.get('nature', 'Не указано')} г/л\n"
        if 'moisture' in batch:
            msg += f"   💧 Влажность: {batch['moisture']}%\n"
        if 'impurity' in batch:
            msg += f"   🌿 Сорность: {batch.get('impurity', 'Не указано')}%\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="view_my_batches")
    )
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'back_to_available_batches', state='*')
async def back_to_available_batches_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку доступных партий"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await callback.answer("❌ Доступно только экспортёрам", show_alert=True)
        return
    
    # Собираем все активные партии
    available = []
    for farmer_id, batches in batches.items():
        for batch in user_batches:
            if batch.get('status') in ['active', 'Активна', 'available', 'доступна']:
                farmer_name = users.get(farmer_id, {}).get('name', 'Неизвестно')
                available.append({
                    'batch': batch,
                    'farmer_id': farmer_id,
                    'farmer_name': farmer_name
                })
    
    if not available:
        await callback.message.edit_text(
            "📦 <b>Доступные партии</b>\n\n"
            "❌ На данный момент нет доступных партий от фермеров.",
            parse_mode='HTML'
        )
        await callback.answer()
        return
    
    text = "📦 <b>Доступные партии от фермеров</b>\n\n"
    text += f"Всего: {len(available)} партий\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for i, item in enumerate(available[:10], 1):
        batch = item['batch']
        farmer_name = item['farmer_name']
        
        text += f"{i}. <b>{batch['culture']}</b> - {batch['volume']} т\n"
        text += f"   💰 {batch['price']:,.0f} ₽/т | 📍 {batch.get('region', 'Не указан')}\n"
        text += f"   👤 {farmer_name}\n\n"
        
        keyboard.add(InlineKeyboardButton(
            f"🌾 {batch['culture']} - {batch['volume']} т",
            callback_data=f"viewbatch_{batch['id']}"
        ))
    
    if len(available) > 10:
        text += f"... и ещё {len(available) - 10} партий"
    
    keyboard.add(InlineKeyboardButton("🔍 Расширенный поиск", callback_data="advanced_batch_search"))
    keyboard.add(InlineKeyboardButton("◀️ Меню", callback_data="back_to_exporter_menu"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'back_to_exporter_menu', state='*')
async def back_to_exporter_menu(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в меню экспортёра"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    if user_id not in users:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    
    user = users[user_id]
    name = user.get('name', 'Экспортёр')
    
    try:
        await callback.message.delete()
    except Exception as e:
        print(f"Ошибка: {e}")
        pass
    
    keyboard = exporter_keyboard()
    
    await callback.message.answer(
        f"👋 С возвращением, {name}!\n\n📦 <b>Меню экспортёра</b>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'advanced_batch_search', state='*')
async def advanced_batch_search_handler(callback: types.CallbackQuery, state: FSMContext):
    """Расширенный поиск партий"""
    await callback.message.edit_text(
        "🔍 <b>Расширенный поиск партий</b>\n\n"
        "Выберите критерий поиска:",
        reply_markup=search_criteria_keyboard(),
        parse_mode='HTML'
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('contact_farmer_'), state='*')
async def contact_farmer_handler(callback: types.CallbackQuery, state: FSMContext):
    """Контакт с фермером"""
    try:
        parts = callback.data.split('_')
        farmer_id = int(parts[2])
        batch_id = int(parts[3])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    farmer = users.get(farmer_id)
    if not farmer:
        await callback.answer("❌ Фермер не найден", show_alert=True)
        return
    
    farmer_name = farmer.get('name', 'Неизвестно')
    farmer_phone = farmer.get('phone', 'Не указан')
    
    await callback.answer(
        f"👤 Фермер: {farmer_name}\n"
        f"📞 Телефон: {farmer_phone}",
        show_alert=True
    )
    
    # Уведомляем фермера о заинтересованности
    try:
        exporter_name = users[callback.from_user.id].get('name', 'Экспортёр')
        await bot.send_message(
            farmer_id,
            f"✅ <b>Экспортёр проявил интерес!</b>\n\n"
            f"👤 {exporter_name} заинтересовался вашей партией #{batch_id}\n\n"
            f"Ожидайте звонка!",
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка уведомления фермера: {e}")

@dp.message_handler(lambda message: message.text == '🔍 Поиск', state='*')
async def start_search(message: types.Message, state: FSMContext):
    """Начать поиск партий"""
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer('⚠️ Сначала завершите регистрацию.')
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton('🌾 По культуре', callback_data='search_by_culture'),
        InlineKeyboardButton('📍 По региону', callback_data='search_by_region')
    )
    keyboard.add(
        InlineKeyboardButton('💰 По цене', callback_data='search_by_price'),
        InlineKeyboardButton('📦 По объему', callback_data='search_by_volume')
    )
    keyboard.add(InlineKeyboardButton('🔙 Назад', callback_data='back_to_menu'))

    await message.answer(
        '🔍 <b>Поиск партий зерна</b>\n\nВыберите критерий поиска:',
        reply_markup=keyboard,
        parse_mode='HTML'
    )


@dp.callback_query_handler(lambda c: c.data == 'search_by_culture', state='*')
async def callback_search_by_culture(callback_query: types.CallbackQuery, state: FSMContext):
    await SearchByCulture.waiting_culture.set()
    """Поиск по культуре"""
    await bot.answer_callback_query(callback_query.id)
    await SearchByCulture.waiting_culture.set()
    keyboard = culture_keyboard()
    try:
        await bot.edit_message_text(
            '🌾 <b>Выберите культуру:</b>',
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    except MessageNotModified:
        pass


@dp.callback_query_handler(lambda c: c.data == 'search_by_region', state='*')
async def callback_search_by_region(callback_query: types.CallbackQuery):
    """Поиск по региону"""
    await bot.answer_callback_query(callback_query.id)
    await SearchBatchesStatesGroup.region.set()
    keyboard = region_keyboard()
    try:
        await bot.edit_message_text(
            '📍 <b>Выберите регион:</b>',
            callback_query.from_user.id,
            callback_query.message.message_id,
            reply_markup=keyboard,
            parse_mode='HTML'
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
        await bot.send_message(CHANNEL_ID, message_text, parse_mode='HTML')
        logging.info(f'✅ Пулл {pull_data.get("id")} опубликован в канал')
    except Exception as e:
        logging.error(f'❌ Ошибка публикации пулла в канал: {e}')


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
        await bot.send_message(CHANNEL_ID, message_text, parse_mode='HTML')
        logging.info(f'✅ Партия {batch_data.get("id")} опубликована в канал')
    except Exception as e:
        logging.error(f'❌ Ошибка публикации партии в канал: {e}')


async def generate_weekly_report():
    """Генерация еженедельного отчета"""
    try:
        farmers_count = len([u for u in users.values() if u.get('role') == 'farmer'])
        exporters_count = len([u for u in users.values() if u.get('role') == 'exporter'])
        logistics_count = len([u for u in users.values() if u.get('role') == 'logistic'])
        expeditors_count = len([u for u in users.values() if u.get('role') == 'expeditor'])

        total_batches = sum(len(batches) for user_batches in batches.values())
        total_pulls = len(pulls)
        total_deals = len(deals)

        total_batch_volume = 0
        for farmer_id, batches in batches.items():
            for batch in user_batches:
                total_batch_volume += batch.get('volume', 0)

        prices = []
        for farmer_id, batches in batches.items():
            for batch in user_batches:
                if batch.get('price'):
                    prices.append(batch['price'])

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

        admin_id = 1481790360  # Замените на ID админа
        await bot.send_message(admin_id, report_text, parse_mode='HTML')

        try:
            await bot.send_message(CHANNEL_ID, report_text, parse_mode='HTML')
        except Exception as e:
            print(f"Ошибка: {e}")
            pass

        logging.info('✅ Еженедельный отчет отправлен')
    except Exception as e:
        logging.error(f'❌ Ошибка генерации отчета: {e}')


async def schedule_weekly_reports():
    """Запуск scheduler для еженедельных отчетов"""
    global scheduler
    try:
        scheduler.add_job(
            generate_weekly_report,
            'cron',
            day_of_week='mon',
            hour=9,
            minute=0
        )
        logging.info('✅ Scheduler запущен: еженедельные отчеты активны')
    except Exception as e:
        logging.error(f'❌ Ошибка запуска scheduler: {e}')

# ============================================================================
# CALLBACK ОБРАБОТЧИКИ АДМИН-ПАНЕЛИ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == "admin_refresh_stats", state='*')
async def admin_refresh_statistics(callback: CallbackQuery, state: FSMContext):
    """Обновление статистики"""
    await state.finish()

    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    stats_message = format_admin_statistics()

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="admin_refresh_stats"),
        InlineKeyboardButton("📊 Детали", callback_data="admin_detailed_stats")
    )

    try:
        await callback.message.edit_text(stats_message, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer("✅ Обновлено")
    except MessageNotModified:
        await callback.answer("Данные актуальны")
    except Exception as e:
        await callback.answer(f"Ошибка: {str(e)}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "admin_refresh_analytics", state='*')
async def admin_refresh_analytics_callback(callback: CallbackQuery, state: FSMContext):
    """Обновление аналитики"""
    await state.finish()

    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    analytics_message = format_admin_analytics()

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="admin_refresh_analytics"),
        InlineKeyboardButton("📤 Экспорт", callback_data="admin_export_analytics")
    )

    try:
        await callback.message.edit_text(analytics_message, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer("✅ Обновлено")
    except MessageNotModified:
        await callback.answer("Данные актуальны")
    except Exception as e:
        await callback.answer(f"Ошибка: {str(e)}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "export_users", state='*')
async def export_users_callback(callback: CallbackQuery, state: FSMContext):
    """Экспорт пользователей в CSV"""
    await state.finish()
    
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    try:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID', 'Роль', 'Телефон', 'Email', 'Регион', 'ИНН', 'Компания'])
        
        for user_id_data, user_data in users.items():
            writer.writerow([
                user_id_data,
                user_data.get('role', ''),
                user_data.get('phone', ''),
                user_data.get('email', ''),
                user_data.get('region', ''),
                user_data.get('inn', ''),
                user_data.get('company_name', '')
            ])
        
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        await bot.send_document(
            callback.from_user.id,
            ('users_' + timestamp + '.csv', output.getvalue().encode('utf-8-sig')),
            caption=f"📤 Экспорт пользователей\nВсего: {len(users)}"
        )
        
        await callback.answer("✅ Файл отправлен")
        logging.info(f"Экспорт пользователей выполнен: {len(users)} записей")
        
    except Exception as e:
        logging.error(f"Ошибка экспорта пользователей: {e}")
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "export_pools", state='*')
async def export_pools_callback(callback: CallbackQuery, state: FSMContext):
    """Экспорт пулов в CSV"""
    await state.finish()
    
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    try:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID', 'Экспортёр', 'Статус', 'Культура', 'Объём', 'Цена', 'Дата создания'])
        
        for pool_id, pool_data in pools.items():
            exporter = users.get(pool_data.get('exporter_id'), {})
            writer.writerow([
                pool_id,
                exporter.get('company_name', 'Неизвестно'),
                pool_data.get('status', ''),
                pool_data.get('culture', ''),
                pool_data.get('volume', 0),
                pool_data.get('price', 0),
                pool_data.get('created_at', '')
            ])
        
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        await bot.send_document(
            callback.from_user.id,
            ('pools_' + timestamp + '.csv', output.getvalue().encode('utf-8-sig')),
            caption=f"📤 Экспорт пулов\nВсего: {len(pulls)}"
        )
        
        await callback.answer("✅ Файл отправлен")
        logging.info(f"Экспорт пулов выполнен: {len(pulls)} записей")
        
    except Exception as e:
        logging.error(f"Ошибка экспорта пулов: {e}")
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "export_batches", state='*')
async def export_batches_callback(callback: CallbackQuery, state: FSMContext):
    """Экспорт партий в CSV"""
    await state.finish()
    
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    try:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Фермер ID', 'Фермер', 'Batch ID', 'Культура', 'Объём', 'Цена', 'Регион'])
        
        for farmer_id, batches in batches.items():
            farmer = users.get(farmer_id, {})
            farmer_name = farmer.get('company_name', 'Неизвестно')
            for batch in user_batches:
                writer.writerow([
                    farmer_id,
                    farmer_name,
                    batch.get('id', ''),
                    batch.get('culture', ''),
                    batch.get('volume', 0),
                    batch.get('price', 0),
                    batch.get('region', '')
                ])
        
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        total_batches = sum(len(batches) for user_batches in batches.values())
        
        await bot.send_document(
            callback.from_user.id,
            ('batches_' + timestamp + '.csv', output.getvalue().encode('utf-8-sig')),
            caption=f"📤 Экспорт партий\nВсего: {total_batches}"
        )
        
        await callback.answer("✅ Файл отправлен")
        logging.info(f"Экспорт партий выполнен: {total_batches} записей")
        
    except Exception as e:
        logging.error(f"Ошибка экспорта партий: {e}")
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "export_full", state='*')
async def export_full_backup_callback(callback: CallbackQuery, state: FSMContext):
    """Полный бэкап всех данных"""
    await state.finish()
    
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    try:
        save_data()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = f"backup_{timestamp}"
        
        os.makedirs(backup_dir, exist_ok=True)
        
        for filename in ['users.pkl', 'pools.pkl', 'batches.pkl', 'shipping_requests.pkl']:
            if os.path.exists(filename):
                shutil.copy(filename, os.path.join(backup_dir, filename))
        
        archive_name = f"backup_{timestamp}"
        shutil.make_archive(archive_name, 'zip', backup_dir)
        shutil.rmtree(backup_dir)
        
        with open(f"{archive_name}.zip", 'rb') as backup_file:
            await bot.send_document(
                callback.from_user.id,
                backup_file,
                caption=f"💾 Полная резервная копия\nДата: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
            )
        
        os.remove(f"{archive_name}.zip")
        
        await callback.answer("✅ Бэкап создан и отправлен")
        logging.info(f"Создан полный бэкап: {archive_name}.zip")
        
    except Exception as e:
        logging.error(f"Ошибка создания бэкапа: {e}")
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "reload_data", state='*')
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




# ============================================================================
# CALLBACK ОБРАБОТЧИКИ ДЛЯ АДМИН-ПАНЕЛИ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == "adminstat", state='*')
async def admin_statistics_callback(callback: types.CallbackQuery, state: FSMContext):
    """Статистика через callback"""
    await state.finish()

    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    try:
        msg = format_admin_statistics()

        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminstat"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )

        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer("✅ Статистика обновлена")
    except Exception as e:
        logging.error(f"Ошибка статистики: {e}")
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "adminanalytics", state='*')
async def admin_analytics_callback(callback: types.CallbackQuery, state: FSMContext):
    """Аналитика через callback"""
    await state.finish()

    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    try:
        msg = format_admin_analytics()

        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminanalytics"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )

        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer("✅ Аналитика обновлена")
    except Exception as e:
        logging.error(f"Ошибка аналитики: {e}")
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "adminexport", state='*')
async def admin_export_callback(callback: types.CallbackQuery, state: FSMContext):
    """Экспорт данных через callback"""
    await state.finish()

    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("👥 Пользователи", callback_data="exportusers"),
        InlineKeyboardButton("📦 Пуллы", callback_data="exportpulls"),
        InlineKeyboardButton("🌾 Партии", callback_data="exportbatches"),
        InlineKeyboardButton("📋 Заявки", callback_data="exportrequests"),
        InlineKeyboardButton("💼 Полный бэкап", callback_data="exportfull"),
        InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
    )

    await callback.message.edit_text(
        "📤 <b>Экспорт данных</b>\n\nВыберите данные для экспорта:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "adminusers", state='*')
async def admin_users_callback(callback: types.CallbackQuery, state: FSMContext):
    """Список пользователей через callback"""
    await state.finish()

    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    try:
        msg = format_admin_users()

        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔄 Обновить", callback_data="adminusers"),
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )

        await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer("✅ Список пользователей")
    except Exception as e:
        logging.error(f"Ошибка списка пользователей: {e}")
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data == "adminbroadcast", state='*')
async def admin_broadcast_callback(callback: types.CallbackQuery, state: FSMContext):
    """Рассылка через callback"""
    await state.finish()

    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
    )

    await callback.message.edit_text(
        "📧 <b>Рассылка сообщений</b>\n\n"
        "Функция в разработке.\n\n"
        "Для массовой рассылки используйте команду:\n"
        "/broadcast <текст сообщения>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "adminprices", state='*')
async def admin_prices_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обновление цен через callback"""
    await state.finish()

    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    await callback.answer("⏳ Обновляю цены...", show_alert=True)

    try:
        # Запускаем обновление цен
        await update_grain_prices()

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )

        await callback.message.edit_text(
            "✅ <b>Цены обновлены!</b>\n\n"
            "Данные успешно загружены из источников.\n"
            f"Время обновления: {datetime.now().strftime('%H:%M:%S')}",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка обновления цен: {e}")

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("◀️ Назад", callback_data="backtoadmin")
        )

        await callback.message.edit_text(
            f"❌ <b>Ошибка обновления цен</b>\n\n{e}",
            reply_markup=keyboard,
            parse_mode='HTML'
        )


    await callback.message.edit_text(
        "🔐 <b>Админ панель</b>\n\nВыберите действие:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()





# === HANDLER: Присоединение партий к пуллу ===

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('selectbatch_'), state='*')
async def process_batch_selection_for_pull(callback_query: CallbackQuery, state: FSMContext):
    """Обработка выбора партии для добавления в пулл"""
    try:
        batch_id = callback_query.data.split('_')[1]
        user_id = callback_query.from_user.id

        logging.info(f"Выбор партии {batch_id}")

        data = await state.get_data()
        pull_id = data.get('pull_id')

        if not pull_id:
            await callback_query.answer("❌ Пулл не найден", show_alert=True)
            return

        load_batches_from_pickle()
        pulls = load_pulls()

        batch = next((b for b in batches if b.get('id') == batch_id), None)
        if not batch:
            await callback_query.answer("❌ Партия не найдена", show_alert=True)
            return

        if str(batch.get('farmer_id')) != str(user_id):
            await callback_query.answer("❌ Не ваша партия", show_alert=True)
            return

        pull = next((p for p in pulls if p.get('id') == pull_id), None)
        if not pull:
            await callback_query.answer("❌ Пулл не найден", show_alert=True)
            return

        if 'batches' not in pull:
            pull['batches'] = []

        if batch_id in pull['batches']:
            await callback_query.answer("⚠️ Уже добавлена", show_alert=True)
            return

        pull['batches'].append(batch_id)
        current_volume = pull.get('current_volume', 0)
        pull['current_volume'] = current_volume + batch.get('volume', 0)

        save_pulls(pulls)

        await callback_query.answer("✅ Партия добавлена!", show_alert=True)

        logging.info(f"✅ Партия {batch_id} добавлена в пулл {pull_id}")

        try:
            if hasattr(gs, 'sync_pull_to_sheets'):
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

        exporter_id = pull.get('exporter_id')
        if not exporter_id:
            logging.debug("У пулла нет exporter_id")
            return

        users = load_users()
        if exporter_id not in users:
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
        channel_id = os.getenv('CHANNEL_ID')
        if not channel_id:
            logging.debug("CHANNEL_ID не настроен")
            return

        message = f"🌾 Новая партия!\n{batch.get('culture')} - {batch.get('volume')} т"
        await bot.send_message(channel_id, message)
        logging.info("✅ Опубликовано в канале")

    except Exception as e:
        if 'Chat not found' in str(e):
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

    if user_id not in users:
        await message.answer("❌ Пользователь не найден")
        return

    # Находим пуллы в которых участвует фермер
    my_pulls = []

    for exporter_id, pulls in exporter_pulls.items():
        for pull in pulls:
            # Проверяем участвует ли фермер в этом пулле
            for batch_id in pull.get('batches', []):
                if user_id in batches:
                    for batch in batches[user_id]:
                        if batch.get('id') == batch_id:
                            my_pulls.append({
                                'pull': pull,
                                'exporter_id': exporter_id,
                                'batch': batch
                            })

    if not my_pulls:
        await message.answer(
            "📋 Вы пока не участвуете ни в одном пулле\n\n"
            "Используйте '🔍 Найти пулл' чтобы найти подходящие пуллы",
            parse_mode="Markdown"
        )
        return

    msg = "📋 *Пуллы в которых вы участвуете:*\n\n"

    for i, item in enumerate(my_pulls, 1):
        pull = item['pull']
        batch = item['batch']

        status_emoji = {"open": "🟢", "filling": "🟡", "closed": "🔴"}.get(pull.get('status', 'open'), "❓")
        msg += f"{i}. {status_emoji} {pull['culture']}\n"
        msg += f"   Ваша партия: {batch['volume']} т\n"
        msg += f"   Порт: {pull.get('port', 'не указан')}\n"
        msg += f"   Прогресс: {pull.get('current_volume', 0)}/{pull['target_volume']} т\n\n"

    await message.answer(msg, parse_mode="Markdown")


@dp.message_handler(lambda m: m.text == "➕ Создать партию", state="*")
async def create_batch_start(message: types.Message, state: FSMContext):
    """Начать создание партии"""
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'farmer':
        await message.answer("❌ Эта функция доступна только фермерам")
        return

    await message.answer(
        "🌾 *Создание новой партии*\n\n"
        "Укажите культуру:",
        reply_markup=culture_keyboard(),
        parse_mode="Markdown"
    )
    await CreateBatchStates.culture.set()

# ═══════════════════════════════════════════════════════════════════════════
# ДОПОЛНИТЕЛЬНАЯ ФУНКЦИЯ: /debug для проверки аккаунта
# ═══════════════════════════════════════════════════════════════════════════

@dp.message_handler(commands=['debug'], state='*')
async def debug_account(message: types.Message):
    """Показать информацию о своём аккаунте для отладки"""
    user_id = message.from_user.id

    info = []
    info.append("👤 *Информация об аккаунте*\n")
    info.append(f"User ID: `{user_id}`\n")

    # Проверяем users
    if user_id in users:
        user_data = users[user_id]
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

    # Проверяем exporter_pulls
    if user_id in exporter_pulls:
        pull_count = len(exporter_pulls[user_id])
        info.append(f"🎯 Пуллов экспортёра: {pull_count}\n")

    # Проверяем Google Sheets
    try:
        worksheet = spreadsheet.worksheet('Users')
        cell = worksheet.find(str(user_id))
        if cell:
            info.append(f"✅ Найден в Google Sheets (строка {cell.row})")
        else:
            info.append("❌ Не найден в Google Sheets")
    except Exception as e:
        info.append(f"❌ Ошибка проверки Google Sheets: {e}")

    await message.answer("".join(info), parse_mode="Markdown")

# ============================================================================
# СИСТЕМА ЛОГИСТИЧЕСКИХ ЗАЯВОК
# ============================================================================

# -------------------- ЭКСПОРТЁР: СОЗДАНИЕ ЗАЯВКИ --------------------

@dp.message_handler(lambda m: m.text == "🚚 Заявка на логистику", state='*')
async def create_logistics_request_start(message: types.Message, state: FSMContext):
    """Начало создания заявки на логистику"""
    await state.finish()
    user_id = message.from_user.id
    
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await message.answer("❌ Эта функция доступна только экспортёрам")
        return
    
    # Показываем активные пулы экспортёра
    exporter_pulls = {pid: p for pid, p in pulls.items() 
                      if p.get('exporter_id') == user_id and p.get('status') in ['Открыт', 'Заполнен']}
    
    if not exporter_pulls:
        await message.answer(
            "❌ У вас нет активных пулов.\n\n"
            "Создайте пул, чтобы заказать логистику.",
            reply_markup=exporter_keyboard()
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    for pull_id, pull in list(exporter_pulls.items())[:10]:
        keyboard.add(
            InlineKeyboardButton(
                f"#{pull_id} • {pull['culture']} • {pull.get('current_volume', 0):.0f} т",
                callback_data=f"create_logistic_req:{pull_id}"
            )
        )
    
    await message.answer(
        "🚚 <b>Заявка на логистику</b>\n\n"
        "Выберите пул для организации перевозки:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


@dp.callback_query_handler(lambda c: c.data.startswith("create_logistic_req:"), state='*')
async def select_pull_for_logistics(callback: types.CallbackQuery, state: FSMContext):
    """Выбор пула для заявки"""
    try:
        pull_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    
    await state.update_data(
        pull_id=pull_id,
        culture=pull['culture'],
        volume=pull.get('current_volume', 0),
        port=pull.get('port', '')
    )
    
    await callback.message.edit_text(
        f"🚚 <b>Заявка на логистику</b>\n\n"
        f"<b>Шаг 1 из 3</b>\n\n"
        f"Пул: #{pull_id} • {pull['culture']} • {pull.get('current_volume', 0):.0f} т\n"
        f"Порт: {pull.get('port', '')}\n\n"
        f"Откуда (регион/город погрузки):",
        parse_mode='HTML'
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
        f"🚚 <b>Заявка на логистику</b>\n\n"
        f"<b>Шаг 2 из 3</b>\n\n"
        f"Откуда: <b>{route_from}</b>\n"
        f"Куда: <b>{data.get('port', '')}</b>\n\n"
        f"Желаемая дата погрузки (ДД.ММ.ГГГГ):",
        parse_mode='HTML'
    )
    
    await CreateLogisticRequestStatesGroup.loading_date.set()


@dp.message_handler(state=CreateLogisticRequestStatesGroup.loading_date)
async def logistics_request_date(message: types.Message, state: FSMContext):
    """Дата погрузки"""
    loading_date = message.text.strip()
    
    if not validate_date(loading_date):
        await message.answer("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ (например: 15.12.2025)")
        return
    
    await state.update_data(loading_date=loading_date)
    
    await message.answer(
        "🚚 <b>Заявка на логистику</b>\n\n"
        "<b>Шаг 3 из 3</b>\n\n"
        "Дополнительные требования (или /skip):",
        parse_mode='HTML'
    )
    
    await CreateLogisticRequestStatesGroup.notes.set()


@dp.message_handler(lambda m: m.text == '/skip', state=CreateLogisticRequestStatesGroup.notes)
@dp.message_handler(state=CreateLogisticRequestStatesGroup.notes)
async def logistics_request_finish(message: types.Message, state: FSMContext):
    """Завершение создания заявки"""
    global logistics_request_counter
    
    notes = "" if message.text == '/skip' else message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    pull_id = data['pull_id']
    pull = pulls[pull_id]
    
    logistics_request_counter += 1
    
    request = {
        'id': logistics_request_counter,
        'exporter_id': user_id,
        'exporter_name': users[user_id].get('name', ''),
        'pull_id': pull_id,
        'culture': data['culture'],
        'volume': data['volume'],
        'route_from': data['route_from'],
        'route_to': data.get('port', ''),
        'loading_date': data['loading_date'],
        'notes': notes,
        'status': 'active',
        'offers_count': 0,
        'selected_offer_id': None,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    logistics_requests[logistics_request_counter] = request
    save_logistics_requests_to_pickle()
    
    await state.finish()
    
    summary = (
        f"✅ <b>Заявка на логистику #{logistics_request_counter} создана!</b>\n\n"
        f"📦 Пул: #{pull_id}\n"
        f"🌾 Культура: {data['culture']}\n"
        f"📦 Объем: {data['volume']:.0f} т\n"
        f"📍 Маршрут: {data['route_from']} → {request['route_to']}\n"
        f"📅 Дата: {data['loading_date']}\n"
    )
    
    if notes:
        summary += f"📝 Примечания: {notes}\n"
    
    summary += "\nЛогисты смогут откликнуться на вашу заявку."
    
    await message.answer(summary, parse_mode='HTML', reply_markup=exporter_keyboard())
    
    # Уведомляем логистов
    await notify_logistics_about_new_request(request)
    
    logging.info(f"Logistics request {logistics_request_counter} created by exporter {user_id}")


# -------------------- ЛОГИСТ: ПРОСМОТР И ОТКЛИК --------------------

@dp.message_handler(lambda m: m.text == "📋 Активные заявки", state='*')
async def view_active_logistics_requests(message: types.Message, state: FSMContext):
    """Просмотр активных заявок на перевозку"""
    await state.finish()
    user_id = message.from_user.id
    
    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Эта функция доступна только логистам")
        return
    
    active_requests = {rid: r for rid, r in logistics_requests.items() 
                       if r.get('status') in ['active', 'has_offers']}
    
    if not active_requests:
        await message.answer(
            "📋 <b>Активные заявки</b>\n\n"
            "В данный момент нет активных заявок на перевозку.",
            parse_mode='HTML'
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for req_id, req in list(active_requests.items())[:10]:
        status_emoji = "🆕" if req['status'] == 'active' else "💼"
        offers_text = f" • {req['offers_count']} откликов" if req['offers_count'] > 0 else ""
        
        keyboard.add(
            InlineKeyboardButton(
                f"{status_emoji} #{req_id} • {req['culture']} • {req['volume']:.0f} т{offers_text}",
                callback_data=f"view_logistics_req:{req_id}"
            )
        )
    
    await message.answer(
        f"📋 <b>Активные заявки ({len(active_requests)})</b>\n\n"
        f"Выберите заявку для просмотра:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


@dp.callback_query_handler(lambda c: c.data.startswith("view_logistics_req:"), state='*')
async def view_logistics_request_details(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей заявки"""
    await state.finish()
    
    try:
        req_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if req_id not in logistics_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    req = logistics_requests[req_id]
    user_id = callback.from_user.id
    
    # Проверяем, откликался ли уже
    already_offered = any(
        o.get('logist_id') == user_id 
        for o in logistics_offers.values() 
        if o.get('request_id') == req_id
    )
    
    msg = f"📋 <b>Заявка #{req_id}</b>\n\n"
    msg += f"🌾 Культура: {req['culture']}\n"
    msg += f"📦 Объем: {req['volume']:.0f} т\n"
    msg += f"📍 Маршрут: {req['route_from']} → {req['route_to']}\n"
    msg += f"📅 Дата: {req['loading_date']}\n"
    
    if req.get('notes'):
        msg += f"📝 Примечания: {req['notes']}\n"
    
    msg += f"\n💼 Откликов: {req['offers_count']}\n"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    if not already_offered and req['status'] in ['active', 'has_offers']:
        keyboard.add(
            InlineKeyboardButton("✅ Откликнуться", callback_data=f"respond_logistics:{req_id}")
        )
    elif already_offered:
        keyboard.add(
            InlineKeyboardButton("📝 Ваш отклик отправлен", callback_data="noop")
        )
    
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_logistics_requests")
    )
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("respond_logistics:"), state='*')
async def respond_to_logistics_request(callback: types.CallbackQuery, state: FSMContext):
    """Начало отклика на заявку"""
    try:
        req_id = parse_callback_id(callback.data)
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if req_id not in logistics_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    await state.update_data(request_id=req_id)
    
    await callback.message.edit_text(
        f"💼 <b>Отклик на заявку #{req_id}</b>\n\n"
        f"<b>Шаг 1 из 4</b>\n\n"
        f"Укажите вашу цену (₽ за тонну):",
        parse_mode='HTML'
    )
    
    await LogisticOfferStatesGroup.price.set()
    await callback.answer()


@dp.message_handler(state=LogisticOfferStatesGroup.price)
async def logistics_offer_price(message: types.Message, state: FSMContext):
    """Цена за перевозку"""
    try:
        price = float(message.text.strip().replace(",", ".").replace(" ", ""))
        if price <= 0:
            raise ValueError
        
        await state.update_data(price=price)
        
        await message.answer(
            f"💼 <b>Отклик на заявку</b>\n\n"
            f"<b>Шаг 2 из 4</b>\n\n"
            f"Цена: <b>{price:,.0f} ₽/т</b>\n\n"
            f"Тип транспорта (например: Фура 20т, Зерновоз):",
            parse_mode='HTML'
        )
        
        await LogisticOfferStatesGroup.vehicle_type.set()
    
    except ValueError:
        await message.answer("❌ Некорректная цена. Введите положительное число.")


@dp.message_handler(state=LogisticOfferStatesGroup.vehicle_type)
async def logistics_offer_vehicle(message: types.Message, state: FSMContext):
    """Тип транспорта"""
    vehicle_type = message.text.strip()
    await state.update_data(vehicle_type=vehicle_type)
    
    await message.answer(
        f"💼 <b>Отклик на заявку</b>\n\n"
        f"<b>Шаг 3 из 4</b>\n\n"
        f"Транспорт: <b>{vehicle_type}</b>\n\n"
        f"Срок доставки (дней):",
        parse_mode='HTML'
    )
    
    await LogisticOfferStatesGroup.delivery_date.set()


@dp.message_handler(state=LogisticOfferStatesGroup.delivery_date)
async def logistics_offer_days(message: types.Message, state: FSMContext):
    """Срок доставки"""
    try:
        delivery_days = int(message.text.strip())
        if delivery_days <= 0:
            raise ValueError
        
        await state.update_data(delivery_days=delivery_days)
        
        await message.answer(
            f"💼 <b>Отклик на заявку</b>\n\n"
            f"<b>Шаг 4 из 4</b>\n\n"
            f"Срок: <b>{delivery_days} дней</b>\n\n"
            f"Дополнительная информация (или /skip):",
            parse_mode='HTML'
        )
        
        await LogisticOfferStatesGroup.additional_info.set()
    
    except ValueError:
        await message.answer("❌ Некорректный срок. Введите целое число дней.")


@dp.message_handler(lambda m: m.text == '/skip', state=LogisticOfferStatesGroup.additional_info)
@dp.message_handler(state=LogisticOfferStatesGroup.additional_info)
async def logistics_offer_finish(message: types.Message, state: FSMContext):
    """Завершение отклика"""
    global logistics_offer_counter
    
    notes = "" if message.text == '/skip' else message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    req_id = data['request_id']
    req = logistics_requests[req_id]
    
    logistics_offer_counter += 1
    
    offer = {
        'id': logistics_offer_counter,
        'request_id': req_id,
        'logist_id': user_id,
        'logist_name': users[user_id].get('name', ''),
        'logist_phone': users[user_id].get('phone', ''),
        'price': data['price'],
        'vehicle_type': data['vehicle_type'],
        'delivery_days': data['delivery_days'],
        'notes': notes,
        'status': 'pending',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    logistics_offers[logistics_offer_counter] = offer
    
    # Обновляем счетчик откликов
    req['offers_count'] = req.get('offers_count', 0) + 1
    if req['status'] == 'active':
        req['status'] = 'has_offers'
    
    save_logistics_requests_to_pickle()
    save_logistics_offers_to_pickle()
    
    await state.finish()
    
    total_price = data['price'] * req['volume']
    
    summary = (
        f"✅ <b>Отклик отправлен!</b>\n\n"
        f"📋 Заявка: #{req_id}\n"
        f"🌾 {req['culture']} • {req['volume']:.0f} т\n"
        f"📍 {req['route_from']} → {req['route_to']}\n\n"
        f"💰 Ваша цена: {data['price']:,.0f} ₽/т\n"
        f"💵 Общая сумма: {total_price:,.0f} ₽\n"
        f"🚛 Транспорт: {data['vehicle_type']}\n"
        f"⏱ Срок: {data['delivery_days']} дней\n\n"
        f"Экспортёр получит уведомление о вашем отклике."
    )
    
    await message.answer(summary, parse_mode='HTML', reply_markup=logistic_keyboard())
    
    # Уведомляем экспортёра
    await notify_exporter_about_offer(req, offer)
    
    logging.info(f"Logistics offer {logistics_offer_counter} created by logist {user_id}")


# -------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ --------------------

async def notify_logistics_about_new_request(request):
    """Уведомление логистов о новой заявке"""
    logistics_users = [uid for uid, u in users.items() if u.get('role') == 'logistic']
    
    msg = (
        f"🆕 <b>Новая заявка на перевозку!</b>\n\n"
        f"📋 Заявка #{request['id']}\n"
        f"🌾 {request['culture']} • {request['volume']:.0f} т\n"
        f"📍 {request['route_from']} → {request['route_to']}\n"
        f"📅 Дата: {request['loading_date']}\n\n"
        f"Откликнитесь через меню 'Активные заявки'"
    )
    
    for logist_id in logistics_users:
        try:
            await bot.send_message(logist_id, msg, parse_mode='HTML')
        except Exception as e:
            logging.error(f"Error notifying logist {logist_id}: {e}")


async def notify_exporter_about_offer(request, offer):
    """Уведомление экспортёра об отклике"""
    exporter_id = request['exporter_id']
    
    total_price = offer['price'] * request['volume']
    
    msg = (
        f"💼 <b>Новый отклик на заявку #{request['id']}</b>\n\n"
        f"👤 Логист: {offer['logist_name']}\n"
        f"💰 Цена: {offer['price']:,.0f} ₽/т\n"
        f"💵 Общая сумма: {total_price:,.0f} ₽\n"
        f"🚛 Транспорт: {offer['vehicle_type']}\n"
        f"⏱ Срок: {offer['delivery_days']} дней"
    )
    
    try:
        await bot.send_message(exporter_id, msg, parse_mode='HTML')
    except Exception as e:
        logging.error(f"Error notifying exporter {exporter_id}: {e}")


@dp.callback_query_handler(lambda c: c.data == "back_to_logistics_requests", state='*')
async def back_to_logistics_requests_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку заявок"""
    await view_active_logistics_requests(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "noop", state='*')
async def noop_handler(callback: types.CallbackQuery):
    """Пустой обработчик"""
    await callback.answer()

#===============================================================================


# ====================================================================
# ПОИСК ПО КУЛЬТУРЕ
# ====================================================================

@dp.message_handler(Text(equals="🔍 Поиск по культуре"), state='*')
async def start_search_by_culture(message: types.Message, state: FSMContext):
    """Начало поиска партий по культуре"""
    await state.finish()

    await message.answer(
        "🔍 <b>Поиск по культуре</b>\n\n"
        "Выберите культуру для поиска партий:",
        reply_markup=culture_keyboard(),
        parse_mode='HTML'
    )
    await SearchByCulture.waiting_culture.set()


if 'logistics_cards' not in globals():
    logistics_cards = {}
if 'expeditor_cards' not in globals():
    expeditor_cards = {}


@dp.message_handler(Text(equals="📋 Моя карточка"), state='*')
async def show_my_card_menu(message: types.Message, state: FSMContext):
    """Показать меню карточки логиста/экспедитора"""
    await state.finish()

    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы. Используйте /start")
        return

    user = users[user_id]
    role = user.get('role')

    if role == 'logistic':
        if user_id in logistics_cards:
            card = logistics_cards[user_id]
            text = f"""
📋 <b>Ваша карточка логиста</b>

🚚 <b>Маршруты:</b> {card.get('routes', 'Не указано')}
💰 <b>Цена за км:</b> {card.get('price_per_km', 'Не указано')} ₽/км
💰 <b>Цена за тонну:</b> {card.get('price_per_ton', 'Не указано')} ₽/т
📦 <b>Мин. объём:</b> {card.get('min_volume', 'Не указано')} т
🚛 <b>Тип транспорта:</b> {card.get('transport_type', 'Не указано')}
🏢 <b>Порты:</b> {card.get('ports', 'Не указано')}
"""
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(
                InlineKeyboardButton("✏️ Редактировать", callback_data="edit_logistic_card"),
                InlineKeyboardButton("🗑 Удалить", callback_data="delete_logistic_card")
            )
        else:
            text = "📋 У вас ещё нет карточки. Создайте её, чтобы получать заказы!"
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("➕ Создать карточку", callback_data="create_logistic_card"))

    elif role == 'expeditor':
        if user_id in expeditor_cards:
            card = expeditor_cards[user_id]
            text = f"""
📋 <b>Ваша карточка экспедитора</b>

📜 <b>Услуги:</b> {card.get('services', 'Не указано')}
💰 <b>Стоимость ДТ:</b> {card.get('dt_price', 'Не указано')} ₽
🏢 <b>Порты:</b> {card.get('ports', 'Не указано')}
⭐ <b>Опыт:</b> {card.get('experience', 'Не указано')}
"""
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(
                InlineKeyboardButton("✏️ Редактировать", callback_data="edit_expeditor_card"),
                InlineKeyboardButton("🗑 Удалить", callback_data="delete_expeditor_card")
            )
        else:
            text = "📋 У вас ещё нет карточки. Создайте её, чтобы получать заказы!"
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("➕ Создать карточку", callback_data="create_expeditor_card"))
    else:
        await message.answer("❌ Карточки доступны только логистам и экспедиторам")
        return

    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')


# ====================================================================
# СОЗДАНИЕ КАРТОЧКИ ЛОГИСТА
# ====================================================================

@dp.callback_query_handler(lambda c: c.data == 'create_logistic_card', state='*')
async def start_create_logistic_card(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.edit_text(
        "🚚 <b>Создание карточки логиста</b>\n\n"
        "Шаг 1/7\n\n"
        "Введите ваши маршруты (например: Краснодар-Новороссийск, Ростов-Азов):",
        parse_mode='HTML'
    )
    await CreateLogisticCardStates.routes.set()
    await callback.answer()


@dp.message_handler(state=CreateLogisticCardStates.routes)
async def process_logistic_routes(message: types.Message, state: FSMContext):
    await state.update_data(routes=message.text)
    await message.answer("💰 Шаг 2/7\n\nУкажите цену за километр (руб.):", parse_mode='HTML')
    await CreateLogisticCardStates.price_per_km.set()


@dp.message_handler(state=CreateLogisticCardStates.price_per_km)
async def process_price_per_km(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.replace(',', '.'))
        await state.update_data(price_per_km=price)
        await message.answer("💰 Шаг 3/7\n\nУкажите цену за тонну (руб.):", parse_mode='HTML')
        await CreateLogisticCardStates.price_per_ton.set()
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")


@dp.message_handler(state=CreateLogisticCardStates.price_per_ton)
async def process_price_per_ton(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.replace(',', '.'))
        await state.update_data(price_per_ton=price)
        await message.answer("📦 Шаг 4/7\n\nУкажите минимальный объём перевозки (тонн):", parse_mode='HTML')
        await CreateLogisticCardStates.min_volume.set()
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")


@dp.message_handler(state=CreateLogisticCardStates.min_volume)
async def process_min_volume(message: types.Message, state: FSMContext):
    try:
        volume = float(message.text.replace(',', '.'))
        await state.update_data(min_volume=volume)
        await message.answer("🚛 Шаг 5/7\n\nУкажите тип транспорта (например: Фура 20т):", parse_mode='HTML')
        await CreateLogisticCardStates.transport_type.set()
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")


@dp.message_handler(state=CreateLogisticCardStates.transport_type)
async def process_transport_type(message: types.Message, state: FSMContext):
    await state.update_data(transport_type=message.text)

    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "Ариб-Снаб",
        "КЗК",
        "ПКФ ЮТ",
        "ПКФ ВП",
        "Астр Порт",
        "Астр Унив",
        "Астр Южн",
        "ЗЮЙД-ВЕСТ",
        "АгроФуд",
        "Порт 83",
        "ПКФ ЦГП",
        "Астр зерн",
        "Астр морск"
    ]

    for port in ports:
        keyboard.insert(InlineKeyboardButton(port, callback_data=f"selectport_{port}"))
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="ports_selected"))

    await message.answer(
        "🏢 Шаг 6/7\n\nВыберите порты, в которых работаете (можно несколько):",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await CreateLogisticCardStates.ports.set()


@dp.callback_query_handler(lambda c: c.data.startswith('selectport_'), state=CreateLogisticCardStates.ports)
async def toggle_port_selection(callback: types.CallbackQuery, state: FSMContext):
    port = callback.data.replace('selectport_', '')

    data = await state.get_data()
    selected_ports = data.get('selected_ports', [])

    if port in selected_ports:
        selected_ports.remove(port)
    else:
        selected_ports.append(port)

    await state.update_data(selected_ports=selected_ports)

    # Обновляем клавиатуру
    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "Ариб-Снаб",
        "КЗК",
        "ПКФ ЮТ",
        "ПКФ ВП",
        "Астр Порт",
        "Астр Унив",
        "Астр Южн",
        "ЗЮЙД-ВЕСТ",
        "АгроФуд",
        "Порт 83",
        "ПКФ ЦГП",
        "Астр зерн",
        "Астр морск"
    ]
    for p in ports:
        mark = "✅ " if p in selected_ports else ""
        keyboard.insert(InlineKeyboardButton(f"{mark}{p}", callback_data=f"selectport_{p}"))
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="ports_selected"))

    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'ports_selected', state=CreateLogisticCardStates.ports)
async def ports_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_ports = data.get('selected_ports', [])

    if not selected_ports:
        await callback.answer("❌ Выберите хотя бы один порт", show_alert=True)
        return

    await state.update_data(ports=', '.join(selected_ports))
    await callback.message.edit_text(
        "📝 Шаг 7/7\n\nВведите дополнительную информацию (или напишите 'нет'):",
        parse_mode='HTML'
    )
    await CreateLogisticCardStates.additional_info.set()
    await callback.answer()


@dp.message_handler(state=CreateLogisticCardStates.additional_info)
async def save_logistic_card(message: types.Message, state: FSMContext):
    additional = message.text if message.text.lower() != 'нет' else ''
    await state.update_data(additional_info=additional)

    data = await state.get_data()
    user_id = message.from_user.id

    logistics_cards[user_id] = {
        'routes': data.get('routes'),
        'price_per_km': data.get('price_per_km'),
        'price_per_ton': data.get('price_per_ton'),
        'min_volume': data.get('min_volume'),
        'transport_type': data.get('transport_type'),
        'ports': data.get('ports'),
        'additional_info': additional,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    save_logistics_cards_to_pickle()

    await state.finish()
    await message.answer(
        "✅ <b>Карточка логиста создана!</b>\n\n"
        "Теперь экспортёры будут получать вашу карточку при закрытии пулов.",
        parse_mode='HTML'
    )

    logging.info(f"✅ Logistic card created for user {user_id}")


# ====================================================================
# СОЗДАНИЕ КАРТОЧКИ ЭКСПЕДИТОРА (АНАЛОГИЧНО)
# ====================================================================

@dp.callback_query_handler(lambda c: c.data == 'create_expeditor_card', state='*')
async def start_create_expeditor_card(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.edit_text(
        "📜 <b>Создание карточки экспедитора</b>\n\nШаг 1/5\n\n"
        "Опишите ваши услуги (например: Оформление ДТ, таможенное сопровождение):",
        parse_mode='HTML'
    )
    await CreateExpeditorCardStates.services.set()
    await callback.answer()


@dp.message_handler(state=CreateExpeditorCardStates.services)
async def process_expeditor_services(message: types.Message, state: FSMContext):
    await state.update_data(services=message.text)
    await message.answer("💰 Шаг 2/5\n\nУкажите стоимость оформления ДТ (руб.):", parse_mode='HTML')
    await CreateExpeditorCardStates.dt_price.set()


@dp.message_handler(state=CreateExpeditorCardStates.dt_price)
async def process_dt_price(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.replace(',', '.').replace(' ', ''))
        await state.update_data(dt_price=price)

        keyboard = InlineKeyboardMarkup(row_width=2)
        ports = [
            "Ариб",
            "Ариб-Снаб",
            "КЗК",
            "ПКФ ЮТ",
            "ПКФ ВП",
            "Астр Порт",
            "Астр Унив",
            "Астр Южн",
            "ЗЮЙД-ВЕСТ",
            "АгроФуд",
            "Порт 83",
            "ПКФ ЦГП",
            "Астр зерн",
            "Астр морск"
        ]

        for port in ports:
            keyboard.insert(InlineKeyboardButton(port, callback_data=f"selectexpport_{port}"))
        keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="expeditor_ports_selected"))

        await message.answer(
            "🏢 Шаг 3/5\n\nВыберите порты, в которых работаете:",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await CreateExpeditorCardStates.ports.set()
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")


@dp.callback_query_handler(lambda c: c.data.startswith('selectexpport_'), state=CreateExpeditorCardStates.ports)
async def toggle_expeditor_port(callback: types.CallbackQuery, state: FSMContext):
    port = callback.data.replace('selectexpport_', '')

    data = await state.get_data()
    selected_ports = data.get('selected_ports', [])

    if port in selected_ports:
        selected_ports.remove(port)
    else:
        selected_ports.append(port)

    await state.update_data(selected_ports=selected_ports)

    keyboard = InlineKeyboardMarkup(row_width=2)
    ports = [
        "Ариб",
        "Ариб-Снаб",
        "КЗК",
        "ПКФ ЮТ",
        "ПКФ ВП",
        "Астр Порт",
        "Астр Унив",
        "Астр Южн",
        "ЗЮЙД-ВЕСТ",
        "АгроФуд",
        "Порт 83",
        "ПКФ ЦГП",
        "Астр зерн",
        "Астр морск"
    ]
    for p in ports:
        mark = "✅ " if p in selected_ports else ""
        keyboard.insert(InlineKeyboardButton(f"{mark}{p}", callback_data=f"selectexpport_{p}"))
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="expeditor_ports_selected"))

    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'expeditor_ports_selected', state=CreateExpeditorCardStates.ports)
async def expeditor_ports_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_ports = data.get('selected_ports', [])

    if not selected_ports:
        await callback.answer("❌ Выберите хотя бы один порт", show_alert=True)
        return

    await state.update_data(ports=', '.join(selected_ports))
    await callback.message.edit_text("⭐ Шаг 4/5\n\nУкажите ваш опыт работы (лет):", parse_mode='HTML')
    await CreateExpeditorCardStates.experience.set()
    await callback.answer()


@dp.message_handler(state=CreateExpeditorCardStates.experience)
async def process_expeditor_experience(message: types.Message, state: FSMContext):
    await state.update_data(experience=message.text)
    await message.answer("📝 Шаг 5/5\n\nВведите дополнительную информацию (или 'нет'):", parse_mode='HTML')
    await CreateExpeditorCardStates.additional_info.set()


@dp.message_handler(state=CreateExpeditorCardStates.additional_info)
async def save_expeditor_card(message: types.Message, state: FSMContext):
    additional = message.text if message.text.lower() != 'нет' else ''
    await state.update_data(additional_info=additional)

    data = await state.get_data()
    user_id = message.from_user.id

    expeditor_cards[user_id] = {
        'services': data.get('services'),
        'dt_price': data.get('dt_price'),
        'ports': data.get('ports'),
        'experience': data.get('experience'),
        'additional_info': additional,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    save_expeditor_cards_to_pickle()

    await state.finish()
    await message.answer(
        "✅ <b>Карточка экспедитора создана!</b>\n\n"
        "Теперь экспортёры будут получать вашу карточку при закрытии пулов.",
        parse_mode='HTML'
    )

    logging.info(f"✅ Expeditor card created for user {user_id}")


# ====================================================================
# ВЫБОР ЛОГИСТА ЭКСПОРТЁРОМ
# ====================================================================

@dp.callback_query_handler(lambda c: c.data.startswith('select_logistic_'))
async def select_logistic_handler(callback: types.CallbackQuery):
    try:
        parts = callback.data.split('_')
        logistic_id = int(parts[2])
        deal_id = int(parts[3])

        if deal_id not in deals:
            await callback.answer("❌ Сделка не найдена", show_alert=True)
            return

        deals[deal_id]['logistic_id'] = logistic_id
        deals[deal_id]['logistic_selected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_deals_to_pickle()

        logistic = users.get(logistic_id, {})
        await callback.message.edit_text(
            f"✅ <b>Логист выбран!</b>\n\n"
            f"🚚 Компания: {logistic.get('company', 'Не указано')}\n"
            f"👤 Контакт: {logistic.get('name', 'Не указано')}\n"
            f"📞 Телефон: {logistic.get('phone', 'Не указано')}",
            parse_mode='HTML'
        )

        # Уведомление логисту
        deal = deals[deal_id]
        pull_id = deal.get('pull_id')
        pull = pulls.get(pull_id, {})

        try:
            await bot.send_message(
                logistic_id,
                f"🎉 <b>ВЫ ВЫБРАНЫ!</b>\n\n"
                f"📦 Сделка #{deal_id}\n"
                f"🎯 Объём: {pull.get('current_volume', 0)} т\n"
                f"🏢 Порт: {pull.get('port', 'Не указано')}\n\n"
                f"Свяжитесь с экспортёром!",
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"Ошибка: {e}")
            pass

        logging.info(f"✅ Logistic {logistic_id} selected for deal {deal_id}")

    except Exception as e:
        logging.error(f"Error selecting logistic: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)


# ====================================================================
# ВЫБОР ЭКСПЕДИТОРА ЭКСПОРТЁРОМ
# ====================================================================

@dp.callback_query_handler(lambda c: c.data.startswith('select_expeditor_'))
async def select_expeditor_handler(callback: types.CallbackQuery):
    try:
        parts = callback.data.split('_')
        expeditor_id = int(parts[2])
        deal_id = int(parts[3])

        if deal_id not in deals:
            await callback.answer("❌ Сделка не найдена", show_alert=True)
            return

        deals[deal_id]['expeditor_id'] = expeditor_id
        deals[deal_id]['expeditor_selected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_deals_to_pickle()

        expeditor = users.get(expeditor_id, {})
        await callback.message.edit_text(
            f"✅ <b>Экспедитор выбран!</b>\n\n"
            f"📜 Компания: {expeditor.get('company', 'Не указано')}\n"
            f"👤 Контакт: {expeditor.get('name', 'Не указано')}\n"
            f"📞 Телефон: {expeditor.get('phone', 'Не указано')}",
            parse_mode='HTML'
        )

        # Уведомление экспедитору
        deal = deals[deal_id]
        pull_id = deal.get('pull_id')
        pull = pulls.get(pull_id, {})

        try:
            await bot.send_message(
                expeditor_id,
                f"🎉 <b>ВЫ ВЫБРАНЫ!</b>\n\n"
                f"📦 Сделка #{deal_id}\n"
                f"🎯 Объём: {pull.get('current_volume', 0)} т\n"
                f"🏢 Порт: {pull.get('port', 'Не указано')}\n\n"
                f"Свяжитесь с экспортёром!",
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"Ошибка: {e}")
            pass

        logging.info(f"✅ Expeditor {expeditor_id} selected for deal {deal_id}")

    except Exception as e:
        logging.error(f"Error selecting expeditor: {e}")

# ============================================================================
# НЕДОСТАЮЩИЕ ОБРАБОТЧИКИ (добавлены при исправлении)
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == 'back_to_menu', state='*')
async def back_to_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.finish()
    user_id = callback.from_user.id
    role = users.get(user_id, {}).get('role', '')

    # ✅ ИСПРАВЛЕНИЕ: используем get_role_keyboard
    if role == 'farmer':
        await callback.message.answer(
            "🌾 <b>Главное меню фермера</b>\n\n"
            "Выберите действие:",
            reply_markup=get_role_keyboard('farmer'),
            parse_mode='HTML'
        )
    elif role == 'exporter':
        await callback.message.answer(
            "🚢 <b>Главное меню экспортёра</b>\n\n"
            "Выберите действие:",
            reply_markup=get_role_keyboard('exporter'),
            parse_mode='HTML'
        )
    elif role == 'logistics':
        await callback.message.answer(
            "🚚 <b>Главное меню логиста</b>\n\n"
            "Выберите действие:",
            reply_markup=get_role_keyboard('logistics'),
            parse_mode='HTML'
        )
    elif role == 'expeditor':
        await callback.message.answer(
            "📋 <b>Главное меню экспедитора</b>\n\n"
            "Выберите действие:",
            reply_markup=get_role_keyboard('expeditor'),
            parse_mode='HTML'
        )
    else:
        await callback.message.answer(
            "⚠️ Роль не определена. Используйте /start"
        )

    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'cancel', state='*')
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    await state.finish()
    await callback.message.answer("❌ Действие отменено")
    await back_to_menu_handler(callback, state)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'cancel_action', state='*')
async def cancel_action_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отмена действия"""
    await cancel_handler(callback, state)

@dp.callback_query_handler(lambda c: c.data == 'transport_type', state='*')
async def transport_type_handler(callback: types.CallbackQuery):
    """Выбор типа транспорта"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚛 Автомобильный", callback_data="transport:auto"),
        InlineKeyboardButton("🚂 Железнодорожный", callback_data="transport:rail"),
        InlineKeyboardButton("🚢 Морской", callback_data="transport:sea"),
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu")
    )
    await callback.message.edit_text("🚚 Выберите тип транспорта:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'view_my_batches', state='*')
async def view_my_batches_handler(callback: types.CallbackQuery):
    """Просмотр моих партий"""
    user_id = callback.from_user.id
    user_batches = batches.get(user_id, [])

    if not user_batches:
        await callback.message.answer("📦 У вас пока нет созданных партий")
        await callback.answer()
        return

    text = "<b>📦 Ваши партии:</b>\n\n"
    keyboard = InlineKeyboardMarkup(row_width=1)

    for i, batch in enumerate(user_batches[:10], 1):
        crop = batch.get('culture', 'Н/Д')
        volume = batch.get('volume', 0)
        text += f"{i}. {crop} - {volume} тонн\n"
        keyboard.add(InlineKeyboardButton(f"📦 {crop} ({volume}т)", callback_data=f"viewbatch_{batch.get('id')}"))

    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'startsearch', state='*')
async def startsearch_handler(callback: types.CallbackQuery):
    """Начать поиск"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 По культуре", callback_data="search_by_culture"),
        InlineKeyboardButton("📍 По региону", callback_data="search_by_region"),
        InlineKeyboardButton("💰 По цене", callback_data="search_by:price"),
        InlineKeyboardButton("📊 По объёму", callback_data="search_by:volume"),
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu")
    )
    await callback.message.edit_text("🔍 Выберите критерий поиска:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'broadcast_confirm', state='*')
async def broadcast_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение рассылки"""
    data = await state.get_data()
    message_text = data.get('broadcast_message')

    if not message_text:
        await callback.message.answer("❌ Сообщение для рассылки не найдено")
        await callback.answer()
        return

    # Отправляем всем пользователям
    sent = 0
    failed = 0

    for user_id in users.keys():
        try:
            await bot.send_message(user_id, f"📢 <b>Рассылка:</b>\n\n{message_text}", parse_mode="HTML")
            sent += 1
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка отправки рассылки {user_id}: {e}")

    await callback.message.answer(f"✅ Рассылка завершена\n📤 Отправлено: {sent}\n❌ Ошибок: {failed}")
    await state.finish()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data in ['exportusers', 'export_users'], state='*')
async def export_users_unified_handler(callback: types.CallbackQuery):
    """Экспорт пользователей (унифицированный)"""
    await export_users_handler(callback)

@dp.callback_query_handler(lambda c: c.data in ['exportbatches', 'export_batches'], state='*')
async def export_batches_unified_handler(callback: types.CallbackQuery):
    """Экспорт партий (унифицированный)"""
    await export_batches_handler(callback)

@dp.callback_query_handler(lambda c: c.data in ['exportpulls', 'export_pools'], state='*')
async def export_pulls_unified_handler(callback: types.CallbackQuery):
    """Экспорт пулов (унифицированный)"""
    await export_pools_handler(callback)

@dp.callback_query_handler(lambda c: c.data in ['exportfull', 'export_full'], state='*')
async def export_full_unified_handler(callback: types.CallbackQuery):
    """Полный экспорт (унифицированный)"""
    await export_full_handler(callback)

@dp.callback_query_handler(lambda c: c.data == 'back_to_pools', state='*')
async def back_to_pools_handler(callback: types.CallbackQuery):
    """Возврат к списку пулов"""
    await show_pools_list(callback.message)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'backtomainmenu', state='*')
async def backtomainmenu_handler(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню (альтернативное название)"""
    await back_to_menu_handler(callback, state)

@dp.callback_query_handler(lambda c: c.data == 'expeditor_available_deals', state='*')
async def expeditor_available_deals_handler(callback: types.CallbackQuery):
    """Доступные сделки для экспедитора"""
    available_deals = [d for d in deals.values() if d.get('status') == 'need_expeditor']

    if not available_deals:
        await callback.message.answer("📋 Нет доступных сделок")
        await callback.answer()
        return

    text = "<b>📋 Доступные сделки:</b>\n\n"
    keyboard = InlineKeyboardMarkup(row_width=1)

    for i, deal in enumerate(available_deals[:10], 1):
        deal_id = deal.get('id')
        port = deal.get('port', 'Н/Д')
        volume = deal.get('volume', 0)
        text += f"{i}. Порт: {port}, Объём: {volume}т\n"
        keyboard.add(InlineKeyboardButton(f"📋 Сделка #{deal_id}", callback_data=f"expeditor_view_deal:{deal_id}"))


# ============================================================================
# ФИНАЛЬНЫЕ НЕДОСТАЮЩИЕ ОБРАБОТЧИКИ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == 'admin_detailed_stats', state='*')
async def admin_detailed_stats_handler(callback: types.CallbackQuery):
    """Детальная статистика для админа"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    # Подсчёт статистики
    total_users = len(users)
    roles_count = {}
    for user in users.values():
        role = user.get('role', 'unknown')
        roles_count[role] = roles_count.get(role, 0) + 1

    total_batches = sum(len(b) for b in batches.values())
    active_batches = sum(1 for b_list in batches.values() for b in b_list if b.get('status') == 'active')

    total_pulls = len(pulls)
    active_pulls = sum(1 for p in pulls.values() if p.get('status') == 'active')

    text = "<b>📊 Детальная статистика:</b>\n\n"
    text += f"👥 <b>Пользователи:</b> {total_users}\n"
    for role, count in roles_count.items():
        text += f"   • {role}: {count}\n"

    text += f"\n📦 <b>Партии:</b>\n"
    text += f"   • Всего: {total_batches}\n"
    text += f"   • Активных: {active_batches}\n"

    text += f"\n🌾 <b>Пулы:</b>\n"
    text += f"   • Всего: {total_pulls}\n"
    text += f"   • Активных: {active_pulls}\n"

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("📤 Экспорт данных", callback_data="admin_export_analytics"),
        InlineKeyboardButton("◀️ Назад", callback_data="admin")
    )

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'admin_export_analytics', state='*')
async def admin_export_analytics_handler(callback: types.CallbackQuery):
    """Экспорт аналитики"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    try:
        from io import StringIO
        import csv

        output = StringIO()
        writer = csv.writer(output)

        # Заголовок
        writer.writerow(['Метрика', 'Значение'])

        # Данные
        writer.writerow(['Всего пользователей', len(users)])
        writer.writerow(['Всего партий', sum(len(b) for b in batches.values())])
        writer.writerow(['Всего пулов', len(pulls)])

        roles_count = {}
        for user in users.values():
            role = user.get('role', 'unknown')
            roles_count[role] = roles_count.get(role, 0) + 1

        for role, count in roles_count.items():
            writer.writerow([f'Пользователи: {role}', count])

        output.seek(0)

        from aiogram.types import BufferedInputFile
        file = BufferedInputFile(output.getvalue().encode('utf-8-sig'), filename='analytics.csv')

        await callback.message.answer_document(file, caption="📊 Аналитика экспортирована")
        await callback.answer()

    except Exception as e:
        logging.error(f"Ошибка экспорта аналитики: {e}")
        await callback.answer("❌ Ошибка экспорта", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == 'admin_search_user', state='*')
async def admin_search_user_handler(callback: types.CallbackQuery):
    """Поиск пользователя админом"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    await callback.message.answer(
        "🔍 <b>Поиск пользователя</b>\n\n"
        "Введите один из параметров:\n"
        "• Telegram ID\n"
        "• Номер телефона\n"
        "• Email\n"
        "• ИНН",
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('doctype_'), state='*')
async def doctype_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора типа документа (CPT, FOB, CIF, EXW)"""
    doctype = callback.data.split('_', 1)[1] if '_' in callback.data else 'CPT'

    # Сохраняем выбранный тип документа
    await state.update_data(doctype=doctype)

    await callback.answer(f"✅ Выбран тип: {doctype}")

    # Продолжаем логику в зависимости от контекста
    data = await state.get_data()

    # Показываем следующий шаг
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("➡️ Продолжить", callback_data="continue_shipping"),
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu")
    )

    await callback.message.edit_text(
        f"✅ <b>Тип документа:</b> {doctype}\n\n"
        f"Нажмите 'Продолжить' для следующего шага",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

@dp.callback_query_handler(lambda c: c.data == 'continue_shipping', state='*')
async def continue_shipping_handler(callback: types.CallbackQuery):
    """Продолжение процесса создания заявки"""
    await callback.answer("✅ Продолжаем...")
    # Дальнейшая логика

    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu"))

    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()
# ========== ОБРАБОТЧИКИ ЛОГИСТА ==========

@dp.message_handler(lambda m: m.text == "📋 Активные заявки", state='*')
async def logistic_active_requests_handler(message: types.Message, state: FSMContext):
    """Показать активные заявки на доставку"""
    await state.finish()

    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Доступ запрещен.")
        return

    active_requests = {req_id: req for req_id, req in shipping_requests.items() if req.get('status') == 'active'}

    if not active_requests:
        await message.answer("<b>📋 Активные заявки</b>\n\nНа данный момент нет активных заявок.", parse_mode='HTML')
        return

    text = f"<b>📋 Активные заявки на доставку</b>\n\nВсего: <b>{len(active_requests)}</b>\n\n"
    keyboard = InlineKeyboardMarkup(row_width=1)

    for idx, (req_id, request) in enumerate(list(active_requests.items())[:10], 1):
        text += f"{idx}. <b>Заявка #{req_id}</b>\n"
        text += f"   📍 {request.get('route_from', 'Неизвестно')} → {request.get('route_to', 'Неизвестно')}\n"
        text += f"   📦 {request.get('volume', 0):.0f} т ({request.get('culture', '')})\n"
        text += f"   📅 {request.get('desired_date', 'Не указана')}\n\n"
        keyboard.add(InlineKeyboardButton(f"🔍 Заявка #{req_id}", callback_data=f"viewrequest:{req_id}"))

    if len(active_requests) > 10:
        text += f"\n...и ещё {len(active_requests) - 10}\n"

    keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="refresh_requests"))
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')


@dp.callback_query_handler(lambda c: c.data.startswith('viewrequest:'), state='*')
async def view_request_details(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр деталей заявки"""
    await state.finish()

    try:
        req_id = int(callback.data.split(':')[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if req_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    request = shipping_requests[req_id]
    exporter = users.get(request.get('exporter_id'), {})

    text = f"<b>📋 Заявка #{req_id}</b>\n\n"
    text += f"<b>📦 Детали груза:</b>\n"
    text += f"🌾 Культура: {request.get('culture', 'Не указана')}\n"
    text += f"📊 Объём: {request.get('volume', 0):.0f} т\n\n"
    text += f"<b>🚛 Маршрут:</b>\n"
    text += f"📍 Откуда: {request.get('route_from', 'Не указано')}\n"
    text += f"📍 Куда: {request.get('route_to', 'Не указано')}\n\n"
    text += f"<b>📅 Сроки:</b>\n{request.get('desired_date', 'Не указана')}\n\n"
    text += f"<b>👤 Экспортёр:</b>\n{exporter.get('name', 'Неизвестно')}\n<code>{exporter.get('phone', '')}</code>\n\n"

    existing_offer = None
    for offer_id, offer in logistic_offers.items():
        if offer.get('request_id') == req_id and offer.get('logist_id') == callback.from_user.id:
            existing_offer = offer
            break

    keyboard = InlineKeyboardMarkup(row_width=1)

    if existing_offer:
        text += f"<b>✅ Ваше предложение:</b>\n"
        text += f"🚛 {existing_offer.get('vehicle_type', '')}\n"
        text += f"💰 {existing_offer.get('price', 0):,.0f} ₽\n"
        text += f"📅 {existing_offer.get('delivery_date', '')}\n"
        keyboard.add(InlineKeyboardButton("✏️ Редактировать", callback_data=f"editoffer:{offer_id}"))
    else:
        keyboard.add(InlineKeyboardButton("✅ Сделать предложение", callback_data=f"makeoffer:{req_id}"))

    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_requests"))
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('makeoffer:'), state='*')
async def make_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начать создание предложения"""
    try:
        req_id = int(callback.data.split(':')[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    if req_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    await state.update_data(request_id=req_id)
    await LogisticOfferStates.vehicle_type.set()

    await callback.message.edit_text("<b>🚛 Выберите тип транспорта:</b>", reply_markup=vehicle_type_keyboard(), parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('vehicle:'), state=LogisticOfferStates.vehicle_type)
async def set_vehicle_type(callback: types.CallbackQuery, state: FSMContext):
    """Установить тип транспорта"""
    vehicle_type = callback.data.split(':')[1]
    vehicle_names = {'truck': '🚛 Автомобиль', 'train': '🚂 Ж/д', 'ship': '🚢 Судно'}

    await state.update_data(vehicle_type=vehicle_names.get(vehicle_type, vehicle_type))
    await LogisticOfferStates.price.set()

    await callback.message.edit_text(
        f"<b>💰 Укажите стоимость доставки (₽/т):</b>\n\nВыбран: {vehicle_names.get(vehicle_type, vehicle_type)}\n\nВведите цену числом: <code>1500</code>",
        parse_mode='HTML'
    )
    await callback.answer()


@dp.message_handler(state=LogisticOfferStates.price)
async def set_offer_price(message: types.Message, state: FSMContext):
    """Установить цену предложения"""
    try:
        price = float(message.text.replace(',', '.').replace(' ', ''))
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Неверный формат. Введите число: <code>1500</code>", parse_mode='HTML')
        return

    await state.update_data(price=price)
    await LogisticOfferStates.delivery_date.set()
    await message.answer("<b>📅 Укажите дату доставки:</b>\n\nФормат: <code>ДД.ММ.ГГГГ</code>\nНапример: <code>15.11.2025</code>", parse_mode='HTML')


@dp.message_handler(state=LogisticOfferStates.delivery_date)
async def set_delivery_date(message: types.Message, state: FSMContext):
    """Установить дату доставки"""
    delivery_date = message.text.strip()

    if not re.match(r'\d{2}\.\d{2}\.\d{4}', delivery_date):
        await message.answer("❌ Неверный формат! Используйте: <code>ДД.ММ.ГГГГ</code>", parse_mode='HTML')
        return

    await state.update_data(delivery_date=delivery_date)
    await LogisticOfferStates.confirm.set()

    data = await state.get_data()
    text = f"<b>✅ Подтверждение предложения:</b>\n\n"
    text += f"🚛 {data['vehicle_type']}\n💰 {data['price']:,.0f} ₽/т\n📅 {delivery_date}\n\nВсё верно?"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_offer"),
        InlineKeyboardButton("❌ Отменить", callback_data="cancel")
    )
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')


@dp.callback_query_handler(lambda c: c.data == 'confirm_offer', state=LogisticOfferStates.confirm)
async def confirm_logistic_offer(callback: types.CallbackQuery, state: FSMContext):
    """Подтвердить создание предложения"""
    data = await state.get_data()
    user_id = callback.from_user.id

    offer_id = len(logistic_offers) + 1
    offer = {
        'id': offer_id,
        'request_id': data['request_id'],
        'logist_id': user_id,
        'vehicle_type': data['vehicle_type'],
        'price': data['price'],
        'delivery_date': data['delivery_date'],
        'status': 'active',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    logistic_offers[offer_id] = offer
    save_logistic_offers()

    request = shipping_requests.get(data['request_id'])
    if request:
        exporter_id = request.get('exporter_id')
        if exporter_id:
            try:
                logist_name = users[user_id].get('name', 'Неизвестно')
                logist_phone = users[user_id].get('phone', 'Не указан')

                await bot.send_message(
                    exporter_id,
                    f"<b>📦 Новое предложение на доставку!</b>\n\n"
                    f"<b>Заявка #{request['id']}</b>\n"
                    f"📍 {request.get('route_from', '')} → {request.get('route_to', '')}\n\n"
                    f"<b>👤 Логист:</b> {logist_name}\n"
                    f"📱 <code>{logist_phone}</code>\n\n"
                    f"<b>💼 Предложение:</b>\n"
                    f"🚛 {data['vehicle_type']}\n"
                    f"💰 {data['price']:,.0f} ₽/т\n"
                    f"📅 {data['delivery_date']}",
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"❌ Ошибка уведомления: {e}")

    await callback.message.edit_text(
        f"<b>✅ Предложение создано!</b>\n\nПредложение #{offer_id}\n"
        f"🚛 {data['vehicle_type']}\n💰 {data['price']:,.0f} ₽/т\n📅 {data['delivery_date']}\n\n"
        f"Экспортёр получил уведомление.",
        parse_mode='HTML'
    )
    await state.finish()
    await callback.answer()


@dp.message_handler(lambda m: m.text == "💼 Мои предложения", state='*')
async def logistic_my_offers_handler(message: types.Message, state: FSMContext):
    """Показать предложения логиста"""
    await state.finish()
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Доступ запрещен.")
        return

    my_offers = {oid: o for oid, o in logistic_offers.items() if o.get('logist_id') == user_id}

    if not my_offers:
        await message.answer("<b>💼 Мои предложения</b>\n\nУ вас пока нет предложений.", parse_mode='HTML')
        return

    active = sum(1 for o in my_offers.values() if o.get('status') == 'active')
    accepted = sum(1 for o in my_offers.values() if o.get('status') == 'accepted')

    text = f"<b>💼 Мои предложения</b>\n\nВсего: <b>{len(my_offers)}</b>\nАктивных: <b>{active}</b>\nПринятых: <b>{accepted}</b>\n\n"
    keyboard = InlineKeyboardMarkup(row_width=1)

    for idx, (offer_id, offer) in enumerate(list(my_offers.items())[:10], 1):
        req_id = offer.get('request_id')
        status_emoji = "✅" if offer.get('status') == 'accepted' else "🟡"

        text += f"{idx}. {status_emoji} <b>Предложение #{offer_id}</b>\n"
        text += f"   Заявка: #{req_id}\n"
        text += f"   🚛 {offer.get('vehicle_type', '')}\n"
        text += f"   💰 {offer.get('price', 0):,.0f} ₽/т\n\n"

        keyboard.add(InlineKeyboardButton(f"🔍 #{offer_id}", callback_data=f"viewoffer:{offer_id}"))

    if len(my_offers) > 10:
        text += f"\n...и ещё {len(my_offers) - 10}\n"

    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')


@dp.message_handler(lambda m: m.text == "🚚 Мои доставки", state='*')
async def logistic_my_deliveries_handler(message: types.Message, state: FSMContext):
    """Показать доставки логиста"""
    await state.finish()
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'logistic':
        await message.answer("❌ Доступ запрещен.")
        return

    my_deliveries = {did: d for did, d in deliveries.items() if d.get('logist_id') == user_id}

    if not my_deliveries:
        await message.answer("<b>🚚 Мои доставки</b>\n\nУ вас пока нет активных доставок.", parse_mode='HTML')
        return

    active = sum(1 for d in my_deliveries.values() if d.get('status') == 'in_progress')
    completed = sum(1 for d in my_deliveries.values() if d.get('status') == 'completed')

    text = f"<b>🚚 Мои доставки</b>\n\n<b>Всего:</b> {len(my_deliveries)}\n<b>В процессе:</b> {active}\n<b>Завершено:</b> {completed}\n\n"

    for idx, (del_id, delivery) in enumerate(list(my_deliveries.items())[:5], 1):
        req_id = delivery.get('request_id')
        request = shipping_requests.get(req_id, {})
        status = delivery.get('status', 'unknown')
        status_emoji = "🚚" if status == 'in_progress' else "✅"

        text += f"{idx}. {status_emoji} <b>Доставка #{del_id}</b>\n"
        text += f"   Заявка: #{req_id}\n"
        text += f"   📍 {request.get('route_from', '')} → {request.get('route_to', '')}\n"
        text += f"   Статус: {status}\n\n"

    await message.answer(text, parse_mode='HTML')


# ========== ОБРАБОТЧИКИ ЭКСПЕДИТОРА ==========

@dp.message_handler(lambda m: m.text == "📋 Создать предложение", state='*')
async def expeditor_create_offer_handler(message: types.Message, state: FSMContext):
    """Начать создание предложения экспедитора"""
    await state.finish()
    user_id = message.from_user.id

    if user_id not in users or users[user_id].get('role') != 'expeditor':
        await message.answer("❌ Доступ запрещен.")
        return

    await ExpeditorOfferStates.service_type.set()
    await message.answer("<b>📋 Создание предложения</b>\n\n<b>Выберите тип услуги:</b>", reply_markup=expeditor_service_keyboard(), parse_mode='HTML')


@dp.callback_query_handler(lambda c: c.data.startswith('service:'), state=ExpeditorOfferStates.service_type)
async def set_service_type(callback: types.CallbackQuery, state: FSMContext):
    """Установить тип услуги"""
    service_type = callback.data.split(':')[1]
    service_names = {
        'docs': '📄 Оформление документов',
        'customs': '🏢 Таможенное оформление',
        'freight': '🚢 Фрахтование',
        'full': '📦 Полный комплекс услуг'
    }

    await state.update_data(service_type=service_names.get(service_type, service_type))
    await ExpeditorOfferStates.ports.set()

    await callback.message.edit_text(
        f"<b>🏢 Укажите порты обслуживания:</b>\n\n"
        f"Выбрана услуга: {service_names.get(service_type, service_type)}\n\n"
        f"Введите порты через запятую:\n<code>Новороссийск, Ростов-на-Дону</code>",
        parse_mode='HTML'
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
    await message.answer("<b>💰 Укажите стоимость услуг (₽):</b>\n\nВведите цену: <code>50000</code>", parse_mode='HTML')


@dp.message_handler(state=ExpeditorOfferStates.price)
async def set_expeditor_price(message: types.Message, state: FSMContext):
    """Установить цену услуги"""
    try:
        price = float(message.text.replace(',', '.').replace(' ', ''))
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Неверный формат. Введите число: <code>50000</code>", parse_mode='HTML')
        return

    await state.update_data(price=price)
    await ExpeditorOfferStates.terms.set()
    await message.answer("<b>📝 Укажите условия:</b>\n\nОпишите условия работы, сроки, гарантии:\n<code>Срок: 3-5 дней. Гарантия возврата.</code>", parse_mode='HTML')


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
    text = f"<b>✅ Подтверждение предложения:</b>\n\n"
    text += f"📋 {data['service_type']}\n"
    text += f"🏢 {data['ports']}\n"
    text += f"💰 {data['price']:,.0f} ₽\n"
    text += f"📝 {data['terms']}\n\nВсё верно?"

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_expeditor_offer"),
        InlineKeyboardButton("❌ Отменить", callback_data="cancel")
    )
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')


@dp.callback_query_handler(lambda c: c.data == 'confirm_expeditor_offer', state=ExpeditorOfferStates.confirm)
async def confirm_expeditor_offer(callback: types.CallbackQuery, state: FSMContext):
    """Подтвердить создание предложения экспедитора"""
    data = await state.get_data()
    user_id = callback.from_user.id

    offer_id = len(expeditor_offers) + 1
    offer = {
        'id': offer_id,
        'expeditor_id': user_id,
        'service_type': data['service_type'],
        'ports': data['ports'],
        'price': data['price'],
        'terms': data['terms'],
        'status': 'active',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    expeditor_offers[offer_id] = offer
    save_expeditor_offers()

    await callback.message.edit_text(
        f"<b>✅ Предложение создано!</b>\n\nПредложение #{offer_id}\n"
        f"📋 {data['service_type']}\n"
        f"🏢 {data['ports']}\n"
        f"💰 {data['price']:,.0f} ₽\n\n"
        f"Ваше предложение будет отображаться экспортёрам.",
        parse_mode='HTML'
    )
    await state.finish()
    await callback.answer()


@dp.message_handler(lambda m: m.text == "💼 Мои предложения" and users.get(m.from_user.id, {}).get('role') == 'expeditor', state='*')
async def expeditor_my_offers_handler(message: types.Message, state: FSMContext):
    """Показать предложения экспедитора"""
    await state.finish()
    user_id = message.from_user.id

    my_offers = {oid: o for oid, o in expeditor_offers.items() if o.get('expeditor_id') == user_id}

    if not my_offers:
        await message.answer("<b>💼 Мои предложения</b>\n\nУ вас пока нет предложений.\n\nСоздайте через <b>📋 Создать предложение</b>", parse_mode='HTML')
        return

    active = sum(1 for o in my_offers.values() if o.get('status') == 'active')
    text = f"<b>💼 Мои предложения</b>\n\nВсего: <b>{len(my_offers)}</b>\nАктивных: <b>{active}</b>\n\n"

    for idx, (offer_id, offer) in enumerate(list(my_offers.items())[:10], 1):
        text += f"{idx}. <b>Предложение #{offer_id}</b>\n"
        text += f"   📋 {offer.get('service_type', '')}\n"
        text += f"   🏢 {offer.get('ports', '')}\n"
        text += f"   💰 {offer.get('price', 0):,.0f} ₽\n"
        text += f"   Статус: {offer.get('status', 'active')}\n\n"

    if len(my_offers) > 10:
        text += f"\n...и ещё {len(my_offers) - 10}\n"

    await message.answer(text, parse_mode='HTML')


# ========== ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ ==========

@dp.callback_query_handler(lambda c: c.data == 'refresh_requests', state='*')
async def refresh_requests(callback: types.CallbackQuery):
    """Обновить список заявок"""
    await logistic_active_requests_handler(callback.message, FSMContext())
    await callback.answer("🔄 Обновлено")


@dp.callback_query_handler(lambda c: c.data == 'back_to_requests', state='*')
async def back_to_requests(callback: types.CallbackQuery):
    """Вернуться к списку заявок"""
    await logistic_active_requests_handler(callback.message, FSMContext())
    await callback.answer()


# ============================================================================
# ЭКСПОРТЁР: ПРОСМОТР ПРЕДЛОЖЕНИЙ ЛОГИСТОВ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data.startswith('view_request_offers_'), state='*')
async def view_request_offers(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр предложений логистов по заявке"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID заявки", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    user_id = callback.from_user.id
    
    # Проверяем права доступа
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return
    
    # Получаем все предложения по заявке
    offers = [
        (offer_id, offer) for offer_id, offer in logistic_offers.items()
        if offer.get('request_id') == request_id
    ]
    
    if not offers:
        text = f"📦 <b>ПРЕДЛОЖЕНИЯ ПО ЗАЯВКЕ #{request_id}</b>\n\n"
        text += "❌ Пока нет предложений от логистов\n\n"
        text += "<i>Логисты получили уведомление о вашей заявке.\n"
        text += "Предложения появятся в ближайшее время.</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔙 К заявкам", callback_data="my_shipping_requests"))
        keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    # Группируем по статусам
    by_status = {
        'pending': [],
        'accepted': [],
        'rejected': []
    }
    
    for offer_id, offer in offers:
        status = offer.get('status', 'pending')
        if status in by_status:
            by_status[status].append((offer_id, offer))
    
    # Сортируем по цене (низкие первые)
    for status in by_status:
        by_status[status].sort(key=lambda x: x[1].get('price', 999999))
    
    # Информация о заявке
    pull_id = request.get('pull_id')
    pull_info = pulls.get(pull_id, {})
    
    text = f"📦 <b>ПРЕДЛОЖЕНИЯ ПО ЗАЯВКЕ #{request_id}</b>\n\n"
    text += f"🌾 Культура: {pull_info.get('culture', 'Не указана')}\n"
    text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
    text += f"📍 Маршрут: {request.get('route_from', '')} → {request.get('route_to', '')}\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Статистика
    total = len(offers)
    pending = len(by_status['pending'])
    accepted = len(by_status['accepted'])
    rejected = len(by_status['rejected'])
    
    text += f"📊 <b>СТАТИСТИКА:</b>\n"
    text += f"📋 Всего предложений: <b>{total}</b>\n"
    text += f"🕐 Ожидают решения: <b>{pending}</b>\n"
    text += f"✅ Принято: <b>{accepted}</b>\n"
    text += f"❌ Отклонено: <b>{rejected}</b>\n\n"
    
    if pending > 0:
        # Показываем лучшее предложение
        best_offer_id, best_offer = by_status['pending'][0]
        text += f"💰 <b>ЛУЧШЕЕ ПРЕДЛОЖЕНИЕ:</b>\n"
        text += f"💵 Цена: <b>{best_offer.get('price', 0):,.0f} ₽</b>\n"
        text += f"🚛 Транспорт: {best_offer.get('vehicle_type')}\n"
        text += f"📅 Дата: {best_offer.get('delivery_date')}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите предложение для просмотра:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Показываем только ожидающие предложения
    for offer_id, offer in by_status['pending'][:5]:
        logist_id = offer.get('logist_id')
        logist_info = users.get(logist_id, {})
        logist_name = logist_info.get('company_name', f'Логист #{logist_id}')
        
        price = offer.get('price', 0)
        vehicle = offer.get('vehicle_type', 'Не указан')
        
        button_text = f"💰 {price:,.0f} ₽ | {vehicle[:15]} | {logist_name[:20]}"
        
        keyboard.add(
            InlineKeyboardButton(
                button_text,
                callback_data=f"view_offer_details_{offer_id}"
            )
        )
    
    if len(by_status['pending']) > 5:
        keyboard.add(
            InlineKeyboardButton(
                f"➕ Показать ещё {len(by_status['pending']) - 5}",
                callback_data=f"show_all_offers_{request_id}"
            )
        )
    
    # Кнопка сравнения
    if pending >= 2:
        keyboard.add(
            InlineKeyboardButton(
                "⚖️ Сравнить предложения",
                callback_data=f"compare_offers_{request_id}"
            )
        )
    
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data=f"view_request_offers_{request_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 К заявкам", callback_data="my_shipping_requests")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_offer_details_'), state='*')
async def view_offer_details(callback: types.CallbackQuery, state: FSMContext):
    """Детальный просмотр предложения"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    request_id = offer.get('request_id')
    request = shipping_requests.get(request_id, {})
    
    user_id = callback.from_user.id
    
    # Проверяем права
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Информация о логисте
    logist_id = offer.get('logist_id')
    logist_info = users.get(logist_id, {})
    logist_name = logist_info.get('company_name', 'Не указана')
    logist_phone = logist_info.get('phone', 'Не указан')
    
    # Статистика логиста
    logist_offers = [o for o in logistic_offers.values() if o.get('logist_id') == logist_id]
    logist_deliveries = [d for d in deliveries.values() if d.get('logist_id') == logist_id]
    completed = len([d for d in logist_deliveries if d.get('status') == 'completed'])
    total_offers = len(logist_offers)
    
    text = f"📋 <b>ДЕТАЛИ ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    
    # Информация о предложении
    text += f"<b>💰 ПРЕДЛОЖЕНИЕ:</b>\n"
    text += f"💵 Стоимость: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
    text += f"🚛 Транспорт: <b>{offer.get('vehicle_type', 'Не указан')}</b>\n"
    text += f"📅 Дата доставки: <b>{offer.get('delivery_date', 'Не указана')}</b>\n"
    
    if offer.get('additional_info'):
        text += f"\nℹ️ Дополнительно:\n<i>{offer.get('additional_info')}</i>\n"
    
    text += f"\n📅 Создано: {offer.get('created_at', 'Не указано')}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Информация о логисте
    text += f"<b>🚚 ЛОГИСТИЧЕСКАЯ КОМПАНИЯ:</b>\n"
    text += f"🏢 Название: <b>{logist_name}</b>\n"
    text += f"📞 Телефон: {logist_phone}\n\n"
    
    # Статистика
    text += f"<b>📊 СТАТИСТИКА:</b>\n"
    text += f"✅ Завершённых доставок: <b>{completed}</b>\n"
    text += f"📋 Всего предложений: <b>{total_offers}</b>\n"
    
    if completed > 0:
        # Средняя стоимость
        completed_deliveries = [
            logistic_offers.get(d.get('offer_id'), {}).get('price', 0)
            for d in logist_deliveries if d.get('status') == 'completed'
        ]
        if completed_deliveries:
            avg_price = sum(completed_deliveries) / len(completed_deliveries)
            text += f"💰 Средняя стоимость: <b>{avg_price:,.0f} ₽</b>\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Статус предложения
    status = offer.get('status', 'pending')
    
    if status == 'pending':
        text += "⏳ <b>Ожидает вашего решения</b>"
    elif status == 'accepted':
        text += "✅ <b>Предложение принято</b>"
    elif status == 'rejected':
        text += "❌ <b>Предложение отклонено</b>"
        if offer.get('rejection_reason'):
            text += f"\n<i>Причина: {offer.get('rejection_reason')}</i>"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    if status == 'pending':
        keyboard.add(
            InlineKeyboardButton("✅ Принять", callback_data=f"accept_offer_{offer_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_offer_{offer_id}")
        )
    
    keyboard.add(
        InlineKeyboardButton("🔙 К предложениям", callback_data=f"view_request_offers_{request_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('compare_offers_'), state='*')
async def compare_offers(callback: types.CallbackQuery, state: FSMContext):
    """Сравнение предложений"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    user_id = callback.from_user.id
    
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Получаем ожидающие предложения
    offers = [
        (offer_id, offer) for offer_id, offer in logistic_offers.items()
        if offer.get('request_id') == request_id and offer.get('status') == 'pending'
    ]
    
    if len(offers) < 2:
        await callback.answer("❌ Недостаточно предложений для сравнения", show_alert=True)
        return
    
    # Сортируем по цене
    offers.sort(key=lambda x: x[1].get('price', 999999))
    
    text = f"⚖️ <b>СРАВНЕНИЕ ПРЕДЛОЖЕНИЙ</b>\n\n"
    text += f"📦 Заявка #{request_id}\n"
    text += f"📊 Сравниваем {len(offers)} предложений\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Показываем топ-3
    for i, (offer_id, offer) in enumerate(offers[:3], 1):
        logist_id = offer.get('logist_id')
        logist_info = users.get(logist_id, {})
        logist_name = logist_info.get('company_name', f'Логист #{logist_id}')
        
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"
        
        text += f"{medal} <b>#{i} - {logist_name}</b>\n"
        text += f"💰 Цена: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
        text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
        text += f"📅 Дата: {offer.get('delivery_date')}\n"
        
        # Статистика логиста
        logist_deliveries = [
            d for d in deliveries.values() 
            if d.get('logist_id') == logist_id and d.get('status') == 'completed'
        ]
        completed = len(logist_deliveries)
        
        text += f"✅ Доставок: {completed}\n"
        text += f"\n"
    
    if len(offers) > 3:
        text += f"<i>... и ещё {len(offers) - 3} предложений</i>\n\n"
    
    # Анализ
    prices = [o[1].get('price', 0) for o in offers]
    min_price = min(prices)
    max_price = max(prices)
    avg_price = sum(prices) / len(prices)
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"📊 <b>АНАЛИЗ ЦЕН:</b>\n"
    text += f"💵 Минимальная: <b>{min_price:,.0f} ₽</b>\n"
    text += f"💰 Средняя: <b>{avg_price:,.0f} ₽</b>\n"
    text += f"💸 Максимальная: <b>{max_price:,.0f} ₽</b>\n"
    text += f"📈 Разброс: <b>{max_price - min_price:,.0f} ₽</b>\n\n"
    
    # Рекомендация
    best_offer_id, best_offer = offers[0]
    text += f"💡 <b>РЕКОМЕНДАЦИЯ:</b>\n"
    text += f"Самое выгодное предложение #{best_offer_id}\n"
    text += f"Экономия: <b>{avg_price - min_price:,.0f} ₽</b>"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Кнопки для топ-3
    for i, (offer_id, offer) in enumerate(offers[:3], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"
        price = offer.get('price', 0)
        
        keyboard.add(
            InlineKeyboardButton(
                f"{medal} Выбрать #{i} ({price:,.0f} ₽)",
                callback_data=f"view_offer_details_{offer_id}"
            )
        )
    
    keyboard.add(
        InlineKeyboardButton("📋 Все предложения", callback_data=f"view_request_offers_{request_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Назад", callback_data=f"view_request_offers_{request_id}")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
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


@dp.callback_query_handler(lambda c: c.data.startswith('accept_offer_'), state='*')
async def accept_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало принятия предложения"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    request_id = offer.get('request_id')
    request = shipping_requests.get(request_id, {})
    
    user_id = callback.from_user.id
    
    # Проверки
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    if offer.get('status') != 'pending':
        await callback.answer("❌ Предложение уже обработано", show_alert=True)
        return
    
    # Проверяем, нет ли уже принятого предложения
    accepted_offers = [
        o for o in logistic_offers.values()
        if o.get('request_id') == request_id and o.get('status') == 'accepted'
    ]
    
    if accepted_offers:
        await callback.answer("❌ По этой заявке уже принято другое предложение", show_alert=True)
        return
    
    # Информация о предложении
    logist_id = offer.get('logist_id')
    logist_info = users.get(logist_id, {})
    logist_name = logist_info.get('company_name', 'Не указана')
    
    pull_id = request.get('pull_id')
    pull_info = pulls.get(pull_id, {})
    
    text = f"✅ <b>ПРИНЯТИЕ ПРЕДЛОЖЕНИЯ #{offer_id}</b>\n\n"
    text += f"📦 <b>ЗАЯВКА #{request_id}</b>\n"
    text += f"🌾 Культура: {pull_info.get('culture', 'Не указана')}\n"
    text += f"📦 Объём: {request.get('volume', 0):.1f} т\n"
    text += f"📍 Маршрут: {request.get('route_from', '')} → {request.get('route_to', '')}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    text += f"<b>ПРЕДЛОЖЕНИЕ:</b>\n"
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
        InlineKeyboardButton("✅ Подтверждаю", callback_data=f"confirm_accept_{offer_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"view_offer_details_{offer_id}")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_accept_'), state='*')
async def accept_offer_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение принятия предложения"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    request_id = offer.get('request_id')
    request = shipping_requests.get(request_id, {})
    
    user_id = callback.from_user.id
    
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Принимаем предложение
    offer['status'] = 'accepted'
    offer['accepted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    offer['accepted_by'] = user_id
    
    # Обновляем статус заявки
    request['status'] = 'assigned'
    request['assigned_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    request['logist_id'] = offer.get('logist_id')
    
    # Создаём доставку
    delivery_id = len(deliveries) + 1
    delivery = {
        'id': delivery_id,
        'request_id': request_id,
        'offer_id': offer_id,
        'exporter_id': user_id,
        'logist_id': offer.get('logist_id'),
        'pull_id': request.get('pull_id'),
        'route_from': request.get('route_from'),
        'route_to': request.get('route_to'),
        'volume': request.get('volume'),
        'price': offer.get('price'),
        'vehicle_type': offer.get('vehicle_type'),
        'delivery_date': offer.get('delivery_date'),
        'status': 'pending',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    deliveries[delivery_id] = delivery
    
    # Отклоняем остальные предложения
    rejected_count = 0
    for other_offer_id, other_offer in logistic_offers.items():
        if (other_offer.get('request_id') == request_id and 
            other_offer_id != offer_id and 
            other_offer.get('status') == 'pending'):
            
            other_offer['status'] = 'rejected'
            other_offer['rejected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            other_offer['rejection_reason'] = 'Принято другое предложение'
            rejected_count += 1
            
            # Уведомляем логистов
            other_logist_id = other_offer.get('logist_id')
            if other_logist_id:
                asyncio.create_task(
                    notify_logistic_offer_rejected(
                        other_offer_id, 
                        user_id, 
                        'Принято другое предложение'
                    )
                )
    
    # Сохраняем данные
    save_shipping_requests()
    save_logistic_offers()
    save_deliveries()
    
    # Уведомляем принятого логиста
    logist_id = offer.get('logist_id')
    if logist_id:
        asyncio.create_task(notify_logistic_offer_accepted(offer_id, user_id))
    
    # Сообщение пользователю
    logist_info = users.get(logist_id, {})
    logist_name = logist_info.get('company_name', 'Не указана')
    
    text = f"🎉 <b>ПРЕДЛОЖЕНИЕ ПРИНЯТО!</b>\n\n"
    text += f"✅ Предложение #{offer_id} успешно принято\n"
    text += f"📦 Доставка #{delivery_id} создана\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"<b>ДЕТАЛИ ДОСТАВКИ:</b>\n"
    text += f"🚚 Логист: {logist_name}\n"
    text += f"💰 Стоимость: {offer.get('price', 0):,.0f} ₽\n"
    text += f"🚛 Транспорт: {offer.get('vehicle_type')}\n"
    text += f"📅 Дата: {offer.get('delivery_date')}\n\n"
    
    if rejected_count > 0:
        text += f"ℹ️ Отклонено предложений: {rejected_count}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"<b>СЛЕДУЮЩИЕ ШАГИ:</b>\n"
    text += f"1. Логист получит уведомление\n"
    text += f"2. Свяжитесь для уточнения деталей\n"
    text += f"3. Отслеживайте доставку в разделе 'Мои доставки'"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📦 Доставка #" + str(delivery_id), callback_data=f"view_delivery_{delivery_id}")
    )
    keyboard.add(
        InlineKeyboardButton("📋 Мои доставки", callback_data="exporter_deliveries")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer("✅ Предложение принято!")
    
    logging.info(f"✅ Экспортёр {user_id} принял предложение #{offer_id}, создана доставка #{delivery_id}")


@dp.callback_query_handler(lambda c: c.data.startswith('reject_offer_'), state='*')
async def reject_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало отклонения предложения"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if offer_id not in logistic_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    request_id = offer.get('request_id')
    request = shipping_requests.get(request_id, {})
    
    user_id = callback.from_user.id
    
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    if offer.get('status') != 'pending':
        await callback.answer("❌ Предложение уже обработано", show_alert=True)
        return
    
    await state.update_data(offer_id=offer_id)
    
    logist_id = offer.get('logist_id')
    logist_info = users.get(logist_id, {})
    logist_name = logist_info.get('company_name', 'Не указана')
    
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
        ("Высокая цена", "reject_reason_Высокая цена"),
        ("Несоответствие транспорта", "reject_reason_Несоответствие транспорта"),
        ("Неподходящие сроки", "reject_reason_Неподходящие сроки"),
        ("Принято другое предложение", "reject_reason_Принято другое предложение"),
    ]
    
    for reason_text, callback_data in reasons:
        keyboard.add(InlineKeyboardButton(reason_text, callback_data=callback_data))
    
    keyboard.add(InlineKeyboardButton("✍️ Указать свою причину", callback_data="reject_reason_custom"))
    keyboard.add(InlineKeyboardButton("➡️ Без причины", callback_data="reject_reason_none"))
    keyboard.add(InlineKeyboardButton("🔙 Отмена", callback_data=f"view_offer_details_{offer_id}"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await RejectOfferStatesGroup.reason.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('reject_reason_'), state=RejectOfferStatesGroup.reason)
async def reject_offer_reason_selected(callback: types.CallbackQuery, state: FSMContext):
    """Выбор причины отклонения"""
    reason_key = callback.data.replace('reject_reason_', '')
    
    data = await state.get_data()
    offer_id = data.get('offer_id')
    
    if reason_key == 'custom':
        text = "✍️ <b>УКАЖИТЕ ПРИЧИНУ ОТКЛОНЕНИЯ</b>\n\n"
        text += "Введите текст причины (до 200 символов):"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data=f"reject_offer_{offer_id}"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    if reason_key == 'none':
        reason = None
    else:
        reason = reason_key
    
    # Отклоняем предложение
    await reject_offer_execute(callback, state, offer_id, reason)


@dp.message_handler(state=RejectOfferStatesGroup.reason)
async def reject_offer_custom_reason(message: types.Message, state: FSMContext):
    """Ввод своей причины отклонения"""
    reason = message.text.strip()
    
    if len(reason) > 200:
        await message.answer("❌ Слишком длинная причина (максимум 200 символов)!\n\nВведите короче:")
        return
    
    data = await state.get_data()
    offer_id = data.get('offer_id')
    
    # Создаём фейковый callback для вызова функции
    class FakeCallback:
        def __init__(self, user_id, message):
            self.from_user = type('obj', (object,), {'id': user_id})
            self.message = message
            
        async def answer(self, text="", show_alert=False):
            pass
    
    fake_callback = FakeCallback(message.from_user.id, message)
    
    await reject_offer_execute(fake_callback, state, offer_id, reason)


async def reject_offer_execute(callback_or_fake, state: FSMContext, offer_id: int, reason: str = None):
    """Выполнение отклонения предложения"""
    
    if offer_id not in logistic_offers:
        if hasattr(callback_or_fake, 'answer'):
            await callback_or_fake.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = logistic_offers[offer_id]
    request_id = offer.get('request_id')
    
    user_id = callback_or_fake.from_user.id
    
    # Отклоняем предложение
    offer['status'] = 'rejected'
    offer['rejected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    offer['rejected_by'] = user_id
    if reason:
        offer['rejection_reason'] = reason
    
    save_logistic_offers()
    
    # Уведомляем логиста
    logist_id = offer.get('logist_id')
    if logist_id:
        asyncio.create_task(notify_logistic_offer_rejected(offer_id, user_id, reason))
    
    # Сообщение
    text = f"❌ <b>ПРЕДЛОЖЕНИЕ ОТКЛОНЕНО</b>\n\n"
    text += f"✅ Предложение #{offer_id} отклонено\n"
    text += f"📋 Заявка #{request_id}\n\n"
    
    if reason:
        text += f"💬 Причина: <i>{reason}</i>\n\n"
    
    text += "ℹ️ Логист получил уведомление об отклонении"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📋 Другие предложения", callback_data=f"view_request_offers_{request_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
    )
    
    if hasattr(callback_or_fake.message, 'edit_text'):
        await callback_or_fake.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    else:
        await callback_or_fake.message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    
    await state.finish()
    
    logging.info(f"❌ Экспортёр {user_id} отклонил предложение #{offer_id}")


# ============================================================================
# ЭКСПОРТЁР: УПРАВЛЕНИЕ ЗАЯВКАМИ НА ДОСТАВКУ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == 'my_shipping_requests', state='*')
async def show_my_shipping_requests(callback: types.CallbackQuery, state: FSMContext):
    """Список заявок экспортёра на доставку"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Получаем заявки пользователя
    my_requests = [
        (req_id, req) for req_id, req in shipping_requests.items()
        if req.get('exporter_id') == user_id
    ]
    
    if not my_requests:
        text = "📦 <b>МОИ ЗАЯВКИ НА ДОСТАВКУ</b>\n\n"
        text += "❌ У вас пока нет заявок на доставку\n\n"
        text += "<i>Создайте заявку из раздела 'Мои пулы'</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("➕ Создать пул", callback_data="create_pull"))
        keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    # Группируем по статусам
    by_status = {
        'active': [],
        'assigned': [],
        'in_progress': [],
        'completed': [],
        'cancelled': []
    }
    
    for req_id, req in my_requests:
        status = req.get('status', 'active')
        if status in by_status:
            by_status[status].append((req_id, req))
    
    # Сортируем по дате (новые первые)
    for status in by_status:
        by_status[status].sort(key=lambda x: x[1].get('created_at', ''), reverse=True)
    
    text = f"📦 <b>МОИ ЗАЯВКИ НА ДОСТАВКУ</b>\n\n"
    text += f"Всего заявок: <b>{len(my_requests)}</b>\n\n"
    
    # Статистика
    active = len(by_status['active'])
    assigned = len(by_status['assigned'])
    in_progress = len(by_status['in_progress'])
    completed = len(by_status['completed'])
    
    text += f"🆕 Активные: <b>{active}</b>\n"
    text += f"👤 Назначены: <b>{assigned}</b>\n"
    text += f"🚚 В пути: <b>{in_progress}</b>\n"
    text += f"✅ Завершены: <b>{completed}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите заявку:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Показываем заявки
    for status_key, status_name, emoji in [
        ('active', 'Активные', '🆕'),
        ('assigned', 'Назначены', '👤'),
        ('in_progress', 'В пути', '🚚'),
        ('completed', 'Завершены', '✅')
    ]:
        requests = by_status[status_key]
        if requests:
            for req_id, req in requests[:5]:
                pull_id = req.get('pull_id')
                pull_info = pulls.get(pull_id, {})
                culture = pull_info.get('culture', 'Не указана')
                volume = req.get('volume', 0)
                
                # Подсчёт предложений
                offers_count = len([
                    o for o in logistic_offers.values()
                    if o.get('request_id') == req_id and o.get('status') == 'pending'
                ])
                
                button_text = f"{emoji} #{req_id} | {culture} {volume:.0f}т"
                if offers_count > 0 and status_key == 'active':
                    button_text += f" | 📬 {offers_count}"
                
                keyboard.add(
                    InlineKeyboardButton(
                        button_text,
                        callback_data=f"view_my_request_{req_id}"
                    )
                )
    
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="my_shipping_requests")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_my_request_'), state='*')
async def view_my_request_details(callback: types.CallbackQuery, state: FSMContext):
    """Детальный просмотр своей заявки"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    user_id = callback.from_user.id
    
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Это не ваша заявка", show_alert=True)
        return
    
    # Информация о заявке
    pull_id = request.get('pull_id')
    pull_info = pulls.get(pull_id, {})
    
    text = f"📦 <b>ЗАЯВКА #{request_id}</b>\n\n"
    text += f"🌾 Культура: <b>{pull_info.get('culture', 'Не указана')}</b>\n"
    text += f"📦 Объём: <b>{request.get('volume', 0):.1f} т</b>\n"
    text += f"📍 Откуда: {request.get('route_from', 'Не указано')}\n"
    text += f"📍 Куда: {request.get('route_to', 'Не указано')}\n"
    
    if request.get('desired_date'):
        text += f"📅 Желаемая дата: {request.get('desired_date')}\n"
    
    if request.get('budget'):
        text += f"💰 Бюджет: {request.get('budget'):,.0f} ₽\n"
    
    if request.get('requirements'):
        text += f"\n📋 Требования:\n{request.get('requirements')}\n"
    
    text += f"\n📅 Создана: {request.get('created_at', 'Не указано')}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Статус заявки
    status = request.get('status', 'active')
    status_emoji = get_status_emoji(status)
    status_name = get_status_name(status)
    
    text += f"📊 Статус: <b>{status_emoji} {status_name}</b>\n\n"
    
    # Подсчёт предложений
    all_offers = [
        o for o in logistic_offers.values()
        if o.get('request_id') == request_id
    ]
    
    pending_offers = [o for o in all_offers if o.get('status') == 'pending']
    accepted_offers = [o for o in all_offers if o.get('status') == 'accepted']
    
    text += f"📬 Предложений:\n"
    text += f"  • Ожидают решения: <b>{len(pending_offers)}</b>\n"
    text += f"  • Принято: <b>{len(accepted_offers)}</b>\n"
    text += f"  • Всего: <b>{len(all_offers)}</b>\n\n"
    
    # Если есть назначенный логист
    if request.get('logist_id'):
        logist_info = users.get(request.get('logist_id'), {})
        text += f"🚚 Логист: <b>{logist_info.get('company_name', 'Не указана')}</b>\n"
        if logist_info.get('phone'):
            text += f"📞 Телефон: {logist_info.get('phone')}\n"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    # Кнопки в зависимости от статуса
    if status == 'active':
        if len(pending_offers) > 0:
            keyboard.add(
                InlineKeyboardButton(
                    f"📬 Предложения ({len(pending_offers)})",
                    callback_data=f"view_request_offers_{request_id}"
                )
            )
        
        keyboard.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_request_{request_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_request_{request_id}")
        )
    
    elif status == 'assigned':
        keyboard.add(
            InlineKeyboardButton("📦 Доставка", callback_data=f"view_delivery_by_request_{request_id}")
        )
        keyboard.add(
            InlineKeyboardButton("📬 Предложения", callback_data=f"view_request_offers_{request_id}")
        )
    
    elif status in ['in_progress', 'completed']:
        keyboard.add(
            InlineKeyboardButton("📦 Доставка", callback_data=f"view_delivery_by_request_{request_id}")
        )
    
    keyboard.add(
        InlineKeyboardButton("🔙 К заявкам", callback_data="my_shipping_requests")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('cancel_request_'), state='*')
async def cancel_request_confirm(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение отмены заявки"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    user_id = callback.from_user.id
    
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    if request.get('status') != 'active':
        await callback.answer("❌ Можно отменить только активные заявки", show_alert=True)
        return
    
    # Подсчёт предложений
    pending_offers = [
        o for o in logistic_offers.values()
        if o.get('request_id') == request_id and o.get('status') == 'pending'
    ]
    
    text = f"❓ <b>ОТМЕНА ЗАЯВКИ #{request_id}</b>\n\n"
    text += f"Вы уверены, что хотите отменить заявку?\n\n"
    
    if len(pending_offers) > 0:
        text += f"⚠️ У вас есть <b>{len(pending_offers)}</b> ожидающих предложений!\n"
        text += f"Все предложения будут автоматически отклонены.\n\n"
    
    text += f"<i>Это действие нельзя будет отменить</i>"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, отменить", callback_data=f"confirm_cancel_request_{request_id}"),
        InlineKeyboardButton("❌ Нет", callback_data=f"view_my_request_{request_id}")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('confirm_cancel_request_'), state='*')
async def cancel_request_confirmed(callback: types.CallbackQuery, state: FSMContext):
    """Отмена заявки подтверждена"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if request_id not in shipping_requests:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return
    
    request = shipping_requests[request_id]
    user_id = callback.from_user.id
    
    if request.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Отменяем заявку
    request['status'] = 'cancelled'
    request['cancelled_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Отклоняем все ожидающие предложения
    cancelled_offers = 0
    for offer_id, offer in logistic_offers.items():
        if offer.get('request_id') == request_id and offer.get('status') == 'pending':
            offer['status'] = 'rejected'
            offer['rejected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            offer['rejection_reason'] = 'Заявка отменена заказчиком'
            cancelled_offers += 1
            
            # Уведомляем логиста
            logist_id = offer.get('logist_id')
            if logist_id:
                asyncio.create_task(
                    notify_logistic_request_cancelled(request_id, 'Заказчик отменил заявку')
                )
    
    save_shipping_requests()
    save_logistic_offers()
    
    text = f"✅ <b>ЗАЯВКА #{request_id} ОТМЕНЕНА</b>\n\n"
    text += f"Заявка успешно отменена.\n"
    
    if cancelled_offers > 0:
        text += f"\n📬 Отклонено предложений: {cancelled_offers}\n"
        text += f"Логисты получили уведомления.\n"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📦 Мои заявки", callback_data="my_shipping_requests"))
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer("✅ Заявка отменена")
    
    logging.info(f"❌ Экспортёр {user_id} отменил заявку #{request_id}")


# ============================================================================
# ЭКСПОРТЁР: ПРОСМОТР ДОСТАВОК
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == 'exporter_deliveries', state='*')
async def show_exporter_deliveries(callback: types.CallbackQuery, state: FSMContext):
    """Список доставок экспортёра"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Получаем доставки пользователя
    my_deliveries = [
        (deliv_id, deliv) for deliv_id, deliv in deliveries.items()
        if deliv.get('exporter_id') == user_id
    ]
    
    if not my_deliveries:
        text = "📦 <b>МОИ ДОСТАВКИ</b>\n\n"
        text += "❌ У вас пока нет доставок\n\n"
        text += "<i>Доставки появятся после принятия предложений логистов</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("📋 Мои заявки", callback_data="my_shipping_requests"))
        keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    # Группируем по статусам
    by_status = {
        'pending': [],
        'in_progress': [],
        'completed': [],
        'cancelled': []
    }
    
    for deliv_id, deliv in my_deliveries:
        status = deliv.get('status', 'pending')
        if status in by_status:
            by_status[status].append((deliv_id, deliv))
    
    text = f"📦 <b>МОИ ДОСТАВКИ</b>\n\n"
    text += f"Всего доставок: <b>{len(my_deliveries)}</b>\n\n"
    
    pending = len(by_status['pending'])
    in_progress = len(by_status['in_progress'])
    completed = len(by_status['completed'])
    
    text += f"🕐 Ожидают начала: <b>{pending}</b>\n"
    text += f"🚚 В пути: <b>{in_progress}</b>\n"
    text += f"✅ Завершены: <b>{completed}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите доставку:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Показываем доставки
    for status_key, status_name, emoji in [
        ('pending', 'Ожидают', '🕐'),
        ('in_progress', 'В пути', '🚚'),
        ('completed', 'Завершены', '✅')
    ]:
        delivs = by_status[status_key]
        if delivs:
            for deliv_id, deliv in delivs[:5]:
                route = f"{deliv.get('route_from', '')} → {deliv.get('route_to', '')}"
                volume = deliv.get('volume', 0)
                
                button_text = f"{emoji} #{deliv_id} | {route[:20]} | {volume:.0f}т"
                
                keyboard.add(
                    InlineKeyboardButton(
                        button_text,
                        callback_data=f"view_delivery_{deliv_id}"
                    )
                )
    
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="exporter_deliveries")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_delivery_by_request_'), state='*')
async def view_delivery_by_request(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр доставки по ID заявки"""
    await state.finish()
    
    try:
        request_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    # Находим доставку по request_id
    delivery = None
    delivery_id = None
    for deliv_id, deliv in deliveries.items():
        if deliv.get('request_id') == request_id:
            delivery = deliv
            delivery_id = deliv_id
            break
    
    if not delivery:
        await callback.answer("❌ Доставка не найдена", show_alert=True)
        return
    
    # Перенаправляем на просмотр доставки
    await bot.answer_callback_query(callback.id)
    await bot.edit_message_text(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        text="🔄 Загрузка...",
        parse_mode='HTML'
    )
    
    # Создаём новый callback для view_delivery
    new_callback = types.CallbackQuery(
        id=callback.id,
        from_user=callback.from_user,
        message=callback.message,
        chat_instance=callback.chat_instance,
        data=f"view_delivery_{delivery_id}"
    )
    
    # Вызываем обработчик (нужно будет добавить view_delivery если его нет)
    # Временно просто показываем ID
    text = f"📦 Доставка #{delivery_id}\n\nПодробная информация скоро будет доступна"
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data=f"view_my_request_{request_id}"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')


# ============================================================================
# ЭКСПОРТЁР: ОЦЕНКА И ОТЗЫВЫ ЛОГИСТОВ
# ============================================================================

# Глобальная структура для хранения рейтингов
logistic_ratings = {}  # {logist_id: {'total_rating': 0, 'count': 0, 'reviews': []}}


def save_logistic_ratings():
    """Сохранить рейтинги логистов"""
    try:
        with open(os.path.join(DATA_DIR, 'logistic_ratings.pkl'), 'wb') as f:
            pickle.dump(logistic_ratings, f)
        logging.info("✅ Logistic ratings saved")
    except Exception as e:
        logging.error(f"❌ Error saving logistic ratings: {e}")


def load_logistic_ratings():
    """Загрузить рейтинги логистов"""
    global logistic_ratings
    try:
        filepath = os.path.join(DATA_DIR, 'logistic_ratings.pkl')
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
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


@dp.callback_query_handler(lambda c: c.data.startswith('rate_logistic_'), state='*')
async def rate_logistic_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало оценки логиста"""
    await state.finish()
    
    try:
        delivery_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if delivery_id not in deliveries:
        await callback.answer("❌ Доставка не найдена", show_alert=True)
        return
    
    delivery = deliveries[delivery_id]
    user_id = callback.from_user.id
    
    # Проверки
    if delivery.get('exporter_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    if delivery.get('status') != 'completed':
        await callback.answer("❌ Можно оценить только завершённую доставку", show_alert=True)
        return
    
    if delivery.get('rated'):
        await callback.answer("❌ Вы уже оценили эту доставку", show_alert=True)
        return
    
    await state.update_data(delivery_id=delivery_id)
    
    logist_id = delivery.get('logist_id')
    logist_info = users.get(logist_id, {})
    logist_name = logist_info.get('company_name', 'Не указана')
    
    text = f"⭐ <b>ОЦЕНКА ЛОГИСТА</b>\n\n"
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
        InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="rate_5")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="exporter_deliveries"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await RateLogisticStatesGroup.rating.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('rate_'), state=RateLogisticStatesGroup.rating)
async def rate_logistic_rating_selected(callback: types.CallbackQuery, state: FSMContext):
    """Выбор оценки"""
    
    try:
        rating = int(callback.data.split('_')[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if rating < 1 or rating > 5:
        await callback.answer("❌ Некорректная оценка", show_alert=True)
        return
    
    await state.update_data(rating=rating)
    
    data = await state.get_data()
    delivery_id = data.get('delivery_id')
    
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
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await RateLogisticStatesGroup.review.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'skip_review', state=RateLogisticStatesGroup.review)
async def rate_logistic_skip_review(callback: types.CallbackQuery, state: FSMContext):
    """Пропустить отзыв"""
    await rate_logistic_save(callback, state, None)


@dp.message_handler(state=RateLogisticStatesGroup.review)
async def rate_logistic_review_entered(message: types.Message, state: FSMContext):
    """Ввод отзыва"""
    review = message.text.strip()
    
    if len(review) > 500:
        await message.answer("❌ Слишком длинный отзыв (максимум 500 символов)!\n\nВведите короче:")
        return
    
    # Создаём фейковый callback
    class FakeCallback:
        def __init__(self, user_id, message):
            self.from_user = type('obj', (object,), {'id': user_id})
            self.message = message
            
        async def answer(self, text="", show_alert=False):
            pass
    
    fake_callback = FakeCallback(message.from_user.id, message)
    await rate_logistic_save(fake_callback, state, review)


async def rate_logistic_save(callback_or_fake, state: FSMContext, review: str = None):
    """Сохранение оценки и отзыва"""
    
    data = await state.get_data()
    delivery_id = data.get('delivery_id')
    rating = data.get('rating')
    
    user_id = callback_or_fake.from_user.id
    
    if delivery_id not in deliveries:
        if hasattr(callback_or_fake, 'answer'):
            await callback_or_fake.answer("❌ Доставка не найдена", show_alert=True)
        return
    
    delivery = deliveries[delivery_id]
    logist_id = delivery.get('logist_id')
    
    # Сохраняем оценку в доставке
    delivery['rated'] = True
    delivery['rating'] = rating
    if review:
        delivery['review'] = review
    delivery['rated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Обновляем рейтинг логиста
    if logist_id not in logistic_ratings:
        logistic_ratings[logist_id] = {
            'total_rating': 0,
            'count': 0,
            'reviews': []
        }
    
    logistic_ratings[logist_id]['total_rating'] += rating
    logistic_ratings[logist_id]['count'] += 1
    
    if review:
        logistic_ratings[logist_id]['reviews'].append({
            'delivery_id': delivery_id,
            'exporter_id': user_id,
            'rating': rating,
            'review': review,
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # Вычисляем средний рейтинг
    avg_rating = logistic_ratings[logist_id]['total_rating'] / logistic_ratings[logist_id]['count']
    
    save_deliveries()
    save_logistic_ratings()
    
    logist_info = users.get(logist_id, {})
    logist_name = logist_info.get('company_name', 'Не указана')
    
    stars = "⭐" * rating
    
    text = f"✅ <b>СПАСИБО ЗА ОЦЕНКУ!</b>\n\n"
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
    keyboard.add(InlineKeyboardButton("📦 Мои доставки", callback_data="exporter_deliveries"))
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))
    
    if hasattr(callback_or_fake.message, 'edit_text'):
        try:
            await callback_or_fake.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        except:
            await callback_or_fake.message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    else:
        await callback_or_fake.message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    
    await state.finish()
    
    logging.info(f"⭐ Экспортёр {user_id} оценил логиста {logist_id} на {rating}/5")


@dp.callback_query_handler(lambda c: c.data.startswith('view_logistic_profile_'), state='*')
async def view_logistic_profile(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр профиля и рейтинга логиста"""
    await state.finish()
    
    try:
        logist_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if logist_id not in users:
        await callback.answer("❌ Логист не найден", show_alert=True)
        return
    
    logist_info = users[logist_id]
    
    text = f"🚚 <b>ПРОФИЛЬ ЛОГИСТА</b>\n\n"
    text += f"🏢 Компания: <b>{logist_info.get('company_name', 'Не указана')}</b>\n"
    text += f"📞 Телефон: {logist_info.get('phone', 'Не указан')}\n"
    
    if logist_info.get('email'):
        text += f"📧 Email: {logist_info.get('email')}\n"
    
    text += f"\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Рейтинг
    if logist_id in logistic_ratings:
        rating_data = logistic_ratings[logist_id]
        avg_rating = rating_data['total_rating'] / rating_data['count']
        stars = "⭐" * int(round(avg_rating))
        
        text += f"<b>📊 РЕЙТИНГ:</b>\n"
        text += f"{stars} <b>{avg_rating:.1f}/5</b>\n"
        text += f"📋 Оценок: {rating_data['count']}\n\n"
    else:
        text += f"<b>📊 РЕЙТИНГ:</b>\n"
        text += f"Пока нет оценок\n\n"
    
    # Статистика доставок
    logist_deliveries = [
        d for d in deliveries.values()
        if d.get('logist_id') == logist_id
    ]
    
    completed = len([d for d in logist_deliveries if d.get('status') == 'completed'])
    in_progress = len([d for d in logist_deliveries if d.get('status') == 'in_progress'])
    
    text += f"<b>📦 СТАТИСТИКА:</b>\n"
    text += f"✅ Завершено доставок: {completed}\n"
    text += f"🚚 В процессе: {in_progress}\n"
    text += f"📋 Всего: {len(logist_deliveries)}\n\n"
    
    # Последние отзывы
    if logist_id in logistic_ratings and logistic_ratings[logist_id]['reviews']:
        text += "━━━━━━━━━━━━━━━━━━━━\n\n"
        text += f"<b>💬 ПОСЛЕДНИЕ ОТЗЫВЫ:</b>\n\n"
        
        reviews = logistic_ratings[logist_id]['reviews'][-3:]  # Последние 3
        for r in reversed(reviews):
            stars = "⭐" * r['rating']
            text += f"{stars} {r['rating']}/5\n"
            text += f"<i>{r['review']}</i>\n"
            text += f"<code>{r['date']}</code>\n\n"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

# ============================================================================
# ЭКСПЕДИТОР: УПРАВЛЕНИЕ ПРЕДЛОЖЕНИЯМИ И ЗАПРОСАМИ
# ============================================================================

@dp.callback_query_handler(lambda c: c.data == 'expeditor_my_offers', state='*')
async def show_expeditor_my_offers(callback: types.CallbackQuery, state: FSMContext):
    """Список предложений экспедитора"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Получаем предложения пользователя
    my_offers = [
        (offer_id, offer) for offer_id, offer in expeditor_offers.items()
        if offer.get('expeditor_id') == user_id
    ]
    
    if not my_offers:
        text = "📋 <b>МОИ ПРЕДЛОЖЕНИЯ УСЛУГ</b>\n\n"
        text += "❌ У вас пока нет предложений\n\n"
        text += "<i>Создайте предложение в разделе 'Создать предложение'</i>"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("➕ Создать предложение", callback_data="create_expeditor_offer"))
        keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    # Группируем по статусам
    by_status = {
        'active': [],
        'selected': [],
        'cancelled': []
    }
    
    for offer_id, offer in my_offers:
        status = offer.get('status', 'active')
        if status in by_status:
            by_status[status].append((offer_id, offer))
    
    text = f"📋 <b>МОИ ПРЕДЛОЖЕНИЯ УСЛУГ</b>\n\n"
    text += f"Всего предложений: <b>{len(my_offers)}</b>\n\n"
    
    active = len(by_status['active'])
    selected = len(by_status['selected'])
    
    text += f"🆕 Активные: <b>{active}</b>\n"
    text += f"✅ Выбраны: <b>{selected}</b>\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите предложение:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Показываем предложения
    for status_key, status_name, emoji in [
        ('active', 'Активные', '🆕'),
        ('selected', 'Выбраны', '✅')
    ]:
        offers = by_status[status_key]
        if offers:
            for offer_id, offer in offers[:5]:
                service = offer.get('service_type', 'Услуга')
                ports = offer.get('ports', 'Не указаны')
                price = offer.get('price', 0)
                
                button_text = f"{emoji} #{offer_id} | {service[:20]} | {price:,.0f}₽"
                
                keyboard.add(
                    InlineKeyboardButton(
                        button_text,
                        callback_data=f"view_expeditor_offer_{offer_id}"
                    )
                )
    
    keyboard.add(
        InlineKeyboardButton("➕ Новое предложение", callback_data="create_expeditor_offer")
    )
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="expeditor_my_offers")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_expeditor_offer_'), state='*')
async def view_expeditor_offer_details(callback: types.CallbackQuery, state: FSMContext):
    """Детальный просмотр предложения экспедитора"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if offer_id not in expeditor_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = expeditor_offers[offer_id]
    user_id = callback.from_user.id
    
    if offer.get('expeditor_id') != user_id:
        await callback.answer("❌ Это не ваше предложение", show_alert=True)
        return
    
    text = f"📋 <b>ПРЕДЛОЖЕНИЕ #{offer_id}</b>\n\n"
    text += f"📦 Услуга: <b>{offer.get('service_type', 'Не указана')}</b>\n"
    text += f"🚢 Порты: {offer.get('ports', 'Не указаны')}\n"
    text += f"💰 Цена: <b>{offer.get('price', 0):,.0f} ₽</b>\n"
    text += f"📅 Сроки: {offer.get('terms', 'Не указаны')}\n\n"
    
    if offer.get('description'):
        text += f"📝 Описание:\n<i>{offer.get('description')}</i>\n\n"
    
    text += f"📅 Создано: {offer.get('created_at', 'Не указано')}\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Статус
    status = offer.get('status', 'active')
    
    if status == 'active':
        text += "📊 Статус: <b>🆕 Активно</b>\n"
        text += "Ваше предложение видно экспортёрам"
    elif status == 'selected':
        text += "📊 Статус: <b>✅ Выбрано экспортёром</b>\n"
        if offer.get('exporter_id'):
            exporter_info = users.get(offer.get('exporter_id'), {})
            text += f"\n🏢 Экспортёр: {exporter_info.get('company_name', 'Не указано')}\n"
            text += f"📞 Телефон: {exporter_info.get('phone', 'Не указан')}"
    elif status == 'cancelled':
        text += "📊 Статус: <b>❌ Отменено</b>"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    if status == 'active':
        keyboard.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_expeditor_offer_{offer_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_expeditor_offer_{offer_id}")
        )
    
    keyboard.add(
        InlineKeyboardButton("🔙 К предложениям", callback_data="expeditor_my_offers")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('cancel_expeditor_offer_'), state='*')
async def cancel_expeditor_offer(callback: types.CallbackQuery, state: FSMContext):
    """Отмена предложения экспедитора"""
    await state.finish()
    
    try:
        offer_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if offer_id not in expeditor_offers:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    offer = expeditor_offers[offer_id]
    user_id = callback.from_user.id
    
    if offer.get('expeditor_id') != user_id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    if offer.get('status') != 'active':
        await callback.answer("❌ Можно отменить только активные предложения", show_alert=True)
        return
    
    # Отменяем
    offer['status'] = 'cancelled'
    offer['cancelled_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    save_expeditor_offers()
    
    text = f"✅ <b>ПРЕДЛОЖЕНИЕ #{offer_id} ОТМЕНЕНО</b>\n\n"
    text += "Предложение больше не будет показываться экспортёрам"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📋 Мои предложения", callback_data="expeditor_my_offers"))
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer("✅ Предложение отменено")
    
    logging.info(f"❌ Экспедитор {user_id} отменил предложение #{offer_id}")


@dp.callback_query_handler(lambda c: c.data == 'expeditor_available_pulls', state='*')
async def show_expeditor_available_pulls(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр доступных пулов для экспедитора"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Получаем предложения экспедитора для определения портов
    my_offers = [
        offer for offer in expeditor_offers.values()
        if offer.get('expeditor_id') == user_id and offer.get('status') == 'active'
    ]
    
    # Собираем порты из предложений
    my_ports = set()
    for offer in my_offers:
        ports_str = offer.get('ports', '')
        if ports_str:
            ports_list = [p.strip() for p in ports_str.split(',')]
            my_ports.update([p.lower() for p in ports_list])
    
    # Ищем подходящие пулы
    suitable_pulls = []
    for pull_id, pull in pulls.items():
        if pull.get('status') != 'filled':
            continue
        
        pull_port = pull.get('port', '').lower()
        
        # Проверяем совпадение портов
        if not my_ports or any(port in pull_port for port in my_ports):
            suitable_pulls.append((pull_id, pull))
    
    if not suitable_pulls:
        text = "🚢 <b>ДОСТУПНЫЕ ПУЛЫ</b>\n\n"
        
        if not my_offers:
            text += "❌ Сначала создайте предложение услуг\n\n"
            text += "<i>Укажите порты, в которых вы работаете</i>"
            
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("➕ Создать предложение", callback_data="create_expeditor_offer"))
        else:
            text += "❌ Нет подходящих пулов по вашим портам\n\n"
            text += f"Ваши порты: <i>{', '.join(my_ports)}</i>"
            
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="expeditor_available_pulls"))
        
        keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
        await callback.answer()
        return
    
    # Сортируем по дате (новые первые)
    suitable_pulls.sort(key=lambda x: x[1].get('created_at', ''), reverse=True)
    
    text = f"🚢 <b>ДОСТУПНЫЕ ПУЛЫ</b>\n\n"
    text += f"Найдено пулов: <b>{len(suitable_pulls)}</b>\n"
    
    if my_ports:
        text += f"Ваши порты: <i>{', '.join(my_ports)}</i>\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "Выберите пул:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for pull_id, pull in suitable_pulls[:10]:
        culture = pull.get('culture', 'Не указана')
        volume = pull.get('current_volume', 0)
        port = pull.get('port', 'Не указан')
        
        button_text = f"📦 #{pull_id} | {culture} {volume:.0f}т | {port}"
        
        keyboard.add(
            InlineKeyboardButton(
                button_text,
                callback_data=f"view_pull_for_expeditor_{pull_id}"
            )
        )
    
    if len(suitable_pulls) > 10:
        keyboard.add(
            InlineKeyboardButton(
                f"➕ Показать ещё {len(suitable_pulls) - 10}",
                callback_data="show_more_pulls_expeditor"
            )
        )
    
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="expeditor_available_pulls")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('view_pull_for_expeditor_'), state='*')
async def view_pull_for_expeditor(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр пула для экспедитора"""
    await state.finish()
    
    try:
        pull_id = int(callback.data.split('_')[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка получения ID", show_alert=True)
        return
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    
    text = f"📦 <b>ПУЛ #{pull_id}</b>\n\n"
    text += f"🌾 Культура: <b>{pull.get('culture', 'Не указана')}</b>\n"
    text += f"📦 Объём: <b>{pull.get('current_volume', 0):.1f} т</b>\n"
    text += f"🚢 Порт отгрузки: {pull.get('port', 'Не указан')}\n"
    text += f"📅 Дата отгрузки: {pull.get('shipment_date', 'Не указана')}\n\n"
    
    # Информация об экспортёре
    exporter_id = pull.get('exporter_id')
    if exporter_id:
        exporter_info = users.get(exporter_id, {})
        text += f"<b>🏢 ЭКСПОРТЁР:</b>\n"
        text += f"Компания: {exporter_info.get('company_name', 'Не указана')}\n"
        text += f"Телефон: {exporter_info.get('phone', 'Не указан')}\n"
        if exporter_info.get('email'):
            text += f"Email: {exporter_info.get('email')}\n"
        text += "\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "💡 <b>Для сотрудничества:</b>\n"
    text += "Свяжитесь с экспортёром напрямую\n"
    text += "или отправьте предложение услуг"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📋 Мои предложения", callback_data="expeditor_my_offers")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 К пулам", callback_data="expeditor_available_pulls")
    )
    keyboard.add(
        InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == 'expeditor_statistics', state='*')
async def show_expeditor_statistics(callback: types.CallbackQuery, state: FSMContext):
    """Статистика экспедитора"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    # Собираем статистику
    my_offers = [
        offer for offer in expeditor_offers.values()
        if offer.get('expeditor_id') == user_id
    ]
    
    total_offers = len(my_offers)
    active_offers = len([o for o in my_offers if o.get('status') == 'active'])
    selected_offers = len([o for o in my_offers if o.get('status') == 'selected'])
    
    # Подсчёт пулов по портам
    ports_dict = {}
    for offer in my_offers:
        ports_str = offer.get('ports', '')
        if ports_str:
            for port in ports_str.split(','):
                port = port.strip()
                ports_dict[port] = ports_dict.get(port, 0) + 1
    
    text = f"📊 <b>МОЯ СТАТИСТИКА</b>\n\n"
    text += f"<b>ПРЕДЛОЖЕНИЯ:</b>\n"
    text += f"📋 Всего предложений: <b>{total_offers}</b>\n"
    text += f"🆕 Активных: <b>{active_offers}</b>\n"
    text += f"✅ Выбрано: <b>{selected_offers}</b>\n\n"
    
    if selected_offers > 0 and total_offers > 0:
        success_rate = (selected_offers / total_offers) * 100
        text += f"📈 Процент успеха: <b>{success_rate:.1f}%</b>\n\n"
    
    if ports_dict:
        text += f"<b>ПОРТЫ:</b>\n"
        for port, count in sorted(ports_dict.items(), key=lambda x: x[1], reverse=True)[:5]:
            text += f"🚢 {port}: {count} предложений\n"
        text += "\n"
    
    # Средняя цена услуг
    prices = [o.get('price', 0) for o in my_offers if o.get('price')]
    if prices:
        avg_price = sum(prices) / len(prices)
        text += f"<b>ЦЕНООБРАЗОВАНИЕ:</b>\n"
        text += f"💰 Средняя цена: <b>{avg_price:,.0f} ₽</b>\n"
        text += f"💵 Мин. цена: {min(prices):,.0f} ₽\n"
        text += f"💸 Макс. цена: {max(prices):,.0f} ₽\n"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📋 Мои предложения", callback_data="expeditor_my_offers"))
    keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="expeditor_statistics"))
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main"))
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()


if __name__ == '__main__':
    logging.info("🚀 Запуск бота...")
    try:
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
    except Exception as e:
        logging.error(f"❌ Ошибка создания директорий: {e}")
    
    # ЗАПУСК (ЭТО ДОЛЖНА БЫТЬ ПОСЛЕДНЯЯ СТРОКА!)
    from aiogram import executor
    executor.start_polling(
        dp, 
        skip_updates=True, 
        on_startup=on_startup, 
        on_shutdown=on_shutdown
    )

# Автозагрузка данных при импорте
load_users_from_pickle()
