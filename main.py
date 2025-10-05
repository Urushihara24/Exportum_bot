import warnings
warnings.filterwarnings("ignore", message=".*LibreSSL.*")
warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")
warnings.filterwarnings("ignore", category=UserWarning, module='urllib3')

import os
import logging
import requests
import time
import asyncio
import re
import json
import pickle
import pandas as pd
from datetime import timedelta, datetime
from bs4 import BeautifulSoup
from collections import defaultdict
from pathlib import Path

from aiogram import Bot, Dispatcher, types
from aiogram.utils.exceptions import MessageNotModified
from aiogram.types import (
    ReplyKeyboardMarkup, InlineKeyboardMarkup, 
    InlineKeyboardButton, KeyboardButton, CallbackQuery
)
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor
from apscheduler.schedulers.asyncio import AsyncIOScheduler

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    logging.warning("⚠️ Google Sheets библиотеки не установлены")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


API_TOKEN = os.getenv('BOT_TOKEN', '8180339161:AAFRk8446gKMagAkUTGjGyFDTFHa__mVOY0')
ADMIN_ID = int(os.getenv('ADMIN_ID', '1481790360'))

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

DATA_DIR = 'data'
LOGS_DIR = 'logs'

USERS_FILE = 'data/users.json'
BATCHES_FILE = 'data/batches.pkl'
PULLS_FILE = 'data/pulls.pkl'
PRICES_FILE = 'data/prices.json'
NEWS_FILE = 'data/news.json'

GOOGLE_SHEETS_CREDENTIALS = 'credentials.json'
SPREADSHEET_ID = "1DywxtuWW4-1Q0O71ajVaBB5Ih15nZjA4rvlpV7P7NOA"

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

users = {}
farmer_batches = {}
pulls = {}
pull_participants = {}
logistics_offers = {}
expeditor_offers = {}
deals = {}
matches = {}   

batch_counter = 0
pull_counter = 0
deal_counter = 0
match_counter = 0

prices_cache = {'data': {}, 'updated': None}
news_cache = {'data': [], 'updated': None}
last_prices_update = None
last_news_update = None

scheduler = AsyncIOScheduler()

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

class RegistrationStatesGroup(StatesGroup):
    """Регистрация пользователя с расширенными данными"""
    name = State()
    phone = State()
    email = State()
    inn = State()
    company_details = State()
    region = State()
    role = State()

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

class CreatePull(StatesGroup):
    """Создание пула экспортёром"""
    culture = State()
    volume = State()
    price = State()
    port = State()
    moisture = State()
    nature = State()
    impurity = State()
    weed = State()
    documents = State()
    doc_type = State()

class JoinPullStatesGroup(StatesGroup):
    """Присоединение фермера к пулу"""
    select_pull = State()
    select_batch = State()
    enter_volume = State()
    confirm_join = State()

class LogisticsOffer(StatesGroup):
    """Предложение логиста"""
    transport_type = State()
    route_from = State()
    route_to = State()
    price_per_ton = State()
    delivery_days = State()
    additional_info = State()

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

class SearchBatchesStatesGroup(StatesGroup):
    """Расширенный поиск партий"""
    select_criteria = State()
    enter_culture = State()
    enter_region = State()
    enter_min_volume = State()
    enter_max_volume = State()
    enter_min_price = State()
    enter_max_price = State()
    enter_quality_class = State()
    enter_storage_type = State()
    show_results = State()

class AttachFilesStatesGroup(StatesGroup):
    """Прикрепление файлов к партии"""
    select_batch = State()
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

def validate_phone(phone):
    """Валидация номера телефона"""
    cleaned = re.sub(r'[\s\-\(\)\+]', '', phone)
    return len(cleaned) >= 10 and cleaned.isdigit()

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

def get_farmer_menu():
    """Клавиатура для фермера"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add("➕ Добавить партию")
    keyboard.row("🔧 Мои партии", "🎯 Пулы")
    keyboard.row("🔍 Поиск экспортёров", "📋 Мои сделки")
    keyboard.row("👤 Профиль")
    keyboard.add("📈 Цены на зерно", "📰 Новости рынка")
    return keyboard

def get_exporter_menu():
    """Клавиатура для экспортёра"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("➕ Создать пул"),
        KeyboardButton("📦 Мои пулы")
    )
    keyboard.add(
        KeyboardButton("🔍 Найти партии"),
        KeyboardButton("📋 Мои сделки")
    )
    keyboard.add(
        KeyboardButton("🚚 Заявка на логистику"),
        KeyboardButton("👤 Профиль")
    )
    keyboard.add(
        KeyboardButton("📈 Цены на зерно"),
        KeyboardButton("📰 Новости рынка")
    )
    return keyboard

def get_logistic_menu():
    """Клавиатура для логиста"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🚚 Моя карточка"),
        KeyboardButton("📋 Активные заявки")
    )
    keyboard.add(
        KeyboardButton("💼 Мои предложения"),
        KeyboardButton("📋 Мои перевозки")
    )
    keyboard.add(
        KeyboardButton("👤 Профиль"),
        KeyboardButton("📈 Цены на зерно")
    )
    keyboard.add(KeyboardButton("📰 Новости рынка"))
    return keyboard

def get_expeditor_menu():
    """Клавиатура для экспедитора (брокера)"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🚛 Моя карточка"),
        KeyboardButton("📋 Активные заявки")
    )
    keyboard.add(
        KeyboardButton("💼 Мои предложения"),
        KeyboardButton("📋 Мои оформления")
    )
    keyboard.add(
        KeyboardButton("👤 Профиль"),
        KeyboardButton("📈 Цены на зерно")
    )
    keyboard.add(KeyboardButton("📰 Новости рынка"))
    return keyboard

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

def joinpull_keyboard(pull_id):
    """Клавиатура для присоединения к пулу"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Присоединиться", callback_data=f"confirm_joinpull:{pull_id}"),
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
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"editpull:{pull_id}")
        )
        keyboard.add(
            InlineKeyboardButton("✅ Закрыть пул", callback_data=f"closepull:{pull_id}"),
            InlineKeyboardButton("❌ Удалить", callback_data=f"deletepull:{pull_id}")
        )

    elif user_id in users and users[user_id].get('role') == 'farmer':
        keyboard.add(
            InlineKeyboardButton("✅ Присоединиться", callback_data=f"joinpull:{pull_id}")
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
    """Клавиатура выбора культуры"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    for culture in CULTURES:
        keyboard.add(
            InlineKeyboardButton(culture, callback_data=f"culture:{culture}")
        )
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
    """Клавиатура выбора порта"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    ports = [
        'Астрахань', 'Новороссийск', 'Азов', 'Ростов-на-Дону', 
        'Тамань', 'Кавказ', 'Туапсе', 'Порт Оля'
    ]
    for i, port in enumerate(ports):
        keyboard.add(
            InlineKeyboardButton(port, callback_data=f"port:{i}")
        )
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

def batch_actions_keyboard(batch_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Редактировать", callback_data=f'edit_batch:{batch_id}'),
        InlineKeyboardButton("🗑 Удалить", callback_data=f'delete_batch:{batch_id}')
    )
    keyboard.add(
        InlineKeyboardButton("📎 Прикрепить файлы", callback_data=f'attach_files:{batch_id}')
    )
    keyboard.add(
        InlineKeyboardButton("👁 Просмотреть файлы", callback_data=f'view_files:{batch_id}')
    )
    keyboard.add(
        InlineKeyboardButton("🔍 Найти экспортёров", callback_data=f'find_exporters:{batch_id}')
    )
    keyboard.add(
        InlineKeyboardButton("🔙 К моим партиям", callback_data='back_to_my_batches')
    )
    return keyboard

def edit_batch_fields_keyboard():
    """Клавиатура выбора поля для редактирования партии"""
    keyboard = InlineKeyboardMarkup(row_width=2)
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
        InlineKeyboardButton("📍 По региону", callback_data="search_by:region"),
        InlineKeyboardButton("📦 По объёму", callback_data="search_by:volume"),
        InlineKeyboardButton("💰 По цене", callback_data="search_by:price")
    )
    keyboard.add(
        InlineKeyboardButton("⭐ По классу качества", callback_data="search_by:quality"),
        InlineKeyboardButton("🏭 По типу хранения", callback_data="search_by:storage")
    )
    keyboard.add(
        InlineKeyboardButton("🔍 Все параметры", callback_data="search_by:all"),
        InlineKeyboardButton("🎯 Только активные", callback_data="search_by:active")
    )
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
    )
    return keyboard

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
    
    message = f"📊 <b>Актуальные цены на зерно</b>\n\n"
    
    if russia:
        message += "🇷🇺 <b>Юг России (руб/т)</b>\n"
        for culture, price in russia.items():
            if isinstance(price, (int, float)):
                message += f"  • {culture}: <code>{price:,.0f} ₽/т</code>\n"
            else:
                message += f"  • {culture}: <code>{price}</code>\n"
    
    message += f"\n🚢 <b>FOB Черное море</b>\n"
    if isinstance(fob, (int, float)):
        message += f"  • Пшеница: <code>${fob:.2f}/т</code>\n"
    else:
        message += f"  • Пшеница: <code>{fob}</code>\n"
    
    if cbot:
        message += f"\n🌎 <b>CBOT</b>\n"
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
        msg += f"<b>🏢 Реквизиты:</b>\n"
        msg += f"ИНН: <code>{farmer.get('inn')}</code>\n"
        if farmer.get('company_details'):
            details = farmer['company_details'][:200]
            msg += f"{details}...\n" if len(farmer['company_details']) > 200 else f"{details}\n"
        msg += "\n"
    
    if batch_id and farmer_id in farmer_batches:
        for batch in farmer_batches[farmer_id]:
            if batch['id'] == batch_id:
                msg += f"<b>📦 Партия #{batch_id}:</b>\n"
                msg += f"🌾 Культура: {batch['culture']}\n"
                msg += f"📦 Объём: {batch['volume']} т\n"
                msg += f"💰 Цена: {batch['price']:,.0f} ₽/т\n"
                msg += f"💧 Влажность: {batch['humidity']}%\n"
                msg += f"🌾 Сорность: {batch['impurity']}%\n"
                msg += f"⭐ Класс: {batch['quality_class']}\n"
                msg += f"🏭 Хранение: {batch['storage_type']}\n"
                msg += f"📅 Готовность: {batch['readiness_date']}\n"
                msg += f"📊 Статус: {batch['status']}\n"
                break
    
    if farmer_id in farmer_batches:
        total_batches = len(farmer_batches[farmer_id])
        active_batches = len([b for b in farmer_batches[farmer_id] if b['status'] == 'Активна'])
        
        msg += f"\n<b>📊 Статистика:</b>\n"
        msg += f"Всего партий: {total_batches}\n"
        msg += f"Активных: {active_batches}\n"
    
    return msg

def get_role_keyboard(role):
    """Получить Reply-клавиатуру по роли пользователя"""
    role = str(role).lower()
    
    if role in ['farmer', 'фермер']:
        return get_farmer_menu()
    elif role in ['exporter', 'экспортёр']:
        return get_exporter_menu()
    elif role in ['logistic', 'логист']:
        return get_logistic_menu()
    elif role in ['expeditor', 'экспедитор', 'broker', 'брокер']:
        return get_expeditor_menu()
    else:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add("👤 Профиль", "📈 Цены на зерно", "📰 Новости рынка")
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

async def find_matching_exporters(batch_data):
    """Поиск подходящих экспортёров для партии"""
    matching_pulls = []
    
    for pull_id, pull in pulls.items():
        if (pull['culture'] == batch_data['culture'] and
            pull['status'] == 'Открыт' and
            pull['current_volume'] < pull['target_volume'] and
            batch_data['price'] <= pull['price'] * 75 and   
            batch_data['humidity'] <= pull['moisture'] and
            batch_data['impurity'] <= pull['impurity']):
            
            matching_pulls.append(pull)
    
    return matching_pulls

async def find_matching_batches(pull_data):
    """Поиск подходящих партий для пула"""
    matching_batches = []
    
    for user_id, batches in farmer_batches.items():
        for batch in batches:
            if (batch['culture'] == pull_data['culture'] and
                batch['status'] == 'Активна' and
                batch['price'] <= pull_data['price'] * 75 and  # Примерный курс
                batch['humidity'] <= pull_data['moisture'] and
                batch['impurity'] <= pull_data['impurity']):
                
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

async def notify_match(farmer_id, exporter_id, batch_id, pull_id):
    """Уведомление о найденном совпадении"""
    match_id = await create_match_notification(batch_id, pull_id)
    
    farmer_msg = (
        f"🎯 <b>Найден экспортёр для вашей партии #{batch_id}</b>\n\n"
        f"Пулл #{pull_id} соответствует вашим параметрам.\n"
        f"Рекомендуем связаться с экспортёром для обсуждения деталей."
    )
    
    exporter_msg = (
        f"🎯 <b>Найдена подходящая партия #{batch_id}</b>\n\n"
        f"Партия соответствует параметрам вашего пулла #{pull_id}.\n"
        f"Рекомендуем связаться с фермером для обсуждения деталей."
    )
    
    try:
        await bot.send_message(farmer_id, farmer_msg, parse_mode='HTML')
        await bot.send_message(exporter_id, exporter_msg, parse_mode='HTML')
        logging.info(f"✅ Уведомления о совпадении #{match_id} отправлены")
    except Exception as e:
        logging.error(f"❌ Ошибка отправки уведомлений: {e}")

async def auto_match_batches_and_pulls():
    """Автоматический поиск совпадений между партиями и пулами"""
    logging.info("🔄 Запуск автоматического поиска совпадений...")
    
    matches_found = 0
    
    for pull_id, pull in pulls.items():
        if pull['status'] != 'Открыт':
            continue
            
        matching_batches = await find_matching_batches(pull)
        
        for batch in matching_batches:
            existing_match = None
            for match in matches.values():
                if (match['batch_id'] == batch['id'] and 
                    match['pull_id'] == pull_id and 
                    match['status'] == 'active'):
                    existing_match = match
                    break
            
            if not existing_match:
                await notify_match(
                    batch['farmer_id'], 
                    pull['exporter_id'], 
                    batch['id'], 
                    pull_id
                )
                matches_found += 1
                await asyncio.sleep(0.1)  
    
    logging.info(f"✅ Автопоиск завершен. Найдено совпадений: {matches_found}")
    return matches_found

@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    """Глобальный обработчик ошибок"""
    logging.error(f"Ошибка: {exception}", exc_info=True)
    
    try:
        if update.message:
            await update.message.answer(
                "⚠️ Произошла ошибка при обработке вашего запроса.\n"
                "Попробуйте позже или обратитесь к администратору."
            )
        elif update.callback_query:
            await update.callback_query.answer(
                "⚠️ Произошла ошибка при обработке",
                show_alert=True
            )
    except Exception as e:
        logging.error(f"Ошибка в обработчике ошибок: {e}")
    
    return True

@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: types.Message, state: FSMContext):
    """Команда /start - главное меню или регистрация"""
    await state.finish()
    user_id = message.from_user.id
    
    if user_id in users:
        user = users[user_id]
        role = user.get('role', '').lower()
        name = user.get('name', message.from_user.first_name)
        
        keyboard = get_role_keyboard(role)
        
        await message.answer(
            f"👋 С возвращением, {name}!\n\n"
            f"🎭 Ваша роль: {ROLES.get(role, role)}\n\n"
            f"Выберите действие из меню:",
            reply_markup=keyboard
        )
    else:
        await message.answer(
            "👋 Добро пожаловать в Exportum!\n\n"
            "🌾 Платформа для торговли зерном\n\n"
            "Для начала работы пройдите регистрацию:",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("📋 Начать регистрацию", callback_data="start_registration")
            )
        )
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
    
    await message.answer(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 6 из 7\n\n"
        "Выберите ваш регион:",
        reply_markup=region_keyboard(),
        parse_mode='HTML'
    )
    await RegistrationStatesGroup.region.set()

@dp.callback_query_handler(lambda c: c.data.startswith('joinpull:'), state='*')
async def join_pull_start(callback: types.CallbackQuery, state: FSMContext):

    await state.finish()
    
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        # # print(f"❌ ОШИБКА парсинга pull_id: {e}")
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        return
    
    if pull_id not in pulls:
        # # print(f"❌ pull_id {pull_id} НЕ НАЙДЕН в pulls!")
        print(f"   Доступные pulls: {list(pulls.keys())}")
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    
    user_id = callback.from_user.id
    
    if user_id not in users:
        # # print(f"❌ user_id {user_id} НЕ НАЙДЕН в users!")
        await callback.answer("❌ Пользователь не зарегистрирован", show_alert=True)
        return
    
    if users[user_id].get('role') != 'farmer':
        # # print(f"❌ Роль пользователя: {users[user_id].get('role')}, а нужна 'farmer'!")
        await callback.answer("❌ Только фермеры могут присоединяться к пулам", show_alert=True)
        return
    
    if user_id not in farmer_batches:
        # # print(f"❌ У фермера НЕТ партий!")
        await callback.answer(f"❌ У вас нет партий культуры {pull['culture']}", show_alert=True)
        return
    
    active_batches = [
        b for b in farmer_batches[user_id]
        if b.get('culture') == pull['culture'] and b.get('status') == 'Активна'
    ]
        
    if not active_batches:
        # # print(f"❌ НЕТ активных партий культуры {pull['culture']}!")
        await callback.answer(f"❌ У вас нет активных партий культуры {pull['culture']}", show_alert=True)
        return
        
    await state.update_data(join_pull_id=pull_id)
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    for batch in active_batches:
        button_text = f"{batch['culture']} - {batch['volume']} т - {batch['price']:,.0f} ₽/т"
        keyboard.add(
            InlineKeyboardButton(button_text, 
                               callback_data=f"selectbatchjoin:{batch['id']}")
        )
        print(f"   Добавлена партия: {button_text}")
    
    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data=f"viewpull:{pull_id}"))
    
    
    await callback.message.edit_text(
        f"🎯 <b>Выберите партию для присоединения к пулу #{pull_id}</b>\n\n"
        f"🌾 Культура: {pull['culture']}\n"
        f"📦 Целевой объём: {pull['target_volume']} т\n\n"
        f"Выберите партию из списка ниже:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('selectbatchjoin:'), state='*')
async def select_batch_for_join(callback: types.CallbackQuery, state: FSMContext):
    """Выбор партии для присоединения к пулу"""
    try:
        batch_id = int(callback.data.split(':', 1)[1])
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

    batch = None
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
                break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        await state.finish()
        return
        
    if batch['culture'] != pull['culture']:
        await callback.answer("❌ Культура партии не совпадает с пулом!", show_alert=True)
        await state.finish()
        return
    
    available = pull['target_volume'] - pull['current_volume']
    print(f"   Объем партии: {batch['volume']}")
    print(f"   Доступно в пуле: {available}")
    
    if batch['volume'] > available:
        await callback.answer("❌ Объем партии больше доступного в пуле!", show_alert=True)
        await state.finish()
        return
    

    if pull_id not in pull_participants:
        pull_participants[pull_id] = []
    
    participant = {
        'farmer_id': user_id,
        'farmer_name': users[user_id].get('name', ''),
        'batch_id': batch_id,
        'volume': batch['volume'],
        'joined_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    pull_participants[pull_id].append(participant)
    pull['current_volume'] += batch['volume']
    batch['status'] = 'Зарезервирована'
    save_pulls_to_pickle()
    save_batches_to_pickle()
    
    await callback.answer("✅ Успешно присоединились к пулу!", show_alert=True)
    
    await callback.message.answer(
        f"✅ <b>Партия #{batch_id} присоединена к пулу #{pull_id}!</b>\n\n"
        f"🌾 Культура: {batch['culture']}\n"
        f"📦 Объем: {batch['volume']} т\n"
        f"💰 Цена: {batch['price']:,.0f} ₽/т\n\n"
        f"Экспортёр свяжется с вами для обсуждения деталей.",
        parse_mode='HTML'
    )
    
    try:
        farmer_card = format_farmer_card(user_id, batch_id)
        
        await bot.send_message(
            pull['exporter_id'],
            f"🎉 <b>Новый участник в пуле #{pull_id}!</b>\n\n"
            f"{farmer_card}\n\n"
            f"Свяжитесь с фермером для обсуждения деталей.",
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка отправки уведомления экспортёру: {e}")
    
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('viewparticipants:'), state='*')
async def view_pull_participants(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр участников пула с полными контактами"""
    await state.finish()
    
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    participants = pull_participants.get(pull_id, [])
    
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
        if farmer_id in farmer_batches:
            for b in farmer_batches[farmer_id]:
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
    """Получение региона при регистрации"""
    region = callback.data.split(':', 1)[1]
    await state.update_data(region=region)
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🌾 Фермер", callback_data="role:farmer"),
        InlineKeyboardButton("📦 Экспортёр", callback_data="role:exporter")
    )
    keyboard.add(
        InlineKeyboardButton("🚚 Логист", callback_data="role:logistic"),
        InlineKeyboardButton("🚛 Экспедитор", callback_data="role:expeditor")
    )
    
    await callback.message.edit_text(
        "📝 <b>Регистрация</b>\n\n"
        "Шаг 7 из 7\n\n"
        "Выберите вашу роль:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await RegistrationStatesGroup.role.set()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('role:'), state=RegistrationStatesGroup.role)
async def registration_role(callback: types.CallbackQuery, state: FSMContext):
    """Завершение расширенной регистрации"""
    role = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    user_id = callback.from_user.id
    username = callback.from_user.username or ''
    
    users[user_id] = {
        'user_id': user_id,
        'username': username,
        'name': data['name'],
        'phone': data['phone'],
        'email': data['email'],
        'inn': data['inn'],
        'company_details': data['company_details'],
        'region': data['region'],
        'role': role,
        'registered_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    save_users_to_json()
    
    if gs and gs.spreadsheet:
        gs.sync_user_to_sheets(user_id, users[user_id])
    
    await state.finish()
    
    keyboard = get_role_keyboard(role)
    
    await callback.message.edit_text(
        f"✅ <b>Регистрация завершена!</b>\n\n"
        f"👤 {data['name']}\n"
        f"🎭 Роль: {ROLES[role]}\n"
        f"📍 Регион: {data['region']}\n"
        f"🏢 ИНН: {data['inn']}\n\n"
        f"Добро пожаловать в Exportum!",
        parse_mode='HTML'
    )
    
    await callback.message.answer(
        "Выберите действие из меню:",
        reply_markup=keyboard
    )
    await callback.answer("✅ Регистрация завершена!")

def admin_keyboard():
    """Клавиатура для админа"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("📊 Статистика"),
        KeyboardButton("📢 Рассылка")
    )
    keyboard.add(
        KeyboardButton("📤 Экспорт"),
        KeyboardButton("🔍 Найти совпадения")
    )
    keyboard.add(KeyboardButton("◀️ Назад"))
    return keyboard

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

@dp.message_handler(lambda m: m.text == "📊 Статистика", state='*')
async def admin_stats(message: types.Message, state: FSMContext):
    """Статистика для админа"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    
    total_users = len(users)
    farmers = len([u for u in users.values() if u.get('role') == 'farmer'])
    exporters = len([u for u in users.values() if u.get('role') == 'exporter'])
    logistics = len([u for u in users.values() if u.get('role') == 'logistic'])
    expeditors = len([u for u in users.values() if u.get('role') == 'expeditor'])
    
    total_batches = sum(len(batches) for batches in farmer_batches.values())
    active_batches = sum(
        len([b for b in batches if b.get('status') == 'Активная'])
        for batches in farmer_batches.values()
    )
    
    total_pulls = len(pulls)
    open_pulls = len([p for p in pulls.values() if p.get('status') == 'Открыт'])
    
    total_deals = len(deals)
    active_deals = len([d for d in deals.values() if d.get('status') not in ['completed', 'cancelled']])
    
    total_matches = len(matches)
    active_matches = len([m for m in matches.values() if m.get('status') == 'active'])
    
    stats_msg = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"   • Всего: {total_users}\n"
        f"   • 🧑‍🌾 Фермеров: {farmers}\n"
        f"   • 🏭 Экспортёров: {exporters}\n"
        f"   • 🚚 Логистов: {logistics}\n"
        f"   • 📦 Экспедиторов: {expeditors}\n\n"
        f"📦 <b>Партии:</b>\n"
        f"   • Всего: {total_batches}\n"
        f"   • Активных: {active_batches}\n\n"
        f"🎯 <b>Пулы:</b>\n"
        f"   • Всего: {total_pulls}\n"
        f"   • Открытых: {open_pulls}\n\n"
        f"🤝 <b>Сделки:</b>\n"
        f"   • Всего: {total_deals}\n"
        f"   • Активных: {active_deals}\n\n"
        f"✨ <b>Совпадения:</b>\n"
        f"   • Всего: {total_matches}\n"
        f"   • Активных: {active_matches}"
    )
    
    await message.answer(stats_msg, parse_mode='HTML')

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
        'batches': {uid: batches for uid, batches in farmer_batches.items()},
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
                    f"📦 Партий: {sum(len(b) for b in farmer_batches.values())}\n"
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

def parse_russia_regional_prices():
    """✅ Парсинг региональных цен РФ (Юг России)"""
    prices = {}
    today = datetime.now().strftime("%Y-%m-%d")
    
    crops_urls = {
        'Пшеница': f'https://www.zerno.ru/cerealspricesdate/{today}/wheat',
        'Ячмень': f'https://www.zerno.ru/cerealspricesdate/{today}/barley',
        'Кукуруза': f'https://www.zerno.ru/cerealspricesdate/{today}/corn',
        'Соя': f'https://www.zerno.ru/cerealspricesdate/{today}/soybean',
        'Подсолнечник': f'https://www.zerno.ru/cerealspricesdate/{today}/sunflower'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9'
    }
    
    south_cities = ['Краснодар', 'Ростов', 'Астрахань', 'Волгоград', 'Ставрополь']
    
    for crop_name, url in crops_urls.items():
        found_prices = []
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logging.warning(f"⚠️ {crop_name}: HTTP {response.status_code}")
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows[1:]:
                    cols = row.find_all(['td', 'th'])
                    
                    if len(cols) < 2:
                        continue
                    
                    location = cols[0].get_text(strip=True)
                    
                    if any(city in location for city in south_cities):
                        for col in cols[1:]:
                            price_text = col.get_text(strip=True)
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            
                            if clean_price and len(clean_price) >= 4:
                                try:
                                    price = int(clean_price)
                                    
                                    if 5000 <= price <= 100000:
                                        found_prices.append(price)
                                        logging.info(f"✅ {crop_name}: {price} ₽/т из {location}")
                                        break
                                except ValueError:
                                    continue
            
            if found_prices:
                avg_price = sum(found_prices) // len(found_prices)
                prices[crop_name] = avg_price
                logging.info(f"✅ {crop_name}: средняя {avg_price} ₽/т")
        
        except Exception as e:
            logging.error(f"❌ {crop_name}: {e}")
    if not prices or len(prices) < 3:
        logging.warning("⚠️ Используем резервные цены")
        prices = {
            'Пшеница': 15650,
            'Ячмень': 13300,
            'Кукуруза': 14000,
            'Соя': 40900,
            'Подсолнечник': 38600
        }
    
    logging.info(f"✅ РФ (Юг): итого {len(prices)} цен")
    return prices

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
    """✅ КЛЮЧЕВАЯ ФУНКЦИЯ: Обновление кэша цен"""
    global prices_cache
    
    try:
        logging.info("🔄 Обновление кэша цен...")
        
        loop = asyncio.get_event_loop()
        
        russia_prices = await loop.run_in_executor(None, parse_russia_regional_prices)
        fob_price = await loop.run_in_executor(None, parse_fob_black_sea)
        cbot_prices = await loop.run_in_executor(None, parse_cbot_futures)
        
        prices_cache['data'] = {
            'russia_south': russia_prices,
            'fob': fob_price,
            'cbot': cbot_prices
        }
        prices_cache['updated'] = datetime.now()
        
        logging.info("✅ Кэш цен обновлён")
        
    except Exception as e:
        logging.error(f"❌ Ошибка обновления кэша цен: {e}")

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
        
        last_news_update = datetime.now()
        logging.info("✅ Новости обновлены")
        
    except Exception as e:
        logging.error(f"❌ update_news_cache: {e}")

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
    global farmer_batches, batch_counter
    try:
        if os.path.exists(BATCHES_FILE):
            with open(BATCHES_FILE, 'rb') as f:
                data = pickle.load(f)
                if isinstance(data, dict):
                    farmer_batches = data
                else:
                    farmer_batches = {}
            
            all_batches = []
            for batches_list in farmer_batches.values():
                if isinstance(batches_list, list):
                    all_batches.extend(batches_list)
            
            if all_batches:
                batch_counter = max([b['id'] for b in all_batches if isinstance(b, dict) and 'id' in b], default=0)
            
            logging.info(f"✅ Партии загружены: {len(all_batches)}")
        else:
            logging.info("ℹ️ Файл партий не найден")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки партий: {e}")
        farmer_batches = {}
        batch_counter = 0

def save_batches_to_pickle():
    """Сохранение партий в pickle"""
    try:
        with open(BATCHES_FILE, 'wb') as f:
            pickle.dump(farmer_batches, f)
        logging.info("✅ Партии сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения партий: {e}")

def load_pulls_from_pickle():
    """Загрузка пулов из pickle"""
    global pulls, pull_participants, logistics_offers, expeditor_offers, deals, pull_counter, deal_counter
    try:
        if os.path.exists(PULLS_FILE):
            with open(PULLS_FILE, 'rb') as f:
                data = pickle.load(f)
                pulls = data.get('pulls', {})
                pull_participants = data.get('participants', {})
                logistics_offers = data.get('logistics_offers', {})
                expeditor_offers = data.get('expeditor_offers', {})
                deals = data.get('deals', {})
                
                pull_counter = len(pulls)
                deal_counter = len(deals)
            
            logging.info(f"✅ Пулы загружены: {pull_counter}, Сделки: {deal_counter}")
        else:
            logging.info("ℹ️ Файл пулов не найден")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки пулов: {e}")

def save_pulls_to_pickle():
    """Сохранение пулов в pickle"""
    try:
        data = {
            'pulls': pulls,
            'pull_participants': pull_participants,
            'logistics_offers': logistics_offers,
            'expeditor_offers': expeditor_offers,
            'deals': deals
        }
        with open(PULLS_FILE, 'wb') as f:
            pickle.dump(data, f)
        logging.info("✅ Пулы сохранены")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения пулов: {e}")

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
        except:
            worksheet = self.spreadsheet.add_worksheet(
                title=title,
                rows=1000,
                cols=len(headers)
            )
            worksheet.append_row(headers)
            logging.info(f"✅ Создан worksheet: {title}")
        
        return worksheet
    
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
                    user_data.get('username', ''),
                    user_data.get('name', ''),
                    user_data.get('phone', ''),
                    user_data.get('email', ''),
                    user_data.get('inn', ''),
                    user_data.get('company_details', ''),
                    user_data.get('region', ''),
                    user_data.get('role', ''),
                    user_data.get('registered_at', '')
                ]
                
                worksheet.update(values=[row_data], range_name=f'A{row_num}:J{row_num}')
                logging.info(f"✅ Обновлён пользователь {user_id} в Google Sheets")
                
            except:
                row_data = [
                    str(user_id),
                    user_data.get('username', ''),
                    user_data.get('name', ''),
                    user_data.get('phone', ''),
                    user_data.get('email', ''),
                    user_data.get('inn', ''),
                    user_data.get('company_details', ''),
                    user_data.get('region', ''),
                    user_data.get('role', ''),
                    user_data.get('registered_at', '')
                ]
                worksheet.append_row(row_data)
                logging.info(f"✅ Добавлен пользователь {user_id} в Google Sheets")
                
        except Exception as e:
            logging.error(f"❌ Ошибка синхронизации пользователя {user_id}: {e}")
    
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
            
        except Exception as e:
            logging.error(f"❌ Ошибка создания worksheet: {e}")
            return
        
        if not worksheet:
            return
        
        try:
            cell = worksheet.find(str(batch_data['id']))
            row_num = cell.row
            
            row_data = [
                str(batch_data['id']),
                str(batch_data['farmer_id']),
                batch_data.get('farmer_name', ''),
                batch_data.get('culture', ''),
                batch_data.get('region', ''),
                str(batch_data.get('volume', '')),
                str(batch_data.get('price', '')),
                str(batch_data.get('humidity', '')),
                str(batch_data.get('impurity', '')),
                batch_data.get('quality_class', ''),
                batch_data.get('storage_type', ''),
                batch_data.get('readiness_date', ''),
                batch_data.get('status', ''),
                batch_data.get('created_at', '')
            ]
            
            worksheet.update(values=[row_data], range_name=f'A{row_num}:N{row_num}')
            logging.info(f"✅ Обновлена партия {batch_data['id']}")
            
        except:
            row_data = [
                str(batch_data['id']),
                str(batch_data['farmer_id']),
                batch_data.get('farmer_name', ''),
                batch_data.get('culture', ''),
                batch_data.get('region', ''),
                str(batch_data.get('volume', '')),
                str(batch_data.get('price', '')),
                str(batch_data.get('humidity', '')),
                str(batch_data.get('impurity', '')),
                batch_data.get('quality_class', ''),
                batch_data.get('storage_type', ''),
                batch_data.get('readiness_date', ''),
                batch_data.get('status', ''),
                batch_data.get('created_at', '')
            ]
            worksheet.append_row(row_data)
            logging.info(f"✅ Добавлена партия {batch_data['id']}")
        
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
                
            except:
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
                
            except:
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
 
            except:
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
            for user_batches in farmer_batches.values():
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

try:
    if GOOGLE_SHEETS_AVAILABLE and os.path.exists(GOOGLE_SHEETS_CREDENTIALS):
        gs = GoogleSheetsManager(GOOGLE_SHEETS_CREDENTIALS, SPREADSHEET_ID)
        if gs and gs.spreadsheet:
            logging.info("✅ Google Sheets Manager инициализирован")
        else:
            logging.warning("⚠️ Google Sheets Manager отключен - не удалось подключиться")
            gs = None
    else:
        gs = None
        logging.warning("⚠️ Google Sheets Manager отключен - файл credentials не найден")
except Exception as e:
    logging.error(f"❌ Ошибка инициализации Google Sheets: {e}")
    gs = None

@dp.message_handler(lambda m: m.text == "📋 Мои сделки", state='*')
async def view_my_deals(message: types.Message, state: FSMContext):
    """Просмотр сделок пользователя"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы")
        return
    
    user_deals = []
    for deal_id, deal in deals.items():
        if (deal.get('exporter_id') == user_id or 
            user_id in deal.get('farmer_ids', []) or
            deal.get('logistic_id') == user_id or
            deal.get('expeditor_id') == user_id):
            user_deals.append(deal)
    
    if not user_deals:
        await message.answer(
            "📋 У вас пока нет сделок.\n\n"
            "Сделки появляются после успешного матчинга с партнерами."
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for deal in user_deals[:10]:  # Ограничиваем показ
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
    
    await message.answer(
        f"📋 <b>Ваши сделки</b> ({len(user_deals)} шт.)\n\n"
        "Выберите сделку для просмотра деталей:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

@dp.callback_query_handler(lambda c: c.data.startswith('view_deal:'), state='*')
async def view_deal_details(callback: types.CallbackQuery):
    """Просмотр деталей сделки"""
    deal_id = int(callback.data.split(':', 1)[1])
    
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
    
    if user_id not in farmer_batches or not farmer_batches[user_id]:
        await message.answer(
            "📦 У вас пока нет партий для поиска экспортёров.\n\n"
            "Сначала добавьте партию через меню '➕ Добавить партию'"
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for batch in farmer_batches[user_id]:
        if batch.get('status') == 'Активна':
            button_text = f"🌾 {batch['culture']} - {batch['volume']} т"
            keyboard.add(
                InlineKeyboardButton(button_text, callback_data=f"find_exporters:{batch['id']}")
            )
    
    await message.answer(
        "🔍 <b>Поиск экспортёров</b>\n\n"
        "Выберите партию для поиска подходящих экспортёров:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

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
        'matches': []  # Для хранения совпадений с пулами
    }
    
    if user_id not in farmer_batches:
        farmer_batches[user_id] = []
    farmer_batches[user_id].append(batch)
    
    save_batches_to_pickle()
    
    if gs and gs.spreadsheet:
        gs.sync_batch_to_sheets(batch)
    
    await state.finish()
    
    matching_pulls = await find_matching_exporters(batch)
    
    keyboard = get_role_keyboard('farmer')
    
    message_text = (
        f"✅ <b>Партия #{batch['id']} добавлена!</b>\n\n"
        f"🌾 Культура: {batch['culture']}\n"
        f"📍 Регион: {batch['region']}\n"
        f"📦 Объём: {batch['volume']} т\n"
        f"💰 Цена: {batch['price']:,.0f} ₽/т\n"
        f"💧 Влажность: {batch['humidity']}%\n"
        f"🌾 Сорность: {batch['impurity']}%\n"
        f"⭐ Класс: {batch['quality_class']}\n"
        f"🏭 Хранение: {batch['storage_type']}\n"
        f"📅 Готовность: {batch['readiness_date']}"
    )
    
    if matching_pulls:
        message_text += f"\n\n🎯 Найдено подходящих пулов: {len(matching_pulls)}"
        for pull in matching_pulls:
            await notify_match(user_id, pull['exporter_id'], batch['id'], pull['id'])
    
    await message.answer(message_text, reply_markup=keyboard, parse_mode='HTML')
    
    if matching_pulls:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🔍 Посмотреть подходящие пулы", callback_data=f"view_matches:{batch['id']}")
        )
        await message.answer(
            "Мы нашли экспортёров, которым может подойти ваша партия!",
            reply_markup=keyboard
        )

@dp.callback_query_handler(lambda c: c.data.startswith('view_matches:'), state='*')
async def view_batch_matches(callback: types.CallbackQuery):
    """Просмотр совпадений для партии"""
    batch_id = int(callback.data.split(':', 1)[1])
    user_id = callback.from_user.id
    
    batch = None
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await message.answer("❌ Эта функция доступна только фермерам")
        return
    
    if user_id not in farmer_batches or not farmer_batches[user_id]:
        await message.answer(
            "📦 У вас пока нет добавленных партий.\n\n"
            "Используйте кнопку '➕ Добавить партию' для создания новой."
        )
        return
    
    batches = farmer_batches[user_id]
    
    active_batches = [b for b in batches if b.get('status') == 'Активна']
    other_batches = [b for b in batches if b.get('status') != 'Активна']
    
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
            InlineKeyboardButton(button_text, callback_data=f"view_batch:{batch['id']}")
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
            InlineKeyboardButton(button_text, callback_data=f"view_batch:{batch['id']}")
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
    
    for pull in open_pulls[:10]:  # Ограничиваем показ
        progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
        button_text = f"🌾 {pull['culture']} - {pull['target_volume']} т ({progress:.0f}% заполнено)"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_pull_for_join:{pull['id']}")
        )
    
    await message.answer(
        f"🎯 <b>Активные пулы</b> ({len(open_pulls)} шт.)\n\n"
        "Выберите пул для просмотра деталей и присоединения:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

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
async def handle_other_news_buttons(callback: types.CallbackQuery):
    """Обработка других кнопок новостей и аналитики"""
    button_texts = {
        "view_analytics": "📊 Аналитика рынка",
        "view_grain_news": "🌾 Новости зерна", 
        "view_export_news": "🚢 Экспортные новости"
    }
    
    text = button_texts.get(callback.data, "Раздел в разработке")
    await callback.answer(f"{text} - в разработке", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "auto_match_all", state='*')
async def auto_match_all_batches(callback: types.CallbackQuery):
    """Автопоиск экспортёров для всех активных партий"""
    user_id = callback.from_user.id
    
    if user_id not in farmer_batches:
        await callback.answer("❌ У вас нет партий", show_alert=True)
        return
    
    active_batches = [b for b in farmer_batches[user_id] if b.get('status') == 'Активна']
    
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
                await notify_match(user_id, pull['exporter_id'], batch['id'], pull['id'])
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

async def view_batch_details_direct(message, batch_id: int, user_id: int):
    """Вспомогательная функция для показа деталей расширенной партии"""
    batch = None
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
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
📍 Регион: {batch['region']}
📦 Объём: {batch['volume']} т
💰 Цена: {batch['price']:,.0f} ₽/т
💧 Влажность: {batch['humidity']}%
🌾 Сорность: {batch['impurity']}%
⭐ Класс: {batch['quality_class']}
🏭 Хранение: {batch['storage_type']}
📅 Готовность: {batch['readiness_date']}
📊 Статус: {batch.get('status', 'Активна')}
📅 Создано: {batch['created_at']}
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
    batch_id = int(callback.data.split(':', 1)[1])
    user_id = callback.from_user.id
    await view_batch_details_direct(callback.message, batch_id, user_id)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('find_exporters:'), state='*')
async def find_exporters_for_batch(callback: types.CallbackQuery, state: FSMContext):
    """Поиск экспортёров для конкретной партии"""
    await state.finish()
    
    try:
        batch_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка", show_alert=True)
        logging.error(f"Ошибка: {e}, data: {callback.data}")
        return
    
    user_id = callback.from_user.id
    batch = None
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
                break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    await callback.answer("🔍 Ищем экспортёров...")
    matching_pulls = await find_matching_exporters(batch)
    
    if matching_pulls:
        text = f"✅ <b>Найдено {len(matching_pulls)} подходящих экспортёров!</b>\n\n"
        
        for i, pull in enumerate(matching_pulls[:5], 1):
            available = pull['target_volume'] - pull['current_volume']
            text += f"{i}. <b>Пул #{pull['id']}</b>\n"
            text += f"   🌾 Культура: {pull['culture']}\n"
            text += f"   📦 Доступно: {available} т\n"
            text += f"   💰 Цена FOB: ${pull['price']}/т\n"
            text += f"   🚢 Порт: {pull['port']}\n"
            text += f"   👤 Экспортёр: {pull['exporter_name']}\n\n"
        
        if len(matching_pulls) > 5:
            text += f"...и ещё {len(matching_pulls) - 5} пулов\n\n"
        
        text += "Используйте меню '🌾 Пулы' для присоединения."
        
        await callback.message.answer(text, parse_mode='HTML')
    else:
        await callback.message.answer(
            "🤷‍♂️ <b>Подходящих экспортёров не найдено.</b>\n\n"
            "Рекомендуем:\n"
            "• Проверить актуальность цен\n"
            "• Уточнить параметры качества\n"
            "• Подождать новых предложений",
            parse_mode='HTML'
        )
    
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'back_to_my_batches', state='*')
async def back_to_my_batches(callback: types.CallbackQuery, state: FSMContext):
    """Возврат к списку партий"""
    await state.finish()
    
    user_id = callback.from_user.id
    
    if user_id not in users or users[user_id].get('role') != 'farmer':
        await callback.answer("❌ Эта функция доступна только фермерам", show_alert=True)
        return
    
    if user_id not in farmer_batches or not farmer_batches[user_id]:
        await callback.message.edit_text(
            "📦 У вас пока нет добавленных партий.\n\n"
            "Используйте кнопку '➕ Добавить партию' для создания новой."
        )
        await callback.answer()
        return
    
    batches = farmer_batches[user_id]
    active_batches = [b for b in batches if b.get('status') == 'Активна']
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    for batch in active_batches:
        button_text = f"✅ {batch['culture']} - {batch['volume']} т ({batch['price']:,.0f} ₽/т)"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_batch:{batch['id']}")
        )
    
    if active_batches:
        keyboard.add(
            InlineKeyboardButton("🔍 Автопоиск экспортёров", callback_data="auto_match_all")
        )
    
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
    """Начало создания расширенного пула"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users or users[user_id].get('role') != 'exporter':
        await message.answer("❌ Эта функция доступна только экспортёрам")
        return
    
    await message.answer(
        "📦 <b>Создание пула</b>\n\n"
        "Шаг 1 из 10\n\n"
        "Выберите культуру:",
        reply_markup=culture_keyboard(),
        parse_mode='HTML'
    )
    await CreatePull.culture.set()

@dp.callback_query_handler(lambda c: c.data.startswith('culture:'), state=CreatePull.culture)
async def create_pull_culture(callback: types.CallbackQuery, state: FSMContext):
    """Выбор культуры для пула"""
    culture = callback.data.split(':', 1)[1]
    await state.update_data(culture=culture)
    
    await callback.message.edit_text(
        "📦 <b>Создание пула</b>\n\n"
        "Шаг 2 из 10\n\n"
        "Введите целевой объём пула (в тоннах):",
        parse_mode='HTML'
    )
    await CreatePull.volume.set()
    await callback.answer()

@dp.message_handler(state=CreatePull.volume)
async def create_pull_volume(message: types.Message, state: FSMContext):
    """Ввод объёма пула"""
    try:
        volume = float(message.text.strip().replace(',', '.'))
        if volume <= 0:
            raise ValueError
        
        await state.update_data(target_volume=volume)
        
        await message.answer(
            "📦 <b>Создание пула</b>\n\n"
            "Шаг 3 из 10\n\n"
            "Введите цену FOB ($/тонна):",
            parse_mode='HTML'
        )
        await CreatePull.price.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число больше 0:")

@dp.message_handler(state=CreatePull.price)
async def create_pull_price(message: types.Message, state: FSMContext):
    """Ввод цены пула"""
    try:
        price = float(message.text.strip().replace(',', '.'))
        if price <= 0:
            raise ValueError
        
        await state.update_data(price=price)
        
        await message.answer(
            "📦 <b>Создание пула</b>\n\n"
            "Шаг 4 из 10\n\n"
            "Выберите порт отгрузки:",
            reply_markup=port_keyboard(),
            parse_mode='HTML'
        )
        await CreatePull.port.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число больше 0:")

@dp.callback_query_handler(lambda c: c.data.startswith('port:'), state=CreatePull.port)
async def create_pull_port(callback: types.CallbackQuery, state: FSMContext):
    """Выбор порта для пула"""
    port_index = int(callback.data.split(':', 1)[1])
    ports = ['Астрахань', 'Новороссийск', 'Азов', 'Ростов-на-Дону', 'Тамань', 'Кавказ', 'Туапсе', 'Порт Оля']
    port = ports[port_index] if port_index < len(ports) else 'Астрахань'
    
    await state.update_data(port=port)
    
    await callback.message.edit_text(
        "📦 <b>Создание пула</b>\n\n"
        "Шаг 5 из 10\n\n"
        "Введите максимальную влажность (%):",
        parse_mode='HTML'
    )
    await CreatePull.moisture.set()
    await callback.answer()

@dp.message_handler(state=CreatePull.moisture)
async def create_pull_moisture(message: types.Message, state: FSMContext):
    """Ввод влажности для пула"""
    try:
        moisture = float(message.text.strip().replace(',', '.'))
        if not 0 <= moisture <= 100:
            raise ValueError
        
        await state.update_data(moisture=moisture)
        
        await message.answer(
            "📦 <b>Создание пула</b>\n\n"
            "Шаг 6 из 10\n\n"
            "Введите минимальную натуру (г/л):",
            parse_mode='HTML'
        )
        await CreatePull.nature.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число от 0 до 100:")

@dp.message_handler(state=CreatePull.nature)
async def create_pull_nature(message: types.Message, state: FSMContext):
    """Ввод натуры для пула"""
    try:
        nature = float(message.text.strip().replace(',', '.'))
        if nature <= 0:
            raise ValueError
        
        await state.update_data(nature=nature)
        
        await message.answer(
            "📦 <b>Создание пула</b>\n\n"
            "Шаг 7 из 10\n\n"
            "Введите максимальную сорность (%):",
            parse_mode='HTML'
        )
        await CreatePull.impurity.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число больше 0:")

@dp.message_handler(state=CreatePull.impurity)
async def create_pull_impurity(message: types.Message, state: FSMContext):
    """Ввод сорности для пула"""
    try:
        impurity = float(message.text.strip().replace(',', '.'))
        if not 0 <= impurity <= 100:
            raise ValueError
        
        await state.update_data(impurity=impurity)
        
        await message.answer(
            "📦 <b>Создание пула</b>\n\n"
            "Шаг 8 из 10\n\n"
            "Введите максимальную засорённость (%):",
            parse_mode='HTML'
        )
        await CreatePull.weed.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число от 0 до 100:")

@dp.message_handler(state=CreatePull.weed)
async def create_pull_weed(message: types.Message, state: FSMContext):
    """Ввод засорённости для пула"""
    try:
        weed = float(message.text.strip().replace(',', '.'))
        if not 0 <= weed <= 100:
            raise ValueError
        
        await state.update_data(weed=weed)
        
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("✅ Да", callback_data="pull_docs:yes"),
            InlineKeyboardButton("❌ Нет", callback_data="pull_docs:no")
        )
        
        await message.answer(
            "📦 <b>Создание пула</b>\n\n"
            "Шаг 9 из 10\n\n"
            "Требуются дополнительные документы?",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await CreatePull.documents.set()
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число от 0 до 100:")

@dp.callback_query_handler(lambda c: c.data.startswith('pull_docs:'), state=CreatePull.documents)
async def create_pull_documents(callback: types.CallbackQuery, state: FSMContext):
    """Выбор необходимости документов"""
    needs_docs = callback.data.split(':', 1)[1] == 'yes'
    
    if needs_docs:
        await state.update_data(documents='Да')
        
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("📄 Фитосанитарный", callback_data="doc_type:phyto"),
            InlineKeyboardButton("🧪 Ветеринарный", callback_data="doc_type:vet"),
            InlineKeyboardButton("📋 Качество", callback_data="doc_type:quality"),
            InlineKeyboardButton("📊 Все", callback_data="doc_type:all")
        )
        
        await callback.message.edit_text(
            "📦 <b>Создание пула</b>\n\n"
            "Шаг 10 из 10\n\n"
            "Какие документы требуются?",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await CreatePull.doc_type.set()
    else:
        await state.update_data(documents='Нет', doc_type='Не требуются')
        await finalize_pull_creation(callback.message, state, callback.from_user.id)
    
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('doc_type:'), state=CreatePull.doc_type)
async def create_pull_doc_type(callback: types.CallbackQuery, state: FSMContext):
    """Выбор типа документов"""
    doc_type_map = {
        'phyto': 'Фитосанитарный',
        'vet': 'Ветеринарный', 
        'quality': 'Качество',
        'all': 'Все документы'
    }
    
    doc_type_key = callback.data.split(':', 1)[1]
    doc_type = doc_type_map.get(doc_type_key, 'Не указан')
    
    await state.update_data(doc_type=doc_type)
    await finalize_pull_creation(callback.message, state, callback.from_user.id)
    await callback.answer()

async def finalize_pull_creation(message, state: FSMContext, user_id: int):
    """Завершение создания расширенного пула"""
    global pull_counter
    
    data = await state.get_data()
    pull_counter += 1
    
    pull = {
        'id': pull_counter,
        'exporter_id': user_id,
        'exporter_name': users[user_id].get('name', ''),
        'culture': data['culture'],
        'target_volume': data['target_volume'],
        'current_volume': 0,
        'price': data['price'],
        'port': data['port'],
        'moisture': data['moisture'],
        'nature': data['nature'],
        'impurity': data['impurity'],
        'weed': data['weed'],
        'documents': data.get('documents', 'Нет'),
        'doc_type': data.get('doc_type', 'Не требуются'),
        'status': 'Открыт',
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'participants': [],
        'matches': []   
    }
    
    pulls[pull_counter] = pull
    pull_participants[pull_counter] = []
    
    save_pulls_to_pickle()
    
    if gs and gs.spreadsheet:
        gs.sync_pull_to_sheets(pull)
    
    await state.finish()
    
    matching_batches = await find_matching_batches(pull)
    
    keyboard = get_role_keyboard('exporter')
    
    message_text = (
        f"✅ <b>Пул #{pull['id']} создан!</b>\n\n"
        f"🌾 Культура: {pull['culture']}\n"
        f"📦 Целевой объём: {pull['target_volume']} т\n"
        f"💰 Цена FOB: ${pull['price']}/т\n"
        f"🚢 Порт: {pull['port']}\n"
        f"💧 Влажность: до {pull['moisture']}%\n"
        f"🏋️ Натура: от {pull['nature']} г/л\n"
        f"🌾 Сорность: до {pull['impurity']}%\n"
        f"🌿 Засорённость: до {pull['weed']}%\n"
        f"📄 Документы: {pull['documents']}\n"
        f"📋 Тип: {pull['doc_type']}"
    )
    
    if matching_batches:
        message_text += f"\n\n🎯 Найдено подходящих партий: {len(matching_batches)}"
        for batch in matching_batches:
            await notify_match(batch['farmer_id'], user_id, batch['id'], pull['id'])
    
    await message.answer(message_text, reply_markup=keyboard, parse_mode='HTML')
    
    if matching_batches:
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🔍 Посмотреть подходящие партии", callback_data=f"view_pull_matches:{pull['id']}")
        )
        await message.answer(
            "Мы нашли фермеров, которые могут удовлетворить ваш запрос!",
            reply_markup=keyboard
        )

@dp.callback_query_handler(lambda c: c.data.startswith('view_pull_for_join:'), state='*')
async def view_pull_for_joining(callback: types.CallbackQuery, state: FSMContext):
    """Просмотр пула для присоединения"""
    await state.finish()
    
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
        return
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
    
    text = f"""
🎯 <b>Пул #{pull['id']} - Присоединение</b>

🌾 Культура: {pull['culture']}
📦 Целевой объём: {pull['target_volume']} т
📊 Заполнено: {progress:.1f}%
💰 Цена FOB: ${pull['price']}/т (~{pull['price'] * 75:,.0f} ₽/т)
🚢 Порт: {pull['port']}

<b>Требования к качеству:</b>
💧 Влажность: до {pull['moisture']}%
🏋️ Натура: от {pull['nature']} г/л  
🌾 Сорность: до {pull['impurity']}%
🌿 Засорённость: до {pull['weed']}%

👤 Экспортёр: {pull['exporter_name']}
"""
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("✅ Присоединиться к пулу", 
                           callback_data=f"joinpull:{pull_id}"), 
        InlineKeyboardButton("◀️ Назад к списку пулов", 
                           callback_data="back_to_pools_list")
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
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
    if user_id in farmer_batches and farmer_batches[user_id]:
        farmer_cultures = set(batch['culture'] for batch in farmer_batches[user_id])
        relevant_pulls = [p for p in open_pulls if p['culture'] in farmer_cultures]
        
        if relevant_pulls:
            open_pulls = relevant_pulls
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    for pull in open_pulls[:10]:
        available = pull['target_volume'] - pull['current_volume']
        progress = (pull['current_volume'] / pull['target_volume'] * 100) if pull['target_volume'] > 0 else 0
        
        button_text = f"🌾 {pull['culture']} | {available} т | ${pull['price']}/т ({progress:.0f}%)"
        keyboard.add(
            InlineKeyboardButton(button_text, callback_data=f"view_pull_for_join:{pull['id']}")
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
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
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
        for batches in farmer_batches.values():
            for batch in batches:
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

@dp.message_handler(lambda m: m.text == "📦 Мои пулы", state='*')
async def view_my_pulls(message: types.Message, state: FSMContext):
    """Просмотр расширенных пулов экспортёра"""
    await state.finish()
    
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
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
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
📊 Статус: {pull.get('status', 'Открыт')}
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
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_pull:{pull_id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"delete_pull:{pull_id}")
        )
        
        if active_matches:
            keyboard.add(
                InlineKeyboardButton("🎯 Показать совпадения", callback_data=f"view_pull_matches:{pull_id}")
            )
        
        keyboard.add(
            InlineKeyboardButton("👥 Участники", callback_data=f"pull_participants:{pull_id}"),
            InlineKeyboardButton("🚚 Логистика", callback_data=f"pull_logistics:{pull_id}")
        )
    else:
        keyboard.add(
            InlineKeyboardButton("✅ Присоединиться", callback_data=f"joinpull:{pull_id}")
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

@dp.callback_query_handler(lambda c: c.data.startswith('culture:'), state=SearchBatchesStatesGroup.enter_culture)
async def search_by_culture(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора культуры при поиске"""
    culture = callback.data.split(':', 1)[1]
    
    data = await state.get_data()
    search_type = data.get('search_type')
    
    if search_type == 'culture':
        await perform_search(callback.message, {'culture': culture})
        await state.finish()
    else:
        await state.update_data(culture=culture)
        await callback.message.edit_text(
            "🔍 <b>Комплексный поиск</b>\n\n"
            "Выберите регион:",
            reply_markup=region_keyboard()
        )
        await SearchBatchesStatesGroup.enter_region.set()
    
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('region:'), state=SearchBatchesStatesGroup.enter_region)
async def search_by_region(callback: types.CallbackQuery, state: FSMContext):
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
    
    for user_batches in farmer_batches.values():
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
    
    text = f"🔍 <b>Результаты поиска</b>\n\n"
    text += f"Найдено партий: {len(found_batches)}\n\n"
    
    for i, batch in enumerate(found_batches[:10], 1):  # Ограничиваем показ
        text += f"{i}. <b>Партия #{batch['id']}</b>\n"
        text += f"   🌾 {batch['culture']} • {batch['volume']} т\n"
        text += f"   💰 {batch['price']:,.0f} ₽/т\n"
        text += f"   📍 {batch['region']}\n"
        text += f"   ⭐ {batch['quality_class']}\n"
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
    if search_params.get('region') and batch['region'] != search_params['region']:
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
        batch['quality_class'] != search_params['quality_class']):
        return False
    if (search_params.get('storage_type') and 
        batch['storage_type'] != search_params['storage_type']):
        return False
    
    return True

@dp.callback_query_handler(lambda c: c.data.startswith('attach_files:'), state='*')
async def attach_files_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало прикрепления файлов к партии"""
    batch_id = int(callback.data.split(':', 1)[1])
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
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
    batch_id = int(callback.data.split(':', 1)[1])
    user_id = callback.from_user.id
    batch = None
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
async def cmd_stats(message: types.Message, state: FSMContext):
    """Статистика для пользователя"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы")
        return
    
    user = users[user_id]
    role = user.get('role')
    
    text = f"📊 <b>Ваша статистика</b>\n\n"
    text += f"🎭 Роль: {ROLES.get(role, role)}\n"
    
    if role == 'farmer':
        user_batches = farmer_batches.get(user_id, [])
        active_batches = [b for b in user_batches if b.get('status') == 'Активна']
        matches_count = len([m for m in matches.values() 
                           if m['status'] == 'active' and 
                           any(b['id'] == m['batch_id'] for b in user_batches)])
        
        text += f"📦 Партий: {len(user_batches)}\n"
        text += f"✅ Активных: {len(active_batches)}\n"
        text += f"🎯 Совпадений: {matches_count}\n"
        
        if user_batches:
            total_volume = sum(b['volume'] for b in user_batches)
            avg_price = sum(b['price'] for b in user_batches) / len(user_batches)
            text += f"📦 Общий объём: {total_volume:.0f} т\n"
            text += f"💰 Средняя цена: {avg_price:,.0f} ₽/т\n"
    
    elif role == 'exporter':
        user_pulls = [p for p in pulls.values() if p['exporter_id'] == user_id]
        open_pulls = [p for p in user_pulls if p.get('status') == 'Открыт']
        matches_count = len([m for m in matches.values() 
                           if m['status'] == 'active' and 
                           any(p['id'] == m['pull_id'] for p in user_pulls)])
        
        text += f"📦 Пулов: {len(user_pulls)}\n"
        text += f"🟢 Открытых: {len(open_pulls)}\n"
        text += f"🎯 Совпадений: {matches_count}\n"
        
        if user_pulls:
            total_volume = sum(p['target_volume'] for p in user_pulls)
            filled_volume = sum(p['current_volume'] for p in user_pulls)
            fill_percentage = (filled_volume / total_volume * 100) if total_volume > 0 else 0
            text += f"📦 Целевой объём: {total_volume:.0f} т\n"
            text += f"📈 Заполнено: {fill_percentage:.1f}%\n"
    
    elif role in ['logistic', 'expeditor']:
        service_type = "логистических" if role == 'logistic' else "экспедиторских"
        text += f"💼 {service_type.capitalize()} услуг: в разработке\n"
    
    text += f"\n📅 На платформе с: {user.get('registered_at', 'Неизвестно')}"
    
    await message.answer(text, parse_mode='HTML')

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
    
    text += f"\n\n📞 <b>Поддержка:</b> @exportum_support"
    
    await message.answer(text, parse_mode='HTML')

@dp.callback_query_handler(lambda c: c.data.startswith('edit_batch:'), state='*')
async def start_edit_batch(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования расширенной партии"""
    batch_id = int(callback.data.split(':', 1)[1])
    user_id = callback.from_user.id
    batch = None
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
                break
    
    if not batch:
        await callback.answer("❌ Партия не найдена", show_alert=True)
        return
    
    await state.update_data(editing_batch_id=batch_id)
    
    await callback.message.edit_text(
        f"✏️ <b>Редактирование партии #{batch_id}</b>\n\n"
        "Выберите поле для редактирования:",
        reply_markup=edit_batch_fields_keyboard(),
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
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
            if b['id'] == batch_id:
                batch = b
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
    batch_id = int(callback.data.split(':', 1)[1])
    user_id = callback.from_user.id
    batch_exists = False
    if user_id in farmer_batches:
        for b in farmer_batches[user_id]:
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
async def delete_batch_confirmed(callback: types.CallbackQuery):
    """Удаление партии"""
    batch_id = int(callback.data.split(':', 1)[1])
    user_id = callback.from_user.id
    
    if user_id in farmer_batches:
        farmer_batches[user_id] = [
            b for b in farmer_batches[user_id] if b['id'] != batch_id
        ]
        save_batches_to_pickle()
        if gs and gs.spreadsheet:
            gs.delete_batch_from_sheets(batch_id)
        
        await callback.message.edit_text(
            f"✅ Партия #{batch_id} успешно удалена"
        )
        await callback.answer("✅ Партия удалена")
    else:
        await callback.answer("❌ Ошибка удаления", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == 'cancel_delete_batch', state='*')
async def cancel_delete_batch(callback: types.CallbackQuery):
    """Отмена удаления партии"""
    await callback.message.edit_text("❌ Удаление отменено")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('edit_pull:'), state='*')
async def start_edit_pull(callback: types.CallbackQuery, state: FSMContext):
    """Начало редактирования пула"""
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
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
    
    await callback.message.edit_text(
        f"✏️ <b>Редактирование пула #{pull_id}</b>\n\n"
        "Выберите поле для редактирования:",
        reply_markup=edit_pull_fields_keyboard(),
        parse_mode='HTML'
    )
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
    await view_pull_details(callback)
    await callback.answer("✅ Культура обновлена")

@dp.callback_query_handler(lambda c: c.data.startswith('port:'), state=EditPullStatesGroup.edit_port)
async def edit_pull_port(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора порта при редактировании пула"""
    port_index = int(callback.data.split(':', 1)[1])
    ports = ['Астрахань', 'Новороссийск', 'Азов', 'Ростов-на-Дону', 'Тамань', 'Кавказ', 'Туапсе', 'Порт Оля']
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
    """Общая функция для редактирования числовых полей пула"""
    try:
        new_value = float(message.text.strip().replace(',', '.'))
        if new_value < min_val:
            await message.answer(f"❌ Значение должно быть не меньше {min_val}. Попробуйте ещё раз:")
            return
        if max_val is not None and new_value > max_val:
            await message.answer(f"❌ Значение должно быть не больше {max_val}. Попробуйте ещё раз:")
            return
        
        data = await state.get_data()
        pull_id = data.get('editing_pull_id')
        
        if not pull_id or pull_id not in pulls:
            await message.answer("❌ Ошибка: пул не найден")
            await state.finish()
            return
        
        pull = pulls[pull_id]
        old_value = pull.get(field, 'Не указано')
        pull[field] = new_value
        
        save_pulls_to_pickle()
        
        if gs and gs.spreadsheet:
            gs.update_pull_in_sheets(pull)
        
        await state.finish()
        
        keyboard = get_role_keyboard('exporter')
        
        await message.answer(
            f"✅ <b>{field_name} обновлена!</b>\n\n"
            f"Пул #{pull_id}\n"
            f"Старое значение: {old_value}\n"
            f"Новое значение: {new_value}",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        await view_pull_details_direct(message, pull_id)
        
    except ValueError:
        await message.answer("❌ Некорректное значение. Введите число. Попробуйте ещё раз:")

async def view_pull_details_direct(message, pull_id: int):
    """Вспомогательная функция для показа деталей пула"""
    if pull_id not in pulls:
        await message.answer("❌ Пул не найден")
        return
    
    pull = pulls[pull_id]
    user_id = message.from_user.id
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
📊 Статус: {pull.get('status', 'Открыт')}
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
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_pull:{pull_id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"delete_pull:{pull_id}")
        )
        
        if active_matches:
            keyboard.add(
                InlineKeyboardButton("🎯 Показать совпадения", callback_data=f"view_pull_matches:{pull_id}")
            )
        
        keyboard.add(
            InlineKeyboardButton("👥 Участники", callback_data=f"pull_participants:{pull_id}"),
            InlineKeyboardButton("🚚 Логистика", callback_data=f"pull_logistics:{pull_id}")
        )
    else:
        keyboard.add(
            InlineKeyboardButton("✅ Присоединиться", callback_data=f"joinpull:{pull_id}")
        )
    
    keyboard.add(
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_pulls")
    )
    
    await message.answer(text, reply_markup=keyboard, parse_mode='HTML')

@dp.callback_query_handler(lambda c: c.data.startswith('delete_pull:'), state='*')
async def delete_pull_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало удаления пула"""
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
        return
    
    user_id = callback.from_user.id
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    if pull['exporter_id'] != user_id:
        await callback.answer("❌ Нет доступа к удалению этого пула", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_pull:{pull_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete_pull")
    )
    
    await callback.message.edit_text(
        f"⚠️ <b>Подтверждение удаления</b>\n\n"
        f"Вы уверены, что хотите удалить пул #{pull_id}?\n\n"
        f"🌾 Культура: {pull['culture']}\n"
        f"📦 Объём: {pull['target_volume']} т\n"
        f"💰 Цена: ${pull['price']}/т\n\n"
        f"<b>Это действие нельзя отменить!</b>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('confirm_delete_pull:'), state='*')
async def delete_pull_confirmed(callback: types.CallbackQuery):
    """Удаление пула"""
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
        return
    
    
    if pull_id in pulls:
        matches_to_delete = [match_id for match_id, match in matches.items() if match['pull_id'] == pull_id]
        for match_id in matches_to_delete:
            del matches[match_id]
        del pulls[pull_id]
        if pull_id in pull_participants:
            del pull_participants[pull_id]
        
        save_pulls_to_pickle()
        
        await callback.message.edit_text(
            f"✅ Пул #{pull_id} успешно удалён"
        )
        await callback.answer("✅ Пул удалён")
    else:
        await callback.answer("❌ Ошибка удаления", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == 'cancel_delete_pull', state='*')
async def cancel_delete_pull(callback: types.CallbackQuery):
    """Отмена удаления пула"""
    await callback.message.edit_text("❌ Удаление отменено")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == 'cancel_edit_pull', state='*')
async def cancel_edit_pull(callback: types.CallbackQuery, state: FSMContext):
    """Отмена редактирования пула"""
    await state.finish()
    await callback.message.edit_text("❌ Редактирование отменено")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('contact_partner:'), state='*')
async def contact_partner(callback: types.CallbackQuery):
    """Показать контакты партнёра по сделке"""
    deal_id = int(callback.data.split(':', 1)[1])
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    user_id = callback.from_user.id
    partner_info = None
    
    if deal.get('exporter_id') == user_id:
        farmer_ids = deal.get('farmer_ids', [])
        if farmer_ids:
            partner_info = "👨‍🌾 <b>Контакты фермеров:</b>\n\n"
            for farmer_id in farmer_ids:
                farmer = users.get(farmer_id)
                if farmer:
                    partner_info += f"📝 {farmer.get('name', 'Неизвестно')}\n"
                    partner_info += f"📱 {farmer.get('phone', 'Не указан')}\n"
                    partner_info += f"📧 {farmer.get('email', 'Не указан')}\n"
                    partner_info += f"📍 {farmer.get('region', 'Не указан')}\n\n"
    
    elif user_id in deal.get('farmer_ids', []):
        exporter_id = deal.get('exporter_id')
        exporter = users.get(exporter_id)
        if exporter:
            partner_info = f"📦 <b>Контакты экспортёра:</b>\n\n"
            partner_info += f"📝 {exporter.get('name', 'Неизвестно')}\n"
            partner_info += f"📱 {exporter.get('phone', 'Не указан')}\n"
            partner_info += f"📧 {exporter.get('email', 'Не указан')}\n"
            partner_info += f"📍 {exporter.get('region', 'Не указан')}\n"
    
    elif deal.get('logistic_id') == user_id:
        exporter_id = deal.get('exporter_id')
        exporter = users.get(exporter_id)
        if exporter:
            partner_info = f"📦 <b>Контакты экспортёра:</b>\n\n"
            partner_info += f"📝 {exporter.get('name', 'Неизвестно')}\n"
            partner_info += f"📱 {exporter.get('phone', 'Не указан')}\n"
            partner_info += f"📧 {exporter.get('email', 'Не указан')}\n"
            partner_info += f"📍 {exporter.get('region', 'Не указан')}\n"
    
    elif deal.get('expeditor_id') == user_id:
        exporter_id = deal.get('exporter_id')
        exporter = users.get(exporter_id)
        if exporter:
            partner_info = f"📦 <b>Контакты экспортёра:</b>\n\n"
            partner_info += f"📝 {exporter.get('name', 'Неизвестно')}\n"
            partner_info += f"📱 {exporter.get('phone', 'Не указан')}\n"
            partner_info += f"📧 {exporter.get('email', 'Не указан')}\n"
            partner_info += f"📍 {exporter.get('region', 'Не указан')}\n"
    
    if not partner_info:
        await callback.answer("🤷‍♂️ Контакты не найдены", show_alert=True)
        return
    
    await callback.message.answer(partner_info, parse_mode='HTML')
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('complete_deal:'), state='*')
async def complete_deal(callback: types.CallbackQuery):
    """Завершение сделки"""
    deal_id = int(callback.data.split(':', 1)[1])
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    user_id = callback.from_user.id
    if deal.get('exporter_id') != user_id:
        await callback.answer("❌ Только экспортёр может завершить сделку", show_alert=True)
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
    deal_id = int(callback.data.split(':', 1)[1])
    
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
    deal_id = int(callback.data.split(':', 1)[1])
    
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
    deal_id = int(callback.data.split(':', 1)[1])
    
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
    deal_id = int(callback.data.split(':', 1)[1])
    
    if deal_id not in deals:
        await callback.answer("❌ Сделка не найдена", show_alert=True)
        return
    
    deal = deals[deal_id]
    
    text = f"🚚 <b>Логистика сделки #{deal_id}</b>\n\n"
    
    if deal.get('logistic_id'):
        logistic = users.get(deal['logistic_id'])
        if logistic:
            text += f"✅ <b>Логист назначен:</b>\n"
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
            text += f"\n✅ <b>Экспедитор назначен:</b>\n"
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

@dp.callback_query_handler(lambda c: c.data.startswith('pull_participants:'), state='*')
async def show_pull_participants(callback: types.CallbackQuery):
    """Показать участников пула"""
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
        return
    
    
    if pull_id not in pulls:
        await callback.answer("❌ Пул не найден", show_alert=True)
        return
    
    pull = pulls[pull_id]
    participants = pull_participants.get(pull_id, [])
    
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
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
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
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
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

@dp.callback_query_handler(lambda c: c.data.startswith('view_shipping_requests:'), state='*')
async def view_shipping_requests(callback: types.CallbackQuery):
    """Просмотр заявок на логистику для пула"""
    try:
        pull_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError) as e:
        await callback.answer("❌ Ошибка обработки данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data: {e}, data: {callback.data}")
        return
    
    await callback.answer("📋 Функция в разработке", show_alert=True)

@dp.message_handler(lambda m: m.text in ["🚚 Моя карточка", "🚛 Моя карточка"], state='*')
async def view_service_provider_card(message: types.Message, state: FSMContext):
    """Просмотр карточки логиста/экспедитора"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы")
        return
    
    user = users[user_id]
    role = user.get('role', '')
    
    if role not in ['logistic', 'expeditor']:
        await message.answer("❌ Эта функция доступна только логистам и экспедиторам")
        return
    
    role_emoji = "🚚" if role == 'logistic' else "🚛"
    role_name = "Логист" if role == 'logistic' else "Экспедитор"
    service_type = "логистических" if role == 'logistic' else "экспедиторских"
    
    text = f"""
{role_emoji} <b>Моя карточка {role_name.lower()}</b>

👤 Имя: {user.get('name', 'Не указано')}
📱 Телефон: {user.get('phone', 'Не указан')}
📧 Email: {user.get('email', 'Не указан')}
📍 Регион: {user.get('region', 'Не указан')}
🎭 Роль: {role_name}
📅 Регистрация: {user.get('registered_at', 'Неизвестно')}

💼 <b>Статистика {service_type} услуг:</b>
• Активных заявок: в разработке
• Выполненных перевозок: в разработке
• Отзывов: в разработке
"""
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Редактировать", callback_data="edit_service_card"),
        InlineKeyboardButton("📊 Статистика", callback_data="service_stats")
    )
    
    if role == 'logistic':
        keyboard.add(
            InlineKeyboardButton("💰 Указать тарифы", callback_data="set_logistics_rates"),
            InlineKeyboardButton("🛣 Указать маршруты", callback_data="set_logistics_routes")
        )
    else:
        keyboard.add(
            InlineKeyboardButton("💰 Указать тарифы", callback_data="set_expeditor_rates"),
            InlineKeyboardButton("📄 Указать услуги", callback_data="set_expeditor_services")
        )
    
    await message.answer(text, parse_mode='HTML', reply_markup=keyboard)

@dp.message_handler(lambda m: m.text == "📋 Активные заявки", state='*')
async def view_active_service_requests(message: types.Message, state: FSMContext):
    """Просмотр активных заявок для логиста/экспедитора"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы")
        return
    
    role = users[user_id].get('role', '')
    if role not in ['logistic', 'expeditor']:
        await message.answer("❌ Эта функция доступна только логистам и экспедиторам")
        return
    
    service_type = "логистических" if role == 'logistic' else "экспедиторских"
    
    text = f"📋 <b>Активные заявки</b>\n\n"
    text += f"Список {service_type} заявок в разработке.\n\n"
    text += "Скоро здесь появятся:\n"
    text += "• Заявки на перевозки\n" if role == 'logistic' else "• Заявки на оформление документов\n"
    text += "• Контакты заказчиков\n"
    text += "• Детали заявок\n"
    text += "• Статусы выполнения\n"
    
    await message.answer(text, parse_mode='HTML')

@dp.message_handler(lambda m: m.text in ["💼 Мои предложения", "📋 Мои перевозки", "📋 Мои оформления"], state='*')
async def view_my_service_offers(message: types.Message, state: FSMContext):
    """Просмотр предложений и выполненных работ"""
    await state.finish()
    
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы")
        return
    
    role = users[user_id].get('role', '')
    if role not in ['logistic', 'expeditor']:
        await message.answer("❌ Эта функция доступна только логистам и экспедиторам")
        return
    
    service_type = "логистических" if role == 'logistic' else "экспедиторских"
    completed_type = "перевозок" if role == 'logistic' else "оформлений"
    
    text = f"💼 <b>Мои {service_type} услуги</b>\n\n"
    text += f"Статистика {completed_type} в разработке.\n\n"
    text += "Скоро здесь появится:\n"
    text += "• История предложений\n"
    text += "• Выполненные работы\n"
    text += "• Отзывы клиентов\n"
    text += "• Финансовая статистика\n"
    
    await message.answer(text, parse_mode='HTML')

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
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit_pull")
    )
    return keyboard
async def setup_scheduler():
    """Настройка планировщика задач"""
    try:
        scheduler.add_job(update_prices_cache, 'interval', hours=6)
        scheduler.add_job(update_news_cache, 'interval', hours=2)
        scheduler.add_job(auto_match_batches_and_pulls, 'interval', minutes=30)
        scheduler.add_job(send_daily_stats, 'cron', hour=9, minute=0)
        
        scheduler.start()
        logging.info("✅ Планировщик задач настроен и запущен")
    except Exception as e:
        logging.error(f"❌ Ошибка настройки планировщика: {e}")

async def send_daily_stats():
    """Ежедневная отправка статистики админу"""
    try:
        total_users = len(users)
        role_stats = defaultdict(int)
        for user in users.values():
            role = user.get('role', 'unknown')
            role_stats[role] += 1
        total_batches = sum(len(batches) for batches in farmer_batches.values())
        active_batches = sum(1 for batches in farmer_batches.values() for b in batches if b.get('status') == 'Активна')
        total_pulls = len(pulls)
        open_pulls = len([p for p in pulls.values() if p.get('status') == 'Открыт'])
        total_deals = len(deals)
        active_deals = len([d for d in deals.values() if d.get('status') in ['pending', 'matched', 'shipping']])
        
        text = f"📊 <b>Ежедневная статистика Exportum</b>\n\n"
        text += f"👥 Пользователей: {total_users}\n"
        text += f"📦 Партий: {total_batches} (активных: {active_batches})\n"
        text += f"🎯 Пулов: {total_pulls} (открытых: {open_pulls})\n"
        text += f"📋 Сделок: {total_deals} (активных: {active_deals})\n"
        text += f"🎯 Совпадений: {len(matches)}\n\n"
        
        text += f"<b>Распределение по ролям:</b>\n"
        for role, count in role_stats.items():
            role_name = ROLES.get(role, role)
            text += f"• {role_name}: {count}\n"
        
        await bot.send_message(ADMIN_ID, text, parse_mode='HTML')
        logging.info("✅ Ежедневная статистика отправлена админу")
        
    except Exception as e:
        logging.error(f"❌ Ошибка отправки ежедневной статистики: {e}")
async def on_startup(dp):
    logging.info("🚀 Бот Exportum запущен")
    load_users_from_json()
    load_batches_from_pickle()
    load_pulls_from_pickle()
    await setup_scheduler()
    try:
        await update_prices_cache()
        await update_news_cache()
        logging.info("✅ Данные обновлены при запуске")
    except Exception as e:
        logging.error(f"❌ Ошибка обновления данных: {e}")
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

@dp.message_handler(state='*')
async def handle_unexpected_messages(message: types.Message, state: FSMContext):
    """Обработка сообщений в непредусмотренных состояниях"""
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

@dp.callback_query_handler(state='*')
async def handle_unexpected_callbacks(callback: types.CallbackQuery, state: FSMContext):
    """Обработка callback'ов в непредусмотренных состояниях"""
    await callback.answer("❌ Это действие недоступно в текущем состоянии", show_alert=True)
def parse_cbot_futures():
    """Парсинг CBoT фьючерсных цен"""
    prices = {}
    try:
        symbols = {
            'Пшеница CBoT': 'ZW=F',
            'Кукуруза CBoT': 'ZC=F', 
            'Соя CBoT': 'ZS=F'
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
                        prices[name] = f"{price_dollars:.2f}¢/bu"
                        logging.info(f"✅ {name}: {price_dollars:.2f}¢/bu")
                        
            except Exception as e:
                logging.error(f"❌ {name}: {e}")
                continue
        
        if not prices:
            prices = {
                'Пшеница CBoT': '5.50¢/bu',
                'Кукуруза CBoT': '4.20¢/bu',
                'Соя CBoT': '10.80¢/bu'
            }
            logging.warning("⚠️ CBoT: fallback значения")
        
        return prices
        
    except Exception as e:
        logging.error(f"❌ parse_cbot_futures: {e}")
        return {
            'Пшеница CBoT': '5.50¢/bu',
            'Кукуруза CBoT': '4.20¢/bu', 
            'Соя CBoT': '10.80¢/bu'
        }
async def on_shutdown(dp):
    """Завершение работы бота"""
    logging.info("⏹ Бот Exportum останавливается...")
    save_users_to_json()
    save_batches_to_pickle()
    save_pulls_to_pickle()
    logging.info("✅ Данные сохранены")
    
    await bot.close()
    await dp.storage.close()
    await dp.storage.wait_closed()

if __name__ == '__main__':
    logging.info("🚀 Бот Exportum запускается...")
    try:
        import os
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        logging.info("✅ Директории созданы")
    except Exception as e:
        logging.error(f"❌ Ошибка создания директорий: {e}")
    from aiogram import executor
    executor.start_polling(
        dp, 
        skip_updates=True, 
        on_startup=on_startup, 
        on_shutdown=on_shutdown
    )