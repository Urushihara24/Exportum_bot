<div align="center">

# 🌾 Exportum Bot

### Telegram-бот для торговли зерновыми культурами нового поколения

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![aiogram](https://img.shields.io/badge/aiogram-3.0+-00ADD8?style=for-the-badge&logo=telegram&logoColor=white)](https://aiogram.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

*Автоматизация зерновой торговли с интеграцией Google Sheets, парсингом цен и управлением сделками*

[Возможности](#-возможности) • [Установка](#-установка) • [Использование](#-использование) • [Технологии](#-технологии) • [Документация](#-документация)

<img src="https://raw.githubusercontent.com/Urushihara24/Exportum_bot/main/docs/demo.gif" alt="Demo" width="600">

</div>

---

## 📋 О проекте

**Exportum Bot** — это комплексное решение для автоматизации торговли зерновыми культурами через Telegram. Бот объединяет фермеров, трейдеров, логистические компании и экспедиторов на единой платформе, обеспечивая прозрачность сделок и автоматизацию рутинных процессов.

### 🎯 Основная идея

Создать экосистему, где:
- 🌾 **Фермеры** быстро находят покупателей для своей продукции
- 💼 **Трейдеры** получают актуальные предложения и цены
- 🚚 **Логисты** находят заказы на перевозку
- 📊 Все участники имеют доступ к реальным ценам рынка

---

## ✨ Возможности

<table>
<tr>
<td width="50%" valign="top">

### 👥 Управление пользователями

- ✅ **Регистрация** с полными реквизитами
- ✅ **Роли:** Фермер, Трейдер, Логист, Экспедитор
- ✅ **Профили компаний** с ИНН, КПП, ОГРН
- ✅ **Контактная информация** и реквизиты
- ✅ **История операций** каждого пользователя

### 🌾 Управление партиями

- ✅ **Создание партий** зерна (тип, объем, цена)
- ✅ **Редактирование** параметров
- ✅ **Удаление** и архивация
- ✅ **Поиск** по критериям
- ✅ **Статусы** партий (активна, в сделке, продана)

### 🔄 Автоматический матчинг

- ✅ **Умный поиск** совпадений
- ✅ **Уведомления** о подходящих партиях
- ✅ **Рекомендации** на основе истории
- ✅ **Фильтрация** по параметрам

</td>
<td width="50%" valign="top">

### 💰 Парсинг цен

- ✅ **Российские цены** (ikar.ru)
- ✅ **FOB цены** (zol.ru)
- ✅ **CBoT** котировки
- ✅ **Автообновление** каждые 6 часов
- ✅ **История цен** и графики

### 📰 Новости рынка

- ✅ **Автоматический парсинг** новостей
- ✅ **Фильтры** по темам
- ✅ **Push-уведомления** о важных событиях
- ✅ **Архив новостей**

### 📊 Интеграция Google Sheets

- ✅ **Синхронизация** данных
- ✅ **Отчеты** в реальном времени
- ✅ **Экспорт** сделок и партий
- ✅ **Аналитика** по пользователям

### 🚚 Логистика

- ✅ **Заявки** на перевозку
- ✅ **Калькулятор** стоимости
- ✅ **Отслеживание** статуса
- ✅ **Документооборот**

### 📄 Управление сделками

- ✅ **Создание сделок** между участниками
- ✅ **Документы** (контракты, спецификации)
- ✅ **Статусы** и этапы сделки
- ✅ **История платежей**

</td>
</tr>
</table>

---

## 🛠️ Технологии

<div align="center">

### Основной стек

![Python](https://img.shields.io/badge/-Python_3.10+-3776AB?style=flat&logo=python&logoColor=white)
![aiogram](https://img.shields.io/badge/-aiogram_3.0-2CA5E0?style=flat&logo=telegram&logoColor=white)
![Google Sheets API](https://img.shields.io/badge/-Google_Sheets_API-34A853?style=flat&logo=google-sheets&logoColor=white)
![BeautifulSoup](https://img.shields.io/badge/-BeautifulSoup4-EA4335?style=flat&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/-Pandas-150458?style=flat&logo=pandas&logoColor=white)

### Дополнительные технологии

![asyncio](https://img.shields.io/badge/-asyncio-3776AB?style=flat&logo=python&logoColor=white)
![FSM](https://img.shields.io/badge/-FSM-00ADD8?style=flat)
![dotenv](https://img.shields.io/badge/-python--dotenv-ECD53F?style=flat&logo=python&logoColor=black)
![gspread](https://img.shields.io/badge/-gspread-34A853?style=flat&logo=google-sheets&logoColor=white)

</div>

### Детальный список библиотек:

| Библиотека | Версия | Назначение |
|------------|--------|------------|
| `aiogram` | 3.0+ | Асинхронный фреймворк для Telegram Bot API |
| `gspread` | 5.0+ | Работа с Google Sheets |
| `oauth2client` | 4.1+ | Авторизация Google API |
| `beautifulsoup4` | 4.12+ | Парсинг веб-страниц |
| `requests` | 2.31+ | HTTP-запросы для парсинга |
| `pandas` | 2.0+ | Обработка и анализ данных |
| `python-dotenv` | 1.0+ | Управление переменными окружения |
| `apscheduler` | 3.10+ | Планировщик задач |

---

## 📦 Установка

### Требования:

- ✅ Python 3.10 или выше
- ✅ Telegram Bot Token (от [@BotFather](https://t.me/BotFather))
- ✅ Google Cloud Project с включенным Google Sheets API
- ✅ Учетная запись Google для Sheets

### Шаг 1: Клонирование репозитория

