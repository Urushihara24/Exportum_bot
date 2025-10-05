<div align="center">

# 🌾 Exportum Bot

### Telegram-бот для торговли зерновыми культурами нового поколения

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![aiogram](https://img.shields.io/badge/aiogram-3.0+-00ADD8?style=for-the-badge&logo=telegram&logoColor=white)](https://aiogram.dev/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

*Автоматизация зерновой торговли с интеграцией Google Sheets, парсингом цен и управлением сделками*

[Возможности](#-возможности) • [Установка](#-установка) • [Использование](#-использование) • [Технологии](#-технологии) • [Документация](#-документация)

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

<table width="100%">
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

---

<div align="center">

### 📚 Детальный список библиотек

<table width="90%">
<thead>
<tr>
<th align="center" width="30%">Библиотека</th>
<th align="center" width="15%">Версия</th>
<th align="left" width="55%">Назначение</th>
</tr>
</thead>
<tbody>
<tr>
<td align="center"><code>aiogram</code></td>
<td align="center">3.0+</td>
<td>Асинхронный фреймворк для Telegram Bot API</td>
</tr>
<tr>
<td align="center"><code>gspread</code></td>
<td align="center">5.0+</td>
<td>Работа с Google Sheets</td>
</tr>
<tr>
<td align="center"><code>oauth2client</code></td>
<td align="center">4.1+</td>
<td>Авторизация Google API</td>
</tr>
<tr>
<td align="center"><code>beautifulsoup4</code></td>
<td align="center">4.12+</td>
<td>Парсинг веб-страниц</td>
</tr>
<tr>
<td align="center"><code>requests</code></td>
<td align="center">2.31+</td>
<td>HTTP-запросы для парсинга</td>
</tr>
<tr>
<td align="center"><code>pandas</code></td>
<td align="center">2.0+</td>
<td>Обработка и анализ данных</td>
</tr>
<tr>
<td align="center"><code>python-dotenv</code></td>
<td align="center">1.0+</td>
<td>Управление переменными окружения</td>
</tr>
<tr>
<td align="center"><code>apscheduler</code></td>
<td align="center">3.10+</td>
<td>Планировщик задач</td>
</tr>
</tbody>
</table>

</div>

---

## 📦 Установка

<div align="center">

### ✅ Требования

<table width="80%">
<tr>
<td align="center" width="50%">
  
**🐍 Python 3.10+**  
Современная версия Python

</td>
<td align="center" width="50%">
  
**🤖 Telegram Bot Token**  
От [@BotFather](https://t.me/BotFather)

</td>
</tr>
<tr>
<td align="center" width="50%">
  
**☁️ Google Cloud Project**  
С включенным Google Sheets API

</td>
<td align="center" width="50%">
  
**📊 Google Account**  
Для работы со Sheets

</td>
</tr>
</table>

</div>

---

