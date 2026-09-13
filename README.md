# 🤖 EXPORTUM Bot — Documentation

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Aiogram](https://img.shields.io/badge/Aiogram-2.25.2-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://docs.aiogram.dev/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![Pandas](https://img.shields.io/badge/Pandas-Data-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Google Sheets](https://img.shields.io/badge/Google_Sheets-Integration-34A853?style=for-the-badge&logo=googlesheets&logoColor=white)](https://developers.google.com/sheets/api)
[![pytest](https://img.shields.io/badge/pytest-Verification-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)](https://pytest.org/)

</div>

## 🚀 Quick start

```bash
cp .env.example .env
# fill .env with your own values
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
./run.sh
```

## 📋 Description

**EXPORTUM** is a Telegram bot for an agricultural logistics platform that connects farmers, exporters, logistics providers and freight forwarders to coordinate grain shipments efficiently.

## 🧠 Status logic (current as of 2026-03-13)

### Request lifecycle
- `active/open/new/has_offers` — request is open to logistics providers.
- `assigned` — logistics provider selected.
- `expeditor_selected` — freight forwarder selected.
- `in_progress` — shipment is in progress.
- `completed/cancelled/rejected` — terminal states.

### Assignee-selection rules
- A freight forwarder can be selected only after a logistics provider has been assigned.
- Reassignment is blocked when a delivery is active or already closed.
- When one offer is selected, competing mutable offers move to `rejected`.
- An offer in `in_progress` is never rolled back to `accepted`.

### Offer-status normalization
- Open logistics-provider statuses: `pending`, `active`, `new`, `open`.
- Open freight-forwarder statuses: `pending`, `active`, `new`, `open`.
- Selected/working statuses: `accepted`, `assigned`, `selected`, `reserved`, `in_progress`.
- Transition checks use effective statuses (`get_effective_*_status`) rather than relying only on the raw `status` field.

### Main features
- Create and manage grain pools.
- Search and match grain batches.
- Organize logistics and freight forwarding.
- Manage deals and deliveries.
- Administrative panel for monitoring and management.

---

## 👥 User roles

### 🌾 Farmer
- Create grain batches: crop, volume, price and quality.
- Join exporter pools.
- Browse available pools and matches.
- Manage own batches.

### 📦 Exporter
- Create pools: crop, volume, port and FOB price.
- Select logistics providers and freight forwarders.
- Manage pools: close and complete.
- Review participants and statistics.

### 🚚 Logistics provider
- Create a logistics profile: vehicle, routes and pricing.
- Browse available logistics requests.
- Submit delivery offers.
- Manage active shipments.

### 🚛 Freight forwarder
- Create a freight-forwarder profile: services, ports and experience.
- Browse available deals.
- Submit forwarding offers.
- Manage own requests.

### 🔐 Administrator
- System statistics.
- Analytics by regions, crops and ports.
- Data export to CSV and JSON.
- User management.
- Broadcast messaging.

---

## 📊 Data structures

### `users` (dict)
```python
{
    user_id: {
        "id": int,
        "role": str,  # "farmer", "exporter", "logistic", "expeditor"
        "name": str,
        "phone": str,
        "email": str,
        "region": str,
        "inn": str,
        "ogrn": str,
        "company_name": str,
        "company_details": str,
        "registered_at": str,
        ...
    }
}
```

### `pools` (dict)
```python
{
    pool_id: {
        "id": int,
        "exporter_id": int,
        "culture": str,
        "target_volume": float,
        "current_volume": float,
        "price": float,
        "port": str,
        "status": str,  # "active", "filled", "closed", "completed"
        "selected_logistic_id": int | None,
        "selected_expeditor_id": int | None,
        "batch_ids": list[int],
        "farmer_ids": list[int],
        "created_at": str,
        "completed_at": str | None,
        ...
    }
}
```

### `batches` (dict)
```python
{
    farmer_id: [
        {
            "id": int,
            "culture": str,
            "volume": float,
            "price": float,
            "status": str,  # runtime values include "Активна", "Зарезервирована", "sold"
            "region": str,
            "moisture": float,
            "impurity": float,
            "quality_class": str,
            "pool_id": int | None,
            ...
        }
    ]
}
```

### `logistics_cards` (dict)
```python
{
    user_id: {
        "vehicle_type": str,  # "truck", "grain", "wagon"
        "capacity": float,
        "regions": list[str],
        "ports": list[str],
        "price_per_km": float,
        "description": str,
        ...
    }
}
```

### `expeditor_cards` (dict)
```python
{
    user_id: {
        "services": str,
        "dt_price": float,
        "ports": list[str],
        "experience": str,
        ...
    }
}
```

> Runtime/UI strings that are still stored in Russian are preserved exactly where their literal value matters.

---

## 🔄 Main E2E scenarios

### Farmer

1. **Registration** → `/start` → select role `Фермер` → enter profile data.
2. **Create batch** → `➕ Добавить партию` → enter crop, volume, price and quality.
3. **Find pools** → `🔍 Найти пулы` → browse available pools → join a pool.
4. **Manage batches** → `📦 Мои партии` → view/edit/delete.

### Exporter

1. **Registration** → `/start` → select role `Экспортёр` → enter profile data.
2. **Create pool** → `➕ Создать пул` → enter crop, volume, port and price → select document type.
3. **Select logistics provider** → browse provider cards → select provider → provider receives a notification.
4. **Select freight forwarder** → browse forwarder cards → select forwarder → forwarder receives a notification.
5. **Complete pool** → `🎉 Завершить пул` → confirmation → notifications to all participants.

### Logistics provider

1. **Registration** → `/start` → select role `Логист` → enter profile data.
2. **Create profile card** → `💳 Моя карточка` → `➕ Создать карточку` → enter vehicle, routes and prices.
3. **Browse requests** → `📋 Доступные заявки` → open request → submit an offer.
4. **Manage shipments** → `🚚 Мои перевозки` → review active deliveries.

### Freight forwarder

1. **Registration** → `/start` → select role `Экспедитор` → enter profile data.
2. **Create profile card** → `💳 Моя карточка` → `➕ Создать карточку` → enter services, ports and experience.
3. **Browse deals** → `📋 Доступные сделки` → review available pools → submit an offer.

### Administrator

1. **Login** → `/admin` → permission check → admin panel.
2. **Statistics** → `📊 Статистика` → review overall system statistics.
3. **Analytics** → `📈 Аналитика` → review regions, crops and ports.
4. **Data export** → `📤 Экспорт данных` → choose data type → download file.
5. **User management** → `👥 Пользователи` → list → user details.
6. **Broadcast** → `📢 Рассылка` → enter message → confirm → send to all users.

> The current Telegram UI is Russian, so literal button labels are kept unchanged in the documentation.

---

## 🚀 Runtime commands

### Main commands
- `/start` — start the bot and enter registration/login flow.
- `/admin` — open the admin panel for administrators only.

### Navigation
- Main-menu buttons depend on the active user role.
- Callback buttons move between sections.
- `Назад` buttons return to the previous menu.

---

## ⚙️ Configuration

### Environment variables
- `BOT_TOKEN` — Telegram bot token, required.
- `ADMIN_ID` — administrator Telegram ID, required.
- `DATA_DIR` — data-storage directory, defaults to `data/`.

### Pickle data files
- `users.pkl` — users.
- `pools.pkl` — pools.
- `batches.pkl` — grain batches.
- `logistics_cards.pkl` — logistics-provider cards.
- `expeditor_cards.pkl` — freight-forwarder cards.
- `shipping_requests.pkl` — delivery requests.
- `logistic_offers.pkl` — logistics offers.
- `expeditor_pool_offers.pkl` — freight-forwarder offers for pools.
- `expeditor_request_offers.pkl` — freight-forwarder offers for requests.

---

## 📁 Code structure

### Main sections

#### 1. Imports and configuration (lines 1–200)
- Library imports.
- Constants and settings.
- Bot and dispatcher initialization.

#### 2. Global data structures (lines 100–200)
- `users`, `pools`, `batches`.
- `logistics_cards`, `expeditor_cards`.
- `shipping_requests`, `logistic_offers`, `expeditor_offers`.

#### 3. FSM states (lines 1300–1500)
- `RegistrationStatesGroup`
- `CreatePoolStatesGroup`
- `JoinPoolStatesGroup`
- `EditProfileStates`
- `BroadcastStates`
- and others.

#### 4. Helper functions (lines 200–3000)
- Data validation.
- Message formatting.
- Keyboard construction.
- Notification helpers.
- Data-export functions.

#### 5. Handlers (lines 4000–33000)
- **Admin handlers** (lines 4500–6500)
- **Registration handlers** (lines 6700–8000)
- **Pool handlers** (lines 10000–13000)
- **Batch handlers** (lines 10000–12000)
- **Logistics handlers** (lines 25000–28000)
- **Freight-forwarder handlers** (lines 18000–20000)
- **Notification handlers** (lines 3500–4000)

#### 6. Save/load functions (lines 700–900)
- `save_data()` — persist all data.
- `load_data()` — load all data.
- `save_*_to_pickle()` — persist individual structures.

---

## 🔧 Known issues / TODO

### Marked removal candidates
- `parse_price()` — unused.
- `get_all_pools_with_format()` — unused.
- `translate_pool_status()` — unused.
- `parse_join_pool_callback()` — unused.
- `validate_batch_volume()` — unused.
- `migrate_all_existing_pools()` — one-time migration.
- `migrate_old_pools()` — one-time migration.

### Improvements
- Add stronger validation when creating pools and batches.
- Improve Telegram API error handling.
- Add rate limiting for bulk operations.
- Optimize large-volume data processing.

---

## 📝 Notes

- All data is held in memory and periodically persisted to pickle files.
- Google Sheets integration requires a configured `gs` object.
- Logging uses Python's standard `logging` module.
- Functions follow the project's CONTEXT7 conventions: complete implementation, error handling, logging and docstrings.

---

## 🔗 Related documents

- `docs/README.md` — documentation index.
- `docs/CUSTOMER_OVERVIEW.md` — non-technical stakeholder overview.
- `docs/reports_2026-02-16.tar.gz` — archived audit and testing reports.
