# Проектирование витрины dm_courier_ledger

## 1. Поля, необходимые для витрины

| Поле | Описание | Источник данных |
|---|---|---|
| `courier_id` | ID курьера | API /couriers → dds.dm_couriers |
| `courier_name` | Ф.И.О. курьера | API /couriers → dds.dm_couriers |
| `settlement_year` | Год отчётного периода (по дате заказа) | dds.fct_deliveries.order_ts или dds.dm_orders.order_ts |
| `settlement_month` | Месяц отчётного периода (1–12, по дате заказа) | dds.fct_deliveries.order_ts или dds.dm_orders.order_ts |
| `orders_count` | Количество доставленных заказов за месяц | dds.fct_deliveries (COUNT по courier_id + месяц) |
| `orders_total_sum` | Общая стоимость заказов за месяц | dds.fct_deliveries.sum (SUM по courier_id + месяц) |
| `rate_avg` | Средний рейтинг курьера за месяц | dds.fct_deliveries.rate (AVG по courier_id + месяц) |
| `order_processing_fee` | Комиссия компании: orders_total_sum × 0.25 | Вычисляется в витрине |
| `courier_order_sum` | Сумма к выплате курьеру за заказы (по рейтингу) | Вычисляется в витрине из dds.fct_deliveries.sum + rate_avg |
| `courier_tips_sum` | Сумма чаевых | dds.fct_deliveries.tip_sum (SUM по courier_id + месяц) |
| `courier_reward_sum` | Итоговая выплата: courier_order_sum + courier_tips_sum × 0.95 | Вычисляется в витрине | 

> Отчёт собирается по дате заказа (order_ts), а не по дате доставки (delivery_ts).

---

## 2. Таблицы в слое DDS

### Уже существующие в хранилище

| Таблица | Поля, используемые в витрине | Назначение |
|---|---|---|
| `dds.dm_orders` | `order_id`, `order_ts` | Справочник заказов; подтверждает, что заказ существует; даёт дату заказа для отчёта |
| `dds.dm_restaurants` | — | Не используется напрямую в витрине, но доступно в DWH |

> Поля в `dds.dm_orders` могут отличаться от проекта к проекту. Если в `dm_orders` уже есть `order_ts` — используем его. Если удобнее использовать `order_ts` из `fct_deliveries` (он дублируется в API-ответе) — тоже допустимо, но свзь с `dm_orders` нужна для целостности.

### Недостающие таблицы (нужно создать)

| Таблица | Назначение | Источник |
|---|---|---|
| `dds.dm_couriers` | Справочник курьеров (ID, Ф.И.О.) | API GET /couriers |
| `dds.fct_deliveries` | Факты доставок: связь курьера с заказом, рейтинг, чаевые, суммы | API GET /deliveries |

### Состав новых таблиц

#### `dds.dm_couriers`
| Поле | Тип | Описание |
|---|---|---|
| `courier_id` | uuid (PK) | Суррогатный ключ |
| `courier_original_id` | text (UNIQUE) | `_id` из API (бизнес-ключ) |
| `courier_name` | text | Ф.И.О. курьера (поле `name` из API) |

#### `dds.fct_deliveries`
| Поле | Тип | Описание |
|---|---|---|
| `delivery_id` | uuid (PK) | Суррогатный ключ |
| `delivery_original_id` | text (UNIQUE) | `delivery_id` из API (бизнес-ключ) |
| `order_id` | text | `order_id` из API (связь с `dds.dm_orders`) |
| `courier_id` | text | `courier_id` из API (связь с `dds.dm_couriers`) |
| `order_ts` | timestamptz | Дата/время заказа (для отчёта по месяцам) |
| `delivery_ts` | timestamptz | Дата/время доставки |
| `address` | text | Адрес доставки |
| `rate` | int | Оценка курьера (1–5) |
| `sum` | numeric | Стоимость заказа |
| `tip_sum` | numeric | Чаевые |

---

## 3. Сущности и поля для загрузки из API

### Эндпоинт: GET /couriers
**Цель:** наполнить `dds.dm_couriers`

| Поле в API | Поле в DDS | Тип |
|---|---|---|
| `_id` | `courier_original_id` | text |
| `name` | `courier_name` | text |

### Эндпоинт: GET /deliveries
**Цель:** наполнить `dds.fct_deliveries`

| Поле в API | Поле в DDS | Тип |
|---|---|---|
| `delivery_id` | `delivery_original_id` | text |
| `order_id` | `order_id` | text |
| `courier_id` | `courier_id` | text |
| `order_ts` | `order_ts` | timestamptz |
| `delivery_ts` | `delivery_ts` | timestamptz |
| `address` | `address` | text |
| `rate` | `rate` | int |
| `sum` | `sum` | numeric |
| `tip_sum` | `tip_sum` | numeric |

### Эндпоинт: GET /restaurants
**Цель:** не требуется для витрины выплат курьерам.

> Загрузка /restaurants не нужна для данной витрины, так как в составе витрины нет полей, связанных с ресторанами.

