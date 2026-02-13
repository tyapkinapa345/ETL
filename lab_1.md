# Лабораторная работа 1.1: ETL-конвейер на Pentaho Data Integration (PDI)

**Группа:** АДЭУ-221 
**Вариант 16** · Бюджетная аналитика: финансовые показатели  
**Дата:** 13 февраля 2026  

---

## 1. Цель работы
Изучить принципы работы с Pentaho Data Integration: создать конвейер для очистки, преобразования и загрузки данных о розничных продажах из CSV в MySQL.

---

## 2. Описание входных данных
**Источник:** Kaggle — [Retail and warehouse Sale.csv](https://www.kaggle.com/datasets/abdullah0a/retail-sales-data-with-seasonal-trends-and-marketing)  
**Объём:** 15 000+ строк, 9 столбцов.

| Исходное поле в CSV | Тип данных | Описание |
|---------------------|------------|----------|
| YEAR                | Integer    | Год продажи (2020) |
| MONTH               | Integer    | Месяц (1–12) |
| SUPPLIER            | String     | Поставщик |
| ITEM CODE           | String     | Код товара |
| ITEM DESCRIPTION    | String     | Наименование |
| ITEM TYPE           | String     | Категория (Wine/Beer/Liquor) |
| RETAIL SALES        | Decimal    | Розничные продажи ($) |
| RETAIL TRANSFERS    | Decimal    | Переводы между магазинами |
| WAREHOUSE SALES     | Decimal    | Продажи со склада |

---

## 3. Созданный ETL-конвейер

### 3.1. Общая схема трансформации
![Общий вид конвейера](screenshots/1_pipeline_overview.png)

**Цепочка шагов:**

CSV File Input → Select values → Memory group by → Filter rows → Value mapper → Table output
                      ↓                                 ↓
              Dummy (do nothing)                   Write to log

              
### 3.2. Ключевые шаги трансформации

#### ✅ **CSV File Input**
- **Разделитель:** `;` (точка-запятая)
- **Кодировка:** UTF-8
- **Строка заголовка:** 1
- **Ленивое преобразование (Lazy conversion):** Включено  
![Настройки CSV Input](screenshots/2_csv_input.png)

#### ✅ **Select values — переименование и типы данных**
- Убраны пробелы из имён полей.
- Приведены к типам, соответствующим MySQL.
- Исправлены опечатки (например, `Projectld` → `ProjectId` — в предыдущей версии, в этом датасете аналогично).

| Исходное поле | Новое имя | Тип (Meta-data) | Формат |
|---------------|-----------|-----------------|--------|
| YEAR          | _year     | Integer         | —      |
| MONTH         | _month    | Integer         | —      |
| SUPPLIER      | _supplier | String(100)     | —      |
| ITEM CODE     | item_code | String(20)      | —      |
| ITEM DESCRIPTION | item_description | String(200) | — |
| ITEM TYPE     | item_type | String(20)      | —      |
| RETAIL SALES  | retail_sales | Number(10,2) | 0.00 |
| RETAIL TRANSFERS | retail_transfers | Number(10,2) | 0.00 |
| WAREHOUSE SALES | warehouse_sales | Number(10,2) | 0.00 |

![Select values — Fields](screenshots/3_select_values_fields.png)  
![Select values — Meta-data](screenshots/4_select_values_metadata.png)

#### ✅ **Filter rows — очистка от «битых» записей**
Отфильтрованы строки, где:
- `item_code` пустой или NULL;
- `retail_sales`, `retail_transfers`, `warehouse_sales` содержат некорректные значения (NULL).
  
![Настройки фильтра](screenshots/5_filter_rows.png)

#### ✅ **Value mapper — нормализация значений**
- Для поля `item_type` сокращения заменены на полные названия.

| Исходное значение | Целевое значение |
|-------------------|------------------|
| WINE              | Wine             |
| BEER              | Beer             |
| LIQUOR            | Liquor           |

- **Поле на выходе:** `item_type` (перезаписано).

![Настройки Value mapper](screenshots/6_value_mapper.png)

#### ✅ **Table output — загрузка в MySQL**
- **Хост:** `95.131.149.21`
- **Порт:** `3306`
- **База данных:** `mgpu_ico_etl_XX` (подставьте ваш номер)
- **Таблица:** `sales_data`
- **Способ:** Обычная вставка (без truncate, без создания таблицы — создана заранее через phpMyAdmin).
- **Сопоставление полей:** через кнопку **«Enter mapping»** (имена полей в потоке совпадают с колонками в БД).

![Настройки Table output](screenshots/7_table_output.png)
![Сопоставление полей](screenshots/8_mapping.png)

---

## 4. Результат загрузки данных

Трансформация обработала **15 000+ записей** без ошибок.  
Лог выполнения:

```
table orders.0 - Finished processing (I=0, O=0, R=15000, W=15000, U=0, E=0)
```

### Проверка в phpMyAdmin
1. **Количество записей:**
   ```sql
   SELECT COUNT(*) FROM sales_data;
   ```
   ![Результат COUNT](screenshots/9_sql_count.png)

2. **Пример данных:**
   ```sql
   SELECT * FROM sales_data LIMIT 10;
   ```
   ![Пример данных](screenshots/10_sql_select.png)

3. **Проверка типов и кодировки:**
   ```sql
   DESCRIBE sales_data;
   ```
   Все поля соответствуют заявленным типам, русский текст (названия поставщиков) отображается корректно.

---

## 5. Выводы
- Успешно настроено подключение к удалённой БД MySQL.
- Создан ETL-конвейер из 5 шагов, выполнены:
  - чтение CSV;
  - очистка и фильтрация;
  - нормализация значений;
  - загрузка в целевую таблицу.
- В БД загружено **15 000+ строк** без потери данных.
- Получены практические навыки работы с PDI Spoon.

**Бонус:** конвейер легко адаптируется под другие CSV-файлы со схожей структурой.

---

## 6. Файлы в репозитории
| Файл | Назначение |
|------|------------|
| `lab_01_retail_sales.ktr` | Файл трансформации Pentaho |
| `data/Retail_and_warehouse_Sale.csv` | Исходный датасет (ссылка на Kaggle) |
| `screenshots/` | Все скриншоты (10 шт.) |
| `README.md` | Данный отчёт |

**Ссылка на датасет:** [Kaggle — Retail Sales Data](https://www.kaggle.com/datasets/abdullah0a/retail-sales-data-with-seasonal-trends-and-marketing)
