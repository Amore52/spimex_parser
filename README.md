# Spimex Parser
Проект для парсинга данных о торгах нефтепродуктами с сайта Spimex.com и сохранения их в базу данных PostgreSQL.
## Особенности
* Два варианта реализации: синхронный и асинхронный
* Загрузка Excel-файлов с данными торгов
* Обработка и очистка данных
* Сохранение в PostgreSQL с проверкой уникальности
* Логирование всех операций
## Настройка
### 1. Клонируйте репозиторий 
```
git clone https://github.com/amore_52/spimex_parser.git
```
### 2. Создайте виртуальное окружение:
```
python -m venv .venv
```
### 3. Активируйте виртуальное окружение:

* Для Windows:
    ```
    .venv\Scripts\activate

* Для Linux:
    ```
  source .venv/bin/activate
### 4. Установка зависимостей:
```
pip install -r requirements.txt
```
### 5. Настройка переменных окружения:  
Создайте файл .env в корне проекта и заполните его по образцу:

Синхронный вариант:  
`DB_NAME` - имя базы данных  
`DB_USER` - пользователь базы данных  
`DB_PASSWORD` - пароль  
`DB_HOST` - localhost  
`DB_PORT` - порт  

Асинхронный вариант:  
`ASYNC_DB_DATABASE` - имя базы данных  
`ASYNC_DB_USER` - пользователь базы данных  
`ASYNC_DB_PASSWORD` - пароль  
`ASYNC_DB_HOST` - localhost  
`ASYNC_DB_PORT` - порт  

### 6. Запуск проекта
Синхронная версия:  
```
python main.py
```
Асинхронная версия:
```
python async_main.py
```
## Важное
В файле `settings.py` лежат настройки парсера:
```
PARSER_CONFIG = {
    'base_url': "https://spimex.com/markets/oil_products/trades/results/",
    'download_dir': os.path.join(BASE_DIR, "downloads"),
    'start_date': datetime(2025, 3, 1),
    'end_date': datetime.now()
}
```
Где ключ `start_date` указывает с какой даты скачивать .xls документы.