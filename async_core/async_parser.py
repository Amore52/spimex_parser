import os
import aiohttp
import asyncio

from bs4 import BeautifulSoup
from datetime import datetime, date
from urllib.parse import urljoin
from config.settings import logger, PARSER_CONFIG


class AsyncSpimexParser:
    def __init__(self, config=None):
        self.config = config or PARSER_CONFIG
        self.config['start_date'] = self._ensure_date(self.config['start_date'])
        self.config['end_date'] = self._ensure_date(self.config['end_date'])

        os.makedirs(self.config['download_dir'], exist_ok=True)
        self.logger = logger.getChild('AsyncSpimexParser')
        self._should_stop = False
        self.session = None

    def _ensure_date(self, dt):
        """Приводит дату к типу datetime.date"""
        if isinstance(dt, datetime):
            return dt.date()
        elif isinstance(dt, date):
            return dt
        raise ValueError(f"Неподдерживаемый тип даты: {type(dt)}")

    async def get_total_pages(self):
        """Получает общее количество страниц асинхронно"""
        try:
            async with self.session.get(self.config['base_url'], timeout=10) as response:
                response.raise_for_status()
                text = await response.text()

                soup = BeautifulSoup(text, 'html.parser')
                pagination = soup.find('div', class_='bx-pagination')

                if pagination:
                    last_page = pagination.find_all('li')[-2].find('a')
                    total_pages = int(last_page.find('span').text.strip())
                    self.logger.debug(f"Найдено страниц: {total_pages}")
                    return total_pages

                self.logger.debug("Пагинация не найдена, предполагаем 1 страницу")
                return 1
        except Exception as e:
            self.logger.error(f"Ошибка получения количества страниц: {e}", exc_info=True)
            return 1

    def parse_date_from_filename(self, filename):
        """Извлекает дату из имени файла -> datetime.date"""
        try:
            import re
            match = re.search(r'(\d{8})', filename)
            if not match:
                raise ValueError("No date found in filename")

            date_str = match.group(1)
            return datetime.strptime(date_str, "%Y%m%d").date()

        except Exception as e:
            self.logger.warning(f"Ошибка извлечения даты из {filename}: {str(e)}")
            return None

    async def download_file(self, url):
        """Скачивает файл асинхронно, возвращает False для остановки"""
        try:
            file_name = os.path.join(self.config['download_dir'], url.split('/')[-1])
            file_date = self.parse_date_from_filename(file_name)

            if file_date < self.config['start_date']:
                self.logger.info(f"Найдена дата {file_date} < start_date {self.config['start_date']}. Остановка.")
                self._should_stop = True
                return False

            if os.path.exists(file_name):
                self.logger.debug(f"Файл существует: {file_name}")
                return True

            async with self.session.get(url, timeout=10) as response:
                response.raise_for_status()
                content = await response.read()

                with open(file_name, 'wb') as f:
                    f.write(content)

            self.logger.info(f"Скачан файл: {file_name}")
            return True

        except aiohttp.ClientError as e:
            self.logger.error(f"Ошибка сети: {str(e)}")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка загрузки: {str(e)}")
            return True

    async def parse_page(self, page_url):
        """Парсит страницу асинхронно, возвращает список URL файлов"""
        if self._should_stop:
            return []

        try:
            async with self.session.get(page_url, timeout=10) as response:
                response.raise_for_status()
                text = await response.text()

                soup = BeautifulSoup(text, 'html.parser')
                files = []

                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('/upload/reports/oil_xls/oil_xls_'):
                        full_url = urljoin("https://spimex.com", href.split('?')[0])
                        files.append(full_url)
                        self.logger.debug(f"Найдена ссылка: {full_url}")

                return files

        except Exception as e:
            self.logger.error(f"Ошибка парсинга страницы: {str(e)}")
            return []

    async def run(self):
        """Основной асинхронный метод запуска парсера"""
        try:
            async with aiohttp.ClientSession() as self.session:
                total_pages = await self.get_total_pages()
                if total_pages == 0:
                    self.logger.warning("Нет страниц для обработки")
                    return False

                tasks = []
                for page in range(1, total_pages + 1):
                    if self._should_stop:
                        break

                    self.logger.info(f"Страница {page}/{total_pages}")
                    page_url = f"{self.config['base_url']}?page=page-{page}"

                    file_urls = await self.parse_page(page_url)
                    if not file_urls:
                        self.logger.debug("Нет файлов на странице")
                        continue

                    for file_url in file_urls:
                        if self._should_stop:
                            break
                        tasks.append(asyncio.create_task(self.download_file(file_url)))
                await asyncio.gather(*tasks)

            return not self._should_stop

        except Exception as e:
            self.logger.critical(f"Критическая ошибка: {str(e)}", exc_info=True)
            return False