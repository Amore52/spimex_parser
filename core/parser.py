import os
import requests

from bs4 import BeautifulSoup
from datetime import datetime, date
from urllib.parse import urljoin
from config.settings import logger, PARSER_CONFIG



class SpimexParser:
    def __init__(self, config=None):
        self.config = config or PARSER_CONFIG
        self.config['start_date'] = self._ensure_date(self.config['start_date'])
        self.config['end_date'] = self._ensure_date(self.config['end_date'])

        os.makedirs(self.config['download_dir'], exist_ok=True)
        self.logger = logger.getChild('SpimexParser')
        self._should_stop = False

    def _ensure_date(self, dt):
        """Приводит дату к типу datetime.date"""
        if isinstance(dt, datetime):
            return dt.date()
        elif isinstance(dt, date):
            return dt
        raise ValueError(f"Неподдерживаемый тип даты: {type(dt)}")

    def get_total_pages(self):
        """Получает общее количество страниц"""
        try:
            response = requests.get(self.config['base_url'], timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
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

    def download_file(self, url):
        """Скачивает файл, возвращает False для остановки"""
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

            response = requests.get(url, timeout=10)
            response.raise_for_status()

            with open(file_name, 'wb') as f:
                f.write(response.content)

            self.logger.info(f"Скачан файл: {file_name}")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Ошибка сети: {str(e)}")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка загрузки: {str(e)}")
            return True

    def parse_page(self, page_url):
        """Парсит страницу, возвращает список URL файлов"""
        if self._should_stop:
            return []

        try:
            response = requests.get(page_url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
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

    def run(self):
        """Основной метод запуска парсера"""
        try:
            self.logger.info(f"Старт парсера. Диапазон: {self.config['start_date']} - {self.config['end_date']}")

            total_pages = self.get_total_pages()
            if total_pages == 0:
                self.logger.warning("Нет страниц для обработки")
                return False

            for page in range(1, total_pages + 1):
                if self._should_stop:
                    break

                self.logger.info(f"Страница {page}/{total_pages}")
                page_url = f"{self.config['base_url']}?page=page-{page}"

                file_urls = self.parse_page(page_url)
                if not file_urls:
                    self.logger.debug("Нет файлов на странице")
                    continue

                for file_url in file_urls:
                    if not self.download_file(file_url):
                        break

            return not self._should_stop

        except Exception as e:
            self.logger.critical(f"Критическая ошибка: {str(e)}", exc_info=True)
            return False