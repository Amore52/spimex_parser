import os
import re
import pandas as pd

from datetime import datetime, date
from config.settings import logger



class FileProcessor:
    def __init__(self):
        self.logger = logger.getChild('FileProcessor')

    @staticmethod
    def _clean_column_name(col):
        """Очищает название столбца"""
        column_map = {
            'обьем': 'объем',
            'предыдуего': 'предыдущего'
        }
        if not isinstance(col, str):
            return col

        col = col.replace('\n', ' ').strip().lower()
        parts = col.split()
        if len(parts) > 1:
            first_word = column_map.get(parts[0], parts[0])
            return ' '.join([first_word] + parts[1:])
        return column_map.get(col, col)

    def _find_data_start(self, df):
        """Находит начало данных в DataFrame"""
        for i, row in df.iterrows():
            if any(isinstance(cell, str) and 'Метрическая тонна' in str(cell) for cell in row):
                return i
        raise ValueError("Не найдена строка с 'Метрическая тонна'")

    def process_file(self, file_path):
        """Обрабатывает файл Excel и возвращает данные"""
        try:
            # Чтение файла
            df = pd.read_excel(file_path, header=None)

            # Поиск начала данных
            start_idx = self._find_data_start(df)
            df = df.iloc[start_idx + 1:].reset_index(drop=True)

            # Установка заголовков
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)

            # Очистка столбцов
            df.columns = [self._clean_column_name(str(col)) for col in df.columns]
            df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
            df = df.loc[:, ~df.columns.str.contains('nan', case=False, na=False)]

            # Проверка обязательных колонок
            required_columns = [
                'код инструмента',
                'наименование инструмента',
                'базис поставки',
                'объем договоров в единицах измерения',
                'объем договоров, руб.',
                'количество договоров, шт.'
            ]

            missing = set(required_columns) - set(df.columns)
            if missing:
                raise ValueError(f"Отсутствуют столбцы: {missing}")

            # Преобразование данных
            df['количество договоров, шт.'] = pd.to_numeric(
                df['количество договоров, шт.'], errors='coerce')
            df = df[df['количество договоров, шт.'] > 0]

            # Переименование колонок
            df = df.rename(columns={
                'код инструмента': 'exchange_product_id',
                'наименование инструмента': 'exchange_product_name',
                'базис поставки': 'delivery_basis_name',
                'объем договоров в единицах измерения': 'volume',
                'объем договоров, руб.': 'total',
                'количество договоров, шт.': 'count'
            })

            # Добавление вычисляемых полей
            file_name = os.path.basename(file_path)
            df['oil_id'] = df['exchange_product_id'].str[:4]
            df['delivery_basis_id'] = df['exchange_product_id'].str[4:7]
            df['delivery_type_id'] = df['exchange_product_id'].str[-1]

            # Парсинг даты из имени файла
            date_match = re.search(r'(\d{8})', file_name)
            if not date_match:
                raise ValueError(f"Не удалось извлечь дату из имени файла: {file_name}")
            df['date'] = datetime.strptime(date_match.group(1), '%Y%m%d').date()

            self.logger.info(f"Файл успешно обработан: {file_path}")
            return df

        except Exception as e:
            self.logger.error(f"Ошибка обработки файла {file_path}: {e}", exc_info=True)
            return None