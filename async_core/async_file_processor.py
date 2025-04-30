import os
import re
import asyncio
import pandas as pd

from datetime import datetime
from config.settings import logger


class AsyncFileProcessor:
    def __init__(self):
        self.logger = logger.getChild('AsyncFileProcessor')

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

    async def process_file(self, file_path):
        """Асинхронно обрабатывает файл Excel и возвращает данные"""
        try:
            loop = asyncio.get_running_loop()

            def read_excel():
                return pd.read_excel(file_path, header=None)

            df = await loop.run_in_executor(None, read_excel)

            start_idx = self._find_data_start(df)
            df = df.iloc[start_idx + 1:].reset_index(drop=True)

            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)

            df.columns = [self._clean_column_name(str(col)) for col in df.columns]
            df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
            df = df.loc[:, ~df.columns.str.contains('nan', case=False, na=False)]

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

            df['количество договоров, шт.'] = pd.to_numeric(
                df['количество договоров, шт.'], errors='coerce').fillna(0)
            df = df[df['количество договоров, шт.'] > 0]

            string_columns = [
                'код инструмента',
                'наименование инструмента',
                'базис поставки'
            ]
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str)

            df = df.rename(columns={
                'код инструмента': 'exchange_product_id',
                'наименование инструмента': 'exchange_product_name',
                'базис поставки': 'delivery_basis_name',
                'объем договоров в единицах измерения': 'volume',
                'объем договоров, руб.': 'total',
                'количество договоров, шт.': 'count'
            })

            file_name = os.path.basename(file_path)
            df['oil_id'] = df['exchange_product_id'].str[:4]
            df['delivery_basis_id'] = df['exchange_product_id'].str[4:7]
            df['delivery_type_id'] = df['exchange_product_id'].str[-1]
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
            df['count'] = pd.to_numeric(df['count'], errors='coerce').fillna(0).astype(int)

            date_match = re.search(r'(\d{8})', file_name)
            if not date_match:
                raise ValueError(f"Не удалось извлечь дату из имени файла: {file_name}")
            df['date'] = datetime.strptime(date_match.group(1), '%Y%m%d').date()

            self.logger.info(f"Файл успешно обработан: {file_path}")
            return df

        except Exception as e:
            self.logger.error(f"Ошибка обработки файла {file_path}: {e}", exc_info=True)
            return None