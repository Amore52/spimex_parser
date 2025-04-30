import time
import os

from core.parser import SpimexParser
from core.database import DatabaseManager
from core.file_processor import FileProcessor
from config.settings import logger, PARSER_CONFIG


def main():
    start_time = time.time()
    logger.info("Запуск приложения spimex_parser")
    try:
        logger.info("Этап 1/2: Загрузка файлов с Spimex")
        parser = SpimexParser()
        parser.run()

        logger.info("Этап 2/2: Обработка файлов и загрузка в БД")
        file_processor = FileProcessor()

        with DatabaseManager() as db:
            db.create_table()

            for file_name in os.listdir(PARSER_CONFIG['download_dir']):
                if file_name.endswith('.xls'):
                    file_path = os.path.join(PARSER_CONFIG['download_dir'], file_name)
                    logger.info(f"Обработка файла: {file_name}")

                    df = file_processor.process_file(file_path)
                    if df is not None and not df.empty:
                        data = [(
                            row['exchange_product_id'],
                            row['exchange_product_name'],
                            row['oil_id'],
                            row['delivery_basis_id'],
                            row['delivery_basis_name'],
                            row['delivery_type_id'],
                            row['volume'],
                            row['total'],
                            row['count'],
                            row['date']
                        ) for _, row in df.iterrows()]

                        db.insert_data(data)

        logger.info(f"Время выполнения синхронного кода: {time.time() - start_time}")
    except Exception as e:
        logger.critical(f"Критическая ошибка в приложении: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()