import os
import asyncio
import time

from async_core.async_database import AsyncDatabaseManager
from config.settings import logger, PARSER_CONFIG
from async_core.async_parser import AsyncSpimexParser
from async_core.async_file_processor import AsyncFileProcessor


async def process_single_file(file_processor, db, file_path):
    """Обрабатывает один файл и загружает данные в БД"""
    try:
        logger.info(f"Обработка файла: {os.path.basename(file_path)}")

        df = await file_processor.process_file(file_path)
        if df is not None and not df.empty:
            data = []
            for _, row in df.iterrows():
                try:
                    data.append((
                        str(row['exchange_product_id']),
                        str(row['exchange_product_name']),
                        str(row['oil_id']),
                        str(row['delivery_basis_id']),
                        str(row['delivery_basis_name']),
                        str(row['delivery_type_id']),
                        float(row['volume']),
                        float(row['total']),
                        int(row['count']),
                        row['date']
                    ))
                except Exception as e:
                    logger.error(f"Ошибка обработки строки: {row.to_dict()} - {str(e)}")
                    continue

            if data:
                await db.insert_data(data)

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {file_path}: {e}", exc_info=True)


async def async_main():
    start_time = time.time()
    logger.info("Запуск асинхронного приложения spimex_parser")
    try:
        logger.info("Этап 1/2: Загрузка файлов с Spimex")
        parser = AsyncSpimexParser()
        await parser.run()

        logger.info("Этап 2/2: Обработка файлов и загрузка в БД")
        file_processor = AsyncFileProcessor()
        db = await AsyncDatabaseManager().connect()

        await db.create_table()

        files = [f for f in os.listdir(PARSER_CONFIG['download_dir']) if f.endswith('.xls')]
        sem = asyncio.Semaphore(5)

        async def process_with_semaphore(file_path):
            async with sem:
                await process_single_file(file_processor, db, file_path)

        tasks = [process_with_semaphore(os.path.join(PARSER_CONFIG['download_dir'], f))
                 for f in files]

        await asyncio.gather(*tasks)

        await db.close()
        logger.info(f"Время выполнения асинхронного кода: {time.time() - start_time}")
    except Exception as e:
        logger.critical(f"Критическая ошибка в приложении: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(async_main())