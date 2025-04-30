import asyncpg
from config.settings import logger, ASYNC_DB_CONFIG



class AsyncDatabaseManager:
    def __init__(self, config=None):
        self.config = config or ASYNC_DB_CONFIG
        self.pool = None
        self.logger = logger.getChild('AsyncDatabaseManager')

    async def connect(self):
        """Устанавливает соединение с базой данных"""
        self.pool = await asyncpg.create_pool(**self.config)
        return self

    async def close(self):
        """Закрывает соединение с базой данных"""
        if self.pool:
            await self.pool.close()

    async def create_table(self):
        """Создает таблицу если она не существует"""
        async with self.pool.acquire() as conn:
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'spimex_trading_results'
                );
            """)

            if not table_exists:
                await conn.execute("""
                    CREATE TABLE spimex_trading_results (
                        id SERIAL PRIMARY KEY,
                        exchange_product_id VARCHAR(20),
                        exchange_product_name TEXT,
                        oil_id VARCHAR(4),
                        delivery_basis_id VARCHAR(3),
                        delivery_basis_name TEXT,
                        delivery_type_id VARCHAR(1),
                        volume NUMERIC(15, 2),
                        total NUMERIC(15, 2),
                        count INTEGER,
                        date DATE,
                        created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    ALTER TABLE spimex_trading_results
                    ADD CONSTRAINT unique_exchange_product_id_date 
                    UNIQUE (exchange_product_id, date);

                    CREATE INDEX idx_spimex_date ON spimex_trading_results (date);
                    CREATE INDEX idx_spimex_product_id ON spimex_trading_results (exchange_product_id);
                """)

    async def insert_data(self, data):
        """Вставляет данные в таблицу"""
        query = """
            INSERT INTO spimex_trading_results (
                exchange_product_id, exchange_product_name, oil_id, 
                delivery_basis_id, delivery_basis_name, delivery_type_id,
                volume, total, count, date
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (exchange_product_id, date) 
            DO UPDATE SET
                exchange_product_name = EXCLUDED.exchange_product_name,
                oil_id = EXCLUDED.oil_id,
                delivery_basis_id = EXCLUDED.delivery_basis_id,
                delivery_basis_name = EXCLUDED.delivery_basis_name,
                delivery_type_id = EXCLUDED.delivery_type_id,
                volume = EXCLUDED.volume,
                total = EXCLUDED.total,
                count = EXCLUDED.count,
                updated_on = CURRENT_TIMESTAMP
        """
        async with self.pool.acquire() as conn:
            await conn.executemany(query, data)