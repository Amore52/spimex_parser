import psycopg2
from config import settings



class DatabaseManager:
    def __init__(self, config=None):
        self.config = config or settings.DB_CONFIG
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Устанавливает соединение с базой данных"""
        self.connection = psycopg2.connect(**self.config)
        self.cursor = self.connection.cursor()

    def close(self):
        """Закрывает соединение с базой данных"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def create_table(self):
        """Создает таблицу если она не существует"""
        self.cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'spimex_trading_results'
            );
        """)

        if not self.cursor.fetchone()[0]:
            self.cursor.execute("""
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
            """)

            self.cursor.execute("""
                ALTER TABLE spimex_trading_results
                ADD CONSTRAINT unique_exchange_product_id_date 
                UNIQUE (exchange_product_id, date);

                CREATE INDEX idx_spimex_date ON spimex_trading_results (date);
                CREATE INDEX idx_spimex_product_id ON spimex_trading_results (exchange_product_id);
            """)
            self.connection.commit()

    def insert_data(self, data):
        """Вставляет данные в таблицу"""
        query = """
            INSERT INTO spimex_trading_results (
                exchange_product_id, exchange_product_name, oil_id, 
                delivery_basis_id, delivery_basis_name, delivery_type_id,
                volume, total, count, date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        self.cursor.executemany(query, data)
        self.connection.commit()