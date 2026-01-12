import random
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
import hashlib
import time
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

faker = Faker(['ru_RU', 'en_US'])

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'online_shop_db'),
    'user': os.getenv('POSTGRES_USER', 'user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password'),
    'port': 5432
}

GENERATED_DATA_LENGTH = 100
        
def connect(config): 
    try:
        connection = psycopg2.connect(**config)
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        logger.info("успешное подключение к БД")
    except Exception as e:
        logger.error(f"ошибка подключения: {e}")
        
    return connection, cursor
            
def disconnect(connection, cursor):
    if cursor: 
        cursor.close()
    if connection:
        connection.close()
    
    logger.info("соединение c БД закрыто")
            
categories = [
    # category, parent_category_id
    ("Смартфоны", None),
    ("Ноутбуки", None),
    ("Телевизоры", None),
    ("Наушники", None),
    ("Игровые консоли", None),
    ("Холодильники", None),
    ("Стиральные машины", None),
    ("Пылесосы", None),
    ("Кухонная техника", None),
    ("Мужская одежда", None),
    ("Женская одежда", None),
    ("Детская одежда", None),
    ("Обувь", None),
    ("Мебель", None),
    ("Освещение", None),
    ("Текстиль", None),
    ("Инструменты", None),
    ("Android смартфоны", 1),
    ("iPhone", 1),
    ("Игровые ноутбуки", 2),
    ("Ультрабуки", 2),
    ("4K телевизоры", 3),
    ("Беспроводные наушники", 4)
]

def generate_categories(connection, cursor):
    global categories
    
    cursor.execute("SELECT COUNT(*) as count FROM categories")
    result = cursor.fetchone()
    if result and result['count'] > 0:
        logger.info("категории уже созданы") 
        return

    for name, parent_id in categories:
        cursor.execute(
            "INSERT INTO categories (name, parent_category_id) VALUES (%s, %s)",
            (name, parent_id)
        )
    
    connection.commit()
    logger.info(f"категории в количестве {len(categories)} успешно созданы")

def hash_password(password): 
    return hashlib.sha256(password.encode()).hexdigest()

def generate_products(connection, cursor, count=100):
    cursor.execute("SELECT COUNT(*) as count FROM products")
    result = cursor.fetchone()
    
    if result:
        count -= result["count"]
        if count <= 0:
            logger.info("указанное количество товаров (параметр count) уже существует в базе данных")
            return


    cursor.execute("SELECT id FROM categories")
    category_rows = cursor.fetchall()
    category_ids = [row['id'] for row in category_rows]
    
    if not category_ids:
        logger.warning("нет категорий для создания товаров")
        return
    for _ in range(count):
        name = f"{faker.word().title()} {faker.word().title()} {random.randint(100, 999)}"
        description = faker.text(max_nb_chars=100)
        price = round(random.uniform(50, 5000), 2)
    
        category_id = random.choice(category_ids)
        rating = round(random.uniform(3.0, 5.0), 2) if random.random() > 0.3 else round(random.uniform(2.0, 3.0), 2)
        
        cursor.execute(
            """INSERT INTO products (name, description, category_id, price, rating)
            VALUES (%s, %s, %s, %s, %s)""",
            (name, description, category_id, price, rating)
        )
    
    connection.commit()
    logger.info(f"cоздано {count} товаров")
    
def generate_user(connection, cursor):
    first_name = faker.first_name()
    last_name = faker.last_name()
    email = faker.unique.email()
    password = hash_password(faker.word())
    phone = faker.phone_number()[:20]
    registration_date = faker.date_time_between(start_date='-2y', end_date='now')

    cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
    existing_user = cursor.fetchone()
    
    if existing_user:
        return

    cursor.execute(
        """INSERT INTO users (email, first_name, last_name, password_hash, phone, 
        registration_date) VALUES (%s, %s, %s, %s, %s, %s)""",
        (email, first_name, last_name, password, phone, registration_date)
    )
        
    connection.commit()
    logger.info(f"создан пользователь {first_name} {last_name}")

def generate_order(connection, cursor):
    cursor.execute("SELECT id FROM users")
    user_ids = [row['id'] for row in cursor.fetchall()]
    
    if not user_ids:
        logger.info("нет активных пользователей для создания заказа")
        return
    
    cursor.execute("SELECT id, price FROM products")
    products = cursor.fetchall()
    
    if not products:
        logger.info("нет товаров в наличии для создания заказа")
        return
    
    user_id = random.choice(user_ids)
    
    product = random.choice(products)
    product_id = product['id']
    product_price = product['price']
    
    quantity = random.randint(1, 5)
    total_amount = round(product_price * quantity, 2)
    order_date = faker.date_time_between(start_date='-1y', end_date='now')
    
    cursor.execute(
        """INSERT INTO orders 
        (user_id, product_id, quantity, order_date, total_amount)
        VALUES (%s, %s, %s, %s, %s)""",
        (user_id, product_id, quantity, order_date, total_amount)
    )
    
    connection.commit()
    logger.info(f"Заказ: пользователь {user_id}, товар {product_id}, {quantity}шт, сумма {total_amount}")
    
    
def generate_wishlist_item(connection, cursor):
    cursor.execute("SELECT id FROM users")
    user_ids = [row['id'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM products")
    product_ids = [row['id'] for row in cursor.fetchall()]
    
    if not user_ids or not product_ids:
        logger.info("Недостаточно данных для генерации избранного")
        return
    
    user_id = random.choice(user_ids)
    product_id = random.choice(product_ids)
    
    cursor.execute(
        "SELECT id FROM wishlist WHERE user_id = %s AND product_id = %s",
        (user_id, product_id)
    )
    if cursor.fetchone():
        return
    
    added_at = faker.date_time_between(start_date='-60d', end_date='now')
    notes = faker.sentence() if random.random() > 0.7 else None
    
    cursor.execute(
        "INSERT INTO wishlist (user_id, product_id, added_at, notes) VALUES (%s, %s, %s, %s)",
        (user_id, product_id, added_at, notes)
    )

    connection.commit()
    logger.info(f"сгенерирован вишлист пользователя {user_id}")
    
    
conn, cur = connect(DB_CONFIG)

generate_categories(conn, cur)
generate_products(conn, cur)

for i in range(10):
    generate_user(conn, cur)

for i in range(GENERATED_DATA_LENGTH): 
    if not i % 17: 
        generate_user(conn, cur)
    if not i % 2: 
        generate_order(conn, cur)
    if not i % 5: 
        generate_wishlist_item(conn, cur)
    
    time.sleep(.1)