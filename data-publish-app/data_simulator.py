import time

import psycopg2
from faker import Faker

"""
CREATE TABLE customers
(
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(50)         NOT NULL,
    last_name   VARCHAR(50)         NOT NULL,
    email       VARCHAR(100) UNIQUE NOT NULL,
    phone       VARCHAR(20),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders
(
    order_id     SERIAL PRIMARY KEY,
    customer_id  INTEGER        NOT NULL,
    order_date   TIMESTAMP               DEFAULT CURRENT_TIMESTAMP,
    created_at   TIMESTAMP               DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP               DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL DEFAULT 0,
    status       VARCHAR(20)    NOT NULL DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

alter table orders
    add column is_cod boolean;

CREATE TABLE order_items
(
    item_id      SERIAL PRIMARY KEY,
    order_id     INTEGER        NOT NULL,
    product_name VARCHAR(100)   NOT NULL,
    quantity     INTEGER        NOT NULL,
    unit_price   DECIMAL(10, 2) NOT NULL,
    subtotal     DECIMAL(10, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (order_id)
);
"""
fake = Faker()


def generate_customers(num_customers=50):
    customers = []
    for i in range(num_customers):
        customer = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.unique.phone_number()[:10],
            "created_at": fake.date_time_between(start_date="-2d", end_date="now"),
            "updated_at": fake.date_time_between(start_date="-1d", end_date="now"),
        }
        customers.append(customer)
    return customers


def insert_customers_to_db(customers, connection_params=None):
    if connection_params is None:
        connection_params = {
            "host": "ods-postgres",
            "database": "ods",
            "user": "ods",
            "password": "ods",
            "port": 5432,
        }

    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    try:
        # Insert customers
        for customer in customers:
            cursor.execute(
                """
                INSERT INTO customers
                (first_name, last_name, email, phone, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (
                    customer["first_name"],
                    customer["last_name"],
                    customer["email"],
                    customer["phone"],
                    customer["created_at"],
                    customer["updated_at"],
                ),
            )

        conn.commit()
        print(f"Inserted {len(customers)} customers into database", flush=True)

    except Exception as e:
        conn.rollback()
        print(f"Error inserting customers: {e}", flush=True)
    finally:
        cursor.close()
        conn.close()


def generate_orders_and_items(connection_params=None):
    if connection_params is None:
        connection_params = {
            "host": "ods-postgres",
            "database": "ods",
            "user": "ods",
            "password": "ods",
            "port": 5432,
        }

    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    try:
        # Select 25 random customers
        cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 25")
        customer_ids = [row[0] for row in cursor.fetchall()]

        orders_inserted = 0
        items_inserted = 0

        for customer_id in customer_ids:
            # Generate 1-3 orders per customer
            num_orders = fake.random_int(min=1, max=3)

            for _ in range(num_orders):
                # Create order
                order_date = fake.date_time_between(start_date="-2h", end_date="now")
                status = fake.random_element(
                    elements=(
                        "pending",
                        "processing",
                        "shipped",
                        "delivered",
                        "cancelled",
                    )
                )
                is_cod = fake.boolean()

                cursor.execute(
                    """
                    INSERT INTO orders (customer_id, order_date, status, is_cod, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING order_id
                    """,
                    (
                        customer_id,
                        order_date,
                        status,
                        is_cod,
                        order_date,
                        order_date,
                    ),
                )
                order_id = cursor.fetchone()[0]
                orders_inserted += 1

                # Generate 1-5 order items per order
                num_items = fake.random_int(min=1, max=5)
                total_amount = 0

                for _ in range(num_items):
                    product_name = fake.word().title() + " " + fake.word().title()
                    quantity = fake.random_int(min=1, max=10)
                    unit_price = round(fake.random.uniform(5.0, 500.0), 2)

                    cursor.execute(
                        """
                        INSERT INTO order_items (order_id, product_name, quantity, unit_price, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (order_id, product_name, quantity, unit_price, order_date),
                    )
                    total_amount += quantity * unit_price
                    items_inserted += 1

                # Update order total_amount
                cursor.execute(
                    "UPDATE orders SET total_amount = %s WHERE order_id = %s",
                    (round(total_amount, 2), order_id),
                )

        conn.commit()
        print(
            f"Inserted {orders_inserted} orders and {items_inserted} order items",
            flush=True,
        )

    except Exception as e:
        conn.rollback()
        print(f"Error inserting orders and items: {e}", flush=True)
    finally:
        cursor.close()
        conn.close()


def update_random_order_statuses(num_orders=10, connection_params=None):
    if connection_params is None:
        connection_params = {
            "host": "ods-postgres",
            "database": "ods",
            "user": "ods",
            "password": "ods",
            "port": 5432,
        }

    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    try:
        # Select random orders to update
        cursor.execute(
            "SELECT order_id FROM orders ORDER BY RANDOM() LIMIT %s", (num_orders,)
        )
        order_ids = [row[0] for row in cursor.fetchall()]

        updated_orders = 0
        statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]

        for order_id in order_ids:
            new_status = fake.random_element(elements=statuses)
            updated_at = fake.date_time_between(start_date="-1h", end_date="now")

            cursor.execute(
                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                (new_status, updated_at, order_id),
            )
            updated_orders += 1

        conn.commit()
        print(f"Updated status for {updated_orders} random orders", flush=True)

    except Exception as e:
        conn.rollback()
        print(f"Error updating order statuses: {e}", flush=True)
    finally:
        cursor.close()
        conn.close()


def update_random_order_items(num_orders=10, connection_params=None):
    if connection_params is None:
        connection_params = {
            "host": "ods-postgres",
            "database": "ods",
            "user": "ods",
            "password": "ods",
            "port": 5432,
        }

    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    try:
        # Select random orders to update
        cursor.execute(
            "SELECT order_id FROM orders ORDER BY RANDOM() LIMIT %s", (num_orders,)
        )
        order_ids = [row[0] for row in cursor.fetchall()]

        updated_orders = 0
        total_items_updated = 0

        for order_id in order_ids:
            # Decide whether to add new items or update existing ones
            action = fake.random_element(elements=("add", "update"))

            if action == "add":
                # Add 1-3 new items to the order
                num_new_items = fake.random_int(min=1, max=3)

                for _ in range(num_new_items):
                    product_name = fake.word().title() + " " + fake.word().title()
                    quantity = fake.random_int(min=1, max=10)
                    unit_price = round(fake.random.uniform(5.0, 500.0), 2)
                    created_at = fake.date_time_between(
                        start_date="-2h", end_date="now"
                    )

                    cursor.execute(
                        """
                        INSERT INTO order_items (order_id, product_name, quantity, unit_price, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (order_id, product_name, quantity, unit_price, created_at),
                    )
                    total_items_updated += 1

            else:  # update existing items
                # Get existing items for this order
                cursor.execute(
                    "SELECT item_id FROM order_items WHERE order_id = %s ORDER BY RANDOM() LIMIT 2",
                    (order_id,),
                )
                item_ids = [row[0] for row in cursor.fetchall()]

                for item_id in item_ids:
                    # Update quantity and/or unit price
                    new_quantity = fake.random_int(min=1, max=10)
                    new_unit_price = round(fake.random.uniform(5.0, 500.0), 2)

                    cursor.execute(
                        "UPDATE order_items SET quantity = %s, unit_price = %s WHERE item_id = %s",
                        (new_quantity, new_unit_price, item_id),
                    )
                    total_items_updated += 1

            # Recalculate and update order total_amount
            cursor.execute(
                """
                SELECT SUM(quantity * unit_price) 
                FROM order_items 
                WHERE order_id = %s
                """,
                (order_id,),
            )
            new_total = cursor.fetchone()[0] or 0

            cursor.execute(
                "UPDATE orders SET total_amount = %s, updated_at = %s WHERE order_id = %s",
                (
                    round(new_total, 2),
                    fake.date_time_between(start_date="-1d", end_date="now"),
                    order_id,
                ),
            )
            updated_orders += 1

        conn.commit()
        print(
            f"Updated order items for {updated_orders} orders, total items affected: {total_items_updated}",
            flush=True,
        )

    except Exception as e:
        conn.rollback()
        print(f"Error updating order items: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # Generate 50 random customers
    # customers_data = generate_customers(50)

    # Insert customers into database
    # insert_customers_to_db(customers_data)

    # Generate and insert orders and items
    # Randomly select and call one of the three methods

    while True:
        print("Starting random data operation", flush=True)
        generate_orders_and_items()
        update_random_order_statuses(10)
        update_random_order_items(10)
        time.sleep(180)  # Sleep for 5 minutes (300 seconds)
