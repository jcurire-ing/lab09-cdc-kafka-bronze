CREATE TABLE sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE sales REPLICA IDENTITY FULL;

INSERT INTO sales (customer_id, product_id, quantity, unit_price) VALUES
(501, 101, 2, 1500.00),
(502, 102, 1, 120.50);