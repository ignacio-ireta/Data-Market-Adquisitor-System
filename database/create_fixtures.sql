CREATE TABLE IF NOT EXISTS half_hour(
	price_id SERIAL PRIMARY KEY,
	open_price DECIMAL,
	high_price DECIMAL,
	low_price DECIMAL,
	close_price DECIMAL,
	date_price TIMESTAMP,
	volume DECIMAL
);
CREATE TABLE IF NOT EXISTS four_hours(
        price_id SERIAL PRIMARY KEY,
        open_price DECIMAL,
        high_price DECIMAL,
        low_price DECIMAL,
        close_price DECIMAL,
        date_price TIMESTAMP,
        volume DECIMAL
);
CREATE TABLE IF NOT EXISTS four_days(
        price_id SERIAL PRIMARY KEY,
        open_price DECIMAL,
        high_price DECIMAL,
        low_price DECIMAL,
        close_price DECIMAL,
        date_price TIMESTAMP,
        volume DECIMAL
);
