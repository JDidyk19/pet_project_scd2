CREATE TABLE IF NOT EXISTS scd2_dimensional_users (
    id SERIAL PRIMARY KEY,
    user_id UUID,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    birthday DATE,
    email TEXT NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ts_db TIMESTAMP,
    actual_from TIMESTAMP,
    actual_to TIMESTAMP,
    is_actual BOOLEAN
)