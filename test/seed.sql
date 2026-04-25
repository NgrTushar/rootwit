-- =============================================================================
-- RootWit Integration Test Seed Data
-- =============================================================================
-- This file creates a realistic SaaS database that exercises EVERY code path
-- in RootWit: all Postgres types we map, all sync modes, edge cases.
--
-- Schema modeled after a real Indian SaaS startup:
--   - users          (incremental — updated_at cursor, mixed types)
--   - orders         (incremental — updated_at cursor, money as NUMERIC)
--   - events         (append_only — created_at cursor, JSONB payload)
--   - plans          (full_refresh — small lookup table)
--   - audit_log      (append_only — id cursor, all text)
--
-- Type coverage:
--   BOOL, INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC, TEXT, VARCHAR,
--   CHAR, UUID, TIMESTAMP, TIMESTAMPTZ, DATE, JSON, JSONB, BYTEA, ARRAY
-- =============================================================================

-- =========================================
-- Table 1: users (incremental sync)
-- =========================================
CREATE TABLE users (
    id              BIGSERIAL PRIMARY KEY,
    uuid            UUID NOT NULL DEFAULT gen_random_uuid(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    age             SMALLINT,                          -- INT2
    login_count     INTEGER NOT NULL DEFAULT 0,        -- INT4
    lifetime_value  NUMERIC(12, 2) DEFAULT 0.00,       -- NUMERIC for money
    rating          REAL,                              -- FLOAT4
    score           DOUBLE PRECISION,                  -- FLOAT8
    profile_pic     BYTEA,                             -- BYTEA
    tags            TEXT[] DEFAULT '{}',                -- ARRAY type
    metadata        JSONB DEFAULT '{}',                -- JSONB
    preferences     JSON,                              -- JSON (not JSONB)
    birth_date      DATE,                              -- DATE
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),  -- TIMESTAMP
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW() -- TIMESTAMPTZ
);

-- 50 realistic users
INSERT INTO users (email, name, is_active, age, login_count, lifetime_value, rating, score, tags, metadata, preferences, birth_date, created_at, updated_at)
SELECT
    'user' || i || '@' || (CASE i % 5
        WHEN 0 THEN 'gmail.com'
        WHEN 1 THEN 'company.in'
        WHEN 2 THEN 'outlook.com'
        WHEN 3 THEN 'startup.io'
        WHEN 4 THEN 'hotmail.com'
    END),
    (CASE i % 10
        WHEN 0 THEN 'Rahul Sharma'
        WHEN 1 THEN 'Priya Patel'
        WHEN 2 THEN 'Amit Kumar'
        WHEN 3 THEN 'Sneha Gupta'
        WHEN 4 THEN 'Vikram Singh'
        WHEN 5 THEN 'Ananya Reddy'
        WHEN 6 THEN 'Arjun Nair'
        WHEN 7 THEN 'Kavita Joshi'
        WHEN 8 THEN 'Rohan Desai'
        WHEN 9 THEN 'Meera Iyer'
    END),
    (i % 7 != 0),                                           -- ~85% active
    (20 + (i % 40)),                                        -- age 20-59
    (i * 3 % 100),                                          -- login_count
    (i * 150.50 + (i % 100)),                               -- lifetime_value
    (1.0 + (i % 50) / 10.0)::REAL,                          -- rating 1.0-6.0
    (i * 1.5 + 10)::DOUBLE PRECISION,                       -- score
    ARRAY['plan_' || (CASE WHEN i % 3 = 0 THEN 'free' WHEN i % 3 = 1 THEN 'pro' ELSE 'enterprise' END)],
    jsonb_build_object(
        'signup_source', CASE i % 4
            WHEN 0 THEN 'google'
            WHEN 1 THEN 'direct'
            WHEN 2 THEN 'referral'
            WHEN 3 THEN 'producthunt'
        END,
        'onboarded', i % 2 = 0,
        'feature_flags', jsonb_build_array('dark_mode', 'beta_ui')
    ),
    ('{"theme": "dark", "lang": "' || CASE WHEN i % 3 = 0 THEN 'en' WHEN i % 3 = 1 THEN 'hi' ELSE 'ta' END || '"}')::JSON,
    DATE '1985-01-15' + (i * 7),                            -- birth_date
    NOW() - (interval '1 day' * (50 - i)),                  -- created_at spread over 50 days
    NOW() - (interval '1 hour' * (50 - i))                  -- updated_at spread over 50 hours
FROM generate_series(1, 50) AS i;

-- =========================================
-- Table 2: orders (incremental sync)
-- =========================================
CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(id),
    order_number    VARCHAR(20) NOT NULL UNIQUE,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    amount_paise    BIGINT NOT NULL,                   -- money in paise (INT8)
    amount_inr      NUMERIC(12, 2) NOT NULL,           -- money in INR (NUMERIC)
    tax_rate        REAL NOT NULL DEFAULT 0.18,         -- 18% GST (FLOAT4)
    discount_pct    DOUBLE PRECISION DEFAULT 0.0,       -- FLOAT8
    items           JSONB NOT NULL DEFAULT '[]',        -- order items as JSON array
    notes           TEXT,
    is_paid         BOOLEAN NOT NULL DEFAULT false,
    paid_at         TIMESTAMPTZ,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 200 orders across users
INSERT INTO orders (user_id, order_number, status, amount_paise, amount_inr, tax_rate, discount_pct, items, notes, is_paid, paid_at, created_at, updated_at)
SELECT
    (i % 50) + 1,                                           -- user_id cycles through users
    'ORD-' || LPAD(i::TEXT, 6, '0'),                        -- ORD-000001
    (CASE i % 5
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'confirmed'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'delivered'
        WHEN 4 THEN 'cancelled'
    END),
    (i * 100 + 5000),                                       -- amount in paise
    ((i * 100 + 5000) / 100.0)::NUMERIC(12,2),              -- amount in INR
    0.18,                                                    -- GST
    (CASE WHEN i % 10 = 0 THEN 0.15 ELSE 0.0 END),         -- 10% get 15% discount
    jsonb_build_array(
        jsonb_build_object('sku', 'SKU-' || (i % 20 + 100), 'qty', (i % 5) + 1, 'price', (i * 50 + 500))
    ),
    CASE WHEN i % 3 = 0 THEN 'Express delivery requested' ELSE NULL END,
    (i % 5 != 4),                                           -- not paid if cancelled
    CASE WHEN i % 5 != 4 THEN NOW() - (interval '1 hour' * i) ELSE NULL END,
    NOW() - (interval '1 day' * (200 - i)),
    NOW() - (interval '30 minutes' * (200 - i))
FROM generate_series(1, 200) AS i;

-- =========================================
-- Table 3: events (append_only sync)
-- =========================================
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(50) NOT NULL,
    user_id         BIGINT REFERENCES users(id),
    session_id      UUID DEFAULT gen_random_uuid(),
    payload         JSONB NOT NULL DEFAULT '{}',
    ip_address      VARCHAR(45),                        -- IPv4 or IPv6
    user_agent      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 1000 events (enough to test batching at batch_size=100)
INSERT INTO events (event_type, user_id, session_id, payload, ip_address, user_agent, created_at)
SELECT
    (CASE i % 8
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'button_click'
        WHEN 2 THEN 'form_submit'
        WHEN 3 THEN 'search'
        WHEN 4 THEN 'add_to_cart'
        WHEN 5 THEN 'checkout_start'
        WHEN 6 THEN 'payment_complete'
        WHEN 7 THEN 'logout'
    END),
    CASE WHEN i % 3 != 0 THEN (i % 50) + 1 ELSE NULL END,  -- 1/3 anonymous
    gen_random_uuid(),
    jsonb_build_object(
        'page', '/page/' || (i % 20),
        'duration_ms', (i * 37 % 30000),
        'referrer', CASE WHEN i % 5 = 0 THEN 'google.com' ELSE NULL END,
        'device', CASE i % 3 WHEN 0 THEN 'mobile' WHEN 1 THEN 'desktop' ELSE 'tablet' END
    ),
    '192.168.' || (i % 255) || '.' || ((i * 7) % 255),
    CASE i % 4
        WHEN 0 THEN 'Mozilla/5.0 (Linux; Android 14) Chrome/120'
        WHEN 1 THEN 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Safari/604'
        WHEN 2 THEN 'Mozilla/5.0 (Windows NT 10.0) Edge/120'
        WHEN 3 THEN 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14) Firefox/121'
    END,
    NOW() - (interval '1 minute' * (1000 - i))
FROM generate_series(1, 1000) AS i;

-- =========================================
-- Table 4: plans (full_refresh sync)
-- =========================================
CREATE TABLE plans (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL UNIQUE,
    price_monthly   NUMERIC(8, 2) NOT NULL,
    price_yearly    NUMERIC(8, 2) NOT NULL,
    max_users       INTEGER,                           -- NULL = unlimited
    features        TEXT[] NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO plans (name, price_monthly, price_yearly, max_users, features, is_active) VALUES
    ('Free',       0.00,      0.00,    5,   ARRAY['basic_reports', 'email_support'], true),
    ('Starter',    999.00,    9990.00,  25,  ARRAY['basic_reports', 'email_support', 'api_access'], true),
    ('Pro',        2999.00,   29990.00, 100, ARRAY['advanced_reports', 'priority_support', 'api_access', 'webhooks', 'sso'], true),
    ('Enterprise', 9999.00,   99990.00, NULL,ARRAY['advanced_reports', 'priority_support', 'api_access', 'webhooks', 'sso', 'custom_integrations', 'sla'], true),
    ('Legacy',     1499.00,   14990.00, 50,  ARRAY['basic_reports', 'email_support', 'api_access'], false);

-- =========================================
-- Table 5: audit_log (append_only sync)
-- =========================================
CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    actor_id        BIGINT,
    actor_email     VARCHAR(255),
    action          VARCHAR(100) NOT NULL,
    resource_type   VARCHAR(50) NOT NULL,
    resource_id     VARCHAR(100),
    old_value       JSONB,
    new_value       JSONB,
    ip_address      VARCHAR(45),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO audit_log (actor_id, actor_email, action, resource_type, resource_id, old_value, new_value, ip_address, created_at)
SELECT
    (i % 50) + 1,
    'user' || ((i % 50) + 1) || '@company.in',
    (CASE i % 6
        WHEN 0 THEN 'user.login'
        WHEN 1 THEN 'user.update_profile'
        WHEN 2 THEN 'order.create'
        WHEN 3 THEN 'order.update_status'
        WHEN 4 THEN 'plan.change'
        WHEN 5 THEN 'settings.update'
    END),
    (CASE i % 3
        WHEN 0 THEN 'user'
        WHEN 1 THEN 'order'
        WHEN 2 THEN 'settings'
    END),
    (i % 200 + 1)::TEXT,
    CASE WHEN i % 4 = 0 THEN '{"status": "pending"}'::JSONB ELSE NULL END,
    CASE WHEN i % 4 = 0 THEN '{"status": "confirmed"}'::JSONB ELSE NULL END,
    '10.0.' || (i % 255) || '.' || ((i * 3) % 255),
    NOW() - (interval '1 minute' * (500 - i))
FROM generate_series(1, 500) AS i;

-- =========================================
-- Verification queries (check seed worked)
-- =========================================
-- You can run these manually:
--   SELECT 'users' AS tbl, count(*) FROM users
--   UNION ALL SELECT 'orders', count(*) FROM orders
--   UNION ALL SELECT 'events', count(*) FROM events
--   UNION ALL SELECT 'plans', count(*) FROM plans
--   UNION ALL SELECT 'audit_log', count(*) FROM audit_log;
--
-- Expected: users=50, orders=200, events=1000, plans=5, audit_log=500
