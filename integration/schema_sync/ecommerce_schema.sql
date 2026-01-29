-- E-Commerce Database Schema
-- Demonstrates: Multiple schemas, partitioning, foreign keys, indexes, views, materialized views,
-- composite types, domains, enums, arrays, JSONB, full-text search, and more

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Drop existing schemas if they exist
DROP SCHEMA IF EXISTS core CASCADE;
DROP SCHEMA IF EXISTS inventory CASCADE;
DROP SCHEMA IF EXISTS sales CASCADE;
DROP SCHEMA IF EXISTS analytics CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;

-- Create schemas
CREATE SCHEMA core;      -- Core entities (users, addresses)
CREATE SCHEMA inventory; -- Products and inventory
CREATE SCHEMA sales;     -- Orders and transactions
CREATE SCHEMA analytics; -- Analytics views and materialized views
CREATE SCHEMA audit;     -- Audit trails

-- ============================================================================
-- DOMAINS AND TYPES
-- ============================================================================

-- Custom domains
CREATE DOMAIN core.email AS VARCHAR(255)
  CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

CREATE DOMAIN core.phone AS VARCHAR(20)
  CHECK (VALUE ~ '^\+?[0-9\s\-\(\)]+$');

CREATE DOMAIN inventory.money_positive AS NUMERIC(12,2)
  CHECK (VALUE >= 0);

CREATE DOMAIN inventory.weight_kg AS NUMERIC(8,3)
  CHECK (VALUE > 0);

-- Enums
CREATE TYPE core.user_status AS ENUM ('active', 'inactive', 'suspended', 'deleted');
CREATE TYPE core.user_role AS ENUM ('customer', 'vendor', 'admin', 'support');
CREATE TYPE core.address_type AS ENUM ('billing', 'shipping', 'both');

CREATE TYPE inventory.product_status AS ENUM ('draft', 'active', 'discontinued', 'out_of_stock');
CREATE TYPE inventory.stock_movement_type AS ENUM ('purchase', 'sale', 'return', 'adjustment', 'damage', 'transfer');

CREATE TYPE sales.order_status AS ENUM ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded');
CREATE TYPE sales.payment_status AS ENUM ('pending', 'authorized', 'captured', 'failed', 'refunded', 'partially_refunded');
CREATE TYPE sales.payment_method AS ENUM ('credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cryptocurrency', 'cash_on_delivery');
CREATE TYPE sales.shipment_status AS ENUM ('pending', 'picked', 'packed', 'in_transit', 'delivered', 'returned');

-- Composite types
CREATE TYPE core.geo_point AS (
  latitude NUMERIC(9,6),
  longitude NUMERIC(9,6)
);

CREATE TYPE inventory.dimensions AS (
  length_cm NUMERIC(6,2),
  width_cm NUMERIC(6,2),
  height_cm NUMERIC(6,2)
);

CREATE TYPE sales.money_with_currency AS (
  amount NUMERIC(12,2),
  currency_code CHAR(3)
);

-- ============================================================================
-- CORE SCHEMA: Users, Addresses, Authentication
-- ============================================================================

CREATE TABLE core.users (
  user_id BIGSERIAL PRIMARY KEY,
  email core.email NOT NULL UNIQUE,
  username VARCHAR(50) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  full_name VARCHAR(201) GENERATED ALWAYS AS (
    TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))
  ) STORED,
  phone core.phone,
  date_of_birth DATE,
  status core.user_status NOT NULL DEFAULT 'active',
  roles core.user_role[] NOT NULL DEFAULT ARRAY['customer']::core.user_role[],
  preferences JSONB DEFAULT '{}',
  metadata JSONB DEFAULT '{}',
  last_login_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT users_age_check CHECK (date_of_birth IS NULL OR date_of_birth < CURRENT_DATE - INTERVAL '13 years')
);

CREATE INDEX idx_users_email ON core.users(email);
CREATE INDEX idx_users_username ON core.users(username);
CREATE INDEX idx_users_status ON core.users(status) WHERE status = 'active';
CREATE INDEX idx_users_roles ON core.users USING GIN(roles);
CREATE INDEX idx_users_preferences ON core.users USING GIN(preferences);
CREATE INDEX idx_users_created_at ON core.users(created_at);

CREATE TABLE core.countries (
  country_code CHAR(2) PRIMARY KEY,
  country_name VARCHAR(100) NOT NULL,
  iso3 CHAR(3) NOT NULL UNIQUE,
  phone_prefix VARCHAR(10),
  currency_code CHAR(3),
  region VARCHAR(50)
);

CREATE TABLE core.addresses (
  address_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES core.users(user_id) ON DELETE CASCADE,
  address_type core.address_type NOT NULL,
  recipient_name VARCHAR(200),
  address_line1 VARCHAR(255) NOT NULL,
  address_line2 VARCHAR(255),
  city VARCHAR(100) NOT NULL,
  state_province VARCHAR(100),
  postal_code VARCHAR(20),
  country_code CHAR(2) NOT NULL REFERENCES core.countries(country_code),
  full_address TEXT GENERATED ALWAYS AS (
    address_line1 ||
    COALESCE(', ' || address_line2, '') ||
    ', ' || city ||
    COALESCE(', ' || state_province, '') ||
    COALESCE(' ' || postal_code, '')
  ) STORED,
  search_vector TSVECTOR,
  coordinates core.geo_point,
  is_default BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_addresses_user_id ON core.addresses(user_id);
CREATE INDEX idx_addresses_country ON core.addresses(country_code);
CREATE INDEX idx_addresses_default ON core.addresses(user_id, is_default) WHERE is_default = TRUE;
CREATE INDEX idx_addresses_search ON core.addresses USING GIN(search_vector);

-- Trigger to update search vector for addresses
CREATE OR REPLACE FUNCTION core.addresses_search_vector_update()
RETURNS TRIGGER AS $$
BEGIN
  NEW.search_vector := to_tsvector('english',
    COALESCE(NEW.recipient_name, '') || ' ' ||
    COALESCE(NEW.address_line1, '') || ' ' ||
    COALESCE(NEW.address_line2, '') || ' ' ||
    COALESCE(NEW.city, '') || ' ' ||
    COALESCE(NEW.state_province, '') || ' ' ||
    COALESCE(NEW.postal_code, '')
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_addresses_search_vector
  BEFORE INSERT OR UPDATE OF recipient_name, address_line1, address_line2, city, state_province, postal_code
  ON core.addresses
  FOR EACH ROW
  EXECUTE FUNCTION core.addresses_search_vector_update();

-- ============================================================================
-- INVENTORY SCHEMA: Products, Categories, Stock
-- ============================================================================

CREATE TABLE inventory.categories (
  category_id BIGSERIAL PRIMARY KEY,
  parent_category_id BIGINT REFERENCES inventory.categories(category_id) ON DELETE SET NULL,
  category_name VARCHAR(100) NOT NULL,
  category_slug VARCHAR(100) NOT NULL UNIQUE,
  description TEXT,
  level INTEGER NOT NULL DEFAULT 0,
  path LTREE,
  metadata JSONB DEFAULT '{}',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_categories_parent ON inventory.categories(parent_category_id);
CREATE INDEX idx_categories_slug ON inventory.categories(category_slug);
CREATE INDEX idx_categories_path ON inventory.categories USING GIST(path);

CREATE TABLE inventory.brands (
  brand_id BIGSERIAL PRIMARY KEY,
  brand_name VARCHAR(100) NOT NULL UNIQUE,
  brand_slug VARCHAR(100) NOT NULL UNIQUE,
  description TEXT,
  logo_url VARCHAR(500),
  website_url VARCHAR(500),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_brands_slug ON inventory.brands(brand_slug);

CREATE TABLE inventory.products (
  product_id BIGSERIAL PRIMARY KEY,
  sku VARCHAR(100) NOT NULL UNIQUE,
  product_name VARCHAR(255) NOT NULL,
  product_slug VARCHAR(255) NOT NULL,
  brand_id BIGINT REFERENCES inventory.brands(brand_id) ON DELETE SET NULL,
  category_id BIGINT REFERENCES inventory.categories(category_id) ON DELETE SET NULL,
  description TEXT,
  long_description TEXT,
  status inventory.product_status NOT NULL DEFAULT 'draft',
  base_price inventory.money_positive NOT NULL,
  currency_code CHAR(3) NOT NULL DEFAULT 'USD',
  cost_price inventory.money_positive,
  weight inventory.weight_kg,
  dimensions inventory.dimensions,
  attributes JSONB DEFAULT '{}',
  tags TEXT[],
  search_vector TSVECTOR,
  image_urls TEXT[],
  is_digital BOOLEAN NOT NULL DEFAULT FALSE,
  requires_shipping BOOLEAN NOT NULL DEFAULT TRUE,
  is_taxable BOOLEAN NOT NULL DEFAULT TRUE,
  tax_category VARCHAR(50),
  min_order_quantity INTEGER NOT NULL DEFAULT 1,
  max_order_quantity INTEGER,
  profit_margin NUMERIC(5,2) GENERATED ALWAYS AS (
    CASE
      WHEN cost_price IS NULL OR cost_price = 0 THEN NULL
      ELSE ((base_price - cost_price) / cost_price * 100)
    END
  ) STORED,
  total_volume_cm3 NUMERIC(12,2) GENERATED ALWAYS AS (
    CASE
      WHEN dimensions IS NULL THEN NULL
      ELSE (dimensions).length_cm * (dimensions).width_cm * (dimensions).height_cm
    END
  ) STORED,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT products_price_check CHECK (base_price >= 0 AND (cost_price IS NULL OR cost_price <= base_price)),
  CONSTRAINT products_quantity_check CHECK (min_order_quantity > 0 AND (max_order_quantity IS NULL OR max_order_quantity >= min_order_quantity))
);

CREATE INDEX idx_products_sku ON inventory.products(sku);
CREATE INDEX idx_products_slug ON inventory.products(product_slug);
CREATE INDEX idx_products_brand ON inventory.products(brand_id);
CREATE INDEX idx_products_category ON inventory.products(category_id);
CREATE INDEX idx_products_status ON inventory.products(status) WHERE status = 'active';
CREATE INDEX idx_products_search ON inventory.products USING GIN(search_vector);
CREATE INDEX idx_products_tags ON inventory.products USING GIN(tags);
CREATE INDEX idx_products_attributes ON inventory.products USING GIN(attributes);
CREATE INDEX idx_products_price ON inventory.products(base_price);
CREATE INDEX idx_products_profit_margin ON inventory.products(profit_margin) WHERE profit_margin IS NOT NULL;

-- Trigger to update search vector for products
CREATE OR REPLACE FUNCTION inventory.products_search_vector_update()
RETURNS TRIGGER AS $$
BEGIN
  NEW.search_vector :=
    setweight(to_tsvector('english', COALESCE(NEW.product_name, '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'B') ||
    setweight(to_tsvector('english', COALESCE(array_to_string(NEW.tags, ' '), '')), 'C');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_products_search_vector
  BEFORE INSERT OR UPDATE OF product_name, description, tags
  ON inventory.products
  FOR EACH ROW
  EXECUTE FUNCTION inventory.products_search_vector_update();

-- Product variants (e.g., different sizes, colors)
CREATE TABLE inventory.product_variants (
  variant_id BIGSERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL REFERENCES inventory.products(product_id) ON DELETE CASCADE,
  variant_sku VARCHAR(100) NOT NULL UNIQUE,
  variant_name VARCHAR(255) NOT NULL,
  variant_attributes JSONB NOT NULL, -- e.g., {"size": "L", "color": "red"}
  price_adjustment NUMERIC(12,2) NOT NULL DEFAULT 0,
  weight_adjustment NUMERIC(8,3) NOT NULL DEFAULT 0,
  image_urls TEXT[],
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_variants_product ON inventory.product_variants(product_id);
CREATE INDEX idx_variants_sku ON inventory.product_variants(variant_sku);
CREATE INDEX idx_variants_attributes ON inventory.product_variants USING GIN(variant_attributes);

-- Warehouses
CREATE TABLE inventory.warehouses (
  warehouse_id BIGSERIAL PRIMARY KEY,
  warehouse_code VARCHAR(20) NOT NULL UNIQUE,
  warehouse_name VARCHAR(100) NOT NULL,
  address_line1 VARCHAR(255) NOT NULL,
  address_line2 VARCHAR(255),
  city VARCHAR(100) NOT NULL,
  state_province VARCHAR(100),
  postal_code VARCHAR(20),
  country_code CHAR(2) NOT NULL REFERENCES core.countries(country_code),
  coordinates core.geo_point,
  capacity_cubic_meters NUMERIC(10,2),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouses_country ON inventory.warehouses(country_code);

-- Stock levels (partitioned by warehouse)
CREATE TABLE inventory.stock_levels (
  stock_id BIGSERIAL,
  warehouse_id BIGINT NOT NULL REFERENCES inventory.warehouses(warehouse_id) ON DELETE CASCADE,
  product_id BIGINT REFERENCES inventory.products(product_id) ON DELETE CASCADE,
  variant_id BIGINT REFERENCES inventory.product_variants(variant_id) ON DELETE CASCADE,
  quantity_available INTEGER NOT NULL DEFAULT 0,
  quantity_reserved INTEGER NOT NULL DEFAULT 0,
  quantity_damaged INTEGER NOT NULL DEFAULT 0,
  reorder_point INTEGER NOT NULL DEFAULT 10,
  reorder_quantity INTEGER NOT NULL DEFAULT 100,
  last_counted_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (stock_id, warehouse_id),
  CONSTRAINT stock_product_or_variant CHECK (
    (product_id IS NOT NULL AND variant_id IS NULL) OR
    (product_id IS NULL AND variant_id IS NOT NULL)
  ),
  CONSTRAINT stock_quantities_positive CHECK (
    quantity_available >= 0 AND
    quantity_reserved >= 0 AND
    quantity_damaged >= 0
  )
) PARTITION BY HASH (warehouse_id);

CREATE TABLE inventory.stock_levels_p0 PARTITION OF inventory.stock_levels
  FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE inventory.stock_levels_p1 PARTITION OF inventory.stock_levels
  FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE inventory.stock_levels_p2 PARTITION OF inventory.stock_levels
  FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE inventory.stock_levels_p3 PARTITION OF inventory.stock_levels
  FOR VALUES WITH (MODULUS 4, REMAINDER 3);

CREATE INDEX idx_stock_warehouse ON inventory.stock_levels(warehouse_id);
CREATE INDEX idx_stock_product ON inventory.stock_levels(product_id) WHERE product_id IS NOT NULL;
CREATE INDEX idx_stock_variant ON inventory.stock_levels(variant_id) WHERE variant_id IS NOT NULL;
CREATE INDEX idx_stock_low ON inventory.stock_levels(warehouse_id, quantity_available)
  WHERE quantity_available <= reorder_point;

-- Stock movements (partitioned by date)
CREATE TABLE inventory.stock_movements (
  movement_id BIGSERIAL,
  warehouse_id BIGINT NOT NULL REFERENCES inventory.warehouses(warehouse_id),
  product_id BIGINT REFERENCES inventory.products(product_id),
  variant_id BIGINT REFERENCES inventory.product_variants(variant_id),
  movement_type inventory.stock_movement_type NOT NULL,
  quantity INTEGER NOT NULL,
  reference_type VARCHAR(50), -- e.g., 'order', 'purchase_order', 'adjustment'
  reference_id BIGINT,
  notes TEXT,
  performed_by BIGINT REFERENCES core.users(user_id),
  movement_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (movement_id, movement_date),
  CONSTRAINT movement_product_or_variant CHECK (
    (product_id IS NOT NULL AND variant_id IS NULL) OR
    (product_id IS NULL AND variant_id IS NOT NULL)
  )
) PARTITION BY RANGE (movement_date);

-- Create partitions for last 12 months and next 12 months
CREATE TABLE inventory.stock_movements_2024_q4 PARTITION OF inventory.stock_movements
  FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE inventory.stock_movements_2025_q1 PARTITION OF inventory.stock_movements
  FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE inventory.stock_movements_2025_q2 PARTITION OF inventory.stock_movements
  FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE inventory.stock_movements_2025_q3 PARTITION OF inventory.stock_movements
  FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE inventory.stock_movements_2025_q4 PARTITION OF inventory.stock_movements
  FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE inventory.stock_movements_2026_q1 PARTITION OF inventory.stock_movements
  FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');

CREATE INDEX idx_movements_warehouse ON inventory.stock_movements(warehouse_id, movement_date);
CREATE INDEX idx_movements_product ON inventory.stock_movements(product_id, movement_date) WHERE product_id IS NOT NULL;
CREATE INDEX idx_movements_variant ON inventory.stock_movements(variant_id, movement_date) WHERE variant_id IS NOT NULL;
CREATE INDEX idx_movements_reference ON inventory.stock_movements(reference_type, reference_id);

-- ============================================================================
-- SALES SCHEMA: Orders, Payments, Shipments
-- ============================================================================

-- Shopping carts
CREATE TABLE sales.carts (
  cart_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT REFERENCES core.users(user_id) ON DELETE CASCADE,
  session_id VARCHAR(255), -- For guest carts
  currency_code CHAR(3) NOT NULL DEFAULT 'USD',
  coupon_code VARCHAR(50),
  discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  tax_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  shipping_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ,
  CONSTRAINT cart_user_or_session CHECK (user_id IS NOT NULL OR session_id IS NOT NULL)
);

CREATE INDEX idx_carts_user ON sales.carts(user_id);
CREATE INDEX idx_carts_session ON sales.carts(session_id);
CREATE INDEX idx_carts_expires ON sales.carts(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE sales.cart_items (
  cart_item_id BIGSERIAL PRIMARY KEY,
  cart_id BIGINT NOT NULL REFERENCES sales.carts(cart_id) ON DELETE CASCADE,
  product_id BIGINT REFERENCES inventory.products(product_id) ON DELETE CASCADE,
  variant_id BIGINT REFERENCES inventory.product_variants(variant_id) ON DELETE CASCADE,
  quantity INTEGER NOT NULL,
  unit_price NUMERIC(12,2) NOT NULL,
  total_price NUMERIC(12,2) NOT NULL,
  added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT cart_items_product_or_variant CHECK (
    (product_id IS NOT NULL AND variant_id IS NULL) OR
    (product_id IS NULL AND variant_id IS NOT NULL)
  ),
  CONSTRAINT cart_items_quantity_positive CHECK (quantity > 0)
);

CREATE INDEX idx_cart_items_cart ON sales.cart_items(cart_id);

-- Orders (partitioned by created_at)
CREATE TABLE sales.orders (
  order_id BIGSERIAL,
  order_number VARCHAR(50) NOT NULL,
  user_id BIGINT NOT NULL REFERENCES core.users(user_id),
  status sales.order_status NOT NULL DEFAULT 'pending',
  billing_address_id BIGINT REFERENCES core.addresses(address_id),
  shipping_address_id BIGINT REFERENCES core.addresses(address_id),
  currency_code CHAR(3) NOT NULL DEFAULT 'USD',
  subtotal_amount NUMERIC(12,2) NOT NULL,
  discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  tax_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  shipping_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  total_amount NUMERIC(12,2) NOT NULL,
  discount_percentage NUMERIC(5,2) GENERATED ALWAYS AS (
    CASE
      WHEN subtotal_amount > 0 THEN (discount_amount / subtotal_amount * 100)
      ELSE 0
    END
  ) STORED,
  effective_tax_rate NUMERIC(5,2) GENERATED ALWAYS AS (
    CASE
      WHEN subtotal_amount > 0 THEN (tax_amount / subtotal_amount * 100)
      ELSE 0
    END
  ) STORED,
  coupon_code VARCHAR(50),
  notes TEXT,
  ip_address INET,
  user_agent TEXT,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  confirmed_at TIMESTAMPTZ,
  shipped_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ,
  PRIMARY KEY (order_id, created_at),
  CONSTRAINT orders_amounts_positive CHECK (
    subtotal_amount >= 0 AND
    discount_amount >= 0 AND
    tax_amount >= 0 AND
    shipping_amount >= 0 AND
    total_amount >= 0
  )
) PARTITION BY RANGE (created_at);

CREATE TABLE sales.orders_2024_q4 PARTITION OF sales.orders
  FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE sales.orders_2025_q1 PARTITION OF sales.orders
  FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE sales.orders_2025_q2 PARTITION OF sales.orders
  FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE sales.orders_2025_q3 PARTITION OF sales.orders
  FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE sales.orders_2025_q4 PARTITION OF sales.orders
  FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE sales.orders_2026_q1 PARTITION OF sales.orders
  FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');

CREATE UNIQUE INDEX idx_orders_order_number ON sales.orders(order_number, created_at);
CREATE INDEX idx_orders_user ON sales.orders(user_id, created_at);
CREATE INDEX idx_orders_status ON sales.orders(status, created_at);
CREATE INDEX idx_orders_created ON sales.orders(created_at);

CREATE TABLE sales.order_items (
  order_item_id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id BIGINT REFERENCES inventory.products(product_id),
  variant_id BIGINT REFERENCES inventory.product_variants(variant_id),
  product_name VARCHAR(255) NOT NULL, -- Snapshot at order time
  product_sku VARCHAR(100) NOT NULL,
  quantity INTEGER NOT NULL,
  unit_price NUMERIC(12,2) NOT NULL,
  discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  tax_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  total_price NUMERIC(12,2) NOT NULL,
  attributes JSONB DEFAULT '{}', -- Snapshot of product attributes
  CONSTRAINT order_items_product_or_variant CHECK (
    (product_id IS NOT NULL AND variant_id IS NULL) OR
    (product_id IS NULL AND variant_id IS NOT NULL)
  ),
  CONSTRAINT order_items_quantity_positive CHECK (quantity > 0),
  CONSTRAINT order_items_amounts_positive CHECK (
    unit_price >= 0 AND
    discount_amount >= 0 AND
    tax_amount >= 0 AND
    total_price >= 0
  )
);

CREATE INDEX idx_order_items_order ON sales.order_items(order_id);
CREATE INDEX idx_order_items_product ON sales.order_items(product_id) WHERE product_id IS NOT NULL;

-- Payments
CREATE TABLE sales.payments (
  payment_id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL,
  payment_method sales.payment_method NOT NULL,
  payment_status sales.payment_status NOT NULL DEFAULT 'pending',
  amount NUMERIC(12,2) NOT NULL,
  currency_code CHAR(3) NOT NULL DEFAULT 'USD',
  transaction_id VARCHAR(255),
  gateway_name VARCHAR(100),
  gateway_response JSONB,
  card_last4 VARCHAR(4),
  card_brand VARCHAR(50),
  payer_email core.email,
  payer_name VARCHAR(200),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  authorized_at TIMESTAMPTZ,
  captured_at TIMESTAMPTZ,
  failed_at TIMESTAMPTZ,
  refunded_at TIMESTAMPTZ,
  CONSTRAINT payments_amount_positive CHECK (amount > 0)
);

CREATE INDEX idx_payments_order ON sales.payments(order_id);
CREATE INDEX idx_payments_status ON sales.payments(payment_status);
CREATE INDEX idx_payments_transaction ON sales.payments(transaction_id);

-- Shipments
CREATE TABLE sales.shipments (
  shipment_id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL,
  warehouse_id BIGINT NOT NULL REFERENCES inventory.warehouses(warehouse_id),
  tracking_number VARCHAR(100),
  carrier_name VARCHAR(100),
  carrier_service VARCHAR(100),
  status sales.shipment_status NOT NULL DEFAULT 'pending',
  estimated_delivery_date DATE,
  actual_delivery_date DATE,
  weight_kg NUMERIC(8,3),
  dimensions inventory.dimensions,
  shipping_cost NUMERIC(12,2),
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  picked_at TIMESTAMPTZ,
  packed_at TIMESTAMPTZ,
  shipped_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_shipments_order ON sales.shipments(order_id);
CREATE INDEX idx_shipments_tracking ON sales.shipments(tracking_number);
CREATE INDEX idx_shipments_status ON sales.shipments(status);
CREATE INDEX idx_shipments_warehouse ON sales.shipments(warehouse_id);

CREATE TABLE sales.shipment_items (
  shipment_item_id BIGSERIAL PRIMARY KEY,
  shipment_id BIGINT NOT NULL REFERENCES sales.shipments(shipment_id) ON DELETE CASCADE,
  order_item_id BIGINT NOT NULL REFERENCES sales.order_items(order_item_id),
  quantity INTEGER NOT NULL,
  CONSTRAINT shipment_items_quantity_positive CHECK (quantity > 0)
);

CREATE INDEX idx_shipment_items_shipment ON sales.shipment_items(shipment_id);
CREATE INDEX idx_shipment_items_order_item ON sales.shipment_items(order_item_id);

-- Reviews and ratings
CREATE TABLE sales.product_reviews (
  review_id BIGSERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL REFERENCES inventory.products(product_id) ON DELETE CASCADE,
  user_id BIGINT NOT NULL REFERENCES core.users(user_id) ON DELETE CASCADE,
  order_id BIGINT,
  rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
  title VARCHAR(200),
  review_text TEXT,
  search_vector TSVECTOR,
  is_verified_purchase BOOLEAN NOT NULL DEFAULT FALSE,
  is_approved BOOLEAN NOT NULL DEFAULT FALSE,
  helpful_count INTEGER NOT NULL DEFAULT 0,
  unhelpful_count INTEGER NOT NULL DEFAULT 0,
  helpfulness_score NUMERIC(5,2) GENERATED ALWAYS AS (
    CASE
      WHEN (helpful_count + unhelpful_count) > 0
      THEN (helpful_count::NUMERIC / (helpful_count + unhelpful_count) * 100)
      ELSE NULL
    END
  ) STORED,
  images TEXT[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(product_id, user_id, order_id)
);

CREATE INDEX idx_reviews_product ON sales.product_reviews(product_id, is_approved) WHERE is_approved = TRUE;
CREATE INDEX idx_reviews_user ON sales.product_reviews(user_id);
CREATE INDEX idx_reviews_rating ON sales.product_reviews(product_id, rating);
CREATE INDEX idx_reviews_search ON sales.product_reviews USING GIN(search_vector) WHERE is_approved = TRUE;

-- Trigger to update search vector for reviews
CREATE OR REPLACE FUNCTION sales.reviews_search_vector_update()
RETURNS TRIGGER AS $$
BEGIN
  NEW.search_vector :=
    setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(NEW.review_text, '')), 'B');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_reviews_search_vector
  BEFORE INSERT OR UPDATE OF title, review_text
  ON sales.product_reviews
  FOR EACH ROW
  EXECUTE FUNCTION sales.reviews_search_vector_update();

-- ============================================================================
-- AUDIT SCHEMA: Change tracking
-- ============================================================================

CREATE TABLE audit.user_activity_log (
  log_id BIGSERIAL,
  user_id BIGINT REFERENCES core.users(user_id) ON DELETE SET NULL,
  activity_type VARCHAR(50) NOT NULL,
  entity_type VARCHAR(50),
  entity_id BIGINT,
  ip_address INET,
  user_agent TEXT,
  request_data JSONB,
  response_data JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (log_id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE audit.user_activity_log_2025_q1 PARTITION OF audit.user_activity_log
  FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE audit.user_activity_log_2025_q2 PARTITION OF audit.user_activity_log
  FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE audit.user_activity_log_2025_q3 PARTITION OF audit.user_activity_log
  FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE audit.user_activity_log_2025_q4 PARTITION OF audit.user_activity_log
  FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');

CREATE INDEX idx_activity_user ON audit.user_activity_log(user_id, created_at);
CREATE INDEX idx_activity_type ON audit.user_activity_log(activity_type, created_at);
CREATE INDEX idx_activity_entity ON audit.user_activity_log(entity_type, entity_id);

-- ============================================================================
-- ANALYTICS SCHEMA: Views and Materialized Views
-- ============================================================================

-- View: Current stock availability across all warehouses
CREATE VIEW analytics.product_stock_summary AS
SELECT
  COALESCE(sl.product_id, pv.product_id) as product_id,
  sl.variant_id,
  p.product_name,
  p.sku,
  pv.variant_sku,
  SUM(sl.quantity_available) as total_available,
  SUM(sl.quantity_reserved) as total_reserved,
  SUM(sl.quantity_damaged) as total_damaged,
  COUNT(DISTINCT sl.warehouse_id) as warehouse_count,
  MAX(sl.updated_at) as last_updated
FROM inventory.stock_levels sl
LEFT JOIN inventory.products p ON sl.product_id = p.product_id
LEFT JOIN inventory.product_variants pv ON sl.variant_id = pv.variant_id
GROUP BY COALESCE(sl.product_id, pv.product_id), sl.variant_id, p.product_name, p.sku, pv.variant_sku;

-- View: Order summary with customer info
CREATE VIEW analytics.order_summary AS
SELECT
  o.order_id,
  o.order_number,
  o.status,
  o.created_at,
  u.user_id,
  u.email,
  u.first_name,
  u.last_name,
  o.subtotal_amount,
  o.discount_amount,
  o.tax_amount,
  o.shipping_amount,
  o.total_amount,
  o.currency_code,
  COUNT(oi.order_item_id) as item_count,
  SUM(oi.quantity) as total_quantity
FROM sales.orders o
JOIN core.users u ON o.user_id = u.user_id
LEFT JOIN sales.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.order_number, o.status, o.created_at,
         u.user_id, u.email, u.first_name, u.last_name,
         o.subtotal_amount, o.discount_amount, o.tax_amount,
         o.shipping_amount, o.total_amount, o.currency_code;

-- Materialized View: Daily sales metrics
CREATE MATERIALIZED VIEW analytics.daily_sales_metrics AS
SELECT
  DATE(created_at) as sale_date,
  status,
  currency_code,
  COUNT(*) as order_count,
  SUM(total_amount) as total_revenue,
  SUM(subtotal_amount) as subtotal_revenue,
  SUM(discount_amount) as total_discounts,
  SUM(tax_amount) as total_tax,
  SUM(shipping_amount) as total_shipping,
  AVG(total_amount) as avg_order_value,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) as median_order_value
FROM sales.orders
WHERE created_at >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY DATE(created_at), status, currency_code
WITH DATA;

CREATE UNIQUE INDEX idx_daily_sales_metrics ON analytics.daily_sales_metrics(sale_date, status, currency_code);
CREATE INDEX idx_daily_sales_date ON analytics.daily_sales_metrics(sale_date);

-- Materialized View: Product performance metrics
CREATE MATERIALIZED VIEW analytics.product_performance AS
SELECT
  p.product_id,
  p.product_name,
  p.sku,
  p.status,
  c.category_name,
  b.brand_name,
  COUNT(DISTINCT oi.order_id) as order_count,
  SUM(oi.quantity) as units_sold,
  SUM(oi.total_price) as total_revenue,
  AVG(pr.rating) as avg_rating,
  COUNT(pr.review_id) as review_count
FROM inventory.products p
LEFT JOIN inventory.categories c ON p.category_id = c.category_id
LEFT JOIN inventory.brands b ON p.brand_id = b.brand_id
LEFT JOIN sales.order_items oi ON p.product_id = oi.product_id
LEFT JOIN sales.product_reviews pr ON p.product_id = pr.product_id AND pr.is_approved = TRUE
GROUP BY p.product_id, p.product_name, p.sku, p.status, c.category_name, b.brand_name
WITH DATA;

CREATE UNIQUE INDEX idx_product_performance ON analytics.product_performance(product_id);
CREATE INDEX idx_product_performance_revenue ON analytics.product_performance(total_revenue DESC NULLS LAST);

-- Materialized View: Customer lifetime value
CREATE MATERIALIZED VIEW analytics.customer_lifetime_value AS
SELECT
  u.user_id,
  u.email,
  u.first_name,
  u.last_name,
  u.created_at as customer_since,
  COUNT(DISTINCT o.order_id) as total_orders,
  SUM(o.total_amount) as lifetime_value,
  AVG(o.total_amount) as avg_order_value,
  MAX(o.created_at) as last_order_date,
  MIN(o.created_at) as first_order_date
FROM core.users u
LEFT JOIN sales.orders o ON u.user_id = o.user_id AND o.status NOT IN ('cancelled', 'refunded')
WHERE 'customer' = ANY(u.roles)
GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.created_at
WITH DATA;

CREATE UNIQUE INDEX idx_customer_ltv ON analytics.customer_lifetime_value(user_id);
CREATE INDEX idx_customer_ltv_value ON analytics.customer_lifetime_value(lifetime_value DESC NULLS LAST);

-- View: Low stock alerts
CREATE VIEW analytics.low_stock_alerts AS
SELECT
  w.warehouse_id,
  w.warehouse_name,
  p.product_id,
  p.product_name,
  p.sku,
  sl.quantity_available,
  sl.quantity_reserved,
  sl.reorder_point,
  sl.reorder_quantity,
  (sl.quantity_available - sl.quantity_reserved) as available_for_sale
FROM inventory.stock_levels sl
JOIN inventory.warehouses w ON sl.warehouse_id = w.warehouse_id
LEFT JOIN inventory.products p ON sl.product_id = p.product_id
WHERE sl.quantity_available <= sl.reorder_point
  AND w.is_active = TRUE;

-- View: Monthly revenue by category
CREATE VIEW analytics.category_revenue_monthly AS
SELECT
  DATE_TRUNC('month', o.created_at) as month,
  c.category_id,
  c.category_name,
  COUNT(DISTINCT o.order_id) as order_count,
  SUM(oi.quantity) as units_sold,
  SUM(oi.total_price) as total_revenue
FROM sales.orders o
JOIN sales.order_items oi ON o.order_id = oi.order_id
LEFT JOIN inventory.products p ON oi.product_id = p.product_id
LEFT JOIN inventory.categories c ON p.category_id = c.category_id
WHERE o.status NOT IN ('cancelled', 'refunded')
  AND o.created_at >= CURRENT_DATE - INTERVAL '24 months'
GROUP BY DATE_TRUNC('month', o.created_at), c.category_id, c.category_name;

-- ============================================================================
-- FUNCTIONS AND PROCEDURES
-- ============================================================================

-- Function to calculate cart total
CREATE OR REPLACE FUNCTION sales.calculate_cart_total(p_cart_id BIGINT)
RETURNS TABLE(
  subtotal NUMERIC(12,2),
  tax NUMERIC(12,2),
  shipping NUMERIC(12,2),
  total NUMERIC(12,2)
) AS $$
DECLARE
  v_subtotal NUMERIC(12,2);
  v_tax NUMERIC(12,2);
  v_shipping NUMERIC(12,2);
  v_total NUMERIC(12,2);
BEGIN
  SELECT COALESCE(SUM(total_price), 0) INTO v_subtotal
  FROM sales.cart_items
  WHERE cart_id = p_cart_id;

  v_tax := v_subtotal * 0.08; -- 8% tax
  v_shipping := CASE
    WHEN v_subtotal > 100 THEN 0
    ELSE 9.99
  END;

  v_total := v_subtotal + v_tax + v_shipping;

  RETURN QUERY SELECT v_subtotal, v_tax, v_shipping, v_total;
END;
$$ LANGUAGE plpgsql;

-- Function to reserve stock
CREATE OR REPLACE FUNCTION inventory.reserve_stock(
  p_warehouse_id BIGINT,
  p_product_id BIGINT,
  p_variant_id BIGINT,
  p_quantity INTEGER
)
RETURNS BOOLEAN AS $$
DECLARE
  v_available INTEGER;
BEGIN
  -- Lock the row for update
  SELECT quantity_available INTO v_available
  FROM inventory.stock_levels
  WHERE warehouse_id = p_warehouse_id
    AND (product_id = p_product_id OR variant_id = p_variant_id)
  FOR UPDATE;

  IF v_available >= p_quantity THEN
    UPDATE inventory.stock_levels
    SET
      quantity_available = quantity_available - p_quantity,
      quantity_reserved = quantity_reserved + p_quantity,
      updated_at = NOW()
    WHERE warehouse_id = p_warehouse_id
      AND (product_id = p_product_id OR variant_id = p_variant_id);

    RETURN TRUE;
  ELSE
    RETURN FALSE;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Procedure to refresh analytics materialized views
CREATE OR REPLACE PROCEDURE analytics.refresh_all_metrics()
LANGUAGE plpgsql
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.daily_sales_metrics;
  REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.product_performance;
  REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.customer_lifetime_value;
  RAISE NOTICE 'All analytics metrics refreshed at %', NOW();
END;
$$;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_users_updated_at BEFORE UPDATE ON core.users
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_addresses_updated_at BEFORE UPDATE ON core.addresses
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_products_updated_at BEFORE UPDATE ON inventory.products
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_carts_updated_at BEFORE UPDATE ON sales.carts
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- SAMPLE DATA
-- ============================================================================

-- Insert sample countries
INSERT INTO core.countries (country_code, country_name, iso3, phone_prefix, currency_code, region) VALUES
('US', 'United States', 'USA', '+1', 'USD', 'North America'),
('CA', 'Canada', 'CAN', '+1', 'CAD', 'North America'),
('GB', 'United Kingdom', 'GBR', '+44', 'GBP', 'Europe'),
('DE', 'Germany', 'DEU', '+49', 'EUR', 'Europe'),
('FR', 'France', 'FRA', '+33', 'EUR', 'Europe'),
('JP', 'Japan', 'JPN', '+81', 'JPY', 'Asia'),
('CN', 'China', 'CHN', '+86', 'CNY', 'Asia'),
('AU', 'Australia', 'AUS', '+61', 'AUD', 'Oceania');

-- Insert sample users
INSERT INTO core.users (email, username, password_hash, first_name, last_name, phone, date_of_birth, status, roles)
VALUES
('john.doe@example.com', 'johndoe', '$2a$10$abcdefghijklmnopqrstuv', 'John', 'Doe', '+1234567890', '1990-05-15', 'active', ARRAY['customer']::core.user_role[]),
('jane.smith@example.com', 'janesmith', '$2a$10$abcdefghijklmnopqrstuv', 'Jane', 'Smith', '+1234567891', '1985-08-22', 'active', ARRAY['customer', 'vendor']::core.user_role[]),
('admin@example.com', 'admin', '$2a$10$abcdefghijklmnopqrstuv', 'Admin', 'User', '+1234567892', '1980-01-01', 'active', ARRAY['admin']::core.user_role[]);

-- Insert sample brands
INSERT INTO inventory.brands (brand_name, brand_slug, description, is_active)
VALUES
('TechPro', 'techpro', 'Premium technology products', true),
('StyleWear', 'stylewear', 'Fashion and apparel', true),
('HomeComfort', 'homecomfort', 'Home and living essentials', true);

-- Insert sample categories
INSERT INTO inventory.categories (category_name, category_slug, level, path)
VALUES
('Electronics', 'electronics', 0, 'electronics'),
('Computers', 'computers', 1, 'electronics.computers'),
('Laptops', 'laptops', 2, 'electronics.computers.laptops'),
('Clothing', 'clothing', 0, 'clothing'),
('Men', 'men', 1, 'clothing.men'),
('Women', 'women', 1, 'clothing.women');

-- Insert sample warehouses
INSERT INTO inventory.warehouses (warehouse_code, warehouse_name, address_line1, city, state_province, postal_code, country_code, is_active)
VALUES
('WH-NYC-01', 'New York Main Warehouse', '123 Industrial Blvd', 'New York', 'NY', '10001', 'US', true),
('WH-LAX-01', 'Los Angeles Distribution Center', '456 Logistics Way', 'Los Angeles', 'CA', '90001', 'US', true),
('WH-LON-01', 'London Fulfillment Center', '789 Commerce Street', 'London', 'England', 'SW1A 1AA', 'GB', true);

-- Insert sample products
INSERT INTO inventory.products (sku, product_name, product_slug, brand_id, category_id, description, status, base_price, weight, tags)
VALUES
('LAPTOP-001', 'TechPro UltraBook Pro 15', 'techpro-ultrabook-pro-15', 1, 3, 'High-performance laptop with 15-inch display', 'active', 1299.99, 1.8, ARRAY['laptop', 'computer', 'tech']),
('LAPTOP-002', 'TechPro PowerStation 17', 'techpro-powerstation-17', 1, 3, 'Workstation laptop for professionals', 'active', 2499.99, 2.5, ARRAY['laptop', 'workstation', 'professional']),
('SHIRT-001', 'StyleWear Classic Cotton Tee', 'stylewear-classic-tee', 2, 5, 'Comfortable cotton t-shirt', 'active', 29.99, 0.2, ARRAY['clothing', 'shirt', 'casual']);

COMMENT ON SCHEMA core IS 'Core business entities including users, addresses, and authentication';
COMMENT ON SCHEMA inventory IS 'Product catalog and inventory management';
COMMENT ON SCHEMA sales IS 'Orders, payments, and transaction processing';
COMMENT ON SCHEMA analytics IS 'Business intelligence views and metrics';
COMMENT ON SCHEMA audit IS 'Activity logging and audit trails';

COMMENT ON TABLE core.users IS 'User accounts with role-based access control';
COMMENT ON TABLE inventory.products IS 'Product catalog with full-text search capabilities';
COMMENT ON TABLE sales.orders IS 'Customer orders partitioned by creation date';
COMMENT ON TABLE inventory.stock_levels IS 'Inventory levels partitioned by warehouse';

-- Simple tables with integer primary keys
CREATE TABLE core.settings (
  setting_id SERIAL PRIMARY KEY,
  setting_key VARCHAR(100) NOT NULL UNIQUE,
  setting_value TEXT
);

CREATE TABLE core.feature_flags (
  flag_id INTEGER PRIMARY KEY,
  flag_name VARCHAR(100) NOT NULL UNIQUE,
  is_enabled BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE core.user_feature_overrides (
  override_id SERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES core.users(user_id) ON DELETE CASCADE,
  flag_id INTEGER NOT NULL REFERENCES core.feature_flags(flag_id) ON DELETE CASCADE,
  is_enabled BOOLEAN NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(user_id, flag_id)
);
