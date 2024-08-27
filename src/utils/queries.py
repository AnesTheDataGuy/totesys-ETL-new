create_sales_order = '''
CREATE TABLE "sales_order" (
  "sales_order_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp),
  "design_id" int NOT NULL,
  "staff_id" int NOT NULL,
  "counterparty_id" int NOT NULL,
  "units_sold" int NOT NULL,
  "unit_price" numeric NOT NULL,
  "currency_id" int NOT NULL,
  "agreed_delivery_date" varchar NOT NULL,
  "agreed_payment_date" varchar NOT NULL,
  "agreed_delivery_location_id" int NOT NULL
);'''

create_design = '''
CREATE TABLE "design" (
  "design_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp),
  "design_name" varchar NOT NULL,
  "file_location" varchar NOT NULL,
  "file_name" varchar NOT NULL
);'''

create_currency = '''
CREATE TABLE "currency" (
  "currency_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "currency_code" varchar(3) NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp)
);'''

create_staff = '''
CREATE TABLE "staff" (
  "staff_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "first_name" varchar NOT NULL,
  "last_name" varchar NOT NULL,
  "department_id" int NOT NULL,
  "email_address" varchar NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp)
);'''

create_counterparty = '''
DROP TABLE IF EXISTS counterparty;
CREATE TABLE "counterparty" (
  "counterparty_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "counterparty_legal_name" varchar NOT NULL,
  "legal_address_id" int NOT NULL,
  "commercial_contact" varchar,
  "delivery_contact" varchar,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp)
);'''

create_address = '''
CREATE TABLE "address" (
  "address_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "address_line_1" varchar NOT NULL,
  "address_line_2" varchar,
  "district" varchar,
  "city" varchar NOT NULL,
  "postal_code" varchar NOT NULL,
  "country" varchar NOT NULL,
  "phone" varchar NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp)
);'''

create_department = '''
CREATE TABLE "department" (
  "department_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "department_name" varchar NOT NULL,
  "location" varchar,
  "manager" varchar,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp)
);'''

create_purchase_order = '''
CREATE TABLE "purchase_order" (
  "purchase_order_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp),
  "staff_id" int NOT NULL,
  "counterparty_id" int NOT NULL,
  "item_code" varchar NOT NULL,
  "item_quantity" int NOT NULL,
  "item_unit_price" numeric NOT NULL,
  "currency_id" int NOT NULL,
  "agreed_delivery_date" varchar NOT NULL,
  "agreed_payment_date" varchar NOT NULL,
  "agreed_delivery_location_id" int NOT NULL
);'''

create_payment_type = '''
CREATE TABLE "payment_type" (
  "payment_type_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "payment_type_name" varchar NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp)
);'''

create_payment = '''
CREATE TABLE "payment" (
  "payment_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp),
  "transaction_id" int NOT NULL,
  "counterparty_id" int NOT NULL,
  "payment_amount" numeric NOT NULL,
  "currency_id" int NOT NULL,
  "payment_type_id" int NOT NULL,
  "paid" boolean NOT NULL,
  "payment_date" varchar NOT NULL,
  "company_ac_number" int NOT NULL,
  "counterparty_ac_number" int NOT NULL
);'''

create_transaction = '''
CREATE TABLE "transaction" (
  "transaction_id" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
  "transaction_type" varchar NOT NULL,
  "sales_order_id" int,
  "purchase_order_id" int,
  "created_at" timestamp NOT NULL DEFAULT (current_timestamp),
  "last_updated" timestamp NOT NULL DEFAULT (current_timestamp)
);'''

column_comments = [
'''COMMENT ON COLUMN "sales_order"."units_sold" IS 'value 1000 - 100000';''',
'''COMMENT ON COLUMN "sales_order"."unit_price" IS 'value 2.00 - 4.00';''',
'''COMMENT ON COLUMN "sales_order"."agreed_delivery_date" IS 'format is yyyy-mm-dd';''',
'''COMMENT ON COLUMN "sales_order"."agreed_payment_date" IS 'format is yyyy-mm-dd';''',
'''COMMENT ON COLUMN "design"."file_location" IS 'directory location';''',
'''COMMENT ON COLUMN "design"."file_name" IS 'file name';''',
'''COMMENT ON COLUMN "currency"."currency_code" IS 'three letter code eg GBP';''',
'''COMMENT ON COLUMN "staff"."email_address" IS 'must be valid email address';''',
'''COMMENT ON COLUMN "counterparty"."commercial_contact" IS 'person name, nullable';''',
'''COMMENT ON COLUMN "counterparty"."delivery_contact" IS 'person name, nullable';''',
'''COMMENT ON COLUMN "address"."address_line_2" IS 'nullable';''',
'''COMMENT ON COLUMN "address"."district" IS 'nullable';''',
'''COMMENT ON COLUMN "address"."phone" IS 'valid phone number';''',
'''COMMENT ON COLUMN "department"."location" IS 'nullable';''',
'''COMMENT ON COLUMN "department"."manager" IS 'nullable';''',
'''COMMENT ON COLUMN "purchase_order"."item_quantity" IS 'value between 1 and 1000';''',
'''COMMENT ON COLUMN "purchase_order"."item_unit_price" IS 'value 3-1000';''',
'''COMMENT ON COLUMN "purchase_order"."agreed_delivery_date" IS 'format is yyyy-mm-dd';''',
'''COMMENT ON COLUMN "purchase_order"."agreed_payment_date" IS 'format is yyyy-mm-dd';''',
'''COMMENT ON COLUMN "payment_type"."payment_type_name" IS 'one of SALES_RECEIPT SALESREFUND PURCHASE_PAYMENT PURCHASE REFUND';''',
'''COMMENT ON COLUMN "payment"."payment_amount" IS 'value 1 - 1000000';''',
'''COMMENT ON COLUMN "payment"."payment_date" IS 'format is yyyy-mm-dd';''',
'''COMMENT ON COLUMN "payment"."company_ac_number" IS '8 digits';''',
'''COMMENT ON COLUMN "payment"."counterparty_ac_number" IS '8 digits';''',
'''COMMENT ON COLUMN "transaction"."transaction_type" IS 'one of SALE or PURCHASE';''',
'''COMMENT ON COLUMN "transaction"."sales_order_id" IS 'nullable depending on transaction type';''',
'''COMMENT ON COLUMN "transaction"."purchase_order_id" IS 'nullable depending on transaction type';'''
]


create_fct_sales_order = '''CREATE TABLE "fact_sales_order" (
    "sales_record_id" SERIAL PRIMARY KEY,
    "sales_order_id" int NOT NULL,
    "created_date" date NOT NULL,
    "created_time" time NOT NULL,
    "last_updated_date" date NOT NULL,
    "last_updated_time" time NOT NULL,
    "sales_staff_id" int NOT NULL,
    "counterparty_id" int NOT NULL,
    "units_sold" int NOT NULL,
    "unit_price" numeric(10, 2) NOT NULL,
    "currency_id" int NOT NULL,
    "design_id" int NOT NULL,
    "agreed_payment_date" date NOT NULL,
    "agreed_delivery_date" date NOT NULL,
    "agreed_delivery_location_id" int NOT NULL
  );'''

create_dim_date = '''CREATE TABLE "dim_date" (
  "date_id" date PRIMARY KEY NOT NULL,
  "year" int NOT NULL,
  "month" int NOT NULL,
  "day" int NOT NULL,
  "day_of_week" int NOT NULL,
  "day_name" varchar NOT NULL,
  "month_name" varchar NOT NULL,
  "quarter" int NOT NULL
);'''

create_dim_staff = '''CREATE TABLE "dim_staff" (
  "staff_id" int PRIMARY KEY NOT NULL,
  "first_name" varchar NOT NULL,
  "last_name" varchar NOT NULL,
  "department_name" varchar NOT NULL,
  "location" varchar NOT NULL,
  "email_address" varchar NOT NULL
);'''

create_dim_location = '''CREATE TABLE "dim_location" (
  "location_id" int PRIMARY KEY NOT NULL,
  "address_line_1" varchar NOT NULL,
  "address_line_2" varchar,
  "district" varchar,
  "city" varchar NOT NULL,
  "postal_code" varchar NOT NULL,
  "country" varchar NOT NULL,
  "phone" varchar NOT NULL
);'''

create_dim_currency = '''CREATE TABLE "dim_currency" (
  "currency_id" int PRIMARY KEY NOT NULL,
  "currency_code" varchar NOT NULL,
  "currency_name" varchar NOT NULL
);'''

create_dim_design = '''CREATE TABLE "dim_design" (
  "design_id" int PRIMARY KEY NOT NULL,
  "design_name" varchar NOT NULL,
  "file_location" varchar NOT NULL,
  "file_name" varchar NOT NULL
);'''

create_dim_counterparty = '''CREATE TABLE "dim_counterparty" (
  "counterparty_id" int PRIMARY KEY NOT NULL,
  "counterparty_legal_name" varchar NOT NULL,
  "counterparty_legal_address_line_1" varchar NOT NULL,
  "counterparty_legal_address_line_2" varchar,
  "counterparty_legal_district" varchar,
  "counterparty_legal_city" varchar NOT NULL,
  "counterparty_legal_postal_code" varchar NOT NULL,
  "counterparty_legal_country" varchar NOT NULL,
  "counterparty_legal_phone_number" varchar NOT NULL
);'''

alter_fact_table = [
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("created_date") REFERENCES "dim_date" ("date_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("last_updated_date") REFERENCES "dim_date" ("date_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("sales_staff_id") REFERENCES "dim_staff" ("staff_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("counterparty_id") REFERENCES "dim_counterparty" ("counterparty_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("currency_id") REFERENCES "dim_currency" ("currency_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("design_id") REFERENCES "dim_design" ("design_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_payment_date") REFERENCES "dim_date" ("date_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_date") REFERENCES "dim_date" ("date_id");',
    'ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_location_id") REFERENCES "dim_location" ("location_id");'
    ]

parquet_to_sql_queries = [
    create_sales_order,
    create_transaction,
    create_address,
    create_counterparty,
    create_currency,
    create_counterparty,
    create_design,
    create_staff,
    create_purchase_order,
    create_payment,
    create_payment_type,
    create_department,
    *column_comments
]


star_schema_queries = [
    create_fct_sales_order,
    create_dim_staff,
    create_dim_location,
    create_dim_counterparty,
    create_dim_currency,
    create_dim_staff,
    create_dim_date,
    create_dim_design,
    *alter_fact_table
]