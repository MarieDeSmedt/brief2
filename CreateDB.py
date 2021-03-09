import pandas as pd
import mysql.connector
from sqlalchemy.types import Integer, String, Float, Date, DateTime
from sqlalchemy import create_engine

mydb = mysql.connector.connect(
    host="localhost",
    user="marie",
    password="marikiki9283",
    database="olist")

engine = create_engine('mysql+pymysql://marie:marikiki9283@localhost/olist')

df_customers = pd.read_csv("csv/olist_customers_dataset.csv")
df_geolocation = pd.read_csv("csv/olist_geolocation_dataset.csv")
df_order_items = pd.read_csv("csv/olist_order_items_dataset.csv")
df_order_payments = pd.read_csv("csv/olist_order_payments_dataset.csv")
df_order_reviews = pd.read_csv("csv/olist_order_reviews_dataset.csv")
df_orders = pd.read_csv("csv/olist_orders_dataset.csv")
df_products = pd.read_csv("csv/olist_products_dataset.csv")
df_sellers = pd.read_csv("csv/olist_sellers_dataset.csv")



try:
    df_order_reviews.to_sql(name="Order_reviews", con=engine, index=False, if_exists='replace',
                            dtype={'review_id': String(255),
                                   'order_id': String(255),
                                   'review_score': Integer,
                                   'review_comment_title': String(255),
                                   'review_comment_message': String(255),
                                   'review_creation_date': DateTime,
                                   'review_answer_timestamp': DateTime})
except ValueError:
    print("Order_reviews existe déjà")
#
try:
    df_customers.to_sql(name="Customers", con=engine, index=False, if_exists='replace',
                        dtype={'customer_id': String(32),
                               'customer_unique_id': String(
                                   32),
                               'customer_zip_code_prefix': String(
                                   5),
                               'customer_city': String(32),
                               'customer_state': String(
                                   2)})
except ValueError:
    print("Customers existe déjà")



try:
    df_geolocation.to_sql(name="Geolocation", con=engine, index=False, if_exists='replace',
                          dtype={'geolocation_zip_code_prefix': String(5),
                                 'geolocation_lat': String(19),
                                 'geolocation_lng': String(5),
                                 'geolocation_city': String(38),
                                 'geolocation_state': String(2)})
except ValueError:
    print("Geolocation existe déjà")

try:
    df_order_items.to_sql(name="Order_items", con=engine, index=False, if_exists='replace',
                          dtype={'order_id': String(32),
                                 'order_item_id': Integer,
                                 'product_id': String(
                                     32),
                                 'seller_id': String(32),
                                 'shopping_limit_date': Date,
                                 'price': Float(7, 2,
                                                None),
                                 'freight_value': Float(
                                     6, 2, None)})
except ValueError:
    print("Order_items existe déjà")

try:
    df_order_payments.to_sql(name="Order_payments", con=engine, index=False, if_exists='replace',
                             dtype={'order_id': String(32),
                                    'payment_sequential': Integer,
                                    'payment_type': String(11),
                                    'payment_installments': Integer,
                                    'payment_value': Float(10, 2, None)})
except ValueError:
    print("Order_payments existe déjà")



try:
    df_orders.to_sql(name="Orders", con=engine, index=False, if_exists='replace', dtype={'order_id': String(32),
                                                                                         'customer_id': String(32),
                                                                                         'order_status': String(11),
                                                                                         'order_purchase_timestamp': DateTime,
                                                                                         'order_approved_at': DateTime,
                                                                                         'order_delivered_carrier_date': DateTime,
                                                                                         'order_delivered_customer_date': DateTime,
                                                                                         'order_estimated_delivery_date': DateTime})
except ValueError:
    print("Orders existe déjà")

try:
    df_products.to_sql(name="Products", con=engine, index=False, if_exists='replace', dtype={'product_id': String(32),
                                                                                             'product_category_name': String(
                                                                                                 46),
                                                                                             'product_name_lenght': Float(
                                                                                                 8, 4, None),
                                                                                             'product_description_lenght': Float(
                                                                                                 12, 6, None),
                                                                                             'product_photos_qty': Float(
                                                                                                 8,
                                                                                                 4,
                                                                                                 None),
                                                                                             'product_weight_g': Float(
                                                                                                 14,
                                                                                                 7,
                                                                                                 None),
                                                                                             'product_length_cm': Float(
                                                                                                 10,
                                                                                                 5,
                                                                                                 None),
                                                                                             'product_height_cm': Float(
                                                                                                 10,
                                                                                                 5,
                                                                                                 None),
                                                                                             'product_width_cm': Float(
                                                                                                 10,
                                                                                                 5,
                                                                                                 None)})
except ValueError:
    print("Products existe déjà")

try:
    df_sellers.to_sql(name="Sellers", con=engine, index=False, if_exists='replace', dtype={'seller_id': String(32),
                                                                                           'seller_zip_code_prefix': String(
                                                                                               5),
                                                                                           'seller_city': String(40),
                                                                                           'seller_state': String(2)})
except ValueError:
    print("Sellers existe déjà")

