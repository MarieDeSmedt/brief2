import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="marie",
    password="marikiki9283",
    database="olist")
mycursor = mydb.cursor()

mycursor.execute("SELECT COUNT(DISTINCT customer_unique_id) FROM Customers")
res = mycursor.fetchone()
total_rows = res[0]
print("\nNombre de clients :")
print(total_rows)

#--------------------------------------------------------------------------------------

mycursor.execute("SELECT COUNT(DISTINCT product_id) FROM Products")
res = mycursor.fetchone()
tot_prod = res[0]
print("\nNombre de produits :")
print(tot_prod)

#--------------------------------------------------------------------------------------

mycursor.execute('''
        SELECT product_category_name,
        COUNT(DISTINCT product_id)
        FROM Products
        GROUP BY product_category_name
''')
res = mycursor.fetchall()
print("\nNombre de produits par catégories :")
i = 0
for x in res:
    i += 1
    print(i, x)

#--------------------------------------------------------------------------------------

mycursor.execute("""
    SELECT COUNT(DISTINCT order_id)
    FROM Orders
""")
res = mycursor.fetchone()
nb_orders = res[0]
print("\nNombre de commandes :")
print(nb_orders)

#--------------------------------------------------------------------------------------

mycursor.execute('''
        SELECT order_status,
        COUNT(DISTINCT order_id)
        FROM Orders
        GROUP BY order_status
''')
res = mycursor.fetchall()
print("\nNombre de commandes par status :")
i = 0
for x in res:
    i += 1
    print(i, x)

#--------------------------------------------------------------------------------------
mycursor.execute('''
        SELECT count(*),MONTH(order_purchase_timestamp)
        FROM Orders
        GROUP BY MONTH(order_purchase_timestamp)
''')
res = mycursor.fetchall()
print("\nNombre de commandes par mois :")
i = 0
for x in res:
    i += 1
    print(i, x)
#--------------------------------------------------------------------------------------

mycursor.execute('''
        SELECT AVG(payment_value)
        FROM Order_payments

''')
res = mycursor.fetchall()
print("\nPrix moyen d'une commande (panier moyen) :")
for x in res:
    print(x)

#--------------------------------------------------------------------------------------

mycursor.execute("""
    SELECT AVG(review_score)
    FROM Order_reviews

""")

res = mycursor.fetchall()
print("\nScore de satisfaction moyen (notation sur la commande):")
print(res[0])

#--------------------------------------------------------------------------------------

mycursor.execute("""
    SELECT *
    FROM Sellers

""")

res = mycursor.fetchall()
print("\nNombre de vendeur: ")
print(len(res))

#--------------------------------------------------------------------------------------

mycursor.execute("""
    SELECT
    DISTINCT seller_city
    FROM Sellers

""")

res = mycursor.fetchall()
print("\nNombre de vendeur par région: ")
i = 0
for x in res:
    i += 1
    print(i, x)

# --------------------------------------------------------------------------------------

mycursor.execute("""
    SELECT  COUNT(p.product_id),p.product_category_name
    FROM Products AS p
    INNER JOIN Order_items AS oi
    ON p.product_id = oi.product_id
    GROUP BY p.product_category_name
    ORDER BY p.product_category_name

""")

res = mycursor.fetchall()
print("\nQuantité de produits  vendus par catégories: ")
for x in res:
    print(x)

# --------------------------------------------------------------------------------------

mycursor.execute('''
        SELECT count(*),DAY(order_purchase_timestamp)
        FROM Orders
        GROUP BY DAY(order_purchase_timestamp)
''')
res = mycursor.fetchall()
print("\nNombre de commandes par jours :")
i = 0
for x in res:
    i += 1
    print(i, x)

# --------------------------------------------------------------------------------------

mycursor.execute('''
        SELECT AVG(DATEDIFF(order_estimated_delivery_date, order_purchase_timestamp))
        AS avg_duration
        FROM Orders
''')
res = mycursor.fetchall()
print("\nDurée moyenne entre la commande et la livraison :")

for x in res:
    print(x)

# --------------------------------------------------------------------------------------

mycursor.execute('''
        SELECT  COUNT(oi.order_id),s.seller_city
        FROM Order_items AS oi
        INNER JOIN Sellers AS s
        ON oi.seller_id = s.seller_id
        GROUP BY s.seller_city
        ORDER BY s.seller_city
''')
res = mycursor.fetchall()
print("\nNombre de commande par ville (ville du vendeur) :")

for x in res:
    print(x)

# --------------------------------------------------------------------------------------

mycursor.execute('''
        SELECT MIN(payment_value)
        FROM Order_payments
''')
res = mycursor.fetchall()
print("\nPrix minimum des commandes:")

for x in res:
    print(x)

# --------------------------------------------------------------------------------------

mycursor.execute('''
    SELECT MAX(sum)
    FROM(
        SELECT order_id, SUM(payment_value) as sum
         FROM Order_payments
         GROUP BY order_id)
    AS sum_table

''')


res = mycursor.fetchall()
print("\nPrix maximum des commandes")

i=0
for x in res:
    i+=1
    print(i,x)

# --------------------------------------------------------------------------------------

    mycursor.execute('''
            SELECT MONTH(order_purchase_timestamp),AVG(DATEDIFF(order_estimated_delivery_date, order_purchase_timestamp))
            FROM Orders
            GROUP BY MONTH(order_purchase_timestamp)
            ORDER BY MONTH(order_purchase_timestamp)
    ''')
    res = mycursor.fetchall()
    print("\nLe temps moyen d'une livraison par mois")

    for x in res:
        print(x)