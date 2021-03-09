import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="marie",
    password="marikiki9283",
    database="olist")
mycursor = mydb.cursor()

mycursor.execute('''
    INSERT INTO Products VALUES ('product_id','product_category_name',4.21,8.77,4.23,44,55,66,77)   
''')

mydb.commit()

mycursor.execute('''
    SELECT *
    FROM Products
    WHERE product_id = 'product_id'
''')
res = mycursor.fetchall()
for x in res:
    print(x)



