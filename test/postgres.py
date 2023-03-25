import psycopg2
import pandas.io.sql as sqlio


conn = psycopg2.connect("dbname=postgres user=nadimtellezbarrera")

#Creating a cursor object using the cursor() method
# cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
# cursor.execute("select version()")
# cursor.execute('''SELECT * from res_partner''')

# data = cursor.fetchone()
# print("Connection established to: ",data)

# #Closing the connection
# conn.close()

table_name = "product_product"
# res_partner
# product_product
# sale_order
# stock_location
# stock_inventory
# product_product

# sql = """   SELECT DISTINCT ON (res_partner.user_id) res_partner.user_id, res_partner.name
#             FROM res_partner
#             INNER JOIN sale_order ON res_partner.user_id = sale_order.partner_id"""

# sql = """   SELECT *
#             FROM res_partner
#             WHERE res_partner.customer_rank > 0"""

# sql = """   SELECT *
#             FROM res_partner
#             WHERE res_partner.supplier_rank > 0"""

# sql = """   SELECT DISTINCT ON (sale_order.partner_id) sale_order.partner_id
#             FROM sale_order"""

# sql = """   SELECT DISTINCT ON (purchase_order.partner_id) purchase_order.partner_id
#             FROM purchase_order"""

data = sqlio.read_sql_query("SELECT * FROM " + table_name, conn)
print(data)
data.to_csv("files/" + table_name + ".csv")

# data = sqlio.read_sql_query(sql, conn)
# print(data)
# data.to_csv("customer_rank.csv")
