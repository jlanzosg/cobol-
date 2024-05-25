"""
  ETL Example using python petl
  JLG 15/10/2021
"""  

# Modules
import petl as etl
import sqlite3
import MySQLdb

# Functions
def drop_table(conn,table):
      sql='drop table if exists ' + table
      cur = conn.cursor()
      cur.execute(sql)
      conn.commit()

# Connections
#   csv
con01_csv='sources\\customers.csv'
con02_csv='sources\\salesrep.csv'
con03_csv='sources\\products.csv'
con04_csv='sources\\promotions.csv'
con05_csv='sources\\orders.csv'
#   xlsx
con01_xlsx='sources\\holidays.xlsx'
#   sqlite
con01_sqlite=sqlite3.connect('etl001.db')
#   MySql
hostname='localhost' ; username='user' ; password='pass' ; database='etl000'
con01_mysql = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database )
con01_mysql.cursor().execute('SET SQL_MODE=ANSI_QUOTES')



# EXTRACT
 
# sample parameter could be 0 when tables are not too big

try:
   con01_data =etl.fromcsv(con01_csv)
   drop_table(con01_sqlite,'customers')
   etl.todb(con01_data, con01_sqlite, 'customers',create=True,dialect='sqlite',commit=True,sample=1000 )
except :
  print ('Error')
try:
   con02_data =etl.fromcsv(con02_csv)
   drop_table(con01_sqlite,'salesrep')
   etl.todb(con02_data, con01_sqlite, 'salesrep',create=True,dialect='sqlite',commit=True,sample=1000)
except :
  print ('Error')
try:
   con03_data =etl.fromcsv(con03_csv)
   drop_table(con01_sqlite,'products')
   etl.todb(con03_data, con01_sqlite, 'products',create=True,dialect='sqlite',commit=True,sample=1000)
except :
  print ('Error')
try:
   con04_data =etl.fromcsv(con04_csv)
   drop_table(con01_sqlite,'promotions')
   etl.todb(con04_data, con01_sqlite, 'promotions',create=True,dialect='sqlite',commit=True,sample=1000)
except :
  print ('Error')
try:
  con05_data =etl.fromcsv(con05_csv)
  drop_table(con01_sqlite,'orders')
  etl.todb(con05_data, con01_sqlite, 'orders',create=True,dialect='sqlite',commit=True,sample=1000)
except :
  print ('Error')
try:
   con06_data =etl.fromxlsx(con01_xlsx)
   drop_table(con01_sqlite,'holidays')
   etl.todb(con06_data, con01_sqlite, 'holidays',create=True,dialect='sqlite',commit=True,sample=1000)
except :
  print ('Error')

# TRANSFORM
# New holiday table with date column in diferent format
drop_table(con01_sqlite,'holidays0')
sql000="create table  holidays0  as "+\
       "  select  "+\
       "    substr(  holiday_date, 9, 2 ) || '-' || "+\
       "    case strftime('%m', datetime(holiday_date) ) "+\
       "      when '01' then 'JAN' "+\
       "      when '02' then 'FEB' "+\
       "      when '03' then 'MAR' "+\
	   "      when '04' then 'APR' "+\
	   "      when '05' then 'MAY' "+\
	   "      when '06' then 'JUN' "+\
	   "      when '07' then 'JUL' "+\
       "      when '08' then 'AUG' "+\
       "      when '09' then 'SEP' "+\
	   "      when '10' then 'OCT' "+\
	   "      when '11' then 'NOV' "+\
	   "      when '12' then 'DEC' "+\
	   "      else '' "+\
       "    end ||'-' || "+\
       "    substr(  holiday_date, 3, 2 ) date, "+\
       "    holiday_description description, is_a_holiday holiday "+\
       "  from holidays "
cur = con01_sqlite.cursor()
cur.execute(sql000)
con01_sqlite.commit()

# LOAD
# Load MYSql table with resumen data from SQLite
sql000="select t1.product_id Product, t3.product_name 'Product Name',t4.promo_id Promo, t2.customer_id Customer, t5.employee_id SalesRep, "+\
       "       t1.order_date Date, t1.order_id OrderID," +\
       "       ROUND((t1.quantity*t1.unit_price),2) Amount, t1.quantity Quantity , t6.holiday  Holiday "+\
       " from orders          t1 "+\
       " join customers       t2 on t2.customer_id = t1.customer_id "+\
       " join products        t3 on t3.product_id  = t1.product_id "+\
       " left join promotions t4 on t4.promo_id    = t1.promo_id "+\
       " join salesrep        t5 on t5.employee_id = t1.sales_rep_id "+\
       " left join holidays0  t6 on t6.date        = t1.order_date "+\
       " group by t1.product_id ,t4.promo_id,t2.customer_id,t5.employee_id,t1.order_date,t1.order_id "

cur = con01_sqlite.cursor()
cur.execute(sql000)
rows = cur.fetchall()
con01_sqlite.commit()
con01_sqlite.close()
headers=['Product','Product Name','Promo','Customer','SalesRep','Date','OrderID','Amount','Quantity','Holiday']
rows.insert(0, headers)

drop_table(con01_mysql,'sales_res')
etl.todb(rows, con01_mysql, 'sales_res',create=True,dialect='mysql',commit=True,sample=1000 ) 

con01_sqlite.close()
con01_mysql.close() 
