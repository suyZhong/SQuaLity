import mysql.connector
import cx_Oracle





dsn_tns = cx_Oracle.makedsn('cs5322-7-i.comp.nus.edu.sg', '1521', service_name='cs5322.comp.nus.edu.sg')
conn = cx_Oracle.connect(user='system', password = 'cs5322database', dsn=dsn_tns)

c= conn.cursor()
c.execute('SELECT * FROM STUDENT')
for row in c:
    print(row[0], '-', row[1])