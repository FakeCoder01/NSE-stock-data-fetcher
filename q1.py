import psycopg2, os
import pandas as pd
import datetime
from datetime import date
from jugaad_data.nse import bhavcopy_save


# creates the tables for two data files
def check_and_create_table(cursor):
    created = True
    try:
        sql = '''CREATE TABLE security_db(index int, SYMBOL varchar(30) NOT NULL,\
        "NAME OF COMPANY" varchar(200),\
        SERIES varchar(4),\
        "DATE OF LISTING" varchar(20),\
        "PAID UP VALUE" bigint,\
        "MARKET LOT" bigint,\
        "ISIN NUMBER" varchar(20),\
        "FACE VALUE" bigint);'''
        cursor.execute(sql)
    except:
        created = False
    try:
        sql = '''CREATE TABLE bhavcopy_db(SYMBOL varchar(30) NOT NULL,\
        SERIES varchar(4),\
        OPEN float,\
        HIGH float,\
        LOW float,\
        CLOSE float,\
        LAST float,\
        PREVCLOSE float,\
        TOTTRDQTY float,\
        TOTTRDVAL float,\
        TIMESTAMP date,\
        TOTALTRADES float, ISIN varchar(30));'''
        cursor.execute(sql)
    except:
        created = False
    return True    


# gets todays date
now = datetime.datetime.today()


# connects to the database [I'm using PostgreSQL]
conn = psycopg2.connect(
        database="postgres",
        user='postgres', 
        password='root',
        host='127.0.0.1', 
        port='5432'
      )



conn.autocommit = True
cursor = conn.cursor()


# downloads the “Securities available for Equity segment (.csv)” file and saves it as "data_csv.csv"
url = 'https://archives.nseindia.com/content/equities/EQUITY_L.csv'
df = pd.read_csv(url).drop_duplicates(keep='first').reset_index()
df.to_csv("data_csv.csv", index=False)


# downloads the latest “bhavcopy file (.csv)” file and saves it as "bhav_csv.csv" temporarily
while True:
    try:
        saved_file = bhavcopy_save(date(now.year, now.month, now.day), ".")
        os.rename(saved_file, 'bhav_csv.csv')
        break
    except:
        # if no files for today checks for the previous day
        now = now - datetime.timedelta(days=1) 


# save the latest “bhavcopy file (.csv)” file as "bhavcopy.csv"
with open("bhav_csv.csv") as f_in, open("bhavcopy.csv", 'w') as f_out:
    header = f_in.readline()
    f_out.write(header)
    for line in f_in:
        f_out.write(line.replace(",\n" , "\n"))

# delete the temporary "bhav_csv.csv" file        
os.remove('bhav_csv.csv')


# checks for the database tables
if check_and_create_table(cursor):


    # insert the “Securities available for Equity segment (.csv)” file which is saved as "data_csv.csv" to the database table `security_db`
    sql = '''COPY security_db (index, SYMBOL,"NAME OF COMPANY", SERIES, "DATE OF LISTING", "PAID UP VALUE", "MARKET LOT", "ISIN NUMBER", "FACE VALUE") \
    FROM 'C:\\FULL_PATH\\data_csv.csv' \
    DELIMITER ',' \
    CSV HEADER;'''
    cursor.execute(sql)
    os.remove('data_csv.csv') # delete the "data_csv.csv" file

    # insert the “bhavcopy file (.csv)” file which is saved as "bhav_csv.csv" to the database table `bhavcopy_db`
    sql = '''COPY bhavcopy_db ( SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN) \
    FROM 'C:\\FULL_PATH\\bhavcopy.csv' \
    DELIMITER ',' \
    CSV HEADER;'''
    cursor.execute(sql)
    os.remove('bhavcopy.csv') # delete the "bhavcopy_db.csv" file

    # query to database for the top 25 gainers using the formula
    sql = '''select bhavcopy_db.SYMBOL, bhavcopy_db.SERIES, ( bhavcopy_db.CLOSE - bhavcopy_db.OPEN ) / bhavcopy_db.OPEN as "gain" from bhavcopy_db ORDER BY gain DESC LIMIT 25;'''
    cursor.execute(sql)
    top_gainers = cursor.fetchall()

    # loop and print the result
    for row in top_gainers:
        print(f"Symbol : {row[0]}\t Series : {row[1]}\t Gain : {row[2]}")

    conn.commit()
    conn.close()

