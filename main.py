
import numpy as np
import pandas as pd
from datetime import datetime, date
#import googleads
import time
from sqlalchemy import create_engine, event
from datetime import datetime, date, timedelta
import pandas as pd
#import random


CONFIG = {
    #'database': 'postgresql://mike:mike@10.125.0.3:5432/mike', # PROD
    #'database': 'postgresql://mike:mike@35.230.140.62:5432/mike', #prod
    #'database': 'postgresql://mike:mike@35.197.200.111:5432/mike', #stage
    #'database': 'postgresql://mike:mike@14.140.104.62:5432/mike', #local
    'database': 'postgresql://mike:mike@35.197.253.86:5432/mike', # QA
    'schema': 'adwords',
}

class Database(object):
    def __init__(self, db, schema, syncDate):
        self.engine = create_engine(db)
        self.connection = self.engine.connect()
        self.schema = schema
        self.syncDate = syncDate
        self.date = f"'{self.syncDate}'"
        self.initializedTime = datetime.now()

    def RunQuery(self, query):
        return pd.read_sql(query, self.engine)
    
    def get_tables(self,query):
        return pd.read_sql(query, self.engine)
    
    def check_duplicacy(self, table_list, date):
        distinct_data_list=[]
        all_data_list=[]
        date=date
        for a in table_list:
            dist_data_count=db.RunQuery(f'''select count(*) from (select distinct * from adwords."{a}") as x where "date" >='{date}' ''')
            distinct_data_list.append(str(dist_data_count)+" = "+a)
            time.sleep(10)
            all_data_count=db.RunQuery(f'''select count(*) from adwords."{a}" where "date" >='{date}' ''')
            all_data_list.append(str(all_data_count)+" = "+a)
            time.sleep(10)
        return all_data_list, distinct_data_list
    
    def check_update(self,date_test):
        df=pd.read_sql(f'''SELECT a.account_id , a.status , b.account_id, b.status, a.created_at, a.updated_at FROM adwords."changes" as a inner join adwords."accounts" as b on a.account_id = b.account_id where a."date"='{date_test}' and a.status=0 and b.status=0 limit 50''', self.engine)
        return df
        
        
    def has_changed(self, date):
        df=pd.read_sql(f'''select distinct account_id from adwords.changes where date ='{date}' and has_changed='true' ''', self.engine)
        return df
    
    def changed_accounts(self,date):
        df= pd.read_sql(f'''select distinct account_name, mcc_name from adwords.accounts where account_id in (select distinct account_id from adwords.changes where date ='{date}' and has_changed='true' and status=0 ) limit 5''', self.engine)
        return df
    
    def changed_KPIs(self, date, account_name_list):
        dataframes=[]
        for account_name in account_name_list:
            df=pd.read_sql(f'''select sum(changed_keywords) as changed_keywords, sum(changed_ads) as changed_ads, sum(removed_keywords) as removed_keywords  from adwords."changes" where date='{date}' and account_name ='{account_name}' and has_changed='true' and status =0''', self.engine)
            dataframes.append(df)
        return dataframes

print("Start here")
db = Database(CONFIG['database'], CONFIG['schema'], date.today())

#tbal = db.RunQuery('select * from pg_catalog.pg_tables')
table_df= db.RunQuery("select tablename from pg_catalog.pg_tables where schemaname='adwords' and tableowner='mike'")

table_list=table_df['tablename'].tolist()

sizeOfTables=len(table_list)

print("total number of accounts are", sizeOfTables)

short_list=['tmp_dailyScore','tmp_devicePerformanceReport','tmp_lastChanges']


'''listname= input("enter the name of list  ")
date= input('enter date yy-mm-dd  ')
method_name= input('enter method name ')
if listname=='short_list' and method_name=='check_duplicacy':
    lst1, lst2 =db.check_duplicacy(short_list,date)
print("all data list= " , lst1)
print("distinct data list= ", lst2)'''

'''i=1
while(i>=0):
    method_name=input("enter methodname  ")
    date= input("enter date yy-mm--dd  ")

    if method_name=='check_update':
        df=db.check_update(date_test=date)
        print(df)   
    else:
        print("wrong input")'''
    
response=input(" want to see the changed accounts  ")
date = input("enter date :  ")
if response.lower()=='yes' or 'y':
    list_of_accounts=db.has_changed(date)
    print(list_of_accounts)

reponse= input('want to see the changed accounts names ')
if response.lower()=='yes' or 'y':
    name_of_accounts=db.changed_accounts(date)
    print(name_of_accounts)

df=db.changed_accounts(date='2019-08-26')

account_name_list=df['account_name'].unique()

changed_kywords_df_list=db.changed_KPIs(date='2019-08-26', account_name_list=account_name_list)



        

