from datetime import datetime
import pandas as pd
import re
import pymongo
import Credential
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector


#Read text file
Log_file=r'C:\Users\Personal\Downloads\mbox.txt'
f=open(Log_file)
Data=f.read()
 
 
#Fetch Email,Date from Data
Email_Pattern=r'From:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
Date_Pattern=r'Date:\s*(\w{3},?\s*\d{1,2}\s*\w{3}\s*\d{4}\s*\d{2}:\d{2}:\d{2}\s*[-+]\d{4})'
 
 
email=re.findall(Email_Pattern,Data)
date=re.findall(Date_Pattern,Data)
 
 
#Format the date
formatted_date_list=[]
for i in date:
# Parse the date string into a datetime object
    dt = datetime.strptime(i, "%a, %d %b %Y %H:%M:%S %z")
 
# Format the datetime object to the desired format
    formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
    formatted_date_list.append(formatted_date)
   
 
 
#Zip the two varibale data & Convert to dictionary
DB_UploaddData=[]
for email,date in zip(email,formatted_date_list):
    DB_UploaddData.append(({'Email': email, 'Formatted_date': date}))
   
 
   
#MongoDb
#Mongo DB Credential
MongoDb_Crendiential=Credential.Mongo_DB
 
Mongo_Username=MongoDb_Crendiential['Username']
Mongo_Password=MongoDb_Crendiential['Password']
Mongo_NewDB=MongoDb_Crendiential['MongoDB']
Mongo_Collection=MongoDb_Crendiential['Collection']
 
 
#insert email & Date data to MongoDB
# Mongo DB connection

client = pymongo.MongoClient(f"mongodb+srv://{Mongo_Username}:{Mongo_Password}@cluster0.mfh68.mongodb.net/{Mongo_NewDB}?retryWrites=true&w=majority&appName=Cluster0")
print(client)
newdb=client[Mongo_NewDB]  #Database
 
Collection=newdb[Mongo_Collection]
 
#Collection.insert_many(DB_UploaddData) #upload & covert data to mongoDB database
 
 
# Fetch data from MongoDB
mongo_data = list(Collection.find({}))  # Fetch all documents from the collection
 
 
# Convert the fetched data to DataFrame
#Convert Dict to Data Frame for upload Mongo Db to MySql
df = pd.DataFrame(mongo_data)
 
 
 
# Remove the MongoDB _id field if present
if '_id' in df.columns:
    df.drop(columns=['_id'], inplace=True)
 
print(df)
 
#Fetch Data from MongoDB and to Upload in MYSQL server
# mongoDB to MYSQL DB
 
#MYSQL Credential
 
MySql_Credential=Credential.R_Mysql_credential
password=Credential.MYSQL_CREDENTIALS
 
MySql_User=MySql_Credential['username']
MySql_Password=MySql_Credential['password']
MySql_DataBase=MySql_Credential['Database']
MySql_Host=MySql_Credential['host']
MySql_Port=MySql_Credential['port']
 
 
 
# Create Engine for Upload data Mysql server
engine=sqlalchemy.create_engine(f"mysql+mysqlconnector://{MySql_User}:{MySql_Password}@{MySql_Host}:{MySql_Port}/{MySql_DataBase}")



# Insert data into MySQL table
df.to_sql('user_history', con=engine, if_exists='append', index=False)
