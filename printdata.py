import pypyodbc as pyodbc
import pandas as pan
import yaml
import smtplib
from email.message import EmailMessage
import sys

#load the yaml file with configs

with open("configfile.yml","r") as yamlfile:
     config=yaml.load(yamlfile, Loader=yaml.FullLoader)


def SendEmail(body):
    message = EmailMessage()
    message['Subject'] =config['email']['subject']
    message['From'] = config['email']['from']
    message['To'] = config['email']['to']
    message.set_content(str(body))
    smtpconn=smtplib.SMTP(config['email']['smtp'])
    smtpconn.send_message(message)
    smtpconn.quit()


server_host = config['mssql']['server_host']
db_name = config['mssql']['db_name']
db_user = config['mssql']['db_user']
db_password = config['mssql']['db_password']


#establish connection to the server
connection_string = 'Driver={SQL Server};Server=' + server_host + ';Database=' + db_name + ';UID=' + db_user + ';PWD=' + db_password + ';'
db = pyodbc.connect(connection_string)
SQL = 'select top 100 * from [EQCAS].[dbo].[cas_report_template]'
db.cursor().execute(SQL)

# the data from sql, manipulate with pandas

data=pan.read_sql(SQL,db)
pan.DataFrame=data
column=data['name']
column=column.to_list()

if 'Accounts' in column:
     value=column.index('Accounts')
     column[value]=str(column[value]).strip('[]')
#column=[]

SendEmail(column[value])
print(column[value])

