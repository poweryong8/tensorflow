import tensorflow as tf
from tensorflow.contrib import skflow
import numpy as np
import pandas as pd
from impala.util import as_pandas
import phoenixdb
import DauCount as dc

dateData1 = dc.DauCount(2017, 01, 10,0,10)
dateData = dateData1.makeDateX()

database_url = 'http://10.107.26.48:12008'
conn = phoenixdb.connect(database_url, autocommit=False)
cursor = conn.cursor()

payment=pd.DataFrame()
for day in dateData:
    cursor.execute("select * from tb_user_currency where datedt ='%s' "% day)
    df_payment = as_pandas(cursor)
    payment =payment.append(df_payment)
    print day

payment.head()
x_train = payment
