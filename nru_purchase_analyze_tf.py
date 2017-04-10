#-*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import DauCount as dc
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import concat, col, lit
import tensorflow as tf

spark = SparkSession\
    .builder\
    .appName('2nd_log')\
    .config("spark.sql.parquet.casheMetadata","true")\
    .config("spark.sql.parquet.compression.codec","gzip")\
    .config("spark.sql.shuffle.partitions","250")\
    .getOrCreate()

dateData1 = dc.DauCount(2017, 02, 10, 0, 15)
dateData = dateData1.makeDate()
dateData_dau = dateData1.makeDateX()

#parquet
user_behavior=pd.DataFrame()
new = pd.DataFrame()
log_all = ''
for day in dateData_dau:
    try:
        for time in range(0, 24):
            if time < 10:
                DT_1 = spark.read.parquet("/home/poweryong8/data/%s/0%s/*.parquet" % (day, time))
            else:
                DT_1 = spark.read.parquet("/home/poweryong8/data/%s/%s/*.parquet" % (day, time))
            DT_1.registerTempTable("log1")
            reg_data = spark.sql("select channeluserid,now_2 from log1 where logid=1 and logdetailid=1")
            new = new.append(reg_data.toPandas())
            total_data = spark.sql("select channeluserid,logid,logdetailid,now_2 from log1 where logid!=101 and channeluserid is not null ")
            if log_all=='':
                log_all = total_data
            else:
                log_all = log_all.union(total_data)
        total_v2 = log_all.orderBy('channeluserid','now_2')
        total_v2 = total_v2.withColumn('logid', total_v2['logid'].cast(StringType()))
        total_v2 = total_v2.withColumn('logdetailid', total_v2['logdetailid'].cast(StringType()))
        total_v2 = total_v2.select('channeluserid', 'now_2',concat(col('logid'),lit('-'),col('logdetailid')).alias('id'))
        windowSpec = Window.partitionBy(total_v2['channeluserid']).orderBy(total_v2['now_2'])
        total_v3 = total_v2.select('channeluserid','id', 'now_2', F.row_number().over(windowSpec).alias('num'))
        user_behavior = user_behavior.append(total_v3.toPandas())
    except:
        pass
    print day

##pre-process
#
new['date']= pd.to_datetime(new.now_2).dt.strftime('%Y-%m-%d')
new.columns = ['channeluserid','join_datetime', 'join_date']
user_behavior['date']= pd.to_datetime(user_behavior.now_2).dt.strftime('%Y-%m-%d')
user_behavior = user_behavior.fillna(0)
#new_summary = pd.merge(new, user_behavior, on=('channeluserid','date'), how='left').reset_index()
new_summary = pd.merge(new, user_behavior, on='channeluserid', how='left').reset_index()
new_summary_v1 = new_summary.dropna()
new_summary_v1['date']= pd.to_datetime(new_summary_v1.now_2).dt.strftime('%Y-%m-%d')
new_summary_v1.now_2 = pd.to_datetime(new_summary_v1.now_2)
new_bu = new_summary_v1[new_summary_v1.id == '2-1']
new_bu.join_date = pd.to_datetime(new_bu.join_date)
new_bu.date = pd.to_datetime(new_bu.date)
new_bu['howlong']= new_bu.date - new_bu.join_date
new_bu.howlong = new_bu.howlong.astype('timedelta64[D]')
new_bu_v1 = new_bu[new_bu.howlong<=3]
new_bu_first = new_bu_v1.groupby('channeluserid').now_2.min().reset_index()
new_bu_first.now_2 = pd.to_datetime(new_bu_first.now_2)
new_bu_last = new_bu.groupby('channeluserid').now_2.max().reset_index()
#new user behavior befoer buying first
new_bu_bf_buy = pd.DataFrame() #구매자 데이터
for pid in new_bu_first.channeluserid:
    user = new_summary_v1[new_summary_v1.channeluserid==pid]
    time = new_bu_first[new_bu_first.channeluserid==pid].now_2
    bf_buy = user[user.now_2<time.values[0]]
    new_bu_bf_buy = new_bu_bf_buy.append(bf_buy)

new_bu_bf_buy['buy'] =1
new_bu_bf_buy_v1 = new_bu_bf_buy[['channeluserid', 'id','now_2', 'buy']]

new_bu_behavior= new_bu_bf_buy.groupby(['channeluserid','id']).now_2.count().reset_index()
new_bu_summary = new_bu_behavior.groupby('id').now_2.mean().reset_index()

new_summary_out_bu = new_summary_v1[~new_summary_v1.channeluserid.isin(new_bu.channeluserid)]
new_summary_out_bu.join_date = pd.to_datetime(new_summary_out_bu.join_date)
new_summary_out_bu.date = pd.to_datetime(new_summary_out_bu.date)
new_summary_out_bu['howlong']= new_summary_out_bu.date - new_summary_out_bu.join_date #계정 생성일부터 3일까지 유저를 추리기 위해 계산
new_summary_out_bu.howlong = new_summary_out_bu.howlong.astype('timedelta64[D]')
new_summary_out_bu_v1 = new_summary_out_bu[new_summary_out_bu.howlong<=3]
new_summary_out_bu_v1['buy'] =0
new_summary_out_bu_v2 = new_summary_out_bu_v1[['channeluserid','id','now_2', 'buy']]
new_behavior_out_bu = new_summary_out_bu_v1.groupby(['channeluserid','id']).now_2.count().reset_index()
new_behavior_summary= new_behavior_out_bu.groupby('id').now_2.mean().reset_index()
compare_nbu_nru = pd.merge(new_bu_summary, new_behavior_summary, on ='id', how='left')
#구매자 비구매자 데이터 append
total_list = new_bu_bf_buy_v1.append(new_summary_out_bu_v2)
total_buy_info = total_list[['channeluserid', 'buy']].drop_duplicates()
total_list_pv = total_list.pivot_table(index='channeluserid',columns='id',values='now_2',aggfunc='count').reset_index().fillna(0)
total_list_pv_v1 = pd.merge(total_list_pv, total_buy_info, on='channeluserid', how='outer')
#compare nru with nbu

#tensorflow
from sklearn.cross_validation import train_test_split
tf_train, tf_test = train_test_split(total_list_pv_v1, test_size=0.3)
tf_train = np.array(tf_train.ix[:,1:])
x_tr = tf_train[:,:-1]
y_tr = tf_train[:,-1:]
tf_test= np.array(tf_test.ix[:,1:])
x_te = tf_test[:,:-1]
y_te = tf_test[:,-1:]

x = tf.placeholder(tf.float32, [None,39])
y = tf.placeholder(tf.float32,[None,1])

W1 = tf.Variable(tf.random_normal([39, 40]), name='weight1')
b1 = tf.Variable(tf.random_normal([40]), name='bias1')
layer1 = tf.sigmoid(tf.matmul(x, W1) + b1)

W2 = tf.Variable(tf.random_normal([40, 40]), name='weight2')
b2 = tf.Variable(tf.random_normal([40]), name='bias2')
layer2 = tf.sigmoid(tf.matmul(layer1, W2) + b2)

W3 = tf.Variable(tf.random_normal([40, 40]), name='weight3')
b3 = tf.Variable(tf.random_normal([40]), name='bias3')
layer3 = tf.sigmoid(tf.matmul(layer2, W3) + b3)

W4 = tf.Variable(tf.random_normal([40, 40]), name='weight4')
b4 = tf.Variable(tf.random_normal([40]), name='bias4')
layer4 = tf.sigmoid(tf.matmul(layer3, W4) + b4)

W5 = tf.Variable(tf.random_normal([40, 1]), name='weight5')
b5 = tf.Variable(tf.random_normal([1]), name='bias5')
hypothesis = tf.sigmoid(tf.matmul(layer4, W5) + b5)

#save model
cost = -tf.reduce_mean(y * tf.log(hypothesis) + (1 - y) * tf.log(1 - hypothesis))
train = tf.train.AdamOptimizer(learning_rate=0.01).minimize(cost)

# Accuracy computation
# True if hypothesis>0.5 else False
predicted = tf.cast(hypothesis > 0.5, dtype=tf.float32)
accuracy = tf.reduce_mean(tf.cast(tf.equal(predicted, y), dtype=tf.float32))
#saver = tf.train.Saver()
with tf.Session() as sess:
    # Initialize TensorFlow variables
    sess.run(tf.global_variables_initializer())
    #new_saver = tf.train.import_meta_graph('predict_churn.meta')
    #new_saver.restore(sess, tf.train.latest_checkpoint('./'))
    #print "Model Restored"

    for step in range(500):
        sess.run(train, feed_dict={x: x_tr, y: y_tr})
        if step % 100 == 0:
            print(step, sess.run(cost, feed_dict={x: x_tr, y: y_tr}), sess.run([W1, W2]))

    h, c, a = sess.run([hypothesis, predicted, accuracy], feed_dict={x: x_te, y: y_te})
    #h, c = sess.run([hypothesis, predicted], feed_dict={x: x_te})
    #print("\nHypothesis: ", h, "\nCorrect: ", c)
    #saver.save(sess, 'predict_churn')
    print("\nHypothesis: ", h, "\nCorrect: ", c, "\nAccuracy: ", a)

