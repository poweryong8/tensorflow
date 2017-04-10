from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import DauCount as dc
import phoenixdb
from impala.util import as_pandas
from sklearn.cross_validation import train_test_split
from tensorflow.contrib import learn
import tensorflow as tf

#database and spark-----------------------------------------------------------------------------------------------------
spark = SparkSession\
    .builder\
    .appName('2nd_log')\
    .getOrCreate()

database_url = 'http://10.107.26.48:12008'
conn = phoenixdb.connect(database_url, autocommit=False)
cursor = conn.cursor()
#-----------------------------------------------------------------------------------------------------------------------
#join data
start=0
end=1
dateData1 = dc.DauCount(2017, 03,01 , start, end)
dateData = dateData1.makeDate()
dateData_dau = dateData1.makeDateX()

cursor.execute("select CHANNELUSERID,DATEDT from tb_user_info where datedt between '%s' and '%s' " %(dateData_dau[start], dateData_dau[end-1]))
join_data_re = as_pandas(cursor) #join
join_data_re.columns.values[0]= 'channeluserid'
join_data_re.columns.values[1]= 'datedt'

cursor.execute("select CHANNELUSERID,datedt,login_time from tb_user_info where datedt>='%s'"%dateData_dau[start] )
login_re_data = as_pandas(cursor) #login_re
login_re_data.columns= login_re_data.columns.str.lower()

#-----------------------------------------------------------------------------------------------------------------------

buy_item_re = pd.DataFrame()
social_re = pd.DataFrame()
get_randombox_re = pd.DataFrame()
tt_start_re = pd.DataFrame()
invite_buddy_re = pd.DataFrame()
tt_play_re =pd.DataFrame()
change_nickname_re= pd.DataFrame()
save_avatar_re = pd.DataFrame()
tt_attend_re= pd.DataFrame()
buy_randombox_re=pd.DataFrame()
buy_character_re = pd.DataFrame()
login_re =pd.DataFrame()
play_data_re = pd.DataFrame()

for day in dateData_dau:
    for time in range(0, 24):
        try:
            if time < 10:
                DT_1 = spark.read.parquet("/home/poweryong8/file/4op/%s/0%s/*.parquet" % (day, time))
            else:
                DT_1 = spark.read.parquet("/home/poweryong8/file/4op/%s/%s/*.parquet" % (day, time))
            DT_1.registerTempTable("log1")
            if DT_1.where("logid=4 and logdetailid=1").count()>0:
                item1 = spark.sql("select channeluserid,usecash,usepoint,itemidx,mypoint, now_2 from log1 where logid=4 and logdetailid=1")
                buy_item_re = buy_item_re.append(item1.toPandas())
            if DT_1.where("logid=10 and logdetailid=100").count() > 0:
                social_re1 = spark.sql("select channeluserid,socialtype,now_2 from log1 where logid=10 and logdetailid=100")
                social_re = social_re.append(social_re1.toPandas())
            if DT_1.where("logid=108 and logdetailid=101").count() > 0:
                random1 = spark.sql("select channeluserid,boxidx,now_2 from log1 where logid=108 and logdetailid=101")
                get_randombox_re = get_randombox_re.append(random1.toPandas())
            if DT_1.where("logid=120 and logdetailid=2").count() > 0:
                start1 = spark.sql("select channeluserid, result,tournamenttype, tournamentchannel,buyintype, buyinvalue,r_key, now_2 from log1 where logid=120 and logdetailid=2")
                tt_start_re = tt_start_re.append(start1.toPandas())
            if DT_1.where("logid=6 and logdetailid=100").count() > 0:
                invite1 = spark.sql("select channeluserid, hashed_talk_user_id, now_2 from log1 where logid=6 and logdetailid=100")
                invite_buddy_re = invite_buddy_re.append(invite1.toPandas())
            if DT_1.where("logid=3 and logdetailid=5").count() > 0:
                ttplay1 = spark.sql("select channeluserid, result,getpoint,mypoint,totalmypoint,prevtotalmypoint, now_2 from log1 where logid=3 and logdetailid=5")
                tt_play_re = tt_play_re.append(ttplay1.toPandas())
            if DT_1.where("logid=20 and logdetailid=1").count() > 0:
                nick1 = spark.sql("select channeluserid, prevnickname,updatenickname, now_2 from log1 where logid=20 and logdetailid=1")
                change_nickname_re = change_nickname_re.append(nick1.toPandas())
            if DT_1.where("logid=20 and logdetailid=100").count() > 0:
                avatar1 = spark.sql("select channeluserid, prevgender,prevequipitems,updategender,updateequipitems, now_2 from log1 where logid=20 and logdetailid=100")
                save_avatar_re = save_avatar_re.append(avatar1.toPandas())
            if DT_1.where("logid=120 and logdetailid=1").count() > 0:
                attend1 = spark.sql("select channeluserid, result,tournamenttype, tournamentchannel,buyintype, buyinvalue,r_key, now_2 from log1 where logid=120 and logdetailid=1")
                tt_attend_re= tt_attend_re.append(attend1.toPandas())
            if DT_1.where("logid=108 and logdetailid=100").count() > 0:
                random11 = spark.sql("select channeluserid,boxidx,usecash,mycash,usepoint,mypoint, now_2 from log1 where logid=108 and logdetailid=100")
                buy_randombox_re = buy_randombox_re.append(random11.toPandas())
            if DT_1.where("logid=5 and logdetailid=1").count() > 0:
                character1 = spark.sql("select channeluserid,characteridx,usecash,mycash,usepoint,mypoint, now_2 from log1 where logid=5 and logdetailid=1")
                buy_character_re= buy_character_re.append(character1.toPandas())
            if DT_1.where("logid=1 and logdetailid=2").count() > 0:
                login_re1 = spark.sql("select channeluserid,mycash,mypoint, now_2 from log1 where logid=1 and logdetailid=2")
                login_re= login_re.append(login_re1.toPandas())
            if DT_1.where("logid=3 and logdetailid=2").count() > 0:
                play1 = spark.sql("select channeluserid, result,getpoint,mypoint,totalmypoint,prevtotalmypoint, now_2 from log1 where logid=3 and logdetailid=2")
                play_data_re= play_data_re.append(play1.toPandas())
        except:
            pass
    print day

buy_item_re['datedt'] = pd.to_datetime(buy_item_re.now_2).dt.strftime('%Y-%m-%d')
social_re['datedt'] = pd.to_datetime(social_re.now_2).dt.strftime('%Y-%m-%d')
get_randombox_re['datedt'] = pd.to_datetime(get_randombox_re.now_2).dt.strftime('%Y-%m-%d')
tt_start_re['datedt'] = pd.to_datetime(tt_start_re.now_2).dt.strftime('%Y-%m-%d')
invite_buddy_re['datedt'] = pd.to_datetime(invite_buddy_re.now_2).dt.strftime('%Y-%m-%d')
tt_play_re['datedt'] = pd.to_datetime(tt_play_re.now_2).dt.strftime('%Y-%m-%d')
change_nickname_re['datedt'] = pd.to_datetime(change_nickname_re.now_2).dt.strftime('%Y-%m-%d')
save_avatar_re['datedt'] = pd.to_datetime(save_avatar_re.now_2).dt.strftime('%Y-%m-%d')
tt_attend_re['datedt'] = pd.to_datetime(tt_attend_re.now_2).dt.strftime('%Y-%m-%d')
buy_randombox_re['datedt'] = pd.to_datetime(buy_randombox_re.now_2).dt.strftime('%Y-%m-%d')
login_re['datedt'] = pd.to_datetime(login_re.now_2).dt.strftime('%Y-%m-%d')
play_data_re['datedt'] = pd.to_datetime(play_data_re.now_2).dt.strftime('%Y-%m-%d')
#data summary
buy_item_re_summary = buy_item_re.groupby(['datedt','channeluserid']).agg({'usecash':'sum', 'usepoint':'sum', 'itemidx':'count'}).reset_index()
buy_item_re_summary.columns.values[2],buy_item_re_summary.columns.values[3],buy_item_re_summary.columns.values[4] = ('buy_item_re_usepoint','num_buy_item_reidx','buy_item_re_usecash')
social_re_summary =social_re.groupby(['datedt','channeluserid']).socialtype.count().reset_index()
social_re_summary.columns.values[2] = 'num_social_re_activity'
get_randombox_re_summary =get_randombox_re.groupby(['datedt','channeluserid']).boxidx.count().reset_index()
get_randombox_re_summary.columns.values[2] = 'num_get_randombox_re'
tt_start_re_summary = tt_start_re.groupby(['datedt','channeluserid']).tournamenttype.count().reset_index()
tt_start_re_summary.columns.values[2]='num_tt_start_re'
invite_buddy_re_summary = invite_buddy_re.groupby(['datedt','channeluserid']).hashed_talk_user_id.count().reset_index()
invite_buddy_re_summary.columns.values[2]='num_invite_buddy_re'
tt_play_re_summary = tt_play_re.groupby(['datedt','channeluserid']).result.count().reset_index()
tt_play_re_summary.columns.values[2]='num_tt_play_re'
change_nickname_re_summary = change_nickname_re.groupby(['datedt','channeluserid']).updatenickname.count().reset_index()
change_nickname_re_summary.columns.values[2]='num_change_nickname_re'
save_avatar_re_summary = save_avatar_re.groupby(['datedt','channeluserid']).updategender.count().reset_index()
save_avatar_re_summary.columns.values[2]='num_save_avatar_re'
tt_attend_re_summary = tt_attend_re.groupby(['datedt','channeluserid']).r_key.count().reset_index()
tt_attend_re_summary.columns.values[2]='num_tt_attend_re'
buy_randombox_re_summary = buy_randombox_re.groupby(['datedt','channeluserid']).boxidx.count().reset_index()
buy_randombox_re_summary.columns.values[2] = 'num_buy_randombox_re'
login_re_summary = login_re.groupby(['datedt','channeluserid']).now_2.count().reset_index()
login_re_summary.columns.values[2] ='num_login_re'
play_summary_re = play_data_re.groupby(['datedt','channeluserid']).result.count().reset_index()
play_summary_re.columns.values[2]='num_play'
#merge all data --------------------------------------------------------------------------------------------------------
pred_churn_re1 =pd.merge(buy_item_re_summary, social_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re2 =pd.merge(pred_churn_re1, get_randombox_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re3 =pd.merge(pred_churn_re2, tt_start_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re4 =pd.merge(pred_churn_re3, invite_buddy_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re5 =pd.merge(pred_churn_re4, tt_play_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re6 =pd.merge(pred_churn_re5, change_nickname_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re7 =pd.merge(pred_churn_re6, save_avatar_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re8 =pd.merge(pred_churn_re7, tt_attend_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re9 =pd.merge(pred_churn_re8, buy_randombox_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re10 =pd.merge(pred_churn_re9, login_re_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re11 =pd.merge(pred_churn_re10, play_summary_re, on = ['datedt', 'channeluserid'], how='outer')
pred_churn_re12 = pred_churn_re11.fillna(0)

#merge nru with all data -----------------------------------------------------------------------------------------------
pred_churn_re12.channeluserid = pred_churn_re12.channeluserid.str.lower()
classif_data_re =pd.merge(join_data_re, pred_churn_re12,on = ['datedt','channeluserid'], how='left')
login_re_data.datedt, login_re_data.login_re_time =  pd.to_datetime(login_re_data.datedt),pd.to_datetime(login_re_data.login_time)
login_re_data['diff'] = login_re_data.login_re_time - login_re_data.datedt
classif_data_re2 =pd.merge(classif_data_re, login_re_data, on = 'channeluserid', how='left')
classif_data_re2['diff'] = classif_data_re2['diff'].astype('timedelta64[D]')
classif_data_re3 = classif_data_re2.drop(['datedt_x','datedt_y','login_time'],1)
classif_data_re3.columns.values[15] = 'features'
classif_data_re3 = classif_data_re3.fillna(0)

classif_data_re3['survival']=0
for i in range(0,len(classif_data_re3)):
    if classif_data_re3.features.values[i]>2:
        classif_data_re3['survival'].values[i]=1

total_info_re = classif_data_re3.drop(['channeluserid','features'],1)
total_info_re.ix[:,0:14]=total_info_re.ix[:,0:14].astype(np.float32)
total_info_re.survival =total_info_re.survival.astype(int)
total_info_train_re, total_info_test_re = train_test_split(total_info_re, test_size=0.3)
#tensorflow
total_train_re = np.array(total_info_train_re)
x_train_re = total_train_re[:,:-1]
y_train_re = total_train_re[:,-1:]
total_test_re = np.array(total_info_test_re)
x_te_re = total_test_re[:,:-1]
y_te_re = total_test_re[:,-1:]

x = tf.placeholder(tf.float32, [None,14])
y = tf.placeholder(tf.float32,[None,1])

W1 = tf.Variable(tf.random_normal([14, 40]), name='weight1')
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
    new_saver = tf.train.import_meta_graph('predict_churn.meta')
    new_saver.restore(sess, tf.train.latest_checkpoint('./'))
    print "Model Restored"

    for step in range(300):
        sess.run(train, feed_dict={x: x_train_re, y: y_train_re})
        if step % 100 == 0:
            print(step, sess.run(cost, feed_dict={x: x_train_re, y: y_train_re}), sess.run([W1, W2]))


    h, c, a = sess.run([hypothesis, predicted, accuracy], feed_dict={x: x_te_re, y: y_te_re})
    h, c = sess.run([hypothesis, predicted], feed_dict={x: x_te_re})
    print("\nHypothesis: ", h, "\nCorrect: ", c)
    #saver.save(sess, 'predict_churn')
    #print("\nHypothesis: ", h, "\nCorrect: ", c, "\nAccuracy: ", a)

