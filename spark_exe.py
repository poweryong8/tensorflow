from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import DauCount as dc
import phoenixdb
from impala.util import as_pandas
from sklearn.neural_network import MLPClassifier
import tensorflow as tf

#database and spark-----------------------------------------------------------------------------------------------------
spark = SparkSession \
    .builder \
    .appName("2nd_Log_Job") \
    .getOrCreate();

database_url = 'http://10.107.26.48:12008'
conn = phoenixdb.connect(database_url, autocommit=False)
cursor = conn.cursor()
#-----------------------------------------------------------------------------------------------------------------------
#join data
start=0
end=5
dateData1 = dc.DauCount(2017, 01,01 , start, end)
dateData = dateData1.makeDate()
dateData_dau = dateData1.makeDateX()

cursor.execute("select CHANNELUSERID,DATEDT from tb_user_info where datedt between '%s' and '%s' " %(dateData_dau[start], dateData_dau[end-1]))
join_data = as_pandas(cursor) #join
join_data.columns.values[0]= 'channeluserid'
join_data.columns.values[1]= 'datedt'

cursor.execute("select CHANNELUSERID,datedt,login_time from tb_user_info where datedt>='%s'"%dateData_dau[start] )
login_data = as_pandas(cursor) #login
login_data.columns= login_data.columns.str.lower()

#-----------------------------------------------------------------------------------------------------------------------

buy_item = pd.DataFrame()
social = pd.DataFrame()
get_randombox = pd.DataFrame()
tt_start = pd.DataFrame()
invite_buddy = pd.DataFrame()
tt_play =pd.DataFrame()
change_nickname= pd.DataFrame()
save_avatar = pd.DataFrame()
tt_attend= pd.DataFrame()
buy_randombox=pd.DataFrame()
buy_character = pd.DataFrame()
login =pd.DataFrame()
play_data = pd.DataFrame()


for day in dateData_dau:
    for time in range(0, 24):
        if time < 10:
            DT_1 = spark.read.parquet("hdfs://172.30.24.16:9010/from_1214/%s/0%s/*.parquet" % (day,time))
        else:
            DT_1 = spark.read.parquet("hdfs://172.30.24.16:9010/from_1214/%s/%s/*.parquet" % (day,time))
        DT_1.registerTempTable("log1")
        if DT_1.where("logid=4 and logdetailid=1").count()>0:
            item1 = spark.sql("select channeluserid,usecash,usepoint,itemidx,mypoint, now_2 from log1 where logid=4 and logdetailid=1")
            buy_item = buy_item.append(item1.toPandas())
        if DT_1.where("logid=10 and logdetailid=100").count() > 0:
            social1 = spark.sql("select channeluserid,socialtype,now_2 from log1 where logid=10 and logdetailid=100")
            social = social.append(social1.toPandas())
        if DT_1.where("logid=108 and logdetailid=101").count() > 0:
            random1 = spark.sql("select channeluserid,boxidx,now_2 from log1 where logid=108 and logdetailid=101")
            get_randombox = get_randombox.append(random1.toPandas())
        if DT_1.where("logid=120 and logdetailid=2").count() > 0:
            start1 = spark.sql("select channeluserid, result,tournamenttype, tournamentchannel,buyintype, buyinvalue,r_key, now_2 from log1 where logid=120 and logdetailid=2")
            tt_start = tt_start.append(start1.toPandas())
        if DT_1.where("logid=6 and logdetailid=100").count() > 0:
            invite1 = spark.sql("select channeluserid, hashed_talk_user_id, now_2 from log1 where logid=6 and logdetailid=100")
            invite_buddy = invite_buddy.append(invite1.toPandas())
        if DT_1.where("logid=3 and logdetailid=5").count() > 0:
            ttplay1 = spark.sql("select channeluserid, result,getpoint,mypoint,totalmypoint,prevtotalmypoint, now_2 from log1 where logid=3 and logdetailid=5")
            tt_play = tt_play.append(ttplay1.toPandas())
        if DT_1.where("logid=20 and logdetailid=1").count() > 0:
            nick1 = spark.sql("select channeluserid, prevnickname,updatenickname, now_2 from log1 where logid=20 and logdetailid=1")
            change_nickname = change_nickname.append(nick1.toPandas())
        if DT_1.where("logid=20 and logdetailid=100").count() > 0:
            avatar1 = spark.sql("select channeluserid, prevgender,prevequipitems,updategender,updateequipitems, now_2 from log1 where logid=20 and logdetailid=100")
            save_avatar = save_avatar.append(avatar1.toPandas())
        if DT_1.where("logid=120 and logdetailid=1").count() > 0:
            attend1 = spark.sql("select channeluserid, result,tournamenttype, tournamentchannel,buyintype, buyinvalue,r_key, now_2 from log1 where logid=120 and logdetailid=1")
            tt_attend= tt_attend.append(attend1.toPandas())
        if DT_1.where("logid=108 and logdetailid=100").count() > 0:
            random11 = spark.sql("select channeluserid,boxidx,usecash,mycash,usepoint,mypoint, now_2 from log1 where logid=108 and logdetailid=100")
            buy_randombox = buy_randombox.append(random11.toPandas())
        if DT_1.where("logid=5 and logdetailid=1").count() > 0:
            character1 = spark.sql("select channeluserid,characteridx,usecash,mycash,usepoint,mypoint, now_2 from log1 where logid=5 and logdetailid=1")
            buy_character= buy_character.append(character1.toPandas())
        if DT_1.where("logid=1 and logdetailid=2").count() > 0:
            login1 = spark.sql("select channeluserid,mycash,mypoint, now_2 from log1 where logid=1 and logdetailid=2")
            login= login.append(login1.toPandas())
        if DT_1.where("logid=3 and logdetailid=2").count() > 0:
            play1 = spark.sql("select channeluserid, result,getpoint,mypoint,totalmypoint,prevtotalmypoint, now_2 from log1 where logid=3 and logdetailid=2")
            play_data= play_data.append(play1.toPandas())
    print day

buy_item['datedt'] = pd.to_datetime(buy_item.now_2).dt.strftime('%Y-%m-%d')
social['datedt'] = pd.to_datetime(social.now_2).dt.strftime('%Y-%m-%d')
get_randombox['datedt'] = pd.to_datetime(get_randombox.now_2).dt.strftime('%Y-%m-%d')
tt_start['datedt'] = pd.to_datetime(tt_start.now_2).dt.strftime('%Y-%m-%d')
invite_buddy['datedt'] = pd.to_datetime(invite_buddy.now_2).dt.strftime('%Y-%m-%d')
tt_play['datedt'] = pd.to_datetime(tt_play.now_2).dt.strftime('%Y-%m-%d')
change_nickname['datedt'] = pd.to_datetime(change_nickname.now_2).dt.strftime('%Y-%m-%d')
save_avatar['datedt'] = pd.to_datetime(save_avatar.now_2).dt.strftime('%Y-%m-%d')
tt_attend['datedt'] = pd.to_datetime(tt_attend.now_2).dt.strftime('%Y-%m-%d')
buy_randombox['datedt'] = pd.to_datetime(buy_randombox.now_2).dt.strftime('%Y-%m-%d')
login['datedt'] = pd.to_datetime(login.now_2).dt.strftime('%Y-%m-%d')
play_data['datedt'] = pd.to_datetime(play_data.now_2).dt.strftime('%Y-%m-%d')
#data summary
buy_item_summary = buy_item.groupby(['datedt','channeluserid']).agg({'usecash':'sum', 'usepoint':'sum', 'itemidx':'count'}).reset_index()
buy_item_summary.columns.values[2],buy_item_summary.columns.values[3],buy_item_summary.columns.values[4] = ('buy_item_usepoint','num_buy_itemidx','buy_item_usecash')
social_summary =social.groupby(['datedt','channeluserid']).socialtype.count().reset_index()
social_summary.columns.values[2] = 'num_social_activity'
get_randombox_summary =get_randombox.groupby(['datedt','channeluserid']).boxidx.count().reset_index()
get_randombox_summary.columns.values[2] = 'num_get_randombox'
tt_start_summary = tt_start.groupby(['datedt','channeluserid']).tournamenttype.count().reset_index()
tt_start_summary.columns.values[2]='num_tt_start'
invite_buddy_summary = invite_buddy.groupby(['datedt','channeluserid']).hashed_talk_user_id.count().reset_index()
invite_buddy_summary.columns.values[2]='num_invite_buddy'
tt_play_summary = tt_play.groupby(['datedt','channeluserid']).result.count().reset_index()
tt_play_summary.columns.values[2]='num_tt_play'
change_nickname_summary = change_nickname.groupby(['datedt','channeluserid']).updatenickname.count().reset_index()
change_nickname_summary.columns.values[2]='num_change_nickname'
save_avatar_summary = save_avatar.groupby(['datedt','channeluserid']).updategender.count().reset_index()
save_avatar_summary.columns.values[2]='num_save_avatar'
tt_attend_summary = tt_attend.groupby(['datedt','channeluserid']).r_key.count().reset_index()
tt_attend_summary.columns.values[2]='num_tt_attend'
buy_randombox_summary = buy_randombox.groupby(['datedt','channeluserid']).boxidx.count().reset_index()
buy_randombox_summary.columns.values[2] = 'num_buy_randombox'
login_summary = login.groupby(['datedt','channeluserid']).now_2.count().reset_index()
login_summary.columns.values[2] ='num_login'
play_summary = play_data.groupby(['datedt','channeluserid']).result.count().reset_index()
play_summary.columns.values[2]='num_play'
#merge all data --------------------------------------------------------------------------------------------------------
pred_churn1 =pd.merge(buy_item_summary, social_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn2 =pd.merge(pred_churn1, get_randombox_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn3 =pd.merge(pred_churn2, tt_start_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn4 =pd.merge(pred_churn3, invite_buddy_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn5 =pd.merge(pred_churn4, tt_play_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn6 =pd.merge(pred_churn5, change_nickname_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn7 =pd.merge(pred_churn6, save_avatar_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn8 =pd.merge(pred_churn7, tt_attend_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn9 =pd.merge(pred_churn8, buy_randombox_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn10 =pd.merge(pred_churn9, login_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn11 =pd.merge(pred_churn10, play_summary, on = ['datedt', 'channeluserid'], how='outer')
pred_churn12 = pred_churn11.fillna(0)

#merge nru with all data -----------------------------------------------------------------------------------------------
pred_churn12.channeluserid = pred_churn12.channeluserid.str.lower()
classif_data =pd.merge(join_data, pred_churn12,on = ['datedt','channeluserid'], how='left')
login_data.datedt, login_data.login_time =  pd.to_datetime(login_data.datedt),pd.to_datetime(login_data.login_time)
login_data['diff'] = login_data.login_time - login_data.datedt
classif_data2 =pd.merge(classif_data, login_data, on = 'channeluserid', how='left')
classif_data2['diff'] = classif_data2['diff'].astype('timedelta64[D]')
classif_data3 = classif_data2.drop(['datedt_x','datedt_y','login_time'],1)
classif_data3.columns.values[15] = 'features'
#ml
#-----------------------------------------------------------------------------------------------------------------------
def init_weights(shape):
    return tf.Variable(tf.random_normal(shape, stddev= 0.1))

def model(x,w):
    return tf.matmul(x,w)


from keras.models import Sequential
from keras.layers import Dense, Activation
model = Sequential()
model.add(Dense(output_dim=64, input_dim=100))
model.add(Activation("relu"))
model.add(Dense(output_dim=10))
model.add(Activation("softmax"))
