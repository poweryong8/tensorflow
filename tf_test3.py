import tensorflow as tf

sess = tf.Session()
new_saver=tf.train.import_meta_graph('predict_churn.meta')
new_saver.restore(sess, tf.train.latest_checkpoint('./'))
all_vars = tf.global_variables()
for v in all_vars:
    v_=sess.run(v)
    print("This is {} with value:{}".format(v.name, v_))