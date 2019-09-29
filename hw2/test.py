import tensorflow as tf
init_op = tf.global_variables_initializer()
a = tf.constant([[1],[2],[3],[4]],dtype=float)
b = tf.constant([[1],[1],[1],[1]],dtype=float)
c = tf.Variable([0,0,0,0])
beta = 0.85
d = tf.sparse.SparseTensor(indices=[[0,0],[1,2]],values=[1,2],dense_shape=[2,4])
mat = tf.sparse.SparseTensor(indices=[[1,0],[2,0],[3,0],[3,1],[0,2],[1,3]],values=[0.85*0.33,0.85*0.33,0.85*0.33,0.85,0.85,0.85],dense_shape=[4,4])
neg_beta = 0.15/4
prev_rank = tf.get_variable("prev_rank",dtype=float,initializer=tf.constant([[1/4] for _ in range(4)]))
curr_rank = tf.get_variable("curr_rank",dtype=float,initializer=tf.constant([[1/4] for _ in range(4)]))
i=0
with tf.Session() as sess:
    sess.run(init_op)
    while True:
        print(i)
        i+=1
        prev_rank = curr_rank
        curr_rank = tf.add(tf.sparse.sparse_dense_matmul(mat, prev_rank),
                           tf.fill(dims=[4, 1], value=tf.reduce_sum(tf.scalar_mul(neg_beta, prev_rank))))
        tf.global_variables_initializer().run()
        #print(sess.run(curr_rank))
        if sess.run(tf.reduce_sum(tf.abs(curr_rank-prev_rank)))<0.00001:
            print("Here")
            print(sess.run(curr_rank))
            break