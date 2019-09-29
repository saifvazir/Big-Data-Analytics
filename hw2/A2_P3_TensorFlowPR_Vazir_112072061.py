import sys
import tensorflow as tf
import numpy as np
f_name = sys.argv[1]

arr = np.loadtxt(f_name)
# idx = []
m=0
d = {}
print("Read lines")
for i in range(arr.shape[0]):
    a = int(arr[i,0])
    b = int(arr[i,1])
    if a>m:
        m=a
    if b>m:
        m=b
    #idx.append([b,a])
    d[a] = d.get(a,0)+1
arr = np.roll(arr,1,axis=1)
# for each in lines:
#     a = int(each.split("\t")[0])
#     b = int(each.split("\t")[1].strip("\n"))
#     if a>m:
#         m=a
#     if b>m:
#         m=b
#     idx.append([b,a])
#     d[a] = d.get(a,0)+1
print("reading done")
#c = len(idx)
c = arr.shape[0]
beta = 0.85
mat = tf.sparse.SparseTensor(indices=arr,values=[beta/d[each[1]] for each in arr],dense_shape=[m+1,m+1])
neg_beta = tf.constant(0.15*(1/(m+1)))
#del(idx)
del(d)
init_op = tf.global_variables_initializer()
prev_rank = tf.get_variable("prev_rank",dtype=float,initializer=tf.constant([[1/(m+1)] for _ in range(m+1)]))
curr_rank = tf.get_variable("curr_rank",dtype=float,initializer=tf.constant([[1/(m+1)] for _ in range(m+1)]))
i=0
rank_vector_final = []
with tf.Session() as sess:
    sess.run(init_op)
    while True:
        print(i)
        i+=1
        prev_rank = curr_rank
        curr_rank = tf.add(tf.sparse.sparse_dense_matmul(mat,prev_rank),tf.fill(dims=[m+1,1],value=neg_beta*(tf.reduce_sum(prev_rank))))
        tf.global_variables_initializer().run()
        if sess.run(tf.reduce_sum(tf.abs(curr_rank-prev_rank))) < (10**-8):
            #print("Final rank vector")
            rank_vector_final = sess.run(curr_rank)
            break
node_list = sorted([[i,rank_vector_final[i]] for i in range(len(rank_vector_final))],key=lambda x:x[1])
bottom_20 = node_list[:20]
top_20 = node_list[-20:]
print("top 20 are ",top_20[::-1])
print("bottom 20 are ",bottom_20)
