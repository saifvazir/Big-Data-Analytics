from pyspark import SparkContext,SparkConf
import csv
import numpy as np
from scipy import stats
import sys
def create_county(data):
    k = data[0]
    mortality = data[24]
    income = data[23]
    if k=="fips":
        return []
    else:
        return [(str(int(k)),(mortality,income))]

def create_words(data):
    k = data[0]
    word = data[1]
    rel_freq = data[3]
    if k=="group_id":
        return []
    else:
        return [(str(int(k)),(word,rel_freq))]

def map_to_words(data):
    id = data[0]
    word = data[1][0][0]
    freq = data[1][0][1]
    mortality = data[1][1][0]
    income = data[1][1][1]
    return (word,(id,freq,mortality,income))

def linear_regression(data):
    #word = data[0]
    #list_counties = data[1]
    x = []
    y = []
    for each in data:
        #print(each)
        x.append(float(each[1]))
        y.append(float(each[2]))
    X = np.reshape(x,(len(x),1))
    Y = np.reshape(y,(len(y),1))
    m = np.mean(X)
    dev = np.std(X)
    m_y = np.mean(Y)
    dev_y = np.std(Y)
    if len(x)>1 and dev!=0:
        X = (X-m)/dev
        Y = (Y-m_y)/dev_y
    X = np.hstack((X,np.ones((len(x),1))))
    X_t = np.transpose(X)
    X_inv = np.linalg.pinv(np.dot(X_t,X))
    w = np.dot(X_inv,X_t)
    weights = np.dot(w,Y)
    df = len(x) - 2
    if df <=0:
        return (weights[0,0],-1,-1)
    rss = np.sum(np.power((Y - np.dot(X,weights)),2))
    s_squared = rss/df
    se = np.sum(np.power((X[:,0]),2))
    tt = (weights[0,0]/np.sqrt(s_squared/se))
    pval = stats.t.sf(np.abs(tt), df) * 2
    return (weights[0,0],pval)


###This function is obsolete
def multivariate_regression(data):
    #word = data[0]
    #list_counties = data[1]
    x = []
    y = []
    for each in data:
        #print(each)
        x.append([float(each[1]),float(each[3])])
        y.append(float(each[2]))
    X = np.reshape(x,(len(x),2))
    #X_temp = X
    Y = np.reshape(y,(len(y),1))
    m = np.mean(X,axis=0)
    dev = np.std(X,axis=0)
    m_y = np.mean(Y)
    dev_y = np.std(Y)
    if len(x)>1:
        X = (X-m)/dev
        Y = (Y-m_y)/dev_y
    X = np.hstack((X,np.ones((len(x),1))))
    X_t = np.transpose(X)
    X_inv = np.linalg.pinv(np.dot(X_t,X))
    w = np.dot(X_inv,X_t)
    weights = np.dot(w,Y)
    df = len(x) - 3
    if df <= 0:
        return (weights[0,0], -1, -1)
    rss = np.sum(np.power((Y - np.dot(X, weights)), 2))
    s_squared = rss / df
    se = np.sum(np.power((X[:, 0]), 2))
    tt = (weights[0, 0] / np.sqrt(s_squared / se))
    pval = stats.t.sf(np.abs(tt), df) * 2
    return (weights[0, 0], pval)


def regression(data):
    #word = data[0]
    #list_counties = data[1]
    x_lin = []
    x_mul = []
    y = []
    for each in data:
        #print(each)
        x_lin.append(float(each[1]))
        x_mul.append([float(each[1]),float(each[3])])
        y.append(float(each[2]))

    X_mul = np.reshape(x_mul,(len(x_mul),2))
    X_lin = np.reshape(x_lin,(len(x_lin),1))
    #X_temp = X
    Y = np.reshape(y,(len(y),1))
    m_lin = np.mean(X_lin)
    m_mul = np.mean(X_mul,axis=0)
    dev_mul = np.std(X_mul,axis=0)
    dev_lin = np.std(X_lin)
    m_y = np.mean(Y)
    dev_y = np.std(Y)
    if len(x_lin)>1:
        X_lin = (X_lin-m_lin)/dev_lin
        X_mul = (X_mul - m_mul) / dev_mul
        Y = (Y-m_y)/dev_y
    X_lin = np.hstack((X_lin,np.ones((len(x_lin),1))))
    X_mul = np.hstack((X_mul, np.ones((len(x_mul), 1))))
    X_t_lin = np.transpose(X_lin)
    X_t_mul = np.transpose(X_mul)
    X_inv_lin = np.linalg.pinv(np.dot(X_t_lin,X_lin))
    X_inv_mul = np.linalg.pinv(np.dot(X_t_mul, X_mul))
    w_lin = np.dot(X_inv_lin,X_t_lin)
    w_mul = np.dot(X_inv_mul, X_t_mul)
    weights_lin = np.dot(w_lin,Y)
    weights_mul = np.dot(w_mul, Y)
    df_lin = float(len(x_lin) - 2)
    df_mul = float(len(x_mul) - 3)
    if df_lin <= 0:
        return (weights_lin[0,0], -1, -1, weights_mul[0,0],-1,-1)
    rss_lin = np.sum(np.power((Y - np.dot(X_lin, weights_lin)), 2))
    rss_mul = np.sum(np.power((Y - np.dot(X_mul, weights_mul)), 2))
    if rss_lin == 0:
        print("RSS 0")
    if rss_mul == 0:
        print("RSS mul 0")
    s_squared_lin = rss_lin / df_lin
    s_squared_mul = rss_mul / df_mul
    se_lin = np.sum(np.power((X_lin[:, 0]), 2))
    se_mul = np.sum(np.power((X_mul[:, 0]), 2))
    tt_lin = (weights_lin[0, 0] / np.sqrt(s_squared_lin / se_lin))
    tt_mul = (weights_mul[0, 0] / np.sqrt(s_squared_mul / se_mul))
    pval_lin = stats.t.sf(np.abs(tt_lin), df_lin) * 2
    pval_mul = stats.t.sf(np.abs(tt_mul), df_mul) * 2
    return (weights_lin[0, 0], tt_lin, pval_lin, weights_mul[0,0], tt_mul,pval_mul)

conf = SparkConf()
sc = SparkContext(conf=conf)
i=0
county_file = sys.argv[2]
word_file = sys.argv[1]
rdd1 = sc.textFile(county_file).mapPartitions(lambda line: csv.reader(line)).flatMap(create_county)
rdd2 = sc.textFile(word_file).mapPartitions(lambda line: csv.reader(line)).flatMap(create_words)

rdd3 = rdd2.join(rdd1).map(map_to_words).groupByKey()
m = rdd3.count()
pos_correlated = rdd3.mapValues(linear_regression).takeOrdered(20,lambda x: -x[1][0])
neg_correlated = rdd3.mapValues(linear_regression).takeOrdered(20,lambda x: x[1][0])
pos_correlated_control = rdd3.mapValues(multivariate_regression).takeOrdered(20,lambda x: -x[1][0])
neg_correlated_control = rdd3.mapValues(multivariate_regression).takeOrdered(20,lambda x: x[1][0])
print("Value of bonferroni correction is ",0.05/m)
print("Top 20 positively correlated words are:")
print(pos_correlated)
print("Top 20 negatively correlated words are:")
print(neg_correlated)
print("Top 20 positively correlated words are with control variable as income:")
print(pos_correlated_control)
print("Top 20 negatively correlated words are with control variable as income:")
print(neg_correlated_control)

##Usage spark-submit filename.py 'hdfs:wordlist' 'hdfs:countylist' --master yarn --deploy-mode cluster