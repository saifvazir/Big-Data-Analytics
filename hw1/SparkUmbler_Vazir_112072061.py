from pyspark import SparkContext
import re
import csv
import numpy as np
from bitarray import bitarray
import mmh3
import statistics
import math
import hashlib

def hash(x):
    x = str(x).encode('utf-8')
    h = hashlib.sha384(x)
    hex_ = h.hexdigest()
    return int(hex_, base=16)


def create_bloom_filter(n,p):
    m = int(-(n*np.log(p))/(np.log(2)**2))
    k = int((m*np.log(2))/n)
    filter_arr = bitarray(m)
    filter_arr.setall(0)
    return [filter_arr,k,m]

def add_item(item,bloom_ds):
    #hashed_list = []
    for i in range(bloom_ds[1]):
        idx = (mmh3.hash(item,i))%bloom_ds[2]
        #hashed_list.append(hashed_idx)
        bloom_ds[0][idx] = True

def check_item(item,bloom_ds):
    for i in range(bloom_ds[1]): 
        idx = (mmh3.hash(item,i)) % bloom_ds[2]
        if bloom_ds[0][idx] == False: 
            return False
    return True

def trailing_zeros(n):
	s = str(n)
	return len(s)-len(s.rstrip('0'))


def get_hash_res(phrase):
    result = ["" for i in range(count_bc.value*5)]
    #result_tail = [[] for i in range(12)]
    result_tail = []
    for i in range(count_bc.value*4):
        result[i] = format(abs(mmh3.hash(phrase, i)), '032b')
        result_tail.append(trailing_zeros(result[i]))

    g1,g2,g3,g4,g5=0,0,0,0,0
    for i in range(count_bc.value):
        g1 += 2**result_tail[i]
    g1 = float(g1)/float(count_bc.value)
    for i in range(count_bc.value,2*count_bc.value):
        g2 += 2**result_tail[i]
    g2 = float(g2)/float(count_bc.value)
    for i in range(count_bc.value*2,3*count_bc.value):
        g3 += 2**result_tail[i]
    g3 = float(g3)/float(count_bc.value)
    # for i in range(count_bc.value*3,4*count_bc.value):
    #     g4 += 2**result_tail[i]
    # g4 = float(g4)/float(count_bc.value)
    # for i in range(count_bc.value*4,5*count_bc.value):
    #     g5 += 2**result_tail[i]
    # g5 = float(g5)/float(count_bc.value)
    return math.ceil(statistics.median([g1, g2,g3]))


def reduce_func(x,y):
    phrase1 = ""
    phrase2 = ""
    res1 = 0
    res2 = 0
    if not isinstance(x,int):
        phrase1 = x[1]
        res1 = get_hash_res(phrase1)
    else:
        res1 = x
    if not isinstance(y,int):
        phrase2 = y[1]
        res2 = get_hash_res(phrase2)
    else:
        res2 = y
    return max(res1,res2)

def map_maxtails(data):
    phrase = data[1][1]
    res = get_hash_res(phrase)
    return [(data[0],res)]

def mapVal_func(vs):
    # result = ["" for i in range(count_bc.value * 4)]
    # result_tail = [[] for i in range(count_bc.value * 4)]
    # result_tail = []
    # for each in vs:
    #     phrase = each[1]
    #     for i in range(count_bc.value * 2):
    #         result[i] = format(abs(mmh3.hash(phrase, i)), '064b')
    #         result_tail[i].append(trailing_zeros(result[i]))
    # Rs = []
    # n = int(math.floor(np.log2(len(vs))))
    # R = 0
    # for each in vs:
    #     phrase = each[1]
    #     r_md5 = trailing_zeros(format(int(hashlib.md5(phrase.encode('utf-8')).hexdigest(),base = 16), '032b'))
    #     r_sha256 = trailing_zeros(format(int(hashlib.sha256(phrase.encode('utf-8')).hexdigest(),base = 16), '032b'))
    #     r_512 = trailing_zeros(format(int(hashlib.sha512(phrase.encode('utf-8')).hexdigest(), base=16), '032b'))
    #     R = max(R,r_md5,r_512,r_sha256)
    #     Rs.append(2**R)
    # group_means = []
    # for i in range(0,len(Rs),n):
    #     group_means.append(np.mean(Rs[i:i+n]))
    # return np.median(group_means)
    # g1, g2, g3, g4 = 0, 0, 0, 0
    # for i in range(count_bc.value):
    #     g1 += 2 ** max(result_tail[i])
    # g1 = float(g1) / float(count_bc.value)
    # for i in range(count_bc.value, 2 * count_bc.value):
    #     g2 += 2 ** max(result_tail[i])
    # g2 = float(g2) / float(count_bc.value)
    # for i in range(count_bc.value * 2, 3 * count_bc.value):
    #     g3 += 2 ** max(result_tail[i])
    # g3 = float(g3) / float(count_bc.value)
    # for i in range(count_bc.value * 3, 4 * count_bc.value):
    #     g4 += 2 ** max(result_tail[i])
    # g4 = float(g4) / float(count_bc.value)
    #return math.ceil(statistics.median([g1]))


def map_nonfluency(data):
    location = data[0]
    post = data[1].lower()
    post = re.sub(r"[.,!@#$%&-_+=*(){}/|:;'<>?]+", '', post)
    post = post.split(" ")
    #post.remove("")
    nf_dict = {"MM": ["mm+"], "OH": ["oh+", "ah+"], "SIGH": ["sigh", "sighed", "sighing", "sighs", "ugh", "uh"],
               "UM": ["umm*", "hmm*", "huh"]}
    d=[]
    for k in nf_dict.keys():
        for each in nf_dict[k]:
            locs = []
            for i,word in enumerate(post):
                if re.match(r"\b"+each+"$",word):
                    locs.append(i)
            for loc in locs:
                if len(post)-loc-1>=3:
                    phrase = " ".join(post[loc+1:loc+4])
                    d.append((k,(location,phrase)))
    return d



sc = SparkContext("local[*]","umbler")

data = [("Stony Brook, NY", "Ugh. lost my keys last night"),("Somewhere someplace", "Ugh. lost my keys last night"),
        ("Stony Brook, NY", "lost my keys last night"),("Springfield, IL", "New AWS tool, Data Bricks. Ummmmmm why?")]
testFileSmall = 'publicSampleLocationTweet_large.csv'

# rdd = sc.parallelize(data)
f = create_bloom_filter(28519,0.0001)
file = open("umbler_locations.txt","r")
lines = file.readlines()
for each in lines:
    add_item(each.strip(" ").strip("\n"),f)
file.close()
smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
filter_bc = sc.broadcast(f)
#print(smallTestRdd.count())
#rdd_temp = smallTestRdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0])>0 and check_item(x[1][0],filter_bc.value)).reduceByKey(reduce_func).collect()
#rdd_temp = smallTestRdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0])>0 and check_item(x[1][0],filter_bc.value)).count()
# c = smallTestRdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0])>0 and check_item(x[1][0],filter_bc.value)).count()
# cnt = int(math.floor(np.log2(c)))
# count_bc = sc.broadcast(cnt)
#rdd_temp = smallTestRdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0])>0 and check_item(x[1][0],filter_bc.value)).flatMap(map_maxtails).reduceByKey(lambda x,y:max(x,y)).collect()
rdd_temp = smallTestRdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0])>0 and check_item(x[1][0],filter_bc.value)).groupByKey().mapValues(mapVal_func).collect()

print(rdd_temp)
#True vals for small: MM =1, SIGH = 2, UM = 3,  OH = 31
#True vals large: MM = 8, SIGH = 43, UM = 41, OH = 227