from pyspark import SparkContext
import re
import csv
import numpy as np
from bitarray import bitarray
import mmh3
import statistics
import math
import hashlib
from math import log

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

def rightmost_binary_1_position(num):
    i = 0
    while (num >> i) & 1 == 0:
        i += 1
    return i + 1

def hyperloglog(vs):
    bits_for_bucket_index = int(math.ceil(np.log2(len(vs))))
    if bits_for_bucket_index ==0:
        bits_for_bucket_index = 2
    #bits_for_bucket_index = 10
    bucket_count = 2 **bits_for_bucket_index
    buckets = [0] * bucket_count

    # Set up the data for "stochastic averaging"
    for v in vs:
        hash_integer = hash(v[1].encode('utf-8'))
        i = hash_integer & (bucket_count - 1)
        w = hash_integer >> bits_for_bucket_index
        buckets[i] = max(buckets[i], rightmost_binary_1_position(w))

    a_m = .7213 / (1 + 1.079 / bucket_count)
    # Do the stochastic averaging.
    E = a_m * bucket_count ** 2 * sum(2 ** (-Mj) for Mj in buckets) ** (-1)
    # Small-range correction
    if E < (5 / 2.0 * bucket_count):
        V = len([b for b in buckets if b == 0])
        if V:
            E = bucket_count * log(bucket_count / float(V))
    # Large-range correction
    elif E > (1 / 30.0) * 2 ** 32:
        E = -(2 ** 32) * log(1 - (E / 2 ** 32))
    return int(math.ceil(E))

def trailing_zeros(n):
	s = str(n)
	return len(s)-len(s.rstrip('0'))

def mapVal_func(vs):
    
    Rs = []
    n = int(math.floor(np.log2(len(vs))))
    for h in range(n * 4):
        R = 0
        for each in vs:
            phrase = each[1]
            r_e = trailing_zeros(format(abs(mmh3.hash(phrase, h)), '032b'))
            R = max(R,r_e)
        Rs.append(2**R)
    group_means = []
    for i in range(0,len(Rs),n):
        group_means.append(np.mean(Rs[i:i+n]))
    return np.median(group_means)


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
file = open("umbler_locations.csv","r")
lines = csv.reader(file, delimiter=",")
for each in lines:
    #add_item(each.strip(" ").strip("\n"),f)
    add_item((each[0]).encode('utf-8'),f)
file.close()
smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
filter_bc = sc.broadcast(f)

rdd_temp = smallTestRdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0])>0 and check_item(x[1][0],filter_bc.value)).groupByKey().mapValues(hyperloglog).collect()

print(rdd_temp)
#True vals for small: MM =1, SIGH = 2, UM = 3,  OH = 31
#True vals large: MM = 8, SIGH = 43, UM = 41, OH = 227