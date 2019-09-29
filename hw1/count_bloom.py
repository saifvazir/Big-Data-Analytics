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
    h = hashlib.sha256(x)
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


def mapVal_func(vs):
    count = 0
    f = create_bloom_filter(300,0.01)
    for each in vs:
        if not check_item(each[1],f):
            add_item(each[1],f)
            count += 1
    return count


def map_nonfluency(data):
    location = data[0]
    post = data[1].lower()
    #post = re.sub(r"[.,!@#$%&-_+=*(){}/|:;'<>?]+", '', post)
    post = post.split(" ")
    nf_dict = {"MM": ["mm+"], "OH": ["oh+", "ah+"], "SIGH": ["sigh", "sighed", "sighing", "sighs", "ugh", "uh"],
               "UM": ["umm*", "hmm*", "huh"]}
    d=[]
    for k in nf_dict.keys():
        for each in nf_dict[k]:
            locs = []
            #print(post)
            for i,word in enumerate(post):
                if re.match(r"\b"+each+"[\W]*$",word):
                    locs.append(i)
            #print(locs)
            for loc in locs:
                trunc = re.findall("[a-z]+", each)[0]
                p=len(post[loc])-1
                for i in range(len(post[loc])-1,-1,-1):
                    if post[loc][i] !=trunc[-1]:
                        p-=1
                #print(p)
                if p!=len(post[loc])-1:
                    if len(post)-loc-1>=2:
                        phrase = " ".join(post[loc+1:loc+3]).strip(" ")
                        new_phrase = post[loc][p:]+" "+phrase
                        d.append((k,(location,new_phrase)))
                else:
                    if len(post)-loc-1>=3:
                        phrase = " ".join(post[loc+1:loc+4])
                        d.append((k,(location,phrase)))
    return d



sc = SparkContext("local[*]","umbler")

data = [("Stony Brook, NY", "Ugh. lost my keys last night"),("Somewhere someplace", "Ugh. lost my keys last night"),
        ("Stony Brook, NY", "lost my keys last night"),("Springfield, IL", "New AWS tool, Data Bricks. Ummmmmm why?")]
testFileSmall = 'publicSampleLocationTweet_large.csv'

# rdd = sc.parallelize(data)
f = create_bloom_filter(28519,0.001)
file = open("umbler_locations.txt","r")
lines = file.readlines()
for each in lines:
    add_item(each.strip(" ").strip("\n"),f)
file.close()
smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
filter_bc = sc.broadcast(f)

rdd_temp = smallTestRdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0])>0 and check_item(x[1][0],filter_bc.value)).groupByKey().mapValues(mapVal_func).collect()

print(rdd_temp)
#True vals for small: MM =1, SIGH = 2, UM = 3,  OH = 31
#True vals large: MM = 8, SIGH = 43, UM = 41, OH = 227