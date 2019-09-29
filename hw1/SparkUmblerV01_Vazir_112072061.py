# name, id

# Template code for CSE545 - Spring 2019
# Assignment 1 - Part II
# v0.01
from pyspark import SparkContext
import re
import mmh3
from bitarray import bitarray
import math
import numpy as np
from pprint import pprint
from scipy import sparse
import csv

def mapper_func(data):
    l = data[0][0].split(":")
    mat1_size = l[1]
    mat2_size = l[2]
    mat_name = l[0]
    if mat1_size.split(",")[1] == mat2_size.split(",")[0]:
        d = []
        if mat_name == 'A':
            for cols in range(0, int(mat2_size.split(",")[1])):
                d.append(((data[0][1], cols),('A', data[0][2], data[1])))
        else:
            for rows in range(0, int(mat1_size.split(",")[0])):
                d.append(((rows, data[0][2]), ('B', data[0][1], data[1])))
        return d
    else:
        print("Error size mismatch")


def reduce_func(vs):
    # <COMPLETE>
    mat1 = []
    mat2 = []
    for each in vs:
        if each[0]=='A':
            mat1.append(each)
        else:
            mat2.append(each)
    mat1.sort(key=lambda x: x[1])
    mat2.sort(key=lambda x: x[1])
    total = 0
    i, j = 0, 0
    while(i<len(mat1) and j<len(mat2)):
        if mat1[i][1] == mat2[j][1]:
            total += mat1[i][2]*mat2[j][2]
            i+=1
            j+=1
        elif mat1[i][1] < mat2[j][1]:
            i+=1
        else:
            j+=1
    return total

def sparkMatrixMultiply(rdd):
    # rdd where records are of the form:
    #  ((â€œA:nA,mA:nB,mBâ€, row, col),value)
    # returns an rdd with the resulting matrix
    resultRdd = rdd.flatMap(mapper_func).groupByKey().mapValues(reduce_func)
    return resultRdd

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
            for i,word in enumerate(post):
                if re.match(r"\b"+each+"[\W]*\b$",word):
                    locs.append(i)
            for loc in locs:
                trunc = re.findall("[a-z]+", each)[0]
                p=len(post[loc])-1
                for i in range(len(post[loc])-1,-1,-1):
                    if post[loc][i] !=trunc[-1]:
                        p-=1
                if p!=0:
                    if len(post)-loc-1>=2:
                        phrase = " ".join(post[loc+1:loc+3]).strip(" ")
                        new_phrase = post[loc][p:]+" "+phrase
                        d.append((k,(location,new_phrase)))
                else:
                    if len(post)-loc-1>=3:
                        phrase = " ".join(post[loc+1:loc+4])
                        d.append((k,(location,phrase)))
    return d

def create_bloom_filter(n,p):
    m = int(-(n*np.log(p))/(np.log(2)**2))
    k = int((m*np.log(2))/n)
    filter_arr = bitarray(m)
    filter_arr.setall(0)
    return [filter_arr,k,m]

def add_item(item,bloom_ds):
    for i in range(bloom_ds[1]):
        idx = (mmh3.hash(item,i))%bloom_ds[2]
        bloom_ds[0][idx] = True

def check_item(item,bloom_ds):
    for i in range(bloom_ds[1]):
        idx = (mmh3.hash(item,i)) % bloom_ds[2]
        if bloom_ds[0][idx] == False:
            return False
    return True


def mapVal_func(vs):
    count = 0
    f = create_bloom_filter(1000,0.01)
    for each in vs:
        if not check_item(each[1],f):
            add_item(each[1],f)
            count += 1
    return count


def umbler(sc, rdd):
    # sc: the current spark context
    #    (useful for creating broadcast or accumulator variables)
    # rdd: an RDD which contains location, post data.
    #
    # returns a *dictionary* (not an rdd) of distinct phrases per um category

    # SETUP for streaming algorithms

    # PROCESS the records in RDD (i.e. as if processing a stream:
    # a foreach transformation
    filteredAndCountedRdd = rdd  # REVISE TO COMPLETE
    #  (will probably require multiple lines and/or methods)
    f = create_bloom_filter(28519, 0.0001)
    file = open("umbler_locations.csv", "r")
    lines = csv.reader(file, delimiter=",")
    for each in lines:
        # add_item(each.strip(" ").strip("\n"),f)
        add_item((each[0]).encode('utf-8'), f)
    file.close()
    filter_bc = sc.broadcast(f)
    rdd_temp = rdd.flatMap(map_nonfluency).filter(lambda x: len(x[1][0]) > 0 and check_item(x[1][0], filter_bc.value)).groupByKey().mapValues(mapVal_func).collect()

    # return value should fit this format:
    distinctPhraseCounts = {}
    for each in rdd_temp:
        distinctPhraseCounts[each[0]] = int(each[1])

    return distinctPhraseCounts


################################################
## Testing Code (subject to change for testing)



def createSparseMatrix(X, label):
    sparseX = sparse.coo_matrix(X)
    list = []
    for i, j, v in zip(sparseX.row, sparseX.col, sparseX.data):
        list.append(((label, i, j), v))
    return list


def runTests(sc):
    # runs MM and Umbler Tests for the given sparkContext

    # MM Tests:
    print("\n*************************\n MatrixMult Tests\n*************************")
    test1 = [(('A:2,1:1,2', 0, 0), 2.0), (('A:2,1:1,2', 1, 0), 1.0), (('B:2,1:1,2', 0, 0), 1), (('B:2,1:1,2', 0, 1), 3)]
    test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix(
        [[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')
    test3 = createSparseMatrix(np.random.randint(-10, 10, (10, 100)), 'A:10,100:100,12') + createSparseMatrix(
        np.random.randint(-10, 10, (100, 12)), 'B:10,100:100,12')
    mmResults = sparkMatrixMultiply(sc.parallelize(test1))
    pprint(mmResults.collect())
    mmResults = sparkMatrixMultiply(sc.parallelize(test2))
    pprint(mmResults.collect())
    mmResults = sparkMatrixMultiply(sc.parallelize(test3))
    pprint(mmResults.collect())

    # Umbler Tests:
    print("\n*************************\n Umbler Tests\n*************************")
    testFileSmall = 'publicSampleLocationTweet_small.csv'
    testFileLarge = 'publicSampleLocationTweet_large.csv'

    # setup rdd
    import csv
    smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
    #pprint(smallTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, smallTestRdd))

    largeTestRdd = sc.textFile(testFileLarge).mapPartitions(lambda line: csv.reader(line))
    #pprint(largeTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, largeTestRdd))

    return

sc = SparkContext("local[*]","umbler")
runTests(sc)