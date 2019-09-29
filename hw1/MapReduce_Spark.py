from pyspark import SparkContext
import numpy as np
from scipy import sparse
def createSparseMatrix(X, label):
    sparseX = sparse.coo_matrix(X)
    list = []
    for i, j, v in zip(sparseX.row, sparseX.col, sparseX.data):
        list.append(((label, i, j), v))
    return list
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

def run_test(sc, test):
    rdd_temp = sc.parallelize(test)
    rdd_fin = rdd_temp.flatMap(mapper_func).groupByKey().mapValues(reduce_func).sortByKey().collect()
    return rdd_fin

#P.S: I've used sortByKey() just to display the results in an efficient manner, it is not required for correct results.

sc = SparkContext('local[*]', 'pyspark tutorial')
test1 = [(('A:1,2:2,1', 0, 0), 2.0), (('A:1,2:2,1', 0, 1), 1.0), (('B:1,2:2,1', 0, 0), 1), (('B:1,2:2,1', 1, 0), 3)]
test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix([[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')
test3 = createSparseMatrix(np.random.randint(-10, 10, (10,100)), 'A:10,100:100,12') + createSparseMatrix(np.random.randint(-10, 10, (100,12)), 'B:10,100:100,12')

print(run_test(sc,test1))
print(run_test(sc,test2))
print(run_test(sc,test3))