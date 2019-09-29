from pyspark import SparkContext
def func(data):
    d = []
    for i in r.value:
        for j in data:
            if i==j:
                d.append([i,j])
    return d


sc = SparkContext("local[*]","xyz")
r = sc.broadcast([1,2,3])
rdd = sc.parallelize([[1,2],[3,4],[5,6]])
t = rdd.flatMap(func).collect()
print(t)
r = sc.broadcast([2,3,4])
t = rdd.flatMap(func).collect()
print(t)


