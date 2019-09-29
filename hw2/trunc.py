import csv
f = open("wiki-topcats.txt","r")
reader = csv.reader(f,delimiter=" ")
f2 = open("tf_test.txt","w")
line = 0
for row in reader:
    if line == 1000:
        break
    f2.write(row[0]+" "+row[1]+"\n")
    line +=1

f.close()
f2.close()