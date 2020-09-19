#python third1.py yelp_dataset/review.json yelp_dataset/business.json third1.txt third2.json
import pyspark
from pyspark import SparkContext, SparkConf
import sys
import time
import json 

#creating a spark context
sc = SparkContext('local[*]','third')

#take command line inputs
input_path1 = sys.argv[1]
input_path2 = sys.argv[2]
output_path1 = sys.argv[3]
output_path2 = sys.argv[4]
print(input_path1,input_path2,output_path1,output_path2)

reviewRDD = sc.textFile(input_path1)
businessRDD = sc.textFile(input_path2)

businessStarsRDD=reviewRDD.map(lambda line: json.loads(line)).map(lambda line: (line['business_id'],line['stars']))
businessCityRDD=businessRDD.map(lambda line: json.loads(line)).map(lambda line: (line['business_id'],line['city']))

cityStarsRDD=businessCityRDD.join(businessStarsRDD).map(lambda line: line[1]).aggregateByKey((0,0), lambda a,b: (a[0]+b,a[1]+1),lambda a,b: (a[0]+b[0],a[1]+b[1]))
cityAvgRDD=cityStarsRDD.map(lambda line: (line[0],line[1][0]/line[1][1]))

solutionB=dict()

#method1:  Collect all the data, sort in python, and then print the first 10 cities
start_time=time.time()
list_of_cities=cityAvgRDD.collect()
list_of_cities.sort(key=lambda x: (-x[1],x[0]))

for i in range(0,10):
    print(list_of_cities[i])
solutionB["m1"]= time.time() - start_time

#method2:Sort in Spark, take the first 10 cities, and then print these 10 cities
start_time=time.time()
sortedAvg=cityAvgRDD.takeOrdered(10,key=lambda x: (-x[1],x[0]))
print(sortedAvg[0:10])
solutionB["m2"]= time.time() - start_time

#solutionB=json.dumps(solutionB)

with open(output_path2, 'w', encoding='utf-8') as out:
    json.dump(solutionB, out)
    
n_city=cityAvgRDD.count()
sort1=cityAvgRDD.takeOrdered(n_city,key=lambda x: (x[1]*-1,x[0]))
print(sort1)  

with open(output_path1, 'w', encoding='utf-8') as out:
    out.write('city,stars\n')
    for i in sort1[:-1]:
        out.write(str(i[0])+','+str(i[1])+'\n')
    out.write(str(sort1[-1][0])+','+str(sort1[-1][1]))