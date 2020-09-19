#python second1.py yelp_dataset/review.json secondoutput.json 65
import pyspark
from pyspark import SparkContext, SparkConf
import sys
import time
import json 

#creating a spark context
sc = SparkContext('local[*]','task2')
sc.setLogLevel('ERROR')

#take commandline input
input_path = sys.argv[1]
output_path = sys.argv[2]
n_partition= sys.argv[3]
n_partition=int(n_partition)
print(input_path,output_path,n_partition)

reviewRDD = sc.textFile(input_path)
solution=dict()

#top 10 businesses
start_time=time.time()
transformedRDD=reviewRDD.map(lambda line: json.loads(line)).map(lambda x: (x['business_id'],1))
transformedRDD.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

#transformedRDD.mapPartitions(iter => Iterator(_.toArray))
businessRDD=transformedRDD.reduceByKey(lambda a,b: a+b)
top_business=businessRDD.takeOrdered(10,key=lambda x: (x[1])*-1)
print(top_business)

default_sol=dict()
default_sol['n_partition']=businessRDD.getNumPartitions()
default_sol['n_items']=[]
#find better way for collect or glom
for i in businessRDD.glom().collect():
    default_sol['n_items'].append(len(i))
default_sol['exe_time']=time.time()-start_time

solution['default']=default_sol

#top 10 businesses
start_time=time.time()

def partition_function(x):
    return hash(x)

businessRDD2=transformedRDD.partitionBy(n_partition,partition_function).reduceByKey(lambda a,b: a+b)
top_business2=businessRDD2.takeOrdered(10,key=lambda x: (x[1])*-1)
print(top_business2)

new_sol=dict()
new_sol['n_partition']=businessRDD2.getNumPartitions()
new_sol['n_items']=[]
for i in businessRDD2.glom().collect():
    new_sol['n_items'].append(len(i))
new_sol['exe_time']=time.time()-start_time

solution['customized']=new_sol

print(solution)

#solution=json.dumps(solution)

with open(output_path, 'w', encoding='utf-8') as out:
    json.dump(solution, out, ensure_ascii=False)


"""
{'default': {'n_partition': 160, 'n_items': [1155, 1214, 1245, 1208, 1235, 1190, 1190, 1196, 1147, 1228, 1182, 1190, 1193, 1173, 1211, 1182, 1212, 1226, 1208, 1143, 1113, 1203, 1193, 1227, 1229, 1240, 1266, 1200, 1189, 1228, 1179, 1247, 1165, 1234, 1200, 1219, 1258, 1198, 1219, 1165, 1142, 1222, 1207, 1150, 1184, 1206, 1217, 1198, 1204, 1135, 1117, 1207, 1230, 1184, 1193, 1239, 1161, 1230, 1237, 1234, 1223, 1166, 1240, 1246, 1215, 1222, 1208, 1235, 1234, 1220, 1195, 1178, 1238, 1238, 1163, 1199, 1203, 1200, 1223, 1212, 1237, 1183, 1204, 1167, 1140, 1246, 1200, 1183, 1177, 1179, 1262, 1257, 1182, 1181, 1255, 1248, 1251, 1230, 1237, 1217, 1182, 1198, 1206, 1223, 1185, 1205, 1246, 1134, 1242, 1192, 1235, 1167, 1228, 1203, 1196, 1210, 1181, 1311, 1188, 1255, 1212, 1110, 1173, 1235, 1202, 1218, 1225, 1192, 1197, 1189, 1203, 1203, 1160, 1265, 1167, 1193, 1146, 1193, 1192, 1186, 1223, 1180, 1233, 1143, 1267, 1130, 1233, 1181, 1210, 1234, 1211, 1178, 1257, 1221, 1188, 1177, 1181, 1267, 1214, 1139], 'exe_time': 281.7116801738739}, 'customized': {'n_partition': 65, 'n_items': [2912, 2909, 2964, 3047, 2978, 3067, 2923, 3014, 2990, 2944, 3023, 2925, 2919, 2896, 2885, 2947, 2993, 3076, 3067, 3032, 2914, 2873, 2997, 2926, 2931, 2945, 2951, 2920, 2969, 2958, 2973, 2963, 2968, 2964, 2916, 2771, 3083, 3031, 2983, 2861, 2995, 2945, 2955, 2994, 2966, 2907, 2950, 2984, 2987, 2998, 3031, 3000, 3012, 2985, 2906, 2934, 2929, 2985, 2938, 3017, 2976, 3037, 2903, 2870, 2994], 'exe_time': 157.77280068397522}}
"""