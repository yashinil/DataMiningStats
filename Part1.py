#python first2.py yelp_dataset/review.json firstoutput.json
from pyspark import SparkContext, SparkConf
import pyspark
import sys
import time
import json 

start_time=time.time()

#creating a spark context
sc = SparkContext('local[*]','first')
sc.setLogLevel('ERROR')
#take command line inputs
input_path = sys.argv[1]
output_path = sys.argv[2]
print(input_path,output_path)

reviewRDD = sc.textFile(input_path)
#solution=dict()

transformedRDD=reviewRDD.map(json.loads).map(lambda row_dict: (row_dict['user_id'],row_dict['business_id'],row_dict['date']))
transformedRDD.persist(pyspark.StorageLevel.DISK_ONLY)

n_review=transformedRDD.count()
#solution['n_review']=n_review #ans:6685900

#get the count of reviews in 2018
n_review_2018=transformedRDD.filter(lambda x: True if (x[2][0:4]=="2018") else False).count()
#solution['n_review_2018']=n_review_2018 #ans: 1177662

#count of distinct users
usersRDD=transformedRDD.map(lambda x: (x[0],1)).reduceByKey(lambda a,b: a+b).persist(pyspark.StorageLevel.DISK_ONLY)
n_user=usersRDD.count()
#solution['n_user']=n_user #ans:1637138

#top 10 users
top_users=usersRDD.takeOrdered(10,key=lambda x: ((x[1]*-1),x[0]))
#solution['top10_user']=top_users

#distinct businesses
businessRDD=transformedRDD.map(lambda x: (x[1],1)).reduceByKey(lambda a,b: a+b)persist(pyspark.StorageLevel.DISK_ONLY)
n_business=businessRDD.count()
#solution['n_business']=n_business 

#top 10 businesses
top_business=businessRDD.takeOrdered(10,key=lambda x: ((x[1]*-1),x[0]))
#solution['top10_business'] = top_business
solution={"n_review":n_review,"n_review_2018":n_review_2018,"n_user":n_user,"top10_user":top_users,"n_business":n_business,"top10_business":top_business}
print(solution)

#solution=json.dumps(solution)

with open(output_path, 'w', encoding='utf-8') as out:
    json.dump(solution, out, indent=1)

print("--- %s seconds ---" % (time.time() - start_time))

"""
{'n_review': 6685900, 'n_review_2018': 1177662, 'n_user': 1637138, 'top10_user': [('CxDOIDnH8gp9KXzpBHJYXw', 4129), ('bLbSNkLggFnqwNNzzq-Ijw', 2354), ('PKEzKWv_FktMm2mGPjwd0Q', 1822), ('ELcQDlf69kb-ihJfxZyL0A', 1764), ('DK57YibC5ShBmqQl97CKog', 1727), ('U4INQZOPSUaj8hMjLlZ3KA', 1559), ('QJI9OSEn6ujRCtrX06vs1w', 1496), ('d_TBs6J3twMy9GChqUEXkg', 1360), ('hWDybu_KvYLSdEFzGrniTw', 1355), ('cMEtAiW60I5wE_vLfTxoJQ', 1255)], 'n_business': 192606, 'top10_business': [('4JNXUYY8wbaaDmk3BPzlWw', 8570), ('RESDUcs7fIiihp38-d6_6g', 8568), ('K7lWdNUhCbcnEvI0NhGewg', 6887), ('f4x1YBxkLrZg652xt2KR5g', 5847), ('cYwJA2A6I12KNkm2rtXd5g', 5575), ('DkYS3arLOhA8si5uUEmHOw', 5206), ('2weQS-RnoOBhb1KsHKyoSQ', 4534), ('5LNZ67Yw9RD6nf4_UhXOjw', 4522), ('iCQpiavjjPzJ5_3gPD5Ebg', 4351), ('SMPbvZLSMMb7KU76YNYMGg', 4350)]}
--- 647.302241563797 seconds ---
"""