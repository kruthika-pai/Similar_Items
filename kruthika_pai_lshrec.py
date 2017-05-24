from pyspark import SparkContext
from collections import defaultdict
from operator import add
import sys
import hashlib

import math
import itertools

sc = SparkContext()



hash_list = []

user_tuples = []



def mapper(temp_row):
	global hash_list

	user_id = temp_row[0]
	movie_list = temp_row[1]

	return_list = []
	min_movie_value = float('-inf')
	min_movie_id = 0

	for i in range(20):
		min_movie_value = float('inf')
		for each_movie in movie_list:
			each_movie = int(each_movie)
			if hash_list[i][each_movie] < min_movie_value:
				min_movie_value = hash_list[i][each_movie]
		return_list.append(str(min_movie_value))

	return([user_id,return_list])


def strings(temp_row):

	user_id = temp_row[0]
	hash_values_20 = temp_row[1]

	return_list = []
	index = 0

	band_counter = 0
	while index < 20:
		band_string = ','.join(hash_values_20[index:index+4])
		return_list.append((band_string,band_counter))
		index = index + 4
		band_counter +=1

	return([user_id,return_list])


def getCands(crdd):

	
	clist = []
	cand_pairs_it = itertools.permutations(crdd[1],2)
	for pair in cand_pairs_it:
		s = pair[0]
		t = pair[1]
		s_index = int(str(pair[0][1:]))
		t_index = int(str(pair[1][1:]))

		set_s = set(user_tuples[s_index-1][1])
		set_t = set(user_tuples[t_index-1][1])

		sUt = set_s.union(set_t)
		sIt = set_s.intersection(set_t)

			
		Jacc = (float(len(sIt))/float(len(sUt))) 




		clist.append(([s,t,Jacc]))
	return clist



def main():
	inFile = sc.textFile(sys.argv[1])
	userKv = inFile.map(lambda x:x.split(','))\
		.map(lambda x: (x[0], x[1:]))
		

	global user_tuples
	user_tuples = userKv.collect()



	hTemp = []
	hList = []
	
	for i in range(20):
		for x in range(100):
			hTemp.append((3*x+13*i)%100)

		hList.append(hTemp)
		hTemp =[]

	global hash_list
	hash_list = hList

	return_rdd = userKv.map(mapper)

	return_bands = return_rdd.map(strings)

	band_vectors = return_bands.flatMapValues(lambda x:x).map(lambda (x,y):(y,x)).map(lambda x:((x[0][1],x[0][0]),x[1])).groupByKey().mapValues(list)

	

	new_list = []

	

	candidates = band_vectors.filter(lambda x: len(x[1])>1)
	cndSet = candidates.flatMap(lambda x: [(x[0],v) for v in set(x[1])]).map(lambda x: x[1]).distinct().map(lambda x: (1,(x))).groupByKey().mapValues(list).map(getCands).flatMap(lambda x: x).filter(lambda x: x[2]>0.0)
	
	
	sim_u_list = cndSet.sortBy(lambda x: (int(x[1][1:]),-x[2])).map(lambda x:(x[0],(x[1],x[2]))).groupByKey().mapValues(list).sortBy(lambda x: int(x[0][1:])).filter(lambda x: (x[0],x[1][0:5])).map(lambda x: (x[0],[y[0] for y in x[1]]))

	
	valList = sim_u_list.collect()
	
	for i in range(len(valList)):
		valList[i] = list(valList[i])
		valList[i][1] = list(valList[i][1])

	outFile = sys.argv[2]
	ofile = open(outFile,'w')

	

	with open(outFile, 'w') as w:
		for each_entry in valList:
			w.write(each_entry[0])
			w.write(':')
			w.write(','.join(each_entry[1][0:5]))
			w.write('\n')




if __name__ == '__main__':
	main()