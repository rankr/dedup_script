#coding: utf-8
import os
import subprocess
import requests as rqs
import random as rd
import time
import re
import json

def get_full_name():
	ret = {}
	for i in xrange(1, 6):
		p = 'e://100_rand_repo%d.csv'%(i)
		f = open(p)
		a = f.readline()
		a = f.readlines()
		for k in a:
			ret[k.split(',')[0]] = ''
	return ret
	
def check_fork():
	'''
	append: if the write append to the csv file
	'''
	root = get_full_name()
	headers = {"Authorization": "token 147b2ccd120f99a4576c8dd28943971fc44d8580"}
	para = {'since': -1}
	
	for i in root:
		temp = i
		while True:
			r = rqs.get("https://api.github.com/repos/%s"%(temp), headers = headers)
			a = r.json()
			if 'parent' not in a:
				root[i] = temp
				break
			else:
				temp = a['parent']['full_name']
	reverse = {}
	for i in root:
		v = root[i]
		if v in reverse:
			reverse[v].append(i)
		else:
			reverse[v] = [i]
	json.dump(reverse, open('just8_fork', 'w'))
	print 'success!'

if __name__ == '__main__':
	check_fork()