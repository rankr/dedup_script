#coding: utf-8
import os
import subprocess
import hashlib
from func import *
import numpy as np
import pandas as pd
import zlib

MSBBIT = 1<<31

NUM_PER_MAIN_COMMIT = 50000

class OBJECT:
	def __init__(self, raw_data = '', type = ''):
		self.raw_data = raw_data
		self.type = type

class PARSED_COMMIT:
	def __init__(self):
		pass

class INDEX_AND_NEW:
	def __init__(self, file, set = {}):
		self.file = file
		self.set = set

def commitFromPack(idxPath, packPath):
	f_idx = open(idxPath, 'rb')
	f_idx.seek(4*257)

	#I dnt know if it's little endian
	obj_num = read_number_from_file(f_idx, 4)
	obj_list = []
	for i in xrange(0, obj_num):
		j = ""
		for k in xrange(0, 20):
			a = hex(ord(f_idx.read(1)))[2:]
			if len(a)==1:
				a = '0' + a
			j = j + a
		obj_list.append(j)

	f_idx.seek(4*258 + 24*obj_num, 0)
	#if offset is negative, then the offset is in layer5 not in layer4
	obj_offset_list = []
	layer5_list = []

	for i in xrange(0, obj_num):
		j = read_number_from_file(f_idx, 4)
		if not j&MSBBIT:
			obj_offset_list.append(j&(~MSBBIT))
		else:
			obj_offset_list.append(-1)
			layer5_list.append((i, j))

	def cmp_second(x, y):
		if x[1]>y[1]:
			return 1
		if x[1]<y[1]:
			return -1
		return 0
	layer5_list.sort(cmp_second)

	for index, offset_of_layer5 in layer5_list:
		obj_offset_list[index] = read_number_from_file(f_idx, 8, bigendian = False)

	f_idx.close()

	#now the offset of obj in packfile are well
	#I've prove the base object is before the deltaed object

	f_pack = open(packPath, 'rb')
	f_pack.seek(12, 0)
	
	obj_list = zip(obj_list, obj_offset_list)
	obj_list.sort(cmp_second)

	obj_hash = {}
	off2sha = {}
	for i, j in obj_list:
		#not need to store sha in offset
		obj_hash[i] = OBJECT()
		off2sha[j] = i

	def handle_delta(string, idx, base_obj):
		string = zlib.decompress(string[idx:])
		tail_idx = len(string)
		#read two var-len int first
		idx = 0
		i = 7
		a = ord(string[idx])
		idx += 1
		src_size = a&0x7f
		while a&0x80:
			a = ord(string[idx])
			src_size |= (a&0x7f)<<i
			i += 7
			idx += 1
		if src_size != len(base_obj.raw_data):
			if src_size != len(base_obj.raw_data).size - 1 or base_obj.raw_data[-1] != "\n":
				print "Error in addObjFromPack:handle_delta: src_size != input_obj_size"
				print "former is %d, latter is %d"%(src_size, len(base_obj.raw_data))
				exit()

		tar_size = 0
		i = 0
		while True:
			#read two var-len int first
			a = ord(string[idx])
			tar_size |= (a&0x7f)<<i
			i += 7
			idx += 1
			if not a&(0x80):
					break
		#now deal with copy and insert command
		tar_data = ''
		while idx < tail_idx:
			a = ord(string[idx])
			idx += 1
			if a&(0x80):#copy
				offset = 0
				copy_len = 0
				if a&(1):
					offset = ord(string[idx])
					idx += 1
				if a&(2):
					offset |= ord(string[idx])<<8
					idx += 1
				if a&(4):
					offset |= ord(string[idx])<<16
					idx += 1
				if a&(8):
					offset |= ord(string[idx])<<24
					idx += 1
				if a&(0x10):
					copy_len = ord(string[idx])
					idx += 1
				if a&(0x20):
					copy_len |= ord(string[idx])<<8
					idx += 1
				if a&(0x40):
					copy_len |= ord(string[idx])<<16
					idx += 1
				if copy_len==0:
					copy_len = 0x10000
				
				tar_data += base_obj.raw_data[offset : offset + copy_len]
			else:#insert
				tar_data += string[idx:idx+a]
				idx += a
			if idx > tail_idx:
				print 'error in handle_delta, idx is bigger than string:\
 idx is %d, tail_idx is %d'%(idx, tail_idx)
				exit()
		return tar_data

	ret = []
	for i in xrange(0, len(obj_list)):
		#the type of base object and deltaed object is the same
		compressed_data = ''
		if i != len(obj_list) - 1:
			read_len = obj_list[i+1][1] - obj_list[i][1]
		else:
			read_len = -1

		obj_type, to_process, header_len = read_chunk_from_pack(f_pack, read_len)

		if obj_type == "ofs_delta":
			j = 1
			a = ord(to_process[0])
			base_real_offset = a&0x7f
			while a&0x80:#from the source code of git
				a = ord(to_process[j])
				base_real_offset = ((base_real_offset + 1)<<7) | (a&(0x7f))
				j += 1

			base_obj_sha1 = off2sha[obj_list[i][1] - base_real_offset]
			obj_type = obj_hash[base_obj_sha1].type
			if obj_type != 'commit':
				continue
			tar_data = handle_delta(to_process, j, obj_hash[base_obj_sha1])
		elif obj_type == "ref_delta":
			base_obj_sha1 = ''
			for k in xrange(0, 20):
				a = hex(ord(to_process[k]))[2:]
				if len(a)==1:
					a = '0' + a
				base_obj_sha1 = base_obj_sha1 + a
			obj_type = obj_hash[base_obj_sha1].type
			if obj_type != 'commit':
				continue
			tar_data = handle_delta(to_process, 20, obj_hash[base_obj_sha1])
		elif obj_type == "not exists":
			print ("Error in addObjFromPack, objType is not exists")
			exit()
		else:
			if obj_type != 'commit':
				continue
			tar_data = zlib.decompress(to_process)
		obj_hash[obj_list[i][0]].raw_data = tar_data
		obj_hash[obj_list[i][0]].type = 'commit'

	f_pack.close()

	for i in obj_hash:
		if obj_hash[i].type == 'commit':
			ret.append((i, obj_hash[i].raw_data))
	return ret

def parse_commit(raw_data):
	ret = PARSED_COMMIT()
	raw_list = raw_data.split('\n')
	ret.tree = raw_list[0].split(' ')[1]
	ret.parent1 = raw_list[1].split(' ')[1]
	temp = raw_list[2].split(' ')
	cnt = 2
	ret.parent2 = ''
	if temp[0] == 'parent':
		ret.parent2 = temp[1]
		cnt += 1
	begin = raw_list[cnt].find(' ')
	end = raw_list[cnt].find('>')
	ret.author = raw_list[cnt][begin+1:end+1]
	ret.author_time = raw_list[cnt][end+2:]

	begin = raw_list[cnt+1].find(' ')
	end = raw_list[cnt+1].find('>')
	ret.committer = raw_list[cnt+1][begin+1:end+1]
	ret.committer_time = raw_list[cnt+1][end+2:]

	ret.msg = '\n'.join(raw_list[cnt+2:])
	return ret



def rgit_commit_csv_store(git_repo_path, commit_store_path):
	'''
	store commit objects from git_repo_path, to csv files in commit_store_path
	'''
	csv_files = os.listdir(commit_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'to_write')
		w = open(new_file_path, 'w')
		w.write('0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_commit_main.csv' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_main0.csv')
		w = open(new_file_path, 'w')
		w.write(','.join(['tree','parent1','parent2',\
			'author','author_time','committer','committer_time','msg\n']))
		w.close()
	if 'rgit_commit_hash8.csv' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_hash8.csv')
		w = open(new_file_path, 'w')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()

	idx_pack_pairs = idx_pack_from_repo(git_repo_path)
	ret = []
	for i, j in idx_pack_pairs:
		ret.extend(commitFromPack(i, j))

	f = open(os.path.join(commit_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	f.close()

	f = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'), 'a')

	#暂且定为每个5万条commit
	w = open(''.join([commit_store_path, '/rgit_commit_main', str(which), '.csv']), 'a')
	h = {}
	for sha, raw_data in ret:
		parsed_commit = parse_commit(raw_data)
		'''
		print parsed_commit.tree
		print parsed_commit.parent1
		print parsed_commit.parent2
		print parsed_commit.author
		print parsed_commit.author_time
		print parsed_commit.committer
		print parsed_commit.committer_time
		print parsed_commit.msg
		print '\n\n\n'
		c = 'tree %s\nparent %s\n'%(parsed_commit.tree, parsed_commit.parent1)
		if parsed_commit.parent2:
			c += 'parent %s\n'%(parsed_commit.parent2)
		c += ''
		c += 'author %s %s\ncommitter %s %s\n%s'%(parsed_commit.author, parsed_commit.author_time,\
		parsed_commit.committer, parsed_commit.committer_time, parsed_commit.msg)
		print hashlib.sha1('commit %d\0%s'%(len(c), c)).hexdigest()
		print sha (those two are same, good work)
		return 0
		'''

		if sha[0:3] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:3]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(commit_store_path, indexpath), 'w')
				f.close()
			else:
				f = open(os.path.join(commit_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(commit_store_path, indexpath), 'a')
			h[sha[0:3]] = INDEX_AND_NEW(file = f, set = temp)
		if sha in h[sha[0:3]].set:#already exists
			continue
		h[sha[0:3]].file.write('%s,%d,%d\n'%(sha, which, already_store))
		h[sha[0:3]].set.add(sha)

		md51 = hashlib.md5(parsed_commit.author).hexdigest()[0:8]
		temp = 0
		j1 = 0
		j2 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_commit.author:
					break
			else:
				hash8[j1] = parsed_commit.author
				hash8_file.write("%s,%s\n"%(j1, parsed_commit.author))
				break
			temp += 1

		md52 = hashlib.md5(parsed_commit.committer).hexdigest()[0:8]
		temp = 0
		while True:
			j2 = md52 + str(temp) 
			if j2 in hash8:
				if hash8[j2] == parsed_commit.committer:
					break
			else:
				hash8[j2] = parsed_commit.committer
				hash8_file.write("%s,%s\n"%(j2, parsed_commit.committer))
				break
			temp += 1
		write_str = "%s,%s,%s,%s,%s,%s,%s,%s\n"%(\
			parsed_commit.tree, parsed_commit.parent1, parsed_commit.parent2,\
			j1, parsed_commit.author_time, j2,\
			parsed_commit.committer_time, zlib.compress(parsed_commit.msg))
		w.write(write_str)
		already_store += 1
		if already_store == NUM_PER_MAIN_COMMIT:
			already_store = 0
			which += 1
			w.close()
			new_file_path = os.path.join(commit_store_path, 'rgit_commit_main%d.csv'%(which))
			w = open(new_file_path, 'w')
			w.write(','.join['tree','parent1','parent2',\
				'author','author_time','committer','committer_time','msg\n'])
	f = open(os.path.join(commit_store_path, 'to_write'), 'w')
	f.write("%d,%d"%(which, already_store))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1