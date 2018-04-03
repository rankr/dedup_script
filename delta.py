#coding: utf-8
import os
import zlib

MSBBIT = 1<<31

class delta:
	def __init__(self):
		pass

class OBJECT:
	def __init__(self, sha = '', type = '', raw_data = '', offset = -1, header_len = -1):
		self.sha1 = sha
		self.type = type
		self.raw_data = raw_data
		self.offset = offset #offset in packfile
		self.header_len = header_len #header_len in packfile

def read_number_from_file(file, bytes, bigendian = True):
	a = 0
	for i in xrange(0, bytes):
		b = ord(file.read(1))
		if bigendian:
			a = a*256 + b
		else:
			a = a + b*(256**i)
	return a
	


OBJTYPE = ["not exists", "commit", "tree", "blob", "tag", "not exists", \
"ofs_delta", "ref_delta"]

def read_chunk_from_pack(file, length = -1):
	#if length == -1, read all remained data
	#have checked to be corrected
	header_len = 0 #bytes of a header of a chunk takes
	obj_type = 0
	while 1:
		header_len += 1
		a = ord(file.read(1))
		if header_len == 1:
			obj_type = OBJTYPE[(a>>4)&7]
		if not a&(0x80):
			break
	if length != -1:
		compressed_data = file.read(length - header_len)
	else:
		compressed_data = file.read()
	return obj_type, compressed_data, header_len


def cal_mean(arr):
	if len(arr) == 0:
		print "Warning: from cal_mean, the list is empty"
		return 0
	a = sum(arr)
	return a*1.0 / len(arr)

def cal_median(arr):
	if len(arr) == 0:
		print "Warning: from cal_median, the list is empty"
		return 0
	b = sorted(arr)
	if len(arr)%2 == 1:
		return b[len(arr)/2]
	else:
		return (b[len(arr)/2] + b[len(arr)/2 - 1]) * 1.0 / 2

def delta_info(delta_csv_path):
	f = open(delta_csv_path)
	a = f.readline()
	ret = []
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		k = delta()
		k.sha = a[0]
		k.type = a[1]
		k.father = a[2]
		k.root = a[3]
		k.size_cmpr = int(a[4])
		k.size_delta_cmpr = int(a[5])
		k.rate = float(a[6])
		k.depth = int(a[7])
		ret.append(k)
	return ret

def delta_stat(delta, store_path):
	depth_max = 0
	depth_max_list = [0, 0, 0, 0]
	depth = []
	depth_list = [[], [], [], []]
	rate = []
	rate_list = [[],[],[],[]]
	obj_cnt = [0,0,0,0]
	h = {'blob':0, 'commit':1, 'tree':2, 'tag':3}
	for i in delta:
		obj_cnt[h[i.type]] += 1
		if i.depth:
			depth.append(i.depth)
			depth_list[h[i.type]].append(i.depth)
			rate.append(i.rate)
			rate_list[h[i.type]].append(i.rate)
	w = open(store_path, 'w')
	w.write("category,obj_cnt,delta_cnt,delta/obj,depth_max,\
depth_mean,depth_median,depth_min,cmpr_rate_max,cmpr_rate_mean,\
cmpr_rate_median,cmpr_rate_min\n")
	w.write("all,%d,%d,%f,%d,%f,%f,%d,%f,%f,%f,%f\n"%(\
		len(delta), len(depth), len(depth)*1.0/len(delta),\
		max(depth), cal_mean(depth), cal_median(depth),min(depth),\
		max(rate),cal_mean(rate),cal_median(rate),min(rate)))
	w.write("%s,%d,%d,%f,%d,%f,%f,%d,%f,%f,%f,%f\n"%(\
		'blob', obj_cnt[0], len(depth_list[0]), len(depth_list[0])*1.0/obj_cnt[0],\
		max(depth_list[0]), cal_mean(depth_list[0]), cal_median(depth_list[0]),min(depth_list[0]),\
		max(rate_list[0]),cal_mean(rate_list[0]),cal_median(rate_list[0]),min(rate_list[0])))
	w.write("%s,%d,%d,%f,%d,%f,%f,%d,%f,%f,%f,%f\n"%(\
		'commit', obj_cnt[1], len(depth_list[1]), len(depth_list[1])*1.0/obj_cnt[1],\
		max(depth_list[1]), cal_mean(depth_list[1]), cal_median(depth_list[1]),min(depth_list[1]),\
		max(rate_list[1]),cal_mean(rate_list[1]),cal_median(rate_list[1]),min(rate_list[1])))
	w.write("%s,%d,%d,%f,%d,%f,%f,%d,%f,%f,%f,%f\n"%(\
		'tree', obj_cnt[2], len(depth_list[2]), len(depth_list[2])*1.0/obj_cnt[2],\
		max(depth_list[2]), cal_mean(depth_list[2]), cal_median(depth_list[2]),min(depth_list[2]),\
		max(rate_list[2]),cal_mean(rate_list[2]),cal_median(rate_list[2]),min(rate_list[2])))
	if obj_cnt[3] == 0:
		w.write("tag,0,0,0,0,0,0,0,0,0,0,0\n")
	elif len(depth_list[3]) == 0:
		w.write("%s,%d,%d,%f,%d,%f,%f,%d,%f,%f,%f,%f\n"%(\
		'tag', obj_cnt[3], 0, 0,\
		0, 0, 0,0,\
		0,0,0,0))
	else:
		w.write("%s,%d,%d,%f,%d,%f,%f,%d,%f,%f,%f,%f\n"%(\
		'tag', obj_cnt[3], len(depth_list[3]), len(depth_list[3])*1.0/obj_cnt[3],\
		max(depth_list[3]), cal_mean(depth_list[3]), cal_median(depth_list[3]),min(depth_list[3]),\
		max(rate_list[3]),cal_mean(rate_list[3]),cal_median(rate_list[3]),min(rate_list[3])))

	w.close()
	return 0

def delta_depth_rate(delta, store_path):
	h = {}
	for i in delta:
		a = i.depth
		if not a:
			continue
		if a in h:
			h[a].append(i.rate)
		else:
			h[a] = [i.rate]
	w = open(store_path, 'w')
	w.write('depth,mean_rate,median_rate,min_rate\n')
	for key in h:
		w.write("%s,%f,%f,%f\n"%(\
			key, cal_mean(h[key]), cal_median(h[key]), min(h[key])))
	w.close()
	return 1

def delta_format(idxPath, packPath, sha):
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
		obj_hash[i] = OBJECT(offset = j)
		obj_hash[i].sha = i
		off2sha[j] = i

	def handle_delta(string, idx):
		string = zlib.decompress(string[idx:])
		return string
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
		ret = ''
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
				
				ret += "copy %d byte, offset %d\n"%(copy_len, offset)
			else:#insert
				ret += "insert %d byte\n"%(a)
				idx += a
			if idx > tail_idx:
				print 'error in handle_delta, idx is bigger than string:\
 idx is %d, tail_idx is %d'%(idx, tail_idx)
				exit()
		return ret

	ret = ''
	for i in xrange(0, len(obj_list)):
		#the type of base object and deltaed object is the same
		compressed_data = ''
		if i != len(obj_list) - 1:
			read_len = obj_list[i+1][1] - obj_list[i][1]
		else:
			read_len = -1

		obj_type, to_process, header_len = read_chunk_from_pack(f_pack, read_len)

		if obj_list[i][0]!=sha:
			continue
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
			ret = handle_delta(to_process, j)
		elif obj_type == "ref_delta":
			base_obj_sha1 = ''
			for k in xrange(0, 20):
				a = hex(ord(to_process[k]))[2:]
				if len(a)==1:
					a = '0' + a
				base_obj_sha1 = base_obj_sha1 + a
			obj_type = obj_hash[base_obj_sha1].type
			ret = handle_delta(to_process, 20)
		elif obj_type == "not exists":
			print ("Error in addObjFromPack, objType is not exists")
			exit()
		else:
			pass
		obj_hash[obj_list[i][0]].type = obj_type
		obj_hash[obj_list[i][0]].header_len = header_len

	f_pack.close()

	return ret