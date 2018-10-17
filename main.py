#coding: utf-8
import os
import argparse
import sys
from rgitmod import init
from rgitmod import absorb
from rgitmod import recover
from rgitmod.clear_all import clear_all
#from test import getidx

def initParse():

	parser = argparse.ArgumentParser()
	#initializing a empty file with rgit format, exec in the empty file
	parser.add_argument('-i', '--init', help = "init directory path/to/new_repo into rgit format")

	#absorb a raw git repository into rgit store, argu is the path
	parser.add_argument('-a', '--absorb', help = "change git repository path/to/git_repo into \
		rgit format, and absorb the git objects into specific directories", nargs = "?")

	#update a repository with a git repository, argu is the path
	parser.add_argument('-u', '--update', help = "not support now")

	#for developer's testing
	parser.add_argument('-t', '--test')

	#for recover a git repository
	parser.add_argument('-r', '--recover', help = "recover a rgit repository into git \
repository now, it can just recover the directory which used to be a git repository")

	#clear all the rgit-object-repos
	parser.add_argument('--clear', help = "clear all the rgit-object-repos")
	
	return parser


if __name__ == '__main__':
	#parsing the argus
	parser = initParse()
	args = vars(parser.parse_args())

	if args['test']:
		if args['test'] == 'getidx':
			#getidx.testgetidx()
			pass
		exit()
	if args['init']:
		init.initRepo(os.path.abspath(args['init']))
		exit()
	if args['absorb']:
		absorb.absorb(os.path.abspath(args['absorb']))
		exit()
	if args['recover']:
		recover.recovergit(os.path.abspath(args['recover']))
		exit()
	if args['update']:
		pass
		exit()
	if args['clear']:
		clear_all()
		exit()