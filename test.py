#coding: utf-8
import os
import commands
import subprocess
import zlib
import delta

'''
p = '/Users/file4/毕设/data/nw.js_delta.csv'
pw = '/Users/file4/毕设/data/nw.js_delta_stat.csv'
a = delta.delta_info(p)
delta.delta_stat(a, pw)
'''

idxp = '/Users/file4/star_cpp_repos/nw.js/.git/objects/pack/pack-9491704395de0d3b3d56d4c9e5507880e51d116f.idx'
packp = '/Users/file4/star_cpp_repos/nw.js/.git/objects/pack/pack-9491704395de0d3b3d56d4c9e5507880e51d116f.pack'
sha = '9bd85d6a16198607eb291bf69f132e5882c3a0c4'

r = delta.delta_format(idxp, packp, sha)
print sha,'\n'
print r

print '\nsuccess'
a = raw_input('enter to end')