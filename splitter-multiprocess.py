#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
import os
import pandas as pd
import splitter as sp   # functions are contained in splitter.py
from multiprocessing import Pool, Process
import time 


# In[2]:


# input parameters

folder = 'work3'        # directory that the target .csv files reside
subfolder = 'processed' # sub directory to store the splitted files in. 
                        # will create the folder new if it doesn't exist
#lines = 5              # number of blank lines that separate two chunks of data


# In[3]:


# make the subfoler

os.makedirs(f'{folder}/{subfolder}', exist_ok = True)

# generate a list of all csv file name under the main folder

all_files = next(os.walk(folder), (None, None, []))[2]
filelist = [i for i in all_files if i.endswith('.csv')]

# generate each csv file's parameters that are stiched together in a string

parm_list = []

for i in filelist:
    parm_list.append(f'{folder}!{subfolder}!{i}')


# In[4]:


# METHOD 1 - SEQUENTIAL PROCESSING

# start = time.time()
# for parm in parm_list:
#     sp.task(parm)
# print("Time Taken: ",str(time.time()-start))


# In[5]:


# METHOD 2 - MULTI-PROCESSING POOL

if __name__ == '__main__':
    with Pool() as pool:
        start = time.time()
        pool.map(sp.task, parm_list)
        pool.close()
        pool.join()
        print("Time Taken: ",str(time.time()-start))


# In[9]:


# METHOD 3 - MULTI-PROCESSING PROCESS

# if __name__ == '__main__':
#     start = time.time()
#     for i in parm_list:
#         p = Process(target=sp.task, args=(i,))
#         p.start()
#         p.join()
#     print("Time Taken: ",str(time.time()-start))


# Out of the three methods - Multiprocessing using the Pool() class appears to be the fastest.

# In[ ]:




