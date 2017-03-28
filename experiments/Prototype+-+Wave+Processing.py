
# coding: utf-8

# In[1]:

get_ipython().magic(u'matplotlib inline')


# In[2]:

import matplotlib as mpl
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy import fftpack
import os


# In[3]:

directory = '/Users/cputnam/proto/1st_test/'


# #### Get a directory listing to use as an iteration point

# In[20]:

os.chdir(directory)


# In[21]:

file_list = os.listdir(directory)


# In[ ]:

seq = 0


# In[ ]:

for test_file in file_list:
    path = directory + test_file
    seq +=1


# ### Set Up Parameters

# In[47]:

pfreq = []


# In[4]:

test_file = '2003.10.22.12.06.24'


# In[5]:

directory = '/Users/cputnam/proto/1st_test/'


# In[6]:

path = directory + test_file


# In[7]:

channels = ['B1CH1','B1CH2','B2CH1','B2CH2','B3CH1','B3CH2','B4CH1','B4CH2']


# In[8]:

sample_interval = 1.0/20000


# ###  Main - Load Data into Pandas Data Frame and analyze data

# In[48]:

for test_file in file_list:
    path = directory + test_file
    raw = pd.read_csv(path,sep = '\t', header=None, names= channels)
    channel = raw[channels[0]]
    pfreq.append(analyze(channel,sample_interval))


# ## Anaylze Function Definition - Take a channel and sample interval return FFT output

# In[46]:

def analyze(channel, sample_interval):
    sample_freq = fftpack.fftfreq(channel.size, d=sample_interval)
    sig_fft = fftpack.fft(channel)
    pidxs = np.where(sample_freq > 0)
    freqs = sample_freq[pidxs]
    power = np.abs(sig_fft)[pidxs]
    peak_freq = freqs[power.argmax()]
    spectrum = zip(power,freqs)
    spectrum_sort = sorted(spectrum, key=lambda tup: tup[0], reverse=True)
    foo = spectrum_sort[:3]
    return foo


# In[43]:

x = np.arange(0, len(pfreq))


# In[45]:

plt.scatter(x,pfreq)


# In[92]:

import itertools


# In[102]:

merged = list(itertools.chain.from_iterable(pfreq))


# In[105]:

df = pd.DataFrame(merged, columns=['power', 'freq'])


# ### Plot of Full FFT ( Top 3 Frequencies in Sample by power)

# In[115]:

plt.scatter(df['freq'], df['power'])


# ### Plot of first 1/4 time period of sample ( Top 3 Frequencies in Sample by power)

# In[131]:

plt.scatter(df['freq'][:1617], df['power'][:1617])


# ### Plot of 2nd 1/4 time period  of sample (Top 3 Frequencies in Sample by Power)

# In[133]:

plt.scatter(df['freq'][1617:3234], df['power'][1617:3234])


# ### Plot of 3rd 1/4 time period of sample (Top 3 Frequencies in Sample by Power)

# In[135]:

plt.scatter(df['freq'][3234:4851], df['power'][3234:4851])


# ### Plot of last 1/4 time period of sample (Top 3 Frequencies in Sample by Power)

# In[136]:

plt.scatter(df['freq'][4851:], df['power'][4851:])


# ### Observation during failure period is that there is pronounced spread in the original 986 Hz area during failure

# In[ ]:



