
# coding: utf-8

# In[13]:

get_ipython().magic(u'matplotlib inline')


# In[1]:

import matplotlib as mpl
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy import fftpack


# In[2]:

raw = pd.read_csv('/Users/cputnam/Desktop/bearing_IMS/1st_test/2003.10.22.15.44.13',sep = '\t', header=None, names= ['B1CH1','B1CH2','B2CH1','B2CH2','B3CH1','B3CH2','B4CH1','B4CH2'])


# The FFT always has a defined number of lines or Bins.
# Common Values for Denominator of T [100,200,400,800,1600,3200] (Bins)

# EXAMPLE of FFT using Generated Sine waves at 50 and 80 HZ to show FFT function

# In[7]:

sig = raw['B1CH1']


# In[8]:

time_step = 1.0/20000.0


# In[9]:

sample_freq = fftpack.fftfreq(sig.size, d=time_step)


# In[10]:

sig_fft = fftpack.fft(sig)


# In[11]:

pidxs = np.where(sample_freq > 0)
freqs = sample_freq[pidxs]
power = np.abs(sig_fft)[pidxs]


# In[14]:

plt.plot(power)


# In[15]:

freq = freqs[power.argmax()]


# In[16]:

freq


# In[ ]:



