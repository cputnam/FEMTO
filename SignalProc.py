import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy import fftpack

N = 100
T = 1.0/600.0

# Create a X Axis which has step intervals of T and ranges from 0.0 to N*T
x = np.linspace(0.0, N*T, N)
# N*T is the interval between values in the array
print (N*T)


# Create a 50Hz Sine Wave and overlay on our created X Axis
y = np.sin(50.0 * 2.0*np.pi*x)

# Show Raw Signal

plt.plot(x,y)

# Use FFT to identify the Frequency of the Signal. It should end up being 50Hz We give the fftfreq function the size of our signal array along with the sampling interval. This is T in our generated 50Hz signal
sample_freq = fftpack.fftfreq(y.size, d=T)

sig_fft = fftpack.fft(y)

pidxs = np.where(sample_freq > 0)
freqs = sample_freq[pidxs]
power = np.abs(sig_fft)[pidxs]

# Lets plot the amount of power by frequency. We can see 50Hz dominates as we should expect
plt.plot(freqs, power)

# Lets get the numerical value of what the graph above has indicated.  Should be approx 50 Hz
freq = freqs[power.argmax()]
print (freq)

#Lets Create another wave at 100Hz and combine this with our 50 HZ wave
# This creates a 100Hz wave
yp = np.sin(100.0 * 2.0*np.pi*x)

# This combines it with our 50Hz wave
ycomb = y + yp

#Now we have a combine set of frequencies lets see if FFT can identify them
sample_freq = fftpack.fftfreq(ycomb.size, d=T)
sig_fft = fftpack.fft(ycomb)
pidxs = np.where(sample_freq > 0)
freqs = sample_freq[pidxs]
power = np.abs(sig_fft)[pidxs]

#We can see that the power behind the frequencies of 50 & 100 Hz are predominant as we would expect.
plt.plot(freqs, power)

#Getting the numerical value of the frequency with highest power is easy. But how do we get the top N frequencies ?
freq = freqs[power.argmax()]
print(freq)

#Lets combine both arrays so we can sort the values together by power and grab the top N frequencies.
foo = zip(power,freqs)
foosort = sorted(foo, key=lambda tup: tup[0], reverse=True)
# Notice this sort is in ascending value so the top N frequencies are last in the list. They are 47.9999 and 101.99999 which are close to original signals of 50 and 100.
foosort