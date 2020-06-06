import numpy as np
import pandas as pd
from scipy.fftpack import fft,fft2, fftshift
from scipy import spatial
import matplotlib.pyplot as plt
import math
from dtw import *
### Dynamic Time Warping(DTW) method
def DTWDistance(s1, s2):
    alignment = dtw(s1,s2, keep_internals=True)
    d = alignment.normalizedDistance
    return d

###distance.correlation
def DCDistance(s1,s2):
    s = spatial.distance.correlation(s1,s2)
    return s

####