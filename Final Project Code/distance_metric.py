import numpy
from scipy.fftpack import fft,fft2, fftshift
from scipy import spatial
import matplotlib.pyplot as plt
import math
import numpy as np

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

def DTW(a, b):
    an = a.size
    bn = b.size
    pointwise_distance = spatial.distance.cdist(a.reshape(-1,1),b.reshape(-1,1))
    cumdist = np.matrix(np.ones((an+1,bn+1)) * np.inf)
    cumdist[0,0] = 0

    for ai in range(an):
        for bi in range(bn):
            minimum_cost = np.min([cumdist[ai, bi+1],
                                   cumdist[ai+1, bi],
                                   cumdist[ai, bi]])
            cumdist[ai+1, bi+1] = pointwise_distance[ai,bi] + minimum_cost

    return cumdist[an, bn]



## And much more!