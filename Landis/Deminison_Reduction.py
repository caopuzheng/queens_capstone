from scipy.fft import fft
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt
import seaborn as sns

#
def dfft(s1):
    new_array = fft(s1)
    # Number of samplepoints
    N = len(new_array)
    # sample spacing
    T = 1.0 / 800.0
    yf = new_array
    xf = np.linspace(0.0, 1.0/(2.0*T), N//2)

    plt.close()
    plt.plot(xf, 2.0/N * np.abs(yf[0:N//2]))
    plt.grid()
    plt.show()
    return new_array

#SVD

#Wavelet Transform

#Seasonal decompostion:
###break down the time series to noise*trend*seasonal
###similar bonds should have similiar major trend
def seasonal_decomp(time_series):
    result = seasonal_decompose(time_series.tolist(), model='Additive', freq=7)
    return result.trend
