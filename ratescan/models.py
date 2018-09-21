import numpy as np

def powerLaw(t, m, a, b):
    """
    Model for the shower dominated part of a ratescan
    """
    return m*np.power(t, a)+b

def powerLawProton(t, m, b):
    """
    Model for the shower dominated part of a ratescan given a fixed 
    spectral index of -2.7 as for protons
    """
    return powerLaw(t, m, -2.7, b)

def nsbContribution(t, m, t_0):
    """
    Model for the nsb dominated part of a ratescan
    """
    return np.exp(m*(t - t_0))

def ratescan_func(t, m, a, b, m_0, t_0):
    """
    Model for both regions of a ratescan without the saturated part
    """
    return nsbContribution(t, m_0, t_0) + powerLaw(t, m, a, b)