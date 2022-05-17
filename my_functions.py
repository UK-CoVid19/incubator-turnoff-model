import pandas as pd
import numpy as np


def datestr_to_seconds(datestr, datestr_reference):
    d_year = int(datestr[0:4]) - int(datestr_reference[0:4])
    d_month = int(datestr[5:7]) - int(datestr_reference[5:7])
    d_day = int(datestr[8:10]) - int(datestr_reference[8:10])
    d_hour = int(datestr[11:13]) - int(datestr_reference[11:13])
    d_min = int(datestr[14:16]) - int(datestr_reference[14:16])
    d_sec = int(datestr[17:19]) - int(datestr_reference[17:19])

    return d_year * 356 * 24 * 60 * 60 + d_month * 30 * 24 * 60 * 60 + d_day * 24 * 60 * 60 + d_hour * 60 * 60 + d_min * 60 + d_sec


def error_turnoff_model(time, temperature_data, initial_temperature):
    rho_cp = 1125.6
    UA = 0.239
    dQ = 3.96
    model = initial_temperature - dQ / UA * (1 - np.exp(-(time * UA / rho_cp)))
    error = (temperature_data - model) ** 2

    return error.sum()
