from datetime import datetime, timedelta
import datetime as dt
from dateutil import relativedelta
import numpy as np
import time
import math
import pandas as pd
from sklearn import linear_model
import json
from sklearn.linear_model import LogisticRegression
from ta import *
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.model_selection import KFold
import pickle
import os
import multiprocessing
import shutil
import sys

def save_model(directory,model):
    filename = directory
    pickle.dump(model, open(filename, 'wb'))

def read_model(directory):
    filename = directory
    model = pickle.load(open(filename, 'rb'))
    return model

#load json file
def read_json(path):
    with open(path) as json_data:
        data = json.load(json_data)
        json_data.close()
    return data

# dump data to json file
def dump_json(path,data):
    with open(path, 'w') as outfile:
        json.dump(data, outfile)

