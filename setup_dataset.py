from req_py_libraries import *


core_directory = "/scry/securities/"
mobar = 'logi_preds.json'
mobar2 = 'logi_preds3.json'


def fetch_data(suid,bt,ll):
    eod_data = read_json(path=core_directory+str(suid)+"/eod.json")
    dates = sorted(eod_data)
    df = pd.DataFrame(columns=['Timestamp', 'Open', 'High',"Low","Close","Volume"])
    df_list = []
    for key in dates:
        ddict = {}
        cobra = datetime.strptime(key,"%Y-%m-%d").date()
        ddict["Timestamp"] = datetime.strptime(key,"%Y-%m-%d").date()
        ddict["Volume"] = float(eod_data[key]["adj_volume"])
        ddict["Open"] = float(eod_data[key]["adj_open"])
        ddict["Close"] = float(eod_data[key]["adj_close"])
        ddict["High"] = float(eod_data[key]["adj_high"])
        ddict["Low"] = float(eod_data[key]["adj_low"])
        if (datetime.now().date()-cobra).days >= int(bt): # upper limit
            if (datetime.now().date()-cobra).days <= int(ll) + int(bt): # lower limit
                df_list.append(ddict)
    if len(df_list) > 0:
        df = df.append(df_list)
    return df

# make change here to change upper limit of cutoff to a particular date
def fetch_data2(suid,lag=0):
    eod_data = read_json(path=core_directory+str(suid)+"/eod.json")
    dates = sorted(eod_data) # sorted in ascending order of dates
    df = pd.DataFrame(columns=['Timestamp', 'Open', 'High',"Low","Close","Volume"])
    df_list = []
    count = 0
    for key in dates:

        if count > (len(dates) - 600):
            #d1 = datetime.strptime(key, "%Y-%m-%d").date()
            #d2 = datetime.strptime("2019-07-31", "%Y-%m-%d").date()
            #cond2 = (d2 - d1).days >= 0
            cond2 = True
            if cond2:
                ddict = {}
                ddict["Timestamp"] = datetime.strptime(key,"%Y-%m-%d").date()
                ddict["Volume"] = float(eod_data[key]["adj_volume"])
                ddict["Open"] = float(eod_data[key]["adj_open"])
                ddict["Close"] = float(eod_data[key]["adj_close"])
                ddict["High"] = float(eod_data[key]["adj_high"])
                ddict["Low"] = float(eod_data[key]["adj_low"])
                df_list.append(ddict)
        count += 1
    if len(df_list) > 0:
        df = df.append(df_list)
    return df



def find_blag(timestamps,delay):
    timestamps = list(timestamps)
    start_date = datetime.now().date()
    tgt_date = start_date - timedelta(days=delay)

    stat = True
    while stat:
        try:
            gothra = timestamps.index(tgt_date)
            gothra = len(timestamps) - gothra
            stat = False
            mothra = True
        except:
            try:
                tgt_date = tgt_date + timedelta(days=1)
            except:
                stat = False
                gothra = False
                mothra = False

    return [mothra,gothra]


# end index for prediction model
def find_blag2(timestamps,start):
    timestamps = list(timestamps)
    start_date = timestamps[start]
    idx = start
    stat = True
    try:
        while stat:
            idx += 1
            temp_date = timestamps[idx]
            if (temp_date - start_date).days >= 30:
                idx = idx-1
                stat = False
                break
    except:
        idx = len(timestamps)

    return idx


def find_blag_lag(timestamps,bt_date):
    timestamps = list(timestamps)
    bt_date = datetime.strptime(bt_date,"%Y-%m-%d").date()
    stat2 = True
    start_idx = False
    try:
        start_idx = timestamps.index(bt_date)
        start_idx = len(timestamps) - start_idx
    except:stat2 = False

    return [stat2,start_idx]


def get_tis(df):
    df = utils.dropna(df)
    df = add_all_ta_features(df, "Open", "High", "Low", "Close", "Volume", fillna=True)
    return df


def get_tis_special(df):
    df= utils.dropna(df)
    df = money_flow_index(df['High'],df['Low'],df['Close'],df['Volume'],n=20,fillna=True)
    return df



def clean_out_store(suid):
    ddict = {}
    tgt_directory = core_directory+str(suid)+'/'+mobar
    dump_json(path=tgt_directory,data=ddict)

    tgt_directory = core_directory + str(suid) + '/' + mobar2
    dump_json(path=tgt_directory, data=ddict)

