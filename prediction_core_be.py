from req_py_libraries import *


def feed_to_modelb(ddict,eod_data,loc):

    try:sdata = read_json(path=loc)
    except:sdata = {}

    slip = 1
    eod_data_keys = sorted(eod_data)
    for key in ddict.keys():
            idx = eod_data_keys.index(key)
            try:
                idx_new = eod_data_keys[idx+1]
                open_val = float(eod_data[idx_new]["adj_open"])
                ddict[key]["5"]["start_val"] = str(open_val)
                cutoff = float(ddict[key]["5"]["cutoff"])
                end_val = np.round_(open_val *(1+(slip*cutoff/100)),2)
                ddict[key]["5"]["end_val"] = str(end_val)
            except:
                # only if current date
               ddict[key]["5"]["start_val"] = "na"
               ddict[key]["5"]["end_val"] = "na"

            sdata[key] = ddict[key]

    dump_json(path=loc,data=sdata)