from req_py_libraries import *

tloc = "/scry/securities/"
bloc = "/scry/stocks_usa/exch_tick_cik.json"


def get_date(tdate):
    if tdate.weekday() == 5:
        tdate = tdate - timedelta(days=1)
    elif tdate.weekday() == 6:
        tdate = tdate - timedelta(days=2)
    return tdate


def tday_pred(suid,tdate):
    tdate = tdate - timedelta(days=1) # prediction made on the day before
    tdate = get_date(tdate=tdate)
    tgt_loc = tloc + str(suid) + "/logi_preds3.json"
    try:tgt_data = float(read_json(path=tgt_loc)[str(tdate)]['5']['end_val'])
    except:tgt_data = "na"
    return tgt_data


def tday_eod(suid,tdate):
    tgt_loc = tloc + str(suid) + "/eod.json"
    try:tgt_data = float(read_json(path=tgt_loc)[str(tdate)]['adj_open'])
    except:tgt_data = "na"
    return tgt_data


def l0(tgt_sector,tdate = datetime.now().date()):
    suid_data = read_json(path=bloc)
    tcount = 0
    pcount = 0
    for row in suid_data:
        if row['content']['sector'].find(tgt_sector) >= 0:
            suid = row['UID']
            ticker = row['content']['ticker']
            pred_val = tday_pred(suid=suid,tdate=tdate)
            open_val = tday_eod(suid=suid,tdate=tdate)
            if pred_val != "na" and open_val != "na":
                print(suid,ticker,pred_val,open_val)
                tcount += 1
                #print(ticker,pred_val,open_val)
                if float(open_val) < float(pred_val):
                    pcount += 1
                    pass
    print(pcount,tcount)



def l4(tgt_sector):
    cutoff = datetime.strptime("2019-01-01","%Y-%m-%d").date()
    cutoff2 = datetime.strptime("2019-07-22", "%Y-%m-%d").date()
    suid_data = read_json(path=bloc)
    tcount = 0
    pcount = 0
    for row in suid_data:
        if row['content']['sector'].find(tgt_sector) >= 0:
            suid = row['UID']
            ticker = row['content']['ticker']
            pred_data = read_json(path=tloc + str(suid) + "/logi_preds3.json")

            for key in pred_data.keys():
                tdate = datetime.strptime(key,"%Y-%m-%d").date()
                if tdate >= cutoff and tdate <= cutoff2 :
                    pred_val = pred_data[key]["5"]["passed"]
                    tcount += 1
                    if pred_val == "passed":
                        #print(suid, ticker, pred_val, open_val)
                        #tcount += 1
                        # print(ticker,pred_val,open_val)
                        #if float(open_val) < float(pred_val):
                        pcount += 1
                        #    pass
            #if tcount > 0:
            #    print(pcount/tcount)
    print(pcount/tcount)

