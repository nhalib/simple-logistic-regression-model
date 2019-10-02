from req_py_libraries import *
from setup_dataset import *
from setup_pca import *
from setup_logi_model import *
from prediction_core_be import *


kdirectory = "/scry/stocks_usa/exch_tick_cik.json"
mdirectory = "/scry/"
mdirectory1 = "/scry_learning/"
m2directory = "/scry/securities/"
ldirectory_base = "/scry/stocks_usa/logi_models/logi_model_perf_"
wdirectory = "/scry_lp/suid_key.json"
gobar = '/scry_learning/logi_master/be/'
kobar = "/logi_preds.json"
kobar2 = "/logi_preds3.json"
mobar = "logi_master/logi_model_"

def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def partitioner(suid_data,tgt_sector):
    eles = [int(x['UID']) for x in suid_data if x['content']['sector'].find(tgt_sector) >= 0]
    eles_size = int(len(eles)/8)
    if eles_size == 0:
        eles_size = 1
    divisions = divide_chunks(l=eles,n=eles_size)
    return divisions


def get_actual_change(eod_data,key,lag):

    dates = sorted(eod_data)
    change = 'na'
    close_val = 'na'
    if True:
        try:
            ll = dates.index(key)
            ul = dates.index(key) + lag + 1
            close_val = float(eod_data[dates[ll]]['adj_close'])
            ll += 1
            max_val = 0
            while ll <= ul:
                temp_val = float(eod_data[dates[ll]]['adj_high'])
                if  temp_val > max_val:
                    max_val = temp_val

                ll += 1
            change = np.round(100*(max_val-close_val)/close_val,4)
        except:
            pass
    return [max_val,close_val]


def final_pred_backtrack(suid):
    eod_data = read_json(path=m2directory + str(suid) + "/eod.json")
    try:master_dict = read_json(path=m2directory+str(suid)+kobar)
    except:master_dict = {}
    kika = {}
    stat = True
    try:
        data = read_json(path=gobar+str(suid)+".json")
    except:
        stat = False
    if stat:
        for key in data:
            df = pd.DataFrame(data[key])
            df["accuracy"] = pd.to_numeric(df["accuracy"])
            df["cutoff"] = pd.to_numeric(df["cutoff"])
            df["lag"] = pd.to_numeric(df["lag"])
            lags = [5]
            for lag in lags:
                temp_df = df[df['lag'] == lag]
                temp_df = temp_df[temp_df['accuracy']>=0.85]
                temp_df = temp_df.sort_values(by=['cutoff'],ascending=False)
                temp_df = temp_df.reset_index()
                if len(temp_df.index>0):
                    accuracy = temp_df.loc[0,'accuracy']
                    cutoff = temp_df.loc[0,'cutoff']
                    mfi=temp_df.loc[0,'short_mfi']
                    [actual,close_val] = get_actual_change(eod_data=eod_data,key=key,lag=lag)
                    slip = 1

                    end_val = np.round_(close_val *(1+(slip*cutoff/100)),2)
                    try:
                        master_dict[key][str(lag)] = \
                                {'bool':'y','passed':'pending','end_val':str(end_val),'start_val':str(close_val),'mfi':str(mfi),'accuracy':str(accuracy),'cutoff':str(cutoff),'actual':str(actual)}
                    except:
                        master_dict[key] = {}
                        master_dict[key][str(lag)] = \
                                {'bool':'y','passed':'pending','end_val':str(end_val),'start_val':str(close_val),'mfi':str(mfi),'accuracy':str(accuracy),'cutoff':str(cutoff),'actual':str(actual)}
                    try:
                        kika[key][str(lag)] = \
                            {'bool': 'n', 'passed': 'pending', 'end_val': str(end_val), 'start_val': str(close_val),
                             'mfi': str(mfi), 'accuracy': str(accuracy), 'cutoff': str(cutoff), 'actual': str(actual)}
                    except:
                        kika[key] = {}
                        kika[key][str(lag)] = \
                            {'bool': 'n', 'passed': 'pending', 'end_val': str(end_val), 'start_val': str(close_val),
                             'mfi': str(mfi), 'accuracy': str(accuracy), 'cutoff': str(cutoff), 'actual': str(actual)}

        try:dump_json(path=m2directory+str(suid)+kobar,data=master_dict)
        except:pass

        #try:
        feed_to_modelb(ddict=kika,eod_data=eod_data,loc=m2directory+str(suid)+kobar2)
        #except:pass

# main logistic regression prediction model ... run @ high frequency
def pred(suid,bt,tgt_sector,ll,super_dict,momo=False,bt_date="2019-08-20"):
    ldirectory = ldirectory_base + tgt_sector+"_"+str(ll)+".json"
    condinue = True
    try:perf_data = read_json(path=ldirectory)[str(suid)][str(bt)]
    except:condinue = False

    if condinue:
        tgt_directory = mdirectory1 + mobar+str(bt)+"_"+str(ll)+"/" + str(suid)
        df = fetch_data2(suid=suid)
        if len(df.index) > 0:
            timestamps = df["Timestamp"].values

            df_temp = get_tis_special(df)
            short_mfis = df_temp.values
            if not momo:
                [stat2,blag] = find_blag(timestamps=timestamps,delay=bt)
            elif momo:
                [stat2,blag] = find_blag_lag(timestamps=timestamps,bt_date=bt_date)

            stat = True
            if stat and stat2:

                blag2 = find_blag2(timestamps=timestamps, start=(len(timestamps) - blag))
                df = get_tis(df=df)  # technical indicators
                df = df.dropna()
                lags = [5]

                for lag in lags:
                    cutoff = 0.5
                    while cutoff <= 5:
                        proceed = False
                        for row in perf_data:
                            if float(row["cutoff"]) == (cutoff/100) and float(row["lag"]) == lag:
                                accuracy = float(row["accuracy"])
                                proceed = True
                                break

                        if proceed:
                            sdirectory = tgt_directory + "/" + str(int(lag)) + "_" + str(int(cutoff * 100))
                            filename = sdirectory + "_ss.sav"
                            ss_model = read_model(directory=filename)

                            filename = sdirectory + "_pca.sav"
                            pca_model = read_model(directory=filename)

                            filename = sdirectory + "_logi_model.sav"
                            logi_model = read_model(directory=filename)

                            components = pca_it2(df=df,ss_model=ss_model,pca_model=pca_model)
                            y_pred = logi_model2(x=components,logi_model=logi_model)

                            #y_pred holds the prediction for past 80 days of trade from today

                            sizee = len(y_pred)
                            y_pred_temp = y_pred[(sizee-blag):blag2]
                            timestamps_temp = timestamps[(sizee-blag):blag2]
                            short_mfis_temp = short_mfis[(sizee-blag):blag2]
                            #print(sizee,sizee-blag,blag2)
                            sizee = len(y_pred_temp)
                            for i in range(0,sizee):
                                mfi = short_mfis_temp[i]
                                if y_pred_temp[i] == 1 : # close 
                                

                                    ddict = {'accuracy':accuracy,'lag':lag,'cutoff':cutoff,'short_mfi':mfi}
                                    try:
                                        super_dict[str(timestamps_temp[i])].append(ddict)
                                    except:
                                        super_dict[str(timestamps_temp[i])] = []
                                        super_dict[str(timestamps_temp[i])].append(ddict)
                        cutoff += 0.25
    return super_dict


#for cumulative
def pred_core(bts,suids,tgt_sector,ll,fstat):
        gonzo = True
        for suid in suids:
            if gonzo:
                if fstat:super_dict = read_json(path=gobar+str(suid)+".json")
                else:super_dict = {}
                for bt in bts:
                        super_dict = pred(suid=suid,bt=bt,tgt_sector=tgt_sector,ll=ll,super_dict=super_dict,momo=False)
                dump_json(path=gobar+str(suid)+'.json', data=super_dict)
            final_pred_backtrack(suid)


#for current
def pred_core_daily(bts, suids, tgt_sector, ll, fstat,pred_date):
    for suid in suids:
        super_dict = {}
        for bt in bts:
            super_dict = pred(suid=suid, bt=bt, tgt_sector=tgt_sector, ll=ll, super_dict=super_dict,momo=True,bt_date=pred_date)

        dump_json(path=gobar + str(suid) + '.json', data=super_dict)

        final_pred_backtrack(suid)


def cumulative_prediction(tgt_sector,ll):
    clean = True
    if clean:
        suid_data = read_json(path=kdirectory)
        for row in suid_data:
            if row['content']['sector'].find(tgt_sector) >= 0:
                clean_out_store(suid=row['UID'])

    bts = [246,226,206,186,166,146,126,106,80, 60, 40, 20, 0]
    suid_data = read_json(path=kdirectory)
    divs = list(partitioner(suid_data=suid_data, tgt_sector=tgt_sector))

    fstat = False
    processes = []
    for i in range(0,len(divs)):
        suids = divs[i]
        processes.append(multiprocessing.Process(target=pred_core, args=(bts,suids,tgt_sector,ll,fstat,)))

    for p in processes: p.start()
    for p in processes: p.join()



def current_prediction(tgt_sector,ll,pred_date):

    bts = [0]
    suid_data = read_json(path=kdirectory)
    divs = list(partitioner(suid_data=suid_data, tgt_sector=tgt_sector))
    fstat = True
    processes = []
    for i in range(0, len(divs)):
        suids = divs[i]
        processes.append(multiprocessing.Process(target=pred_core_daily, args=(bts,suids,tgt_sector, ll,fstat,pred_date,)))

    for p in processes: p.start()
    for p in processes: p.join()


sectors1 = ["transportation",'electronics',"automotive","aerospace","computer","communication"]
sectors2 = ["oil and gas","instruments","electric","food","semiconductor","internet","software","retail"]
sectors3 = ["bank"]
sectors4 = ['agriculture', 'mining', 'utility', 'manufacturing', 'publishing', 'building products', 'chemical',
               'diversified operations', 'real estate', 'wireline', 'gaming', 'social network', 'wireless',
               'insurance', 'business', 'beverage', 'school', 'cosmetics', 'financial']
sectors = sectors1 + sectors2 + sectors3 + sectors4

tgt_sectors = sectors



script_stat = True
if script_stat:
    pred_date = sys.argv[1]
    #pred_date = "2019-09-04"
    for tgt_sector in tgt_sectors:
           current_prediction(tgt_sector=tgt_sector,ll=2362,pred_date=pred_date)
           #print(tgt_sector)
