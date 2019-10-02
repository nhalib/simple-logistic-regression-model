# all training related data

from req_py_libraries import *
from setup_dataset import *
from setup_predicted_vals import *
from setup_pca import *
from setup_logi_model import *


kdirectory = "/scry/stocks_usa/exch_tick_cik.json"
mdirectory = "/scry/"
mdirectory1 = "/scry_learning/"
ldirectory = "/scry/stocks_usa/logi_models/logi_model_perf"
directory1 = "/scry/stocks_usa/logi_models/"
gobar = '/scry_learning/logi_master/be/'
jobar = "logi_master/logi_model_"
kobar = "/logi_model_perf_"

def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def partitioner(suid_data,tgt_sector):
    eles = [int(x['UID']) for x in suid_data if x['content']['sector'].find(tgt_sector) >= 0]
    eles_size = int(len(eles)/15)
    if eles_size == 0:
        eles_size = 1
    divisions = divide_chunks(l=eles,n=eles_size)

    return divisions


def feeder(acc_dict,suid,bt,acc_dict_ele):
    if str(suid) in acc_dict:
        if str(bt) in acc_dict[str(suid)]:
            acc_dict[str(suid)][str(bt)] = acc_dict_ele
        else:
            acc_dict[str(suid)][str(bt)] = {}
            acc_dict[str(suid)][str(bt)] = acc_dict_ele
    else:
        acc_dict[str(suid)] = {}
        acc_dict[str(suid)][str(bt)] = {}
        acc_dict[str(suid)][str(bt)] = acc_dict_ele
    return acc_dict


def central_learner(ll,acc_dict,suid,lag=15,cutoff=0.005,bt=40):


    tgt_directory = mdirectory1+jobar+str(bt)+"_"+str(ll)+"/"+str(suid)
    if (os.path.isdir(tgt_directory)):
        pass
    else:
        os.mkdir(tgt_directory)
    sdirectory = tgt_directory+"/"+str(int(lag))+"_"+str(int(cutoff*10000))

    # prepare raw dataset
    df = fetch_data(suid=suid,bt=bt,ll=ll) # raw End of Day data
    df = get_tis(df=df) # technical indicators

    # prepare predicted values
    df = predicted_values(df=df,lag=lag,cutoff=cutoff) # predicted values

    # clear out dirty rows
    df = df.dropna()
    df = df[df["predicted"] != -1]

    # Standardise and PCA
    rslt = pca_it(df=df,directory=sdirectory)

    # Logistic Regression Model
    accuracy = np.round_(logi_model(x=rslt[0],y=rslt[1],directory=sdirectory),3)
    ddict = {"accuracy":str(accuracy),"lag":str(lag),"cutoff":str(cutoff),'bt':str(bt)}

    acc_dict.append(ddict)

    return acc_dict



# main logistic regression learning model ... run @ low frequency
def main_learn_core(bt,tgt_sector,ll,suid_range,file_address):
    lags = [5]

    gojira = True
    for suid in suid_range:
                ldirectory3 = directory1 + str(str(tgt_sector))
                ldirectory3 += file_address

                try:acc_dict = read_json(path=ldirectory3)
                except:acc_dict = {}

                acc_dict_ele = []
                for lag in lags:
                    cutoff = 0.5
                    while cutoff <= 5:
                        temp_cutoff = cutoff / 100
                        temp_lag = lag
                        try:acc_dict_ele = central_learner(ll=ll,suid=suid, lag=temp_lag, cutoff=temp_cutoff,
                                                       acc_dict=acc_dict_ele, bt=bt)
                        except:pass

                        cutoff += 0.25

                acc_dict = feeder(acc_dict, suid, bt, acc_dict_ele)
                dump_json(data=acc_dict, path=ldirectory3)

                if not gojira:
                    break


def master_thread(tgt_sector,ll,bts,suid_range,file_address):
    for bt in bts:
        main_learn_core(bt=bt,tgt_sector=tgt_sector,ll=ll,suid_range=suid_range,file_address=file_address)

def hyperthread_learner(tgt_sector,ll,bts):

    for bt in bts:
        tgt_directory = mdirectory1 + jobar + str(bt) + "_" + str(ll)
        if (os.path.isdir(tgt_directory)):pass
        else:os.mkdir(tgt_directory)

    base_dir = directory1 + str(tgt_sector)

    try:shutil.rmtree(base_dir)
    except:pass

    os.mkdir(base_dir)

    suid_data = read_json(path=kdirectory)
    divs = list(partitioner(suid_data=suid_data,tgt_sector=tgt_sector))

    splits = []
    for i in range(0,len(divs)):
        splits.append(chr(ord("a") + i))

    stat1 = True
    if stat1:
        processes = []
        count = 0

        for split in splits:
            suid_range = divs[count]
            file_address = kobar +split+".json"
            processes.append(
                multiprocessing.Process(target=master_thread,args=(tgt_sector,ll,bts,suid_range,file_address,)))
            count += 1

        for p in processes:p.start()
        for p in processes:p.join()



    return splits



def cumulative(tgt_sector,ll,bts):
    splits = hyperthread_learner(tgt_sector=tgt_sector,ll=ll,bts=bts)

    ddict_final = {}
    for split in splits:
        ldirectory1 = directory1 + str(tgt_sector) + kobar + split + ".json"
        ddict1 = read_json(path=ldirectory1)
        for key in ddict1.keys():
            ddict_final[key] = ddict1[key]
        del ddict1
    tgt_directory = ldirectory + '_' + tgt_sector + '_' + str(ll) + '.json'
    dump_json(path=tgt_directory, data=ddict_final)
    del ddict_final

    base_dir = directory1 + str(tgt_sector)
    shutil.rmtree(base_dir)


def current(tgt_sector,ll,bts):
    bts0 = [246,226,206,186,166,146,126,106,80, 60, 40, 20, 0]
    bts1 = bts0[0:len(bts0) - 1]
    bts2 = bts0[1:len(bts0)]


    suid_data = read_json(path=kdirectory)
    suids = [x['UID'] for x in suid_data if x['content']['sector'].find(tgt_sector) >= 0]
    for suid in suids:
        for i in range(0,len(bts1)):
            dir1 = mdirectory1 + jobar + str(bts1[i]) + "_" + str(ll) + "/" + str(suid) + "/"
            dir2 = mdirectory1 + jobar + str(bts2[i]) + "_" + str(ll) + "/" + str(suid) + "/"
            if (os.path.isdir(dir1) and os.path.isdir(dir2)):
                files = os.listdir(dir2)
                for f in files:
                    shutil.copy(dir2 + f, dir1)

    inter = True

    if inter:

        splits = hyperthread_learner(tgt_sector=tgt_sector, ll=ll, bts=bts)

        ddict_final = {}
        for split in splits:
            ldirectory1 = directory1 + str(tgt_sector) + kobar + split + ".json"
            ddict1 = read_json(path=ldirectory1)
            for key in ddict1.keys():
                ddict_final[key] = ddict1[key]
            del ddict1

        tgt_directory = ldirectory + '_' + tgt_sector + '_' + str(ll) + '.json'
        ddict_previous = read_json(path=tgt_directory)

        for key in ddict_previous.keys():
            if key in ddict_final.keys():
                for i in range(0,len(bts1)):
                    try:ddict_previous[key][str(bts1[i])] = ddict_previous[key][str(bts2[i])]
                    except:pass
                ddict_previous[key]["0"] = ddict_final[key]["0"]
        dump_json(path=tgt_directory,data=ddict_previous)

        base_dir = directory1 + str(tgt_sector)
        shutil.rmtree(base_dir)


def cumulative_trainer():
    bts = [246,226,206,186,166,146,126,106,80, 60, 40, 20, 0]
    sectors1 = ["transportation", 'electronics', "automotive", "aerospace", "computer", "communication"]
    sectors2 = ["oil and gas", "instruments", "electric", "food", "semiconductor", "internet", "software", "retail"]
    sectors3 = ["bank"]
    sectors4 = ['agriculture', 'mining', 'utility', 'manufacturing', 'publishing', 'building products', 'chemical',
                'diversified operations', 'real estate', 'wireline', 'gaming', 'social network', 'wireless',
                'insurance', 'business', 'beverage', 'school', 'cosmetics', 'financial']
    sectors = sectors1 + sectors2 + sectors3 + sectors4
    for tgt_sector in sectors:
        cumulative(tgt_sector=tgt_sector, ll=2362, bts=bts)
        print(tgt_sector)

def single_trainer():
    bts = [0]
    sectors1 = ["transportation", 'electronics', "automotive", "aerospace", "computer", "communication"]
    sectors2 = ["oil and gas", "instruments", "electric", "food", "semiconductor", "internet", "software", "retail"]
    sectors3 = ["bank"]
    sectors4 = ['agriculture', 'mining', 'utility', 'manufacturing', 'publishing', 'building products', 'chemical',
                'diversified operations', 'real estate', 'wireline', 'gaming', 'social network', 'wireless',
                'insurance', 'business', 'beverage', 'school', 'cosmetics', 'financial']
    sectors = sectors1 + sectors2 + sectors3 + sectors4
    for tgt_sector in sectors:
        current(tgt_sector=tgt_sector, ll=2362, bts=bts)
        print(tgt_sector)


single_trainer()