from req_py_libraries import *


def pca_it(df,directory):
    all_features = list(df.columns)
    all_features.remove("predicted")
    all_features.remove("Timestamp")
    x = df.loc[:, all_features].values
    y = df.loc[:, ['predicted']].values

    ss = StandardScaler() # standardise datasets
    x = ss.fit_transform(x)
    pca = PCA(n_components=4)
    components = pca.fit_transform(x)

    filename = directory + '_ss.sav'
    save_model(directory=filename,model=ss)
    filename = directory + '_pca.sav'
    save_model(directory=filename, model=pca)

    return [components,y]

def pca_it2(df,ss_model,pca_model):
    all_features = list(df.columns)
    all_features.remove("Timestamp")
    x = df.loc[:, all_features].values

    x = ss_model.transform(x)
    components = pca_model.transform(x)

    return components


