from req_py_libraries import *



def predicted_values(df,lag,cutoff):

    count = 0
    predicted_val = []
    for i,row in df.iterrows():

        if count < len(df.index)-lag:
            temp_df = df[(count+1):(count+1+lag)]
            close = row['Close']
            high = max(temp_df["High"])
            change_percent = np.round_((high-close)/close,3)
            if change_percent >= cutoff: predicted_val.append(1)
            else: predicted_val.append(0)

        else:
            predicted_val.append(-1)
        count += 1

    df["predicted"] = predicted_val

    return df
