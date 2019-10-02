from req_py_libraries import *



def calc_accuracy(y_act,y_pred):
    count = 0
    correct_preds = 0
    for _ in y_act:
        if y_act[count][0] == y_pred[count]:
            correct_preds += 1
        count += 1
    accuracy = np.round_(correct_preds/count,3)
    return accuracy


def logi_model(x,y,directory):
    kf = KFold(n_splits=2)
    kf.get_n_splits(x)
    accuracy = []

    for train_index, test_index in kf.split(x):
        y_train = y[train_index]
        y_test = y[test_index]
        x_train = x[train_index]
        x_test = x[test_index]

        lr = LogisticRegression(solver='lbfgs')
        lr.fit(x_train, y_train.ravel())

        y_test_pred = lr.predict(x_test)
        accuracy.append(calc_accuracy(y_act=y_test,y_pred=y_test_pred))

    lr = LogisticRegression(solver='lbfgs')
    lr.fit(x,y.ravel())
    filename = directory + '_logi_model.sav'
    save_model(directory=filename,model=lr)
    
    return np.mean(accuracy)

def logi_model2(x,logi_model):
    y_pred = logi_model.predict(x)
    return y_pred
