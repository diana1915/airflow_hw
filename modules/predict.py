# <YOUR_IMPORTS>
import datetime

import dill
import glob
import os
import json

import pandas as pd


def latest_model():
    list_of_files = glob.glob('/home/airflow/airflow/data/models/*')
    return max(list_of_files, key=os.path.getctime)


def predict_on_data(data):
    with open(f'{latest_model()}', 'rb') as file:
        model = dill.load(file)
    return model.predict(data)[0]


def main():
    list_of_predicts = []
    list_of_id = []
    list_of_files = glob.glob('/home/airflow/airflow/data/test/*')

    for json_file in list_of_files:
        with open(json_file, 'rb') as file:
            data = json.load(file)
        df = pd.DataFrame.from_dict([data])
        list_of_predicts.append(predict_on_data(df))
        list_of_id.append(os.path.basename(json_file).split('.')[0])

    result = pd.DataFrame({'car_id': list_of_id, 'pred': list_of_predicts})
    result.to_csv(f'/home/airflow/airflow/data/predictions/predict_{datetime.datetime.now().strftime("%Y%m%d%H%M")}.csv', index=False)


if __name__ == '__main__':
    main()
