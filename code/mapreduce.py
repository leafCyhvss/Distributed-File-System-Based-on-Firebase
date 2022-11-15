import json
import requests
import pandas as pd
from EDFS2 import EDFSURL
import edfs

edfs_url = edfs.url  # https://demo01-76e03-default-rtdb.firebaseio.com/
EDFS2_url = 'https://ds551-ad195-default-rtdb.firebaseio.com/'
e1 = EDFSURL()


def map_edfs(url1: str):
    dataset = pd.DataFrame()
    for item in requests.get(url1).json():
        item = item.replace("'", "\"")
        item_dict = json.loads(item)
        dataset = dataset.append(item_dict, ignore_index=True)
    dataset['price'] = pd.to_numeric(dataset['price'])
    dataset['mileage'] = pd.to_numeric(dataset['mileage'])
    dataset['tax'] = pd.to_numeric(dataset['tax'])
    dataset['mpg'] = pd.to_numeric(dataset['mpg'])
    dataset['engineSize'] = pd.to_numeric((dataset['engineSize']))
    return dataset


def map_EDFS2(url2: str):
    jsonContent = e1.get(url2)
    return jsonContent


def trim_str(l: list) -> list:
    index = 0
    for i in l:
        l[index] = i.rsplit(" ")[-1]
        index += 1
    return l


# def get_data_audi_ford(locationList: list, url2: str) -> pd.DataFrame():
#     index = 1
#     dataframe = pd.DataFrame()
#     for i in locationList:
#         url_i = url2 + "actualData/" + i + ".json"
#         temp_dataframe = pd.DataFrame()
#         jsonContent_i = requests.get(url_i).json()
#         for content in jsonContent_i:
#             temp_dataframe = temp_dataframe.append(content, ignore_index=True)
#         group_by: pd.DataFrame = temp_dataframe.groupby('year')['price'].mean().to_frame()
#         dataframe = pd.concat([dataframe, group_by], axis=0)
#         index += 1
#     return dataframe  # .groupby('year')['price'].mean()  # : change this line to your analytics


# for analytics
def analytics_1_edfs(url1: str, index: int):
    result: pd.DataFrame = pd.DataFrame()
    # call mapping function
    url1_partition = map_edfs(url1)
    # : start of analytics function
    # ...
    return result


def analytics_2_edfs(url1: str, index: int):
    result: pd.DataFrame = pd.DataFrame()
    # call mapping function
    url1_partition = map_edfs(url1)
    # : start of analytics function
    # ...
    return result


def analytics_1_EDFS2(url2: str):
    result = pd.DataFrame()
    # call mapping function
    jsonContent = map_EDFS2(url2)
    temp_dataframe = pd.DataFrame()
    for content in jsonContent:
        temp_dataframe = temp_dataframe.append(content, ignore_index=True)
    # : start of analytics function
    # ...
    return result


def analytics_2_EDFS2(url2: str):
    result = pd.DataFrame()
    # call mapping function
    jsonContent = map_EDFS2(url2)
    temp_dataframe = pd.DataFrame()
    for content in jsonContent:
        temp_dataframe = temp_dataframe.append(content, ignore_index=True)
    # : start of analytics function
    # ...
    return result


def reduce(url1: str, url2: str, result):
    combinedDataFrame = pd.DataFrame()
    for i in range(1, 8):
        url1_partition_url = url1 + "data/-user-yyy-toyota" + str(i) + ".json"
        analytics_1_edfs_result = analytics_1_edfs(url1_partition_url, i)
        analytics_2_edfs_result = analytics_2_edfs(url1_partition_url, i)
        # : start of original nested analytics function
        # url1_partition = map_edfs(url1_partition_url)
        # group_by: pd.DataFrame = url1_partition.groupby('year')['price'].mean().to_frame()
        # combinedDataFrame = pd.concat([combinedDataFrame, group_by], axis=0)
        # : end of original nested analytics function
    ToyotaDataFrame = pd.concat(combinedDataFrame)
    audi_locations = trim_str(e1.getPartitionLocations("/test1/audi.csv"))
    audi_index = 0
    for i in audi_locations:
        analytics_1_EDFS2_result_audi = analytics_1_EDFS2(i)
        analytics_2_EDFS2_result_audi = analytics_2_EDFS2(i)
        audi_index += 1
    ford_locations = trim_str(e1.getPartitionLocations("/test2/23/ford.csv"))
    ford_index = 0
    for i in ford_locations:
        analytics_1_EDFS2_result_ford = analytics_1_EDFS2(i)
        analytics_2_EDFS2_result_ford = analytics_2_EDFS2(i)
        ford_index += 1
    # : start of original nested analytics function
    # AudiDataFrame = get_data_audi_ford(audi_locations, url2)
    # FordDataFrame = get_data_audi_ford(ford_locations, url2)
    # for j in range(len(AudiDataFrame)):
    #     if 5000 < AudiDataFrame.iloc[j, 5] < 10000:
    #         dict_df = {AudiDataFrame.iloc[j, 8]: AudiDataFrame.iloc[j, 5]}
    #         df = pd.DataFrame(dict_df)
    #         combinedDataFrame = combinedDataFrame.append(df)
    # for j in range(len(FordDataFrame)):
    #     if 5000 < FordDataFrame.iloc[j, 5] < 10000:
    #         dict_df = {FordDataFrame.iloc[j, 8]: FordDataFrame.iloc[j, 5]}
    #         df = pd.DataFrame(dict_df)
    #         combinedDataFrame = combinedDataFrame.append(df)
    # : end of original nested analytics function; the next line is for concatenating audi  and ford dataset
    # combinedDataFrame1 = pd.concat([AudiDataFrame, FordDataFrame], axis=0)
    # : new reduce for EDFS2
    combinedDataFrame1 = pd.DataFrame()
    # : start of actual reducing
    TotalDataFrame = pd.concat([combinedDataFrame1, ToyotaDataFrame], axis=0)  # : .groupby('year').mean()
    # : end of actual reducing
    # : start of original nested analytics function aggregation processing
    # TotalDataFrame = TotalDataFrame.fillna(0).round(2)
    # TotalDataFrame['ave_price'] = TotalDataFrame[0] + TotalDataFrame['price']
    # TotalDataFrame.drop(TotalDataFrame[[0]], axis=1, inplace=True)
    # TotalDataFrame.drop('price', axis=1, inplace=True)
    # : end of original nested analytics function aggregation processing
    return TotalDataFrame


if __name__ == '__main__':
    print("welcome Mapreduce")
    # print(reduce(edfs_url, EDFS2_url))
    print(reduce(edfs_url, EDFS2_url))
