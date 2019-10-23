import pandas as pd
import multiprocessing
from geopy.distance import distance, lonlat


def read(path, sep, usecols, column_names):
    global in_txt
    in_txt = pd.read_csv(path, sep=sep, usecols=usecols, names=column_names)


def process(No_list):
    global in_txt
    tmp_ans = pd.DataFrame(columns=['No', 'jing', 'wei', 'time'])
    for item in No_list:
        flag = True
        temp = in_txt.query('No == %i' % item)
        temp = temp.sort_values(by='time')
        length = int(temp.size / 4)
        if (length <= 1):
            continue
        for i in range(length):
            first_row = temp.iloc[i]
            if i + 1 >= length:
                break
            second_row = temp.iloc[i + 1]
            first_place = (first_row['jing'], first_row['wei'])
            second_place = (second_row['jing'], second_row['wei'])
            if distance(lonlat(*first_place), lonlat(*second_place)).kilometers > max_difference:
                if i + 2 >= length:
                    break
                else:
                    third_row = temp.iloc[i + 2]
                    third_place = (third_row['jing'], third_row['wei'])
                    if distance(lonlat(*first_place), lonlat(*third_place)).kilometers > 2 * max_difference:
                        flag = False
                        break
                    else:
                        temp.iloc[i + 1]['jing'] = (first_row['jing'] + third_row['jing']) / 2
                        temp.iloc[i + 1]['wei'] = (first_row['wei'] + third_row['wei']) / 2
        if flag:
            tmp_ans = tmp_ans.append(temp)
    return tmp_ans


#
# def main():
#     max_difference = 1
#     read('./1804010000.txt', sep='|', usecols=[0, 9, 10, 11])
#     # in_txt = in_txt.sort_values(by='Noo')
#
#     Noo_list = in_txt['Noo']
#     Noo_list = Noo_list.drop_duplicates("first")
#     Noo_list = Noo_list.sort_values()
# #    ans = pd.DataFrame(columns=['Noo', '8', 'jing', 'wei'])
#
#     ans = process(Noo_list, in_txt, max_difference)
#
#     ans.to_csv('./processed.csv', index=False)


if __name__ == '__main__':
    column_names = ['No', 'jing', 'wei', 'time']
    max_difference = 2
    read('./GPS_2017_06_01', sep=',', usecols=[0, 1, 2, 3])
    # in_txt = in_txt.sort_values(by='Noo')

    No_list = in_txt['No']
    No_list = No_list.drop_duplicates("first")
    No_list = No_list.sort_values()
    #    ans = pd.DataFrame(columns=['Noo', '8', 'jing', 'wei'])

    processes = 18

    length = No_list.size
    process_length = int(length / processes)
    list = [pd.Series(name='No')]
    for i in range(processes):
        list.append(No_list.iloc[i * process_length:(i + 1) * process_length])

    pool = multiprocessing.Pool(processes=processes)
    pool_results = pool.map_async(process, list).get()
    pool.close()
    pool.join()

    ans = pd.DataFrame(columns=['No', 'jing', 'wei', 'time'])
    for item in pool_results:
        ans = ans.append(item)

    ans.to_csv('./processed_01.csv', index=False)
