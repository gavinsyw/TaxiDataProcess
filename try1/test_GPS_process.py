import pandas as pd


def main():
    max_difference = 1
    in_txt = pd.read_csv('./1804010000.txt', sep='|', usecols=[0, 9, 10, 11])
    # in_txt = in_txt.sort_values(by='Noo')

    Noo_list = in_txt['Noo']
    Noo_list = Noo_list.drop_duplicates("first")
    Noo_list = Noo_list.sort_values()
    ans = pd.DataFrame(columns=['Noo', '8', 'jing', 'wei'])

    for item in Noo_list:
        flag = True
        temp = in_txt.query('Noo == %i' % item)
        temp = temp.sort_values(by='8')
        length = int(temp.size / 4)
        if (length <= 1):
            continue
        for i in range(length):
            first_row = temp.iloc[i]
            if i + 1 >= length:
                break
            second_row = temp.iloc[i + 1]
            if abs(first_row['jing'] - second_row['jing']) > max_difference \
                    or abs(first_row['wei'] - second_row['wei']) > max_difference:
                if i + 2 >= length:
                    break
                else:
                    third_row = temp.iloc[i + 2]
                    if abs(first_row['jing'] - third_row['jing']) > 2 * max_difference \
                            or abs(first_row['wei'] - third_row['wei']) > 2 * max_difference:
                        flag = False
                        break
                    else:
                        temp.iloc[i + 1]['jing'] = (first_row['jing'] + third_row['jing']) / 2
                        temp.iloc[i + 1]['wei'] = (first_row['wei'] + third_row['wei']) / 2
        if flag:
            ans = ans.append(temp)

    ans.to_csv('./processed.csv', index=False)


if __name__ == '__main__':
    main()
