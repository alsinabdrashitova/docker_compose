import pandas as pd
import psycopg2
import time


def get_brands(words):
    with open('data/brands_7841.txt', 'r', encoding='utf-8') as file:
        data_brands = file.read()
        data_brands = data_brands.lower()
        data_brands = data_brands.split('\n')
        data_map = {}
        for line in data_brands:
            l = line.split(' ')
            arr = data_map.get(l[0], [])
            arr.append(line)
            data_map[l[0]] = arr

    arr = words
    result = []
    for line in arr:
        str_line = ' '.join(line)
        brand = get_line_brand(line, str_line, data_map)
        if brand:
            new_line = ' '.join(str_line.split(brand))
            new_line = new_line.replace('  ', ' ').strip()
            result.append([new_line, brand])
        else:
            result.append([str_line, ''])

    return result


def get_line_brand(line_arr, str_l, dat):
    for word in line_arr:
        r = dat.get(word, False)
        if r:
            for br in r:
                if br in str_l:
                    return br


def get_category(result):
    dataAssoc = pd.read_csv("data/assoc.csv", sep=';')
    with open('data/sigs_138.txt', 'r', encoding='utf-8') as file:
        arr = file.readlines()
        arr = [i.strip() for i in arr]
    dataSigs = pd.DataFrame(arr)
    dataAssoc['assoc'] = dataAssoc['assoc'].str.lower()
    dataSigs[0] = dataSigs[0].str.lower()

    eat_words = list(dataAssoc[dataAssoc['assoc'] == 'еда']['word'])
    products_words = list(dataAssoc[dataAssoc['assoc'] == 'продукты']['word'])
    water_words = list(dataAssoc[dataAssoc['assoc'] == 'питьё']['word'])
    alco_words = list(dataAssoc[dataAssoc['assoc'] == 'алкоголь']['word'])
    meat_words = list(dataAssoc[dataAssoc['assoc'] == 'мясо']['word'])
    vegetables_words = list(dataAssoc[dataAssoc['assoc'] == 'овощ']['word'])
    fruit_words = list(dataAssoc[dataAssoc['assoc'] == 'фрукт']['word'])
    muka_words = list(dataAssoc[dataAssoc['assoc'] == 'мучное']['word'])
    sigs_word = list(dataSigs[0])
    mol_word = ['молоко', 'творог', 'мол', 'кефир', 'ряженка', 'йогурт']

    jaccardArr = []
    jaccardName = 'n0'
    for i in range(len(result)):
        for j in eat_words:
            if j == result[i]:
                jaccardName = 'eat'
        for j in products_words:
            if j == result[i]:
                jaccardName = 'product'
        for j in water_words:
            if j == result[i]:
                jaccardName = 'water'
        for j in alco_words:
            if j == result[i]:
                jaccardName = 'alco'
        for j in meat_words:
            if j == result[i]:
                jaccardName = 'meat'
        for j in vegetables_words:
            if j == result[i]:
                jaccardName = 'vegetable'
        for j in fruit_words:
            if j == result[i]:
                jaccardName = 'fruit'
        for j in muka_words:
            if j == result[i]:
                jaccardName = 'muka'
        for j in sigs_word:
            if j == result[i]:
                jaccardName = 'sigs'
        for j in mol_word:
            if j == result[i]:
                jaccardName = 'mol'
        jaccardArr.append(jaccardName)

    return jaccardArr


def db_connection():
    con = psycopg2.connect(
        database="db",
        user="db",
        password="123456",
        host="postgres",
        port="5432"
    )

    cur = con.cursor()
    for i in range(1000):
        try:
            cur.execute("SELECT * from data_markup")
            return cur.fetchall()
        except:
            print('База данных еще не готова')
            time.sleep(1)


if __name__ == "__main__":
    data = pd.DataFrame(data=db_connection(),
                        columns=['ind', 'items.name', 'clusters', 'items.price', 'items.quantity'])
    data = data.iloc[:, 1:]

    nmp = data['items.name'].to_numpy()

    i = 0
    list_of_sentance = []
    for sentance in nmp:
        list_of_sentance.append(sentance.split())

    features = pd.DataFrame(get_brands(list_of_sentance), columns=['name', 'brand'])

    elem = []
    for line in list_of_sentance:
        elem.append(line[0])

    features['category'] = get_category(elem)

    data = data.join(features, rsuffix='_right')
    print(data)
