import datetime
import string

import psycopg2 as psql
import vk

connection = psql.connect(dbname='crawlerone', user='postgres', password='', host='localhost')
cursor = connection.cursor()

now = datetime.datetime.now()

GROUP_URN = "itis_kfu"
TOKEN = '45eec18445eec18445eec184c745985afd445ee45eec18425d9183af519b65585e20db0'


def get_wall(group_urn):
    first = vk_api.wall.get(domain=group_urn, count=100, v=5.137)
    posts = first["items"]
    posts = posts + vk_api.wall.get(domain=group_urn, count=100, offset=100, v=5.137)["items"]
    return posts


if __name__ == "__main__":
    session = vk.Session(access_token=TOKEN)
    vk_api = vk.API(session)
    wall = get_wall(GROUP_URN)

    tt = str.maketrans(dict.fromkeys(string.punctuation))

    dictionary = {}

    for post in wall:
        text = post["text"].translate(tt).replace("«", "").replace("\"", "").replace("»", "").lower().split()

        for word in text:
            if word in dictionary:
                dictionary[word] += 1
            else:
                dictionary[word] = 1

    table_name = "data" + str(now.strftime("%d%m%Y%H%M"))

    cursor.execute("CREATE TABLE " + table_name + " (id bigserial, word varchar, count bigint)")
    connection.commit()

    for val in dictionary:
        insert = "INSERT INTO " + table_name + " (word, count) VALUES ('" + val + "', " + str(dictionary[val]) + ");"
        cursor.execute(insert)
    connection.commit()
    connection.close()
