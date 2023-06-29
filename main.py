import json
import os
from os import path

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from pathlib import Path
import json
import glob, json
import pandas as pd
from pathlib import Path
import re
DB_USER = "postgres"
DB_NAME = "hw1"
DB_PASSWORD = "Vrt342zf"
DB_HOST = "127.0.0.1"
#
#
# #Параметры подключения к Postgresql
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

# filesList = []
for filename in glob.glob('D:/Users/Админ/Downloads/egrul/*.json'):
    dataf = pd.read_json(filename, encoding="utf-8")
    # with open(filename, "r", encoding="utf-8") as file:
    #    filesList.append(json.load(file))
    kp_p = dataf['data']
    for count, i in enumerate(kp_p):
        w = i.setdefault('СвОКВЭД')
        try:
            t = w['СвОКВЭДДоп']
            sd = t[1]
            okved = sd['КодОКВЭД']
            # print(f'Код', okved)
            # print(type(okved))
            # result = re.findall(r'[0-9-.]+\.[0-9-.]', okved)
            if okved == '61' or okved =='61.1' or okved =='61.10' or okved =='61.10.1' or okved =='61.10.2' or okved =='61.10.3' or okved =='61.10.4' or okved =='61.10.5' or okved =='61.10.6' or okved =='61.10.7' or okved =='61.10.8' or okved =='61.10.9' or okved =='61.2' or okved =='61.20' or okved =='61.20.1' or okved =='61.20.2' or okved =='61.20.3' or okved =='61.20.4' or okved =='61.20.5' or okved =='61.3' or okved =='61.30' or okved =='61.30.1'  or okved ==' 61.30.2' or okved ==' 61.9' or okved =='61.90':
                # print(f'Наш код', i)
                # print(count)
                okv_d = [okved]
                # print(type(i))
                s = dataf.iloc[[count]]
                print(f'Датафрейм', s)
                s['okvd']= okv_d
                # print(f'Сплюсованный датафрейм', s)
                engine = create_engine(DATABASE_URL)
                s[['ogrn', 'inn', 'kpp', 'okvd', 'name', 'full_name']].to_sql("telecom_companys", engine, if_exists='append')
                # print("Отправлен в бд")
                print('Наш код для БД- Запись в БД отправлена.')
                # print(s)
            else:
                ...
                # print('Не наш оквэд')
        except:
            ...
            # print('Запись ОКВЭД не найдена')


print("Отправлен в бд")

# print("Подсчет выполнен")

# #
# class Base(DeclarativeBase):
#     pass
#
# class Okved(Base):
#     __tablename__ = 'hw_test10'
#
#     index: Mapped[str] = mapped_column(primary_key=True)
#     inn: Mapped[str]
#     kpp: Mapped[str]
#     full_name: Mapped[str]
#
# engine = create_engine(DATABASE_URL)
# Base.metadata.create_all(engine)
# print("Таблица создана")
#
# with Session(engine) as session:
#     for record in filesList:
#         new_name = Okved(
#                  # ogrn=record['ogrn'],
#                  # inn=record['inn'],
#                  kpp=record['kpp'],
#                  full_name=record['full_name'])
#





