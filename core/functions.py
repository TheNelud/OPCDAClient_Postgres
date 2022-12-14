import xml.etree.ElementTree as ET
import psycopg2, time
from data import *
from datetime import datetime
from log import *

logger = get_logger(__name__)

def get_settings(config_file="settings.xml"):   
    try:
        tree = ET.parse(config_file)
        root = tree.getroot()
        res = {}
        for child in root:
            res[child.tag] = child.text
        return res
    except Exception as e:
        logger.warning("Error in settings", e)
   
def create_connection():
    try:
        setting = get_settings()
        connection = psycopg2.connect(database=setting['DB_NAME'],
                                        user=setting['DB_USER'],
                                        password=setting['DB_PASS'],
                                        host=setting['DB_HOST'],
                                        port=setting['DB_PORT'])
        logger.warning("Connecting to the database")
        return connection
    except:
        logger.warning("No connection to the database")
        time.sleep(15)
        create_connection()

def select_all_tags(connect):
    sql_all_tags = f"SELECT tag_name FROM all_tags "
    cursor = connect.cursor()
    cursor.execute(sql_all_tags)
    return [elem for elem in cursor.fetchall()]

def update_all_tags(connect,tag_name, value, status, date_update):
    sql_all_tags = """UPDATE all_tags SET tag_value=%s, date_update=%s, status=%s WHERE tag_name=%s"""
    cursor = connect.cursor()
    cursor.execute(sql_all_tags,(value, date_update, status, tag_name))
    connect.commit()


"""Отправка данных в таблицы отчетов"""

def insert_ser_per_day(connect = create_connection()):
    
    """СЭР за сутки """

    sql_query_select="""SELECT id, tag_name,  tag_value FROM all_tags"""
    cursor = connect.cursor()
    cursor.execute(sql_query_select)
    tempstamp = datetime.now()
    tags_result = []
    for items in cursor.fetchall():
        for i in range(len(data_ser_per_day)):
            if items[1] == data_ser_per_day[i]:
                tags_result.append(items)

    tags_sorted = sorted(tags_result)
    sql_query_insert="""INSERT INTO raport.ser_per_day (ser_1,ser_2,ser_3,ser_4, ser_5, ser_6, ser_7, ser_8, ser_9, ser_10,ser_11,ser_12,ser_13,ser_14,ser_15,ser_16,ser_17,ser_18,ser_19,ser_20,date_update)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    cursor.execute(sql_query_insert, (tags_sorted[0][2],tags_sorted[1][2],tags_sorted[2][2],tags_sorted[3][2],tags_sorted[4][2],tags_sorted[5][2],tags_sorted[6][2],
                                        tags_sorted[7][2],tags_sorted[8][2],tags_sorted[9][2],tags_sorted[10][2],tags_sorted[11][2],tags_sorted[12][2],tags_sorted[13][2],
                                        tags_sorted[14][2],tags_sorted[15][2],tags_sorted[16][2],tags_sorted[17][2],tags_sorted[18][2],tags_sorted[19][2],tempstamp))
    connect.commit()
                
def insert_ser_per_month(connect = create_connection()):
    
    """СЭР за месяц"""

    sql_query_select="""SELECT id, tag_name,  tag_value FROM all_tags"""
    cursor = connect.cursor()
    cursor.execute(sql_query_select)
    tempstamp = datetime.now()
    tags_result = []
    for items in cursor.fetchall():
        for i in range(len(data_ser_per_month)):
            if items[1] == data_ser_per_month[i]:
                tags_result.append(items)

    tags_sorted = sorted(tags_result)
    sql_query_insert="""INSERT INTO raport.ser_per_month (ser_101,ser_102,ser_103,ser_104, ser_105, ser_106, ser_107, ser_108, ser_109, ser_110,ser_111,ser_112,ser_113,ser_114,ser_115,ser_116,ser_117,ser_118,ser_119,ser_120,date_update)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    cursor.execute(sql_query_insert, (tags_sorted[0][2],tags_sorted[1][2],tags_sorted[2][2],tags_sorted[3][2],tags_sorted[4][2],tags_sorted[5][2],tags_sorted[6][2],
                                        tags_sorted[7][2],tags_sorted[8][2],tags_sorted[9][2],tags_sorted[10][2],tags_sorted[11][2],tags_sorted[12][2],tags_sorted[13][2],
                                        tags_sorted[14][2],tags_sorted[15][2],tags_sorted[16][2],tags_sorted[17][2],tags_sorted[18][2],tags_sorted[19][2],tempstamp))
    connect.commit()

def insert_mer_per_month(connect = create_connection()):
    
    """МЭР за месяц"""

    sql_query_select="""SELECT id, tag_name,  tag_value FROM all_tags"""
    cursor = connect.cursor()
    cursor.execute(sql_query_select)
    tempstamp = datetime.now()
    tags_result = []
    for items in cursor.fetchall():
        for i in range(len(data_mer_per_month)):
            if items[1] == data_mer_per_month[i]:
                tags_result.append(items)

    tags_sorted = sorted(tags_result)
    sql_query_insert="""INSERT INTO raport.mer_per_month (
                                mer_1,mer_2,mer_3,mer_4,mer_5,mer_6,mer_7,mer_8,mer_9,mer_10
                                ,mer_11,mer_12,mer_13,mer_14,mer_15,mer_16,mer_17,mer_18,mer_19
                                ,mer_20,mer_21,mer_22,mer_23,mer_24,mer_25,mer_26,mer_27,mer_28
                                ,mer_29,mer_30,mer_31,mer_32,mer_33,mer_34,mer_35,mer_36,mer_37,date_update
                                )VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                        %s,%s,%s,%s,%s,%s,%s,%s)"""

    

    cursor.execute(sql_query_insert, (tags_sorted[0][2],tags_sorted[1][2],tags_sorted[2][2],tags_sorted[3][2],tags_sorted[4][2],tags_sorted[5][2],tags_sorted[6][2],
                                        tags_sorted[7][2],tags_sorted[8][2],tags_sorted[9][2],tags_sorted[10][2],tags_sorted[11][2],tags_sorted[12][2],tags_sorted[13][2],
                                        tags_sorted[14][2],tags_sorted[15][2],tags_sorted[16][2],tags_sorted[17][2],tags_sorted[18][2],tags_sorted[19][2],
                                        tags_sorted[20][2],tags_sorted[21][2],tags_sorted[22][2],tags_sorted[23][2],tags_sorted[24][2],
                                        tags_sorted[25][2],tags_sorted[26][2],tags_sorted[27][2],tags_sorted[28][2],tags_sorted[29][2],
                                        tags_sorted[30][2],tags_sorted[31][2],tags_sorted[32][2],tags_sorted[33][2],tags_sorted[34][2],
                                        tags_sorted[35][2],tags_sorted[36][2],tempstamp))
    connect.commit()


def insert_mag_techno(connect = create_connection()):

    """МЭГ, Датчиковое оборудование на схеме """

    sql_query_select="""SELECT id, tag_name,  tag_value FROM all_tags"""
    cursor = connect.cursor()
    cursor.execute(sql_query_select)
    tempstamp = datetime.now()
    tags_result = []
    for items in cursor.fetchall():
        for i in range(len(data_mag_techno)):
            if items[1] == data_mag_techno[i]:
                tags_result.append(items)

    tags_sorted = sorted(tags_result)
    # print(tags_sorted)
    # print(tags_sorted[28][1])

    sql_query_insert="""INSERT INTO raport.sen_equip (r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,
                                                        r11,r12,dy13,rt13,dy14,rt14,dy15,rt15,dy16,rt16,
                                                        r17,r18,r19,r22,r23,r24,r25,r26,r27,date_update) 
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" #32
    cursor.execute(sql_query_insert, (tags_sorted[0][2],tags_sorted[1][2],tags_sorted[2][2],tags_sorted[3][2],tags_sorted[4][2],tags_sorted[5][2],
                                        tags_sorted[6][2],tags_sorted[7][2],tags_sorted[8][2],tags_sorted[9][2],tags_sorted[10][2],
                                        tags_sorted[11][2],tags_sorted[12][2],tags_sorted[13][2],tags_sorted[14][2],tags_sorted[15][2],
                                        tags_sorted[16][2],tags_sorted[17][2],tags_sorted[18][2],tags_sorted[19][2],tags_sorted[20][2],
                                        tags_sorted[21][2],tags_sorted[22][2],tags_sorted[23][2],tags_sorted[24][2],tags_sorted[25][2],
                                        tags_sorted[26][2],tags_sorted[27][2],tags_sorted[28][2],tempstamp))
    connect.commit()



