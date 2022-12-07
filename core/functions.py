import xml.etree.ElementTree as ET
import psycopg2

def get_settings(config_file="settings.xml"):   
    try:
        tree = ET.parse(config_file)
        root = tree.getroot()
        res = {}
        for child in root:
            res[child.tag] = child.text
        return res
    except Exception as e:
        print(e)
   



# def create_connection():
#     setting = get_settings()
#     print(setting['db_name'])
#     connection = psycopg2.connect(database=setting['db_name'],
#                                     user=setting['db_user'],
#                                     password=setting['db_password'],
#                                     host=setting['db_host'],
#                                     port=setting['db_port'])
#     return connection

# def select_asrmb_all_tags():
#     connect = create_connection()
#     sql_all_tags = f"SELECT tag_name FROM all_tags "
#     cursor = connect.cursor()
#     cursor.execute(sql_all_tags)
#     dict_hfrpok = [elem for elem in cursor.fetchall()]
#     print(dict_hfrpok)


# select_asrmb_all_tags()