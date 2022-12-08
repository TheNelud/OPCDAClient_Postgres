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
   
def create_connection():
    setting = get_settings()
    connection = psycopg2.connect(database=setting['DB_NAME'],
                                    user=setting['DB_USER'],
                                    password=setting['DB_PASS'],
                                    host=setting['DB_HOST'],
                                    port=setting['DB_PORT'])
    print('Connections to database')
    return connection

def select_all_tags(connect):
    sql_all_tags = f"SELECT tag_name FROM all_tags "
    cursor = connect.cursor()
    cursor.execute(sql_all_tags)
    return [elem for elem in cursor.fetchall()]

def update_all_tags(connect,tag_name, value, status, date_update):
    sql_all_tags = """UPDATE all_tags SET value=%s, date_update=%s, status=%s WHERE tag_name=%s"""
    cursor = connect.cursor()
    cursor.execute(sql_all_tags,(value, date_update, status, tag_name))
    connect.commit()

