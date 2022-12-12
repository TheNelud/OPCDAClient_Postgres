from openopc2.utils import get_opc_da_client
from openopc2.config import OpenOpcConfig
from functions import *
import schedule

import time 
from log import *

logger = get_logger(__name__)



def main():
    try:
        
        tags = opc_client.list(paths=paths, recursive=False, include_type=False, flat=True)
        tags = [tag for tag in tags if "@" not in tag]
        data_full = opc_client.read(tags, sync=False)
        db_tags = select_all_tags(connect)

        for tag in data_full:
            for i in range(len(db_tags)):
                if tag[0] in db_tags[i]:
                    update_all_tags(connect,tag[0],tag[1],tag[2],tag[3]) 
                    break
        # print("Update process")
        logger.info('Updating data in the database')
    except:
        logger.warning("Disconnection from the server opc da")
        time.sleep(15)
        main()


if __name__ == '__main__':
    setting = get_settings()
    open_opc_config = OpenOpcConfig()
    paths = "*"
    open_opc_config.OPC_SERVER = setting['OPC_SERVER']
    open_opc_config.OPC_GATEWAY_HOST = setting['OPC_GATEWAY_HOST']
    open_opc_config.OPC_CLASS = setting['OPC_CLASS']    #"Matrikon.OPC.Automation;Graybox.OPC.DAWrapper;HSCOPC.Automation;RSI.OPCAutomation;OPC.Automation"
    open_opc_config.OPC_MODE = setting['OPC_MODE']
    opc_client = get_opc_da_client(open_opc_config)
    
    logger.warning("Connection server OPC DA")

    connect = create_connection()
   
    
    insert_ser_per_day()
    insert_ser_per_month()
    insert_mer_per_month()
    insert_mag_techno()

    schedule.every(int(setting['UPDATE'])).seconds.do(main)
    schedule.every().day.at('00:00:00').do(insert_ser_per_day)
    schedule.every().day.at("00:00:00").do(insert_ser_per_month)
    schedule.every().day.at("00:00:00").do(insert_mer_per_month)
    schedule.every().day.at("00:00:00").do(insert_mag_techno)

    while True:
        schedule.run_pending()
        time.sleep(1)
    
    