from openopc2.utils import get_opc_da_client
from openopc2.config import OpenOpcConfig
from functions import *

import time 


def main():
    tags = opc_client.list(paths=paths, recursive=False, include_type=False, flat=True)
    tags = [tag for tag in tags if "@" not in tag]
    data_full = opc_client.read(tags, sync=False)
    db_tags = select_all_tags(connect)

    for tag in data_full:
        for i in range(len(db_tags)):
            if tag[0] in db_tags[i]:
                update_all_tags(connect,tag[0],tag[1],tag[2],tag[3]) 
                break



if __name__ == '__main__':
    setting = get_settings()
    open_opc_config = OpenOpcConfig()
    paths = "*"
    open_opc_config.OPC_SERVER = setting['OPC_SERVER']
    open_opc_config.OPC_GATEWAY_HOST = setting['OPC_GATEWAY_HOST']
    open_opc_config.OPC_CLASS = setting['OPC_CLASS']    #"Matrikon.OPC.Automation;Graybox.OPC.DAWrapper;HSCOPC.Automation;RSI.OPCAutomation;OPC.Automation"
    open_opc_config.OPC_MODE = setting['OPC_MODE']
    opc_client = get_opc_da_client(open_opc_config)
    print("Connection to OPC DA")

    connect = create_connection()
    
    while True:
        main()
        time.sleep(int(setting['UPDATE']))
    