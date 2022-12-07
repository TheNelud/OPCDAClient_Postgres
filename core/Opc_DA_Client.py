from openopc2.utils import get_opc_da_client
from openopc2.config import OpenOpcConfig
from functions import get_settings
import time


def main():
    setting = get_settings()
    open_opc_config = OpenOpcConfig()
    paths = "*"
    open_opc_config.OPC_SERVER = setting['OPC_SERVER']
    open_opc_config.OPC_GATEWAY_HOST = setting['OPC_GATEWAY_HOST']
    open_opc_config.OPC_CLASS = setting['OPC_CLASS']    #"Matrikon.OPC.Automation;Graybox.OPC.DAWrapper;HSCOPC.Automation;RSI.OPCAutomation;OPC.Automation"
    open_opc_config.OPC_MODE = setting['OPC_MODE']

    n_reads = 1
    sync = False

    opc_client = get_opc_da_client(open_opc_config)
    tags = opc_client.list(paths=paths, recursive=False, include_type=False, flat=True)

    tags = [tag for tag in tags if "@" not in tag]
    # if limit:
    #     tags = tags[:limit]
    print("TAGS:")
    for n, tag in enumerate(tags):
        print(f"{n:3} {tag}")

    print("READ: LIST")
    for n in range(n_reads):
        start = time.time()
        read = opc_client.read(tags, sync=sync)
        print(f'{n:3} {time.time()-start:.3f}s {read}')


if __name__ == '__main__':
    main()