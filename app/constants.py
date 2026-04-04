import os
# server IP address to RX and TX the data
mqtt_server_ip = os.getenv('CHIRPSTACK_URL', 'chirpstack.vandyam.com')

# server Port number
mqtt_port = 1883

# Provided Application ID and Device EUI
application_id = os.getenv('CHIRPSTACK_APPLICATION_ID')
device_eui = os.getenv('CHIRPSTACK_DEVICE_EUI')

# FW File name
FW_FILE_NAME = "viot.app.blob"
# FW_FILE_NAME = "test_1024.bin"

# EPO File name
EPO_FILE_NAME = "QGPS.DAT.BLOB"

# To foce a Firmware DFU
FORCE_DFU_FW = 0
FORCE_DFU_EPO = 0

# Supported maximum payload
CFG_MAX_PAYLOAD_CR0 = 47
CFG_MAX_PAYLOAD_CR5 = 218

# Current payload size
CFG_MAX_PAYLOAD = CFG_MAX_PAYLOAD_CR5

# Configure Wakup interval
SETTINGS_WAKEUP_HOUR = 0
SETTINGS_WAKEUP_MINUTE = 1

if (FORCE_DFU_FW == 1):
    # To force a DFU - Firmware update
    FORCE_DFU = 0x02
elif (FORCE_DFU_EPO == 1):
    # To force a DFU - EPO update
    FORCE_DFU = 0x01
else:
    # No forced DFU
    FORCE_DFU = 0x00

######## Commands from the node ########
# Requesting for DFU
CMD_DFU_REQ = 0xA8
# Sensor Data
CMD_SENSOR_DATA = 0xAB
# Sensor troubleshoot data
CMD_SENSOR_T_DATA = 0xAC

########  DFU States ########
# Waiting for a JOIN request
STATE_JOIN = 0
# Waiting for a CONNECT request
STATE_CONNECT = 1
# Send FW file details
STATE_FW_CFG = 2
# Send firmware data
STATE_FW_DAT = 3
# Send EPO file details
STATE_EPO_CFG = 4
# Send firmware data≠
STATE_EPO_DAT = 5
# Undefined state
STATE_UNKNOWN = 6

log_dir = "log"


SECRET_KEY = os.getenv("SECRET_KEY", "your_secret_key")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1000000
REFRESH_TOKEN_EXPIRE_DAYS = 1000000
