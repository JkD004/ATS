#!/usr/bin/python3
import asyncio
import base64
import json
import logging
import math
import os
import threading
from datetime import datetime, timezone, timedelta

# import context  # Ensures paho is in PYTHONPATH
import paho.mqtt.client as mqtt
import pytz

from app.utils import store_device_data
from app.database import sync_db

import redis


# Use your ChirpStack Redis connection details
redis_client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'),
                          port=int(os.getenv('REDIS_PORT', 6379)),
                          db=int(os.getenv('REDIS_DB', 0)))

# Get log directory from environment variable, default to current directory if not set
log_dir = os.getenv("LOG_DIR", "./logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "app.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Logs to console
        logging.FileHandler(log_file)  # Logs to file
    ]
)

# mqtt_server_ip = "127.0.0.1"
mqtt_server_ip = os.getenv('CHIRPSTACK_URL', 'chirpstack.vandyam.com')
# mqtt_server_ip = os.getenv('CHIRPSTACK_URL', '192.168.29.170')
mqtt_port = 1883

# To foce a Firmware DFU
FORCE_DFU_FW = 0
FORCE_DFU_EPO = 0

# Supported maximum payload
CFG_MAX_PAYLOAD_CR0 = 47
CFG_MAX_PAYLOAD_CR5 = 218

# Current payload size
CFG_MAX_PAYLOAD = CFG_MAX_PAYLOAD_CR5

# Configure Wakup interval
SETTINGS_WAKEUP_HOUR = 3
SETTINGS_WAKEUP_MINUTE = 0

if (FORCE_DFU_FW == 1):
    # To force a DFU - Firmware update
    FORCE_DFU = 0x02
elif (FORCE_DFU_EPO == 1):
    # To force a DFU - EPO update
    FORCE_DFU = 0x01
else:
    # No forced DFU
    FORCE_DFU = 0x00

####### Settings
# Retrive backlogs (1) or not (0)
APP_SETTINGS_BACKLOG = 1
# Retrive trooubleshoot data (1) or not (0)
APP_SETTINGS_TDATA = 0

######## Commands from the node ########
# Requesting for DFU
CMD_DFU_REQ = 0xA8
# Sensor Data
CMD_SENSOR_DATA = 0xAB
# Sensor troubleshoot data
CMD_SENSOR_T_DATA = 0xAC
# Ack for the downlink
CMD_ACK_FOR_DOWNLINK = 0xAD
#Sensor radar data
CMD_RADAR_DATA = 0xAE
#Sensor Daughter Board
CMD_DAUGHTER_DATA = 0xAA
# Pressure sensor data
#CMD_PRESSURE_DATA = 0xAF
# Industrial sensor data
CMD_INDUSTRIAL_DATA = 0xAF
# Distance sensor data
CMD_DISTANCE_DATA = 0xBB

########  DFU States ########
# Height level sensor data
CMD_HEIGHT_LEVEL_SENSOR_DATA = 0xBB

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
# Send firmware data
STATE_EPO_DAT = 5
# Undefined state
STATE_UNKNOWN = 6
# Waiting for a acknowledgement
STATE_ACK_WAIT = 7
#Retransmitting the last packet
STATE_RETRANSMIT= 8


loop = asyncio.get_event_loop()
class dfu:
    def init_dfu(self):
        self.state = STATE_JOIN
        self.app_id = ""
        self.dev_eui = ""
        self.crc = 0xFFFF
        self.file_size = 0
        # For DFU Transfer
        self.packet_no = 0
        self.max_payload = CFG_MAX_PAYLOAD
        self.byte_txed = 0
        self.flash_backlog = 0
        self.SETTINGS_WAKEUP_HOUR = SETTINGS_WAKEUP_HOUR
        self.SETTINGS_WAKEUP_MINUTE = SETTINGS_WAKEUP_MINUTE
    # Constructor
    def __init__(self):
        self.mqttc = None
        self.init_dfu()
        self.force_dfu_epo_request = 0
        self.log_filename = f"mylog_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        self.log_filename = os.path.join(log_dir, self.log_filename)
        logging.basicConfig(filename=self.log_filename, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Vandyam Python MQTT client instance created")

    # Helper function - Compute CRC
    def compute_crc16(self, ba_input=None):
        crc = 0xFFFF  # Initial CRC value

        # Check input is given
        if ba_input is None:
            ba_input = self.file_byte_array

        for byte in ba_input:
            crc ^= byte
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001  # XOR with the CRC16 polynomial
                else:
                    crc >>= 1
        return crc

    # Helper function - Read a complete file
    def helper_get_file(self, file_name):
        self.file_size = 0
        self.crc = 0xFFFF
        # Read the file content
        try:
            with open(file_name, "rb") as file:
                # Read the file content into a byte array
                self.file_byte_array = bytearray(file.read())
        except FileNotFoundError:
            print("File not found: {file_name}")
            return
        except Exception as e:
            print("An error occurred: {e}")

        # Get File Size
        self.file_size = os.path.getsize(file_name)
        print("[DFU] File Size: " + str(self.file_size))

        # Align the File size by 8
        aligned = self.file_size % 8
        if aligned != 0:
            aligned = 8 - aligned
            # Padd the remaining with zero
            ba_padding = bytearray([0xBA] * aligned)
            self.file_byte_array += ba_padding
            self.file_size += aligned

        # Compute CRC16
        self.crc = self.compute_crc16()
        print("[DFU] File CRC: " + str(hex(self.crc)))

    # Helper functions - Create a Chirpstack payload
    # msg is of type bytearray, eg:- msg = bytearray([0xA9, 0, 0])
    def encode_payload(self, msg):
        # base64 encode
        msg_base64 = base64.b64encode(msg)
        # Decode from binary
        msg_base64_d = msg_base64.decode()
        # Chripstack format
        payload_raw = {"dev_eui": self.dev_eui, "confirmed": True, "f_port": 2, "data": msg_base64_d}
        # Convert to string & in binary
        payload_json = json.dumps(payload_raw)
        payload_json = payload_json.encode()
        return payload_json

    # Helper functions - Send a command
    def send_a_command(self, msg):
        # Topic
        topic = "application/" + self.app_id + "/device/" + self.dev_eui + "/command/down"
        # QoS
        qos = 0
        # Payload
        payload = self.encode_payload(msg)
        logging.info("[DEBUG] Publish msg topic = " + topic)
        # Send to MQTT
        self.mqttc.publish(topic, payload, qos)
        # Preserve last sent message
        self.last_sent_message = msg

    # Function to be executed after the timer expires
    def on_timer_expiry(self):
        # Check we got an ACK
        if self.state != STATE_ACK_WAIT:
            return

        print("[DFU] Downlink Re-transmission timer expired! Retransmitting...")
        # Increase count by one
        self.retransmit_attempts += 1
        # Check howmany retransmission
        if self.retransmit_attempts <= 3:
            print(f"[DFU] Retransmitting packet (attempt {self.retransmit_attempts})")
            # Retransmit the packet
            self.transmit_a_response(self.last_sent_message)
        else:
            self.state = STATE_CONNECT

    # Function to start a non-blocking timer
    def start_timer(self, seconds):
        print(f"[DFU] Downlink re-transmission timer started for {seconds} seconds...")
        logging.info("[DFU] Downlink re-transmission timer started for {seconds} seconds...")
        # Start the timer in a new thread
        self.timer_thread = threading.Thread(target=lambda: threading.Timer(seconds, self.on_timer_expiry).start())
        self.timer_thread.start()

    def transmit_a_response(self, msg):
        self.state = STATE_ACK_WAIT
        # Resend the last sent packet
        self.send_a_command(msg)
        # start timer
        self.start_timer(30)

    def pack_current_date_time(self):

        # Get current UTC time
        utc_now = datetime.now(timezone.utc)

        # 4 bits month   0 - 3  (1 to 12),
        # 5 bits day -   4 - 8  (1 to 31),
        # 5 bits hour    9 - 13 (0 to 23),
        # 6 bits minute 14 - 19 (0 to 59),
        # 6 bits secs   20 - 25 (0 to 59)
        # 3 bits week   26 - 28 (1 to 7),

        # Extract components
        year = utc_now.year - 2020
        month = utc_now.month
        day = utc_now.day
        hour = utc_now.hour
        minute = utc_now.minute
        second = utc_now.second
        msecond = int(utc_now.microsecond / 1000)

        # Calculate the week number
        week = (utc_now.day - 1) // 7 + 1

        date_time = month
        date_time |= (day << 4)
        date_time |= (hour << 9)
        date_time |= (minute << 14)
        date_time |= (second << 20)
        date_time |= (week << 26)

        # Pack the data
        ba_dt = date_time.to_bytes(4, byteorder='little')
        ba_year = year.to_bytes(1, byteorder='little')
        ba_subsec = msecond.to_bytes(2, byteorder='little')

        return ba_dt + ba_year + ba_subsec

    def set_wakeup_time(self):
        device = sync_db["devices"].find_one({
            "device_id": self.dev_eui}, {"device_settings": 1})
        if device:
            try:
                device_settings = device.get("device_settings", None)
                if device_settings and device_settings.get("wake_up_time", None):
                    wake_up_time = device_settings.get("wake_up_time", None)
                    wake_up_time = wake_up_time.get("value", None)
                    if wake_up_time:
                        SETTINGS_WAKEUP_HOUR = wake_up_time // 60
                        SETTINGS_WAKEUP_MINUTE = wake_up_time % 60
                        return SETTINGS_WAKEUP_HOUR, SETTINGS_WAKEUP_MINUTE
                return self.SETTINGS_WAKEUP_HOUR, self.SETTINGS_WAKEUP_MINUTE
            except Exception as e:
                print(f'set_wakeup_time error: {e}')
            # sync_db["devices"].update_one(
            #     {"device_id": self.dev_eui, "device_settings.wake_up_time.is_set": False},
            #     {"$set": {"device_settings.wake_up_time.is_set": True}}
            # )
        return self.SETTINGS_WAKEUP_HOUR, self.SETTINGS_WAKEUP_MINUTE

    # Helper function, pack the sensor wakuptime
    def form_wakeup_time(self):
        # First 5 bites = Hour
        # Next 6 bits = Minutes
        #SETTINGS_WAKEUP_HOUR, SETTINGS_WAKEUP_MINUTE = self.set_wakeup_time()
        SETTINGS_WAKEUP_HOUR, SETTINGS_WAKEUP_MINUTE = 0, 1
        w_time = SETTINGS_WAKEUP_HOUR & 0x1F
        w_time |= ((SETTINGS_WAKEUP_MINUTE & 0x3F) << 5)

        return w_time.to_bytes(2, byteorder='little')

    # Helper function - Send an ACK
    def send_a_response(self, status, epo_time, fw_version, flash_backlog, lat_det_status, lon_det_status):
        logging.info("[DFU] Sending Response")
        print("[DFU] Sending Response")

        # Fill flag
        ba_flag = bytearray([0x07])

        # Fill system time
        ba_time = self.pack_current_date_time()

        # Fill DFU Request - status
        if status != 0:
            if self.force_dfu_epo_request == 0:
                self.force_dfu_epo_request = 1
            else:
                status = 0

        ba_st = status.to_bytes(1, byteorder='little')

        # Fill DFU Request - 4 Byte Available EPO time
        ba_epo = epo_time.to_bytes(4, byteorder='little')

        # Fill DFU Request - 4 Byte Available Firmware version
        ba_fw = fw_version.to_bytes(4, byteorder='little')

        # Set the wakeup time
        ba_wakeup = self.form_wakeup_time()

        if flash_backlog != 0:
            ba_backlog = flash_backlog.to_bytes(2, byteorder='little')
            ba_flag[0] = ba_flag[0] | 0x8
        else:
            ba_backlog = bytearray([0x00, 0x00])

        if lat_det_status == 0 or lon_det_status == 0:
            ba_flag[0] = ba_flag[0] | 0x10

        # Create message payload
        msg_payload = ba_flag + ba_time + ba_st + ba_epo + ba_fw + ba_wakeup + ba_backlog

        # Create message
        msg = self.create_a_message(0xA9, msg_payload)

        # Init retransmission attemps
        self.retransmit_attempts = 0

        # Send the message
        self.transmit_a_response(msg)

    # Helper function - Create a message
    def create_a_message(self, cmd, msg_payload):
        crc = self.compute_crc16(msg_payload)
        b_crc = crc.to_bytes(2, byteorder='little')
        hdr = bytearray([cmd])
        hdr_sz = len(msg_payload).to_bytes(1, byteorder='little')
        msg = hdr + hdr_sz + b_crc + msg_payload
        #print(msg)
        return msg

    # Function to wait for acknowledgement
    def on_wait_ack(self):
        # We got an ACK, Change state to OK
        self.state = STATE_CONNECT
        print("[DFU] Got ACK, stopping re-transmit timer")
        logging.info("[DFU] Got ACK, stopping re-transmit timer")

    # Send firmware configuration data
    def send_fw_cfg(self):
        # Get the firmware data
        self.helper_get_file(FW_FILE_NAME)

        # Check file exist and do action
        if self.file_size != 0:
            print("[DFU] Sending Firmware configuration to node")
            ba_crc = self.crc.to_bytes(2, byteorder='little')
            ba_fsize = self.file_size.to_bytes(2, byteorder='little')
            msg_payload = ba_fsize + ba_crc

            # Create message
            msg = self.create_a_message(0xA6, msg_payload)

            # Send the message
            self.send_a_command(msg)

            # Change state to firmwaredata
            self.state = STATE_FW_DAT

    def send_fw_dat(self):

        # Increment packet no
        self.packet_no += 1

        # Packet number
        ba_pkt_no = self.packet_no.to_bytes(2, byteorder='little')

        # Find next transfer size
        if (self.byte_txed + self.max_payload) > self.file_size:
            next_size = (self.file_size - self.byte_txed)
            # Change state to firmwaredata, for temporary
            self.state = STATE_UNKNOWN
        else:
            next_size = self.max_payload

        # Send 45 bytes
        msg_payload = ba_pkt_no + self.file_byte_array[self.byte_txed : next_size + self.byte_txed]

        # Update total transffered
        self.byte_txed += next_size

        # Create message
        msg = self.create_a_message(0xA7, msg_payload)

        # Send the message
        self.send_a_command(msg)

        print("[DFU] FW Data Packet No: " + str(self.packet_no) + " Size: " + str(self.byte_txed) + "/" + str(self.file_size))
        #print(msg)

    # Send EPO configuration data
    def send_epo_cfg(self):
        #Get the EPO data
        self.helper_get_file(EPO_FILE_NAME)

        print("[EPO ] recieve requirest\n\r")
        #check file exist and do the action
        if self.file_size != 0:
            print("[DFU] Sending EPO configuration to node")
            ba_crc   = self.crc.to_bytes(2, byteorder='little')
            ba_fsize = self.file_size.to_bytes(2, byteorder='little')
            msg_payload = ba_fsize + ba_crc

           # Create message
            msg = self.create_a_message(0xA4, msg_payload)

            # Send the message
            self.send_a_command(msg)

            # Change state to EPO data
            self.state = STATE_EPO_DAT

    # Send the EPO data
    def send_epo_dat(self):

        # Increment packet no
        self.packet_no += 1

        # Packet number
        ba_pkt_no = self.packet_no.to_bytes(2, byteorder='little')

        # Find next transfer size
        if (self.byte_txed + self.max_payload) > self.file_size:
            next_size = (self.file_size - self.byte_txed)
            # Change state to firmwaredata, for temporary
            self.state = STATE_UNKNOWN
        else:
            next_size = self.max_payload

        # Send 45 bytes
        msg_payload = ba_pkt_no + self.file_byte_array[self.byte_txed : next_size + self.byte_txed]

        # Update total transffered
        self.byte_txed += next_size

        # Create message,
        msg = self.create_a_message(0xA5, msg_payload)

        # Send the message
        self.send_a_command(msg)

        print("[DFU] EPO Data Packet No: " + str(self.packet_no) + " Size: " + str(self.byte_txed) + "/" + str(self.file_size))
        # print(msg)

    # Command Porcessing - DFU_REQ
    def process_cmd_dfu(self, msg):
        if(self.state == STATE_CONNECT):
            print("[DFU] Processing DFU request");
            print("[DFU] Req Type:" + str(msg[4]) + " Reason:" + str(msg[5]))

            crc=self.compute_crc16(msg[4:])
            print(f"[DFU] CRC of DFU_REQ (server) : {crc:04X}")

            if msg[4] == 0x11:
                print("[DFU] Firmware DFU request")
                # Send an ACK
                self.send_a_response(0, 0, 0, 0, 1, 1)
                # change state
                self.state = STATE_FW_CFG
            elif msg[4] == 0x12:
                print("[DFU] EPO DFU request")
                self.send_a_response(0, 0, 0, 0, 1, 1)
                self.state = STATE_EPO_CFG
            else:
                print("[DFU] Unknown DFU Request type")
        else:
            print("[DFU] DFU Request not allowed in this stage, ignoring")

    # Process the sensor data
    def process_cmd_sensor_data(self, msg, lora_rssi, lora_snr, payload):
        if (self.state == STATE_CONNECT):
            print("[DFU] Processing Sensor Data");
            logging.info("Sensor Data")
            # Calculate the CRC, skip header four bytes
            print(f"[DFU] Processing for device {self.dev_eui}");
            logging.info(f"[DFU] Processing for device self.dev_eui = " + self.dev_eui)

            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Computed CRC value: {comp_crc:04X}")
            logging.info("[DFU] Computed CRC value: " + str(comp_crc))

            #print CRC calculated value
            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")
            logging.info("Sensor Data: CRC value: " + str(crc_value))

            # Check for data corruption
            if(comp_crc != crc_value):
                print("[DFU] Error, Tx Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Tx Packet corrupted, CRC mismatch")
                return

            # Print Last System UP time
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little')
            print("[DFU] Sensor Data: System uptime:", sys_uptime)
            logging.info("Sensor Data: System uptime: " + str(sys_uptime))

            # Print RTC time
            rtc_secs = int.from_bytes(msg[8:12], byteorder='little')
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little')
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)

            # Convert RTC UTC time to IST
            print("[DFU] Sensor Data: RTC time, Secs:" + str(rtc_secs) + " Sub-secs" + str(rtc_subsecs))
            logging.info("Sensor Data: RTC time, Secs:" + str(rtc_secs) + " Sub-secs" + str(rtc_subsecs))

            # Print GPS Inf
            gps_uptime = int.from_bytes(msg[14:18], byteorder='little')
            gps_latti = int.from_bytes(msg[18:22], byteorder='little')
            gps_longi = int.from_bytes(msg[22:26], byteorder='little')
            print("[DFU] Sensor Data: GPS: Uptime:" + str(gps_uptime) + " Lattitude:" + str(
                gps_latti) + " Longitude:" + str(gps_longi))
            logging.info(
                "Sensor Data: GPS: Uptime:" + str(gps_uptime) + " Lattitude:" + str(gps_latti) + " Longitude:" + str(
                    gps_longi))
            # Print Skin and Ambient temperature
            temp_skin     = int.from_bytes(msg[26:28], byteorder='little')
            temp_ambient  = int.from_bytes(msg[28:30], byteorder='little')
            print("[DFU] Sensor Data: Temperature, Skin:" + str(temp_skin) + " Ambient:" + str(temp_ambient))
            logging.info("Sensor Data: Temperature, Skin:" + str(temp_skin) + " Ambient:" + str(temp_ambient))

            # Print Battery
            batt_capa  = int.from_bytes(msg[30:31], byteorder='little')
            batt_volt  = int.from_bytes(msg[31:33], byteorder='little')
            print("[DFU] Sensor Data: Battery, Capacity:" + str(batt_capa) + " Voltage:" + str(batt_volt))
            logging.info("Sensor Data: Battery, Capacity:" + str(batt_capa) + " Voltage:" + str(batt_volt))

            # Print Firmware version
            fwv_app1  = int.from_bytes(msg[33:35], byteorder='little')
            fwv_app2  = int.from_bytes(msg[35:37], byteorder='little')
            fwv_bl    = int.from_bytes(msg[37:39], byteorder='little')
            fwv_sdk   = int.from_bytes(msg[39:41], byteorder='little')
            print("[DFU] Sensor Data: Firmware Version, App1:" + str(fwv_app1) + " App2:" + str(fwv_app2) + " BL:" + str(fwv_bl) + " SDK:" + str(fwv_sdk))
            logging.info("Sensor Data: Firmware Version, App1:" + str(fwv_app1) + " App2:" + str(fwv_app2) + " BL:" + str(fwv_bl) + " SDK:" + str(fwv_sdk))

            # EPO Date & Time in seconds
            epo_time  = int.from_bytes(msg[41:45], byteorder='little')
            print("[DFU] Sensor Data: EPO Date stamp:" + str(epo_time))
            logging.info("Sensor Data: EPO Date stamp:" + str(epo_time))

            # Print howmnay Backlog flash records present in the device
            self.flash_backlog = int.from_bytes(msg[45:47], byteorder='little')
            print("[DFU] Sensor Data: Number of old flash records:" + str(self.flash_backlog))
            logging.info("[DFU] Sensor Data: Number of old flash records:" + str(self.flash_backlog))

            gps_snr = int.from_bytes(msg[47:48], byteorder='little')
            print("[DFU] Sensor Data: GPS-SNR: " + str(gps_snr))
            logging.info("Sensor Data: GPS-SNR: " + str(gps_snr))

            # Print Lora rx RSSI & SNR, this inidcates how good device is connected with gateway
            print("[DFU] Sensor Data: LoRa-SNR: " + str(lora_snr) + " RSSI: " + str(lora_rssi))
            logging.info("Sensor Data: LoRa-SNR: " + str(lora_snr) + " RSSI: " + str(lora_rssi))

            type_device = "SENSOR_GPS_DEVICE_V1"

            # Prepare data to store in MongoDB
            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": type_device,
                "device_data": {
                    "temp": temp_skin / 100,
                    "lat": gps_latti,
                    "long": gps_longi,
                    "battery": batt_capa,
                    "voltage": batt_volt,
                    "systemtime": sys_uptime / 1000,
                    "gpstime": gps_uptime / 1000,
                    "epotime": epo_time,
                    "firmware_version": fwv_app1,
                    "rssi": lora_rssi,
                    "snr": gps_snr,
                    "data_retrieve_time": rtc_utc.isoformat(),
                    "buffer": str(int.from_bytes(msg, byteorder='little'))
                },
                "payload": json.dumps(payload)
            }

            # Check global flag & request backlog only if required
            if (APP_SETTINGS_BACKLOG == 0):
                self.flash_backlog = 0

            # Check global flag & request troubleshoot data or not
            if (APP_SETTINGS_TDATA == 0):
                gps_latti = 1
                gps_longi = 1

            # Store data to MongoDB
            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))

            # Send a response, can trigger a DFU based on FORCE_DFU value
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, gps_latti, gps_longi)
        else:
            print("[DFU] Sensor data not allowed in this stage, ignoring")

    # Process the sensor troubleshoot data
    def process_cmd_sensor_t_data(self, msg):
        print("[DFU] Processing Sensor Troubleshoot Data");

        # Calculate the CRC, skip header four bytes
        comp_crc = self.compute_crc16(msg[4:])
        print(f"[DFU] Computed CRC value: {comp_crc:04X}")

        # print CRC calculated value
        crc_value = int.from_bytes(msg[2:4], byteorder='little')
        print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")

        # Check for data corruption
        if (comp_crc != crc_value):
            print("[DFU] Error, Tx T Packet corrupted, CRC mismatch")
            return

        # Total number of satellites
        total_sats = int.from_bytes(msg[4:5], byteorder='little')
        print("[DFU] Total Satellites: " + str(total_sats))

        # Each sats
        mean = int.from_bytes(msg[37:41], byteorder='little')
        mean = mean / 100
        variance = int.from_bytes(msg[41:45], byteorder='little')
        variance = variance / 100
        std_deviation = math.sqrt(variance)

        print("[DFU] MEAN: " + str(mean) + " VARIANCE: " + str(variance) + " STD_DEVIATION: " + str(std_deviation))

        # Print Lora RSSI & SNR
        lora_RSSI = int.from_bytes(msg[45:47], byteorder='little', signed=True)
        lora_SNR = int.from_bytes(msg[47:48], byteorder='little')
        print("[DFU] LoRa RSSI: " + str(lora_RSSI) + " LoRa SNR: " + str(lora_SNR))

        self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, 1, 1)

    def process_cmd_pressure_data(self, msg, payload):
        if self.state == STATE_CONNECT:
            print("[DFU] Processing Pressure Sensor Data")
            logging.info("[DFU] Processing Pressure Sensor Data")
            # Validate packet length
            if len(msg) < 48:
                print("[DFU] Error, Pressure Packet too short")
                logging.info("[DFU] Error, Pressure Packet too short")
                return

            # Calculate the CRC, skip header (4 bytes: id, size, crc)
            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Computed CRC value: {comp_crc:04X}")
            logging.info(f"[DFU] Computed CRC value: {comp_crc:04X}")

            # Extract CRC from message header (bytes 2-3)
            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")
            logging.info(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")

            # Check for data corruption
            if comp_crc != crc_value:
                print("[DFU] Error, Sensor Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Sensor Packet corrupted, CRC mismatch")
                return

            # Extract msg_sensor_time_t (system_up_time, seconds, sub_seconds)
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little')
            rtc_secs = int.from_bytes(msg[8:12], byteorder='little')
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little')
            print(f"[DFU] Sensor Data: System uptime: {sys_uptime}")
            print(f"[DFU] Sensor Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            logging.info(f"[DFU] Sensor Data: System uptime: {sys_uptime}")
            logging.info(f"[DFU] Sensor Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)

            # Extract msg_sensor_temp_t (skin, ambient)
            skin_temp = int.from_bytes(msg[14:16], byteorder='little', signed=True)
            ambient_temp = int.from_bytes(msg[16:18], byteorder='little', signed=True)
            print(f"[DFU] Sensor Data: Skin Temperature: {skin_temp} C")
            print(f"[DFU] Sensor Data: Ambient Temperature: {ambient_temp} C")
            logging.info(f"[DFU] Sensor Data: Skin Temperature: {skin_temp} C")
            logging.info(f"[DFU] Sensor Data: Pressure Data: {ambient_temp} Pa")

            # Extract msg_sensor_battery_t (capacity, voltage)
            batt_capa = int.from_bytes(msg[18:19], byteorder='little')
            batt_volt = int.from_bytes(msg[19:21], byteorder='little')
            print(f"[DFU] Sensor Data: Battery, Capacity: {batt_capa}% Voltage: {batt_volt}")
            logging.info(f"[DFU] Sensor Data: Battery, Capacity: {batt_capa}% Voltage: {batt_volt}")

            # Extract msg_sensor_fw_version_t (app1, app2, bl, sdk)
            fwv_app1 = int.from_bytes(msg[21:23], byteorder='little')
            fwv_app2 = int.from_bytes(msg[23:25], byteorder='little')
            fwv_bl = int.from_bytes(msg[25:27], byteorder='little')
            fwv_sdk = int.from_bytes(msg[27:29], byteorder='little')
            print(f"[DFU] Sensor Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")
            logging.info(f"[DFU] Sensor Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")

            # Extract msg_sensor_eop_version_t (epo)
            epo_time = int.from_bytes(msg[29:33], byteorder='little')
            print(f"[DFU] Sensor Data: EPO Date stamp: {epo_time}")
            logging.info(f"[DFU] Sensor Data: EPO Date stamp: {epo_time}")

            # Extract msg_sensor_flash_status_t (count)
            self.flash_backlog = int.from_bytes(msg[46:48], byteorder='little')
            print(f"[DFU] Sensor Data: Number of old flash records: {self.flash_backlog}")
            logging.info(f"[DFU] Sensor Data: Number of old flash records: {self.flash_backlog}")

            # Prepare data to store in MongoDB
            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": "PRESSURE_SENSOR_DEVICE_V1",
                "device_data": {
                    "temp": skin_temp / 100,
                    "pressure": ambient_temp / 100,
                    "battery": batt_capa,
                    "batt_voltage" : batt_volt,
                    "systemtime": sys_uptime / 1000,
                    "firmware_version": fwv_app1,
                    "data_retrieve_time": rtc_utc.isoformat(),
                    "buffer": str(int.from_bytes(msg, byteorder='little'))
                },
                "payload": json.dumps(payload)
            }

            # Check global flag & request backlog only if required
            if (APP_SETTINGS_BACKLOG == 0):
                self.flash_backlog = 0

            # Store data to MongoDB
            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))

            # Send a response, can trigger a DFU based on FORCE_DFU value
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, 1, 1)

        else:
            print("[DFU] Sensor data not allowed in this stage, ignoring")

    def process_cmd_radar_data(self, msg, payload):
        if self.state == STATE_CONNECT:
            print("[DFU] Processing Radar Data")
            logging.info("Radar Data")

            # Validate packet length
            if len(msg) < 48:
                print("[DFU] Error, Radar Packet too short")
                logging.info("[DFU] Error, Radar Packet too short")
                return

            # CRC validation
            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Computed CRC value: {comp_crc:04X}")
            logging.info(f"[DFU] Computed CRC value: {comp_crc}")

            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Radar Data: CRC value: {crc_value:04X}")
            logging.info(f"[DFU] Radar Data: CRC value: {crc_value}")

            if comp_crc != crc_value:
                print("[DFU] Error, Radar Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Radar Packet corrupted, CRC mismatch")
                return

            # Parse timestamp (10 bytes: 4 bytes system uptime, 4 bytes seconds, 2 bytes sub-seconds)
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: System uptime: {sys_uptime}")
            logging.info(f"Radar Data: System uptime: {sys_uptime}")

            rtc_secs = int.from_bytes(msg[8:12], byteorder='little', signed=False)
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            logging.info(f"Radar Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)

            # Parse battery (3 bytes: 1 byte capacity, 2 bytes voltage)
            batt_capa = int.from_bytes(msg[14:15], byteorder='little', signed=False)
            batt_volt = int.from_bytes(msg[15:17], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Battery, Capacity: {batt_capa} Voltage: {batt_volt}")
            logging.info(f"[DFU] Radar Data: Battery, Capacity: {batt_capa} Voltage: {batt_volt}")

            # Parse firmware version (8 bytes: 2 bytes each for App1, App2, BL, SDK)
            fwv_app1 = int.from_bytes(msg[17:19], byteorder='little', signed=False)
            fwv_app2 = int.from_bytes(msg[19:21], byteorder='little', signed=False)
            fwv_bl = int.from_bytes(msg[21:23], byteorder='little', signed=False)
            fwv_sdk = int.from_bytes(msg[23:25], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")
            logging.info(f"Radar Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")

            # Parse EPO version (2 bytes)
            epo_version = int.from_bytes(msg[25:27], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: EPO Version: {epo_version}")
            logging.info(f"Radar Data: EPO Version: {epo_version}")

            # Parse target data (18 bytes, 3 targets, 6 bytes each)
            target_data = msg[27:45]
            print("[DFU] Radar Data: Target Data (18 bytes):")


            # Parse flash status (2 bytes)
            flash_records = int.from_bytes(msg[45:47], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Number of old flash records: {flash_records}")
            logging.info(f"Radar Data: Number of old flash records: {flash_records}")

            # Dummy byte (1 byte, ignored but logged for completeness)
            dummy_byte = int.from_bytes(msg[47:48], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Dummy Byte: {dummy_byte}")
            logging.info(f"Radar Data: Dummy Byte: {dummy_byte}")

            device_data = {
                "battery": batt_capa,
                "battery_voltage": batt_volt,
                "system_uptime": sys_uptime / 1000,
                "rtc_time": rtc_secs,
                "subsecond": rtc_subsecs,
                "epo_version": epo_version,
                "firmware_version": {
                    "app1": fwv_app1,
                    "app2": fwv_app2,
                    "bootloader": fwv_bl,
                    "sdk": fwv_sdk
                },
                "flash_record_count": flash_records,
                "dummy_byte": dummy_byte,
                "targets": [],
                "data_retrieve_time": rtc_utc.isoformat(),
                "buffer": msg.hex()
            }

            for i in range(0, len(target_data), 6):
                if i < len(target_data):
                    x_coord = int.from_bytes(target_data[i:i+2], byteorder='little', signed=True)
                    y_coord = int.from_bytes(target_data[i+2:i+4], byteorder='little', signed=True)
                    speed = int.from_bytes(target_data[i+4:i+6], byteorder='little', signed=True)
                    targe_distance = math.sqrt(x_coord**2 + y_coord**2)
                    device_data["targets"].append({
                        "x_coord": x_coord,
                        "y_coord": y_coord,
                        "speed": speed,
                        "distance": round(targe_distance, 2)
                    })

                    print(f"[DFU] Target {i//6 + 1}: X={x_coord}, Y={y_coord}, Speed={speed}, Distance={round(targe_distance,2)}")
                    logging.info(f"Target {i//6 + 1}: X={x_coord}, Y={y_coord}, Speed={speed}, Distance={round(targe_distance,2)}")





            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": "SENSOR_RADAR_DEVICE_V1",
                "device_data": device_data,
                "payload": json.dumps(payload)
            }

            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))

            # Send response
            self.flash_backlog = flash_records  # Update flash_backlog for response
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, 1, 1)
        else:
            print("[DFU] Radar data not allowed in this stage, ignoring")

    def process_cmd_sensor_data_daughter(self, msg, lora_rssi, lora_snr, payload):
        if (self.state == STATE_CONNECT):
            print("[DFU] Processing Sensor Data");
            logging.info("Sensor Data")
            # Calculate the CRC, skip header four bytes
            print(f"[DFU] Processing for device {self.dev_eui}");
            logging.info(f"[DFU] Processing for device self.dev_eui = " + self.dev_eui)
            
            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Computed CRC value: {comp_crc:04X}")
            logging.info("[DFU] Computed CRC value: " + str(comp_crc))

            #print CRC calculated value
            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")
            logging.info("Sensor Data: CRC value: " + str(crc_value))

            # Check for data corruption
            if(comp_crc != crc_value):
                print("[DFU] Error, Tx Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Tx Packet corrupted, CRC mismatch")
                return

            # Print Last System UP time
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little')
            print("[DFU] Sensor Data: System uptime:", sys_uptime)
            logging.info("Sensor Data: System uptime: " + str(sys_uptime))

            # Print RTC time
            rtc_secs = int.from_bytes(msg[8:12], byteorder='little')
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little')
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)

            # Convert RTC UTC time to IST
            print("[DFU] Sensor Data: RTC time, Secs:" + str(rtc_secs) + " Sub-secs" + str(rtc_subsecs))
            logging.info("Sensor Data: RTC time, Secs:" + str(rtc_secs) + " Sub-secs" + str(rtc_subsecs))

            # Print GPS Inf
            gps_uptime = int.from_bytes(msg[14:18], byteorder='little')
            gps_latti = int.from_bytes(msg[18:22], byteorder='little')
            gps_longi = int.from_bytes(msg[22:26], byteorder='little')
            bin_level = int.from_bytes(msg[26:28], byteorder='little')
            print("[DFU] Sensor Data: Bin Level:" + str(bin_level))
            print("[DFU] Sensor Data: GPS: Uptime:" + str(gps_uptime) + " Lattitude:" + str(
                gps_latti) + " Longitude:" + str(gps_longi))
            logging.info(
                "Sensor Data: GPS: Uptime:" + str(gps_uptime) + " Lattitude:" + str(gps_latti) + " Longitude:" + str(
                    gps_longi))
            # Print Skin and Ambient temperature
            temp_skin     = int.from_bytes(msg[26:28], byteorder='little')
            temp_ambient  = int.from_bytes(msg[28:30], byteorder='little')
            print("[DFU] Sensor Data: Temperature, Skin:" + str(temp_skin) + " Ambient:" + str(temp_ambient))
            logging.info("Sensor Data: Temperature, Skin:" + str(temp_skin) + " Ambient:" + str(temp_ambient))

            # Print Battery
            batt_capa  = int.from_bytes(msg[30:31], byteorder='little')
            batt_volt  = int.from_bytes(msg[31:33], byteorder='little')
            print("[DFU] Sensor Data: Battery, Capacity:" + str(batt_capa) + " Voltage:" + str(batt_volt))
            logging.info("Sensor Data: Battery, Capacity:" + str(batt_capa) + " Voltage:" + str(batt_volt))

            # Print Firmware version
            fwv_app1  = int.from_bytes(msg[33:35], byteorder='little')
            fwv_app2  = int.from_bytes(msg[35:37], byteorder='little')
            fwv_bl    = int.from_bytes(msg[37:39], byteorder='little')
            fwv_sdk   = int.from_bytes(msg[39:41], byteorder='little')
            print("[DFU] Sensor Data: Firmware Version, App1:" + str(fwv_app1) + " App2:" + str(fwv_app2) + " BL:" + str(fwv_bl) + " SDK:" + str(fwv_sdk))
            logging.info("Sensor Data: Firmware Version, App1:" + str(fwv_app1) + " App2:" + str(fwv_app2) + " BL:" + str(fwv_bl) + " SDK:" + str(fwv_sdk))

            # EPO Date & Time in seconds
            epo_time  = int.from_bytes(msg[41:45], byteorder='little')
            print("[DFU] Sensor Data: EPO Date stamp:" + str(epo_time))
            logging.info("Sensor Data: EPO Date stamp:" + str(epo_time))

            # Print howmnay Backlog flash records present in the device
            self.flash_backlog = int.from_bytes(msg[45:47], byteorder='little')
            print("[DFU] Sensor Data: Number of old flash records:" + str(self.flash_backlog))
            logging.info("[DFU] Sensor Data: Number of old flash records:" + str(self.flash_backlog))

            gps_snr = int.from_bytes(msg[47:48], byteorder='little')
            print("[DFU] Sensor Data: GPS-SNR: " + str(gps_snr))
            logging.info("Sensor Data: GPS-SNR: " + str(gps_snr))

            # Print Lora rx RSSI & SNR, this inidcates how good device is connected with gateway
            print("[DFU] Sensor Data: LoRa-SNR: " + str(lora_snr) + " RSSI: " + str(lora_rssi))
            logging.info("Sensor Data: LoRa-SNR: " + str(lora_snr) + " RSSI: " + str(lora_rssi))

            # Prepare data to store in MongoDB
            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": "MOTION_DETECTOR_DEVICE_V1",
                "device_data": {
                    "temp": temp_skin / 100,
                    "motion": temp_ambient,
                    "lat": gps_latti,
                    "long": gps_longi,
                    "battery": batt_capa,
                    "batt_voltage" : batt_volt,
                    "systemtime": sys_uptime / 1000,
                    "gpstime": gps_uptime / 1000,
                    "epotime": epo_time,
                    "firmware_version": fwv_app1,
                    "rssi": lora_rssi,
                    "snr": gps_snr,
                    "data_retrieve_time": rtc_utc.isoformat(),
                    "buffer": str(int.from_bytes(msg, byteorder='little'))
                },
                "payload": json.dumps(payload)
            }

            # Check global flag & request backlog only if required
            if (APP_SETTINGS_BACKLOG == 0):
                self.flash_backlog = 0

            # Check global flag & request troubleshoot data or not
            if (APP_SETTINGS_TDATA == 0):
                gps_latti = 1
                gps_longi = 1

            # Store data to MongoDB
            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))

            # Send a response, can trigger a DFU based on FORCE_DFU value
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, gps_latti, gps_longi)
        else:
            print("[DFU] Sensor data not allowed in this stage, ignoring")

    def process_cmd_distance_data(self, msg, payload):
        if self.state == STATE_CONNECT:
            print("[DFU] Processing Distance Sensor Data")
            logging.info("[DFU] Processing Distance Sensor Data")
            # Validate packet length
            if len(msg) < 48:
                print("[DFU] Error, Distance Packet too short")
                logging.info("[DFU] Error, Distance Packet too short")
                return

            # Calculate the CRC, skip header (4 bytes: id, size, crc)
            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Sensor Data: CRC value: {comp_crc:04X}")
            logging.info("[DFU] Computed CRC value: " + str(comp_crc))

            # Extract CRC from message header (bytes 2-3)
            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")
            logging.info("Sensor Data: CRC value: " + str(crc_value))

            # Check for data corruption
            if comp_crc != crc_value:
                print("[DFU] Error, Sensor Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Sensor Packet corrupted, CRC mismatch")
                return

            # Extract msg_sensor_time_t (system_up_time, seconds, sub_seconds)
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little')
            rtc_secs = int.from_bytes(msg[8:12], byteorder='little')
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little')
            print(f"[DFU] Sensor Data: System uptime: {sys_uptime}")
            print(f"[DFU] Sensor Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            logging.info(f"[DFU] Sensor Data: System uptime: {sys_uptime}")
            logging.info(f"[DFU] Sensor Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)

            # Extract msg_sensor_temp_t (skin, ambient)
            ch1 = int.from_bytes(msg[14:16], byteorder='little', signed=True)
            ch2 = int.from_bytes(msg[16:18], byteorder='little', signed=True)
            print(f"[DFU] Sensor Distance Data: Distance: {ch1} mm")

            # Extract msg_sensor_battery_t (capacity, voltage)
            batt_capa = int.from_bytes(msg[18:19], byteorder='little')
            batt_volt = int.from_bytes(msg[19:21], byteorder='little')
            print(f"[DFU] Sensor Data: Battery, Capacity: {batt_capa}% Voltage: {batt_volt}")
            logging.info(f"[DFU] Sensor Data: Battery, Capacity: {batt_capa}% Voltage: {batt_volt}")

            # Extract msg_sensor_fw_version_t (app1, app2, bl, sdk)
            fwv_app1 = int.from_bytes(msg[21:23], byteorder='little')
            fwv_app2 = int.from_bytes(msg[23:25], byteorder='little')
            fwv_bl = int.from_bytes(msg[25:27], byteorder='little')
            fwv_sdk = int.from_bytes(msg[27:29], byteorder='little')
            print(f"[DFU] Sensor Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")
            logging.info(f"[DFU] Sensor Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")

            # Extract msg_sensor_eop_version_t (epo)
            epo_time = int.from_bytes(msg[29:33], byteorder='little')
            print(f"[DFU] Sensor Data: EPO Date stamp: {epo_time}")
            logging.info(f"[DFU] Sensor Data: EPO Date stamp: {epo_time}")

            # Extract msg_sensor_flash_status_t (count)
            self.flash_backlog = int.from_bytes(msg[46:48], byteorder='little')
            print(f"[DFU] Sensor Data: Number of old flash records: {self.flash_backlog}")
            logging.info(f"[DFU] Sensor Data: Number of old flash records: {self.flash_backlog}")
            # Prepare data to store in MongoDB
            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": "DISTANCE_SENSOR_DEVICE",
                "device_data": {
                    "distance_percentage":(1504 - ch1) * 100/1504 ,
                    "battery": batt_capa,
                    "systemtime": sys_uptime / 1000,
                    "firmware_version": fwv_app1,
                    "data_retrieve_time": rtc_utc.isoformat(),
                    "buffer": str(int.from_bytes(msg, byteorder='little'))
                },
                "payload": json.dumps(payload)
            }

            # Check global flag & request backlog only if required
            if (APP_SETTINGS_BACKLOG == 0):
                self.flash_backlog = 0

            # Store data to MongoDB
            logging.info(f"[Line 1159] Pushing sensor data to Redis queue for device {self.dev_eui}")
            print(f"[Line 1159] Pushing sensor data to Redis queue for device {self.dev_eui}")
            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))
            logging.info(f"[Line 1159] Successfully pushed to queue. Queue length: {redis_client.llen('device_data_queue')}")
            print(f"[Line 1159] Successfully pushed to queue. Queue length: {redis_client.llen('device_data_queue')}")

            # Send a response, can trigger a DFU based on FORCE_DFU value
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, 1, 1)

        else:
            logging.info(f"[DFU] Distance sensor data not allowed in this stage. Current state: {self.state}, Expected: {STATE_CONNECT}")
            print(f"[DFU] Distance sensor data not allowed in this stage. Current state: {self.state}, Expected: {STATE_CONNECT}")
            print("[DFU] Sensor data not allowed in this stage, ignoring")


    def process_cmd_industrial_data(self, msg, payload):
        # Mirrors test_mqtt_client.py industrial parser
        if self.state == STATE_CONNECT:
            print("DFU: Processing Industrial Sensor Data")
            logging.info("DFU: Processing Industrial Sensor Data")

            deviceinfo = payload["deviceInfo"]
            self.appid = deviceinfo["applicationId"]
            self.deveui = deviceinfo["devEui"]

            if len(msg) < 48:
                print("DFU: Error, Industrial Packet too short")
                logging.info("DFU: Error, Industrial Packet too short")
                return

            # CRC validation (skip 4-byte header)
            compcrc = self.compute_crc16(msg[4:])
            print("DFU: Computed CRC value %04X" % compcrc)
            logging.info("DFU: Computed CRC value %s" % str(compcrc))

            crcvalue = int.from_bytes(msg[2:4], byteorder='little')
            print("DFU: Sensor Data CRC value %04X" % crcvalue)
            logging.info("Sensor Data CRC value %s" % str(crcvalue))

            if compcrc != crcvalue:
                print("DFU: Error, Tx Packet corrupted, CRC mismatch")
                logging.info("DFU: Error, Tx Packet corrupted, CRC mismatch")
                return

            # Parse core fields (names/offsets preserve test file behavior)
            sysuptime = int.from_bytes(msg[4:8], byteorder='little')
            print("DFU: Sensor Data System uptime:", sysuptime)
            logging.info("Sensor Data System uptime %s" % str(sysuptime))

            # RTC epoch handled like test client (RTC = 2000-01-01 UTC + seconds)
            rtcsecs = int.from_bytes(msg[8:12], byteorder='little')
            rtcsubsecs = int.from_bytes(msg[12:14], byteorder='little')
            epochtime = rtcsecs + rtcsubsecs/1000
            rtcutc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)
            rtcutc = rtcutc_epoch + timedelta(seconds=epochtime)
            print("DFU: Sensor Data RTC time, Secs", rtcsecs, "Sub-secs", rtcsubsecs)
            logging.info("Sensor Data RTC time, Secs %s Sub-secs %s" % (str(rtcsecs), str(rtcsubsecs)))

            # Temperatures
            ch1 = int.from_bytes(msg[14:16], byteorder='little')
            ch2 = int.from_bytes(msg[16:18], byteorder='little')
            print("DFU: Sensor Data, CH1", ch1, "CH2", ch2)
            logging.info("Sensor Data, CH1 %s CH2 %s" % (str(ch1), str(ch2)))

            # Firmware versions
            fwvapp1 = int.from_bytes(msg[21:23], byteorder='little')
            fwvapp2 = int.from_bytes(msg[23:25], byteorder='little')
            fwvbl   = int.from_bytes(msg[25:27], byteorder='little')
            fwvsdk  = int.from_bytes(msg[27:29], byteorder='little')
            print("DFU: Sensor Data Firmware Version, App1", fwvapp1, "App2", fwvapp2, "BL", fwvbl, "SDK", fwvsdk)
            logging.info("Sensor Data Firmware Version, App1 %s App2 %s BL %s SDK %s" % (str(fwvapp1), str(fwvapp2), str(fwvbl), str(fwvsdk)))

            # Flash backlog
            flashrecords = int.from_bytes(msg[46:48], byteorder='little')
            self.flashbacklog = flashrecords
            print("DFU: Sensor Data Number of old flash records", self.flashbacklog)
            logging.info("DFU: Sensor Data Number of old flash records %s" % str(self.flashbacklog))

            # Build document exactly like test file’s style
            devicedataforUser = {
                "device_id": self.deveui,
                "app_id": self.appid,
                "type_device": "INDUSTRIAL_SENSOR_DEVICE_V1",
                "device_data": {
                    "CH1_mA": ch1 / 100,
                    "CH2_mA": ch2/ 100,
                    "systemuptime": sysuptime/1000,
                    "rtctime": rtcsecs,
                    "subsecond": rtcsubsecs,
                    "firmwareversion": {
                        "app1": fwvapp1,
                        "app2": fwvapp2,
                        "bootloader": fwvbl,
                        "sdk": fwvsdk,
                        "flashrecordcount": flashrecords,
                        "targets": [],
                        "dataretrievetime": rtcutc.isoformat(),
                        "buffer": str(int.from_bytes(msg, byteorder='little'))
                    }
                },
                "payload": json.dumps(payload)
            }

            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))

            # Respond per test logic
            self.send_a_response(FORCE_DFU, 0, 0, self.flashbacklog, 1, 1)
        else:
            print("DFU: Industrial data not allowed in this stage, ignoring")


    def process_cmd_distance_data(self, msg, payload):
        if self.state == STATE_CONNECT:
            print("[DFU] Processing Distance Sensor Data")
            logging.info("[DFU] Processing Distance Sensor Data")
            # Validate packet length
            if len(msg) < 48:
                print("[DFU] Error, Distance Packet too short")
                logging.info("[DFU] Error, Distance Packet too short")
                return

            # Calculate the CRC, skip header (4 bytes: id, size, crc)
            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Computed CRC value: {comp_crc:04X}")
            logging.info(f"[DFU] Computed CRC value: {comp_crc:04X}")

            # Extract CRC from message header (bytes 2-3)
            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")
            logging.info(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")

            # Check for data corruption
            if comp_crc != crc_value:
                print("[DFU] Error, Sensor Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Sensor Packet corrupted, CRC mismatch")
                return

            # Extract msg_sensor_time_t (system_up_time, seconds, sub_seconds)
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little')
            rtc_secs = int.from_bytes(msg[8:12], byteorder='little')
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little')
            print(f"[DFU] Sensor Data: System uptime: {sys_uptime}")
            print(f"[DFU] Sensor Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            logging.info(f"[DFU] Sensor Data: System uptime: {sys_uptime}")
            logging.info(f"[DFU] Sensor Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)

            # Extract distance data
            distance = int.from_bytes(msg[14:16], byteorder='little', signed=True)
            ch2 = int.from_bytes(msg[16:18], byteorder='little', signed=True)
            print(f"[DFU] ********** Sensor Distance Data: Distance: {distance} mm")

            # Extract msg_sensor_battery_t (capacity, voltage)
            batt_capa = int.from_bytes(msg[18:19], byteorder='little')
            batt_volt = int.from_bytes(msg[19:21], byteorder='little')
            print(f"[DFU] Sensor Data: Battery, Capacity: {batt_capa}% Voltage: {batt_volt}")
            logging.info(f"[DFU] Sensor Data: Battery, Capacity: {batt_capa}% Voltage: {batt_volt}")

            # Extract msg_sensor_fw_version_t (app1, app2, bl, sdk)
            fwv_app1 = int.from_bytes(msg[21:23], byteorder='little')
            fwv_app2 = int.from_bytes(msg[23:25], byteorder='little')
            fwv_bl = int.from_bytes(msg[25:27], byteorder='little')
            fwv_sdk = int.from_bytes(msg[27:29], byteorder='little')
            print(f"[DFU] Sensor Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")
            logging.info(f"[DFU] Sensor Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")

            # Extract msg_sensor_eop_version_t (epo)
            epo_time = int.from_bytes(msg[29:33], byteorder='little')
            print(f"[DFU] Sensor Data: EPO Date stamp: {epo_time}")
            logging.info(f"[DFU] Sensor Data: EPO Date stamp: {epo_time}")

            # Extract msg_sensor_flash_status_t (count)
            self.flash_backlog = int.from_bytes(msg[46:48], byteorder='little')
            print(f"[DFU] Sensor Data: Number of old flash records: {self.flash_backlog}")
            logging.info(f"[DFU] Sensor Data: Number of old flash records: {self.flash_backlog}")
            print(f"hhhhhhhhhhh{self.dev_eui}")

            # Prepare data to store in MongoDB
            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": "DISTANCE_SENSOR_DEVICE",
                "device_data": {
                    "distance_percentage":(1504 - distance) * 100/1504 ,
                    "battery": batt_capa,
                    "systemtime": sys_uptime / 1000,
                    "firmware_version": fwv_app1,
                    "data_retrieve_time": rtc_utc.isoformat(),
                    "buffer": str(int.from_bytes(msg, byteorder='little'))
                },
                "payload": json.dumps(payload)
            }

            # Check global flag & request backlog only if required
            if (APP_SETTINGS_BACKLOG == 0):
                self.flash_backlog = 0
                
            # Store data to MongoDB
            logging.info(f"[Line 1356] Pushing distance sensor data to Redis queue for device {self.dev_eui}")
            print(f"[Line 1356] Pushing distance sensor data to Redis queue for device {self.dev_eui}")
            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))
            logging.info(f"[Line 1356] Successfully pushed to queue. Queue length: {redis_client.llen('device_data_queue')}")
            print(f"[Line 1356] Successfully pushed to queue. Queue length: {redis_client.llen('device_data_queue')}")

            # Send a response, can trigger a DFU based on FORCE_DFU value
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, 1, 1)

        else:
            print("[DFU] Sensor data not allowed in this stage, ignoring")

    # Process the sensor troubleshoot data
    def process_cmd_sensor_t_data(self, msg):
        print("[DFU] Processing Sensor Troubleshoot Data");

        # Calculate the CRC, skip header four bytes
        comp_crc = self.compute_crc16(msg[4:])
        print(f"[DFU] Computed CRC value: {comp_crc:04X}")

        # print CRC calculated value
        crc_value = int.from_bytes(msg[2:4], byteorder='little')
        print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")

        # Check for data corruption
        if (comp_crc != crc_value):
            print("[DFU] Error, Tx T Packet corrupted, CRC mismatch")
            return

        # Total number of satellites
        total_sats = int.from_bytes(msg[4:5], byteorder='little')
        print("[DFU] Total Satellites: " + str(total_sats))

        # Each sats
        mean = int.from_bytes(msg[37:41], byteorder='little')
        mean = mean / 100
        variance = int.from_bytes(msg[41:45], byteorder='little')
        variance = variance / 100
        std_deviation = math.sqrt(variance)

        print("[DFU] MEAN: " + str(mean) + " VARIANCE: " + str(variance) + " STD_DEVIATION: " + str(std_deviation))

        # Print Lora RSSI & SNR
        lora_RSSI = int.from_bytes(msg[45:47], byteorder='little', signed=True)
        lora_SNR = int.from_bytes(msg[47:48], byteorder='little')
        print("[DFU] LoRa RSSI: " + str(lora_RSSI) + " LoRa SNR: " + str(lora_SNR))

        self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, 1, 1)

    def process_cmd_radar_data(self, msg, payload):
        if self.state == STATE_CONNECT:
            print("[DFU] Processing Radar Data")
            logging.info("Radar Data")
            
            # Validate packet length
            if len(msg) < 48:
                print("[DFU] Error, Radar Packet too short")
                logging.info("[DFU] Error, Radar Packet too short")
                return
            
            # CRC validation
            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Computed CRC value: {comp_crc:04X}")
            logging.info(f"[DFU] Computed CRC value: {comp_crc}")
            
            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Radar Data: CRC value: {crc_value:04X}")
            logging.info(f"[DFU] Radar Data: CRC value: {crc_value}")
            
            if comp_crc != crc_value:
                print("[DFU] Error, Radar Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Radar Packet corrupted, CRC mismatch")
                return
            
            # Parse timestamp (10 bytes: 4 bytes system uptime, 4 bytes seconds, 2 bytes sub-seconds)
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: System uptime: {sys_uptime}")
            logging.info(f"Radar Data: System uptime: {sys_uptime}")
            
            rtc_secs = int.from_bytes(msg[8:12], byteorder='little', signed=False)
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            logging.info(f"Radar Data: RTC time, Secs: {rtc_secs} Sub-secs: {rtc_subsecs}")
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)
            
            # Parse battery (3 bytes: 1 byte capacity, 2 bytes voltage)
            batt_capa = int.from_bytes(msg[14:15], byteorder='little', signed=False)
            batt_volt = int.from_bytes(msg[15:17], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Battery, Capacity: {batt_capa} Voltage: {batt_volt}")
            logging.info(f"[DFU] Radar Data: Battery, Capacity: {batt_capa} Voltage: {batt_volt}")
            
            # Parse firmware version (8 bytes: 2 bytes each for App1, App2, BL, SDK)
            fwv_app1 = int.from_bytes(msg[17:19], byteorder='little', signed=False)
            fwv_app2 = int.from_bytes(msg[19:21], byteorder='little', signed=False)
            fwv_bl = int.from_bytes(msg[21:23], byteorder='little', signed=False)
            fwv_sdk = int.from_bytes(msg[23:25], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")
            logging.info(f"Radar Data: Firmware Version, App1: {fwv_app1} App2: {fwv_app2} BL: {fwv_bl} SDK: {fwv_sdk}")
            
            # Parse EPO version (2 bytes)
            epo_version = int.from_bytes(msg[25:27], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: EPO Version: {epo_version}")
            logging.info(f"Radar Data: EPO Version: {epo_version}")
            
            # Parse target data (18 bytes, 3 targets, 6 bytes each)
            target_data = msg[27:45]
            print("[DFU] Radar Data: Target Data (18 bytes):")


            # Parse flash status (2 bytes)
            flash_records = int.from_bytes(msg[45:47], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Number of old flash records: {flash_records}")
            logging.info(f"Radar Data: Number of old flash records: {flash_records}")
            
            # Dummy byte (1 byte, ignored but logged for completeness)
            dummy_byte = int.from_bytes(msg[47:48], byteorder='little', signed=False)
            print(f"[DFU] Radar Data: Dummy Byte: {dummy_byte}")
            logging.info(f"Radar Data: Dummy Byte: {dummy_byte}")

            device_data = {
                "battery": batt_capa,
                "battery_voltage": batt_volt,
                "system_uptime": sys_uptime / 1000,
                "rtc_time": rtc_secs,
                "subsecond": rtc_subsecs,
                "epo_version": epo_version,
                "firmware_version": {
                    "app1": fwv_app1,
                    "app2": fwv_app2,
                    "bootloader": fwv_bl,
                    "sdk": fwv_sdk
                },
                "flash_record_count": flash_records,
                "dummy_byte": dummy_byte,
                "targets": [],
                "data_retrieve_time": rtc_utc.isoformat(),
                "buffer": msg.hex()
            }
            
            for i in range(0, len(target_data), 6):
                if i < len(target_data):
                    x_coord = int.from_bytes(target_data[i:i+2], byteorder='little', signed=True)
                    y_coord = int.from_bytes(target_data[i+2:i+4], byteorder='little', signed=True)
                    speed = int.from_bytes(target_data[i+4:i+6], byteorder='little', signed=True)
                    targe_distance = math.sqrt(x_coord**2 + y_coord**2)
                    device_data["targets"].append({
                        "x_coord": x_coord,
                        "y_coord": y_coord,
                        "speed": speed,
                        "distance": round(targe_distance, 2)
                    })
                    
                    print(f"[DFU] Target {i//6 + 1}: X={x_coord}, Y={y_coord}, Speed={speed}, Distance={round(targe_distance,2)}")
                    logging.info(f"Target {i//6 + 1}: X={x_coord}, Y={y_coord}, Speed={speed}, Distance={round(targe_distance,2)}")
            




            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": "SENSOR_RADAR_DEVICE_V1",
                "device_data": device_data,
                "payload": json.dumps(payload)
            }

            logging.info(f"Pushing radar data to Redis queue for device {self.dev_eui}")
            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))
            logging.info(f"Successfully pushed to queue. Queue length: {redis_client.llen('device_data_queue')}")
            
            # Send response
            self.flash_backlog = flash_records  # Update flash_backlog for response
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, 1, 1)
        else:
            print("[DFU] Radar data not allowed in this stage, ignoring")

    def process_cmd_sensor_data_daughter(self, msg, lora_rssi, lora_snr, payload):
        if (self.state == STATE_CONNECT):
            print("[DFU] Processing Sensor Data");
            logging.info("Sensor Data")
            # Calculate the CRC, skip header four bytes
            print(f"[DFU] Processing for device {self.dev_eui}");
            logging.info(f"[DFU] Processing for device self.dev_eui = " + self.dev_eui)
            
            comp_crc = self.compute_crc16(msg[4:])
            print(f"[DFU] Computed CRC value: {comp_crc:04X}")
            logging.info("[DFU] Computed CRC value: " + str(comp_crc))

            #print CRC calculated value
            crc_value = int.from_bytes(msg[2:4], byteorder='little')
            print(f"[DFU] Sensor Data: CRC value: {crc_value:04X}")
            logging.info("Sensor Data: CRC value: " + str(crc_value))

            # Check for data corruption
            if(comp_crc != crc_value):
                print("[DFU] Error, Tx Packet corrupted, CRC mismatch")
                logging.info("[DFU] Error, Tx Packet corrupted, CRC mismatch")
                return

            # Print Last System UP time
            sys_uptime = int.from_bytes(msg[4:8], byteorder='little')
            print("[DFU] Sensor Data: System uptime:", sys_uptime)
            logging.info("Sensor Data: System uptime: " + str(sys_uptime))

            # Print RTC time
            rtc_secs = int.from_bytes(msg[8:12], byteorder='little')
            rtc_subsecs = int.from_bytes(msg[12:14], byteorder='little')
            epoch_time = rtc_secs + rtc_subsecs / 1000

            # Define the RTC UTC epoch (January 1, 2000)
            rtc_utc_epoch = datetime(2000, 1, 1, tzinfo=pytz.UTC)

            # Add the epoch_time to the RTC UTC epoch
            rtc_utc = rtc_utc_epoch + timedelta(seconds=epoch_time)

            # Convert RTC UTC time to IST
            print("[DFU] Sensor Data: RTC time, Secs:" + str(rtc_secs) + " Sub-secs" + str(rtc_subsecs))
            logging.info("Sensor Data: RTC time, Secs:" + str(rtc_secs) + " Sub-secs" + str(rtc_subsecs))

            # Print GPS Inf
            gps_uptime = int.from_bytes(msg[14:18], byteorder='little')
            gps_latti = int.from_bytes(msg[18:22], byteorder='little')
            gps_longi = int.from_bytes(msg[22:26], byteorder='little')
            print("[DFU] Sensor Data: GPS: Uptime:" + str(gps_uptime) + " Lattitude:" + str(
                gps_latti) + " Longitude:" + str(gps_longi))
            logging.info(
                "Sensor Data: GPS: Uptime:" + str(gps_uptime) + " Lattitude:" + str(gps_latti) + " Longitude:" + str(
                    gps_longi))
            # Print Skin and Ambient temperature
            temp_skin     = int.from_bytes(msg[26:28], byteorder='little')
            temp_ambient  = int.from_bytes(msg[28:30], byteorder='little')
            print("[DFU] Sensor Data: Temperature, Skin:" + str(temp_skin) + " Ambient:" + str(temp_ambient))
            logging.info("Sensor Data: Temperature, Skin:" + str(temp_skin) + " Ambient:" + str(temp_ambient))

            # Print Battery
            batt_capa  = int.from_bytes(msg[30:31], byteorder='little')
            batt_volt  = int.from_bytes(msg[31:33], byteorder='little')
            print("[DFU] Sensor Data: Battery, Capacity:" + str(batt_capa) + " Voltage:" + str(batt_volt))
            logging.info("Sensor Data: Battery, Capacity:" + str(batt_capa) + " Voltage:" + str(batt_volt))

            # Print Firmware version
            fwv_app1  = int.from_bytes(msg[33:35], byteorder='little')
            fwv_app2  = int.from_bytes(msg[35:37], byteorder='little')
            fwv_bl    = int.from_bytes(msg[37:39], byteorder='little')
            fwv_sdk   = int.from_bytes(msg[39:41], byteorder='little')
            print("[DFU] Sensor Data: Firmware Version, App1:" + str(fwv_app1) + " App2:" + str(fwv_app2) + " BL:" + str(fwv_bl) + " SDK:" + str(fwv_sdk))
            logging.info("Sensor Data: Firmware Version, App1:" + str(fwv_app1) + " App2:" + str(fwv_app2) + " BL:" + str(fwv_bl) + " SDK:" + str(fwv_sdk))

            # EPO Date & Time in seconds
            epo_time  = int.from_bytes(msg[41:45], byteorder='little')
            print("[DFU] Sensor Data: EPO Date stamp:" + str(epo_time))
            logging.info("Sensor Data: EPO Date stamp:" + str(epo_time))

            # Print howmnay Backlog flash records present in the device
            self.flash_backlog = int.from_bytes(msg[45:47], byteorder='little')
            print("[DFU] Sensor Data: Number of old flash records:" + str(self.flash_backlog))
            logging.info("[DFU] Sensor Data: Number of old flash records:" + str(self.flash_backlog))

            gps_snr = int.from_bytes(msg[47:48], byteorder='little')
            print("[DFU] Sensor Data: GPS-SNR: " + str(gps_snr))
            logging.info("Sensor Data: GPS-SNR: " + str(gps_snr))

            # Print Lora rx RSSI & SNR, this inidcates how good device is connected with gateway
            print("[DFU] Sensor Data: LoRa-SNR: " + str(lora_snr) + " RSSI: " + str(lora_rssi))
            logging.info("Sensor Data: LoRa-SNR: " + str(lora_snr) + " RSSI: " + str(lora_rssi))

            # Prepare data to store in MongoDB
            devicedataforUser = {
                "device_id": self.dev_eui,
                "app_id": self.app_id,
                "type_device": "MOTION_DETECTOR_DEVICE_V1",
                "device_data": {
                    "temp": temp_skin / 100,
                    "motion": temp_ambient,
                    "lat": gps_latti,
                    "long": gps_longi,
                    "battery": batt_capa,
                    "batt_voltage" : batt_volt,
                    "systemtime": sys_uptime / 1000,
                    "gpstime": gps_uptime / 1000,
                    "epotime": epo_time,
                    "firmware_version": fwv_app1,
                    "rssi": lora_rssi,
                    "snr": gps_snr,
                    "data_retrieve_time": rtc_utc.isoformat(),
                    "buffer": str(int.from_bytes(msg, byteorder='little'))
                },
                "payload": json.dumps(payload)
            }

            # Check global flag & request backlog only if required
            if (APP_SETTINGS_BACKLOG == 0):
                self.flash_backlog = 0

            # Check global flag & request troubleshoot data or not
            if (APP_SETTINGS_TDATA == 0):
                gps_latti = 1
                gps_longi = 1

            # Store data to MongoDB
            redis_client.rpush("device_data_queue", json.dumps(devicedataforUser))

            # Send a response, can trigger a DFU based on FORCE_DFU value
            self.send_a_response(FORCE_DFU, 0, 0, self.flash_backlog, gps_latti, gps_longi)
        else:
            print("[DFU] Sensor data not allowed in this stage, ignoring")

    # Process JOIN message, new DFU request
    def process_join_message(self, payload):

        # Init to default
        self.init_dfu()

        device_info = payload["deviceInfo"]
        print("[DFU] JOIN Message from " + device_info["deviceName"])
        logging.info(f"[DFU] JOIN Message from {device_info['deviceName']}")

        # Extract APP ID and Device EUI for further communication
        self.app_id = device_info["applicationId"]
        self.dev_eui = device_info["devEui"]
        # Change state to connect
        self.state = STATE_CONNECT
        logging.info(f"State changed to STATE_CONNECT ({STATE_CONNECT}) for device {self.dev_eui}")

    # A command from the node
    def process_uplink_message(self, payload):
        print('payload: ', payload)
        # Decode the message
        message = payload["data"]
        message_bytes = base64.b64decode(message)
        device_info = payload["deviceInfo"]
        
        # Set device info if not already set (in case JOIN wasn't received)
        if not self.dev_eui:
            self.dev_eui = device_info["devEui"]
            self.app_id = device_info["applicationId"]
            logging.info(f"Setting dev_eui={self.dev_eui}, app_id={self.app_id} from uplink")
            print(f"Setting dev_eui={self.dev_eui}, app_id={self.app_id} from uplink")
        
        print("[DFU] UPLINK Message from " + device_info["deviceName"] + " Message: " + str(hex(message_bytes[0])))

        # Get RSSI and SNR
        if 'rssi' in payload["rxInfo"][0]:
            rssi = payload["rxInfo"][0]["rssi"]
        else:
            rssi = None

        # Observed sometime no SNR info
        if 'snr' in payload["rxInfo"][0]:
            snr = payload["rxInfo"][0]["snr"]
        else:
            snr = None

        print("RSSI: " + str(rssi) + " SNR: " + str(snr))

        # Extract the command number
        cmd_from_node = message_bytes[0]
        
        logging.info(f"Command from node: 0x{cmd_from_node:02X} (decimal: {cmd_from_node})")
        print(f"Command from node: 0x{cmd_from_node:02X} (decimal: {cmd_from_node})")
        logging.info(f"CMD_DISTANCE_DATA = 0x{CMD_DISTANCE_DATA:02X}, Current state: {self.state}, STATE_CONNECT: {STATE_CONNECT}")
        print(f"CMD_DISTANCE_DATA = 0x{CMD_DISTANCE_DATA:02X}, Current state: {self.state}, STATE_CONNECT: {STATE_CONNECT}")

        # Process the command
        if CMD_DFU_REQ == cmd_from_node:
            self.process_cmd_dfu(message_bytes)
        elif CMD_SENSOR_DATA == cmd_from_node:
            self.process_cmd_sensor_data(message_bytes, rssi, snr, payload)
        elif CMD_SENSOR_T_DATA == cmd_from_node:
            self.process_cmd_sensor_t_data(message_bytes, payload)
        elif CMD_RADAR_DATA == cmd_from_node:
            self.process_cmd_radar_data(message_bytes, payload)
        elif CMD_DAUGHTER_DATA == cmd_from_node:
            self.process_cmd_sensor_data_daughter(message_bytes, rssi, snr, payload)
        #elif CMD_PRESSURE_DATA == cmd_from_node:
            #self.process_cmd_pressure_data(message_bytes, payload)
        elif CMD_DISTANCE_DATA == cmd_from_node:
            logging.info(f"===== Calling process_cmd_distance_data =====")
            print(f"===== Calling process_cmd_distance_data =====")
            # Auto-set state to CONNECT if not already (device may not send JOIN)
            if self.state != STATE_CONNECT:
                logging.info(f"Auto-setting state from {self.state} to STATE_CONNECT for distance sensor")
                print(f"Auto-setting state from {self.state} to STATE_CONNECT for distance sensor")
                self.state = STATE_CONNECT
            self.process_cmd_distance_data(message_bytes, payload)
        elif CMD_INDUSTRIAL_DATA == cmd_from_node:
            self.process_cmd_industrial_data(message_bytes, payload)
        elif CMD_ACK_FOR_DOWNLINK == cmd_from_node:
            self.process_txack(payload)
        elif CMD_HEIGHT_LEVEL_SENSOR_DATA == cmd_from_node:
            self.process_cmd_distance_data(message_bytes, payload)
        else:
            print("[DFU] Unhandled command " + str(cmd_from_node))

    # Process an ACK from the Chirpstack
    def process_txack(self, payload):
        if self.state == STATE_FW_CFG:
            # Send FW configuration
            self.send_fw_cfg()
        elif self.state == STATE_FW_DAT:
            # Send firmware data
            self.send_fw_dat()
        elif self.state == STATE_EPO_CFG:
            # Send EPO Configuration
            self.send_epo_cfg()
        elif self.state == STATE_EPO_DAT:
            # Send EPO data
            self.send_epo_dat()
        elif self.state == STATE_ACK_WAIT:
            # wait for ACK to complete
            self.on_wait_ack()
        elif self.state == STATE_RETRANSMIT:
            # Retransmit data
            self.retransmit_packet()
        else:
            print(f"[DFU] Unknown state in process_txack {payload}")

    # Process payload from MQTT
    def process_message(self, mqttc, msg):
        self.mqttc = mqttc
        payload = msg.payload.decode();
        payload = json.loads(payload)
        delimiter = "/"
        substring_list = msg.topic.split(delimiter)
        type = substring_list[-1]
        logging.info("process_message type = " + type)
        if type == "join":
            self.process_join_message(payload)
        elif type == "up":
            self.process_uplink_message(payload)
        elif type == "txack":
            self.process_txack(payload)
        else:
            logging.info("[DFU] Unhandled message from Chirpstack, type:" + type)
            if type == "log":
                logging.info(payload)


# MQTT Client Callbacks
def on_connect(mqttc, obj, flags, rc):
    logging.info(f"========== Connected to MQTT broker with result code: {rc} ==========")
    print(f"========== Connected to MQTT broker with result code: {rc} ==========")

device_dict = {}

def on_message(mqttc, obj, msg):
    # Process message by DFU - Device firmware upgrade
    logging.info(f"========== MQTT MESSAGE RECEIVED on topic: {msg.topic} ==========")
    print(f"========== MQTT MESSAGE RECEIVED on topic: {msg.topic} ==========")
    try:
        substring_list = msg.topic.split("/")
        app_id = substring_list[1]
        dev_eui = substring_list[3]
        logging.info(f"Processing message for device: {dev_eui}")
        print(f"Processing message for device: {dev_eui}")
        if dev_eui in device_dict:
            dfu_o = device_dict[dev_eui]
        else:
            dfu_o = dfu()
            device_dict[dev_eui] = dfu_o
        dfu_o.process_message(mqttc, msg)
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        print(f"Error processing message: {e}")


def on_publish(mqttc, obj, mid):
    logging.info("mid: " + str(mid))


def on_subscribe(mqttc, obj, mid, granted_qos):
    logging.info(f"========== Subscribed successfully: {mid} QoS: {granted_qos} ==========")
    print(f"========== Subscribed successfully: {mid} QoS: {granted_qos} ==========")


def on_log(mqttc, obj, level, string):
    print(string)


# DFU Object - Global scope for handling MQTT messages
# dfu_o = dfu()


# MQTT Client Setup
def mqtt_client_start():
    mqttc = mqtt.Client()
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    logging.info("mqtt_server_ip: "+mqtt_server_ip)
    mqttc.username_pw_set("VNetwork", "test")
    mqttc.connect(mqtt_server_ip, mqtt_port, 60)

    # Subscribe to the application topic for the specific device
    topic = f"application/+/device/+/event/#"
    mqttc.subscribe(topic, 0)

    mqttc.loop_forever()


# FastAPI Background Task for starting the MQTT client listener
async def start_mqtt_listener():
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, mqtt_client_start)
