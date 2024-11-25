import json
import logging
import sys
import threading
import time
import uuid
import ssl

import paho.mqtt.client as mqtt

from . import errors  # Assuming errors module exists


# Configure logging
logger = logging.getLogger("DeviceManager")
logger.setLevel(logging.INFO)

class DeviceManager:
    """
    Entry point for managing BLE GATT devices.

    This class manages Bluetooth devices discovered by a remote hub via MQTT.
    It connects to an MQTT broker, subscribes to device discovery topics,
    filters devices based on provided names, and maintains a list of applicable devices.
    """

    def __init__(
        self,
        host_name,
        mqtt_host,
        mqtt_port,
        mqtt_user,
        mqtt_password,
        target_host_name,
        device_code,
        gateway_mac
    ):
        """
        Initializes the DeviceManager by establishing an MQTT connection.

        :param host_name: The hostname used in MQTT topics.
        :param mqtt_host: The MQTT broker address.
        :param mqtt_port: The MQTT broker port.
        :param mqtt_user: MQTT username.
        :param mqtt_password: MQTT password.
        :param target_host_name: The target host name for MQTT topics.
        :param device_code: UUID of gateway.
        :param gateway_mac: MAC of gateway.
        """
        logger.info('Initializing DeviceManager')
        self.host_name = host_name
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_password = mqtt_password
        self.target_host_name = target_host_name
        self.device_code = device_code
        self.gateway_mac = gateway_mac

        self.listener = None
        self.adapter_name = 'mqtt'
        self._adapter = None
        self._adapter_properties = False

        self._devices = {}
        self._dev_names = set()
        self._inspect_pending_devices = set()  # Set of MAC addresses awaiting mod.ble.inspect reportAttribute
        self._discovery_active = False

        self._stop_event = threading.Event()

        # Command tracking
        # Maps command_id to a dict with 'event', 'response', 'report_event', 'report',
        # 'characteristic', 'command_type', 'service_uuid', 'characteristic_uuid'
        self._pending_commands = {}
        self._commands_lock = threading.RLock()

        # Initialize MQTT client
        self._init_mqtt()

    def _init_mqtt(self):
        """
        Initializes and configures the MQTT client.
        """
        self._adapter = mqtt.Client(client_id=self.host_name, protocol=mqtt.MQTTv311)

        # Set username and password if provided
        if self.mqtt_user and self.mqtt_password:
            self._adapter.username_pw_set(self.mqtt_user, self.mqtt_password)

        # Configure TLS
        try:
            self._adapter.tls_set(
                ca_certs='certs/mqtt.crt',  # Path to CA certificate
                certfile='certs/mqtt.cert',  # Path to client certificate
                keyfile='certs/mqtt.key',    # Path to client key
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLSv1_2,
                ciphers=None
            )
            self._adapter.tls_insecure_set(False)  # Ensure certificate verification
        except ssl.SSLError as e:
            logger.error(f"SSL configuration failed: {e}")
            raise

        # Set Last Will and Testament (LWT)
        will_topic = f"telldus/tellstick/{self.host_name}/will"
        will_payload = json.dumps({"status": "offline"})
        self._adapter.will_set(will_topic, payload=will_payload, qos=1, retain=True)

        # Assign callback methods
        self._adapter.on_connect = self._on_connect
        self._adapter.on_disconnect = self._on_disconnect
        self._adapter.on_message = self._on_message

        try:
            self._adapter.connect_async(self.mqtt_host, self.mqtt_port, keepalive=60)
            logger.debug(f"Connecting to MQTT broker at {self.mqtt_host}:{self.mqtt_port}")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise _error_from_mqtt_error(f"Failed to connect to MQTT broker: {e}") from e

        # Start the MQTT network loop in a separate thread
        self._adapter.loop_start()

    @property
    def is_adapter_powered(self):
        """
        Returns the connection status of the MQTT client.

        :return: True if connected to the MQTT broker, False otherwise.
        """
        return self._adapter_properties

    @is_adapter_powered.setter
    def is_adapter_powered(self, powered):
        """
        Sets the adapter power status.

        :param powered: True if connected, False otherwise.
        """
        self._adapter_properties = powered

    def _on_connect(self, client, userdata, flags, rc):
        """
        Callback when the MQTT client connects to the broker.

        :param client: The MQTT client instance.
        :param userdata: The private user data.
        :param flags: Response flags sent by the broker.
        :param rc: The connection result.
        """
        if rc == 0:
            self.is_adapter_powered = True
            # Subscribe to the event topic
            event_topic = f"telldus/tellstick/{self.target_host_name}/event"
            client.subscribe(event_topic)
            logger.info(f"Connected to MQTT broker and subscribed to {event_topic}")
        else:
            self.is_adapter_powered = False
            logger.error(f"Failed to connect to MQTT broker with result code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """
        Callback when the MQTT client disconnects from the broker.

        :param client: The MQTT client instance.
        :param userdata: The private user data.
        :param rc: The disconnection result.
        """
        self.is_adapter_powered = False
        if rc != 0:
            logger.warning("Unexpected MQTT disconnection.")
        else:
            logger.info("MQTT client disconnected successfully.")

    def _on_message(self, client, userdata, msg):
        """
        Callback when a message is received from the MQTT broker.

        :param client: The MQTT client instance.
        :param userdata: The private user data.
        :param msg: The received MQTT message.
        """
        event_topic = f"telldus/tellstick/{self.target_host_name}/event"
        try:
            retained = msg.retain
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
            message_type = data.get('type', '')
            command_id = None
            if retained:
                logger.warning(f"Retained message")
                self._adapter.publish(
                    event_topic,
                    payload=None,
                    qos=1,
                    retain=True
                )
            if message_type == 'cmdResult':
                logger.debug(f"Handling {message_type}")
                cmd_id_raw = data.get('data', {}).get('id', '').strip()
                cmd_code = data.get('data', {}).get('code', -1)

                with self._commands_lock:
                    command_entry = self._pending_commands.get(cmd_id_raw)
                    if command_entry:
                        command_entry['response'] = data
                        command_entry['event'].set()
                        logger.debug(f"Received cmdResult for command ID: {cmd_id_raw} with code: {cmd_code}")

                        if command_entry['command_type'] == "inspect" and cmd_code == 0:
                            target_mac = data.get('data', {}).get('arguments', {}).get('mac', '').strip()
                            if target_mac:
                                self._inspect_pending_devices.add(target_mac)
                                logger.debug(f"Device {target_mac} is pending mod.ble.inspect reportAttribute.")

            elif message_type == 'reportAttribute':
                attribute = data.get('data', {}).get('attribute', '').strip()
                target_mac = data.get('data', {}).get('mac', '').strip()

                if attribute == "mod.ble.connected":
                    logger.debug(f"Received connected event for MAC: {target_mac}")
                    device = self._devices.get(target_mac)
                    if device:
                        device.connect_succeeded()
                    else:
                        logger.error(f"No device found with MAC: {target_mac} for connected event.")

                elif attribute == "mod.ble.disconnected":
                    logger.debug(f"Received disconnected event for MAC: {target_mac}")
                    device = self._devices.get(target_mac)
                    if device:
                        value = data.get('data', {}).get('value', {})
                        reason = value.get('reason', None)
                        reason_hex = self._convert_reason_to_hex(reason)
                        scan_rssi = device.rssi if device.rssi is not None else "Not available"

                        logger.warning(f"Device {target_mac} disconnected.")
                        logger.warning(f"Reason (hex): {reason_hex}")
                        logger.warning(f"RSSI: {scan_rssi}")

                        device.disconnect_succeeded()
                    else:
                        logger.error(f"No device found with MAC: {target_mac} for disconnected event.")

                elif attribute == "mod.ble.attr":
                    service_uuid = data.get('data', {}).get('value', {}).get('service', '')
                    characteristic_uuid = data.get('data', {}).get('value', {}).get('characteristic', '')
                    characteristic_data = data.get('data', {}).get('value', {}).get('data', '')

                    with self._commands_lock:
                        for cmd_id, command in self._pending_commands.items():
                            if (
                                command.get('service_uuid', '').lower() == service_uuid.lower() and
                                command.get('characteristic_uuid', '').lower() == characteristic_uuid.lower()
                            ):
                                command['report'] = data
                                command['report_event'].set()
                                logger.debug(f"Received reportAttribute for command ID: {cmd_id}")

                                characteristic = command.get('characteristic')
                                if characteristic:
                                    device = characteristic.service.device if characteristic.service else None
                                    if device and characteristic_uuid:
                                        device.characteristic_value_updated(
                                            characteristic_uuid,
                                            characteristic_data
                                        )
                                break

                elif attribute == "mod.device_list":
                    logger.debug("Handling mod.device_list reportAttribute.")
                    device_list = data.get('data', {}).get('value', {}).get('device_list', [])

                    if self._discovery_active and self._dev_names:
                        for device_info in device_list:
                            dev_name = device_info.get('dev_name', '')
                            if dev_name in self._dev_names:
                                mac = device_info.get('ble_addr') or device_info.get('mac')
                                scan_rssi = device_info.get('scan_rssi', None)
                                if mac and mac not in self._devices:
                                    device = self.make_device(mac)
                                    if device:
                                        self._devices[mac] = device
                                        device.rssi = scan_rssi
                                        logger.debug(
                                            f"Discovered device named {dev_name} with MAC: {mac} "
                                            f"and RSSI: {scan_rssi}"
                                        )
                    else:
                        # Discovery is not active; ignore incoming device information
                        pass

                elif attribute == "mod.ble.inspect":
                    logger.debug(f"Handling {attribute} reportAttribute.")
                    gateway_uuid = data.get('deviceCode', '').strip()
                    mac = data.get('data', {}).get('mac', '').strip()

                    if mac in self._inspect_pending_devices:
                        device = self._devices.get(mac)
                        if device:
                            if gateway_uuid == self.device_code:
                                device.services_resolved(data)
                                logger.debug(f"Processed mod.ble.inspect for device {mac}.")
                            else:
                                logger.error(
                                    f"Gateway UUID mismatch for device {mac}: expected "
                                    f"{self.device_code}, got {gateway_uuid}"
                                )
                        else:
                            logger.error(
                                f"No device found with MAC: {mac} for mod.ble.inspect reportAttribute."
                            )
                        self._inspect_pending_devices.discard(mac)
                    else:
                        logger.warning(
                            f"Received mod.ble.inspect reportAttribute for device {mac}, "
                            f"which is not pending inspection."
                        )
                else:
                    # Other reportAttribute messages can be handled here
                    pass

            else:
                # Other message types can be handled here
                pass

        except json.JSONDecodeError:
            logger.error("Received invalid JSON payload.")
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")

    def _convert_reason_to_hex(self, reason):
        """
        Converts the disconnection reason to a hexadecimal string.

        :param reason: The reason for disconnection.
        :return: Hexadecimal string representation of the reason.
        """
        if reason is None:
            return "Not provided."
        try:
            if isinstance(reason, int):
                return hex(reason)
            elif isinstance(reason, str):
                return reason.encode('utf-8').hex()
            else:
                return str(reason)
        except Exception as e:
            return f"Error converting reason to hex: {e}"

    def send_command(self, command_json, command_id, command_type=None, characteristic=None):
        """
        Sends a command JSON to the MQTT broker and tracks the command by its UUID.

        :param command_json: The command JSON to send.
        :param command_id: The UUID of the command.
        :param command_type: Type of the command (e.g., 'read', 'write', 'notify').
        :param characteristic: The Characteristic instance associated with this command.
        """
        with self._commands_lock:
            logger.debug("Acquiring lock at send_command")
            try:
                self._pending_commands[command_id] = {
                    'event': threading.Event(),
                    'response': None,
                    'report_event': threading.Event(),
                    'report': None,
                    'characteristic': characteristic,
                    'command_type': command_type,
                    'service_uuid': self._get_service_uuid(characteristic),
                    'characteristic_uuid': self._get_characteristic_uuid(characteristic)
                }

                if command_type == "inspect":
                    target_mac = command_json.get('data', {}).get('arguments', {}).get('mac', '').strip()
                    if target_mac:
                        self._inspect_pending_devices.add(target_mac)
                        logger.debug(f"Device {target_mac} is pending mod.ble.inspect reportAttribute.")
            finally:
                logger.debug("Released lock at send_command")

        # Publish the command to the appropriate topic
        command_topic = f"telldus/tellstick/{self.target_host_name}/command"
        self._adapter.publish(
            command_topic,
            payload=json.dumps(command_json),
            qos=1,
            retain=False
        )
        logger.debug(f"Sent command ID: {command_id} to topic: {command_topic}")

    def _get_service_uuid(self, characteristic):
        """
        Retrieves the service UUID for a given characteristic.

        :param characteristic: The Characteristic instance.
        :return: Service UUID as a string.
        """
        return characteristic.service.uuid if characteristic and characteristic.service else ''

    def _get_characteristic_uuid(self, characteristic):
        """
        Retrieves the characteristic UUID.

        :param characteristic: The Characteristic instance.
        :return: Characteristic UUID as a string.
        """
        return characteristic.uuid if characteristic else ''

    def wait_for_cmd_result(self, command_id, timeout=30):
        """
        Waits for the cmdResult of a specific command.

        :param command_id: The UUID of the command.
        :param timeout: Timeout in seconds.
        :return: The cmdResult data if received, else None.
        """
        with self._commands_lock:
            logger.debug("Acquiring lock at wait_for_cmd_result")
            try:
                command = self._pending_commands.get(command_id)
            finally:
                logger.debug("Released lock at wait_for_cmd_result")

        if command:
            event_set = command['event'].wait(timeout)
            if event_set:
                return command['response']
            else:
                logger.error(f"Timeout waiting for cmdResult of command ID: {command_id}")
        return None

    def wait_for_report_attribute(self, command_id, timeout=30):
        """
        Waits for the reportAttribute of a specific command.

        :param command_id: The UUID of the command.
        :param timeout: Timeout in seconds.
        :return: The reportAttribute data if received, else None.
        """
        with self._commands_lock:
            logger.debug("Acquiring lock at wait_for_report_attribute")
            try:
                command = self._pending_commands.get(command_id)
            finally:
                logger.debug("Released lock at wait_for_report_attribute")

        if command:
            event_set = command['report_event'].wait(timeout)
            if event_set:
                return command['report']
            else:
                logger.error(f"Timeout waiting for reportAttribute of command ID: {command_id}")
        return None

    def run(self):
        """
        Starts the main loop to keep the application running and processing MQTT events.

        This call blocks until `stop()` is called.
        """
        logger.info("DeviceManager is running. Press Ctrl+C to stop.")
        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Stops the MQTT client and the main loop.
        """
        if not self._stop_event.is_set():
            logger.debug("Stopping DeviceManager...")
            self._stop_event.set()
            if self._adapter:
                logger.debug("Disconnecting MQTT client...")
                self._adapter.disconnect()
                self._adapter.loop_stop()
            logger.info("DeviceManager stopped.")

    def devices(self):
        """
        Returns all known and filtered Bluetooth devices.

        :return: A list of Device instances.
        """
        return list(self._devices.values())

    def start_discovery(self, dev_names=None):
        """
        Starts discovery for BLE devices with the specified device names.

        :param dev_names: A list of device names to filter discovered devices.
        """
        if dev_names is None:
            dev_names = []
        if not dev_names:
            logger.error("No device names provided for discovery.")
            return

        self._dev_names = set(dev_names)
        self._discovery_active = True
        self._devices.clear()
        logger.info(f"Started discovery for devices: {self._dev_names}")

    def stop_discovery(self):
        """
        Stops the ongoing device discovery.
        """
        if self._discovery_active:
            self._discovery_active = False
            self._dev_names.clear()
            self._devices.clear()
            logger.info("Stopped discovery.")
        else:
            logger.warning("Discovery is not active.")

    def _device_discovered(self, mac_address):
        """
        Handles actions when a device is discovered.

        :param mac_address: The MAC address of the discovered device.
        """
        if not mac_address:
            logger.warning("Discovered device with empty MAC address.")
            return
        device = self._devices.get(mac_address) or self.make_device(mac_address)
        if device:
            self.device_discovered(device)

    def device_discovered(self, device):
        """
        Callback when a device is discovered.

        :param device: The discovered Device instance.
        """
        device.advertised()

    def make_device(self, mac_address):
        """
        Creates and returns a Device instance for the given MAC address.

        Override this method to return specific subclasses of Device.

        :param mac_address: The MAC address of the device.
        :return: An instance of Device or its subclass, or None if unsupported.
        """
        return Device(mac_address=mac_address, manager=self)

    def add_device(self, mac_address):
        """
        Adds a device with the given MAC address without discovery.

        :param mac_address: The MAC address of the device to add.
        """
        if mac_address not in self._devices:
            device = self.make_device(mac_address)
            if device:
                self._devices[mac_address] = device
                logger.debug(f"Manually added device with MAC: {mac_address}")

    def remove_device(self, mac_address):
        """
        Removes a device with the given MAC address.

        :param mac_address: The MAC address of the device to remove.
        """
        if mac_address in self._devices:
            del self._devices[mac_address]
            logger.debug(f"Removed device with MAC: {mac_address}")
        else:
            logger.warning(f"Attempted to remove non-existent device with MAC: {mac_address}")

    def remove_all_devices(self, skip_alias=None):
        """
        Removes all devices, optionally skipping a device with a specific alias.

        :param skip_alias: The alias of a device to skip during removal.
        """
        keys_to_be_deleted = [
            key for key, device in self._devices.items()
            if not (skip_alias and device.alias == skip_alias)
        ]

        for key in keys_to_be_deleted:
            del self._devices[key]
            logger.debug(f"Removed device with MAC: {key}")

    def update_devices(self):
        """
        Placeholder for any additional device update logic.
        """
        pass  # Implement any necessary update logic here


class Device:
    """
    Represents a BLE GATT Device.
    """

    def __init__(self, mac_address, manager):
        self.mac_address = mac_address
        self.manager = manager
        self._is_services_resolved = False
        self._is_connected = False
        self.alias = None  # Assuming alias attribute exists
        self.services = []
        self.connected_event = threading.Event()  # Event to manage connection status
        self.rssi = None  # Initialize RSSI attribute

    def advertised(self):
        """
        Called when an advertisement package has been received from the device.
        Initiates connection to resolve services.
        """
        logger.debug(f"Device {self.mac_address} advertised.")
        self.connect()

    def invalidate(self):
        """
        Invalidates the device, performing any necessary cleanup.
        """
        logger.debug(f"Device {self.mac_address} invalidated.")
        self.disconnect()

    def connect(self):
        """
        Initiates connection by sending mod.ble.inspect command.
        """
        logger.debug(f"Connecting to device {self.mac_address}...")
        self.send_inspect_command()

    def send_inspect_command(self):
        """
        Sends the mod.ble.inspect command to the MQTT broker to inspect the device.
        """
        command_id = str(uuid.uuid4())
        current_time = int(time.time())

        inspect_command = {
            "mac": self.manager.gateway_mac,
            "type": "cmd",
            "time": current_time,
            "from": "CLOUD",
            "deviceCode": self.manager.device_code or "00000000-0000-0000-0000-000000000000",
            "data": {
                "command": "getAttribute",
                "arguments": {
                    "mac": self.mac_address,
                    "value": {
                        "mac": self.mac_address,
                    },
                    "attribute": "mod.ble.inspect",
                    "ep": 1
                },
                "id": command_id
            },
            "to": "BLE"
        }

        logger.debug(
            f"Sending mod.ble.inspect command to device {self.mac_address} "
            f"with command ID: {command_id}"
        )
        self.manager.send_command(
            inspect_command,
            command_id,
            command_type="inspect",
            characteristic=None
        )

    def services_resolved(self, report_attribute_data):
        """
        Processes the mod.ble.inspect reportAttribute data to resolve services and characteristics.

        :param report_attribute_data: The JSON data from the reportAttribute message.
        """
        try:
            services_data = report_attribute_data.get('data', {}).get('value', {}).get('services', [])
            if not services_data:
                raise ValueError("No services data found in mod.ble.inspect reportAttribute.")

            self._parse_services(services_data)
            self._is_services_resolved = True
            self.connect_succeeded()
            logger.debug(f"Services and characteristics resolved for device {self.mac_address}.")
        except Exception as e:
            self.connect_failed(f"Failed to parse services: {e}")

    def _parse_services(self, services_data):
        """
        Parses the services and characteristics data and populates the services list.

        :param services_data: List of services data from MQTT.
        """
        self.services = []
        for service_info in services_data:
            service = Service(
                device=self,
                uuid=service_info.get('uuid', ''),
                servicename=service_info.get('servicename', '')
            )
            characteristics_data = service_info.get('characteristics', [])
            for char_info in characteristics_data:
                characteristic = Characteristic(
                    service=service,
                    uuid=char_info.get('uuid', ''),
                    handle=char_info.get('handle', 0),
                    properties=char_info.get('properties', ''),
                    length=char_info.get('len', 0),
                    value=char_info.get('value', ''),
                    hexvalue=char_info.get('hexvalue', '')
                )
                service.characteristics.append(characteristic)
            self.services.append(service)

    def connect_succeeded(self):
        """
        Called when the device has successfully connected and services are resolved.
        """
        if not self._is_connected:
            self._is_connected = True
            self.connected_event.set()
            logger.debug(f"Device {self.mac_address} connected successfully.")

    def connect_failed(self, error):
        """
        Called when the connection could not be established.

        :param error: The error message or code.
        """
        if self._is_connected:
            self._is_connected = False
            self.connected_event.clear()
        logger.error(f"Failed to connect to device {self.mac_address}: {error}")

    def disconnect(self):
        """
        Disconnects from the device, if connected.
        """
        if self._is_connected:
            logger.debug(f"Disconnecting from device {self.mac_address}...")
            # Implement actual disconnection logic here
            self._is_connected = False
            self.connected_event.clear()
            self.disconnect_succeeded()

    def disconnect_succeeded(self):
        """
        Called when the device has disconnected successfully.
        """
        self.services = []
        logger.debug(f"Device {self.mac_address} disconnected successfully.")

    def is_connected(self):
        """
        Returns `True` if the device is connected, otherwise `False`.
        """
        return self._is_connected

    def is_services_resolved(self):
        """
        Returns `True` if services are discovered, otherwise `False`.
        """
        return self._is_services_resolved

    def characteristic_value_updated(self, uuid, value):
        """
        Handles the updated value of a characteristic.
        This method is intended to be overridden by subclasses for application-specific handling.

        :param uuid: UUID of the characteristic that was updated.
        :param value: The new value of the characteristic as a hexadecimal string.
        """
        logger.debug(
            f"Device {self.mac_address}: Characteristic {uuid} updated with value {value}"
        )
        # Subclasses like FirmwareDevice can override this method to implement specific behavior

    def characteristic_read_value_failed(self, characteristic, error):
        """
        Handles a failed read operation.

        :param error: The error message or code.
        """
        logger.error(
            f"Failed to read value from Characteristic {characteristic.uuid} on device {self.mac_address}: {error}"
        )

    def characteristic_write_value_succeeded(self, characteristic):
        """
        Handles a successful write operation.

        :param characteristic: The Characteristic instance that succeeded the write operation.
        """
        logger.debug(
            f"Successfully wrote value to Characteristic {characteristic.uuid} on device {self.mac_address}."
        )

    def characteristic_write_value_failed(self, characteristic, error):
        """
        Handles a failed write operation.

        :param error: The error message or code.
        """
        logger.error(
            f"Failed to write value to Characteristic {characteristic.uuid} on device {self.mac_address}: {error}"
        )

    def characteristic_enable_notifications_succeeded(self, characteristic):
        """
        Handles successful notification/indication configuration.

        :param characteristic: The Characteristic instance that was configured.
        """
        logger.debug(
            f"Successfully configured notifications/indications for Characteristic {characteristic.uuid} on device {self.mac_address}."
        )

    def characteristic_enable_notifications_failed(self, characteristic, error):
        """
        Handles failed notification/indication configuration.

        :param characteristic: The Characteristic instance that failed to configure.
        :param error: The error message or code.
        """
        logger.error(
            f"Failed to configure notifications/indications for Characteristic {characteristic.uuid} on device {self.mac_address}: {error}"
        )


class Service:
    """
    Represents a GATT service.
    """

    def __init__(self, device, uuid, servicename):
        """
        Initializes the Service instance.

        :param device: The parent Device instance.
        :param uuid: The UUID of the service.
        :param servicename: The name of the service.
        """
        self.device = device
        self.uuid = uuid
        self.servicename = servicename
        self.characteristics = []

    def characteristics_resolved(self):
        """
        Called when all service's characteristics got resolved.
        """
        pass  # Implement if needed


class Descriptor:
    """
    Represents a GATT Descriptor which can contain metadata or configuration of its characteristic.
    """

    def __init__(self, characteristic, uuid):
        """
        Initializes the Descriptor instance.

        :param characteristic: The parent Characteristic instance.
        :param uuid: The UUID of the descriptor.
        """
        self.characteristic = characteristic
        self.uuid = uuid

    def read_value(self, offset=0):
        """
        Reads the value of this descriptor.

        When successful, the value will be returned, otherwise `descriptor_read_value_failed()` of the related
        device is invoked.

        :param offset: Offset from where to start reading the bytes (defaults to 0).
        """
        pass  # Implement descriptor reading logic


class Characteristic:
    """
    Represents a GATT characteristic.
    """

    def __init__(self, service, uuid, handle, properties, length, value, hexvalue):
        """
        Initializes the Characteristic instance.

        :param service: The parent Service instance.
        :param uuid: The UUID of the characteristic.
        :param handle: The handle of the characteristic.
        :param properties: The properties of the characteristic (e.g., Read, Write).
        :param length: The length of the characteristic value in bytes.
        :param value: The value of the characteristic.
        :param hexvalue: The hexadecimal representation of the characteristic value.
        """
        self.service = service
        self.uuid = uuid
        self.handle = handle
        self.properties = properties
        self.length = length
        self.value = value
        self.hexvalue = hexvalue

    def properties_changed(self, properties, changed_properties, invalidated_properties):
        """
        Called when a Characteristic property has changed.

        :param properties: All properties of the characteristic.
        :param changed_properties: The properties that have changed.
        :param invalidated_properties: The properties that have been invalidated.
        """
        value = changed_properties.get('Value')
        if value is not None:
            self.service.device.characteristic_value_updated(
                uuid=self.uuid,
                value=bytes(value).hex()
            )

    def read_value(self, timeout=30):
        """
        Reads the value of this characteristic.

        :param timeout: Timeout in seconds for waiting for the response.
        """
        if "read" not in self.properties.lower():
            logger.error(f"Characteristic {self.uuid} does not have read property.")
            return

        device = self.service.device
        logger.debug(f"Attempting to read Characteristic {self.uuid} on device {device.mac_address}")

        # Wait for the device to be connected before proceeding
        if not device.connected_event.wait(timeout=10):
            error_msg = "Connection timeout before read operation"
            logger.error(f"Device {device.mac_address} not connected within timeout. Cannot read Characteristic {self.uuid}.")
            device.connect_failed(error_msg)
            self.characteristic_read_value_failed(characteristic=self, error=error_msg)
            return

        command_id = str(uuid.uuid4())
        current_time = int(time.time())

        get_attribute_command = {
            "mac": device.manager.gateway_mac,
            "type": "cmd",
            "time": current_time,
            "from": "CLOUD",
            "deviceCode": device.manager.device_code or "00000000-0000-0000-0000-000000000000",
            "data": {
                "command": "getAttribute",
                "arguments": {
                    "mac": device.mac_address,
                    "value": {
                        "mac": device.mac_address,
                        "service": self.service.uuid,
                        "characteristic": self.uuid,
                        "handle": self.handle
                    },
                    "attribute": "mod.ble.attr.get",
                    "ep": 1
                },
                "id": command_id
            },
            "to": "BLE"
        }

        try:
            # Send the getAttribute command
            device.manager.send_command(
                get_attribute_command,
                command_id,
                command_type="read",
                characteristic=self
            )
            logger.debug(f"Sent getAttribute command with ID: {command_id} for Characteristic {self.uuid}")

            # Wait for cmdResult
            cmd_result = device.manager.wait_for_cmd_result(command_id, timeout=timeout)
            if not cmd_result:
                error_msg = f"Timeout waiting for cmdResult of getAttribute command ID: {command_id}"
                logger.error(error_msg)
                self.characteristic_read_value_failed(characteristic=self, error=error_msg)
                return

            cmd_code = cmd_result.get('data', {}).get('code', -1)
            if cmd_code == 0:
                logger.debug(f"getAttribute command succeeded with code 0 for Characteristic {self.uuid}")
            elif cmd_code == 99:
                logger.warning(
                    f"Gateway is processing connection for Characteristic {self.uuid} with code 99. "
                    "Waiting for connection confirmation."
                )
                # Wait for "mod.ble.connected" event
                if not device.connected_event.wait(timeout=20):
                    error_msg = "Connection timeout after receiving code 99 during read operation"
                    logger.error(
                        f"Device {device.mac_address} not connected within timeout after code 99. "
                        f"Cannot read Characteristic {self.uuid}."
                    )
                    device.connect_failed(error_msg)
                    self.characteristic_read_value_failed(characteristic=self, error=error_msg)
                    return
                else:
                    logger.debug(f"Device {device.mac_address} connected after code 99 for read operation.")
            else:
                error_msg = f"getAttribute command failed with code: {cmd_code}"
                logger.error(error_msg)
                self.characteristic_read_value_failed(characteristic=self, error=error_msg)
                return

            # Wait for reportAttribute only if services are resolved
            if self.service.device.is_services_resolved():
                report_attribute = device.manager.wait_for_report_attribute(command_id, timeout=timeout)
                if not report_attribute:
                    error_msg = f"Timeout waiting for reportAttribute of getAttribute command ID: {command_id}"
                    logger.error(error_msg)
                    self.characteristic_read_value_failed(characteristic=self, error=error_msg)
                    return

                # Extract the characteristic value
                data = report_attribute.get('data', {}).get('value', {})
                if data.get('service') != self.service.uuid or data.get('characteristic') != self.uuid:
                    error_msg = "Received reportAttribute does not match the requested service and characteristic UUIDs."
                    logger.error(error_msg)
                    self.characteristic_read_value_failed(characteristic=self, error=error_msg)
                    return

                # Update the characteristic value
                char_data = data.get('data', '')
                self.value = char_data
                self.hexvalue = char_data  # Assuming the data is already in hex string format
                logger.debug(f"Read value from Characteristic {self.uuid}: {self.hexvalue}")

        except Exception as e:
            logger.error(f"Exception during read_value: {e}")
            self.characteristic_read_value_failed(characteristic=self, error=str(e))

    def write_value(self, value, timeout=30):
        """
        Writes a value to this characteristic.

        :param value: The value to write as a hexadecimal string.
        :param timeout: Timeout in seconds for waiting for the response.
        :return: True if write was successful, False otherwise.
        """
        if "write" not in self.properties.lower():
            logger.error(f"Characteristic {self.uuid} does not have write property.")
            return False

        # Validate that the value is a hexadecimal string
        if not isinstance(value, str) or not all(c in '0123456789abcdefABCDEF' for c in value):
            error_msg = "Value to write must be a hexadecimal string."
            logger.error(error_msg)
            self.characteristic_write_value_failed(characteristic=self, error=error_msg)
            return False

        device = self.service.device
        logger.debug(f"Attempting to write to Characteristic {self.uuid} on device {device.mac_address}")

        # Wait for the device to be connected before proceeding
        if not device.connected_event.wait(timeout=10):
            error_msg = "Connection timeout before write operation"
            logger.error(
                f"Device {device.mac_address} not connected within timeout. Cannot write Characteristic {self.uuid}."
            )
            device.connect_failed(error_msg)
            self.characteristic_write_value_failed(characteristic=self, error=error_msg)
            return False

        command_id = str(uuid.uuid4())
        current_time = int(time.time())

        set_attribute_command = {
            "mac": device.manager.gateway_mac,
            "type": "cmd",
            "time": current_time,
            "from": "CLOUD",
            "deviceCode": device.manager.device_code or "00000000-0000-0000-0000-000000000000",
            "data": {
                "command": "setAttribute",
                "arguments": {
                    "mac": device.mac_address,
                    "value": {
                        "mac": device.mac_address,
                        "service": self.service.uuid,
                        "data": value,
                        "characteristic": self.uuid,
                        "handle": self.handle
                    },
                    "attribute": "mod.ble.attr.set",
                    "ep": 1
                },
                "id": command_id
            },
            "to": "BLE"
        }

        try:
            # Send the setAttribute command
            device.manager.send_command(
                set_attribute_command,
                command_id,
                command_type="write",
                characteristic=self
            )
            logger.debug(
                f"Sent setAttribute command with ID: {command_id} to write value: {value}"
            )

            # Wait for cmdResult
            cmd_result = device.manager.wait_for_cmd_result(command_id, timeout=timeout)
            if not cmd_result:
                error_msg = f"Timeout waiting for cmdResult of setAttribute command ID: {command_id}"
                logger.error(error_msg)
                self.characteristic_write_value_failed(characteristic=self, error=error_msg)
                return False

            cmd_code = cmd_result.get('data', {}).get('code', -1)
            if cmd_code == 0:
                logger.debug(
                    f"setAttribute command succeeded with code 0 for Characteristic {self.uuid}"
                )
            elif cmd_code == 99:
                logger.warning(
                    f"Gateway is processing connection for Characteristic {self.uuid} with code 99. "
                    "Waiting for connection confirmation."
                )
                # Wait for "mod.ble.connected" event
                if not device.connected_event.wait(timeout=20):
                    error_msg = "Connection timeout after receiving code 99 during write operation"
                    logger.error(
                        f"Device {device.mac_address} not connected within timeout after code 99. "
                        f"Cannot write Characteristic {self.uuid}."
                    )
                    device.connect_failed(error_msg)
                    self.characteristic_write_value_failed(characteristic=self, error=error_msg)
                    return False
                else:
                    logger.debug(
                        f"Device {device.mac_address} connected after code 99 for write operation."
                    )
            else:
                error_msg = f"setAttribute command failed with code: {cmd_code}"
                logger.error(error_msg)
                self.characteristic_write_value_failed(characteristic=self, error=error_msg)
                return False

            # Wait for reportAttribute only if services are resolved
            if self.service.device.is_services_resolved():
                report_attribute = device.manager.wait_for_report_attribute(command_id, timeout=timeout)
                if not report_attribute:
                    error_msg = f"Timeout waiting for reportAttribute of setAttribute command ID: {command_id}"
                    logger.error(error_msg)
                    self.characteristic_write_value_failed(characteristic=self, error=error_msg)
                    return False

                # Assuming setAttribute does not return data, so just mark success
                self.value = value
                self.hexvalue = value
                logger.debug(f"Wrote value to Characteristic {self.uuid}: {self.hexvalue}")
                self.characteristic_write_value_succeeded(self)
                return True

            # If services are already resolved, assume write was successful
            self.value = value
            self.hexvalue = value
            logger.debug(f"Wrote value to Characteristic {self.uuid}: {self.hexvalue}")
            self.characteristic_write_value_succeeded(self)
            return True

        except Exception as e:
            logger.error(f"Exception during write_value: {e}")
            self.characteristic_write_value_failed(characteristic=self, error=str(e))
            return False

    def enable_notifications(self, notify=True, timeout=30):
        """
        Configures the characteristic for notifications or indications.

        :param notify: True to enable notifications, False to disable, or 'indicate' to enable indications.
        :param timeout: Timeout in seconds for waiting for the response.
        :return: True if configuration was successful, False otherwise.
        """
        properties_lower = self.properties.lower()
        if "notify" not in properties_lower and "indicate" not in properties_lower:
            error_msg = f"Characteristic {self.uuid} does not support notifications or indications."
            logger.error(error_msg)
            self.characteristic_enable_notifications_failed(
                self, "Notifications/Indications not supported."
            )
            return False

        device = self.service.device
        logger.debug(
            f"Attempting to configure notifications/indications for Characteristic {self.uuid} "
            f"on device {device.mac_address}"
        )

        # Wait for the device to be connected before proceeding
        if not device.connected_event.wait(timeout=10):
            error_msg = "Connection timeout before configure notifications/indications"
            logger.error(
                f"Device {device.mac_address} not connected within timeout. "
                f"Cannot configure notifications/indications for Characteristic {self.uuid}."
            )
            device.connect_failed(error_msg)
            self.characteristic_enable_notifications_failed(
                self, "Connection timeout before configure notifications/indications"
            )
            return False

        # Determine the mode
        if isinstance(notify, str) and notify.lower() == 'indicate':
            mode = 2
        elif isinstance(notify, bool):
            mode = 1 if notify else 0
        else:
            error_msg = "Invalid parameter for notify. Must be True, False, or 'indicate'."
            logger.error(error_msg)
            self.characteristic_enable_notifications_failed(
                self, "Invalid parameter for notify."
            )
            return False

        # Determine attribute based on mode
        attribute = "mod.ble.attr.notify" if mode != 0 else "mod.ble.attr.notify"

        command_id = str(uuid.uuid4())
        current_time = int(time.time())

        set_attribute_command = {
            "mac": device.manager.gateway_mac,
            "type": "cmd",
            "time": current_time,
            "from": "CLOUD",
            "deviceCode": device.manager.device_code or "00000000-0000-0000-0000-000000000000",
            "data": {
                "command": "setAttribute",
                "arguments": {
                    "mac": device.mac_address,
                    "value": {
                        "mac": device.mac_address,
                        "service": self.service.uuid,
                        "mode": mode,
                        "characteristic": self.uuid,
                        "handle": self.handle
                    },
                    "attribute": attribute,
                    "ep": 1
                },
                "id": command_id
            },
            "to": "BLE"
        }

        try:
            # Send the setAttribute command
            device.manager.send_command(
                set_attribute_command,
                command_id,
                command_type="notify",
                characteristic=self
            )
            logger.debug(
                f"Sent setAttribute command with ID: {command_id} to set notifications mode: {mode}"
            )

            # Wait for cmdResult
            cmd_result = device.manager.wait_for_cmd_result(command_id, timeout=timeout)
            if not cmd_result:
                error_msg = f"Timeout waiting for cmdResult of setAttribute command ID: {command_id}"
                logger.error(error_msg)
                self.characteristic_enable_notifications_failed(
                    self, "Timeout waiting for cmdResult"
                )
                return False

            cmd_code = cmd_result.get('data', {}).get('code', -1)
            if cmd_code == 0:
                logger.debug(
                    f"setAttribute command succeeded with code 0 for Characteristic {self.uuid}"
                )
            elif cmd_code == 99:
                logger.warning(
                    f"Gateway is processing connection for Characteristic {self.uuid} with code 99. "
                    "Waiting for connection confirmation."
                )
                # Wait for "mod.ble.connected" event
                if not device.connected_event.wait(timeout=20):
                    error_msg = "Connection timeout after receiving code 99 during configure notifications/indications"
                    logger.error(
                        f"Device {device.mac_address} not connected within timeout after code 99. "
                        f"Cannot configure notifications/indications for Characteristic {self.uuid}."
                    )
                    device.connect_failed(error_msg)
                    self.characteristic_enable_notifications_failed(
                        self, "Connection timeout after receiving code 99 during configure notifications/indications"
                    )
                    return False
                else:
                    logger.debug(
                        f"Device {device.mac_address} connected after code 99 for configure notifications/indications."
                    )
            else:
                error_msg = f"setAttribute command failed with code: {cmd_code}"
                logger.error(error_msg)
                self.characteristic_enable_notifications_failed(
                    self, f"Command failed with code: {cmd_code}"
                )
                return False

            # Wait for reportAttribute only if services are resolved
            if not self.service.is_services_resolved():
                report_attribute = device.manager.wait_for_report_attribute(command_id, timeout=timeout)
                if not report_attribute:
                    error_msg = f"Timeout waiting for reportAttribute of setAttribute command ID: {command_id}"
                    logger.error(error_msg)
                    self.characteristic_enable_notifications_failed(
                        self, "Timeout waiting for reportAttribute"
                    )
                    return False

                # Assuming setAttribute does not return data, so just mark success
                logger.debug(f"Configured notifications/indications for Characteristic {self.uuid}.")
                self.characteristic_enable_notifications_succeeded(self)
                return True

            # If services are already resolved, assume configuration was successful
            logger.debug(f"Configured notifications/indications for Characteristic {self.uuid}.")
            self.characteristic_enable_notifications_succeeded(self)
            return True

        except Exception as e:
            logger.error(f"Exception during enable_notifications: {e}")
            self.characteristic_enable_notifications_failed(
                self, str(e)
            )
            return False

    def characteristic_read_value_failed(self, characteristic, error):
        """
        Handles a failed read operation.

        :param error: The error message or code.
        """
        self.service.device.characteristic_read_value_failed(error)

    def characteristic_write_value_succeeded(self, characteristic):
        """
        Handles a successful write operation.

        :param characteristic: The Characteristic instance that succeeded the write operation.
        """
        self.service.device.characteristic_write_value_succeeded(characteristic)

    def characteristic_write_value_failed(self, characteristic, error):
        """
        Handles a failed write operation.

        :param error: The error message or code.
        """
        self.service.device.characteristic_write_value_failed(error)

    def characteristic_enable_notifications_succeeded(self, characteristic):
        """
        Handles successful notification/indication configuration.

        :param characteristic: The Characteristic instance that was configured.
        """
        self.service.device.characteristic_enable_notifications_succeeded(characteristic)

    def characteristic_enable_notifications_failed(self, characteristic, error):
        """
        Handles failed notification/indication configuration.

        :param characteristic: The Characteristic instance that failed to configure.
        :param error: The error message or code.
        """
        self.service.device.characteristic_enable_notifications_failed(characteristic, error)


def _error_from_mqtt_error(e):
    """
    Maps MQTT errors to custom errors.

    :param e: The original exception.
    :return: An instance of a custom error.
    """
    return {
        'mqtt': errors.AccessDenied("MQTT error")
    }.get('mqtt', errors.Failed(str(e)))


# Example usage of DeviceManager in a main function
def main():
    # Initialize DeviceManager with appropriate parameters
    device_manager = DeviceManager(
        host_name="TELLDUS_03000C",
        mqtt_host="mqtt.telldus.com",
        mqtt_port=30042,  # Typically 8883 for MQTT over TLS
        mqtt_user="TELLDUS_030000",
        mqtt_password="qM9KXFw3Dkpt",
        target_host_name="TELLDUS_E87F95",  # Who we are listening to and communicating with
        device_code="5e3f749c-f2b2-45f9-82ce-1a4ccfb10d82",
        gateway_mac="30:ae:7b:e8:7f:95"
    )

    # Start discovery for devices named "TelldusFlow", "BLE Mesh", "BLE MESH"
    target_device_names = ["TelldusFlow", "BLE Mesh", "BLE MESH"]
    device_manager.start_discovery(dev_names=target_device_names)

    # Run the DeviceManager in a separate daemon thread
    manager_thread = threading.Thread(target=device_manager.run, daemon=True)
    manager_thread.start()

    discovery_timeout = 40  # seconds
    devices = {}

    while not devices:
        logger.info(f"Waiting for {discovery_timeout} seconds to discover devices...")
        time.sleep(discovery_timeout)

        devices = device_manager.devices()
        if not devices:
            logger.warning("No devices discovered. Retrying...")

    if not devices:
        logger.error("No devices discovered. Exiting.")
    else:
        logger.info(f"Discovered {len(devices)} device(s). Retrieving firmware versions...")
        for device in devices:
            logger.info(f"Retrieving firmware version for device {device.mac_address}...")

            if not device.is_connected():
                logger.debug(
                    f"Device {device.mac_address} is not connected. Attempting to connect..."
                )
                if device.rssi is not None and device.rssi < -70:
                    logger.warning(
                        f"Device {device.mac_address} is too far away (RSSI: {device.rssi})."
                    )
                    continue
                else:
                    device.connect()  # Initiates connection; connection status is managed internally
                    # Wait for connection to be established
                    if not device.connected_event.wait(timeout=10):
                        logger.error(
                            f"Failed to connect to device {device.mac_address} within timeout."
                        )
                        continue  # Skip to the next device

            # Retrieve firmware version
            device.retrieve_firmware_version()
            # Optional: Wait a short time between commands to prevent flooding
            time.sleep(1)

    # Keep the main thread alive to handle asynchronous MQTT responses
    try:
        logger.info("Firmware retrieval initiated. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nShutting down DeviceManager...")
        device_manager.stop()
        manager_thread.join()
        logger.info("DeviceManager has been stopped.")


if __name__ == "__main__":
    main()
