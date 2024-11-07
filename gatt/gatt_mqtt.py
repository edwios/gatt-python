import json
import threading
import time
import paho.mqtt.client as mqtt
import ssl
from . import errors  # Assuming errors module exists

class DeviceManager:
    """
    Entry point for managing BLE GATT devices.

    This class manages Bluetooth devices discovered by a remote hub via MQTT.
    It connects to an MQTT broker, subscribes to device discovery topics,
    filters devices based on provided names, and maintains a list of applicable devices.
    """

    def __init__(self, host_name, mqtt_host, mqtt_port, mqtt_user, mqtt_password, target_host_name):
        """
        Initializes the DeviceManager by establishing an MQTT connection.

        :param host_name: The hostname used in MQTT topics.
        :param mqtt_host: The MQTT broker address.
        :param mqtt_port: The MQTT broker port.
        :param mqtt_user: MQTT username.
        :param mqtt_password: MQTT password.
        """
        self.host_name = host_name
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_password = mqtt_password
        self.target_host_name = target_host_name

        self.listener = None
        self.adapter_name = 'mqtt'
        self._adapter = None
        self._adapter_properties = False

        self._devices = {}
        self._dev_names = set()
        self._discovery_active = False

        self._stop_event = threading.Event()

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
        self._adapter.tls_set(
            ca_certs='certs/mqtt.crt',  # Use default CA certificates
            certfile='certs/mqtt.cert',
            keyfile='certs/mqtt.key',
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2,
            ciphers=None
        )
        self._adapter.tls_insecure_set(False)  # Ensure certificate verification

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
        except Exception as e:
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
            print(f"Connected to MQTT broker and subscribed to {event_topic}")
        else:
            self.is_adapter_powered = False
            print(f"Failed to connect to MQTT broker with result code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """
        Callback when the MQTT client disconnects from the broker.

        :param client: The MQTT client instance.
        :param userdata: The private user data.
        :param rc: The disconnection result.
        """
        self.is_adapter_powered = False
        if rc != 0:
            print("Unexpected MQTT disconnection.")
        else:
            print("MQTT client disconnected successfully.")

    def _on_message(self, client, userdata, msg):
        """
        Callback when a message is received from the MQTT broker.

        :param client: The MQTT client instance.
        :param userdata: The private user data.
        :param msg: The received MQTT message.
        """
        try:
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
            # print(f'MQTT msg:\n{payload}\n')
            # Navigate to the device_list in the JSON payload
            device_list = data.get('data', {}).get('value', {}).get('device_list', [])

            if self._discovery_active and self._dev_names:
                for device_info in device_list:
                    dev_name = device_info.get('dev_name', '')
                    if dev_name in self._dev_names:
                        # Prefer 'ble_addr' over 'mac' if available
                        mac = device_info.get('ble_addr') or device_info.get('mac')
                        if mac:
                            if mac not in self._devices:
                                device = self.make_device(mac)
                                if device:
                                    self._devices[mac] = device
                                    print(f"Discovered device: {dev_name} with MAC: {mac}")
                            # Update device attributes if necessary
                            # self._devices[mac].update_attributes(device_info)
            else:
                # Discovery is not active; ignore incoming device information
                pass

        except json.JSONDecodeError:
            print("Received invalid JSON payload.")
        except Exception as e:
            print(f"Error processing MQTT message: {e}")

    def run(self):
        """
        Starts the main loop to keep the application running and processing MQTT events.

        This call blocks until `stop()` is called.
        """
        print("DeviceManager is running. Press Ctrl+C to stop.")
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
            print("Stopping DeviceManager...")
            self._stop_event.set()
            if self._adapter:
                print("Disconnecting MQTT client...")
                self._adapter.disconnect()
                self._adapter.loop_stop()
            print("DeviceManager stopped.")

    def devices(self):
        """
        Returns all known and filtered Bluetooth devices.

        :return: A list of Device instances.
        """
        return list(self._devices.values())

    def start_discovery(self, dev_names=[]):
        """
        Starts discovery for BLE devices with the specified device names.

        :param dev_names: A list of device names to filter discovered devices.
        """
        if not dev_names:
            print("No device names provided for discovery.")
            return

        self._dev_names = set(dev_names)
        self._discovery_active = True
        self._devices.clear()
        print(f"Started discovery for devices: {self._dev_names}")

    def stop_discovery(self):
        """
        Stops the ongoing device discovery.
        """
        if self._discovery_active:
            print("Stopping discovery...")
            self._discovery_active = False
            self._dev_names.clear()
            self._devices.clear()
        else:
            print("Discovery is not active.")

    def _device_discovered(self, mac_address):
        """
        Handles actions when a device is discovered.

        :param mac_address: The MAC address of the discovered device.
        """
        if not mac_address:
            return
        device = self._devices.get(mac_address) or self.make_device(mac_address)
        if device is not None:
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
                print(f"Manually added device with MAC: {mac_address}")

    def remove_device(self, mac_address):
        """
        Removes a device with the given MAC address.

        :param mac_address: The MAC address of the device to remove.
        """
        if mac_address in self._devices:
            del self._devices[mac_address]
            print(f"Removed device with MAC: {mac_address}")

    def remove_all_devices(self, skip_alias=None):
        """
        Removes all devices, optionally skipping a device with a specific alias.

        :param skip_alias: The alias of a device to skip during removal.
        """
        keys_to_be_deleted = []
        for key, device in self._devices.items():
            if skip_alias and device.alias == skip_alias:
                continue
            keys_to_be_deleted.append(key)

        for key in keys_to_be_deleted:
            del self._devices[key]
            print(f"Removed device with MAC: {key}")

    def update_devices(self):
        """
        Placeholder for any additional device update logic.
        """
        # Implement any necessary update logic here
        pass

class Device:
    """
    Placeholder Device class.
    Assume this class is implemented elsewhere with necessary attributes and methods.
    """
    def __init__(self, mac_address, manager):
        self.mac_address = mac_address
        self.manager = manager
        self._connect_retry_attempt = 0
        self._is_services_resolved = False
        self._is_connected = False

    def advertised(self):
        """
        Called when an advertisement package has been received from the device. Requires device discovery to run.
        """
        pass

    def invalidate(self):
        pass

    def connect(self):
        """
        Refresh device services and characteristics.
        """
        self._connect()

    def _connect(self):
        self._connect_retry_attempt += 1

    def connect_succeeded(self):
        """
        Will be called when `connect()` has finished connecting to the device.
        Will not be called if the device was already connected.
        """
        pass

    def connect_failed(self, error):
        """
        Called when the connection could not be established.
        """
        pass

    def disconnect(self):
        """
        Disconnects from the device, if connected.
        """
        pass

    def disconnect_succeeded(self):
        """
        Will be called when the device has disconnected.
        """
        self.services = []

    def is_connected(self):
        """
        Returns `True` if the device was refreshed successfully, otherwise `False`.
        """
        return self._is_connected

    def is_services_resolved(self):
        """
        Returns `True` is services are discovered, otherwise `False`.
        """
        return self._is_services_resolved

    def properties_changed(self, sender, changed_properties, invalidated_properties):
        """
        Called when a device property has changed or got invalidated.
        """
        pass

    def services_resolved(self):
        """
        Called when all device's services and characteristics got resolved.
        """
        pass

    def characteristic_value_updated(self, characteristic, value):
        """
        Called when a characteristic value has changed.
        """
        # To be implemented by subclass
        pass

    def characteristic_read_value_failed(self, characteristic, error):
        """
        Called when a characteristic value read command failed.
        """
        # To be implemented by subclass
        pass

    def characteristic_write_value_succeeded(self, characteristic):
        """
        Called when a characteristic value write command succeeded.
        """
        # To be implemented by subclass
        pass

    def characteristic_write_value_failed(self, characteristic, error):
        """
        Called when a characteristic value write command failed.
        """
        # To be implemented by subclass
        pass

    def characteristic_enable_notifications_succeeded(self, characteristic):
        """
        Called when a characteristic notifications enable command succeeded.
        """
        # To be implemented by subclass
        pass

    def characteristic_enable_notifications_failed(self, characteristic, error):
        """
        Called when a characteristic notifications enable command failed.
        """
        # To be implemented by subclass
        pass

    def descriptor_read_value_failed(self, descriptor, error):
        """
        Called when a descriptor read command failed.
        """
        # To be implemented by subclass
        pass


class Service:
    """
    Represents a GATT service.
    """

    def __init__(self, device, uuid):
        # TODO: Don'T requore `path` argument, it can be calculated from device's path and uuid
        self.device = device
        self.uuid = uuid
        self.characteristics = []
        self.characteristics_resolved()


    def characteristics_resolved(self):
        """
        Called when all service's characteristics got resolved.
        """
        pass

class Descriptor:
    """
    Represents a GATT Descriptor which can contain metadata or configuration of its characteristic.
    """

    def __init__(self, characteristic, path, uuid):
        self.characteristic = characteristic
        self.uuid = uuid

    def read_value(self, offset=0):
        """
        Reads the value of this descriptor.

        When successful, the value will be returned, otherwise `descriptor_read_value_failed()` of the related
        device is invoked.
        """
        pass


class Characteristic:
    """
    Represents a GATT characteristic.
    """

    def __init__(self, service, path, uuid):
        # TODO: Don't require `path` parameter, it can be calculated from service's path and uuid
        self.service = service
        self.uuid = uuid

    def properties_changed(self, properties, changed_properties, invalidated_properties):
        """
        Called when a Characteristic property has changed.
        """
        pass

    def read_value(self, offset=0):
        """
        Reads the value of this characteristic.

        When successful, `characteristic_value_updated()` of the related device will be called,
        otherwise `characteristic_read_value_failed()` is invoked.
        """
        pass

    def write_value(self, value, offset=0):
        """
        Attempts to write a value to the characteristic.

        Success or failure will be notified by calls to `write_value_succeeded` or `write_value_failed` respectively.

        :param value: array of bytes to be written
        :param offset: offset from where to start writing the bytes (defaults to 0)
        """
        pass

    def _write_value_succeeded(self):
        """
        Called when the write request has succeeded.
        """
        pass

    def _write_value_failed(self, dbus_error):
        """
        Called when the write request has failed.
        """
        pass

    def enable_notifications(self, enabled=True):
        """
        Enables or disables value change notifications.

        Success or failure will be notified by calls to `characteristic_enable_notifications_succeeded`
        or `enable_notifications_failed` respectively.

        Each time when the device notifies a new value, `characteristic_value_updated()` of the related
        device will be called.
        """
        pass

    def _enable_notifications_succeeded(self):
        """
        Called when notification enabling has succeeded.
        """
        self.service.device.characteristic_enable_notifications_succeeded(characteristic=self)

    def _enable_notifications_failed(self, error):
        """
        Called when notification enabling has failed.
        """
        self.service.device.characteristic_enable_notifications_failed(characteristic=self, error=error)


def _error_from_mqtt_error(e):
    return {
        'mqtt': errors.AccessDenied("MQTT error")
    }.get('mqtt', errors.Failed(e))
