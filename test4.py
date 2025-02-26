import json
import logging
import sys
import threading
import time
import uuid

from gatt import Characteristic, Device, DeviceManager, Service

# Step 1: Get the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # Set to the lowest level to capture all messages

# Step 2: Create a handler for INFO messages to stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)  # Handle INFO and above
stdout_handler.addFilter(lambda record: record.levelno == logging.INFO)  # Only INFO
stdout_formatter = logging.Formatter('>> %(message)s')
stdout_handler.setFormatter(stdout_formatter)

# Step 3: Create a handler for WARNING and above messages to stderr
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.DEBUG)  # Handle WARNING and above
stderr_formatter = logging.Formatter('%(levelname)s: %(message)s')
stderr_handler.setFormatter(stderr_formatter)

# Step 4: Add handlers to the root logger
root_logger.addHandler(stdout_handler)
root_logger.addHandler(stderr_handler)

# Optional: Prevent log messages from being propagated to the root logger multiple times
root_logger.propagate = False

class FirmwareDevice(Device):
    """
    Subclass of Device to handle firmware version retrieval.
    """

    SERVICE_STANDARD_FIRMWARE="0000180a-0000-1000-8000-00805f9b34fb"
    SERVICE_CUSTOM_FIRMWARE="00010203-0405-0607-0809-0a0b0c0d1920"
    STANDARD_UUID = "00002a26-0000-1000-8000-00805f9b34fb"  # Standard Firmware Revision String UUID
    CUSTOM_UUID = "00010203-0405-0607-0809-0A0B0C0D1921"  # Example Custom UUID

    is_busy = True

    def __init__(self, mac_address, manager):
        super().__init__(mac_address, manager);
        self.is_busy = True

    def connect_succeeded(self):
        super().connect_succeeded()
        root_logger.info(f"Device {self.mac_address} connected successfully.")
        self.retrieve_firmware_version()

    def retrieve_firmware_version(self):
        """
        Retrieves the firmware version by reading the standard or custom characteristic.
        """
        firmware_char = self.find_characteristic(self.STANDARD_UUID)

        if not firmware_char:
            root_logger.warning(
                f"Standard firmware characteristic UUID {self.STANDARD_UUID} not found. "
                f"Searching for custom UUID {self.CUSTOM_UUID}."
            )
            firmware_char = self.find_characteristic(self.CUSTOM_UUID)
            if not firmware_char:
                root_logger.error(
                    f"Custom firmware characteristic UUID {self.CUSTOM_UUID} not found "
                    f"for device {self.mac_address}."
                )
                return

        self.firmware_char = firmware_char
        root_logger.info(
            f"Reading firmware version from Characteristic UUID {firmware_char.uuid} "
            f"for device {self.mac_address}."
        )
        firmware_char.read_value(timeout=60)

    def find_characteristic(self, target_uuid):
        """
        Finds a characteristic by UUID.

        :param target_uuid: The UUID of the characteristic to find.
        :return: The Characteristic instance if found, else None.
        """
        target_uuid = target_uuid.lower()

        # Construct the firmware services and characteristics
        service_custom = Service(
            device=self,
            uuid=self.SERVICE_CUSTOM_FIRMWARE,
            servicename=''
        )
        service_standard = Service(
            device=self,
            uuid=self.SERVICE_STANDARD_FIRMWARE,
            servicename=''
        )
        characteristic = Characteristic(
            service=service_custom,
            uuid=self.CUSTOM_UUID,
            handle=0,
            properties='read',
            length=6,
            value='',
            hexvalue=''
        )
        service_custom.characteristics.append(characteristic)
        characteristic = Characteristic(
            service=service_standard,
            uuid=self.STANDARD_UUID,
            handle=0,
            properties='read',
            length=6,
            value='',
            hexvalue=''
        )
        service_standard.characteristics.append(characteristic)
        self.services.append(service_standard)
        self.services.append(service_custom)

        if len(self.services) == 0:
            root_logger.warning('No service found!')
        else:
            for service in self.services:
                if len(service.characteristics) == 0:
                    root_logger.warning(f'No charateristic found for service {service}!')
                else:
                    for characteristic in service.characteristics:
                        root_logger.info(f'Matching char {characteristic.uuid} to {target_uuid}')
                        if characteristic.uuid.lower() == target_uuid:
                            return characteristic
        return None

    def characteristic_value_updated(self, characteristic, value):
        """
        Handles the updated value of a characteristic.
        Prints the firmware version if this characteristic is the firmware characteristic.

        :param uuid: UUID of the characteristic that was updated.
        :param value: The new value of the characteristic as a hexadecimal string.
        """

        uuid = characteristic.uuid.lower()
        if uuid in [self.STANDARD_UUID.lower(), self.CUSTOM_UUID.lower()]:
            try:
                firmware_version = bytes.fromhex(value).decode('utf-8')
                root_logger.info(
                    f"Device {self.mac_address} Firmware Version: {firmware_version}"
                )
            except (ValueError, UnicodeDecodeError):
                root_logger.error(
                    f"Device {self.mac_address} Firmware Version (decoded error): {value}"
                )
            finally:
                self.is_busy = False
        else:
            root_logger.debug(
                f"Characteristic {uuid} value updated for device {self.mac_address}: {value}"
            )
        self.is_busy = False

    def characteristic_read_value_failed(self, error):
        """
        Callback when reading a characteristic value fails.

        :param error: The error message or code.
        """

        root_logger.error(
            f"Failed to read firmware version from device {self.mac_address}: {error}"
        )
        self.is_busy = False

    def characteristic_write_value_succeeded(self, characteristic):
        pass

    def characteristic_write_value_failed(self, characteristic, error):
        pass

    def characteristic_enable_notifications_succeeded(self, characteristic):
        pass

    def characteristic_enable_notifications_failed(self, characteristic, error):
        pass


class FirmwareDeviceManager(DeviceManager):
    """
    Subclass of DeviceManager to instantiate FirmwareDevice instead of Device.
    """

    def make_device(self, mac_address):
        """
        Creates and returns a FirmwareDevice instance for the given MAC address.

        :param mac_address: The MAC address of the device.
        :return: An instance of FirmwareDevice.
        """
        return FirmwareDevice(mac_address=mac_address, manager=self)


def main():
    # Initialize FirmwareDeviceManager with appropriate parameters
    device_manager = FirmwareDeviceManager(
        host_name="TELLDUS_030000",
        mqtt_host="mqtt.telldus.com",
        mqtt_port=30042,  # Typically 8883 for MQTT over TLS
        mqtt_user="TELLDUS_030000",
        mqtt_password="qM9KXFw3Dkpt",
        target_host_name="TELLDUS_0300C6",  # Who we are listening to and communicating with
        device_code="948ccb66-7f80-431f-a094-814d997774ab}",
        gateway_mac="ac:ca:54:03:00:c6"
    )

    # Start discovery for devices named "TelldusFlow", "BLE Mesh", "BLE MESH"
    target_device_names = ["TelldusFlow", "BLE Mesh", "BLE MESH", "jR8bzI9joR", "uRskKI4BUh"]
    device_manager.disconnect_all_devices()
    a=device_manager.device_connected()
    print(f'Device connected: {a}')
    while a is None:
        time.sleep(1)
        a=device_manager.device_connected()
    device_manager.start_discovery(dev_names=target_device_names)

    # Run the DeviceManager in a separate daemon thread
    manager_thread = threading.Thread(target=device_manager.run, daemon=True)
    manager_thread.start()

    discovery_timeout = 20  # seconds
    devices = {}

    try:
        while not devices:
            root_logger.info(f"Waiting for {discovery_timeout} seconds to discover devices...")
            time.sleep(discovery_timeout)

            devices = device_manager.devices()
            if not devices:
                root_logger.warning("No devices discovered. Retrying...")

        if not devices:
            root_logger.error("No devices discovered. Exiting.")
        else:
            device_manager.stop_discovery()
            root_logger.info(f"Discovered {len(devices)} device(s).")
            device_manager.getWhiteList()
            for device in devices:
                root_logger.info(f"Retrieving firmware version for device {device.mac_address}...")

                if not device.is_connected():
                    root_logger.warning(f"Device {device.mac_address} is not connected. Attempting to connect...")
                    if device.rssi < -70:
                        root_logger.warning(
                            f"Device {device.mac_address} is too far away (RSSI: {device.rssi})."
                        )
                        continue
                    else:
                        device.connect()
                        if not device.connected_event.wait(timeout=10):
                            root_logger.error(
                                f"Failed to connect to device {device.mac_address} within timeout."
                            )
                            device.disconnect()
                if device.is_busy:
                    """
                    Does this wait contribute the unexpected MQTT disconnect??
                    """
                    while device.is_busy:
                        print("Waiting for device to be free")
                        time.sleep(1)
                    device.disconnect()

                # device.retrieve_firmware_version()
                time.sleep(1)  # Prevent command flooding
        device_manager.stop_discovery()

        # Keep the main thread alive to handle asynchronous MQTT responses
        root_logger.info("Firmware retrieval initiated. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        device_manager.stop_discovery()
        device_manager.disconnect_all_devices()
        time.sleep(10)
        root_logger.info("\nShutting down DeviceManager...")
        device_manager.stop()
        manager_thread.join()
        root_logger.info("DeviceManager has been stopped.")


if __name__ == "__main__":
    main()
