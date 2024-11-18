import uuid
import time
import json
import threading
from gatt import Device, DeviceManager, Service, Characteristic

class FirmwareDevice(Device):
    """
    Subclass of Device to handle firmware version retrieval.
    """

    def retrieve_firmware_version(self):
        """
        Retrieves the firmware version by reading the standard or custom characteristic.
        """
        # Define UUIDs
        standard_uuid = "2A26"  # Standard Firmware Revision String UUID
        custom_uuid = "00010203-0405-0607-0809-0A0B0C0D1921"  # Example Custom UUID

        # Attempt to find the standard firmware version characteristic
        firmware_char = self.find_characteristic(standard_uuid)

        # If standard characteristic is not found, attempt to find the custom one
        if not firmware_char:
            print(f"Standard firmware characteristic UUID {standard_uuid} not found. Searching for custom UUID {custom_uuid}.")
            firmware_char = self.find_characteristic(custom_uuid)
            if not firmware_char:
                print(f"Custom firmware characteristic UUID {custom_uuid} not found for device {self.mac_address}.")
                return

        # Store the firmware_char for reference in characteristic_value_updated()
        self.firmware_char = firmware_char

        # Read the firmware version without handling the return value
        print(f"Reading firmware version from Characteristic UUID {firmware_char.uuid} for device {self.mac_address}.")
        firmware_char.read_value()

    def find_characteristic(self, target_uuid):
        """
        Finds a characteristic by UUID.

        :param target_uuid: The UUID of the characteristic to find.
        :return: The Characteristic instance if found, else None.
        """
        for service in self.services:
            for characteristic in service.characteristics:
                if characteristic.uuid.lower() == target_uuid.lower():
                    return characteristic
        return None

    # The following methods are not used in firmware retrieval but must be implemented
    def characteristic_value_updated(self, uuid, value):
        """
        Handles the updated value of a characteristic.
        Prints the firmware version if this characteristic is the firmware characteristic.

        :param uuid: UUID of the characteristic that was updated.
        :param value: The new value of the characteristic as a hexadecimal string.
        """
        # Check if the updated characteristic is the firmware characteristic
        if hasattr(self, 'firmware_char') and self.firmware_char and self.firmware_char.uuid.lower() == uuid.lower():
            try:
                # Decode the hexadecimal string to get the firmware version
                firmware_version = bytes.fromhex(value).decode('utf-8')
                print(f"***** Device {self.mac_address} Firmware Version: {firmware_version}")
            except ValueError:
                print(f"Device {self.mac_address} Firmware Version (hex): {value}")
            except UnicodeDecodeError:
                print(f"Device {self.mac_address} Firmware Version (decoded error): {value}")
        else:
            print(f"Characteristic {uuid} value updated for device {self.mac_address}: {value}")

    def characteristic_read_value_failed(self, error):
        """
        Callback when reading a characteristic value fails.

        :param error: The error message or code.
        """
        print(f"Failed to read firmware version from device {self.mac_address}: {error}")

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
        host_name="TELLDUS_03000C",
        mqtt_host="mqtt.telldus.com",
        mqtt_port=30042,  # Typically 8883 for MQTT over TLS
        mqtt_user="TELLDUS_030000",
        mqtt_password="qM9KXFw3Dkpt",
        target_host_name="TELLDUS_E87F95",  # Who we are listening to and communicating with
        device_code="5e3f749c-f2b2-45f9-82ce-1a4ccfb10d82",
        gateway_mac="30:ae:7b:e8:7f:95"
    )

    # Start discovery for devices named "TelldusFlow"
    target_device_names = ["TelldusFlow", "BLE Mesh", "BLE MESH"]  # Replace with your target device names
    device_manager.start_discovery(dev_names=target_device_names)

    # Run the DeviceManager in a separate thread
    manager_thread = threading.Thread(target=device_manager.run, daemon=True)
    manager_thread.start()

    # Allow some time for device discovery
    discovery_timeout = 40  # seconds
    devices = {}
    while not devices:
        print(f"Waiting for {discovery_timeout} seconds to discover devices...")
        time.sleep(discovery_timeout)

        # Iterate through all discovered devices and retrieve firmware versions
        devices = device_manager.devices()
        print("No devices discovered. Retrying...")
    if not devices:
        print("No devices discovered. We are done.")
    else:
        print(f"Discovered {len(devices)} device(s). Retrieving firmware versions...")
        for device in devices:
            print(f"Retrieving firmware version for device {device.mac_address}...")
            # Ensure the device is connected
            if not device.is_connected():
                print(f"Device {device.mac_address} is not connected. Attempting to connect...")
                if device.rssi < -70:
                    print(f'Device {device.mac_address} is too far away (rssi: {device.rssi})')
                else:
                    device.connect()  # Initiates connection; connection status is managed internally
                    # Wait for connection to be established
                    if not device.connected_event.wait(timeout=10):
                        print(f"Failed to connect to device {device.mac_address} within timeout.")
                        continue  # Skip to the next device

            # Retrieve firmware version
            device.retrieve_firmware_version()
            # Optional: Wait a short time between commands to prevent flooding
            time.sleep(1)

    # Keep the main thread alive to handle asynchronous MQTT responses
    try:
        print("Firmware retrieval initiated. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down DeviceManager...")
        device_manager.stop()
        manager_thread.join()
        print("DeviceManager has been stopped.")

if __name__ == "__main__":
    main()
