import time
from argparse import ArgumentParser
import threading
import gatt

class CustomDevice(gatt.Device):
    def characteristic_value_updated(self, characteristic, value):
        super().characteristic_value_updated(characteristic, value)
        # Implement custom behavior, e.g., update UI or trigger events
        print(f"[CustomDevice] Characteristic {characteristic.uuid} updated to {value}")

    def characteristic_read_value_failed(self, characteristic, error):
        super().characteristic_read_value_failed(characteristic, error)
        # Implement custom error handling
        print(f"[CustomDevice] Failed to read Characteristic {characteristic.uuid}: {error}")

    def characteristic_write_value_succeeded(self, characteristic):
        super().characteristic_write_value_succeeded(characteristic)
        # Implement custom success handling
        print(f"[CustomDevice] Write to Characteristic {characteristic.uuid} succeeded.")

    def characteristic_write_value_failed(self, characteristic, error):
        super().characteristic_write_value_failed(characteristic, error)
        # Implement custom error handling
        print(f"[CustomDevice] Write to Characteristic {characteristic.uuid} failed: {error}")

    def characteristic_enable_notifications_succeeded(self, characteristic):
        super().characteristic_enable_notifications_succeeded(characteristic)
        # Implement custom success handling
        print(f"[CustomDevice] Notifications enabled for Characteristic {characteristic.uuid}.")

    def characteristic_enable_notifications_failed(self, characteristic, error):
        super().characteristic_enable_notifications_failed(characteristic, error)
        # Implement custom error handling
        print(f"[CustomDevice] Failed to enable notifications for Characteristic {characteristic.uuid}: {error}")

# Usage Example
if __name__ == "__main__":
    # Initialize DeviceManager
    device_manager = gatt.DeviceManager(
        host_name = "TELLDUS_GATT",
        mqtt_host = "mqtt.telldus.com",
        mqtt_port = 30042,  # Typically 8883 for MQTT over TLS
        mqtt_user = "TELLDUS_030000",
        mqtt_password = "qM9KXFw3Dkpt",
        target_host_name = "TELLDUS_E87F95", # who are we listening to and communicating with
        device_code = "5e3f749c-f2b2-45f9-82ce-1a4ccfb10d82",
        gateway_mac = "30:ae:7b:e8:7f:95"
    )

    # Start discovery for a specific device name
    device_manager.start_discovery(dev_names=['TelldusFlow'])

    # Run the DeviceManager in a separate thread or as a daemon
    threading.Thread(target=device_manager.run, daemon=True).start()

    # Wait for some time to allow device discovery
    time.sleep(5)

    arg_parser = ArgumentParser(description="GATT Connect Demo")
    arg_parser.add_argument('mac_address', help="MAC address of device to connect")
    args = arg_parser.parse_args()


    # Access the discovered device
    devices = device_manager.devices()
    if devices:
        device = devices[0]  # Assuming the first device is the target
        # Replace Device with CustomDevice
        device = CustomDevice(mac_address=device.mac_address, manager=device_manager)

        # Access a specific service
        if device.services:
            service = device.services[0]
            # Access a specific characteristic
            if service.characteristics:
                characteristic = service.characteristics[0]

                # Read the characteristic value
                value = characteristic.read_value()
                if value:
                    print(f"Characteristic Value: {value}")

                # Write a new value to the characteristic
                success = characteristic.write_value("0101")
                if success:
                    print("Write operation successful.")

                # Enable notifications for the characteristic
                success = characteristic.enable_notifications(notify=True)
                if success:
                    print("Notifications enabled.")
    else:
        print("No devices discovered.")

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        device_manager.stop()
