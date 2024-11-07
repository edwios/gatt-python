import gatt

from argparse import ArgumentParser

# Example usage of DeviceManager

host_name = "TELLDUS_GATT"
mqtt_host = "mqtt.telldus.com"
mqtt_port = 30042  # Typically 8883 for MQTT over TLS
mqtt_user = "TELLDUS_030000"
mqtt_password = "qM9KXFw3Dkpt"
target_host_name = "TELLDUS_E87F95" # who are we listening to and communicating with
device_code = "5e3f749c-f2b2-45f9-82ce-1a4ccfb10d82"
gateway_mac = "30:ae:7b:e8:7f:95"

class AnyDevice(gatt.Device):
    def connect_succeeded(self):
        super().connect_succeeded()
        print("[%s] Connected" % (self.mac_address))

    def connect_failed(self, error):
        super().connect_failed(error)
        print("[%s] Connection failed: %s" % (self.mac_address, str(error)))

    def disconnect_succeeded(self):
        super().disconnect_succeeded()
        print("[%s] Disconnected" % (self.mac_address))

    def services_resolved(self):
        super().services_resolved()

        print("[%s] Resolved services" % (self.mac_address))
        for service in self.services:
            print("[%s]  Service [%s]" % (self.mac_address, service.uuid))
            for characteristic in service.characteristics:
                print("[%s]    Characteristic [%s]" % (self.mac_address, characteristic.uuid))


arg_parser = ArgumentParser(description="GATT Connect Demo")
arg_parser.add_argument('mac_address', help="MAC address of device to connect")
args = arg_parser.parse_args()

print("Connecting...")

manager = gatt.DeviceManager(host_name, mqtt_host, mqtt_port, mqtt_user, mqtt_password, target_host_name, device_code, gateway_mac)

device = AnyDevice(manager=manager, mac_address=args.mac_address)
device.connect()

manager.run()
