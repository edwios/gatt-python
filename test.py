from gatt import DeviceManager

if __name__ == "__main__":
    # Example usage of DeviceManager

    host_name = "TELLDUS_GATT"
    mqtt_host = "mqtt.telldus.com"
    mqtt_port = 30042  # Typically 8883 for MQTT over TLS
    mqtt_user = "TELLDUS_030000"
    mqtt_password = "qM9KXFw3Dkpt"
    target_host_name = "TELLDUS_E87F95" # who are we listening to and communicating with

    manager = DeviceManager(host_name, mqtt_host, mqtt_port, mqtt_user, mqtt_password, target_host_name)

    # Start discovery for specific device names
    device_names = ["uRskKI4BUh"]
    manager.start_discovery(device_names)

    try:
        manager.run()
    except KeyboardInterrupt:
        print("Interrupted by user, stopping...")
        manager.stop()
