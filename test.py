from gatt import DeviceManager

if __name__ == "__main__":
    # Example usage of DeviceManager

    host_name = "TELLDUS_030000"
    mqtt_host = "mqtt.telldus.com"
    mqtt_port = 8883  # Typically 8883 for MQTT over TLS
    mqtt_user = "TELLDUS_030000"
    mqtt_password = "qM9KXFw3Dkpt"

    manager = DeviceManager(host_name, mqtt_host, mqtt_port, mqtt_user, mqtt_password)

    # Start discovery for specific device names
    device_names = ["uRskKI4BUh"]
    manager.start_discovery(device_names)

    try:
        manager.run()
    except KeyboardInterrupt:
        print("Interrupted by user, stopping...")
        manager.stop()
