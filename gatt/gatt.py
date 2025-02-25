import os
import platform

if platform.system() == 'Linux':
        from .gatt_mqtt import *
elif platform.system() == 'Darwin':
        from .gatt_mqtt import *
else:
    # TODO: Add support for more platforms
    from .gatt_stubs import *
