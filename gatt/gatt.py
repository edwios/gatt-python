import os
import platform

if platform.system() == 'Linux':
        from .gatt_linux import *
else:
    # TODO: Add support for more platforms
    from .gatt_stubs import *
