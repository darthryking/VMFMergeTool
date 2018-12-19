"""

constants.py

As it says on the tin.

"""

import os
import sys


try:
    IS_FROZEN = sys.frozen
except AttributeError:
    IS_FROZEN = False
    
    
if IS_FROZEN:
    GUI_FILE_DIR = sys._MEIPASS
else:
    GUI_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    
ICON_NAME = 'icon.png'
ICON_PATH = os.path.join(GUI_FILE_DIR, ICON_NAME)
