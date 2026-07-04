# conftest.py
# Lets pytest resolve "from src.transform import ..." style imports
# without needing to install the project as a package.

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
