"""
Version corresponding to the git version tag
"""

import pkg_resources
from bba_data_push import __name__

__version__ = pkg_resources.get_distribution(__name__).version

