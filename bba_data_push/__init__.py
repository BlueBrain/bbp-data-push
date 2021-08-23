"""
Version corresponding to the git version tag
"""
from pkg_resources import get_distribution, DistributionNotFound
from bba_data_push import __name__

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass
