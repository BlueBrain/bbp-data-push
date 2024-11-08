"""
Version corresponding to the git version tag
"""
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution('blue_brain_data_push').version
except DistributionNotFound:
    # package is not installed
    pass
