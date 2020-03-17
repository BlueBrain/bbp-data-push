import argparse
import logging
import sys
from abc import abstractmethod

from blue_brain_atlas_nexus_push import __version__
from kgforge.core import KnowledgeGraphForge, Resource

__author__ = "Alejandra Garcia Rojas Martinez"
__copyright__ = "Blue Brain Project"
__license__ = "mit"

_logger = logging.getLogger(__name__)


class Cli:

    def __init__(self, args):
        args = self.parse_args(args)
        self.setup_logging(logging.DEBUG)

        _logger.debug("init Cli")
        self.forge = KnowledgeGraphForge(args.forge_config)
        self.shape_file = args.shape_file
        print(args.mapping_file)

    @abstractmethod
    def build_resource(self) -> Resource:
        pass

    @abstractmethod
    def push(self, resource: Resource) -> None:
        pass

    @staticmethod
    def parse_args(args):
        """Parse command line parameters

        Args:
          args ([str]): command line parameters as list of strings

        Returns:
          :obj:`argparse.Namespace`: command line parameters namespace
        """
        release_id = None
        parser = argparse.ArgumentParser(
            description="Blue Brain Project Atlas Nexus Push")
        parser.add_argument(
            dest="shape_file",
            help="shape file",
            type=str,
            metavar="SHAPE_FILE")
        parser.add_argument(
            dest="forge_config",
            help="configuration file for the KG-Forge",
            type=str,
            metavar="FORGE_CONFIG")
        parser.add_argument(
            "-m",
            "--mapping",
            dest="mapping_file",
            help="mapping file",
            type=str,
            metavar="FILE")
        parser.add_argument(
            "-r",
            "--release",
            help="id to be used as release",
            action="store",
            type=str,
            default=None,
            metavar="ID")
        parser.add_argument(
            "--version",
            action="version",
            version="blue_brain_atlas_nexus_push {ver}".format(ver=__version__))
        return parser.parse_args(args)

    @staticmethod
    def setup_logging(loglevel):
        """Setup basic logging

        Args:
          loglevel (int): minimum loglevel for emitting messages
        """
        logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
        logging.basicConfig(level=loglevel, stream=sys.stdout,
                            format=logformat, datefmt="%Y-%m-%d %H:%M:%S")
