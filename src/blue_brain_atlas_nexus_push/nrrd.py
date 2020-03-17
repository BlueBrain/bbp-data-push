import sys
import logging

import nrrd
from blue_brain_atlas_nexus_push.cli import Cli
from kgforge.core import Resource

_logger = logging.getLogger(__name__)


class NrrdHandler(Cli):

    def build_resource(self) -> Resource:
        header = nrrd.read_header(self.shape_file)
        _logger.debug(header)
        return None

    def push(self, resource: Resource) -> None:
        pass

    def __init__(self, args):
        super().__init__(args)


def run():
    handler = NrrdHandler(sys.argv[1:])
    handler.push()


if __name__ == "__main__":
    run()
