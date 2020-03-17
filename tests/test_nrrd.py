import pytest
from blue_brain_atlas_nexus_push.nrrd import NrrdHandler

FORGE_CONFIG = "tests/configuration/forge-tests.yml"
NRRD_MAPPING = "tests/configuration/nrrd_mapping.hjson"
NRRD_FILE = "tests/data/sample.nrrd"


@pytest.fixture
def nrrd_args():
    return [NRRD_FILE, FORGE_CONFIG, "-m", NRRD_MAPPING]


@pytest.fixture
def nrrd_handler(nrrd_args):
    return NrrdHandler(nrrd_args)


def test_nrrd_handler(nrrd_handler: NrrdHandler):
    nrrd_handler.build_resource()