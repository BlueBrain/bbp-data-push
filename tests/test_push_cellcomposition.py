import os
import logging
from datetime import datetime

from bba_data_push.bba_dataset_push import _initialize_pusher_cli, push_cellcomposition

import certifi
with open(certifi.where()) as cert:
    print(cert.read())

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

forge_config_file = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

nexus_env = "https://staging.nise.bbp.epfl.ch/nexus/v1"
nexus_org = "bbp"
nexus_proj = "atlasdatasetrelease"
nexus_token = os.environ["NEXUS_STAGING_TOKEN"]

atlasrelease_id = "https://bbp.epfl.ch/neurosciencegraph/data/brainatlasrelease/c96c71a8-4c0d-4bc1-8a1a-141d9ed6693d"

test_folder = os.environ["TEST_FOLDER"]
folder = os.path.join(test_folder, "tests_data")
volume_path = os.path.join(folder, "cellCompVolume_path")
densities_path = folder
summary_path = os.path.join(folder, "density_stats.json")

def test_push_cellcomposition():
    verbose = 2

    forge, verbose_L = _initialize_pusher_cli(verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token)
    files_name = "GitLab unit test"
    files_desc = f"{files_name} on {datetime.now()}"
    cc_id = push_cellcomposition(forge, L, atlasrelease_id, volume_path, densities_path, summary_path, files_name, files_desc, "output_dir")
    L.info("The Nexus ID of the registered CellComposition is\n%s" % cc_id)
