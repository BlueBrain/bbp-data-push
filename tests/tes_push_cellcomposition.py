import logging
from bba_data_push.bba_dataset_push import initialize_pusher_cli_plain, push_cellcomposition_plain

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

forge_config_file = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

nexus_env = "https://staging.nise.bbp.epfl.ch/nexus/v1"
nexus_org = "bbp"
nexus_proj = "atlasdatasetrelease"
nexus_token = "" # paste a valid Nexus token

atlasrelease_id = "https://bbp.epfl.ch/neurosciencegraph/data/brainatlasrelease/c96c71a8-4c0d-4bc1-8a1a-141d9ed6693d"

folder = "/gpfs/bbp.cscs.ch/home/lcristel/BBP/atlas_pipelines/"
volume_path = folder+"cellCompVolume_path"
summary_path = folder+"cellCompSummary_density"

def test_push_cellcomposition():
    verbose = 2

    forge, verbose_L = initialize_pusher_cli_plain(verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token)
    cc_id = push_cellcomposition_plain(forge, verbose_L, atlasrelease_id, volume_path, summary_path, "my_name", "my_description")
    L.info("The Nexus ID of the registered CellComposition is", cc_id)
