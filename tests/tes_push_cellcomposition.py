import logging
from bba_data_push.bba_dataset_push import _initialize_pusher_cli, push_cellcomposition

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)

forge_config_file = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

nexus_env = "https://staging.nise.bbp.epfl.ch/nexus/v1"
nexus_org = "bbp"
nexus_proj = "atlasdatasetrelease"

atlasrelease_id = "https://bbp.epfl.ch/neurosciencegraph/data/brainatlasrelease/c96c71a8-4c0d-4bc1-8a1a-141d9ed6693d"

folder = "/gpfs/bbp.cscs.ch/home/lcristel/BBP/atlas_pipelines/"
volume_path = folder+"cellCompVolume_small_path"
summary_path = folder+"cellCompSummary_density"

def test_push_cellcomposition():
    verbose = 2

    forge, verbose_L = _initialize_pusher_cli(verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, $NEXUS_STAGING_TOKEN)
    cc_id = push_cellcomposition(forge, L, atlasrelease_id, volume_path, summary_path, "my_name", "my_description", "output_dir")
    L.info("The Nexus ID of the registered CellComposition is\n%s" % cc_id)
