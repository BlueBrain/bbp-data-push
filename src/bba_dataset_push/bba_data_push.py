''' 
Create resource payload and push them along with the corresponding dataset files into Nexus.
To know more about Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation: https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
'''
import logging
import click
from kgforge.core import KnowledgeGraphForge

from bba_dataset_push.push_nrrd_volumetricdatalayer import createVolumetricResources
from bba_dataset_push.push_brainmesh import createMeshResources
from bba_dataset_push.push_sonata_cellrecordseries import createCellRecordResources
from bba_dataset_push.logging import log_args, close_handler
from bba_dataset_push import __version__

L = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _push_to_Nexus(dataset, forge, schema_id):

    L.info("\n----------------------- Resource content ----------------------" \
           f"\n{dataset[-1]}")
    try:
        L.info("\n-------------- Registration & Validation Status ---------------" \
               "\nRegistering the constructed payload along the input dataset in Nexus...")
        forge.register(dataset, schema_id)
        L.info(f"<<Resource synchronization status>>: {str(dataset[-1]._synchronized)}")
    except Exception as e:
        L.error(f"Error when registering resource. {e}")            

@click.group()
@click.version_option(__version__)
@click.option('-v', '--verbose', count=True)
@click.option('--forge_config_file', default = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml", help="Path to the configuration file "\
              "used to  instantiate the Forge") #type=click.Path(exists=True)
@click.option("--nexus_env",  default="prod", help="Nexus environment to use, can be 'dev',"\
              "'staging', 'prod' or the URL of a custom env")
@click.option("--nexus_org",  default='bbp', help="The Nexus organization to push into")
@click.option("--nexus_proj", default='atlas', help="The Nexus project to push into")
@click.option("--nexus_token_file", required = True, help="token from Nexus fusion")
@click.pass_context
@log_args(L)
def initialize_pusher_cli(ctx, verbose, forge_config_file, nexus_env, nexus_org, nexus_proj,
                          nexus_token_file):
    """Run the dataset pusher CLI starting by the Initialisation of the Forge python framework to 
    communicate with Nexus.\n
    The Forge will enable to build and push into Nexus the metadata payload along with the input dataset.
    """
    L.setLevel((logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)])
    
    default_environments = {
    "dev": "https://dev.nexus.ocp.bbp.epfl.ch/v1",
    "staging": "https://staging.nexus.ocp.bbp.epfl.ch/v1",
    "prod": "https://bbp.epfl.ch/nexus/v1"
    }

    if nexus_env[-1] == '/':
        nexus_env = nexus_env[:-1]
    if nexus_env in default_environments:
        nexus_env = default_environments[nexus_env]
    elif nexus_env in default_environments.values():
        pass
    else:
        L.error(f"Error: {nexus_env} do not correspond to one of the 3 possible environment: "
                "dev, staging, prod")
        exit(1)

    bucket = f"{nexus_org}/{nexus_proj}"
    try:
        token = open(nexus_token_file, 'r').read().strip()
        L.info("Initializing the forge...")
        forge = KnowledgeGraphForge(forge_config_file, endpoint = nexus_env,
                                           bucket = bucket, token = token)
    except Exception as e:
        L.error(f"Error when initializing the forge. {e}")
        exit(1)

    ctx.obj['forge'] = forge

    close_handler(L)


def base_ressource(f):
    f = click.option( '--dataset_path', type=click.Path(exists=True), required=True, multiple=True, 
                     help = "The file or directory of files to push on Nexus")(f)
    f = click.option('--config', type=click.Path(exists=True), required=True, help="Path to the "\
                     "generated dataset configuration file. This is a yaml file containing the paths "\
                    "to the Atlas pipeline generated dataset")(f)
    f = click.option('--provenances', type=str, multiple=True, default=[None], help = "name "\
                     "and version of the module that generated the dataset")(f)    
    return f

@initialize_pusher_cli.command()
@base_ressource
@click.option('--voxels_resolution', required=True, default=None, help="The Allen annotation volume "\
              "voxels resolution in microns")
@click.pass_context
@log_args(L)
def push_volumetric(ctx, dataset_path, voxels_resolution, config, provenances):
    """Create a VolumetricDataLayer resource payload and push it along with the corresponding 
    volumetric input dataset files into Nexus.\n
    """
    L.info("Filling the metadata of the volumetric payloads...")
    dataset = createVolumetricResources(
                            ctx.obj['forge'],
                            dataset_path, 
                            voxels_resolution,
                            config,
                            provenances
                            )
    if dataset:
        _push_to_Nexus(dataset,
                       ctx.obj['forge'],
                       "https://neuroshapes.org/dash/volumetricdatalayer")

@initialize_pusher_cli.command()
@base_ressource
@click.option('--hierarchy_path', type=click.Path(exists=True), required=True, multiple=True,
              help = "Hierarchy file")
@click.pass_context
@log_args(L)
def push_meshes(ctx, dataset_path, config, hierarchy_path, provenances):
    """Create a Mesh resource payload and push it along with the corresponding brain .OBJ mesh 
    folder input dataset files into Nexus.\n
    """
    L.info("Filling the metadata of the mesh payloads...")     
    dataset = createMeshResources(
                            ctx.obj['forge'],
                            dataset_path,
                            config,
                            hierarchy_path,
                            provenances
                            )
    if dataset:    
        _push_to_Nexus(dataset,
                       ctx.obj['forge'],
                       "https://neuroshapes.org/dash/brainparcellationmesh")


@initialize_pusher_cli.command()
@base_ressource
@click.option('--voxels_resolution', required=True, default=None, help="The Allen annotation volume "\
              "voxels resolution in microns")
@click.pass_context
@log_args(L)
def push_cellrecords(ctx, dataset_path, voxels_resolution, config, provenances):
    """Create a CellRecordSerie resource payload and push it along with the corresponding 
    Sonata hdf5 file input dataset files into Nexus.\n
    """
    L.info("Filling the metadata of the CellRecord payloads...")     
    dataset = createCellRecordResources(
                            ctx.obj['forge'],
                            dataset_path,
                            voxels_resolution,
                            config,
                            provenances
                            )
    if dataset:    
        _push_to_Nexus(dataset,
                       ctx.obj['forge'],
                       "https://neuroshapes.org/dash/cellrecordseries")



def start():
    initialize_pusher_cli(obj={})

if __name__ == '__main__':
    start()