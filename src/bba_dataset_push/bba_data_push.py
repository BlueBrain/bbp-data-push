''' Create resource payload and push them along with the corresponding dataset files into Nexus.

(definition, lexicon, link to Atlas pipeline confluence)
'''
import logging
import click
from kgforge.core import KnowledgeGraphForge

from bba_dataset_push.push_nrrd_volumetricdatalayer import createNrrdResources
from bba_dataset_push.push_brainmesh import createMeshResources
from bba_dataset_push.logging import log_args, close_handler
from bba_dataset_push import __version__

L = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _push_to_Nexus(dataset, forge, schema_id):

    print("----------------------- Resource content ----------------------")
    print(dataset[-1])
    # try:
    #     print("-------------- Registration & Validation Status ---------------")
    #     L.info("Registering the constructed payload along the input dataset in  Nexus...")
    #     forge.register(dataset, schema_id)
    #     L.info(f"<<Resource synchronization status>>: {str(dataset[-1]._synchronized)}")
    # except Exception as e:
    #     L.error(f"Error when registering resource. {e}")
    print("---------------------------------------------------------------")
            

@click.group()
@click.version_option(__version__)
@click.option('-v', '--verbose', count=True)
@click.option( '--forge_config_file', required = True, type=click.Path(exists=True),
              help='The configuration file used to instantiate the Forge')
@click.option("--nexus_env",  default="staging", help="Nexus environment to use, can be 'dev',"\
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
    "staging": 'https://staging.nexus.ocp.bbp.epfl.ch/v1',
    "prod": "https://bbp.epfl.ch/nexus/v1"
    }
    L.info("Initializing the forge...")
    try:
        nexus_env = default_environments[nexus_env]
        bucket = nexus_org + '/' + nexus_proj
        token = open(nexus_token_file, 'r').read().strip()
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
    f = click.option('--hierarchy_path', type=click.Path(exists=True), required=True, multiple=True,
                     help = "Hierarchy file")(f)
    #f = click.option('--description', required=False, default=None, help="The description of the volumetric dataset being pushed")(f)
    return f

#def extra_arguments(f):
    # f = click.option('--extra_keys_values', "-extrakv", required=False, multiple=True, default=[], type = (str, str), help="Additionnal property couple key/value to add to the metadata")(f)
    # f = click.option("--contributor_name", '-contrib', required=False, default=[], type = str, help="Nexus ID of contributing organizations")(f) #multiple = True
    # f = click.option("--version", required=False, default=None, type = str, help="Human-friendly version name")(f)
    #f = click.option("--license", required=False, default=None, help="License of the file")(f)
#    return f


@initialize_pusher_cli.command()
@base_ressource
@click.option('--voxels_resolution', required=True, default=None, help="The Allen annotation volume "\
              "voxels resolution in microns")
@click.pass_context
@log_args(L)
def push_volumetric(ctx, dataset_path, voxels_resolution, config, hierarchy_path):
    """Create a VolumetricDataLayer resource payload and push it along with the corresponding 
    volumetric input dataset files into Nexus.\n
    """
    L.info("Filling the metadata of the volumetric payloads...")
    dataset = createNrrdResources(
                            ctx.obj['forge'], 
                            dataset_path, 
                            voxels_resolution,
                            config,
                            hierarchy_path
                            )
    if dataset:
        _push_to_Nexus(dataset, 
                        ctx.obj['forge'],
                        "https://neuroshapes.org/dash/volumetricdatalayer")

@initialize_pusher_cli.command()
@base_ressource
@click.pass_context
@log_args(L)
def push_meshes(ctx, dataset_path, config, hierarchy_path):
    """Create a Mesh resource payload and push it along with the corresponding brain .OBJ mesh 
    folder input dataset files into Nexus.\n
    """
    L.info("Filling the metadata of the mesh payloads...")     
    dataset = createMeshResources(
                            ctx.obj['forge'], 
                            dataset_path,
                            config,
                            hierarchy_path
                            )
    if dataset:    
        _push_to_Nexus(dataset, 
                        ctx.obj['forge'],
                        "https://neuroshapes.org/dash/brainparcellationmesh")


@initialize_pusher_cli.command()
@base_ressource
@click.pass_context
@log_args(L)
def push_cellpositions():
    pass



def start():
    initialize_pusher_cli(obj={})

if __name__ == '__main__':
    start()