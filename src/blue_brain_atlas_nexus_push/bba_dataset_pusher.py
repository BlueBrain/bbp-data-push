#import logging
import click
from kgforge.core import KnowledgeGraphForge

from push_nrrd_volumetricdatalayer import createResource, addSpatialRefSystem, addNrrdProps
from push_brainmesh import createDataset
from commons import getExtraValues, addContribution 

# Warning: Required parameters have precedence over subcommands --help option
@click.group()
@click.option( '--forge_config_file','-config', required=True, type=click.Path(exists=True), default=None, help='The configuration file used to instantiate the Forge')
@click.option("--org", required=True,  default='bbp', help="The Nexus organization to push into")
@click.option("--proj", required=True,  default='atlas', help="The Nexus project to push into")
@click.option("--env", required=True,  default="staging", help="Nexus environment to use, can be 'dev', 'staging', 'prod' or the URL of a custom env")
@click.option("--token", required=True,  default=None, help="copy pasted from Nexus fusion")
@click.pass_context
def initialize_pusher_cli(ctx, forge_config_file, org, proj, env, token):
    default_environments = {
    "dev": "https://dev.nexus.ocp.bbp.epfl.ch/v1",
    "staging": 'https://staging.nexus.ocp.bbp.epfl.ch/v1',
    "prod": "https://bbp.epfl.ch/nexus/v1"
    }
    if env in default_environments:
        env = default_environments[env]
    bucket = org + '/' + proj
    forge = KnowledgeGraphForge(forge_config_file, endpoint = env, 
                                           bucket = bucket, token = token)
    ctx.obj['forge'] = forge


def base_ressource(f):
    f = click.option('--description', "-desc", required=False, default=None, help="The description of the volumetric dataset being pushed")(f)
    f = click.option('--spatial_unit', '-spatialunit', required=False, default = 'µm', type = str, help='The spatial unit used for world coordinates')(f)
# These arguments should be automatically resolved:
    f = click.option('--id_atlas_release', '-idatlasrelease', required=True, default = None, help="The @id of the atlas release")(f)
    f = click.option('--id_spatial_ref', '-idspatialref', required=True, default = 'allen_ccfv3_spatial_reference_system', help="The @id to the atlas spatial reference system used with this dataset")(f)
    return f

def extra_arguments(f):
    f = click.option('--extra_keys_values', "-extrakv", required=False, multiple=True, default=[], type = (str, str), help="Additionnal property couple key/value to add to the metadata")(f)
    f = click.option("--contributor_name", '-contrib', required=False, default=[], type = str, help="Nexus ID of contributing organizations")(f) #multiple = True
    f = click.option("--version", required=False, default=None, type = str, help="Human-friendly version name")(f)
    #f = click.option("--license", required=False, default=None, help="License of the file")(f)

    return f
 

# def a verbose command
# add logger

@initialize_pusher_cli.command()
@base_ressource
@extra_arguments
# The proper option 'nrrd_nexus_id' to update an already existing file in Nexus would be a custom click Class to handle mutually exclusive options (even if it would be a bit heavy). But not mandatory for now as every datasets pushed (new or refined versions) will go through a proper integration on Nexus by DKE.
# @click.option('--nrrd_nexus_id', '-idnf', required=False, default=None, help='The ID of the NRRD file already into Nexus. Alternative to -f')
@click.option( '--nrrd_local_path','-f', required=True, type=click.Path(exists=True), default = [], help='The NRRD file to push to Nexus')

# These arguments will be automatically resolved
@click.option('--brain_region_id', '-brainregion', required=False, default='997', help="Brain region id as defined by Allen. This is an integer number (default: 997 --> root, whole brain)")
@click.option('--id_file', "-idfile", required=False, default=None, help="The @id to give to the file pushed (default: automatically created by Nexus)")
@click.option('--id_vol', "-idvol", required=False, default=None,  help="The @id to give to this VolumetricDataLayer resource created alongside the file (default: automatically created by Nexus)")

@click.option('--voxel_type', "-voxeltype", required=True, default=None,
              type = click.Choice(['multispectralIntensity', 'vector', 'intensity','mask', 'label', ], 
                                  case_sensitive=False), 
              help="Type of voxel. Can be 'intensity', 'label' or 'mask' for single-component voxels and 'multispectralIntensity' or 'vector' for multi-component voxels. (default: intensity and vector)")

@click.option('--extra_types', "-extratypes", required=True, 
              type = click.Choice(['CellDensityDataLayer', 'GliaCellDensity', 'PHDataLayer',
                                   'BrainParcellationDataLayer', 'MorphologyOrientationDataLayer', 
                                   'NISSLImageDataLayer', 'TwoPhotonImageDataLayer', 
                                   'BrainTemplateDataLayer', 
                                   'GeneExpressionVolumetricDataLayer'], case_sensitive=False), 
              multiple = True, default=[], help="The volume will be pushed as a 'VolumetricDataLayer', but it can have additionnal types.")
@click.pass_context
def push_volumetric(ctx, nrrd_local_path, spatial_unit, brain_region_id, id_atlas_release, id_vol,
                    id_spatial_ref, voxel_type, description,  extra_types, **kwargs):
            
    dataset, header, config = createResource(ctx.obj['forge'], nrrd_local_path, extra_types, id_vol, spatial_unit)
    
    dataset_with_SpatialRef = addSpatialRefSystem(dataset, ctx.obj['forge'], brain_region_id, id_spatial_ref,
                                                  id_atlas_release, description)

    dataset_with_NrrdProps  = addNrrdProps(dataset_with_SpatialRef, header, config, voxel_type)
    
    if kwargs["extra_keys_values"]:
        dataset_with_NrrdProps = getExtraValues(dataset_with_NrrdProps, kwargs["extra_keys_values"])
    
    if kwargs["contributor_name"]:
        dataset_with_NrrdProps = addContribution(dataset_with_NrrdProps, ctx.obj['forge'], kwargs["contributor_name"])
    
    print("----------------------- Resource content ----------------------")
    print(dataset_with_NrrdProps)
    
    try:
        print("-------------- Registration & Validation Status ---------------")
        # forge.validate doesn't work with multi-types resource
        #ctx.obj['forge'].validate(dataset_with_NrrdProps, execute_actions_before=True) 
        # if dataset_with_NrrdProps._validated:    
        ctx.obj['forge'].register(dataset_with_NrrdProps, schema_id = 'https://neuroshapes.org/dash/volumetricdatalayer')
        print('<<Resource synchronization status>>: ' + str(dataset_with_NrrdProps._synchronized))
        if kwargs["version"]:
            ctx.obj['forge'].tag(dataset_with_NrrdProps, kwargs["version"])
        # else: 
        #     pass
    except Exception as e:
        print(e)
        print("---------------------------------------------------------------")

        
@initialize_pusher_cli.command()
@base_ressource
@extra_arguments
@click.option( '--mesh_local_path','-f', required=True, type=click.Path(exists=True), multiple = True, default = [], help='The Brain meshes files to push to Nexus')
@click.pass_context
def push_meshes(ctx, mesh_local_path, description, id_spatial_ref, id_atlas_release, 
                spatial_unit, extra_keys_values, **kwargs):
   
    dataset = createDataset(ctx.obj['forge'], mesh_local_path, description, id_spatial_ref, 
                            id_atlas_release, spatial_unit)
    
    print("----------------------- Resource content ----------------------")
    print(dataset[-1])    
    try:
        print("-------------- Registration & Validation Status ---------------")
        # forge.validate doesn't work with list
        #ctx.obj['forge'].validate(dataset[-1], execute_actions_before=True)
        #if dataset[-1]._validated:    
        ctx.obj['forge'].register(dataset, schema_id = 'https://neuroshapes.org/dash/brainparcellationmesh')
        print('<<Resource synchronization status>>: ' + str(dataset[-1]._synchronized))
        #else:
         #   exit()
    except Exception as e:
        print(e)
        print("---------------------------------------------------------------")

@initialize_pusher_cli.command()
@base_ressource
@extra_arguments
@click.pass_context
def push_cellposition():
    pass



def start():
    initialize_pusher_cli(obj={})

if __name__ == '__main__':
    start()