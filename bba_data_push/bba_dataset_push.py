""" 
Create resource payload and push them along with the corresponding dataset files into 
Nexus. Eventually return a JSON temporary file containing the Activity resource payload 
detailings the Atlas pipeline run specifications. This file can be provided as an input 
by the bba-dataset-push module that will erase it after use.
To know more about Nexus, see https://bluebrainnexus.io.
Link to BBP Atlas pipeline confluence documentation: 
https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""
import logging
import click
from datetime import datetime
from kgforge.core import KnowledgeGraphForge

from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources
from bba_data_push.push_brainmesh import create_mesh_resources
from bba_data_push.push_sonata_cellrecordseries import create_cell_record_resources
from bba_data_push.push_json_regionsummary import create_regionsummary_resources
from bba_data_push.logging import log_args, close_handler
from bba_data_push import __version__

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)


def _push_datasets_to_Nexus(datasets, forge, schema_id):
    L.info(
        "\n----------------------- Resource content ----------------------"
        f"\n{datasets[-1]}"
    )
    L.info(f"\n{datasets[0]}")
    try:
        L.info(
            "\n-------------- Registration & Validation Status ---------------"
            "\nRegistering the constructed resource payload along the input dataset in "
            "Nexus..."
        )
        forge.register(datasets, schema_id)
        L.info(
            f"<<Resource synchronization status>>: {str(datasets[-1]._synchronized)}"
        )
    except Exception as e:
        L.error(f"Error when registering the resource. {e}")


def _push_activity_to_Nexus(resources_dict, forge):

    endedAtTime = {
        "@type": "xsd:dateTime",
        "@value": f"{datetime.today().strftime('%Y-%m-%dT%H:%M:%S')}",
    }

    if resources_dict["activity"]._store_metadata:
        try:
            resources_dict["activity"].endedAtTime = forge.from_json(
                {
                    "@type": resources_dict["activity"].endedAtTime.type,
                    "@value": endedAtTime["@value"],
                }
            )
            L.info("\nUpdating the Activity Resource in Nexus...")
            forge.update(resources_dict["activity"])
        except Exception as e:
            L.error(f"Error when updating the resource. {e}")
            exit(1)
    else:
        try:
            resources_dict["activity"].endedAtTime = endedAtTime
            L.info("\nRegistering the constructed Activity Resource in Nexus...")
            forge.register(
                resources_dict["activity"], "https://neuroshapes.org/dash/activity"
            )
        except Exception as e:
            L.error(f"Error when registering the resource. {e}")
            exit(1)


@click.group()
@click.version_option(__version__)
@click.option("-v", "--verbose", count=True)
@click.option(
    "--forge-config-file",
    type=click.Path(),
    default=(
        "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/"
        "notebooks/use-cases/prod-forge-nexus.yml"
    ),
    help="Path to the configuration file used to instantiate the Forge",
)
@click.option(
    "--nexus-env",
    default="prod",
    help="Nexus environment to use, can be 'dev',"
    "'staging', 'prod' or the URL of a custom environment",
)
@click.option("--nexus-org", default="bbp", help="The Nexus organisation to push into")
@click.option("--nexus-proj", default="atlas", help="The Nexus project to push into")
@click.option(
    "--nexus-token",
    required=True,
    help="Value of the Nexus token",
)
@click.pass_context
@log_args(L)
def initialize_pusher_cli(
    ctx, verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token
):
    """Run the dataset pusher CLI starting by the Initialisation of the Forge python
    framework to communicate with Nexus.\n
    The Forge will enable to build and push into Nexus the metadata payload along with
    the input dataset.
    """
    L.setLevel((logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)])

    default_environments = {
        "dev": "https://dev.nexus.ocp.bbp.epfl.ch/v1",
        "staging": "https://staging.nexus.ocp.bbp.epfl.ch/v1",
        "prod": "https://bbp.epfl.ch/nexus/v1",
    }

    if nexus_env[-1] == "/":
        nexus_env = nexus_env[:-1]
    if nexus_env in default_environments:
        nexus_env = default_environments[nexus_env]
    elif nexus_env in default_environments.values():
        pass
    else:
        L.error(
            f"Error: {nexus_env} do not correspond to one of the 3 possible "
            "environment: dev, staging, prod"
        )
        exit(1)

    bucket = f"{nexus_org}/{nexus_proj}"
    try:
        L.info("Initializing the forge...")
        forge = KnowledgeGraphForge(
            forge_config_file, endpoint=nexus_env, bucket=bucket, token=nexus_token
        )
    except Exception as e:
        L.error(f"Error when initializing the forge. {e}")
        exit(1)

    ctx.obj["forge"] = forge
    ctx.obj["verbose"] = L.level

    close_handler(L)


def base_ressource(f):
    f = click.option(
        "--dataset-path",
        type=click.Path(exists=True),
        required=True,
        multiple=True,
        help="The files or directories of file to push on Nexus",
    )(f)
    f = click.option(
        "--config-path",
        type=click.Path(exists=True),
        required=True,
        help="Path to the generated dataset configuration file. This is a yaml file "
        "containing the paths to the Atlas pipeline generated dataset",
    )(f)
    f = click.option(
        "--provenance-metadata-path",
        type=click.Path(exists=True),
        help="Json file containing metadata for the derivation properties as well as "
        "the Activity and SoftwareAgent resources.",
    )(f)
    return f


@initialize_pusher_cli.command()
@base_ressource
@click.option(
    "--atlasrelease-id",
    type=str,
    help="The @id for the Atlasrelease resource. If empty, the @id will be "
    "automaticaly generated.",
)
@click.option(
    "--hierarchy-path",
    type=click.Path(exists=True),
    help="The path to the json hierarchy file containing an AIBS hierarchy structure.",
)
@click.option(
    "--voxels-resolution",
    required=True,
    help="The Allen annotation volume voxels resolution in microns",
)
@click.option(
    "--link-regions-path",
    help="Optional json file containing link between regions and resources  (@ ids of "
    "mask, mesh and atlas release resources) to be extracted by the CLI "
    "push-regionsummary. If the file already exists it will be annoted else it will be "
    "created.",
)
@click.pass_context
@log_args(L)
def push_volumetric(
    ctx,
    dataset_path,
    voxels_resolution,
    config_path,
    atlasrelease_id,
    hierarchy_path,
    link_regions_path,
    provenance_metadata_path,
):
    """Create a VolumetricDataLayer resource payload and push it along with the "
    corresponding volumetric input dataset files into Nexus.\n
    """
    L.setLevel(ctx.obj["verbose"])
    L.info("Filling the metadata of the volumetric payloads...")
    resources_dict = create_volumetric_resources(
        ctx.obj["forge"],
        dataset_path,
        voxels_resolution,
        config_path,
        atlasrelease_id,
        hierarchy_path,
        link_regions_path,
        provenance_metadata_path,
        ctx.obj["verbose"],
    )
    if resources_dict["atlasreleases"]:
        try:
            L.info(
                "\nRegistering the constructed BrainAtlasRelease resources in Nexus..."
            )
            ctx.obj["forge"].register(
                resources_dict["atlasreleases"],
                "https://neuroshapes.org/dash/atlasrelease",
            )
        except Exception as e:
            L.error(f"Error when registering the resource. {e}")
            exit(1)

    if resources_dict["hierarchy"]:
        try:
            L.info("\nRegistering the constructed Parcellation ontology in Nexus...")
            ctx.obj["forge"].register(
                resources_dict["hierarchy"], "https://neuroshapes.org/dash/ontology"
            )
        except Exception as e:
            L.error(f"Error when registering the resource. {e}")
            exit(1)

    if resources_dict["activity"]:
        _push_activity_to_Nexus(resources_dict, ctx.obj["forge"])

    if resources_dict["datasets"]:
        _push_datasets_to_Nexus(
            resources_dict["datasets"],
            ctx.obj["forge"],
            "https://neuroshapes.org/dash/volumetricdatalayer",
        )


@initialize_pusher_cli.command()
@base_ressource
@click.option(
    "--hierarchy-path",
    type=click.Path(exists=True),
    required=True,
    multiple=True,
    help="The path to the json hierarchy file containing an AIBS hierarchy structure.",
)
@click.option(
    "--voxels-resolution",
    required=True,
    help="The Allen annotation volume voxels resolution in microns",
)
@click.option(
    "--link-regions-path",
    help="Optional json file containing link between regions and resources  (@ ids of "
    "mask, mesh and atlas release resources) to be extracted by the CLI "
    "push-regionsummary. If the file already exists it will be annoted else it will be "
    "created.",
)
@click.pass_context
@log_args(L)
def push_meshes(
    ctx,
    dataset_path,
    config_path,
    hierarchy_path,
    voxels_resolution,
    link_regions_path,
    provenance_metadata_path,
):
    """Create a Mesh resource payload and push it along with the corresponding brain
    .OBJ mesh folder input dataset files into Nexus.\n
    """
    L.setLevel(ctx.obj["verbose"])
    L.info("Filling the metadata of the mesh payloads...")
    resources_dict = create_mesh_resources(
        ctx.obj["forge"],
        dataset_path,
        config_path,
        hierarchy_path,
        voxels_resolution,
        link_regions_path,
        provenance_metadata_path,
        ctx.obj["verbose"],
    )
    if resources_dict["atlasreleases"]:
        try:
            L.info(
                "\nRegistering the constructed BrainAtlasRelease resources in Nexus..."
            )
            ctx.obj["forge"].register(
                resources_dict["atlasreleases"],
                "https://neuroshapes.org/dash/atlasrelease",
            )
        except Exception as e:
            L.error(f"Error when registering the resource. {e}")
            exit(1)

    if resources_dict["activity"]:
        _push_activity_to_Nexus(resources_dict, ctx.obj["forge"])

    if resources_dict["datasets"]:
        _push_datasets_to_Nexus(
            resources_dict["datasets"],
            ctx.obj["forge"],
            "https://neuroshapes.org/dash/brainparcellationmesh",
        )


@initialize_pusher_cli.command()
@base_ressource
@click.pass_context
@log_args(L)
def push_cellrecords(
    ctx,
    dataset_path,
    voxels_resolution,
    config_path,
    provenance_metadata_path,
):
    """Create a CellRecordSerie resource payload and push it along with the
    corresponding Sonata hdf5 file input dataset files into Nexus.\n
    """
    L.setLevel(ctx.obj["verbose"])
    L.info("Filling the metadata of the CellRecord payloads...")
    resources_dict = create_cell_record_resources(
        ctx.obj["forge"],
        dataset_path,
        voxels_resolution,
        config_path,
        provenance_metadata_path,
        ctx.obj["verbose"],
    )

    if resources_dict["activity"]:
        _push_activity_to_Nexus(resources_dict, ctx.obj["forge"])

    if resources_dict["datasets"]:
        _push_datasets_to_Nexus(
            resources_dict["datasets"],
            ctx.obj["forge"],
            "https://neuroshapes.org/dash/cellrecordseries",
        )


@initialize_pusher_cli.command()
@base_ressource
@click.option(
    "--hierarchy-path",
    type=click.Path(exists=True),
    required=True,
    multiple=True,
    help="The path to the json hierarchy file containing an AIBS hierarchy structure.",
)
@click.option(
    "--link-regions-path",
    required=True,
    help="Json file containing link between regions and resources  (@ ids of "
    "mask, mesh and atlas release resources) to be extracted.",
)
@click.pass_context
@log_args(L)
def push_regionsummary(
    ctx,
    dataset_path,
    config_path,
    hierarchy_path,
    link_regions_path,
    provenance_metadata_path,
):
    """Create a RegionSummary resource payload and push it along with the corresponding
    brain region metadata json input dataset files into Nexus.\n
    """
    L.setLevel(ctx.obj["verbose"])
    L.info("Filling the metadata of the RegionSummary payload...")
    resources_dict = create_regionsummary_resources(
        ctx.obj["forge"],
        dataset_path,
        config_path,
        hierarchy_path,
        link_regions_path,
        provenance_metadata_path,
        ctx.obj["verbose"],
    )

    if resources_dict["activity"]:
        _push_activity_to_Nexus(resources_dict, ctx.obj["forge"])

    if resources_dict["datasets"]:
        _push_datasets_to_Nexus(
            resources_dict["datasets"],
            ctx.obj["forge"],
            "",
        )  # https://neuroshapes.org/dash/entity


def start():
    initialize_pusher_cli(obj={})


if __name__ == "__main__":
    start()
