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
from kgforge.core import KnowledgeGraphForge

from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources
from bba_data_push.push_brainmesh import create_mesh_resources
from bba_data_push.push_sonata_cellrecordseries import create_cell_record_resources
from bba_data_push.logging import log_args, close_handler
from bba_data_push import __version__

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)


def _push_to_Nexus(datasets, forge, schema_id):
    L.info(
        "\n----------------------- Resource content ----------------------"
        f"\n{datasets[-1]}"
    )
    try:
        L.info(
            "\n-------------- Registration & Validation Status ---------------"
            "\nRegistering the constructed payload along the input dataset in Nexus..."
        )
        forge.register(datasets, schema_id)
        L.info(
            f"<<Resource synchronization status>>: {str(datasets[-1]._synchronized)}"
        )
    except Exception as e:
        L.error(f"Error when registering resource. {e}")


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
        "--config",
        type=click.Path(exists=True),
        required=True,
        help="Path to the generated dataset configuration file. This is a yaml file "
        "containing the paths to the Atlas pipeline generated dataset",
    )(f)
    f = click.option(
        "--provenances",
        type=str,
        multiple=True,
        default=[None],
        help="Strings containing the name and version of the module that generated the "
        "dataset. They must follow the form '<module_name>:<anything> <version>'.",
    )(f)
    # f = click.option("--activity-id", required=True, type=str, help="string which "
    #                   "corresponds to the Nexus identifier of the Activity Resource. "
    #                   "If the Activity Resource already exists in Nexus it will be "
    #                   "linked to the datasets to be pushed else the Activity will be "
    #                   "created and pushed too. ")(f)
    return f


@initialize_pusher_cli.command()
@base_ressource
@click.option(
    "--voxels-resolution",
    required=True,
    help="The Allen annotation volume voxels resolution in microns",
)
@click.option(
    "--new-atlasrelease-hierarchy-path",
    type=click.Path(exists=True),
    help="The path to the json hierarchy file containing an AIBS hierarchy "
    "structure corresponding to the new Atlas Release payload to push."
    "In fact, if provided, a new AtlasRelease resource payload will be created as well "
    "as a ParcellationOntology resource with the input hierarchy file attached. The "
    "AtlasRelease payload will then be linked to the  ParcellationOntology resource "
    "created and will itself be referenced by the others resources created from the "
    "input datasets. Finally the atlas release and the hierarchy file will be pushed "
    "along the others resources.",
)
@click.pass_context
@log_args(L)
def push_volumetric(
    ctx,
    dataset_path,
    voxels_resolution,
    config,
    provenances,
    new_atlasrelease_hierarchy_path,
):
    """Create a VolumetricDataLayer resource payload and push it along with the "
    corresponding volumetric input dataset files into Nexus.\n
    """
    L.setLevel(ctx.obj["verbose"])
    L.info("Filling the metadata of the volumetric payloads...")
    datasets, atlasreleases = create_volumetric_resources(
        ctx.obj["forge"],
        dataset_path,
        voxels_resolution,
        config,
        provenances,
        new_atlasrelease_hierarchy_path,
        ctx.obj["verbose"],
    )
    if atlasreleases["atlas_releases"]:
        try:
            L.info(
                "\nRegistering the constructed BrainAtlasRelease payloads in Nexus..."
            )
            ctx.obj["forge"].register(
                atlasreleases["atlas_releases"],
                "https://neuroshapes.org/dash/atlasrelease",
            )
        except Exception as e:
            L.error(f"Error when registering resource. {e}")
            exit(1)
    if atlasreleases["hierarchy"]:
        try:
            L.info(
                "\nRegistering the constructed Parcellation ontology payload in "
                "Nexus..."
            )
            ctx.obj["forge"].register(
                atlasreleases["hierarchy"], "https://neuroshapes.org/dash/ontology"
            )
        except Exception as e:
            L.error(f"Error when registering resource. {e}")
            exit(1)

    if datasets:
        _push_to_Nexus(
            datasets,
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
@click.pass_context
@log_args(L)
def push_meshes(ctx, dataset_path, config, hierarchy_path, provenances):
    """Create a Mesh resource payload and push it along with the corresponding brain
    .OBJ mesh folder input dataset files into Nexus.\n
    """
    L.setLevel(ctx.obj["verbose"])
    L.info("Filling the metadata of the mesh payloads...")
    datasets, atlasreleases = create_mesh_resources(
        ctx.obj["forge"],
        dataset_path,
        config,
        hierarchy_path,
        provenances,
        ctx.obj["verbose"],
    )
    if atlasreleases:
        try:
            L.info(
                "\nRegistering the constructed BrainAtlasRelease payloads in Nexus..."
            )
            ctx.obj["forge"].register(
                atlasreleases, "https://neuroshapes.org/dash/atlasrelease"
            )
        except Exception as e:
            L.error(f"Error when registering resource. {e}")
            exit(1)
    if datasets:
        _push_to_Nexus(
            datasets,
            ctx.obj["forge"],
            "https://neuroshapes.org/dash/brainparcellationmesh",
        )


@initialize_pusher_cli.command()
@base_ressource
@click.option(
    "--voxels-resolution",
    required=True,
    help="The Allen annotation volume voxels resolution in microns",
)
@click.pass_context
@log_args(L)
def push_cellrecords(ctx, dataset_path, voxels_resolution, config, provenances):
    """Create a CellRecordSerie resource payload and push it along with the
    corresponding Sonata hdf5 file input dataset files into Nexus.\n
    """
    L.setLevel(ctx.obj["verbose"])
    L.info("Filling the metadata of the CellRecord payloads...")
    datasets = create_cell_record_resources(
        ctx.obj["forge"],
        dataset_path,
        voxels_resolution,
        config,
        provenances,
        ctx.obj["verbose"],
    )
    if datasets:
        _push_to_Nexus(
            datasets, ctx.obj["forge"], "https://neuroshapes.org/dash/cellrecordseries"
        )


def start():
    initialize_pusher_cli(obj={})


if __name__ == "__main__":
    start()
