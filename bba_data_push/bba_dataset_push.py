"""
Create resource payload and push them along with the corresponding dataset files into
Nexus. If the Resource already exists in Nexus then update it instead. Eventually
push/update their linked atlasRelease and ontology resources. Tag all these resources
with the input tag or, if not provided, with a timestamp.
Each CLI can process multiple files/directories at once.
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
from bba_data_push.push_cellComposition import create_densityPayloads, create_cellCompositionVolume, create_cellCompositionSummary, create_cellComposition
from bba_data_push.push_cellComposition import COMP_SCHEMA
from bba_data_push import constants as const
from bba_data_push.logging import log_args, close_handler
from bba_data_push import __version__

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)


def _integrate_datasets_to_Nexus(forge, datasets_toupdate, datasets_topush, tag):

    ############
    # TEMPORARY: change the mba jsonLD file name from the ontology distribution to
    # update so that the resource is compliant with the Atlas web app ontology file
    # extraction
    try:
        ontology_distributions = datasets_toupdate[
            "https://neuroshapes.org/dash/ontology"
        ][-1].distribution
        ontology_id = datasets_toupdate["https://neuroshapes.org/dash/ontology"][-1].id
        mba_jsonld = const.hierarchy_dict["hierarchy_l23split"]["mba_jsonld"]
        for i in range(0, len(ontology_distributions)):
            if ontology_distributions[i].name == f"{mba_jsonld}.json":
                uid = f"{ontology_id}".rsplit("/", 1)[-1]
                ontology_distributions[i].name = "_".join(
                    [uid, ontology_distributions[i].name]
                )
                pass
    except AttributeError:
        pass
    except KeyError:
        pass
    except IndexError:
        pass
    ###########
    for dataset_schema, datasets in datasets_toupdate.items():
        if datasets:
            dataset_type = f"{dataset_schema}".rsplit("/", 1)[-1]
            if not dataset_type:
                dataset_type = "Entity"
            try:
                L.info(
                    "\n-------------- Update & Validation Status ---------------"
                    f"\nUpdating '{dataset_type}' resource payloads in Nexus..."
                )
                L.info("\nUpdating %d resources with schema %s:" % (len(datasets), dataset_schema))
                #forge.update(datasets, dataset_schema)
                L.info(
                    f"<<Resource synchronization status>>:"
                    f" {str(datasets[-1]._synchronized)}"
                )
                if tag:
                    try:
                        L.info("Tagging %d with tag %s" % (len(datasets), tag))
                        #forge.tag(datasets, tag)
                    except Exception as e:
                        L.error(f"Error when tagging the resource. {e}")
                        exit(1)
            except Exception as e:
                L.error(f"Error when updating the resource. {e}")
                exit(1)

    for dataset_schema, datasets in datasets_topush.items():
        if datasets:
            dataset_type = f"{dataset_schema}".rsplit("/", 1)[-1]
            if not dataset_type:
                dataset_type = "Entity"
            try:
                L.info(
                    "\n----------------------- Resource content ----------------------"
                    f"\n{datasets[-1]}"
                    "\n-------------- Registration & Validation Status ---------------"
                    f"\nRegistering the constructed  '{dataset_type}' resource payload "
                    "along the input dataset in Nexus..."
                )
                L.info("\nRegistering %d resources with schema %s, first is:\n%s" % (len(datasets), dataset_schema, datasets[0]))
                forge.register(datasets, dataset_schema)
                L.info(
                    f"<<Resource synchronization status>>: "
                    f"{str(datasets[-1]._synchronized)}"
                )
                if tag:
                    try:
                        L.info("Tagging %d resources with tag %s" % (len(datasets), tag))
                        #forge.tag(datasets, tag)
                    except Exception as e:
                        L.error(f"Error when tagging the resource. {e}")
                        exit(1)
            except Exception as e:
                L.error(f"Error when registering the resource. {e}")
                exit(1)


def _push_activity_to_Nexus(activity_resource, forge):

    endedAtTime = {
        "@type": "xsd:dateTime",
        "@value": f"{datetime.today().strftime('%Y-%m-%dT%H:%M:%S')}",
    }
    if activity_resource._store_metadata:
        try:
            activity_resource.endedAtTime = forge.from_json(
                {
                    "@type": activity_resource.endedAtTime.type,
                    "@value": endedAtTime["@value"],
                }
            )
            L.info("\nUpdating the Activity Resource in Nexus...")
            #forge.update(activity_resource)
        except Exception as e:
            L.error(f"Error when updating the resource. {e}")
            exit(1)
    else:
        try:
            activity_resource.endedAtTime = endedAtTime
            L.info("\nRegistering the constructed Activity Resource in Nexus...")
            L.info("activity_resource:", activity_resource)
            #forge.register(activity_resource, const.schema_activity)
        except Exception as e:
            L.error(f"Error when registering the resource. {e}")
            exit(1)


def validate_token(ctx, param, value):
    if len(value) < 1:
        raise click.BadParameter("The string provided is empty'")
    else:
        return value

@click.group()
@click.version_option(__version__)
@click.option("-v", "--verbose", count=True)
@click.option("--forge-config-file",
    type = click.Path(),
    default = ("https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/"
        "notebooks/use-cases/prod-forge-nexus.yml"),
    help = "Path to the configuration file used to instantiate the Forge",)
@click.option("--nexus-env",
    default = "prod",
    help = "Nexus environment to use, can be 'dev',"
    "'staging', 'prod' or the URL of a custom environment",)
@click.option("--nexus-org", default="bbp", help="The Nexus organisation to push into")
@click.option("--nexus-proj", default="atlas", help="The Nexus project to push into")
@click.option("--nexus-token",
    type = click.STRING,
    callback = validate_token,
    required = True,
    help = "Value of the Nexus token",)
@click.pass_context
@log_args(L)
def initialize_pusher_cli(
    ctx, verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token
):
    forge, verbose_L = _initialize_pusher_cli(verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token)
    ctx.obj["forge"] = forge
    ctx.obj["verbose"] = verbose_L

def _initialize_pusher_cli(
    verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token
):
    """Run the dataset pusher CLI starting by the Initialisation of the Forge python
    framework to communicate with Nexus.\n
    The Forge will enable to build and push into Nexus the metadata payload along with
    the input dataset.
    """
    L.setLevel((logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)])

    default_environments = {
        "dev": "https://dev.nexus.ocp.bbp.epfl.ch/v1",
        "staging": "https://staging.nise.bbp.epfl.ch/nexus/v1",
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
            forge_config_file, endpoint=nexus_env, bucket=bucket, token=nexus_token)
    except Exception as e:
        L.error(f"Error when initializing the forge. {e}")
        exit(1)

    close_handler(L)

    return forge, L.level

def base_resource(f):
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
        "--hierarchy-path",
        type=click.Path(exists=True),
        required=True,
        multiple=True,
        help="The path to the json hierarchy file containing an AIBS hierarchy "
        "structure.",
    )(f)
    f = click.option(
        "--hierarchy-jsonld-path",
        type=click.Path(exists=True),
        help="Path to the AIBS hierarchy structure as a JSON-LD file. It is mandatory "
        "in case of the creation of a new Ontology resource as it will be attached to "
        "it when integrated in the knowledge graph. New Ontology resource is created "
        "at the same time a new atlasRelease resource need to be created.",
    )(f)
    f = click.option(
        "--atlasrelease-config-path",
        required=True,
        help="Json file containing the atlasRelease @id as well as its ontology and "
        "parcellation volume @id. It needs to contains at least these informations for "
        "the atlasRelease Allen Mouse CCFV2 and CCFV3 stocked in the Nexus project "
        "bbp/atlas.",
    )(f)
    f = click.option(
        "--provenance-metadata-path",
        type=click.Path(exists=True),
        help="Json file containing metadata for the derivation properties as well as "
        "the Activity and SoftwareAgent resources.",
    )(f)
    f = click.option(
        "--resource-tag",
        help="Optional tag value with which to tag the resources",
    )(f)

    return f


@initialize_pusher_cli.command()
@base_resource
@click.option(
    "--link-regions-path",
    help="Optional json file containing link between regions and resources  (@ ids of "
    "mask and mesh for each brain region) to be extracted by the CLI "
    "push-regionsummary. If the file already exists it will be annotated else it will "
    "be created.",
)
@click.option(
    "--new-atlas",
    type=bool,
    required=True,
    help="Flag to trigger the creation of a new atlas release Resource.",
)
@click.pass_context
@log_args(L)
def push_volumetric(
    ctx,
    dataset_path,
    config_path,
    new_atlas,
    atlasrelease_config_path,
    hierarchy_path,
    hierarchy_jsonld_path,
    provenance_metadata_path,
    link_regions_path,
    resource_tag,
):
    """Create a VolumetricDataLayer resource payload and push it along with the "
    corresponding volumetric input dataset files into Nexus. If the Resource already
    exists in Nexus then it will be updated instead. Eventually push/update their
    linked atlasRelease and ontology resources. Tag all these resources with the input
    tag or, if not provided, with a timestamp\n
    """
    L.setLevel(ctx["verbose"])
    L.info("Filling the metadata of the volumetric payloads...")
    resources_payloads = create_volumetric_resources(
        ctx["forge"],
        dataset_path,
        config_path,
        new_atlas,
        atlasrelease_config_path,
        hierarchy_path,
        hierarchy_jsonld_path,
        provenance_metadata_path,
        link_regions_path,
        resource_tag,
        ctx["verbose"],
    )

    #print("\nresources_payloads[\"activity\"]: ", resources_payloads["activity"])
    if resources_payloads["activity"]:
        _push_activity_to_Nexus(resources_payloads["activity"], ctx["forge"])

    _integrate_datasets_to_Nexus(
        ctx["forge"],
        resources_payloads["datasets_toUpdate"],
        resources_payloads["datasets_toPush"],
        resources_payloads["tag"],
    )


@initialize_pusher_cli.command()
@base_resource
@click.option(
    "--link-regions-path",
    help="Optional json file containing link between regions and resources  (@ ids of "
    "mask and mesh for each brain region) to be extracted by the CLI "
    "push-regionsummary. If the file already exists it will be annoted else it will be "
    "created.",
)
@click.pass_context
@log_args(L)
def push_meshes(
    ctx,
    dataset_path,
    config_path,
    atlasrelease_config_path,
    hierarchy_path,
    hierarchy_jsonld_path,
    provenance_metadata_path,
    link_regions_path,
    resource_tag,
):
    """Create a Mesh resource payload and push it along with the corresponding brain
    .OBJ mesh folder input dataset files into Nexus. If the Resource already exists in
    Nexus then it will be updated instead. Eventually push/update their linked
    atlasRelease and ontology resources. Tag all these resources with the input tag or,
    if not provided, with a timestamp\n
    """
    L.setLevel(ctx["verbose"])
    L.info("Filling the metadata of the mesh payloads...")
    resources_payloads = create_mesh_resources(
        ctx["forge"],
        dataset_path,
        config_path,
        atlasrelease_config_path,
        hierarchy_path,
        hierarchy_jsonld_path,
        provenance_metadata_path,
        link_regions_path,
        resource_tag,
        ctx["verbose"],
    )

    if resources_payloads["activity"]:
        _push_activity_to_Nexus(resources_payloads["activity"], ctx["forge"])

    _integrate_datasets_to_Nexus(
        ctx["forge"],
        resources_payloads["datasets_toUpdate"],
        resources_payloads["datasets_toPush"],
        resources_payloads["tag"],
    )


@initialize_pusher_cli.command()
@base_resource
@click.option(
    "--link-regions-path",
    help="Optional json file containing link between regions and resources  (@ ids of "
    "mask and mesh for each brain region) to be extracted by the CLI "
    "push-regionsummary. If the file already exists it will be annoted else it will be "
    "created.",
)
@click.pass_context
@log_args(L)
def push_regionsummary(
    ctx,
    dataset_path,
    config_path,
    atlasrelease_config_path,
    hierarchy_path,
    hierarchy_jsonld_path,
    provenance_metadata_path,
    link_regions_path,
    resource_tag,
):
    """Create a RegionSummary resource payload and push it along with the corresponding
    brain region metadata json input dataset files into Nexus. If the Resource already
    exists in Nexus then it will be updated instead. Eventually push/update their
    linked atlasRelease and ontology resources. Tag all these resources with the input
    tag or, if not provided, with a timestamp\n
    """
    L.setLevel(ctx["verbose"])
    L.info("Filling the metadata of the RegionSummary payload...")
    resources_payloads = create_regionsummary_resources(
        ctx["forge"],
        dataset_path,
        config_path,
        atlasrelease_config_path,
        hierarchy_path,
        hierarchy_jsonld_path,
        provenance_metadata_path,
        link_regions_path,
        resource_tag,
        ctx["verbose"],
    )

    if resources_payloads["activity"]:
        _push_activity_to_Nexus(resources_payloads["activity"], ctx["forge"])

    _integrate_datasets_to_Nexus(
        ctx["forge"],
        resources_payloads["datasets_toUpdate"],
        resources_payloads["datasets_toPush"],
        resources_payloads["tag"],
    )


@initialize_pusher_cli.command()
@base_resource
@click.pass_context
@log_args(L)
def push_cellrecords(
    ctx,
    dataset_path,
    config_path,
    atlasrelease_config_path,
    hierarchy_path,
    hierarchy_jsonld_path,
    provenance_metadata_path,
    resource_tag,
):
    """Create a CellRecordSerie resource payload and push it along with the
    corresponding Sonata hdf5 file input dataset files into Nexus. If the Resource
    already exists in Nexus then it will be updated instead. Eventually push/update
    their linked atlasRelease and ontology resources. Tag all these resources with the
    input tag or, if not provided, with a timestamp\n
    """
    L.setLevel(ctx["verbose"])
    L.info("Filling the metadata of the CellRecord payloads...")
    resources_payloads = create_cell_record_resources(
        ctx["forge"],
        dataset_path,
        config_path,
        atlasrelease_config_path,
        hierarchy_path,
        hierarchy_jsonld_path,
        provenance_metadata_path,
        resource_tag,
        ctx["verbose"],
    )

    if resources_payloads["activity"]:
        _push_activity_to_Nexus(resources_payloads["activity"], ctx["forge"])

    _integrate_datasets_to_Nexus(
        ctx["forge"],
        resources_payloads["datasets_toUpdate"],
        resources_payloads["datasets_toPush"],
        resources_payloads["tag"],
    )


@initialize_pusher_cli.command()
@click.option(
    "--atlasrelease-id",
    type=click.STRING,
    required=True,
    multiple=False,
    help="The Nexus ID of the related AtlasRelease.")
@click.option(
    "--volume-path",
    type=click.Path(exists=True),
    required=True,
    multiple=False,
    help="The path to the json CellCompositionVolume file.")
@click.option(
    "--summary-path",
    type=click.Path(exists=True),
    required=False,
    multiple=False,
    help="The path to the json CellCompositionSummary file.")
@click.option(
    "--name",
    type=click.STRING,
    required=False,
    multiple=False,
    help="The name to assign to the CellComposition(Volume,Summary).")
@click.option(
    "--description",
    type=click.STRING,
    required=False,
    multiple=False,
    help="The description to assign to the CellComposition(Volume,Summary).")
@click.pass_context
@log_args(L)
def push_cellcomposition(
    ctx,
    atlasrelease_id,
    volume_path,
    summary_path,
    name, description,
    resource_tag=None
) -> str:
    """Create a CellComposition resource payload and push it along with the "
    corresponding CellCompositionVolume and CellCompositionSummary into Nexus.
    Tag all these resources with the input tag or, if not provided, with a timestamp\n
    """
    return push_cellcomposition_(ctx.obj["forge"], ctx.obj["verbose"], atlasrelease_id, volume_path, summary_path, name, description, resource_tag=None)

def push_cellcomposition_(forge, verbose, atlasrelease_id, volume_path, summary_path, name, description, resource_tag=None) -> str:
    cellComps = {"tag": resource_tag}
    resources_payloads = create_densityPayloads(forge,
        atlasrelease_id,
        volume_path,
        resource_tag,
        cellComps,
        verbose)

    _integrate_datasets_to_Nexus(forge,
        resources_payloads["datasets_toUpdate"],
        resources_payloads["datasets_toPush"],
        resources_payloads["tag"])

    create_cellCompositionVolume(forge,
        atlasrelease_id,
        volume_path,
        resources_payloads,
        name,
        description,
        resource_tag,
        cellComps,
        verbose)

    if summary_path:
        create_cellCompositionSummary(forge,
            atlasrelease_id,
            resources_payloads,
            summary_path,
            name,
            description,
            cellComps,
            verbose)

    _integrate_datasets_to_Nexus(forge,
        cellComps["datasets_toUpdate"],
        cellComps["datasets_toPush"],
        cellComps["tag"])

    create_cellComposition(forge,
        atlasrelease_id,
        resources_payloads,
        name,
        description,
        resource_tag,
        cellComps,
        verbose)

    _integrate_datasets_to_Nexus(forge,
        cellComps["datasets_toUpdate"],
        cellComps["datasets_toPush"],
        cellComps["tag"])

    if resources_payloads.get("activity"):
        _push_activity_to_Nexus(resources_payloads["activity"], forge)

    cellComp_id = ""
    if getattr(cellComps[COMP_SCHEMA], 'id', None):
        cellComp_id = cellComps[COMP_SCHEMA].id
    else:
        L.error(f"The following {COMP_SCHEMA} has no id, probably it has not been registered:\n{cellComps[COMP_SCHEMA]}")
    return cellComp_id

def start():
    initialize_pusher_cli(obj={})


if __name__ == "__main__":
    start()
