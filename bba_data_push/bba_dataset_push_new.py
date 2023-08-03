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

from bba_data_push.push_nrrd_volumetricdatalayer_new import create_volumetric_resources
from bba_data_push.logging import log_args, close_handler, create_log_handler
import bba_data_push.commons_new as comm

from bba_data_push import __version__

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_tag(forge, res_id, tag):
    res = forge.retrieve(res_id, version=tag)
    if res:
        msg = f"Tag '{tag}' already exists for res id '{res_id}' (revision {res._store_metadata._rev}), please choose a different tag."
        msg += " No resource with this schema has been tagged."
        raise Exception(msg)

class TypePropertiesMap:
    # Dict of {resource_type: resource_property} to check for a pair of resources:
    # each property is the key to the list of attributes to check (empty list means the whole property)
    prop = {
        comm.meType: {
            "brainLocation": ["brainRegion", "layer"],
            "annotation": []},
    }

type_for_schema = {
    comm.meType: "VolumetricDataLayer"}

def match_properties(res_json, existing_ress, forge, props_to_check):
    """Check whether a local Resource matches any Resource in the provided list.
    The dictionary of properties that define the matching is passed as last argument.

    Parameters
    ----------
    res_json: dict
        payload of the local Resource
    existing_ress: list
        list of Resources fetched from Nexus
    forge: KnowledgeGraphForge
        instance of forge
    props_to_check: dict
        properties to match

    Returns
    -------
    (res_id, res_metadata): tuple
        id and metadata of the matched Resource
    """

    res_id = None
    res_metadata = None
    if existing_ress is None:
        return res_id, res_metadata

    for exist_res in existing_ress:
        matched = True
        exist_res_json = forge.as_jsonld(exist_res)
        for prop, sub_props in props_to_check.items():
            res_prop = res_json.get(prop)
            exist_res_prop = exist_res_json.get(prop)
            if not exist_res_prop:
                matched = False
                break
            if not sub_props:  # check the entire property
                if res_prop != exist_res_prop:
                    matched = False
                    break
            else:  # check only the specified sub-properties
                for sub_prop in sub_props:
                    if res_prop.get(sub_prop) != exist_res_prop.get(sub_prop):
                        matched = False
                        break
        if matched:
            res_id = exist_res.id
            res_metadata = exist_res._store_metadata
            break

    return res_id, res_metadata

def _integrate_datasets_to_Nexus(forge, resources, dataset_type, atlas_release_id, tag):

    filters = {
        "type": dataset_type,
        "atlasRelease": {"@id": atlas_release_id}}
    logger.info(f"Searching Nexus with filters:\n{filters}")
    n_existing_ress = 0
    existing_ress = forge.search(filters, limit=10000)
    if existing_ress:
        n_existing_ress = len(existing_ress)
    logger.info(f"Found {n_existing_ress} resources")

    dataset_schema = forge._model.schema_id(type_for_schema.get(dataset_type))

    for res in resources:
        res_name = res.name
        res_id, res_store_metadata = match_properties(forge.as_json(res), existing_ress, forge, TypePropertiesMap.prop[dataset_type])
        if res_id:
            res.id = res_id
            check_tag(forge, res_id, tag)
            # TODO: consider to skip update if distribution SHA is identical between res and existing_res
            logger.info(f"Updating Resource '{res_name}' (Nexus id: {res_id})")
            setattr(res, "_store_metadata", res_store_metadata)
            forge.update(res, dataset_schema)
        else:
            logger.info(f"Registering Resource '{res_name}'")
            forge.register(res, dataset_schema)

        if hasattr(res, "id"):
            logger.info(f"Tagging Resource '{res_name}' (Nexus id: {res.id}) with tag '{tag}'")
            forge.tag(res, tag)
        else:
            logger.info(f"Resource '{res_name}' has no id. No tag will be applied")


def validate_token(ctx, param, value):
    if len(value) < 1:
        raise click.BadParameter("The string provided is empty'")
    else:
        return value


@click.group()
@click.version_option(__version__)
@click.option("-v", "--verbose", count=True)
@click.option("--forge-config-file",
              type=click.Path(),
              default=("../forge_configuration/forge_config.yml"),
              help="Path to the configuration file used to instantiate the Forge", )
@click.option("--nexus-env",
              default="https://staging.nise.bbp.epfl.ch/nexus/v1",
              help="Nexus environment to use", )
@click.option("--nexus-org", default="bbp", help="The Nexus organisation to push into")
@click.option("--nexus-proj", default="atlas", help="The Nexus project to push into")
@click.option("--nexus-token",
              type=click.STRING,
              callback=validate_token,
              required=True,
              help="Value of the Nexus token", )
@click.pass_context
@log_args(logger)
def initialize_pusher_cli(
        ctx, verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token
):
    forge, verbose_L = _initialize_pusher_cli(verbose, forge_config_file, nexus_env,
                                              nexus_org, nexus_proj, nexus_token)
    ctx.obj["forge"] = forge
    ctx.obj["env"] = nexus_env
    ctx.obj["bucket"] = "/".join([nexus_org, nexus_proj])
    ctx.obj["token"] = nexus_token
    ctx.obj["verbose"] = verbose_L


def _initialize_pusher_cli(
        verbose, forge_config_file, nexus_env, nexus_org, nexus_proj, nexus_token
):
    """Run the dataset pusher CLI starting by the Initialisation of the Forge python
    framework to communicate with Nexus.\n
    The Forge will enable to build and push into Nexus the metadata payload along with
    the input dataset.
    """
    level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)]
    logging.basicConfig(level=level)

    bucket = f"{nexus_org}/{nexus_proj}"
    try:
        logger.info("Initializing the forge...")
        forge = KnowledgeGraphForge(
            forge_config_file, endpoint=nexus_env, bucket=bucket, token=nexus_token)
    except Exception as e:
        logger.error(f"Error when initializing the forge. {e}")

    close_handler(logger)

    return forge, logger.level


class Args:
    brain_region = "brain-region"
    species = "species"

def get_property_type(name, arg, forge):
    if not arg.startswith("http"):
        raise Exception(
            f"The '{name}' argument provided ({arg}) is not a valid Nexus id")
    arg_res = forge.retrieve(arg, cross_bucket=True)
    if not arg_res:
        raise Exception(f"The '{name}' argument provided ({arg}) can not be retrieved")

    property = {"@id": arg_res.id, "@type": arg_res.type}
    return property


def get_property_label(name, arg, forge):
    name_target_map = {Args.species: "Species",
                       Args.brain_region: "BrainRegion"}
    arg_res = None
    if arg.startswith("http"):
        arg_res = forge.retrieve(arg, cross_bucket=True)
    else:
        arg_res = forge.resolve(arg, scope="ontology", target=name_target_map[name],
                                strategy="EXACT_MATCH")
    if not arg_res:
        raise Exception(
            f"The provided '{name}' argument ({arg}) can not be retrieved/resolved")

    property = {"@id": arg_res.id, "label": arg_res.label}
    return property


@initialize_pusher_cli.command()
@click.pass_context
@log_args(logger)
@click.option("--dataset-path",
              type=click.Path(exists=True),
              required=True,
              multiple=True,
              help="The files or directories of files to push on Nexus", )
@click.option("--dataset-type",
              type=click.STRING,
              required=True,
              help="Type to set for registration of Resources from dataset-path")
@click.option("--atlas-release-id",
              type=click.STRING,
              required=True,
              help="Nexus ID of the atlas release of interest")
@click.option("--" + Args.species,
              type=click.STRING,
              required=True,
              help="Nexus ID or label of the species")
@click.option("--" + Args.brain_region,
              type=click.STRING,
              required=True,
              help="Nexus ID or label of the brain region")
@click.option("--reference-system-id",
              type=click.STRING,
              required=True,
              help="Nexus ID of the reference system Resource")
@click.option("--resource-tag",
              type=click.STRING,
              default=f"{datetime.today().strftime('%Y-%m-%dT%H:%M:%S')}",
              help="Optional tag value with which to tag the resources (default to "
                   "'datetime.today()')")
def push_volumetric(
        ctx,
        dataset_path,
        dataset_type,
        atlas_release_id,
        species,
        brain_region,
        reference_system_id,
        resource_tag,
):
    """Create a VolumetricDataLayer resource payload and push it along with the "
    corresponding volumetric input dataset files into Nexus.
    """
    L = create_log_handler(__name__, "./push_nrrd_volumetricdatalayer.log")
    L.setLevel(ctx.obj["verbose"])

    forge = ctx.obj["forge"]

    # Validate input arguments
    atlas_release_prop = get_property_type("atlas-release-id", atlas_release_id, forge)
    species_prop = get_property_label(Args.species, species, forge)
    brain_region_prop = get_property_label(Args.brain_region, brain_region, forge)
    reference_system_prop = get_property_type("reference-system-id",
                                                  reference_system_id, forge)
    contribution, log_info = comm.return_contribution(forge, ctx.obj["bucket"],
                                                      ctx.obj["token"],
                                                      organization="staging" not in ctx.obj[
                                                          "env"])
    L.info("\n".join(log_info))

    L.info("Filling the metadata of the volumetric payloads...")
    resources = create_volumetric_resources(
        dataset_path,
        dataset_type,
        atlas_release_prop,
        forge,
        species_prop,
        brain_region_prop,
        reference_system_prop,
        contribution,
        L
    )

    n_resources = len(resources)
    if n_resources == 0:
        L.info("No resource created, nothing to push into Nexus.")
        return

    L.info(f"{n_resources} resources will be pushed into Nexus.")
    _integrate_datasets_to_Nexus(
        forge,
        resources,
        dataset_type,
        atlas_release_id,
        resource_tag,
    )


def start():
    initialize_pusher_cli(obj={})


if __name__ == "__main__":
    start()
