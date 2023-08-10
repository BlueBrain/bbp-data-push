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
import os
import logging
import click
from datetime import datetime

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.core.wrappings.paths import Filter, FilterOperator, create_filters_from_dict

from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources
from bba_data_push.push_cellComposition import create_cellComposition_prop
from bba_data_push.logging import log_args, close_handler, create_log_handler
import bba_data_push.commons as comm

from bba_data_push import __version__

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ATLASRELEASE_TYPE = ["AtlasRelease","BrainAtlasRelease"]
REFSYSTEM_TYPE = ["AtlasSpatialReferenceSystem", "BrainAtlasSpatialReferenceSystem"]
VOLUME_TYPE = "CellCompositionVolume"
SUMMARY_TYPE = "CellCompositionSummary"
COMPOSITION_TYPE = "CellComposition"
COMPOSITION_ABOUT = ["nsg:Neuron", "nsg:Glia"]


def check_tag(forge, res_id, tag):
    logger.debug(f"Verify that tag '{tag}' does not exist already for Resource id '{res_id}':")
    res = forge.retrieve(res_id, version=tag)
    if res:
        msg = f"Tag '{tag}' already exists for res id '{res_id}' (revision {res._store_metadata._rev}, Nexus address"\
              f" '{res._store_metadata._self}'), please choose a different tag."
        msg += " No resource with this schema has been tagged."
        raise Exception(msg)


type_for_schema = {
    comm.meTypeDensity: "VolumetricDataLayer",
    VOLUME_TYPE: VOLUME_TYPE,
    SUMMARY_TYPE: SUMMARY_TYPE,
    COMPOSITION_TYPE: COMPOSITION_TYPE}


def get_existing_resources(dataset_type, atlas_release_id, res, forge, limit):
    filters = {"type": dataset_type,
               "atlasRelease": {"id": atlas_release_id},
               "brainLocation": {"brainRegion": {"id": res.brainLocation.brainRegion.get_identifier()}},
               "subject": {"species": {"id": res.subject.species.get_identifier()}}
               }

    def get_filters_by_type(res, type):
        filters_by_type = []
        if type == comm.meTypeDensity:
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL, path=["annotation", "type"],
                       value=res.annotation[0].get_type()))
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL, path=["annotation", "type"],
                       value=res.annotation[1].get_type()))
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL,
                       path=["annotation", "hasBody", "id"],
                       value=res.annotation[0].hasBody.get_identifier()))
            filters_by_type.append(Filter(operator=FilterOperator.EQUAL,
                       path=["annotation", "hasBody", "id"],
                       value=res.annotation[1].hasBody.get_identifier()))

        return filters_by_type

    filter_list = create_filters_from_dict(filters)

    filter_list.extend(get_filters_by_type(res, dataset_type))

    if hasattr(res.brainLocation, "layer"):
        for layer in res.brainLocation.layer:
            filter_list.append(Filter(operator=FilterOperator.EQUAL, path=["brainLocation", "layer", "id"], value=layer.get_identifier()))

    return forge.search(filters, limit=limit), filter_list

def get_res_store_metadata(res_id, forge):
    res = forge.retrieve(res_id, cross_bucket=True)
    return res._store_metadata

def check_res_list(res_list, filepath_list, logger, action):
    error_messages = []
    for i, res in enumerate(res_list):
        if not res._last_action.succeeded:
            l_a = res._last_action
            error_messages.append(f"{res.get_identifier()},{res.name},{res.get_type()},{filepath_list[i]},"
                                  f"{l_a.error},{action},{l_a.message}")
    n_error_msg = len(error_messages)
    if n_error_msg != 0:
        errors = "\n".join(error_messages)
        logger.warning(f"Got the following {n_error_msg} errors:\n"
                       "res ID,res name,res tyoe,filepath,error,action,message\n"
                       f"{errors}")

def _integrate_datasets_to_Nexus(forge, resources, dataset_type, atlas_release_id, tag):

    dataset_schema = forge._model.schema_id(type_for_schema.get(dataset_type))

    ress_to_update = []
    ress_to_regster = []
    filepath_update_list = []  # matching the resource list by list index
    filepath_register_list = []  # matching the resource list by list index
    res_count = 0
    for res in resources:
        res_count += 1
        res_name = res.name
        res_msg = f"Resource '{res_name}' ({res_count} of {len(resources)})"

        if hasattr(res, "id"):
            res_id = res.id
            res_store_metadata = get_res_store_metadata(res_id, forge)
        else:
            logger.info(f"Searching Nexus for {res_msg}")
            limit = 100
            orig_ress, matching_filters = get_existing_resources(dataset_type, atlas_release_id, res, forge, limit)
            if len(orig_ress) > 1:
                raise Exception(f"Error: at least {limit} matching Resources found using the criteria: {matching_filters}")
            elif len(orig_ress) == 1:
                res_id = orig_ress[0].id
                res_store_metadata = get_res_store_metadata(res_id, forge)
            else:
                logger.info("No Resource found")

        if res_id:
            res.id = res_id
            check_tag(forge, res_id, tag)
            # TODO: consider to skip update if distribution SHA is identical between res and existing_res
            logger.info(f"Scheduling to update {res_msg} with Nexus id: {res_id}")
            setattr(res, "_store_metadata", res_store_metadata)
            if hasattr(res, "filepath"):
                filepath_update_list.append(res.filepath)
                delattr(res, "filepath")
            else:
                filepath_update_list.append(None)
            ress_to_update.append(res)
        else:
            logger.info(f"Scheduling to register {res_msg}")
            if hasattr(res, "filepath"):
                filepath_register_list.append(res.filepath)
                delattr(res, "filepath")
            else:
                filepath_register_list.append(None)
            ress_to_regster.append(res)

    logger.info(f"Updating {len(ress_to_update)} Resources with schema '{dataset_schema}'")
    forge.update(ress_to_update, dataset_schema)
    check_res_list(ress_to_update, filepath_update_list, "updating", logger)

    logger.info(f"Registering {len(ress_to_regster)} Resources with schema '{dataset_schema}'")
    forge.register(ress_to_regster, dataset_schema)
    check_res_list(ress_to_regster, filepath_register_list, "registering", logger)

    ress_to_tag = ress_to_update + ress_to_regster
    filepath_tag_list = filepath_update_list + filepath_register_list
    logger.info(f"Tagging {len(ress_to_tag)} Resources with tag '{tag}'")
    forge.tag(ress_to_tag, tag)
    check_res_list(ress_to_tag, filepath_tag_list, "tagging", logger)


def validate_token(ctx, param, value):
    len_value = len(value)
    if len_value < 1:
        raise click.BadParameter("The string provided is empty")
    elif len_value < 100:
        raise click.BadParameter(f"{value}\nProbably the variable provided is not defined and the next string has been "
                                 "parsed as token.")
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
        raise Exception(f"Error when initializing the forge: {e}")

    close_handler(logger)

    return forge, logger.level


class Args:
    species = "species"
    brain_region = "brain-region"
    name_target_map = {species: "Species",
                       brain_region: "BrainRegion"}

def get_property_type(arg_id, arg_type):
    return Resource(id=arg_id, type=arg_type)


def get_property_label(name, arg, forge):

    if arg.startswith("http"):
        arg_res = forge.retrieve(arg, cross_bucket=True)
    else:
        arg_res = forge.resolve(arg, scope="ontology", target=Args.name_target_map[name],
                                strategy="EXACT_MATCH")
    if not arg_res:
        raise Exception(
            f"The provided '{name}' argument ({arg}) can not be retrieved/resolved")

    property = Resource(id=arg_res.id, label=arg_res.label)
    return property

def get_brain_location_prop(brain_region, reference_system):
    return Resource(
        brainRegion=brain_region,
        atlasSpatialReferenceSystem=reference_system)

def get_derivation(atlas_release_id):
    base_derivation = Resource.from_json(
        {"@type": "Derivation",
        "entity": {
            "@id": atlas_release_id,
            "@type": "Entity"}}
    )
    return base_derivation

def get_subject_prop(species_prop):
    return Resource(
        type="Subject",
        species=species_prop)

atlas_release_option = click.option("--atlas-release-id",
    type=click.STRING, required=True, multiple=False,
    help="Nexus ID of the atlas release of interest")
tag_option = click.option("--resource-tag",
    type=click.STRING, default=f"{datetime.today().strftime('%Y-%m-%dT%H:%M:%S')}",
    help="Optional tag value with which to tag the resources (default to 'datetime.today()')")
species_option = click.option("--" + Args.species,
    type=click.STRING, required=True, help="Nexus ID or label of the species")
brain_region_option = click.option("--" + Args.brain_region,
    type=click.STRING, required=True, help="Nexus ID or label of the brain region")
reference_system_option = click.option("--reference-system-id",
    type=click.STRING, required=True, help="Nexus ID of the reference system Resource")

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
@atlas_release_option
@species_option
@brain_region_option
@reference_system_option
@tag_option
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
    atlas_release_prop = get_property_type(atlas_release_id, ATLASRELEASE_TYPE)
    species_prop = get_property_label(Args.species, species, forge)
    subject = get_subject_prop(species_prop)
    brain_region_prop = get_property_label(Args.brain_region, brain_region, forge)
    reference_system_prop = get_property_type(reference_system_id, REFSYSTEM_TYPE)
    brain_location_prop = get_brain_location_prop(brain_region_prop, reference_system_prop)
    derivation = get_derivation(atlas_release_id)
    contribution, log_info = comm.return_contribution(forge, ctx.obj["bucket"],
        ctx.obj["token"], organization="staging" not in ctx.obj["env"])
    L.info("\n".join(log_info))

    L.info("Filling the metadata of the volumetric payloads...")
    resources = create_volumetric_resources(
        dataset_path,
        dataset_type,
        atlas_release_prop,
        forge,
        subject,
        brain_location_prop,
        reference_system_prop,
        contribution,
        derivation,
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



@initialize_pusher_cli.command(name="push-cellcomposition")
@atlas_release_option
@species_option
@brain_region_option
@reference_system_option
@click.option("--cell-composition-id",
    type=click.STRING, required=True, multiple=False,
    help="Nexus ID of the CellComposition of interest")
@click.option("--volume-path",
    type=click.Path(exists=True), required=True, multiple=False,
    help="The path to the json CellCompositionVolume file.")
@click.option("--summary-path",
    type=click.Path(exists=True), required=False, multiple=False,
    help="The path to the json CellCompositionSummary file.")
@click.option("--name",
    type=click.STRING, required=False, multiple=False,
    help="The name to assign to the CellComposition(Volume,Summary).")
@click.option("--description",
    type=click.STRING, required=False, multiple=False,
    help="The description to assign to the CellComposition(Volume,Summary).")
@click.option("--log-dir",
    type = click.Path(), default = ("."),
    help = "The output dir for log and by-products",)
@tag_option
@click.pass_context
def cli_push_cellcomposition(
    ctx, atlas_release_id, cell_composition_id, species, brain_region, reference_system_id, volume_path, summary_path, name, description, log_dir, resource_tag) -> str:
    """Create a CellComposition resource payload and push it along with the "
    corresponding CellCompositionVolume and CellCompositionSummary into Nexus.
    Tag all these resources with the input tag or, if not provided, with a timestamp\n
    """

    logger = create_log_handler(__name__, os.path.join(log_dir, "push_cellComposition.log"))
    logger.setLevel(ctx.obj["verbose"])

    return push_cellcomposition(ctx, atlas_release_id, cell_composition_id, brain_region, reference_system_id, species,
                                volume_path, summary_path, name, description, resource_tag, logger)


def check_id(resource, resource_type):
    if not hasattr(resource, 'id'):
        raise Exception(f"The following {resource_type} has no id, probably it has not been pushed in Nexus:\n{resource}")


def push_cellcomposition(ctx, atlas_release_id, cell_composition_id, brain_region, reference_system_id, species, volume_path, summary_path, name,
                         description, resource_tag, logger) -> str:

    forge = ctx.obj["forge"]

    atlas_release_prop = get_property_type(atlas_release_id, ATLASRELEASE_TYPE)
    brain_region_prop = get_property_label(Args.brain_region, brain_region, forge)
    reference_system_prop = get_property_type(reference_system_id, REFSYSTEM_TYPE)
    brain_location_prop = get_brain_location_prop(brain_region_prop, reference_system_prop)
    species_prop = get_property_label(Args.species, species, forge)
    subject_prop = get_subject_prop(species_prop)
    contribution, log_info = comm.return_contribution(forge, ctx.obj["bucket"], ctx.obj["token"], organization="staging" not in ctx.obj["env"])
    derivation = get_derivation(atlas_release_id)
    logger.info("\n".join(log_info))

    #volume_about = ["https://bbp.epfl.ch/ontologies/core/bmo/METypeDensity"]  # should this be set as the others?
    #summary_about = ["nsg:Neuron", "nsg:Glia"]
    #composition_about = summary_about

    cell_comp_volume = create_cellComposition_prop(
        forge, VOLUME_TYPE, COMPOSITION_ABOUT, atlas_release_prop, brain_location_prop, subject_prop, contribution,
        derivation, name, description, volume_path)
    _integrate_datasets_to_Nexus(forge, [cell_comp_volume], VOLUME_TYPE, atlas_release_id, resource_tag)
    check_id(cell_comp_volume, VOLUME_TYPE)

    cell_comp_summary = create_cellComposition_prop(
        forge, SUMMARY_TYPE, COMPOSITION_ABOUT, atlas_release_prop, brain_location_prop, subject_prop, contribution,
        derivation, name, description, summary_path)
    _integrate_datasets_to_Nexus(forge, [cell_comp_summary], SUMMARY_TYPE, atlas_release_id, resource_tag)
    check_id(cell_comp_summary, SUMMARY_TYPE)

    cell_composition = create_cellComposition_prop(
        forge, COMPOSITION_TYPE, COMPOSITION_ABOUT, atlas_release_prop, brain_location_prop, subject_prop, contribution,
        derivation, name, description, None, reference_system_prop)
    cell_composition.cellCompositionVolume = {"@id": cell_comp_volume.id, "@type": VOLUME_TYPE}
    cell_composition.cellCompositionSummary = [{"@id": cell_comp_summary.id, "@type": SUMMARY_TYPE}]
    cell_composition.id = cell_composition_id
    _integrate_datasets_to_Nexus(forge, [cell_composition], COMPOSITION_TYPE, atlas_release_id, resource_tag)
    check_id(cell_composition, COMPOSITION_TYPE)

    return cell_composition.id


def start():
    initialize_pusher_cli(obj={})


if __name__ == "__main__":
    start()
