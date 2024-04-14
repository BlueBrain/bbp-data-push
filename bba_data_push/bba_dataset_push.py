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
import json
import os
import logging
import click
from datetime import datetime
from uuid import uuid4
import urllib.parse

from kgforge.core import KnowledgeGraphForge, Resource

from bba_data_push.push_atlas_release import create_base_resource, \
    create_volumetric_property, create_atlas_release, \
    create_ph_catalog_distribution, validate_atlas_release, \
    atlas_release_properties
from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources, type_attributes_map
from bba_data_push.push_brainmesh import create_mesh_resources
from bba_data_push.push_cellComposition import register_densities

from bba_data_push.push_cellComposition import create_cellComposition_prop
from bba_data_push.logging import log_args, close_handler, create_log_handler
import bba_data_push.commons as comm

from bba_data_push import __version__

logger = logging.getLogger(__name__)

BRAIN_TEMPLATE_TYPE = "BrainTemplateDataLayer"
REFSYSTEM_TYPE = ["AtlasSpatialReferenceSystem", "BrainAtlasSpatialReferenceSystem"]
VOLUME_TYPE = "CellCompositionVolume"
SUMMARY_TYPE = "CellCompositionSummary"
COMPOSITION_TYPE = "CellComposition"
COMPOSITION_ABOUT = ["Neuron", "Glia"]


def validate_token(ctx, param, value):
    len_value = len(value)
    if len_value < 1:
        raise click.BadParameter("The string provided is empty")
    elif len_value < 100:
        raise click.BadParameter(f"'{value}' is too short for a token."
            "\nProbably the env variable provided is not defined and the next "
            "string has been parsed as token.")
    else:
        return value


@click.group()
@click.version_option(__version__)
@click.option("-v", "--verbose", count=True)
@click.option("--forge-config-file", type=click.STRING,
              default=("https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"),
              help="Path to the configuration file used to instantiate the Forge")
@click.option("--nexus-env", required=True, help="The Nexus environment to use")
@click.option("--nexus-org", required=True, help="The Nexus organisation to push into")
@click.option("--nexus-proj", required=True, help="The Nexus project to push into")
@click.option("--nexus-token",
              type=click.STRING,
              callback=validate_token,
              required=True,
              default=os.getenv("NEXUS_TOKEN"),
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
    """Run the dataset pusher CLI starting by the Initialisation of the Forge
    python framework to communicate with Nexus.\n
    The Forge will enable to build and push into Nexus the metadata payload
    along with the input dataset.
    """
    level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)]
    logging.basicConfig(level=level)

    bucket = f"{nexus_org}/{nexus_proj}"
    try:
        logger.info("Initializing the forge...")
        forge = KnowledgeGraphForge(forge_config_file, endpoint=nexus_env,
                                    bucket=bucket, token=nexus_token)
    except Exception as e:
        raise Exception(f"Error when initializing the forge: {e}")

    close_handler(logger)

    return forge, logger.level


def get_region_prop(hierarchy_path, brain_region):
    region_map = comm.get_region_map(hierarchy_path)
    brain_region_id = brain_region.split("/")[-1]
    brain_region_label = comm.get_region_label(region_map, int(brain_region_id))

    return comm.get_property_id_label(brain_region, brain_region_label)


def get_derivation(atlas_release_id):
    base_derivation = Resource.from_json(
        {"@type": "Derivation",
        "entity": {
            "@id": atlas_release_id,
            "@type": "Entity"}}
    )
    return base_derivation


def get_subject_prop(species_prop):
    return Resource(type="Subject", species=species_prop)


def common_options(opt):
    opt = click.option("--atlas-release-id", type=click.STRING, required=True, multiple=False,
        help="Nexus ID of the atlas release of interest")(opt)
    opt = click.option("--atlas-release-rev", type=click.INT, default=None, multiple=False,
        help="Nexus revision of the atlas release of interest")(opt)
    opt = click.option("--resource-tag", type=click.STRING,
        default=f"{datetime.today().strftime('%Y-%m-%dT%H:%M:%S')}",
        help="Optional tag value with which to tag the resources (default to 'datetime.today()')")(opt)
    opt = click.option("--" + comm.Args.species, type=click.STRING, required=True,
        help="Nexus ID or label of the species")(opt)
    opt = click.option("--" + comm.Args.brain_region, type=click.STRING, required=False,
        default=None, help="Nexus ID of the brain region")(opt)
    opt = click.option("--hierarchy-path", type=click.Path(exists=True), required=True, multiple=False,
        help="The json file containing the hierarchy of the brain regions", )(opt)
    opt = click.option("--reference-system-id", type=click.STRING, required=True,
        help="Nexus ID of the reference system Resource")(opt)
    opt = click.option("--dryrun", default=False,
        help="Boolean flag indicating whether to perform a dryrun execution "
             "that will run the CLI without pushing data in Nexus.")(opt)

    return opt


@initialize_pusher_cli.command(name="push-volumetric")
@click.pass_context
@log_args(logger)
@common_options
@click.option("--dataset-path",
              type=click.Path(exists=True), required=True, multiple=True,
              help="The files or directories of files to push on Nexus")
@click.option("--dataset-metadata",
              type=click.Path(exists=True), required=False, multiple=True,
              help="The metadata file(s) for dataset-path(s)")
@click.option("--dataset-type",
              type=click.STRING, required=True,
              help="Type to set for registration of Resources from dataset-path")
def push_volumetric(ctx, dataset_path, dataset_type, atlas_release_id,
    atlas_release_rev, species, hierarchy_path, brain_region, reference_system_id,
    resource_tag, dryrun, dataset_metadata
):
    """Create a VolumetricDataLayer resource payload and push it along with the
    corresponding volumetric input dataset files into Nexus.
    """
    L = create_log_handler(__name__, "./push_nrrd_volumetricdatalayer.log")
    L.setLevel(ctx.obj["verbose"])

    if dataset_type not in type_attributes_map:
        raise Exception(f"The dataset type provided ('{dataset_type}') is not supported."
                        f"The types supported are: {', '.join(type_attributes_map.keys())}")

    n_dataset_path = len(dataset_path)
    n_metadata_path = len(dataset_metadata)
    if n_dataset_path > n_metadata_path:
        L.warning(f"The number of dataset-paths provided ({n_dataset_path}) is "
            f"larger than the dataset-metadata ({n_metadata_path}), the "
            f"files in the last {n_dataset_path - n_metadata_path} "
            "dataset-paths will be resolved from their filenames.")
    elif n_dataset_path < n_metadata_path:
        L.warning(f"The number of dataset-metadata provided ({n_metadata_path})"
            f" is larger than the dataset-paths ({n_dataset_path}), the last "
            f"{n_metadata_path - n_dataset_path} dataset-metadata will be ignored.")

    forge = ctx.obj["forge"]
    atlas_release_rev = atlas_release_rev or comm.get_resource_rev(forge, atlas_release_id,
        resource_tag, cross_bucket=True)

    # Validate input arguments
    atlas_release_prop = comm.get_property_type(atlas_release_id,
        comm.all_types[comm.atlasrelaseType], atlas_release_rev)
    species_prop = comm.get_property_label(comm.Args.species, species, forge)
    subject = get_subject_prop(species_prop)
    reference_system_prop = comm.get_property_type(reference_system_id, REFSYSTEM_TYPE)
    brain_location_prop = None
    region_map = None
    if brain_region:
        if dataset_type in [comm.brainMaskType]:
            raise Exception(f"The argument --{comm.Args.brain_region} can not be used with "
                f"dataset-type '{dataset_type}' because the brain region for such files"
                " is extracted automatically from the filename")
        brain_region_prop = get_region_prop(hierarchy_path, brain_region)
        brain_location_prop = comm.get_brain_location_prop(brain_region_prop,
                                                           reference_system_prop)
    else:
        region_map = comm.get_region_map(hierarchy_path)
    derivation = get_derivation(atlas_release_id)
    contribution, log_info = comm.return_contribution(forge, dryrun=dryrun)
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
        L,
        None,
        region_map,
        dataset_metadata
    )

    n_resources = len(resources)
    if n_resources == 0:
        L.info("No resource created, nothing to push into Nexus.")
        return

    L.info(f"{n_resources} resources of type {dataset_type} will be pushed into Nexus.")
    comm._integrate_datasets_to_Nexus(forge, resources, dataset_type, atlas_release_id,
        resource_tag, L, dryrun=dryrun)

@initialize_pusher_cli.command(name="push-meshes")
@click.pass_context
@log_args(logger)
@common_options
@click.option("--dataset-path", type=click.Path(exists=True), required=True, multiple=False,
              help="The files or directories of files to push on Nexus",)
@click.option("--dataset-type", type=click.STRING, required=True,
              help="Type to set for registration of Resources from dataset-path")
def push_meshes(ctx, dataset_path, dataset_type, brain_region, hierarchy_path,
    atlas_release_id, atlas_release_rev, species, reference_system_id, resource_tag,
    dryrun
):
    """Create a BrainParcellationMesh Resource payload and push it along with
    the corresponding input dataset files into Nexus.
    """
    L = create_log_handler(__name__, "./push_meshes.log")
    L.setLevel(ctx.obj["verbose"])

    region_map = comm.get_region_map(hierarchy_path)

    forge = ctx.obj["forge"]
    atlas_release_rev = atlas_release_rev or comm.get_resource_rev(forge, atlas_release_id,
        resource_tag, cross_bucket=True)

    # Validate input arguments
    atlas_release_prop = comm.get_property_type(atlas_release_id,
        comm.all_types[comm.atlasrelaseType], atlas_release_rev)
    species_prop = comm.get_property_label(comm.Args.species, species, forge)
    subject = get_subject_prop(species_prop)
    reference_system_prop = comm.get_property_type(reference_system_id, REFSYSTEM_TYPE)
    derivation = get_derivation(atlas_release_id)
    contribution, log_info = comm.return_contribution(forge, dryrun=dryrun)
    L.info("\n".join(log_info))

    L.info("Filling the metadata of the mesh payloads...")
    resources = create_mesh_resources(
        [dataset_path],
        dataset_type,
        region_map,
        atlas_release_prop,
        forge,
        subject,
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
    comm._integrate_datasets_to_Nexus(forge, resources, dataset_type, atlas_release_id,
                                      resource_tag, L, dryrun=dryrun)


@initialize_pusher_cli.command(name="push-cellcomposition")
@common_options
@click.option("--cell-composition-id",
    type=click.STRING, required=False, multiple=False, default=None,
    help="Nexus ID of the CellComposition of interest")
@click.option("--volume-path",
    type=click.Path(exists=True), required=True, multiple=False,
    help="The path to the json CellCompositionVolume file.")
@click.option("--summary-path",
    type=click.Path(exists=True), required=False, multiple=False,
    help="The path to the json CellCompositionSummary file.")
@click.option("--name",
    type=click.STRING, nargs=3, required=False, default=(None, None, None),
    help="The name to assign to the CellComposition(Volume,Summary).")
@click.option("--description",
    type=click.STRING, nargs=3, required=False, default=(None, None, None),
    help="The description to assign to the CellComposition(Volume,Summary).")
@click.option("--log-dir",
    type = click.Path(), default = ("."),
    help = "The output dir for log and by-products",)
@click.option("--force-registration",
    is_flag=True,
    default=False,
)
@click.option(
    "--output-resource-file",
    type=click.Path(),
    required=False,
    default=None,
    help="Optional output jsonld file to write the registered resource metadata."
)
@click.pass_context
def cli_push_cellcomposition(
    ctx, atlas_release_id, atlas_release_rev, cell_composition_id, species, brain_region,
    hierarchy_path, reference_system_id, volume_path, summary_path,
    name, description, log_dir, resource_tag, dryrun, force_registration, output_resource_file) -> Resource:
    """Create a CellComposition resource payload and push it along with the
    corresponding CellCompositionVolume and CellCompositionSummary into Nexus.
    Tag all these resources with the input tag or, if not provided, with a timestamp
    """

    logger = create_log_handler(__name__, os.path.join(log_dir, "push_cellComposition.log"))
    logger.setLevel(ctx.obj["verbose"])

    forge = ctx.obj["forge"]

    cell_composition = push_cellcomposition(forge, atlas_release_id, atlas_release_rev,
        cell_composition_id, brain_region, hierarchy_path, reference_system_id, species,
        volume_path, summary_path, name, description, resource_tag, logger,
        force_registration=force_registration, dryrun=dryrun)

    if output_resource_file:
        comm.write_json(data=forge.as_jsonld(cell_composition), filepath=output_resource_file, indent=2)
        logger.info("CellComposition Resource written at %s", output_resource_file)

    return cell_composition

def check_id(resource, resource_type):
    if not hasattr(resource, 'id'):
        raise Exception(f"The following {resource_type} has no id, probably it has not been pushed in Nexus:\n{resource}")


def push_cellcomposition(forge, atlas_release_id, atlas_release_rev, cell_composition_id,
    brain_region, hierarchy_path, reference_system_id, species, volume_path,
    summary_path, name, description, resource_tag, logger, force_registration=False,
    dryrun=False) -> Resource:
    """
    Create a CellComposition resource payload and push it along with the corresponding
    CellCompositionVolume and CellCompositionSummary into Nexus.
    Tag these resources with resource_tag.

    :param forge: KnowledgeGraphForge object.
    :param atlas_release_id: Nexus ID of the atlas release associated with the cell composition.
    :param atlas_release_rev: None or Nexus revision of the atlas release.
    :param cell_composition_id: None or Nexus ID of the CellComposition resource.
    :param brain_region: Nexus ID of the brain region.
    :param hierarchy_path: path to the json file containing the hierarchy of brain regions.
    :param reference_system_id: Nexus ID of the reference system Resource.
    :param species: Nexus ID or label of the species.
    :param volume_path: path to the json CellCompositionVolume file.
    :param summary_path: path to the json CellCompositionSummary file.
    :param name: tuple of None or Resource names to assign (CellComposition, CellCompositionSummary, CellCompositionVolume).
    :param description: tuple of None or Resource descriptions to assign (CellComposition, CellCompositionSummary, CellCompositionVolume).
    :param resource_tag: tag value with which to tag the resources.
    :param logger: logger object for logging messages.
    :param force_registration: optional boolean flag indicating whether to force
        Resource registration over update.
    :param dryrun: optional boolean flag indicating whether to perform a dry run execution.

    :return: CellComposition Resource.
    """
    atlas_release_rev = atlas_release_rev or comm.get_resource_rev(forge, atlas_release_id,
        resource_tag, cross_bucket=True)

    atlas_release_prop = comm.get_property_type(atlas_release_id,
        comm.all_types[comm.atlasrelaseType], atlas_release_rev)
    brain_region_prop = get_region_prop(hierarchy_path, brain_region)
    reference_system_prop = comm.get_property_type(reference_system_id, REFSYSTEM_TYPE)
    brain_location_prop = comm.get_brain_location_prop(brain_region_prop, reference_system_prop)
    species_prop = comm.get_property_label(comm.Args.species, species, forge)
    subject_prop = get_subject_prop(species_prop)
    contribution, log_info = comm.return_contribution(forge, dryrun=dryrun)
    derivation = get_derivation(atlas_release_id)
    logger.info("\n".join(log_info))

    #volume_about = ["https://bbp.epfl.ch/ontologies/core/bmo/METypeDensity"]  # should this be set as the others?
    #summary_about = ["nsg:Neuron", "nsg:Glia"]
    #composition_about = summary_about

    cell_comp_volume = create_cellComposition_prop(
        forge, VOLUME_TYPE, COMPOSITION_ABOUT, atlas_release_prop, brain_location_prop, subject_prop, contribution,
        derivation, name[2], description[2], volume_path, reference_system_prop)
    comm._integrate_datasets_to_Nexus(forge, [cell_comp_volume], VOLUME_TYPE,
        atlas_release_id, resource_tag, logger, force_registration=force_registration,
        dryrun=dryrun)
    check_id(cell_comp_volume, VOLUME_TYPE)

    cell_comp_summary = create_cellComposition_prop(
        forge, SUMMARY_TYPE, COMPOSITION_ABOUT, atlas_release_prop, brain_location_prop, subject_prop, contribution,
        derivation, name[1], description[1], summary_path, reference_system_prop)
    comm._integrate_datasets_to_Nexus(forge, [cell_comp_summary], SUMMARY_TYPE,
        atlas_release_id, resource_tag, logger, force_registration=force_registration,
        dryrun=dryrun)
    check_id(cell_comp_summary, SUMMARY_TYPE)

    cell_composition = create_cellComposition_prop(
        forge, COMPOSITION_TYPE, COMPOSITION_ABOUT, atlas_release_prop, brain_location_prop, subject_prop, contribution,
        derivation, name[0], description[0], None, reference_system_prop)
    cell_composition.cellCompositionVolume = {"@id": cell_comp_volume.id, "@type": VOLUME_TYPE}
    cell_composition.cellCompositionSummary = {"@id": cell_comp_summary.id, "@type": SUMMARY_TYPE}
    if cell_composition_id:
        cell_composition.id = cell_composition_id
    comm._integrate_datasets_to_Nexus(forge, [cell_composition], COMPOSITION_TYPE,
        atlas_release_id, resource_tag, logger, force_registration=force_registration, dryrun=dryrun)
    check_id(cell_composition, COMPOSITION_TYPE)

    return cell_composition


@initialize_pusher_cli.command(name="push-atlasrelease")
@click.pass_context
@common_options
@log_args(logger)
@click.option("--brain-template-id",
    type=click.STRING, required=True, multiple=False,
    help="Nexus ID of the brain template")
@click.option("--hierarchy-ld-path",
              type=click.Path(exists=True), required=True, multiple=False,
              help="The hierarchy json-ld file to push in Nexus",)
@click.option("--annotation-path",
              type=click.Path(exists=True), required=True, multiple=False,
              help="The annotation volume to push in Nexus",)
@click.option("--hemisphere-path",
              type=click.Path(exists=True), required=True, multiple=False,
              help="The hemisphere file to push in Nexus",)
@click.option("--placement-hints-path",
              type=click.Path(exists=True), required=True, multiple=False,
              help="The files or directory of placement hints",)
@click.option("--placement-hints-metadata",
              type=click.Path(exists=True), required=True, multiple=False,
              help="The file path of placement hints metadata",)
@click.option("--direction-vectors-path",
              type=click.Path(exists=True), required=True, multiple=False,
              help="The direction vectors file to push in Nexus",)
@click.option("--cell-orientations-path",
              type=click.Path(exists=True), required=True, multiple=False,
              help="The cell orientations file to push in Nexus",)
@click.option("--name",
    type=click.STRING, required=False, multiple=False,
    default="Blue Brain Atlas",
    help="The name to assign to the AtlasRelease resource.")
@click.option("--description",
    type=click.STRING, required=False, multiple=False,
    default="The official Atlas of the Blue Brain Project",
    help="The description to assign to the AtlasRelease resource.")
def push_atlasrelease(ctx, species, brain_region, reference_system_id, brain_template_id,
    hierarchy_path, hierarchy_ld_path, annotation_path, hemisphere_path,
    placement_hints_path, placement_hints_metadata, direction_vectors_path,
    cell_orientations_path, atlas_release_id, atlas_release_rev, resource_tag,
    name, description, dryrun
):
    forge = ctx.obj["forge"]
    bucket = ctx.obj["bucket"]

    atlas_release_id_orig = None
    properties_id_map = {}
    for prop in atlas_release_properties:
        properties_id_map[prop] = None

    if atlas_release_id:
        force_registration = False
        atlas_release_orig = comm.retrieve_resource(atlas_release_id, forge)
        if not atlas_release_orig:
            raise Exception(f"Resource with id '{atlas_release_id}' not found in Nexus bucket '{bucket}'\n"
                            "Please provide a valid id (or 'None' to create a new AtlasRelease)")
        else:
            atlas_release_id_orig = atlas_release_id
            atlas_release_rev = atlas_release_orig._store_metadata["_rev"]
            for prop in properties_id_map:
                existing_prop = getattr(atlas_release_orig, prop, None)
                if existing_prop:
                    properties_id_map[prop] = existing_prop.id
    else:
        atlas_release_schema = forge._model.schema_id(comm.atlasrelaseType)
        atlas_release_id = "/".join(["https://bbp.epfl.ch", "data", bucket,
                                     urllib.parse.quote(atlas_release_schema), str(uuid4())])
        atlas_release_rev = 0
        force_registration = True

    species_prop = comm.get_property_label(comm.Args.species, species, forge)
    subject_prop = get_subject_prop(species_prop)
    brain_region_prop = get_region_prop(hierarchy_path, brain_region)
    reference_system_prop = comm.get_property_type(reference_system_id, REFSYSTEM_TYPE)
    brain_location_prop = comm.get_brain_location_prop(brain_region_prop, reference_system_prop)
    brain_template_prop = comm.get_property_type(brain_template_id, BRAIN_TEMPLATE_TYPE)

    contribution, log_info = comm.return_contribution(forge, dryrun=dryrun)
    logger.info("\n".join(log_info))

    atlas_release_prop = comm.get_property_type(atlas_release_id,
        comm.all_types[comm.atlasrelaseType], rev=atlas_release_rev + 1) # Anticipating AtlasRelease tagging
    derivation = get_derivation(atlas_release_id)

    # Create ParcellationOntology resource
    ont_name = "BBP Mouse Brain region ontology"
    ont_res = create_base_resource(comm.all_types[comm.ontologyType],
        brain_location_prop, reference_system_prop, subject_prop, contribution,
        atlas_release_prop, ont_name, None, None, properties_id_map["parcellationOntology"])
    ont_dis = [{"path": hierarchy_path, "content_type": "application/json"},
               {"path": hierarchy_ld_path, "content_type": "application/ld+json"}]
    comm.add_distribution(ont_res, forge, ont_dis)
    ont_res.label = "BBP Mouse Brain region ontology"
    comm._integrate_datasets_to_Nexus(forge, [ont_res], comm.ontologyType,
        atlas_release_id_orig, resource_tag, logger, dryrun=dryrun)

    # Create ParcellationVolume resource
    par_name = "BBP Mouse Brain Annotation Volume"
    par_prop = create_volumetric_property(par_name, comm.parcellationType,
        properties_id_map["parcellationVolume"], annotation_path, atlas_release_prop,
        atlas_release_id_orig, forge, subject_prop, brain_location_prop,
        reference_system_prop, contribution, derivation, resource_tag, logger, dryrun)

    # Create HemisphereAnnotation resource
    hem_name = "Hemisphere annotation from Allen ccfv3 volume"
    hem_prop = create_volumetric_property(hem_name, comm.hemisphereType,
        properties_id_map["hemisphereVolume"], hemisphere_path, atlas_release_prop,
        atlas_release_id_orig, forge, subject_prop, brain_location_prop,
        reference_system_prop, contribution, derivation, resource_tag, logger, dryrun)

    # Create PlacementHints resources
    ph_res = create_volumetric_resources((placement_hints_path,), comm.placementHintsType,
        atlas_release_prop, forge, subject_prop, brain_location_prop, reference_system_prop,
        contribution, derivation, logger)
    resource_to_filepath = comm._integrate_datasets_to_Nexus(forge, ph_res, comm.placementHintsType,
        atlas_release_id_orig, resource_tag, logger, dryrun=dryrun)

    # Create PlacementHints catalog (i.e a collection of PlacementHints)
    ph_catalog_name = "Placement Hints volumes catalog"
    ph_catalog_description = "Placement Hints volumes catalog"
    ph_catalog = create_base_resource(comm.all_types[comm.placementHintsDataLayerCatalogType],
        brain_location_prop, reference_system_prop, subject_prop, contribution,
        atlas_release_prop, ph_catalog_name, ph_catalog_description,
        forge.get_model_context().expand(comm.placementHintsType),
        properties_id_map["placementHintsDataCatalog"])

    with open(placement_hints_metadata, "r") as f:
        filepath_to_brainregion_json = json.load(f)
    ph_catalog_distribution = create_ph_catalog_distribution(ph_res,
        filepath_to_brainregion_json, resource_to_filepath, forge, dryrun)
    with open("./ph_catalog_distribution.json", "w") as f:
        json.dump(ph_catalog_distribution, f)
    
    comm.add_distribution(ph_catalog, forge, [{"path":"./ph_catalog_distribution.json", "content_type": "application/json"}])
    comm._integrate_datasets_to_Nexus(forge, [ph_catalog],
        comm.placementHintsDataLayerCatalogType, atlas_release_id_orig,
        resource_tag, logger, dryrun=dryrun)
    ph_catalog_prop = comm.get_property_type(ph_catalog.id, comm.placementHintsDataLayerCatalogType)

    # Create DirectionVectorsField resource
    dv_name = "Direction Vectors volume"
    dv_prop = create_volumetric_property(dv_name, comm.directionVectorsType,
        properties_id_map["directionVector"], direction_vectors_path,
        atlas_release_prop, atlas_release_id_orig, forge, subject_prop,
        brain_location_prop, reference_system_prop, contribution, derivation,
        resource_tag, logger, dryrun)

    # Create CellOrientationField resource
    co_name = "Orientation Field volume"
    co_prop = create_volumetric_property(co_name, comm.cellOrientationType,
        properties_id_map["cellOrientationField"], cell_orientations_path,
        atlas_release_prop, atlas_release_id_orig, forge, subject_prop,
        brain_location_prop, reference_system_prop, contribution, derivation,
        resource_tag, logger, dryrun)

    # Create AtlasRelease resource
    ont_prop = comm.get_property_type(ont_res.id, comm.ontologyType)
    atlas_release_resource = create_atlas_release(atlas_release_id_orig, brain_location_prop,
        reference_system_prop, brain_template_prop, subject_prop, ont_prop, par_prop,
        hem_prop, ph_catalog_prop, dv_prop, co_prop, contribution, name, description)
    comm._integrate_datasets_to_Nexus(forge, [atlas_release_resource], comm.atlasrelaseType,
        atlas_release_id_orig, resource_tag, logger, force_registration, dryrun=dryrun)

    # Sanity check
    validated = validate_atlas_release(atlas_release_id, forge, resource_tag, logger)
    if not validated:
        logger.error(f"The properties of AtlasRelease Id {atlas_release_id} at "
                     f"tag '{resource_tag}' did not pass the validation!")


@initialize_pusher_cli.command(name="register-cell-composition-volume-distribution")
@click.pass_context
@common_options
@click.option(
    "--input-distribution-file",
    type=click.Path(exists=True),
    required=True,
    help="Input cell composition volume distribution JSON with either id or local path densities.",
)
@click.option(
    "--output-distribution-file",
    type=click.Path(exists=False),
    required=True,
    help="Output cell composition volume distribution JSOn with only density ids.",
)
def cli_register_cell_composition_volume_distribution(
    ctx,
    atlas_release_id,
    atlas_release_rev,
    resource_tag,
    species,
    brain_region,
    hierarchy_path,
    reference_system_id,
    dryrun,
    input_distribution_file,
    output_distribution_file,
):
    forge = ctx.obj["forge"]
    subject_prop = get_subject_prop(
        species_prop=comm.get_property_label(
            name=comm.Args.species,
            arg=species,
            forge=forge,
        ),
    )
    reference_system_prop = comm.get_property_type(
            arg_id=reference_system_id,
            arg_type=REFSYSTEM_TYPE,
    )
    brain_location_prop = comm.get_brain_location_prop(
        brain_region=get_region_prop(
            hierarchy_path=hierarchy_path,
            brain_region=brain_region,
        ),
        reference_system=reference_system_prop,
    )
    atlas_release_prop = comm.get_property_type(
        arg_id=atlas_release_id,
        arg_type=comm.all_types[comm.atlasrelaseType],
        rev=atlas_release_rev,
    )
    register_densities(
        volume_path=input_distribution_file,
        atlas_release_prop=atlas_release_prop,
        forge=forge,
        subject=subject_prop,
        brain_location_prop=brain_location_prop,
        reference_system_prop=reference_system_prop,
        contribution=None,
        derivation=None,
        resource_tag=resource_tag,
        force_registration=True,
        dryrun=dryrun,
        output_volume_path=output_distribution_file
    )


def start():
    initialize_pusher_cli(obj={})


if __name__ == "__main__":
    start()
