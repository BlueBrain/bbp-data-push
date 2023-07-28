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
from bba_data_push.commons_new import return_contribution

from bba_data_push import __version__

logging.basicConfig(level=logging.INFO)
L = logging.getLogger(__name__)


def check_tag(forge, dataset, tag):
    for res_to_tag in dataset:
        res = forge.retrieve(res_to_tag.id, version=tag)
        if res:
            print(
                f"Tag '{tag}' already exists for res id '{res.id}' (revision "
                f"{res._store_metadata._rev}), please choose a different tag.")
            print("No resource with this schema has been tagged.")
            exit(1)


def _integrate_datasets_to_Nexus(forge, resources, schema, tag):
    for res in resources:
        # Update resources in Nexus with the data provided here (i.e no new ids are
        # created if already exist for the given atlas).
        # If the resource already exists, then update. If the resource does not
        # exist, then create new. After tag

        # forge.retrieve resources with type = dataset_type and atlasrelease_id =
        # resources_payloads["atlasrelease_id"]
        pass


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
@log_args(L)
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
        L.info("Initializing the forge...")
        forge = KnowledgeGraphForge(
            forge_config_file, endpoint=nexus_env, bucket=bucket, token=nexus_token)
    except Exception as e:
        L.error(f"Error when initializing the forge. {e}")

    close_handler(L)

    return forge, L.level


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
@log_args(L)
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
    atlas_release_prop = get_property_type("atlas-release-id", atlas_release_id,
                                               forge)
    species_prop = get_property_label(Args.species, species, forge)
    brain_region_prop = get_property_label(Args.brain_region, brain_region, forge)
    reference_system_prop = get_property_type("reference-system-id",
                                                  reference_system_id, forge)
    contribution, log_info = return_contribution(forge, ctx.obj["bucket"],
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
        contribution
    )

    dataset_schema = forge._model.schema_id(dataset_type)

    _integrate_datasets_to_Nexus(
        forge,
        resources,
        dataset_schema,
        resource_tag,
    )


def start():
    initialize_pusher_cli(obj={})


if __name__ == "__main__":
    start()
