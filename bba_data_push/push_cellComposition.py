"""
Create a 'CellCompositionVolume', a 'CellCompositionSummary' and the corresponding
'CellComposition' resource payload to push into Nexus.
Link to BBP Atlas pipeline confluence documentation:
https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""
import logging
import json
from kgforge.specializations.resources import Dataset
import bba_data_push.commons as comm
from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources

logger = logging.getLogger(__name__)

part_key = "hasPart"
path_key = "path"
id_key = "@id"


def create_cellComposition_prop(
    forge,
    schema,
    about,
    atlas_release,
    brain_location,
    subject,
    contribution,
    derivation,
    name,
    description,
    file_path,
    reference_system_prop=None
):
    res_type = [schema, "Dataset"]
    # "AtlasDatasetRelease" is kept for backward compatibility
    if schema == "CellCompositionVolume":
        res_type.append("AtlasDatasetRelease")

    expanded_about = []
    for a in about:
        expanded_about.append(forge.get_model_context().expand(a))

    base_res = Dataset(forge, type=res_type,
        atlasRelease = atlas_release,
        about = expanded_about,
        brainLocation = brain_location,
        subject = subject,
        contribution = contribution,
        derivation = [derivation],
        name = name if name else get_name(schema, contribution)
    )
    if description:
        base_res.description = f"{description} ({schema})"

    if file_path:
        base_res.distribution = forge.attach(file_path, content_type="application/json")
        base_res.temp_filepath = file_path

    if reference_system_prop:
        base_res.atlasSpatialReferenceSystem = reference_system_prop

    return base_res


def register_densities(volume_path, atlas_release_prop, forge, subject,
    brain_location_prop, reference_system_prop, contribution, derivation,
    resource_tag, force_registration, dryrun, output_volume_path):
    # Parse input volume
    volume_content = json.loads(open(volume_path).read())

    no_key = f"At least one '{part_key}' key is required"
    len_vc = len(volume_content)
    if len_vc < 1:
        raise Exception(f"No key found in {volume_path}! {no_key}")
    if part_key not in volume_content:
        raise Exception(f"No {part_key} key found anong the {len_vc} keys in {volume_path}! {no_key}")
    if len_vc > 1:
        logger.warning(f"More than one key ({len_vc}) found in {volume_path}, only '{part_key}' will be considered")

    mts = volume_content[part_key]
    logger.info(f"Parsing {len(mts)} M-types...")
    for mt in mts:
        mt_label = mt["label"]
        ets = mt[part_key]
        logger.info(f"\nParsing {len(ets)} E-types for M-type '{mt_label}'...")
        for et in ets:
            et_label = et["label"]
            et_part = et[part_key][0]

            if et_part.get(id_key):
                me_density = f"Density {mt_label}-{et_label}"
                if et_part.get(path_key):
                    raise ValueError(f"{me_density} hss both an '{id_key}' and a '{path_key}', please remove one")
                logger.info(f"{me_density} has an '{id_key}' key, hence will not be modified")
                continue
            elif not et_part.get(path_key):
                raise ValueError(f"Neither '{id_key}' nor '{path_key}' available for m-type {mt_label}, e-type {et_label}."
                                 " Please provide one.")

            filepath = et_part[path_key]
            res_type = comm.meTypeDensity
            # Create Resource payload
            resources = create_volumetric_resources((filepath,), res_type,
                atlas_release_prop, forge, subject, brain_location_prop,
                reference_system_prop, contribution, derivation, logger)
            # Register Resource
            comm._integrate_datasets_to_Nexus(forge, resources, res_type,
                atlas_release_prop.id, resource_tag, logger,
                force_registration=force_registration, dryrun=dryrun)
            res = resources[0]
            et_part[id_key] = res.id
            et_part["_rev"] = res._store_metadata["_rev"]
            et_part["@type"] = res.type
            et_part.pop(path_key)

    with open(output_volume_path, "w") as volume_distribution_path:
        volume_distribution_path.write(json.dumps(volume_content))

    return volume_content


def get_name(schema, user_contribution):
    username = user_contribution[0].agent['@id'].split("/")[-1]
    return f"{schema} from {username}"
