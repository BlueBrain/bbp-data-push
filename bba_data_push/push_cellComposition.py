"""
Create a 'CellCompositionVolume', a 'CellCompositionSummary' and the corresponding
'CellComposition' resource payload to push into Nexus.
Link to BBP Atlas pipeline confluence documentation:
https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

import os
import json
import nrrd
import copy
from kgforge.core import Resource
from kgforge.specializations.resources import Dataset


from bba_data_push.commons import (
    return_activity_payload,
    return_contribution,
    create_unresolved_payload,
    return_base_annotation,
    resolve_cellType,
    get_layer,
    forge_resolve)

from bba_data_push.push_nrrd_volumetricdatalayer import add_nrrd_props

import bba_data_push.constants as const

VOLUME_SCHEMA = "CellCompositionVolume"
SUMMARY_SCHEMA = "CellCompositionSummary"
COMP_SCHEMA = "CellComposition"
DENSITY_SCHEMA = f"{const.schema_volumetricdatalayer}"

COMMON_TYPES = ["Dataset", "Entity"]

subject = Resource.from_json(const.subject)

region_id = "http://api.brain-map.org/api/v2/data/Structure/997"
region_name = "root"

brainRegion = Resource(id = region_id,
                       label = region_name)

brainLocation_def = {
    "brainRegion": {"@id": f"mba:{region_id}", "label": f"{region_name}"},
    "atlasSpatialReferenceSystem": {
         "@type": ["BrainAtlasSpatialReferenceSystem", "AtlasSpatialReferenceSystem"],
         "@id": const.atlas_spatial_reference_system_id}
}

actions = ["toUpdate", "toPush"]

PART_KEY = "hasPart"
PATH_KEY = "path"

def create_densityPayloads(
    forge,
    atlasrelease_id,
    volume_path,
    densities_path,
    resource_tag,
    cellComps,
    output_dir,
    L,
):
    cellComps[DENSITY_SCHEMA] = {}
    resources_payloads = {}
    for action in actions:
        resources_payloads.update({"datasets_"+action: {DENSITY_SCHEMA: []}})

    me_description = "Morphology-Electric type density"
    # Parse input volume
    try:
        with open(volume_path) as volume_file:
            volume_content = json.loads(volume_file.read())
    except json.decoder.JSONDecodeError as error:
        L.error(f"{error} when opening the input volume json file.")
        exit(1)

    no_key = f"At least one '{PART_KEY}' key is required"
    len_vc = len(volume_content)
    if len_vc < 1:
        L.info(f"No key found in {volume_path}! {no_key}")
        exit(1)
    if PART_KEY not in volume_content:
        L.info(f"No {PART_KEY} key found anong the {len_vc} keys in {volume_path}! {no_key}")
        exit(1)
    if len_vc > 1:
            L.info(f"WARNING! More than one key ({len_vc}) found in {volume_path}, only '{PART_KEY}' will be considered")

    config = copy.deepcopy(const.config)
    mts = volume_content[PART_KEY]
    unresolved = []
    L.info(f"Parsing {len(mts)} M-types...")
    for mt in mts:
        mt_name = mt["label"]
        m_ct = resolve_cellType(forge, mt_name)
        if not m_ct["@id"]:
            unresolved.append( mt )
            continue
        m_annotation = get_cellAnnotation("M", m_ct)
        brainLocation_layer = copy.deepcopy(brainLocation_def)
        brainLocation_layer["layer"] = get_layer(forge, mt_name)

        ets = mt[PART_KEY]
        L.info(f"\nParsing {len(ets)} E-types for M-type '{mt_name}'...")
        for et in ets:
            et_name = et["label"]
            e_ct = resolve_cellType(forge, et_name)
            if not e_ct["@id"]:
                unresolved.append( et )
                continue

            et_part = et[PART_KEY][0]
            if not et_part.get(PATH_KEY):
                L.info(f"No 'path' available for m-type {mt_name}, e-type {et_name}. Skipping such density!")
                continue

            filepath = os.path.join(densities_path, et_part[PATH_KEY])
            header = None
            try:
                header = nrrd.read_header(filepath)
            except nrrd.errors.NRRDError as e:
                L.error(f"NrrdError: {e}")
                L.error(f"\nin parsing the header from {filepath} (m-type {mt_name}, e-type {et_name}). Skipping such density!")
                continue

            file_ext = os.path.splitext(os.path.basename(filepath))[1][1:]
            config["file_extension"] = file_ext

            e_annotation = get_cellAnnotation("E", e_ct)
            met = Resource(
                type = const.me_type,
                name = get_densName(mt_name, et_name),
                distribution = forge.attach(filepath, f"application/{file_ext}"),
                description = me_description,
                isRegisteredIn = const.isRegisteredIn,
                brainLocation = brainLocation_layer,
                dataSampleModality = const.cell_densiry_dsm,
                subject = const.subject,
                contribution = get_user_contribution(forge, L, True),
                annotation = [m_annotation, e_annotation],
                atlasRelease = get_atlasrelease_dict(atlasrelease_id),
                cellType = [m_ct, e_ct]
            )
            met = add_nrrd_props(met, header, config, const.voxel_vol)

            cellComps[DENSITY_SCHEMA][met.name] = met
            action = "toPush" # replace with some logic to pick 'toPush' or 'toUpdate'
            resources_payloads["datasets_"+action][DENSITY_SCHEMA].append( met )

    if unresolved:
        create_unresolved_payload(forge, unresolved, os.path.join(output_dir, "unresolved_densities"))
    else:
        L.info("No unresolved resources!")

    # Add Activity
# need to define provenance_metadata first
#    try:
#        activity_resource = return_activity_payload(forge, provenance_metadata)
#        if not activity_resource._store_metadata:
#            L.info("Existing activity resource not found in the Nexus destination "
#                f"project '{forge._store.bucket}'. A new activity will be registered")
#    except Exception as e:
#        L.error(f"{e}")
#        exit(1)
#
#   resources_payloads["activity"] = activity_resource

    resources_payloads["tag"] = resource_tag

    return resources_payloads


def create_cellCompositionVolume(
    forge,
    atlasrelease_id,
    volume_path,
    resources_payloads,
    name,
    description,
    resource_tag,
    cellComps,
    output_dir,
    L,
):
    schema = VOLUME_SCHEMA
    schema_id = forge._model.schema_id(type = schema)
    for action in actions:
        cellComps.update({"datasets_"+action: {schema_id: []}})

    # "AtlasDatasetRelease" is kept for backward compatiblity
    cell_compostion_volume_release = Dataset(forge, type = COMMON_TYPES + ["AtlasDatasetRelease", schema])

    cell_compostion_volume_release.atlasRelease = get_atlasrelease_dict(atlasrelease_id)

    cell_compostion_volume_release.about = ["https://bbp.epfl.ch/ontologies/core/bmo/METypeDensity"]
    #cell_compostion_volume_release.atlasSpatialReferenceSystem = Resource(id=atlas_release.spatialReferenceSystem.id,
    #    type=atlas_release.spatialReferenceSystem.type,
    #    _rev=atlas_release._store_metadata._rev)


    # Constants

    cell_compostion_volume_release.brainLocation = Resource(type="BrainLocation", brainRegion=brainRegion)

    cell_compostion_volume_release.subject = subject

    user_contribution = get_user_contribution(forge, L, True)
    cell_compostion_volume_release.contribution = user_contribution

    # Parse input volume
    try:
        with open(volume_path) as volume_file:
            volume_content = json.loads(volume_file.read())
    except json.decoder.JSONDecodeError as error:
        L.error(f"{error} when opening the input volume json file.")
        exit(1)

    # Create volume distribution
    volume_distribution = copy.deepcopy(volume_content)
    for mt in volume_distribution[PART_KEY]:
        mt_name = mt["label"]
        for et in mt[PART_KEY]:
            et_name = et["label"]
            dens_name = get_densName(mt_name, et_name)
            et_part = et[PART_KEY][0]
            if et_part.get("@id"):
                has_id = f"Density {dens_name} has an '@id' key, hence will not be modified"
                if et_part.get(PATH_KEY):
                    L.info(f"Warning! {has_id} and the 'path' key provided will be ignored")
                else:
                    L.info(has_id)
                continue

            if dens_name in cellComps[DENSITY_SCHEMA]:
                res = cellComps[DENSITY_SCHEMA][dens_name]
                if getattr(res, 'id', None):
                    et_part = et[PART_KEY][0]
                    et_part.pop(PATH_KEY) # PATH_KEY must be there by construction of cellComps[DENSITY_SCHEMA]
                    et_part["@id"] = res.id
                    et_part["@type"] = res.type
                    et_part["_rev"] = res._store_metadata["_rev"]
                else:
                    L.info(f"Resource {dens_name} has no 'id', probably it has not been registered.")
            else:
                L.info(f"No Resource {dens_name} found in cellComps[{DENSITY_SCHEMA}].")

    cell_compostion_volume_release.name = get_name(name, schema, user_contribution)

    distrib_filename = cell_compostion_volume_release.name.replace(" ", "_") + "_distrib.json"
    distrib_filepath = os.path.join(output_dir, distrib_filename)
    with open(distrib_filepath, "w") as volume_distribution_path:
        volume_distribution_path.write(json.dumps(volume_distribution, indent=4))
    cell_compostion_volume_release.distribution = forge.attach(distrib_filepath, content_type="application/json")

    if description:
        cell_compostion_volume_release.description = f"{description} ({schema})"

    cellComps[schema] = cell_compostion_volume_release
    action = "toPush" # replace with some logic to pick 'toPush' or 'toUpdate'
    cellComps["datasets_"+action][schema_id].append( cell_compostion_volume_release )


def create_cellCompositionSummary(
    forge,
    atlasrelease_id,
    resources_payloads,
    summary_path,
    name,
    description,
    cellComps,
    L,
):
    schema = SUMMARY_SCHEMA
    schema_id = forge._model.schema_id(type = schema)
    for action in actions:
        cellComps["datasets_"+action].update({schema_id: []})

    cell_compostion_summary_release = Dataset(forge, type = COMMON_TYPES + [schema])

    cell_compostion_summary_release.atlasRelease = get_atlasrelease_dict(atlasrelease_id)

    cell_compostion_summary_release.brainLocation = Resource(type="BrainLocation", brainRegion=brainRegion)

    cell_compostion_summary_release.subject = subject

    user_contribution = get_user_contribution(forge, L, True)
    cell_compostion_summary_release.contribution = user_contribution

    cell_compostion_summary_release.distribution = forge.attach(summary_path, content_type="application/json")

    cell_compostion_summary_release.name = get_name(name, schema, user_contribution)
    if description:
        cell_compostion_summary_release.description = f"{description} ({schema})"

    cellComps[schema] = cell_compostion_summary_release
    action = "toPush" # replace with some logic to pick 'toPush' or 'toUpdate'
    cellComps["datasets_"+action][schema_id].append( cell_compostion_summary_release )


def create_cellComposition(
    forge,
    atlasrelease_id,
    resources_payloads,
    name,
    description,
    resource_tag,
    cellComps,
    L,
):
    schema = COMP_SCHEMA
    schema_id = forge._model.schema_id(type = schema)

    for vol_sum in [VOLUME_SCHEMA, SUMMARY_SCHEMA]:
        if not getattr(cellComps[vol_sum], 'id', None):
            L.error(f"Error: the {vol_sum} has no 'id', probably it has not been registered")
            exit(1)

    for action in actions:
        cellComps.update({"datasets_"+action: {schema_id : []}})

    cell_compostion_release = Dataset(forge, type = COMMON_TYPES + [schema])
    cell_compostion_release.about = ["nsg:Neuron", "nsg:Glia"]
    cell_compostion_release.atlasRelease = get_atlasrelease_dict(atlasrelease_id)
    cell_compostion_release.atlasSpatialReferenceSystem = {
        "@id": const.atlas_spatial_reference_system_id,
        "@type": "AtlasSpatialReferenceSystem" }
    cell_compostion_release.brainLocation = {
        "@type": "BrainLocation",
        "brainRegion": {
            "@id": region_id,
            "label": "Whole mouse brain" }
    }

    user_contribution = get_user_contribution(forge, L, True)
    cell_compostion_release.contribution = user_contribution

    cell_compostion_release.cellCompositionVolume = {
        "@id": cellComps[VOLUME_SCHEMA].id,
        "@type": VOLUME_SCHEMA }

    # This is a list because may have more than one CellCompositionSummary
    cell_compostion_release.cellCompositionSummary = [{
        "@id": cellComps[SUMMARY_SCHEMA].id,
        "@type": SUMMARY_SCHEMA }]

    cell_compostion_release.name = get_name(name, schema, user_contribution)
    if description:
        cell_compostion_release.description = f"{description} ({schema})"

    cellComps[schema] = cell_compostion_release
    action = "toPush" # replace with some logic to pick 'toPush' or 'toUpdate'
    cellComps["datasets_"+action][schema_id].append( cell_compostion_release )

    cellComps["tag"] = resource_tag


def get_atlasrelease_dict(atlasrelease_id):
    atlasrelease_dict = {
        "@id": atlasrelease_id,
        "@type": ["AtlasRelease", "BrainAtlasRelease"]}
    return atlasrelease_dict

def get_user_contribution(forge, L, cellComp=True):
    try:
        user_contribution, log_info = return_contribution(forge, cellComp)
        L.info("\n".join(log_info))
    except Exception as e:
        L.error(f"{e}")
        exit(1)
    return user_contribution

def get_name(name, schema, user_contribution):
    if name:
        return f"{name} {schema}"
    else:
        return f"{schema} from {user_contribution[0].agent['name']}"

def get_densName(m, e):
    return "-".join([m, e])

def get_cellAnnotation(initial, cType):
    annotation = return_base_annotation(initial)
    annotation["hasBody"].update(cType)
    return annotation
