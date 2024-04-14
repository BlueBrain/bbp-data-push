"""
Create an Atlas Release , to push into Nexus.
"""
import os

from kgforge.core import Resource

import bba_data_push.commons as comm
from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources

atlas_release_properties = ["parcellationOntology", "parcellationVolume",
    "hemisphereVolume", "placementHintsDataCatalog", "directionVector",
    "cellOrientationField"]


def create_atlas_release(atlas_release_id, brain_location_prop,
    reference_system_prop, brain_template_prop, subject_prop, ont_prop,
    par_prop, hem_prop, ph_catalog_prop, dv_prop, co_prop, contribution, name,
    description
):
    atlas_release = create_base_resource(comm.all_types[comm.atlasrelaseType],
        brain_location_prop, reference_system_prop, subject_prop, contribution,
        None, name, description, None, atlas_release_id)
    atlas_release.spatialReferenceSystem = reference_system_prop
    atlas_release.brainTemplateDataLayer = brain_template_prop
    atlas_release.parcellationOntology = ont_prop
    atlas_release.parcellationVolume = par_prop
    atlas_release.hemisphereVolume = hem_prop
    atlas_release.placementHintsDataCatalog = ph_catalog_prop
    atlas_release.directionVector = dv_prop
    atlas_release.cellOrientationField = co_prop
    atlas_release.releaseDate = comm.get_date_prop()

    return atlas_release


def validate_atlas_release(atlas_release_id, forge, resource_tag, logger):
    logger.info(f"Validating AtlasRelease id {atlas_release_id} at tag '{resource_tag}'")
    atlas_release_res = forge.retrieve(atlas_release_id, version=resource_tag)
    if not atlas_release_res:
        logger.error(f"No Resource found with Id {atlas_release_id} and tag '{resource_tag}'")
        return False

    atlas_release_rev = comm.get_resource_rev(forge, atlas_release_id,
                                              resource_tag, cross_bucket=True)
    atlas_release_prop_ref = comm.get_property_type(atlas_release_id,
        comm.all_types[comm.atlasrelaseType], atlas_release_rev)

    for prop in atlas_release_properties:
        existing_prop = getattr(atlas_release_res, prop, None)
        if not existing_prop:
            logger.error(f"No property '{prop}' found in AtlasRelease Id {atlas_release_id}")
            return False
        # Retrieving property Resource
        prop_id = existing_prop.id
        prop_res = forge.retrieve(prop_id, version=resource_tag)
        if not prop_res:
            logger.error(f"No Resource found with Id {prop_id} and tag '{resource_tag}'")
            return False
        # Validating Resource property
        atlas_release_prop = getattr(prop_res, "atlasRelease", None)
        if atlas_release_prop != atlas_release_prop_ref:
            logger.error(f"The atlasRelease property of Resource Id {prop_id}:"
                f"\n{atlas_release_prop}\n\nis different from the reference:"
                f"\n{atlas_release_prop_ref}")
            return False
        logger.info(f"Validated property '{prop}'")

    logger.info(f"The selected properties of AtlasRelease Id {atlas_release_id}"
        f" contain the correct 'atlasRelease' property:\n{atlas_release_prop_ref}")
    return True


def create_volumetric_property(res_name, res_type, res_id, file_path,
    atlas_release_prop, atlas_release_id_orig, forge, subject_prop,
    brain_location_prop, reference_system_prop, contribution, derivation,
    resource_tag, logger, dryrun=False
):
    vol_res = create_volumetric_resources((file_path,), res_type,
        atlas_release_prop, forge, subject_prop, brain_location_prop,
        reference_system_prop, contribution, derivation, logger, res_name)[0]
    if res_id:
        vol_res.id = res_id
    comm._integrate_datasets_to_Nexus(forge, [vol_res], res_type,
        atlas_release_id_orig, resource_tag, logger, dryrun=dryrun)
    vol_prop = comm.get_property_type(vol_res.id, res_type)

    return vol_prop


def create_ph_catalog_distribution(ph_resources, filepath_to_brainregion,
                                   ph_res_to_filepath, forge, debug=False):
    placementHints = []
    voxelDistanceToRegionBottom = {}
    for ph_resource in ph_resources:
        a_ph_item = dict()
        a_ph_item["@id"] = ph_resource.get_identifier()
        a_ph_item["_rev"] = ph_resource._store_metadata.get("_rev")
        a_ph_item["distribution"] = {"atLocation": {
            "location": ph_resource.distribution.atLocation.location},
            "name": ph_resource.distribution.name}
        if ph_resource.distribution.name == "[PH]y.nrrd":
            voxelDistanceToRegionBottom = a_ph_item
        elif (ph_resource.distribution.name != "Isocortex_problematic_voxel_mask.nrrd"):
            # get layer from filename
            layer_prop_resource_list = comm.get_placementhintlayer_prop_from_name(
                forge, ph_resource.distribution.name)
            layer_prop = layer_prop_resource_list[0]
            layer_id = layer_prop.get_identifier()
            ph_resource_filename = os.path.basename(ph_res_to_filepath[ph_resource.get_identifier()])
            brain_region_names = filepath_to_brainregion[ph_resource_filename]
            if not isinstance(brain_region_names, list):
                raise Exception(f"The type of the '{ph_resource_filename}' "
                    f"value in the placement hints metadata is not a list")
            regions = {}
            for brain_region_name in brain_region_names:
                # resolve brain_region_name and get leaf under layer
                brain_region_prop = comm.get_property_label(
                    comm.Args.brain_region, brain_region_name, forge)
                brain_region_key = brain_region_prop.notation if hasattr(
                    brain_region_prop, "notation") else brain_region_name
                brain_region_id = brain_region_prop.get_identifier()
                regions[brain_region_key] = {
                    "@id": brain_region_id,
                    "hasLeafRegionPart": [],
                    "layer": {"@id": layer_id, "label": layer_prop.label}
                }
                brain_region_layer_leaves = forge.search(
                    {"^hasLeafRegionPart": {"id": brain_region_id},
                     "hasLayerLocationPhenotype": layer_id}, cross_bucket=True,
                    search_in_graph=False, distinct=True, debug=debug)
                if layer_id == "http://purl.obolibrary.org/obo/UBERON_0005395":  # layer 6, need to also collect layer 6a and layer 6b:
                    brain_region_layer6a_leaves = forge.search(
                        {"^hasLeafRegionPart": {"id": brain_region_id},
                         "hasLayerLocationPhenotype": "https://bbp.epfl.ch/ontologies/core/bmo/neocortex_layer_6a"},
                        cross_bucket=True, search_in_graph=False, distinct=True,
                        debug=debug)
                    brain_region_layer6b_leaves = forge.search(
                        {"^hasLeafRegionPart": {"id": brain_region_id},
                         "hasLayerLocationPhenotype": "http://purl.obolibrary.org/obo/UBERON_8440003"},
                        cross_bucket=True, search_in_graph=False, distinct=True,
                        debug=debug)
                    brain_region_layer_leaves.extend(
                        brain_region_layer6a_leaves)
                    brain_region_layer_leaves.extend(
                        brain_region_layer6b_leaves)

                if not brain_region_layer_leaves:
                    raise Exception(f"No leaf regions found for region id "
                        f"'{brain_region_id}' and layer id '{layer_id}'")
                for brain_region_layer_leave in brain_region_layer_leaves:
                    regions[brain_region_key]["hasLeafRegionPart"].append(
                        brain_region_layer_leave.notation)
            a_ph_item["regions"] = regions
            a_ph_item["layer"] = layer_prop.label
            placementHints.append(a_ph_item)
    placementHints_sorted = sorted(placementHints, key=lambda x: x["layer"])
    for ph in placementHints_sorted:
        ph.pop("layer")
    return {"placementHints": placementHints_sorted,
            "voxelDistanceToRegionBottom": voxelDistanceToRegionBottom}


def create_base_resource(res_type, brain_location_prop, reference_system_prop,
    subject_prop, contribution, atlas_release_prop=None, name=None,
    description=None, about=None, res_id=None):
    resource = Resource(
        type=res_type,
        brainLocation=brain_location_prop,
        atlasReleaseSpatialReferenceSystem=reference_system_prop,
        subject=subject_prop,
        contribution=contribution)

    if atlas_release_prop:
        resource.atlasRelease = atlas_release_prop
    if name:
        resource.name = name
    if description:
        resource.description = description
    if about:
        resource.about = about

    if res_id:
        resource.id = res_id

    return resource
