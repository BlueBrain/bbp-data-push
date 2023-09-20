"""
Create an Atlas Release , to push into Nexus.
"""

import json
from kgforge.core import Resource

from bba_data_push.logging import create_log_handler
import bba_data_push.commons as comm
from bba_data_push.push_nrrd_volumetricdatalayer import create_volumetric_resources

logger = create_log_handler(__name__, "./create_atlas_release.log")


def create_atlas_release(atlas_release_id, brain_location_prop,
        reference_system_prop, brain_template_prop, subject_prop, ont_prop, par_prop,
        hem_prop, ph_catalog_prop, dv_prop, co_prop, contribution, name, description):

    atlas_release = create_base_resource(comm.all_types[comm.atlasrelaseType],
        brain_location_prop, reference_system_prop, subject_prop, contribution, None,
        name, description, atlas_release_id)
    atlas_release.brainTemplateDataLayer = brain_template_prop
    atlas_release.parcellationOntology = ont_prop
    atlas_release.parcellationVolume = par_prop
    atlas_release.hemisphereVolume = hem_prop
    atlas_release.placementHintsDataCatalogDataCatalog = ph_catalog_prop
    atlas_release.directionVector = dv_prop
    atlas_release.cellOrientationField = co_prop

    return atlas_release


def create_volumetric_property(res_name, res_type, res_id, file_path, atlas_release_prop,
    atlas_release_id_orig, forge, subject_prop, brain_location_prop, reference_system_prop,
    contribution, derivation, resource_tag, logger):

    vol_res = create_volumetric_resources((file_path,), res_type, atlas_release_prop,
        forge, subject_prop, brain_location_prop, reference_system_prop,
        contribution, derivation, logger, res_name)[0]
    if res_id:
        vol_res.id = res_id
    comm._integrate_datasets_to_Nexus(forge, [vol_res], res_type,
                                      atlas_release_id_orig, resource_tag, logger)
    vol_prop = comm.get_property_type(vol_res.id, res_type)

    return vol_prop

def create_ph_catalog_distribution(ph_resources, filepath_to_brainregion_json_file, ph_res_to_filepath, forge):

   placementHints = []
   voxelDistanceToRegionBottom = {}
   with open(filepath_to_brainregion_json_file, "r") as f:
        filepath_to_brainregion= json.load(f)
   for ph_resource in ph_resources:
        a_ph_item = {}
        a_ph_item["id"] = ph_resource.get_identifier()
        a_ph_item["_rev"] = ph_resource._store_metadata._rev
        a_ph_item["distribution"] = {"atLocation": {"location":ph_resource.distribution.atLocation.location}, "name":ph_resource.distribution.name}
        if ph_resource.distribution.name == "[PH]y.nrrd":
            voxelDistanceToRegionBottom  = a_ph_item
        else:
            # get layer from filename
            layer_label = comm.get_placementhintlayerlabel_from_name(forge, ph_resource.distribution.name)
            layer_prop_resource_list = comm.get_layer(forge, layer_label, initial="layer", regex="_(\d){1,}", split_separator=None, layer_number_offset=1)
            layer_prop = layer_prop_resource_list[0]
            ph_resource_filepath = ph_res_to_filepath[ph_resource.get_identifier()]
            brain_region_names = filepath_to_brainregion[ph_resource_filepath]
            regions = {}
            for brain_region_name in brain_region_names:
                regions[brain_region_name] = {"hasLeafRegionPart":[], "layer":layer_prop}
                # resolve brain_region_name and get leaf under layer
                brai_region_prop = comm.get_property_label(comm.Args.brain_region, brain_region_name, forge)
                
                brain_region_layer_leaves = forge.search({"^hasLeafRegionPart":brai_region_prop.get_identifier(),
                                                "hasLayerLocationPhenotype":layer_prop.get_identifier()})
                
                for brain_region_layer_leave in brain_region_layer_leaves:
                    regions[brain_region_name]["hasLeafRegionPart"].append(brain_region_layer_leave.notation)
            a_ph_item["regions"] = regions
            placementHints.append(a_ph_item)
   return {"placementHints":placementHints, "voxelDistanceToRegionBottom":voxelDistanceToRegionBottom}

def create_base_resource(res_type, brain_location_prop, reference_system_prop, subject_prop,
    contribution, atlas_release_prop=None, name=None, description=None, res_id=None):
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

    if res_id:
        resource.id = res_id

    return resource
