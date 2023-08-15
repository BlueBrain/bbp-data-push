"""
Create an Atlas Release , to push into Nexus.
"""

from kgforge.core import Resource

from bba_data_push.logging import create_log_handler
import bba_data_push.commons as comm

logger = create_log_handler(__name__, "./create_atlas_release.log")


def create_atlas_release(atlas_release_id, brain_location_prop,
        reference_system_prop, brain_template_prop, subject_prop, ont_prop, par_prop,
        hem_prop, ph_prop, contribution, name, description):

    atlas_release = create_base_resource(comm.all_types[comm.atlasrelaseType],
        brain_location_prop, reference_system_prop, subject_prop, contribution, None,
        name, description, atlas_release_id)
    atlas_release.brainTemplateDataLayer = brain_template_prop
    atlas_release.parcellationOntology = ont_prop
    atlas_release.parcellationVolume = par_prop
    atlas_release.hemisphereVolume = hem_prop
    if ph_prop:
        atlas_release.placementHints = ph_prop

    return atlas_release


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
