"""
Create a 'CellCompositionVolume', a 'CellCompositionSummary' and the corresponding
'CellComposition' resource payload to push into Nexus.
Link to BBP Atlas pipeline confluence documentation:
https://bbpteam.epfl.ch/project/spaces/x/rS22Ag
"""

from kgforge.specializations.resources import Dataset

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
    res_type = ["Dataset", "Entity", schema]
    # "AtlasDatasetRelease" is kept for backward compatibility
    if schema == "CellCompositionVolume":
        res_type.append("AtlasDatasetRelease")

    expanded_about=[]
    for a in about:
        expanded_about.append(forge.get_model_context().expand(a))

    base_res = Dataset(forge, type=res_type,
        atlasRelease = atlas_release,
        about = expanded_about,
        brainLocation = brain_location,
        subject = subject,
        contribution = contribution,
        derivation = [derivation],
        name = get_name(name, schema, contribution)
                       )
    if description:
        base_res.description = f"{description} ({schema})"

    if file_path:
        base_res.distribution = forge.attach(file_path, content_type="application/json")
        base_res.temp_filepath = file_path

    if reference_system_prop:
        base_res.atlasSpatialReferenceSystem = reference_system_prop

    return base_res

def get_name(name, schema, user_contribution):
    if name:
        return f"{name} {schema}"
    else:
        username = user_contribution[0].agent['@id'].split("/")[-1]
        return f"{schema} from {username}"
