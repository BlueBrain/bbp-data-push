"""
Brain meshes
"""

import os
from kgforge.core import Resource 
from commons import getBrainRegionNameAllen


def createDataset(forge, mesh_local_path, description, id_spatial_ref, id_atlas_release, spatial_unit):
    
    dataset = []
    i = 0
    for f in mesh_local_path:
        
        region_id = os.path.splitext(os.path.basename(f))[0]
        extension = os.path.splitext(os.path.basename(f))[1][1:]
        region_name = getBrainRegionNameAllen(region_id)
        print("Pushing region ", region_id)
        
        if region_name is None:
            print("❌ ", f, " Name not matching a region.")
            continue
        
        atlas_reference_system_payload = forge.retrieve(id = id_spatial_ref, cross_bucket = True)

        # Add the link to the spatial ref system
        isRegisteredIn = {
            "@type": ["BrainAtlasSpatialReferenceSystem","AtlasSpatialReferenceSystem"],
            "@id": atlas_reference_system_payload.id 
        }
            
        brainLocation = {
            "brainRegion": {
                "@id": "http://api.brain-map.org/api/v2/data/Structure/" + str(region_id),
                "label": region_name
            },

            "atlasSpatialReferenceSystem": {
                "@type": ["BrainAtlasSpatialReferenceSystem","AtlasSpatialReferenceSystem"],
                "@id": atlas_reference_system_payload.id 
            }
        }
        
        distribution_file = forge.attach(f, content_type = "application/" + extension) #content_type = "application/" + extension
        
        resource_mesh = Resource(
            type = ["Mesh", "BrainParcellationMesh"],
            name = region_name.title() + " mesh",
            description= description + " - " + region_name.title() + " (ID: " + region_id + ")",
            atlasRelease = {"@id": id_atlas_release},
            isRegisteredIn = isRegisteredIn,
            brainLocation = brainLocation,
            spatialUnit = spatial_unit,
            distribution = distribution_file
            ) # 
        
        # TODO Optimal manner to add these attributes to a batch of resources
        #getExtraValues (resource_mesh, extra_keys_values)
        #addContribution(resource_mesh, forge, contributor_name)
        
        dataset.append(resource_mesh)
        
        i += 1
    
    return dataset
