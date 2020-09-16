import requests
import json
import yaml
from kgforge.core import Resource

# Add the entered forge parameters to the yml/json config file
def createForgeConfig(config_file, env, org, proj, token):
    # These are the default environment supported by BBP
    default_environments = {
    "dev": "https://dev.nexus.ocp.bbp.epfl.ch/v1",
    "staging": 'https://staging.nexus.ocp.bbp.epfl.ch/v1',
    "prod": "https://bbp.epfl.ch/nexus/v1"
    }
    if env in default_environments:
        env = default_environments[env]
    with open(config_file, 'r+') as f: #r+ need to reset the index + truncate its content
        if config_file.endswith('.yml'):
            config = yaml.safe_load(f)
            config['Store']['endpoint'] = env
            config['Store']['bucket'] = org + '/' + proj
            config['Store']['token'] = token
            f.seek(0)
            try:
                yaml.dump(config, f, default_flow_style=False) #keep the yml style
                f.truncate()
            except yaml.YAMLError as exc:
                print(exc)
        elif config_file.endswith('.txt') or config_file.endswith('.json'):
            config = json.load(f)
            config['Store']['endpoint'] = env
            config['Store']['bucket'] = org + '/' + proj
            config['Store']['token'] = token
            f.seek(0)
            try: 
                json.dumps(config)
                f.truncate()
            except ValueError as exc:
                print(exc)
        else:
            print("The forge configuration file' format is incorrect")
        
    return config_file

# Simplify it ? resolve automatically the voxel_type ?
def getVoxelType(voxel_type, component_size):
    
    # this could be "multispectralIntensity", "vector"
    default_sample_type_multiple_components = "vector"

    # This could be "intensity", "mask", "label"
    default_sample_type_single_component = "intensity"
    
    allow_multiple_components = {
    "multispectralIntensity": True,
    "vector": True,
    "intensity": False,
    "mask": False,
    "label": False
    }
        
    if not voxel_type and component_size == 1:
        return default_sample_type_single_component
    elif not voxel_type and component_size > 1:
        return default_sample_type_multiple_components
    elif voxel_type:
        if component_size > 1 and allow_multiple_components[voxel_type]:
            return voxel_type
        elif component_size == 1 and not allow_multiple_components[voxel_type]:
            return voxel_type
        else:
            raise ValueError("The type provided (" + voxel_type + ") is not compatible with the number of component per voxel.")
            exit(1)

# For now, brain region names are taken from Allen api
def getBrainRegionNameAllen(region_id):
    url_base = "http://api.brain-map.org/api/v2/data/Structure/"
    response = requests.get( url_base + str(region_id))
    response_parsed = json.loads(response.text)

    if response.status_code >= 300 or not response_parsed["success"] or response_parsed["num_rows"] == 0:
        return None # TODO raise an error ?

    brain_region_info = response_parsed["msg"][0]
    return brain_region_info["name"]

# Not used but maybe should be instead of picking information on Allen site
def getBrainRegionNameNexus(region_id, forge):
    region_name = None
    try:
        region_info = forge.retrieve(region_id)
        region_name = region_info["label"]
    except Exception as e:
        print(e)
        region_name = getBrainRegionNameAllen(region_id)
    return region_name


# Only used by push-nrrd
def getExtraValues (resource, extra_keys_values):
    
    extra_informations = {}
    for t in extra_keys_values:
        extra_informations[t[0]] = t[1]
    resource.extra_informations = extra_informations
    
    return resource

# Multiple contributors nor handled yet
def addContribution(resource, forge, contributor_name):
     
    # Resolving works only with givenName (and not familyName)
    agent = forge.resolve(contributor_name, target="agents", scope = "agent", type="Person")
    print('contributor: ', agent)
    contribution = Resource(type = "Contribution", agent = agent)
    resource.contribution = contribution
    
    return resource

#    contribution = {}
#    for c in contributor_name: