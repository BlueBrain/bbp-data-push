'''push modules common functions'''
import json
import copy
import requests
import jwt
from kgforge.core import Resource

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
            raise ValueError(f"The type provided ({voxel_type }) is not compatible with the "\
                             "number of component per voxel.")

# For now, brain region names are taken from Allen api
def getBrainRegionNameAllen(region_id):
    url_base = "http://api.brain-map.org/api/v2/data/Structure/"
    response = requests.get(f"{url_base}{str(region_id)}")
    response_parsed = json.loads(response.text)

    if response.status_code >= 300 or not response_parsed["success"] or response_parsed["num_rows"] == 0:
        return None # TODO raise an error ?

    brain_region_info = response_parsed["msg"][0]
    return brain_region_info["name"]


def getHierarchyContent(input_hierarchy, config_content, hierarchy_tag):
    
    hierarchy_path = None
    try: 
        for hierarchy_file in input_hierarchy:
            if hierarchy_file == config_content["GeneratedHierarchyJson"][hierarchy_tag]:
                hierarchy_path = hierarchy_file
    except KeyError: 
        raise KeyError(f"The hierarchy files in input do not contain the right one. The correct "\
                       f"hierarchy {config_content['GeneratedHierarchyJson'][hierarchy_tag]} "\
                       "is missing.")

    return hierarchy_path

def getBrainRegionName(region_id, hierarchy_path, flat_tree: dict):
    region_name = None
    region_id = int(region_id)
    if not flat_tree:
        with open(hierarchy_path, 'r') as hierarchy_file:
            hierarchy = json.load(hierarchy_file)
            try:
                hierarchy = hierarchy['msg'][0]
            except KeyError:
                raise KeyError("Wrong input. The AIBS hierarchy json file dict-structure is "\
                               "expected.")
            tree_copy = copy.deepcopy(hierarchy)
            root_node = tree_copy
            flat_tree = {}
            node_to_explore = [root_node]
            while len(node_to_explore):
                node = node_to_explore.pop()
                node_id = node['id']
                flat_tree[node_id] = node
                children_ids = []
                if 'children' in node:
                    for child in node['children']:
                        children_ids.append(child['id'])
                        node_to_explore.append(child)
                node['children'] = children_ids
            for node_id in flat_tree:
                node = flat_tree[node_id]
                node_to_explore = [] + node['children']
                while len(node_to_explore):
                    child_id = node_to_explore.pop()
                    child_node = flat_tree[child_id]
                    node_to_explore = node_to_explore + child_node['children']
    try:
        hierarchy = flat_tree
        region_name = hierarchy[region_id]['name']
    except KeyError:
        raise KeyError(f"Region name corresponding to id '{region_id}' is not found in the "\
                       f"hierarchy json file ({hierarchy_path}).")
        region_name = getBrainRegionNameAllen(region_id)

    return region_name, hierarchy


# Only used by push-nrrd
def getExtraValues (resource, extra_keys_values):
    
    extra_informations = {}
    for t in extra_keys_values:
        extra_informations[t[0]] = t[1]
    resource.extra_informations = extra_informations
    
    return resource

# Multiple contributors not handled yet
def addContribution(forge, resource):
    token_info = jwt.decode(forge._store.token, options={'verify_signature': False})
    #print(f"TOKEN: {token_info}")
    agent_email = token_info["email"]
    agent_name = token_info["given_name"]
    agent_familyname = token_info["family_name"]
    #agent_email = "charlotte.lorin@epfl.ch"
    agent_given_name = "Charlotte"
    agent_family_name = "Bussard" 
    #agent_mail
    agent_id = forge.resolve(agent_family_name, target="agents", scope = "agent", type="Person")
    if not agent_id:
        raise ValueError(f"Error: The agent email {agent_email} extracted from the user "\
                         "token does not correspond to an agent registered in the 'agents' "\
                         "project in Nexus.")

    contribution = Resource(type = "Contribution", agent = agent_id)
    
    return contribution

#    contribution = {}
#    for c in contributor_name:
