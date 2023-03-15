import json
from field_information import name_to_field_and_key

print(json.dumps(list(name_to_field_and_key.keys())))