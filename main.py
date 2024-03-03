import sys
import json
import avro
from collections import OrderedDict
from confluent_kafka.schema_registry import SchemaRegistryClient
from avro.schema import UnionSchema, ArraySchema, MapSchema, RecordSchema, SchemaParseException

with open('schema-registry.conf') as f:
    conf = json.loads(f.read())

schemaRegistryClient = SchemaRegistryClient(conf)


def has_circular_reference(schema, visited=None, name=None):
    if visited is None:
        visited = OrderedDict()
    if isinstance(schema, ArraySchema):
        return has_circular_reference(schema.items, visited.copy(), name + "[]")
    elif isinstance(schema, MapSchema):
        return has_circular_reference(schema.values, visited.copy(), name + "{}")
    elif isinstance(schema, RecordSchema):
        record_name = schema.name
        if record_name in visited.keys():
            # print("{} ==> {}"
            #       .format(record_name, ";".join([("{}:{}".format(value, key)) for key, value in visited.items()])))
            return True
        visited[record_name] = str(name or '')
        return any([has_circular_reference(field.type, visited.copy(), field.name) for field in schema.fields])
    elif isinstance(schema, UnionSchema):
        return any([has_circular_reference(skjema, visited.copy(), name) for skjema in schema.schemas])
    else:
        return False


def avro_crc(schema_id, schema_str):
    try:
        return has_circular_reference(avro.schema.parse(schema_str))
    except SchemaParseException as err:
        print("{} -> {}".format(schema_id, err), file=sys.stderr)
        return None


for subject in sorted(schemaRegistryClient.get_subjects()):
    # print("{}".format(subject))
    if subject.endswith("-value"):
        schemaObj = schemaRegistryClient.get_latest_version(subject)
        schemaId = "{}:{}[{}]".format(subject, schemaObj.version, schemaObj.schema_id)
        if schemaObj.schema.schema_type == "AVRO":
            result = avro_crc(schemaId, schemaObj.schema.schema_str)
            if result:
                print("{}".format(schemaId))
                print("========================================================")

# skjema = avro.schema.parse(open("test.avsc", "rb").read())
# result = crc(skjema)
# print("Result: {}".format(result))
