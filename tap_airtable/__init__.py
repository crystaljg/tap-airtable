#!/usr/bin/env python3
import os
import json
import re
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from pyairtable import Base


REQUIRED_CONFIG_KEYS = ['api_key', 'base_id', 'tables']
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def normalize(field):
    """
    Lowercases, removes non-alpha characters, and replaces space with underscore
    """
    return re.sub("[^a-z ]+", "", field.lower()).replace(" ", "_")


def get_table_schema(base, table):
    """
    creates a raw schema for the given table in the airtable base
    """
    raw_schema = {
        "type": ["null", "object"],
        "additionalProperties": "false",

        "properties": {
            "airtable_id": {"type": "string"},
            "createdTime": {
                "anyOf": [
                    {
                        "type": ["null", "string"],
                        "format": "date-time"
                    },
                    {
                        "type": ["null", "string"]
                    }
                ]
            }
        }
    }

  # TODO: throw error if table in config doesn't match table in base
  # TODO: change this to table.iterate
    table_data = base.all(table)

    for row in table_data:
        for field, value in row["fields"].items():

            inferred_type = infer_type(value)
            raw_schema["properties"][field] = {
                "type": ["null", inferred_type]}

    return raw_schema


def infer_type(datum):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        int(str(datum))
        return 'integer'
    except (ValueError, TypeError):
        pass
    try:
        float(str(datum))
        return 'number'
    except (ValueError, TypeError):
        pass

    return 'string'


def create_schema_files(config):
    """
    Creates a schema.json for each table given in the config
    """
    base = Base(config["api_key"], config["base_id"])

    for table in config["tables"]:
        table_schema = get_table_schema(base, table)

        with open(f"{get_abs_path('schemas')}/{table}.json", "w") as fout:
            fout.write(json.dumps(table_schema))


def load_schemas(config):
    """ Load schemas from schemas folder"""
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover(config):
    LOGGER.info("Starting discover")

    # each airtable base is different so the schemas must be created
    create_schema_files(config)
    raw_schemas = load_schemas(config)

    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = metadata.get_standard_metadata(schema.to_dict(),
                                                         stream_id,
                                                         replication_method='FULL_TABLE')
        for field in stream_metadata:
            field["metadata"]["selected"] = "true"

        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=["airtable_id"],
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    catalog = Catalog(streams)
    return catalog  # Catalog(streams)


def tap_table_data(base, table):
    for table in base.iterate(table, page_size=100):
        for record in table:

            flat_record = {}

            flat_record["airtable_id"] = record["id"]
            flat_record["createdTime"] = record["createdTime"]
            for field, value in record["fields"].items():
                flat_record[field] = value

            yield flat_record


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        table = Base(config["api_key"], config["base_id"])

        for row in tap_table_data(table, stream.tap_stream_id):
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(config)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(config)
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
