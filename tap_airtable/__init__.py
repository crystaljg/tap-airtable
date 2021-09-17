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


def normalize_name(field):
    # lowercase, remove non-alpha chars, replace space with underscore
    return re.sub("[^a-z ]+", "", field.lower()).replace(" ", "_")


def get_schema(base, table):
    """
    creates a raw schema for the given table in the airtable base
    """
    schema = {
        "type": ["null", "object"],
        "additionalProperties": False,
        "properties": {
            "airtable_id": {"type": ["string"]},
            "created_time": {
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

    table_data = base.all(table)

    for row in table_data:
        for field, value in row["fields"].items():

            clean_field = normalize_name(field)

            inferred_type = infer_type(value)
            schema['properties'][clean_field] = {
                "type": ["null", inferred_type]}

    return Schema(schema)


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


def discover_schemas(config):
    """
    Creates a dict containing schema for each table given in the config
    """
    base = Base(config["api_key"], config["base_id"])
    raw_schemas = {}

    for table in config["tables"]:
        stream = normalize_name(table)
        schema = get_schema(base, table)

        raw_schemas[stream] = schema

    return raw_schemas


def discover(config):
    LOGGER.info("Starting discover")
    raw_schemas = discover_schemas(config)

    streams = []
    for stream_id, schema in raw_schemas.items():
        # all streams get replicated by default
        stream_metadata = {"inclusion": "automatic",
                           "table-key-properties": ["id"],
                           "replication_method": "FULL_TABLE"}
        key_properties = ["airtable_id"]
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
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


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
            key_properties=stream.key_properties,
        )

        # TODO: delete and replace this inline function with your own data retrieval process:
        def tap_data(): return [{"id": x, "name": "row${x}"}
                                for x in range(1000)]

        max_bookmark = None
        for row in tap_data():
            # TODO: place type conversions or transformations here

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state(
                        {stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
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
