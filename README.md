# tap-airtable

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [FIXME](http://example.com)
- Extracts the following resources:
  - [FIXME](http://example.com)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


# Config
Because each Airtable base has custom tables and table names unique to each base, each tablename must be delineated in the config. In the future, Airtable's [Metadata API](https://airtable.com/api/meta) may be used in schema discovery, but registration for this API is currently closed.

---

Copyright &copy; 2018 Stitch
