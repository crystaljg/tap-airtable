# tap-airtable

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from an [Airtable base](https://airtable.com/api)
- Extracts the entirety of each table named in the config file
- Outputs the schema for each resource


# Config
Because each Airtable base has custom tables and table names unique to each base, the schema is newly discovered upon each `discover`. In the future, Airtable's [Metadata API](https://airtable.com/api/meta) may be used in schema discovery, but registration for this API is currently closed. 

---

Copyright &copy; 2018 Stitch
