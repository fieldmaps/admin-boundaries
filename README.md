# Edge Matcher

This repository represents the final step of a multi-source data processing pipeline to create global edge-matched administrative boundary layers. Datasets are updated when new layers are published by each source, typically done on a weekly basis. Data can be accessed through the following table, as well as this URL which provides additional metadata such as date last updated: [data.fieldmaps.io/edge-matched.json](https://data.fieldmaps.io/edge-matched.json)

| Layer Type (ADM 0-4) | COD + geoBoundaries                                                                                  | geoBoundaries                                                                        |
| -------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Boundary Polygons    | [Humanitarian Polygons](https://data.fieldmaps.io/edge-matched/humanitarian/adm_polygons.gpkg.zip)   | [Open Polygons](https://data.fieldmaps.io/edge-matched/open/adm_polygons.gpkg.zip)   |
| Cartographic Lines   | [Humanitarian Lines](https://data.fieldmaps.io/edge-matched/humanitarian/adm_lines.gpkg.zip)         | [Open Lines](https://data.fieldmaps.io/edge-matched/open/adm_lines.gpkg.zip)         |
| Label Points         | [Humanitarian Points](https://data.fieldmaps.io/edge-matched/humanitarian/adm_points.gpkg.zip)       | [Open Points](https://data.fieldmaps.io/edge-matched/open/adm_points.gpkg.zip)       |
| Attributes Only      | [Humanitarian Attributes](https://data.fieldmaps.io/edge-matched/humanitarian/adm_polygons.xlsx.zip) | [Open Attributes](https://data.fieldmaps.io/edge-matched/open/adm_polygons.xlsx.zip) |

## Sources

Individual administrative layers are sourced from groups who provide their own curated services.

- [Common Operational Datasets (COD)](https://cod.unocha.org/): by United Nations Office for the Coordination of Humanitarian Affairs (UN OCHA). Boundary data organized by Field Information Services (FIS), with vetting performed by [Information Technology Outreach Services (ITOS)](https://cviog.uga.edu/international-center/).
- [geoBoundaries](https://www.geoboundaries.org/): by the University of William & Mary geoLab in Williamsburg, Virginia.

These groups of sources are combined together into the following thematic outputs:

- Humanitarian (aka operational): Utilizes COD layers when available, falling back on geoBoundaries to fill in gaps. Commonly licensed under Creative Commons Attribution for Intergovernmental Organizations, although some layers may be more restrictive.
- Open (aka academic): Uses geoBoundaries exclusively as a source group. Commonly licensed under Open Data Commons Open Database License, as many sources are derived from OpenStreetMap, but a variety of other permissive licenses are included as well.

In the future, [Second Administrative Level Boundaries (SALB)](https://www.unsalb.org) will be added as a third source for the Authoritative (aka member state provided) thematic output.

## Methodology

Being the last stage of a multi-step pipeline, other related repositories are involved in the production of this dataset, each with a discrete and self-contained purpose. Overall, data goes through the following 6 steps from originals to edge-matched:

1. Originals: Represents data as collected from the source. For some layers these are downloaded manually from web portals. For others, programatic API access is available.
2. Normalized: Within a single source group, there may be small variations in the way layers are formatted. This stage preforms operations to ensure file, layer, and attribute names are consistent within the same source group.
3. Standardized: Between multiple source groups, this step conditions attributes in a common format suitable for merging together in a hybrid dataset. Here, versioned ID's are assigned to each admin area.
4. Extended: To resolve gaps between layers, a voronoi-like algorithm is applied along polygon edges to extend them outward. The extent of the resulting polygon is 100% larger in each direction, purposefully over-sized for later clipping.
5. Clipped: To resolve overlaps between layers, extended layers are clipped to a pre-defined ADM0 layer.
6. Edge-Matched: An aggregation of all clipped layers are assembled into the final output.

### Intermediate Data

The outputs of intermediate steps 2-5 can be accessed through the following links. These URLs are used internally to keep track of metadata about layers within source groups.

- COD: [data.fieldmaps.io/cod.json](https://data.fieldmaps.io/cod.json)
- geoBoundaries: [data.fieldmaps.io/geoboundaries.json](https://data.fieldmaps.io/geoboundaries.json)

### Data Adapter (Step 1-3)

The [Data Adapter](https://github.com/fieldmaps/data-adapter) repository is responsible for processing originals into normalized and standardized versions. Since each transformation at this stage involves only re-naming or files / layers / fields, and doesn't require geometry transformation or spatial relations, it is contained within this sub-pipeline.

### Polygon to Voronoi (Step 4)

The [Polygon to Voronoi](https://github.com/fieldmaps/polygon-voronoi) repository is a general purpose tool that has been broken out into its own self-contained unit. It does not modify attributes in any way, and the geometry transformations do not rely on any external data as overlays.

### ADM0 Template (Step 5)

The [ADM0 Template](https://github.com/fieldmaps/adm0-template) repository combines UNmap, used for political boundaries, with OpenStreetMap, used for coastline boundaries. The result is a very high resolution ADM0 boundary layer that is used for the clipping stage. Like the polygon to voronoi tool, this is also separated into its own unit as it can be used apart from this project.

### Edge Matcher (Step 5-6)

The current repository takes extended boundaries from the polygon to voronoi tool, and clips them with the output of the ADM0 Template repo into the datasets linked to at the top of this page.

## Attribute Fields

Each layer from ADM1 to ADM4 contains the following list of attributes.

| Field Name | Field Description                                                |
| ---------- | ---------------------------------------------------------------- |
| admX_id    | Unique ID for admin area, uses last updated value for versioning |
| admX_src   | Original ID from source                                          |
| admX_name  | Default romanized name (en, fr, es, pt), is never blank          |
| admX_name1 | 1st additional name field in alternate language                  |
| admX_name1 | 2nd additional name field in alternate language                  |
| iso3       | ISO 3166-1 alpha-3 code                                          |
| iso2       | ISO 3166-1 alpha-2 code                                          |
| lvl_full   | Number of full coverage admin levels in source                   |
| lvl_part   | Number of partial coverage admin levels in source                |
| lang       | Language of the default name field                               |
| lang1      | Language of the 1st additional name field                        |
| lang2      | Language of the 2nd additional name field                        |
| src_date   | Date of dataset publication from primary source                  |
| src_update | Date of dataset publication through curation service             |
| src_name   | Primary source name                                              |
| src_org    | Secondary source name, sub-group in charge of collecting updates |
| src_lic    | License of dataset and attribution required                      |
| src_url    | URL link of original dataset                                     |
| src_grp    | Source group dataset was taken from (COD, geoBoundaries, etc)    |

The ADM0 layer has its own attribute fields distinct from the different source layers.

| Field Name | Field Description                                                                                                                               |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| id         | Unique ID for admin area based on ISO alpha-3 code, some ADM0's are broken up to provide additional labels for islands and other distinct areas |
| iso3       | ISO 3166-1 alpha-3 code                                                                                                                         |
| iso2       | ISO 3166-1 alpha-2 code                                                                                                                         |
| name       | Name for use in charts and reports                                                                                                              |
| label      | Name for use in map labels                                                                                                                      |
| color      | Thematic grouping of admin areas with separate IDs                                                                                              |
| region_cd  | Region code of admin area (AME, EUR, AFR, etc)                                                                                                  |
| region_nm  | Region name of admin area (Americas, Europe, Africa, etc)                                                                                       |
| status_cd  | Status code of admin area (1, 5, 7, etc)                                                                                                        |
| status_nm  | Status name of admin area (State, Territory, Autonomous Region, etc)                                                                            |
