"""Configuration constants for the download stage."""

HDX_PACKAGE = "https://data.humdata.org/api/3/action/package_show?id=cod-ab-global"
GDB_NAME = "global_admin_boundaries_extended_latest"
META_NAME = "global_admin_boundaries_metadata_latest"

HTTP_CONNECT_TIMEOUT = 30
DOWNLOAD_CHUNK_SIZE = 4 * 1024 * 1024
