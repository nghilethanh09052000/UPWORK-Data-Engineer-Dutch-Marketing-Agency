"""
Shared resources for all assets in the pipeline.

This module defines configurable resources that can be used across
the entire scraping pipeline.
"""

import dagster as dg
from psycopg_pool import ConnectionPool


class PostgresResource:
    """
    Singleton resource for all assets to use the Postgres connection pool.
    """

    pool: ConnectionPool

    def __init__(self, conn_string: str) -> None:
        self.pool = ConnectionPool(
            conninfo=conn_string,
            timeout=3600,  # 1 hour
            kwargs={"autocommit": True, "keepalives_idle": 60},
        )


class ConfigurablePostgresResource(dg.ConfigurableResource):
    """
    Configuration for creating the Postgres resource that can be used by any
    other assets in the project.
    """

    conn_string: str

    def create_resource(self, _: dg.InitResourceContext) -> PostgresResource:
        return PostgresResource(self.conn_string)


class ScraperConfig(dg.ConfigurableResource):
    """
    Configuration for the scraper behavior.
    """

    # Output directory for JSON files
    output_dir: str = "./output"

    # Request timeout in seconds
    request_timeout: int = 30

    # Delay between requests to avoid rate limiting (seconds)
    request_delay: float = 1.0

    # Maximum retries for failed requests
    max_retries: int = 3

    # Whether to use headless browser mode
    headless: bool = True


defs = dg.Definitions(
    resources={
        "pg": ConfigurablePostgresResource(
            conn_string=dg.EnvVar("PIPELINE_PG_CONNSTRING")
        ),
        "scraper_config": ScraperConfig(
            output_dir=dg.EnvVar("OUTPUT_DIR").get_value(default="./output") or "./output",
        ),
    }
)

