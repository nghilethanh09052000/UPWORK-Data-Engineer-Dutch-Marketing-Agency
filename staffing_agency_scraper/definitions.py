"""
Aggregation of all definitions from all of the data pipeline.

If you've added new definitions (based on concept and technology), add them
here. To see an example on how to create definitions, check the already existing
definitions.
"""

import dagster as dg

# Make sure to not import the defs vars directly. As it will
# trigger "Multiple Definitions Error"
from . import resources as resources
from .scraping import definitions as scraping


defs = dg.Definitions.merge(
    scraping.defs,
    resources.defs,
)

