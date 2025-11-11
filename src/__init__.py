import sys

from . import boardings, complexes, mta_graph, socrata_od_client

__all__ = ["boardings", "complexes", "mta_graph", "socrata_od_client"]

# Maintain backward-compatible import paths like `import boardings`.
sys.modules.setdefault("boardings", boardings)
sys.modules.setdefault("complexes", complexes)
sys.modules.setdefault("mta_graph", mta_graph)
sys.modules.setdefault("socrata_od_client", socrata_od_client)

