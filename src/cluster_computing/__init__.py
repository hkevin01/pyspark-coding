"""
Cluster Computing Examples
==========================

Learn how to use PySpark with multiple computers (cluster computing)
to process massive datasets that don't fit on a single machine.

Examples:
- cluster_setup: Configure YARN, Kubernetes, and Standalone clusters
- data_partitioning: Optimize data distribution across nodes
- distributed_joins: Join large datasets across nodes efficiently
- aggregations_at_scale: Aggregate billions of rows with window functions
- fault_tolerance: Checkpointing and recovery strategies
"""

from . import cluster_setup
from . import data_partitioning
# Note: Examples 03-05 are standalone scripts, import as needed
# from . import distributed_joins
# from . import aggregations_at_scale
# from . import fault_tolerance

__all__ = ['cluster_setup', 'data_partitioning']

__version__ = "1.0.0"
__author__ = "PySpark Learning Project"

__all__ = [
    "cluster_setup",
    "data_partitioning",
]
