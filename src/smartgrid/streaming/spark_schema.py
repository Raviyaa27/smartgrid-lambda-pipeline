"""
Spark schemas generated from the FieldSpec tables.

The third consumer of `common.schemas`, alongside the two validators: the
Spark StructType is derived from the same declaration, never written by hand,
so a field added to the contract appears in the speed layer's schema without
anyone remembering to add it.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from smartgrid.common.schemas import FieldSpec

_SPARK_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "double": DoubleType(),
    "integer": LongType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
}


def struct_for(specs: Iterable[FieldSpec], *, extra: Iterable[StructField] = ()) -> StructType:
    """
    One nullable field per spec. Nullable even when `required`: validation,
    not the schema, decides what a missing value means, and quarantined rows
    must still fit the schema on their way to the dead-letter queue.
    """
    return StructType(
        [StructField(spec.name, _SPARK_TYPES[spec.dtype], nullable=True) for spec in specs]
        + list(extra)
    )
