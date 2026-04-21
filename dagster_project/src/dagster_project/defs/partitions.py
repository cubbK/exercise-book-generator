"""Shared partition definitions used across bronze, silver, and gold layers."""

from dagster import DynamicPartitionsDefinition

book_partitions = DynamicPartitionsDefinition(name="books")
