from django.db import models

from src.common.models import BaseModel
from .data_source import DataSource


class TableMetadata(BaseModel):
    """
    Represents metadata for a table within a data source.
    
    This model stores information about tables, views, or other data structures
    within a data source, including their schema and descriptive metadata.
    """
    
    table_metadata_id = models.AutoField(
        primary_key=True,
        help_text="Primary key for the table metadata"
    )
    
    data_source = models.ForeignKey(
        DataSource,
        on_delete=models.CASCADE,
        related_name='table_metadata_set',
        help_text="The data source this table belongs to"
    )
    
    name = models.CharField(
        max_length=255,
        help_text="Name of the table or data structure"
    )
    
    description = models.TextField(
        blank=True,
        null=True,
        help_text="Human-readable description of the table"
    )
    
    metadata = models.JSONField(
        default=dict,
        help_text="Additional metadata about the table (schema, stats, etc.)"
    )
    
    class Meta(BaseModel.Meta):
        verbose_name = "Table Metadata"
        verbose_name_plural = "Table Metadata"
        ordering = ['data_source', 'name']
        unique_together = ['data_source', 'name']
    
    def __str__(self):
        return f"{self.data_source.name}.{self.name}"
    
    @property
    def full_name(self):
        """Get the full qualified name including data source."""
        return f"{self.data_source.name}.{self.name}"
    
    @property
    def display_name(self):
        """Get a user-friendly display name."""
        if self.description:
            return f"{self.name} - {self.description}"
        return self.name
    
    def get_field_count(self):
        """Get the number of fields in this table."""
        return self.field_metadata_set.count()
    
    # TODO: Add relationships when related models are created
    # Relationships based on ER diagram:
    # - One-to-many with FieldMetadata (table_metadata.field_metadata_set)