from django.db import models

from src.common.models import BaseModel
from .table_metadata import TableMetadata


class FieldMetadata(BaseModel):
    """
    Represents metadata for a field/column within a table.
    
    This model stores information about individual fields including their
    data types, constraints, and other metadata within a table structure.
    """
    
    # Common data types
    class DataType(models.TextChoices):
        STRING = 'string', 'String'
        INTEGER = 'integer', 'Integer'
        FLOAT = 'float', 'Float'
        DOUBLE = 'double', 'Double'
        DECIMAL = 'decimal', 'Decimal'
        BOOLEAN = 'boolean', 'Boolean'
        DATE = 'date', 'Date'
        DATETIME = 'datetime', 'DateTime'
        TIMESTAMP = 'timestamp', 'Timestamp'
        TEXT = 'text', 'Text'
        JSON = 'json', 'JSON'
        ARRAY = 'array', 'Array'
        BINARY = 'binary', 'Binary'
        UUID = 'uuid', 'UUID'
        OTHER = 'other', 'Other'
    
    field_metadata_id = models.AutoField(
        primary_key=True,
        help_text="Primary key for the field metadata"
    )
    
    table = models.ForeignKey(
        TableMetadata,
        on_delete=models.CASCADE,
        related_name='field_metadata_set',
        help_text="The table this field belongs to"
    )
    
    name = models.CharField(
        max_length=255,
        help_text="Name of the field/column"
    )
    
    dtype = models.CharField(
        max_length=50,
        choices=DataType.choices,
        help_text="Data type of the field"
    )
    
    metadata = models.JSONField(
        default=dict,
        help_text="Additional metadata about the field (constraints, format, etc.)"
    )
    
    class Meta:
        verbose_name = "Field Metadata"
        verbose_name_plural = "Field Metadata"
        ordering = ['table', 'name']
        unique_together = ['table', 'name']
    
    def __str__(self):
        return f"{self.table.full_name}.{self.name} ({self.get_dtype_display()})"
    
    @property
    def full_name(self):
        """Get the full qualified name including table and data source."""
        return f"{self.table.full_name}.{self.name}"
    
    @property
    def display_name(self):
        """Get a user-friendly display name with data type."""
        return f"{self.name} ({self.get_dtype_display()})"
    
    def is_numeric_type(self):
        """Check if this field is a numeric data type."""
        numeric_types = [
            self.DataType.INTEGER,
            self.DataType.FLOAT,
            self.DataType.DOUBLE,
            self.DataType.DECIMAL
        ]
        return self.dtype in numeric_types
    
    def is_string_type(self):
        """Check if this field is a string/text data type."""
        string_types = [
            self.DataType.STRING,
            self.DataType.TEXT
        ]
        return self.dtype in string_types
    
    def is_date_type(self):
        """Check if this field is a date/time data type."""
        date_types = [
            self.DataType.DATE,
            self.DataType.DATETIME,
            self.DataType.TIMESTAMP
        ]
        return self.dtype in date_types
    
    def get_constraints_count(self):
        """Get the number of constraints defined for this field."""
        return self.field_constraint_set.count()
    
    def get_latest_stats(self):
        """Get the most recent field statistics."""
        return self.field_stats_set.order_by('-stat_date').first()
    
    # TODO: Add relationships when related models are created
    # Relationships based on ER diagram:
    # - One-to-many with FieldStats (field_metadata.field_stats_set)
    # - One-to-many with FieldConstraint (field_metadata.field_constraint_set)
    # - One-to-many with Relation as source (field_metadata.source_relations)
    # - One-to-many with Relation as destination (field_metadata.destination_relations)
    # - One-to-many with Alert (field_metadata.alert_set)