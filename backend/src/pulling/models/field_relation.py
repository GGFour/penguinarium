from django.db import models

from common.models import BaseModel
from pulling.models.field_metadata import FieldMetadata


class FieldRelation(BaseModel):
    """
    Represents a relationship between two fields across tables or data sources.
    
    This model stores information about how fields relate to each other,
    such as foreign key relationships, joins, or other data lineage connections.
    """
    
    # Common relation types
    class RelationType(models.TextChoices):
        FOREIGN_KEY = 'foreign_key', 'Foreign Key'
        PRIMARY_KEY = 'primary_key', 'Primary Key'
        JOIN = 'join', 'Join'
        REFERENCE = 'reference', 'Reference'
        DERIVED = 'derived', 'Derived'
        CALCULATED = 'calculated', 'Calculated'
        LINEAGE = 'lineage', 'Data Lineage'
        MAPPING = 'mapping', 'Field Mapping'
        DEPENDENCY = 'dependency', 'Dependency'
        OTHER = 'other', 'Other'
    
    field_relation_id = models.AutoField(
        primary_key=True,
        help_text="Primary key for the field relation"
    )
    
    src_field = models.ForeignKey(
        FieldMetadata,
        on_delete=models.CASCADE,
        related_name='source_relations',
        help_text="The source field in the relationship"
    )
    
    dst_field = models.ForeignKey(
        FieldMetadata,
        on_delete=models.CASCADE,
        related_name='destination_relations',
        help_text="The destination field in the relationship"
    )
    
    relation_type = models.CharField(
        max_length=50,
        choices=RelationType.choices,
        help_text="Type of relationship between the fields"
    )
    
    class Meta:
        verbose_name = "Field Relation"
        verbose_name_plural = "Field Relations"
        ordering = ['src_field', 'dst_field']
        unique_together = ['src_field', 'dst_field', 'relation_type']
    
    def __str__(self):
        return f"{self.src_field.full_name} → {self.dst_field.full_name} ({self.get_relation_type_display()})"
    
    @property
    def display_name(self):
        """Get a user-friendly display name for the relation."""
        return f"{self.src_field.name} → {self.dst_field.name} ({self.get_relation_type_display()})"
    
    @property
    def is_cross_table_relation(self):
        """Check if this relation crosses table boundaries."""
        return self.src_field.table != self.dst_field.table
    
    @property
    def is_cross_source_relation(self):
        """Check if this relation crosses data source boundaries."""
        return self.src_field.table.data_source != self.dst_field.table.data_source
    
    def is_foreign_key_relation(self):
        """Check if this is a foreign key relationship."""
        return self.relation_type == self.RelationType.FOREIGN_KEY
    
    def is_join_relation(self):
        """Check if this is a join relationship."""
        return self.relation_type == self.RelationType.JOIN
    
    def is_lineage_relation(self):
        """Check if this is a data lineage relationship."""
        return self.relation_type == self.RelationType.LINEAGE
    
    def get_reverse_relation(self):
        """Get the reverse relation if it exists."""
        return FieldRelation.objects.filter(
            src_field=self.dst_field,
            dst_field=self.src_field
        ).first()
    
    @classmethod
    def get_field_relations(cls, field):
        """Get all relations for a specific field (both as source and destination)."""
        return cls.objects.filter(
            models.Q(src_field=field) | models.Q(dst_field=field)
        )
    
    @classmethod
    def get_table_relations(cls, table):
        """Get all relations involving fields from a specific table."""
        return cls.objects.filter(
            models.Q(src_field__table=table) | models.Q(dst_field__table=table)
        )
    
    def clean(self):
        """Validate the relation."""
        from django.core.exceptions import ValidationError
        
        # Prevent self-referencing relations
        if self.src_field == self.dst_field:
            raise ValidationError("A field cannot have a relation with itself.")
        
        super().clean()
    
    def save(self, *args, **kwargs):
        """Override save to run validation."""
        self.full_clean()
        super().save(*args, **kwargs)