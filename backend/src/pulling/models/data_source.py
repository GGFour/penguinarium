from django.db import models
from django.apps import apps

from common.models import BaseModel

class DataSource(BaseModel):
    """
    Represents a data source that can be connected to for data extraction.
    
    This model stores information about various data sources such as databases,
    APIs, files, etc. that can be used in data pipelines.
    """
    
    # Data source types
    class DataSourceType(models.TextChoices):
        DATABASE = 'database', 'Database'
        API = 'api', 'API'
        FILE = 'file', 'File'
        STREAM = 'stream', 'Stream'
        CLOUD = 'cloud', 'Cloud Storage'
        OTHER = 'other', 'Other'
    
    data_source_id = models.AutoField(
        primary_key=True,
        help_text="Primary key for the data source"
    )
    
    name = models.CharField(
        max_length=255,
        help_text="Human-readable name for the data source"
    )
    
    type = models.CharField(
        max_length=50,
        choices=DataSourceType.choices,
        help_text="Type of data source (database, API, file, etc.)"
    )
    
    connection_info = models.JSONField(
        help_text="Configuration and connection details for the data source"
    )
    
    class Meta(BaseModel.Meta):
        verbose_name = "Data Source"
        verbose_name_plural = "Data Sources"
        ordering = ['name']
    
    def __str__(self):
        return f"{self.name} ({self.get_type_display()})"
    
    def is_database_type(self):
        """Check if this data source is a database type."""
        return self.type == self.DataSourceType.DATABASE
    
    def is_api_type(self):
        """Check if this data source is an API type."""
        return self.type == self.DataSourceType.API
    
    def is_file_type(self):
        """Check if this data source is a file type."""
        return self.type == self.DataSourceType.FILE
    
    @property
    def display_name(self):
        """Get a user-friendly display name."""
        return f"{self.name} ({self.get_type_display()})"

    # TODO Relationships (reverse relations via related_name on the other models):
    # - One-to-many with Pipeline (data_source.pipeline_set) — pending Pipeline model

    @property
    def tables(self):
        """Convenience accessor for all TableMetadata rows related to this data source."""
        TableMetadata = apps.get_model('pulling', 'TableMetadata')
        return TableMetadata.objects.filter(data_source=self)

    def table_count(self) -> int:
        """Number of tables registered for this data source."""
        TableMetadata = apps.get_model('pulling', 'TableMetadata')
        return TableMetadata.objects.filter(data_source=self).count()

    def has_tables(self) -> bool:
        """Whether this data source has any registered tables."""
        TableMetadata = apps.get_model('pulling', 'TableMetadata')
        return TableMetadata.objects.filter(data_source=self).exists()

    # def pipelines(self) -> "QuerySet[Pipeline]":
    #     """Accessor for related Pipeline rows (available once Pipeline model exists)."""
    #     return self.pipeline_set.all()
