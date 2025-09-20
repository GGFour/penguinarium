import uuid
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("pulling", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="FieldStats",
            fields=[
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("global_id", models.UUIDField(default=uuid.uuid4, unique=True, editable=False)),
                ("is_deleted", models.BooleanField(default=False)),
                ("field_stats_id", models.AutoField(primary_key=True, serialize=False)),
                (
                    "field",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="field_stats_set",
                        to="pulling.fieldmetadata",
                        help_text="The column/field this statistic belongs to",
                    ),
                ),
                (
                    "stat_date",
                    models.DateTimeField(
                        help_text="Timestamp when the statistics were collected"
                    ),
                ),
                (
                    "value",
                    models.JSONField(
                        help_text="Arbitrary JSON statistics payload for the column"
                    ),
                ),
            ],
            options={
                "verbose_name": "Field Statistics",
                "verbose_name_plural": "Field Statistics",
                "ordering": ["-stat_date", "field_stats_id"],
            },
        ),
        migrations.AddIndex(
            model_name="fieldstats",
            index=models.Index(fields=["field", "-stat_date"], name="idx_field_date_desc"),
        ),
    ]
