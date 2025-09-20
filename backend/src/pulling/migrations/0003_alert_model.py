from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):
    dependencies = [
        ("pulling", "0002_add_user_to_datasource"),
    ]

    operations = [
        migrations.CreateModel(
            name="Alert",
            fields=[
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("global_id", models.UUIDField(default=uuid.uuid4, unique=True, editable=False)),
                ("is_deleted", models.BooleanField(default=False)),
                ("alert_id", models.AutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=255)),
                ("severity", models.CharField(max_length=16, choices=[("info", "Info"), ("warning", "Warning"), ("critical", "Critical")], default="warning")),
                ("status", models.CharField(max_length=16, choices=[("active", "Active"), ("resolved", "Resolved"), ("snoozed", "Snoozed")], default="active")),
                ("details", models.JSONField(default=dict)),
                ("triggered_at", models.DateTimeField()),
                ("data_source", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="alerts", to="pulling.datasource")),
                ("table", models.ForeignKey(on_delete=django.db.models.deletion.SET_NULL, related_name="alerts", null=True, blank=True, to="pulling.tablemetadata")),
                ("field", models.ForeignKey(on_delete=django.db.models.deletion.SET_NULL, related_name="alerts", null=True, blank=True, to="pulling.fieldmetadata")),
            ],
            options={
                "verbose_name": "Alert",
                "verbose_name_plural": "Alerts",
                "ordering": ["-triggered_at", "-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="alert",
            index=models.Index(fields=["data_source", "status"], name="pulling_alert_ds_status_idx"),
        ),
        migrations.AddIndex(
            model_name="alert",
            index=models.Index(fields=["severity"], name="pulling_alert_severity_idx"),
        ),
    ]
