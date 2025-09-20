from __future__ import annotations

import logging
import re
from typing import Any

from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from dagster_app.utils.connectors import create_run_config

from dagster_support import DagsterClientError, DagsterRunLauncher
from ..logging import get_request_id
from ..serializers.dagster import DagsterRunSerializer


JOB_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


class DagsterRunView(APIView):
    """Launch Dagster jobs via REST."""

    launcher_class = DagsterRunLauncher
    logger = logging.getLogger("api.dagster")

    def post(self, request: Request, job_name: str, *args: Any, **kwargs: Any) -> Response:
        if not JOB_NAME_PATTERN.match(job_name):
            request_id = get_request_id()
            return Response(
                {
                    "error": {
                        "code": "invalid_parameter",
                        "message": "Job name must contain only letters, numbers, and underscores.",
                        "target": "DagsterRunView",
                        "status": status.HTTP_400_BAD_REQUEST,
                        "request_id": request_id,
                    }
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = DagsterRunSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)

        validated = serializer.validated_data
        launcher = self.launcher_class.from_settings()

        source_config = validated["source"]
        base_run_config = validated.get("run_config")
        run_config = create_run_config(
            base_run_config,
            source_config=source_config,
            op_key=validated.get("source_op_key", "load_dataset_op"),
        )

        try:
            run_id = launcher.launch_job(
                job_name=job_name,
                run_config=run_config,
                tags=validated.get("tags"),
                mode=validated.get("mode"),
                op_selection=validated.get("op_selection"),
            )
        except DagsterClientError as error:
            self.logger.error("Dagster job launch failed: %s", error)
            request_id = get_request_id()
            return Response(
                {
                    "error": {
                        "code": "dagster_error",
                        "message": str(error),
                        "target": "DagsterRunView",
                        "status": status.HTTP_502_BAD_GATEWAY,
                        "request_id": request_id,
                    }
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )

        return Response({"run_id": run_id}, status=status.HTTP_202_ACCEPTED)
