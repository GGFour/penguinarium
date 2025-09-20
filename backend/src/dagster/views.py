from __future__ import annotations

import re
from typing import Any

from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework import status

from .service import trigger_job


JOB_RE = re.compile(r"^[A-Za-z0-9_\-\.]+$")


class RunDagsterJobView(APIView):
	"""POST handler to trigger a Dagster job run.

	Request JSON:
	  - config: optional dict passed to run config
	  - tags: optional dict of tags

	Response 202 JSON:
	  { "job": job_name, "run_id": "...", "status": "submitted", "message": str }
	"""

	permission_classes = [AllowAny]

	def post(self, request: Request, job_name: str, *args: Any, **kwargs: Any) -> Response:
		if not JOB_RE.match(job_name or ""):
			return Response(
				{
					"error": {
						"code": "invalid_parameter",
						"message": "Invalid job name",
						"target": "job_name",
					}
				},
				status=status.HTTP_400_BAD_REQUEST,
			)

		body: dict[str, Any]
		if isinstance(request.data, dict):
			body = request.data
		else:
			body = {}

		config = body.get("config") if isinstance(body.get("config"), dict) else None
		tags = body.get("tags") if isinstance(body.get("tags"), dict) else None

		try:
			result = trigger_job(job_name=job_name, config=config, tags=tags)
		except Exception as exc:  # Safety net: don't leak stack traces
			return Response(
				{
					"error": {
						"code": "job_submit_failed",
						"message": str(exc),
					}
				},
				status=status.HTTP_500_INTERNAL_SERVER_ERROR,
			)

		return Response(
			{
				"job": job_name,
				"run_id": result.get("run_id"),
				"status": result.get("status", "submitted"),
				"message": result.get("message"),
			},
			status=status.HTTP_202_ACCEPTED,
		)
