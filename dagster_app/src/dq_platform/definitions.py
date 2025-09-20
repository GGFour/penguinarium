from dagster import Definitions

from .jobs.static_checker_job import static_checker_job
from .jobs.ai_checker_job import ai_checker_job
from .jobs.agentic_checker_job import agentic_checker_job
from .resources.datasource import DataSourceResource
from .resources.storage import ResultSink
from .resources.llm import LLMResource

resources = {
    "datasource": DataSourceResource(),
    "result_sink": ResultSink(base_path="/app/storage/results"),
    "llm": LLMResource(),
}

defs = Definitions(
    jobs=[
        static_checker_job,
        ai_checker_job,
        agentic_checker_job,
    ],
    resources=resources,
)
