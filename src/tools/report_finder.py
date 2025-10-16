import os
from datetime import date
from pydantic import BaseModel
from langchain.tools import BaseTool
from html_to_markdown import convert

class ReportSearchResult(BaseModel):
    exists: bool
    path: str = None
    content: str = None
    message: str = None

class FindTodaySRAGReportTool(BaseTool):
    name: str = "find_today_srag_report"
    description: str = (
        "Searches for today's SRAG HTML report."
        "If found, returns the file path and content."
        "If not found, indicates that a new report needs to be generated."
    )

    reports_dir: str = "../../reports"

    def _run(self) -> dict:
        today_str = date.today().strftime("%Y-%m-%d")
        expected_name = f"SRAG_report_{today_str}.html"
        file_path = os.path.join(self.reports_dir, expected_name)

        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    html_content = f.read()
                markdown_content = convert(html_content)

                return ReportSearchResult(
                    exists=True,
                    path=file_path,
                    content=markdown_content,
                    message=f"Today's report found ({date.today()}) and returned in markdown format."
                ).dict()

            except Exception as e:
                return ReportSearchResult(
                    exists=True,
                    path=file_path,
                    message=f"Today's report found ({date.today()}), but failed to load content: {str(e)}"
                ).dict()

        # If no report for today.
        return ReportSearchResult(
            exists=False,
            message=f"No report found for today ({date.today()}). You may generate a new one."
        ).dict()

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async version not implemented.")