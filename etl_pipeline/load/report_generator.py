import logging
from pathlib import Path
from typing import Dict
import pandas as pd
from jinja2 import Template

logger = logging.getLogger(__name__)

TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Inventory & Warehouse Analytics</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    h1, h2 { color: #333; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
    th, td { border: 1px solid #ddd; padding: 8px; font-size: 12px; }
    th { background: #f5f5f5; text-align: left; }
    .section { margin-bottom: 40px; }
    .small { font-size: 12px; color: #666; }
  </style>
</head>
<body>
  <h1>Inventory & Warehouse Analytics</h1>
  <p class="small">Generated at: {{ generated_at }}</p>

  {% for key, df in tables.items() %}
  <div class="section">
    <h2>{{ key }}</h2>
    <table>
      <thead>
        <tr>
          {% for col in df.columns %}<th>{{ col }}</th>{% endfor %}
        </tr>
      </thead>
      <tbody>
        {% for row in df.itertuples(index=False) %}
        <tr>
          {% for value in row %}<td>{{ value }}</td>{% endfor %}
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
  {% endfor %}
</body>
</html>
"""

class ReportGenerator:
    def __init__(self, config: dict):
        self.config = config
        self.report_dir = Path(config["run"]["report_dir"])

    def generate_html_report(self, tables: Dict[str, pd.DataFrame]):
        self.report_dir.mkdir(parents=True, exist_ok=True)
        # Limit to a subset to avoid huge HTMLs
        keys = list(tables.keys())
        subset_keys = keys[:15]  # include up to 15 tables
        subset = {k: tables[k].head(200) for k in subset_keys}  # show first 200 rows each

        tpl = Template(TEMPLATE)
        html = tpl.render(generated_at=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                          tables=subset)
        out = self.report_dir / "analytics_report.html"
        out.write_text(html, encoding="utf-8")
        logger.info(f"Wrote HTML report to {out}")
