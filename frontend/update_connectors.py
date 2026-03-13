import glob
import os
import re


TEMPLATES_PATH = r"c:/Users/HP/OneDrive/Desktop/PROJECTS/Segmento_Collector/frontend/templates/connectors"
FILES = glob.glob(os.path.join(TEMPLATES_PATH, "*.html"))

DESTINATIONS = [
    ("mysql", "MySQL", "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg", "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg"),
    ("postgres", "PostgreSQL", "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg", "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg"),
    ("redshift", "Amazon Redshift", "/static/images/logos/redshift.png", "/static/images/logos/redshift.png"),
    ("bigquery", "Google BigQuery", "https://cdn.simpleicons.org/googlebigquery/4285F4", "https://cdn.simpleicons.org/googlebigquery/4285F4"),
    ("snowflake", "Snowflake", "https://cdn.simpleicons.org/snowflake/29B5E8", "https://cdn.simpleicons.org/snowflake/29B5E8"),
    ("clickhouse", "ClickHouse", "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/clickhouse/clickhouse-original.svg", "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/clickhouse/clickhouse-original.svg"),
    ("s3", "AWS S3", "", "{{ url_for('static', filename='images/logos/s3.png') }}"),
    ("azure_datalake", "Azure Data Lake", "/static/images/logos/adls.png", "/static/images/logos/adls.png"),
    ("databricks", "Databricks", "https://cdn.simpleicons.org/databricks/EF3A2C", "https://cdn.simpleicons.org/databricks/EF3A2C"),
    ("mongodb", "MongoDB", "https://cdn.simpleicons.org/mongodb/47A248", "https://cdn.simpleicons.org/mongodb/47A248"),
    ("elasticsearch", "Elasticsearch", "https://cdn.simpleicons.org/elasticsearch/005571", "https://cdn.simpleicons.org/elasticsearch/005571"),
    ("duckdb", "DuckDB", "https://cdn.simpleicons.org/duckdb/FFF000", "https://cdn.simpleicons.org/duckdb/FFF000"),
    ("gcs", "Google Cloud Storage", "https://cdn.simpleicons.org/googlecloudstorage/4285F4", "https://cdn.simpleicons.org/googlecloudstorage/4285F4"),
]


def detect_indent(block: str) -> str:
    match = re.search(r"^(\s*)<input type=\"hidden\" id=\"destType\" value=\"\">", block, flags=re.MULTILINE)
    return match.group(1) if match else "                "


def detect_item_classes(block: str):
    item_class_match = re.search(r'class="([^"]*cursor-pointer transition-colors group)"', block)
    img_class_match = re.search(r'<img[^>]*class="([^"]+)"', block)
    span_class_match = re.search(r'<span class="([^"]+font-medium)">', block)
    item_class = item_class_match.group(1) if item_class_match else "flex items-center gap-3 px-4 py-3 rounded-xl cursor-pointer transition-colors group"
    img_class = img_class_match.group(1) if img_class_match else "w-6 h-6 object-contain"
    span_class = span_class_match.group(1) if span_class_match else "text-slate-200 group-hover:text-white font-medium"
    return item_class, img_class, span_class


def build_dropdown_block(block: str) -> str:
    indent = detect_indent(block)
    item_class, img_class, span_class = detect_item_classes(block)
    item_indent = indent + "    "
    img_indent = item_indent + "  "

    lines = [
        f'{indent}<input type="hidden" id="destType" value="">',
        "",
        f'{indent}<div id="dropdownMenu"',
        f'{indent}  class="hidden absolute w-full mt-2 bg-slate-800 border border-slate-700 rounded-2xl shadow-2xl z-50 max-h-60 overflow-y-auto backdrop-blur-xl">',
        f'{indent}  <div class="p-2 space-y-1">',
    ]

    for key, label, onclick_logo, img_logo in DESTINATIONS:
        lines.extend([
            f'{item_indent}<div onclick="selectDestination(\'{key}\', \'{label}\', \'{onclick_logo}\')"',
            f'{item_indent}  class="{item_class}">',
            f'{img_indent}<img src="{img_logo}" class="{img_class}">',
            f'{img_indent}<span class="{span_class}">{label}</span>',
            f'{item_indent}</div>',
        ])

    lines.extend([
        f'{indent}  </div>',
        f'{indent}</div>',
        f'{indent}</div>',
    ])
    return "\n".join(lines)


def repair_template(filename: str) -> str:
    with open(filename, "r", encoding="utf-8") as handle:
        content = handle.read()

    pattern = re.compile(
        r'<input type="hidden" id="destType" value="">[\s\S]*?(?=\n\s*<div id="dbFields")'
    )
    match = pattern.search(content)
    if not match:
        raise RuntimeError(f"Destination block not found in {os.path.basename(filename)}")

    replacement = build_dropdown_block(match.group(0))
    return content[:match.start()] + replacement + content[match.end():]


count = 0
for filename in FILES:
    updated = repair_template(filename)
    with open(filename, "w", encoding="utf-8") as handle:
        handle.write(updated)
    count += 1
    print(f"Repaired: {os.path.basename(filename)}")

print(f"Total Repaired: {count}")
