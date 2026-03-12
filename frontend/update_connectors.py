import os
import glob
import re

# Path where all connector templates live
path = r"c:/Users/HP/OneDrive/Desktop/PROJECTS/Segmento_Collector/frontend/templates/connectors"
files = glob.glob(os.path.join(path, "*.html"))

def update_template(f):
    with open(f, "r", encoding="utf-8") as file:
        content = file.read()
    
    # 1. Extract Source ID
    # Usually in saveDestination: let payload = { source: "google_news", type }; or similar
    source_match = re.search(r'source:\s*["\']([^"\']+)["\']', content)
    if not source_match:
        # Fallback: try to guess from filename
        source_id = os.path.basename(f).replace(".html", "")
    else:
        source_id = source_match.group(1)

    # 2. Dropdown Entries (Ensure Icons + OnClick for new types)
    # We'll look for the end of the menu entries
    new_items = '''                    <div onclick="selectDestination('mongodb', 'MongoDB', 'https://cdn.simpleicons.org/mongodb/47A248')"
                      class="flex items-center gap-3 px-4 py-3 hover:bg-cyan-500/10 rounded-xl cursor-pointer transition-colors group">
                      <img src="https://cdn.simpleicons.org/mongodb/47A248" class="w-6 h-6 object-contain">
                      <span class="text-slate-200 group-hover:text-white font-medium">MongoDB</span>
                    </div>
                    <div onclick="selectDestination('elasticsearch', 'Elasticsearch', 'https://cdn.simpleicons.org/elasticsearch/005571')"
                      class="flex items-center gap-3 px-4 py-3 hover:bg-cyan-500/10 rounded-xl cursor-pointer transition-colors group">
                      <img src="https://cdn.simpleicons.org/elasticsearch/005571" class="w-6 h-6 object-contain">
                      <span class="text-slate-200 group-hover:text-white font-medium">Elasticsearch</span>
                    </div>
                    <div onclick="selectDestination('duckdb', 'DuckDB', 'https://cdn.simpleicons.org/duckdb/FFF000')"
                      class="flex items-center gap-3 px-4 py-3 hover:bg-cyan-500/10 rounded-xl cursor-pointer transition-colors group">
                      <img src="https://cdn.simpleicons.org/duckdb/FFF000" class="w-6 h-6 object-contain">
                      <span class="text-slate-200 group-hover:text-white font-medium">DuckDB</span>
                    </div>
                    <div onclick="selectDestination('gcs', 'Google Cloud Storage', 'https://cdn.simpleicons.org/googlecloudstorage/4285F4')"
                      class="flex items-center gap-3 px-4 py-3 hover:bg-cyan-500/10 rounded-xl cursor-pointer transition-colors group">
                      <img src="https://cdn.simpleicons.org/googlecloudstorage/4285F4" class="w-6 h-6 object-contain">
                      <span class="text-slate-200 group-hover:text-white font-medium">Google Cloud Storage</span>
                    </div>'''
    
    # If mongodb not in dropdown, inject it after databricks or ClickHouse or Snowflake
    if "selectDestination('mongodb'" not in content:
        for anchor in ["databricks", "clickhouse", "snowflake", "bigquery"]:
            pattern = fr'(<div onclick="selectDestination\(\'{anchor}\'.*?</div>)'
            if re.search(pattern, content, flags=re.DOTALL):
                content = re.sub(pattern, r'\1\n' + new_items, content, flags=re.DOTALL)
                break

    # 3. Unified Toggle Function
    new_toggle = '''  function toggleDestFields(type) {
    const fields = document.getElementById("dbFields");
    if (!type) {
      if (fields) fields.classList.add("hidden");
      return;
    }
    if (fields) fields.classList.remove("hidden");

    const sqlFields = document.getElementById("sqlFields");
    const bqFields = document.getElementById("bigqueryFields");
    const s3Fields = document.getElementById("s3Fields");
    const adlsFields = document.getElementById("adlsFields");
    const dbxFields = document.getElementById("databricksFields");
    const formatField = document.getElementById("formatField");

    if (sqlFields) sqlFields.classList.toggle("hidden", type === "bigquery" || type === "s3" || type === "azure_datalake" || type === "databricks");
    if (bqFields) bqFields.classList.toggle("hidden", type !== "bigquery");
    if (s3Fields) s3Fields.classList.toggle("hidden", type !== "s3");
    if (adlsFields) adlsFields.classList.toggle("hidden", type !== "azure_datalake");
    if (dbxFields) dbxFields.classList.toggle("hidden", type !== "databricks");
    if (formatField) formatField.classList.toggle("hidden", type === "mongodb" || type === "elasticsearch");

    const hostInp = document.getElementById("dbHost");
    const portInp = document.getElementById("dbPort");
    const userInp = document.getElementById("dbUser");
    const passInp = document.getElementById("dbPass");
    const nameInp = document.getElementById("dbName");

    [hostInp, portInp, userInp, passInp, nameInp].forEach(el => {
      if (el) el.classList.remove("hidden");
    });

    if (type === "mongodb") {
      if (hostInp) hostInp.placeholder = "Cluster URI (mongodb+srv://...)";
      if (nameInp) nameInp.placeholder = "Database Name";
      if (userInp) userInp.placeholder = "Username";
      if (passInp) passInp.placeholder = "Password";
      if (portInp) portInp.classList.add("hidden");
    } else if (type === "elasticsearch") {
      if (hostInp) hostInp.placeholder = "Endpoint (http://...)";
      if (portInp) portInp.placeholder = "Port (9200)";
      if (nameInp) nameInp.placeholder = "Index Name";
    } else if (type === "duckdb") {
      if (hostInp) hostInp.placeholder = "File Path (e.g. data/my.db)";
      if (portInp) portInp.classList.add("hidden");
      if (userInp) userInp.classList.add("hidden");
      if (passInp) passInp.classList.add("hidden");
      if (nameInp) nameInp.classList.add("hidden");
    } else if (type === "gcs") {
      if (hostInp) hostInp.placeholder = "Bucket Name";
      if (portInp) portInp.placeholder = "Region (optional)";
      if (userInp) userInp.classList.add("hidden");
      if (passInp) passInp.placeholder = "Service Account JSON Key";
      if (nameInp) nameInp.classList.add("hidden");
    } else {
      if (hostInp) hostInp.placeholder = "Host (e.g. 127.0.0.1)";
      if (portInp) portInp.placeholder = "Port";
      if (userInp) userInp.placeholder = "Username";
      if (passInp) passInp.placeholder = "Password";
      if (nameInp) nameInp.placeholder = "Database Name";
    }

    const icebergBtn = document.getElementById("format-iceberg");
    const hudiBtn = document.getElementById("format-hudi");
    const formatInput = document.getElementById("dataFormat");
    const currentFormat = formatInput ? formatInput.value : "";

    if (type === "bigquery" || type === "duckdb") {
      if (icebergBtn) icebergBtn.classList.add("hidden");
      if (hudiBtn) hudiBtn.classList.add("hidden");
      if (currentFormat === "iceberg" || currentFormat === "hudi") {
        if (typeof selectFormat === "function") selectFormat("parquet");
      }
    } else {
      if (icebergBtn) icebergBtn.classList.remove("hidden");
      if (hudiBtn) hudiBtn.classList.remove("hidden");
    }
  }'''

    # Replace both toggleDestFields and toggleDest with a single toggleDestFields
    toggle_pattern = r'function (toggleDestFields|toggleDest)\s*\([^)]*\)\s*\{.*?\}\s*(?=async function|function|window\.onclick|$)'
    content = re.sub(toggle_pattern, new_toggle + "\n", content, flags=re.DOTALL)
    
    # Fix call sites (in selectDestination or elsewhere)
    content = content.replace("toggleDest(", "toggleDestFields(")

    # 4. Save Destination Function Reconstruction
    new_save_dest = f'''  async function saveDestination() {{
    const btn = document.getElementById("saveDestBtn");
    btn.innerText = "CONNECTING...";
    const type = document.getElementById("destType").value;
    let payload = {{ source: "{source_id}", type }};

    if (type === "bigquery") {{
      payload.host = document.getElementById("bqProject").value;
      payload.database = document.getElementById("bqDataset").value;
      payload.password = document.getElementById("bqKey").value;
      payload.format = document.getElementById("dataFormat").value;
    }}
    else if (type === "s3") {{
      payload.host = document.getElementById("s3Bucket").value;
      payload.port = document.getElementById("s3Region").value;
      payload.username = document.getElementById("s3AccessKey").value;
      payload.password = document.getElementById("s3SecretKey").value;
      payload.database = "s3";
      payload.format = document.getElementById("dataFormat").value;
    }}
    else if (type === "azure_datalake") {{
      payload.host = document.getElementById("adlsAccount").value;
      payload.port = document.getElementById("adlsFilesystem").value;
      payload.username = document.getElementById("adlsPath").value;
      payload.password = document.getElementById("adlsKey").value;
      payload.database = "adls";
      payload.format = document.getElementById("dataFormat").value;
    }}
    else if (type === "databricks") {{
      payload.host = document.getElementById("dbxHost").value;
      payload.port = document.getElementById("dbxHttpPath").value;
      payload.password = document.getElementById("dbxToken").value;
      payload.database = document.getElementById("dbxCatalogSchema").value || "hive_metastore.default";
    }}
    else if (type === "duckdb") {{
      payload.host = document.getElementById("dbHost").value;
      payload.format = document.getElementById("dataFormat").value;
    }}
    else if (type === "mongodb") {{
      payload.host = document.getElementById("dbHost").value;
      payload.database = document.getElementById("dbName").value;
      payload.username = document.getElementById("dbUser").value;
      payload.password = document.getElementById("dbPass").value;
    }}
    else if (type === "elasticsearch") {{
      payload.host = document.getElementById("dbHost").value;
      payload.port = document.getElementById("dbPort").value;
      payload.database = document.getElementById("dbName").value;
    }}
    else if (type === "gcs") {{
      payload.host = document.getElementById("dbHost").value;
      payload.port = document.getElementById("dbPort").value;
      payload.password = document.getElementById("dbPass").value;
      payload.database = "gcs";
      payload.format = document.getElementById("dataFormat").value;
    }}
    else {{
      payload.host = document.getElementById("dbHost").value;
      payload.port = document.getElementById("dbPort").value;
      payload.username = document.getElementById("dbUser").value;
      payload.password = document.getElementById("dbPass").value;
      payload.database = document.getElementById("dbName").value;
    }}

    try {{
      const res = await fetch("/destination/save", {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify(payload)
      }});
      if (res.ok) {{
        btn.innerText = "SUCCESS ✓";
        if (typeof loadDestinations === "function") loadDestinations();
        setTimeout(() => btn.innerText = "Authenticate & Save", 2000);
      }} else {{
        btn.innerText = "Failed";
        setTimeout(() => btn.innerText = "Authenticate & Save", 2000);
      }}
    }} catch (e) {{
      btn.innerText = "Error";
      setTimeout(() => btn.innerText = "Authenticate & Save", 2000);
    }}
  }}'''

    save_pattern = r'async function saveDestination\(\)\s*\{.*?\}\s*(?=async function|function|window\.onclick|$)'
    content = re.sub(save_pattern, new_save_dest + "\n", content, flags=re.DOTALL)

    return content

count = 0
for f in files:
    new_content = update_template(f)
    with open(f, "w", encoding="utf-8") as file:
        file.write(new_content)
    count += 1
    print(f"Propagated: {os.path.basename(f)}")

print(f"Total Propagated: {count}")