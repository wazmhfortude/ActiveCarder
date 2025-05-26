from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta, timezone
import time

# SharePoint config
site_url = "https://klnaclk.sharepoint.com/sites/WAZ"
folder_url = "/sites/WAZ/Shared Documents/Employee"
list_name = "Employees"
username = "raseenam-bs17192@stu.kln.ac.lk"
password = "17192977"

# Sri Lanka timezone (UTC+5:30)
sri_lanka_tz = timezone(timedelta(hours=5, minutes=30))

# Authenticate
ctx_auth = AuthenticationContext(site_url)
if not ctx_auth.acquire_token_for_user(username, password):
    print("❌ Authentication failed.")
    exit()

ctx = ClientContext(site_url, ctx_auth)
folder = ctx.web.get_folder_by_server_relative_url(folder_url)
ctx.load(folder)
ctx.execute_query()

# Find all Excel files modified in the last 20 minutes
modified_files = []

def collect_recent_excel_files(folder):
    files = folder.files
    ctx.load(files)
    ctx.execute_query()
    for file in files:
        modified_time = file.time_last_modified.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        time_diff = now_utc - modified_time

        # Debug output with local time
        local_modified = modified_time.astimezone(sri_lanka_tz)
        local_now = now_utc.astimezone(sri_lanka_tz)
        print(f"🕒 Checking file: {file.name}")
        print(f"   Modified (UTC): {modified_time} | Now (UTC): {now_utc} | Diff: {time_diff}")
        print(f"   Modified (Local): {local_modified} | Now (Local): {local_now}")

        if time_diff <= timedelta(minutes=20) and file.name.endswith((".xlsx", ".xls")):
            modified_files.append((file, modified_time))
    sub_folders = folder.folders
    ctx.load(sub_folders)
    ctx.execute_query()
    for sub_folder in sub_folders:
        collect_recent_excel_files(sub_folder)

collect_recent_excel_files(folder)

if not modified_files:
    print("⚠️ No Excel files modified in the last 20 minutes found.")
    exit()

# Sort files by modified time
modified_files.sort(key=lambda x: x[1])

# Process each file
for file, mod_time in modified_files:
    print(f"\n📄 Processing file: {file.name} (modified at {mod_time})")

    # Download file
    file_url = file.serverRelativeUrl
    file_stream = BytesIO()
    ctx.web.get_file_by_server_relative_url(file_url).download(file_stream).execute_query()
    file_stream.seek(0)

    # Load Excel data
    try:
        df_excel = pd.read_excel(file_stream, sheet_name="Sheet4")
        df_excel = df_excel.astype(str)
        required_columns = {"Title", "Plant", "Team", "EPF", "Name"}
        if not required_columns.issubset(df_excel.columns):
            print(f"❌ Skipping file {file.name} — missing required columns: {required_columns - set(df_excel.columns)}")
            continue
    except Exception as e:
        print(f"❌ Failed to read Excel file: {e}")
        continue

    # Merge loop
    while True:
        print("🔄 Starting merge cycle...")
        changes_made = False

        # Fetch all SharePoint list items
        sp_list = ctx.web.lists.get_by_title(list_name)
        all_items = []
        paged_items = sp_list.items.paged(500)

        while True:
            ctx.load(paged_items)
            ctx.execute_query()
            all_items.extend(paged_items)
            if not paged_items.has_next:
                break
            paged_items = paged_items.get_next()

        # Convert SharePoint items to a dictionary using composite key
        sp_data = {}
        for item in all_items:
            key = (
                str(item.properties.get("Title", "")).strip(),
                str(item.properties.get("Plant", "")).strip(),
                str(item.properties.get("EPF", "")).strip()
            )
            sp_data[key] = {
                "ID": item.properties["ID"],
                "Team": str(item.properties.get("Team", "")).strip(),
                "Name": str(item.properties.get("Name", "")).strip()
            }

        # Track processed keys
        processed_keys = set()

        # Insert or update
        for _, row in df_excel.iterrows():
            key = (row["Title"].strip(), row["Plant"].strip(), row["EPF"].strip())
            processed_keys.add(key)

            if key in sp_data:
                if sp_data[key]["Team"] != row["Team"].strip() or sp_data[key]["Name"] != row["Name"].strip():
                    item = sp_list.get_item_by_id(sp_data[key]["ID"])
                    item.set_property("Team", row["Team"].strip())
                    item.set_property("Name", row["Name"].strip())
                    item.update()
                    changes_made = True
                    print(f"✅ Updated: {key}")
            else:
                sp_list.add_item({
                    "Title": row["Title"].strip(),
                    "Plant": row["Plant"].strip(),
                    "Team": row["Team"].strip(),
                    "EPF": row["EPF"].strip(),
                    "Name": row["Name"].strip()
                })
                changes_made = True
                print(f"➕ Inserted: {key}")

        ctx.execute_query()

        # Delete only items that match Title and Plant from current file
        valid_title_plant_pairs = set(zip(df_excel["Title"].str.strip(), df_excel["Plant"].str.strip()))
        for key, data in sp_data.items():
            title, plant, epf = key
            if (title, plant) in valid_title_plant_pairs and key not in processed_keys:
                item = sp_list.get_item_by_id(data["ID"])
                item.delete_object()
                changes_made = True
                print(f"❌ Deleted: {key}")

        ctx.execute_query()

        if not changes_made:
            print("✅ Merge complete. No more changes needed.")
            break
        else:
            print("🔁 Changes made. Repeating merge cycle to ensure full sync...")
            time.sleep(1)
