
# SharePoint config
site_url = "https://brandix.sharepoint.com/sites/ManufacturingTools"
folder_url = "/sites/ManufacturingTools/Shared Documents/UATCARDER"
list_name = "Employee Details_1"


# Sri Lanka timezone
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

# Collect recent Excel files
modified_files = []

def collect_recent_excel_files(folder):
    files = folder.files
    ctx.load(files)
    ctx.execute_query()
    for file in files:
        modified_time = file.time_last_modified.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - modified_time <= timedelta(minutes=20) and file.name.endswith((".xlsx", ".xls")):
            modified_files.append((file, modified_time))
    sub_folders = folder.folders
    ctx.load(sub_folders)
    ctx.execute_query()
    for sub_folder in sub_folders:
        collect_recent_excel_files(sub_folder)

collect_recent_excel_files(folder)

if not modified_files:
    print("⚠️ No recent Excel files found.")
    exit()

modified_files.sort(key=lambda x: x[1])

for file, mod_time in modified_files:
    print(f"\n📄 Processing: {file.name} (modified at {mod_time})")
    file_url = file.serverRelativeUrl
    file_stream = BytesIO()
    ctx.web.get_file_by_server_relative_url(file_url).download(file_stream).execute_query()
    file_stream.seek(0)

    try:
        df_excel = pd.read_excel(file_stream, sheet_name="Sheet4", engine="openpyxl")
    except BadZipFile:
        file_stream.seek(0)
        try:
            df_excel = pd.read_excel(file_stream, sheet_name="Sheet4", engine="xlrd")
        except Exception as e:
            print(f"❌ Failed to read {file.name}: {e}")
            continue
    except Exception as e:
        print(f"❌ Failed to read {file.name}: {e}")
        continue

    df_excel = df_excel.astype(str)
    required_columns = {"Title", "Plant", "Team", "EPF", "Name"}
    if not required_columns.issubset(df_excel.columns):
        print(f"❌ Missing columns: {required_columns - set(df_excel.columns)}")
        continue

    title_plant_pairs = set(zip(df_excel["Title"].str.strip(), df_excel["Plant"].str.strip()))

    # Delete matching items using CAML query
    sp_list = ctx.web.lists.get_by_title(list_name)
    for title, plant in title_plant_pairs:
        query = CamlQuery()
        query.ViewXml = f"""
        <View>
            <Query>
                <Where>
                    <And>
                        <Eq><FieldRef Name='Title' /><Value Type='Text'>{title}</Value></Eq>
                        <Eq><FieldRef Name='Plant' /><Value Type='Text'>{plant}</Value></Eq>
                    </And>
                </Where>
            </Query>
        </View>
        """
        items = sp_list.get_items(query)
        ctx.load(items)
        ctx.execute_query()
        for item in items:
            item.delete_object()
            print(f"❌ Deleted: ({title}, {plant})")
        ctx.execute_query()

    # Insert new items
    batch_size = 20
    for i, (_, row) in enumerate(df_excel.iterrows(), 1):
        sp_list.add_item({
            "Title": row["Title"].strip(),
            "Plant": row["Plant"].strip(),
            "Team": row["Team"].strip(),
            "EPF": row["EPF"].strip(),
            "Name": row["Name"].strip()
        })
        print(f"➕ Inserted: ({row['Title'].strip()}, {row['Plant'].strip()}, {row['EPF'].strip()})")

        if i % batch_size == 0:
            try:
                ctx.execute_query()
                time.sleep(0.5)
            except Exception as e:
                print(f"⚠️ Batch insert error: {e}")
                break

    try:
        ctx.execute_query()
    except Exception as e:
        print(f"⚠️ Final insert error: {e}")

    print("✅ Sync complete.")
