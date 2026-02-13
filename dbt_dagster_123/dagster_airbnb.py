import os
from pathlib import Path
from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    define_asset_job
)
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator
)

# ==========================================================
# 1. CONFIGURATION PATHS
# ==========================================================
# We verify where this script is, then find the 'airbnb' folder next to it.
CURRENT_DIR = Path(__file__).parent
DBT_PROJECT_DIR = CURRENT_DIR.joinpath("airbnb")
DBT_MANIFEST_PATH = DBT_PROJECT_DIR.joinpath("target", "manifest.json")

# ==========================================================
# 2. DEFINE THE DBT RESOURCE
# ==========================================================
# This object tells Dagster how to run dbt commands.
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    # If your profiles.yml is inside the airbnb folder, uncomment the line below:
    # profiles_dir=os.fspath(DBT_PROJECT_DIR),
)

# ==========================================================
# 3. LOAD DBT ASSETS
# ==========================================================
# This reads the manifest.json and automatically creates a Dagster Asset
# for every model in your dbt project.
@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=DagsterDbtTranslator(),
    name="airbnb_dbt_models",  # Groups them in the UI
)
def airbnb_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # This function runs when you click "Materialize" in the UI.
    yield from dbt.cli(["build"], context=context).stream()

# ==========================================================
# 4. JOBS & SCHEDULES
# ==========================================================
# Create a job that runs all these assets
run_airbnb_job = define_asset_job(
    name="run_airbnb_pipeline",
    selection=[airbnb_assets]
)

# Schedule it to run daily at midnight
daily_schedule = ScheduleDefinition(
    job=run_airbnb_job,
    cron_schedule="0 0 * * *",
)

# ==========================================================
# 5. EXPORT DEFINITIONS (CRITICAL STEP)
# ==========================================================
# This variable MUST be named 'defs' (or passed to a repo)
# and must be at the global scope (not indented).
defs = Definitions(
    assets=[airbnb_assets],
    resources={"dbt": dbt_resource},
    schedules=[daily_schedule],
    jobs=[run_airbnb_job],
)