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
CURRENT_DIR = Path(__file__).parent
DBT_PROJECT_DIR = CURRENT_DIR.joinpath("airbnb")
DBT_MANIFEST_PATH = DBT_PROJECT_DIR.joinpath("target", "manifest.json")

# ==========================================================
# 2. DEFINE THE DBT RESOURCE
# ==========================================================
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    # Uncomment if profiles.yml is inside the airbnb folder:
    # profiles_dir=os.fspath(DBT_PROJECT_DIR),
)

# ==========================================================
# 3. CUSTOM TRANSLATOR (METHOD 3 LOGIC)
# ==========================================================
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """
    This class defines the rules for how dbt models are displayed in Dagster.
    We override the 'get_group_name' function to group them automatically.
    """
    def get_group_name(self, dbt_resource_props):
        # 1. Get the name of the dbt model (e.g., "stg_users", "fct_orders")
        model_name = dbt_resource_props["name"]
        
        # 2. Apply Logic: Check prefixes to assign groups
        if model_name.startswith("stg_"):
            return "staging_layer"
        
        elif model_name.startswith("dim_") or model_name.startswith("fct_"):
            return "marts_layer"
        
        elif model_name.startswith("seed_") or model_name.startswith("raw_"):
            return "raw_data"
            
        else:
            # Fallback for anything else (tests, snapshots, etc.)
            return "other_models"

# ==========================================================
# 4. LOAD DBT ASSETS WITH CUSTOM TRANSLATOR
# ==========================================================
@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    # HERE IS THE CHANGE: We inject our custom class
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    name="airbnb_dbt_models",
)
def airbnb_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# ==========================================================
# 5. JOBS & SCHEDULES
# ==========================================================
run_airbnb_job = define_asset_job(
    name="run_airbnb_pipeline",
    selection=[airbnb_assets]
)

daily_schedule = ScheduleDefinition(
    job=run_airbnb_job,
    cron_schedule="0 0 * * *",
)

# ==========================================================
# 6. EXPORT DEFINITIONS
# ==========================================================
defs = Definitions(
    assets=[airbnb_assets],
    resources={"dbt": dbt_resource},
    schedules=[daily_schedule],
    jobs=[run_airbnb_job],
)