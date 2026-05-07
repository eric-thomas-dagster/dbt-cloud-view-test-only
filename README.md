# dbt Cloud View Test-Only Component

A custom Dagster component that extends `DbtCloudComponent` to skip rebuilding dbt view models while still running their tests.

## Problem

Views are cheap (`CREATE VIEW`) but on some platforms they count as billable model builds. Running `dbt build` rebuilds every view on every run, even though views always reflect the current state of their upstream tables.

Using `--exclude config.materialized:view` doesn't work because dbt's exclude cascades to tests — excluding views also excludes their tests.

## Solution

This component rewrites the dbt selection at runtime so that views are selected as **test-only** using dbt's intersection syntax:

```
model_name,resource_type:test
```

This tells dbt: "select only tests for this model, not the model itself." Non-view models (tables, incrementals) build normally.

After the run completes, Dagster yields `Output` events for view assets so they appear as materialized (green) in the asset graph — since the view exists in the warehouse from a prior build and its tests passed.

### Example

Selecting `customers` (table), `stg_customers` (view), `stg_orders` (view) becomes:

```
--select customers --select stg_customers,resource_type:test --select stg_orders,resource_type:test
```

## Setup

1. Clone this repo
2. Update `src/dbt_cloud_view_test_only/defs/my_dbt_project/defs.yaml` with your dbt Cloud credentials
3. Set the `DBT_CLOUD_TOKEN` environment variable
4. Install and run:

```bash
uv sync
uv run dg dev
```

## Runtime override: force a full build

When you need to rebuild views (e.g. after schema changes or new view creation), set `build_views` to `true` in the launchpad config JSON:

```json
{"ops": {"dbt_cloud_assets": {"config": {"build_views": true}}}}
```

Replace `dbt_cloud_assets` with your `op.name` from `defs.yaml`. Subsequent runs resume skipping views.

## Configuration

In `defs.yaml`:

```yaml
type: dbt_cloud_view_test_only.components.dbt_cloud_view_test_only_component.DbtCloudViewTestOnlyComponent

attributes:
  workspace:
    account_id: YOUR_ACCOUNT_ID
    token: "{{ env.DBT_CLOUD_TOKEN }}"
    access_url: "https://cloud.getdbt.com"
    project_id: YOUR_PROJECT_ID
    environment_id: YOUR_ENVIRONMENT_ID

  op:
    name: dbt_cloud_assets

  # Enabled by default. Set to false to use standard DbtCloudComponent behavior.
  skip_view_builds: true
```

All standard `DbtCloudComponent` attributes (`exclude`, `select`, `create_sensor`, `translation_settings`, etc.) are also available.
