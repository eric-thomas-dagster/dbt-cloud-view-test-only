"""dbt Cloud component that skips view builds but still runs their tests.

Views are cheap (just CREATE VIEW) but on some platforms they count as billable
model builds. This component rewrites the dbt selection so that views are
selected as test-only using dbt's intersection syntax
(model_name,resource_type:test), while non-view models build normally.

After the run completes, Dagster yields Output events for view assets so they
show as materialized (green) in the asset graph.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import dagster as dg
from dagster.components.resolved.model import Resolver
from dagster_dbt import DbtCloudComponent


class DbtCloudViewTestOnlyRunConfig(dg.Config):
    """Runtime config — visible as JSON in the Dagster launchpad."""

    build_views: bool = dg.Field(
        default=False,
        description=(
            "Force a full build including views for this run. "
            "Useful after schema changes or new view creation."
        ),
    )


class DbtCloudViewTestOnlyComponent(DbtCloudComponent):
    """A DbtCloudComponent that skips view builds but runs their tests.

    All view models are selected as test-only (model_name,resource_type:test)
    so dbt runs their tests without rebuilding the view. Non-view models build
    normally. After the run, Output events are yielded for views so they show
    as materialized in Dagster.

    To force a full build including views (e.g. after schema changes), set
    ``build_views: true`` in the launchpad config JSON.
    """

    skip_view_builds: Annotated[
        bool,
        Resolver.default(
            description=(
                "Skip building view models but still run their tests. "
                "When enabled, Dagster rewrites the dbt selection so views are "
                "selected as test-only. Set to false to disable and use the "
                "standard DbtCloudComponent behavior."
            ),
        ),
    ] = True

    @property
    def op_config_schema(self) -> type[dg.Config] | None:
        if self.skip_view_builds:
            return DbtCloudViewTestOnlyRunConfig
        return None

    def get_cli_args(self, context: dg.AssetExecutionContext) -> list[str]:
        """Rewrite selection so view models are test-only.

        For each selected asset that is a view, rewrites the selection from
        'model_name' to 'model_name,resource_type:test'. Non-view models are
        selected normally. Multiple --select flags create a union in dbt.

        Example: selecting customers (table), stg_customers (view)
        becomes: --select customers --select stg_customers,resource_type:test
        """
        if not self.skip_view_builds:
            return super().get_cli_args(context)

        # Runtime override — build everything including views
        if context.op_config and isinstance(context.op_config, DbtCloudViewTestOnlyRunConfig):
            if context.op_config.build_views:
                return super().get_cli_args(context)

        args = super().get_cli_args(context)

        # Get the manifest to identify which models are views
        workspace_data = self.workspace.get_or_fetch_workspace_data()
        manifest = workspace_data.manifest
        view_names: set[str] = set()
        for node in manifest.get("nodes", {}).values():
            if node.get("config", {}).get("materialized") == "view":
                view_names.add(node.get("name", ""))

        if not view_names:
            return args

        # Rewrite --select args: for views, append ,resource_type:test
        rewritten: list[str] = []
        i = 0
        while i < len(args):
            if args[i] == "--select" and i + 1 < len(args):
                select_value = args[i + 1]
                models = select_value.split()
                for model in models:
                    clean_name = model.lstrip("+@").rstrip("+")
                    short_name = clean_name.split(".")[-1] if "." in clean_name else clean_name
                    if short_name in view_names:
                        rewritten.extend(["--select", f"{model},resource_type:test"])
                    else:
                        rewritten.extend(["--select", model])
                i += 2
            else:
                rewritten.append(args[i])
                i += 1

        return rewritten

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        base_defs = super().build_defs_from_state(context, state_path)

        if not self.skip_view_builds:
            return base_defs

        # Wrap each AssetsDefinition to yield view Outputs after the run
        wrapped_assets: list[Any] = []
        for asset in base_defs.assets or []:
            if isinstance(asset, dg.AssetsDefinition):
                wrapped_assets.append(self._wrap_with_view_outputs(asset))
            else:
                wrapped_assets.append(asset)

        return dg.Definitions(
            assets=wrapped_assets,
            resources=base_defs.resources,
            schedules=base_defs.schedules,
            sensors=base_defs.sensors,
        )

    def _wrap_with_view_outputs(
        self, original: dg.AssetsDefinition
    ) -> dg.AssetsDefinition:
        """Replace the multi_asset execution to add view Output yielding."""
        workspace = self.workspace
        translator = self.translator

        @dg.multi_asset(
            specs=list(original.specs),
            check_specs=list(original.check_specs),
            can_subset=True,
            name=original.op.name,
        )
        def _view_aware_assets(context: dg.AssetExecutionContext):
            cli_args = self.get_cli_args(context)
            invocation = workspace.cli(
                cli_args,
                dagster_dbt_translator=translator,
                context=context,
            )

            # Run the standard wait() and track which outputs were yielded
            yielded_output_names: set[str] = set()
            for event in invocation.wait():
                if isinstance(event, dg.Output):
                    yielded_output_names.add(event.output_name)
                yield event

            # Check if views were force-built via runtime config
            build_views_override = (
                context.op_config.build_views
                if context.op_config
                and isinstance(context.op_config, DbtCloudViewTestOnlyRunConfig)
                else False
            )
            if build_views_override:
                return

            # Yield Output for view assets that were selected but not built.
            # The view exists in the warehouse from a prior build and its tests
            # passed — record the materialization so it shows green in Dagster.
            manifest = invocation.manifest
            for key in context.selected_asset_keys:
                output_name = key.to_python_identifier()
                if output_name in yielded_output_names:
                    continue
                for node in manifest.get("nodes", {}).values():
                    if node.get("config", {}).get("materialized") == "view":
                        node_key = translator.get_asset_key(node)
                        if node_key == key:
                            context.log.info(
                                f"Recording materialization for view {key} "
                                f"(build skipped, tests ran)"
                            )
                            yield dg.Output(
                                value=None,
                                output_name=output_name,
                                metadata={
                                    "build_skipped": True,
                                    "reason": "view",
                                },
                            )
                            break

        return _view_aware_assets
