from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

# Defining the plugin class
from plugins.auto_backfill import AutoBackfillBlueprint


class AutobackfillPlugin(AirflowPlugin):
    name = "autobackfill_plugin"
    flask_blueprints = [AutoBackfillBlueprint]
