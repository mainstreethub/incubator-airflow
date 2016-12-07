from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

# Defining the plugin class
from plugins.auto_backfill import AutoBackfillBlueprint
from plugins.code_manager import CodeManagerView, CodeManagerBlueprint


class AutobackfillPlugin(AirflowPlugin):
    name = "autobackfill_plugin"
    flask_blueprints = [AutoBackfillBlueprint]


class CodeManagerPlugin(AirflowPlugin):
    name = "codemanager_plugin"
    flask_blueprints = [CodeManagerBlueprint]
    admin_views = [CodeManagerView(name="SourceManager", category="MSH")]
