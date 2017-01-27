from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

# Defining the plugin class
from plugins.auto_backfill import AutoBackfillBlueprint
from plugins.code_manager import CodeManagerView, CodeManagerBlueprint
from plugins.sla_check import SLACheckBlueprint, SLACheckView


class AutobackfillPlugin(AirflowPlugin):
    name = "autobackfill_plugin"
    flask_blueprints = [AutoBackfillBlueprint]


class CodeManagerPlugin(AirflowPlugin):
    name = "codemanager_plugin"
    flask_blueprints = [CodeManagerBlueprint]
    admin_views = [CodeManagerView(name="SourceManager", category="MSH")]

class SLACheckPlugin(AirflowPlugin):
    name="slacheck_plugin"
    flask_blueprints = [SLACheckBlueprint]
    admin_views = [SLACheckView(name="SLA Check", category="MSH")]