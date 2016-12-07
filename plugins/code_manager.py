import os
import subprocess

from flask import Blueprint, request
from flask_admin import BaseView
from flask_admin import expose
from werkzeug.utils import redirect

from flask import url_for, flash
from airflow.configuration import conf, AirflowConfigException, get_dags_folder
from airflow.models import Variable


def env_or_else(key, subkey, f):
    try:
        return conf.get(key, subkey)
    except AirflowConfigException, e:
        return f()


PATH_ROOT = '/codemanager'
CM_VARIABLE = 'code_manager'
OVERRIDE_KEYS = ['dag_repo', 'dag_commit', 'resource_directory']
STATUS_KEYS = ['git_status']
airflow_env = env_or_else('custom', 'airflow_environment', lambda: 'UNKNOWN')
refresh_dags = env_or_else('custom', 'refresh_dags_command', lambda: None)
restart_server = env_or_else('custom', 'restart_server_command', lambda: None)

CodeManagerBlueprint = Blueprint(
    "code_manager", __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/code_manager')


class CodeManagerView(BaseView):
    def _overrides(self):
        return Variable.get(CM_VARIABLE, default_var=dict(), deserialize_json=True)

    def _status(self):
        if refresh_dags is None:
            return {"git_status": "UNAVAILABLE - not configured"}
        else:
            try:
                status_command = refresh_dags + ' --status'
                result = subprocess.check_output(status_command, shell=True)
                return {"git_status": result}
            except Exception, e:
                return {"git_status": "UNAVAILABLE - error %s" % e.message}

    @expose('/', methods=['get'])
    def do_get(self):
        data = {}
        data.update(self._overrides())
        data.update(self._status())
        return self.render("code_manager/show_sources.html", data=data, override_keys=OVERRIDE_KEYS, status_keys=STATUS_KEYS, env=airflow_env)

    @expose('/edit', methods=['get'])
    def start_edit(self):
        return self.render("code_manager/edit_sources.html", overrides=self._overrides())

    @expose('/', methods=['post'])
    def do_post(self):
        values = {k: v for k, v in request.form.items() if not k.startswith('_')}
        updated = self._overrides()
        updated.update(values)
        Variable.set(CM_VARIABLE, updated, serialize_json=True)
        return redirect(url_for('.do_get'))

    @expose('/refresh', methods=['get'])
    def do_refresh(self):
        if refresh_dags is None:
            flash('refresh_dags_command not specified in airflow.cfg', category='error')
        else:
            try:
                subprocess.check_call(refresh_dags, shell=True)
            except Exception, e:
                flash('Refresh dags failed: %s.' % e.message, category='warning')
        return redirect(url_for('.do_get'))

    @expose('/restart', methods=['get'])
    def do_restart(self):
        if restart_server is None:
            flash('restart_server_command not specified in airflow.cfg', category='error')
        else:
            try:
                restart_cmd = 'nohup %s &' % restart_server
                subprocess.Popen(restart_cmd,
                                 stdout=open('/tmp/restart_webserver.log', 'a'),
                                 stderr=open('/tmp/restart_webserver.log', 'a'),
                                 preexec_fn=os.setpgrp,
                                 shell=True)
                flash('restarting web service in a few seconds...')
            except:
                flash('Restart_server failed', category='error')
        return redirect(url_for('.do_get'))
