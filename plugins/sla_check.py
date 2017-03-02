"""
This provides a way for a DataDog monitor to ping airflow and query for jobs currently
out of compliance with their SLA defined in an Airflow variable.

DataDogs should be configured similar to the following:

init_config:

instances:
  - name: Airflow SLA's
    url: http://localhost:8080/admin/slacheckview/slacheck
    timeout: 10

    collect_response_time: true
    include_content: true
"""
import logging
from datetime import datetime, timedelta

from flask import Blueprint
from flask import make_response
from flask_admin import BaseView, expose

from airflow import settings
from airflow.models import DagBag, DagRun, Variable
from airflow.utils.dates import date_range, cron_presets
from airflow.utils.state import State

'''
Airflow Variable is expecting a JSON Array formatted as:

[
  {
    'dag_id': 'id_1',
    'max_time': 5
  },
  {
    'dag_id': 'id_2',
    'max_time': 2
  }
]

Which would create two SLA checks, one that a DAG with a DAG ID of id_1
which will begin reporting if the job has not succeeded after 5 hours
and another which will begin reporting if the DAG with DAG ID of id_2 has
not succeeded after 2 hours.
'''
SLA_CONFIG_VARIABLE='slas'

SLACheckBlueprint = Blueprint(
    "sla_check", __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/sla_check')

class SLACheckView(BaseView):
  # Diagnostic endpoint which lists out the discovered SLA's and
  # what time they are due at today.
  @expose('/')
  def index(self):
    logging.info("SLA Check index() called")
    slas = Variable.get(SLA_CONFIG_VARIABLE, default_var=dict(), deserialize_json=True)
    print slas
    return self.render("sla_check/index.html", slas=slas)

  # Wraps the violations for DataDog's format, it will return 200 if
  # there are no violations and 500 and a comma-separated string of
  # dag_id's that are currently in violation.
  @expose('/slacheck')
  def sla_check(self):
    logging.info("SLA Check() called")

    violations = []

    # Load SLA's and DAG configurations
    slas = Variable.get(SLA_CONFIG_VARIABLE, default_var=dict(), deserialize_json=True)
    logging.info("Loaded SLA configuration for {0}".format(slas))

    dagbag = DagBag()

    # Filter down to active, unpaused DAG's with SLA's
    enforceable_slas = []
    for x in slas:
      if x['dag_id'] in dagbag.dags.keys():
        dag = dagbag.dags[x['dag_id']]
        if dag.schedule_interval and not dag.is_paused and not dag.is_subdag:
          enforceable_slas.append(x)
    logging.debug("Found {0} enforceable SLA's".format(len(enforceable_slas)))

    for sla in enforceable_slas:
      dag = dagbag.dags[sla['dag_id']]
      # Check if this DAG uses an Airflow Cron Preset
      if dag.schedule_interval in cron_presets:
        delta = cron_presets[dag.schedule_interval]
      else:
        delta = dag.schedule_interval
      # Compute when it should have run last
      runs = date_range(start_date=dag.start_date, end_date=dag.end_date, delta=delta)

      # Pop -2 because the date_range util will include the execution date of the next run
      # which would false report out of compliance during most recent execution
      last_scheduled_run = runs.pop(-2)

      # Compute when the last run should have finished by
      max_delta = last_scheduled_run + timedelta(hours=sla['max_time'])

      # Check if we care if the last DAG run has succeeded yet
      if datetime.now() >= max_delta:
        logging.debug("Last run SLA for {0} is enforceable".format(sla))
        last_run = settings.Session.query(DagRun).filter(
          DagRun.dag_id == dag.dag_id,
          DagRun.execution_date == last_scheduled_run
        ).first()

        # Report if not successful
        if not last_run or last_run.state != State.SUCCESS:
          logging.debug("Found that the SLA for {0} is in violation of configured SLA".format(sla))
          violations.append(dag.dag_id)

    if len(violations) == 0:
      logging.debug("Found no SLA's out of compliance")
      return make_response()

    else:
      logging.info("Found {0} SLA's out of compliance".format(len(violations)))
      return make_response(", ".join(violations)), 500
