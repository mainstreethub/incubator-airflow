"""
This provides a way to ping airflow and query for jobs currently
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

from flask import Blueprint, jsonify
from flask_admin import BaseView, expose
from airflow import settings
from airflow.models import DagBag, DagRun, Variable
from airflow.utils.state import State

'''
Airflow Variable is expecting a JSON Array formatted as:

[
  {
    'dag_id': 'id_1',
    'duration': 300
  },
  {
    'dag_id': 'id_2',
    'duration': 120
  }
]

Which would create two SLA checks, one that a DAG with a DAG ID of id_1
which will begin reporting if the job has not succeeded after 5 hours
and another which will begin reporting if the DAG with DAG ID of id_2 has
not succeeded after 2 hours.
'''
SLA_CONFIG_VARIABLE = 'slas'

SLACheckBlueprint = Blueprint(
  "sla_check", __name__,
  template_folder='templates',
  static_folder='static',
  static_url_path='/static/sla_check')


class SLACheckView(BaseView):
  @expose('/')
  def index(self):
    logging.info("SLA Check index() called")
    slas = Variable.get(SLA_CONFIG_VARIABLE, default_var=dict(), deserialize_json=True)
    return self.render("sla_check/index.html", slas=slas)

  @expose('/slacheck')
  def sla_check(self):
    logging.info("SLA Check called")

    # load sla configuration
    slas = Variable.get(SLA_CONFIG_VARIABLE, default_var=list(), deserialize_json=True)
    logging.info("Loaded SLA configuration: {}".format(slas))

    dagbag = DagBag()

    logging.info("DAG keys: {}".format(dagbag.dags.keys()))

    # get all dags with an SLA defined in the slas variable
    dags_slas = [(dagbag.dags[s['dag_id']], s['duration']) for s in slas if s['dag_id'] in dagbag.dags.keys()]
    logging.info("DAGs with an sla defined {}".format(dags_slas))

    violations = []

    # for each of these dags with SLAs, find the last run which should have happened
    dags_last_runs = []
    for dag, sla_max_duration in dags_slas:
      if dag.schedule_interval and not dag.is_paused:
        dttm_now = datetime.now()

        latest_execution_run_date, following_schedule_date = _get_run_dates(dag, timedelta(minutes=sla_max_duration))

        # check if execution date + schedule_interval + sla_duration is after the current time
        sla_due_date = following_schedule_date + timedelta(minutes=sla_max_duration)

        logging.info("execution_date={0} for dag_id={1} with an SLA={2} has due_date={3}"
                     .format(latest_execution_run_date, dag.dag_id, sla_max_duration, sla_due_date))

        if (dttm_now - sla_due_date) < timedelta(minutes=50):
          last_dag_run = settings.Session.query(DagRun).filter(
            DagRun.dag_id == dag.dag_id,
            DagRun.execution_date == latest_execution_run_date).first()

          # report if not successful
          if not last_dag_run or last_dag_run.state != State.SUCCESS:
            logging.info("Found that the SLA for {0} is in violation of configured SLA".format(dag.dag_id))
            violations.append(
              {"dag_id": dag.dag_id,
               "due_date": sla_due_date.isoformat(),
               "execution_date": latest_execution_run_date.isoformat(),
               "sla_duration": sla_max_duration,
               "start_date": last_dag_run.start_date.isoformat() if last_dag_run else None
               }
            )

    logging.info("Found {0} SLA's out of compliance".format(len(violations)))
    return jsonify({"sla_misses": violations})


def _get_run_dates(dag, sla_timedelta, dttm=None):
  if dttm is None:
    dttm = datetime.now()

  following_run = dag.following_schedule(dttm)
  previous_run = dag.previous_schedule(dttm)

  while (dttm < previous_run or ((following_run + sla_timedelta) > datetime.now())):
    following_run = previous_run
    previous_run = dag.previous_schedule(previous_run)

  logging.info("previous run to check is {} at the following schedule {}".format(previous_run, following_run))
  return previous_run, following_run

