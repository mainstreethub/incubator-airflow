from datetime import datetime
import json
import logging
import os
from itertools import product

from flask import Blueprint, jsonify, redirect, request
from flask_login import flash
from airflow.configuration import AirflowConfigException, get_dags_folder

from airflow.models import DagBag, TaskInstance
from airflow import settings, conf
from airflow.www.app import csrf
from airflow.utils.state import State

PATH_ROOT = "/auto_backfill"
AutoBackfillBlueprint = Blueprint('auto_backfill', __name__, url_prefix=PATH_ROOT)
dagbag = DagBag(get_dags_folder())
try:
    airflow_env = conf.get('custom', 'airflow_environment')
except AirflowConfigException, e:
    airflow_env = 'UNKNOWN'

@AutoBackfillBlueprint.route('/<dag_id>', methods=['GET'])
@csrf.exempt
def backfill_dag(dag_id):
    """
    .. http:post:: /auto_backfill/<dag_id>?origin=...

        Backfills a dag and all its task instances from the start_date to the current date.

        **Example request**:

        .. sourcecode:: http
            GET /auto_backfill/example_subdag_operator
            Host: localhost:8080
            Content-Type: application/json

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Vary: Accept
            Content-Type: application/json
            {
                example_subdag_operator: {
                    created: 0,
                    updated: 0
                },
                example_subdag_operator.section-1: {
                    created: 0,
                    updated: 0
                },
                example_subdag_operator.section-2: {
                    created: 0,
                    updated: 0
                }
            }

    """
    origin = request.args.get('origin')
    def respond(msg, level='info'):
        if origin is None:
            return jsonify({'message': msg, 'level': level})
        else:
            flash(msg, level)
            return redirect(origin)
    if airflow_env not in ('dev', 'test'):
        return respond('Failed to auto_backfill %s.  Auto backfill is only supported in dev or test.' % dag_id, 'warning')
    else:
        try:
            stats = backfill_past(dag_id)
            msg = "Completed auto_backfill for %s - %s" % (dag_id, json.dumps(stats))
            logging.info(msg)
            return respond(msg, 'info')
        except Exception, e:
            msg = "Unable to update %s because of %s" % (dag_id, e.message)
            logging.warn(msg)
            return respond(msg, 'warning')


def backfill_past(dag_id, dates=None):
    '''
    recursively set all tasks to success for a dag back to the start date
    Copied from airflow.www.views.Airflow#success
    :param dag_id:
    :return:
    '''
    dag = dagbag.get_dag(dag_id)

    execution_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = execution_date

    if 'start_date' in dag.default_args:
        start_date = dag.default_args['start_date']
    elif dag.start_date:
        start_date = dag.start_date
    else:
        start_date = execution_date

    task_ids = dag.task_ids

    TI = TaskInstance

    if dates is None:
        if dag.schedule_interval == '@once':
            dates = [start_date]
        else:
            dates = dag.date_range(start_date, end_date=end_date)

    session = settings.Session()
    tis = session.query(TI).filter(
        TI.dag_id == dag_id,
        TI.execution_date.in_(dates),
        TI.task_id.in_(task_ids)).all()
    tis_to_change = session.query(TI).filter(
        TI.dag_id == dag_id,
        TI.execution_date.in_(dates),
        TI.task_id.in_(task_ids),
        TI.state != State.SUCCESS).all()
    tasks = list(product(task_ids, dates))
    tis_to_create = list(
        set(tasks) -
        set([(ti.task_id, ti.execution_date) for ti in tis]))

    for ti in tis_to_change:
        ti.state = State.SUCCESS
    session.commit()

    for task_id, task_execution_date in tis_to_create:
        ti = TI(
            task=dag.get_task(task_id),
            execution_date=task_execution_date,
            state=State.SUCCESS)
        session.add(ti)
        session.commit()

    session.commit()
    session.close()

    stats = {dag_id: {"created": len(tis_to_create), "updated": len(tis_to_change)}}

    # modify any subdagOperators
    subdags_ids = [sd.subdag.dag_id for sd in dag.tasks if sd.task_type == "SubDagOperator"]
    for sd in subdags_ids:
        sub_stats = backfill_past(sd, dates)
        stats.update(sub_stats)

    return stats
