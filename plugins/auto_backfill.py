from datetime import datetime
import json
import logging
import os

from flask import Blueprint, jsonify, redirect, request
from itertools import product, chain
from airflow.models import DagModel, DagBag, TaskInstance
from airflow import settings, conf
from airflow.www.app import csrf
from airflow.utils.state import State

PATH_ROOT = "/auto_backfill"
AutoBackfillBlueprint = Blueprint('auto_backfill', __name__, url_prefix=PATH_ROOT)
dagbag = DagBag(os.path.expanduser(conf.get('core', 'DAGS_FOLDER')))


@AutoBackfillBlueprint.route('/<dag_id>', methods=['GET'])
@csrf.exempt
def backfill_dag(dag_id):
    """
    .. http:post:: /auto_backfill/<dag_id>/

        Backfills a dag and all its task instances from the start_date to the current date.

        **Example request**:

        .. sourcecode:: http
            POST /auto_backfill/sf_parent_dag_v2
            Host: localhost:7357
            Content-Type: application/json

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 201 OK
            Vary: Accept
            Content-Type: application/json

    """
    origin = request.args.get('origin')
    stats = backfill_past(dag_id)
    logging.info("auto_backfill completed for %s - %s" % (dag_id, json.dumps(stats)))
    if origin is None:
        return jsonify(stats)
    else:
        return redirect(origin)


def backfill_past(dag_id, dates=None):
    '''
    recursively set all tasks to success for a dag back to the start date
    Copied from airflow.www.views.Airflow#success
    :param dag_id:
    :return:
    '''
    dag = dagbag.get_dag(dag_id)

    # Flagging tasks as successful
    session = settings.Session()
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

    tis_all_altered = list(chain(
        [(ti.task_id, ti.execution_date) for ti in tis_to_change],
        tis_to_create))

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
