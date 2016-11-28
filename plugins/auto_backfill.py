from datetime import datetime
import httplib
import os

from flask import Blueprint, jsonify, Response
from itertools import product, chain
from airflow.models import DagModel, DagBag, TaskInstance
from airflow import settings, conf
from airflow.www.app import csrf
from airflow.utils.state import State


AutoBackfillBlueprint = Blueprint('auto_backfill', __name__, url_prefix='/auto_backfill')
dagbag = DagBag(os.path.expanduser(conf.get('core', 'DAGS_FOLDER')))


def find_dag(session, dag_id):
    """
    find a dag with the given dag_id
    """
    return session.query(DagModel).filter(DagModel.dag_id == dag_id).first()


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
    return backfill_past(dag_id)


def backfill_past(dag_id):
    '''
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

    return "Altered/Created %d task instances" % len(tis_all_altered)
