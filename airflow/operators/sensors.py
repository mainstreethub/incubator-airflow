# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from datetime import datetime
import logging
from urllib.parse import urlparse
from time import sleep

import airflow
from airflow import hooks, settings
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
from airflow.models import BaseOperator, TaskInstance, Connection as DB
from airflow.hooks.base_hook import BaseHook
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults


class BaseSensorOperator(BaseOperator):
    '''
    Sensor operators are derived from this class an inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
        a criteria is met and fail if and when they time out.

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    '''
    ui_color = '#e6f1f2'

    @apply_defaults
    def __init__(
            self,
            poke_interval=60,
            timeout=60*60*24*7,
            soft_fail=False,
            *args, **kwargs):
        super(BaseSensorOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout

    def poke(self, context):
        '''
        Function that the sensors defined while deriving this class should
        override.
        '''
        raise AirflowException('Override me.')

    def execute(self, context):
        started_at = datetime.now()
        while not self.poke(context):
            if (datetime.now() - started_at).total_seconds() > self.timeout:
                if self.soft_fail:
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            sleep(self.poke_interval)
        logging.info("Success criteria met. Exiting.")


class SqlSensor(BaseSensorOperator):
    """
    Runs a sql statement until a criteria is met. It will keep trying until
    sql returns no row, or if the first cell in (0, '0', '').

    :param conn_id: The connection to run the sensor against
    :type conn_id: string
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    """
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(self, conn_id, sql, *args, **kwargs):
        self.sql = sql
        self.conn_id = conn_id
        super(SqlSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        hook = BaseHook.get_connection(self.conn_id).get_hook()

        logging.info('Poking: ' + self.sql)
        records = hook.get_records(self.sql)
        if not records:
            return False
        else:
            if str(records[0][0]) in ('0', '',):
                return False
            else:
                return True
            print(records[0][0])


class MetastorePartitionSensor(SqlSensor):
    """
    An alternative to the HivePartitionSensor that talk directly to the
    MySQL db. This was created as a result of observing sub optimal
    queries generated by the Metastore thrift service when hitting
    subpartitioned tables. The Thrift service's queries were written in a
    way that wouldn't leverage the indexes.

    :param schema: the schema
    :type schema: str
    :param table: the table
    :type table: str
    :param partition_name: the partition name, as defined in the PARTITIONS
        table of the Metastore. Order of the fields does matter.
        Examples: ``ds=2016-01-01`` or
        ``ds=2016-01-01/sub=foo`` for a sub partitioned table
    :type partition_name: str
    :param mysql_conn_id: a reference to the MySQL conn_id for the metastore
    :type mysql_conn_id: str
    """
    template_fields = ('partition_name', 'table', 'schema')

    @apply_defaults
    def __init__(
            self, table, partition_name, schema="default",
            mysql_conn_id="metastore_mysql",
            *args, **kwargs):

        self.partition_name = partition_name
        self.table = table
        self.schema = schema
        self.first_poke = True
        self.conn_id = mysql_conn_id
        super(SqlSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        if self.first_poke:
            self.first_poke = False
            if '.' in self.table:
                self.schema, self.table = self.table.split('.')
            self.sql = """
            SELECT 'X'
            FROM PARTITIONS A0
            LEFT OUTER JOIN TBLS B0 ON A0.TBL_ID = B0.TBL_ID
            LEFT OUTER JOIN DBS C0 ON B0.DB_ID = C0.DB_ID
            WHERE
                B0.TBL_NAME = '{self.table}' AND
                C0.NAME = '{self.schema}' AND
                A0.PART_NAME = '{self.partition_name}';
            """.format(self=self)
        return super(MetastorePartitionSensor, self).poke(context)


class ExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a task to complete in a different DAG

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_delta: datetime.timedelta
    :param execution_date_fn: function that receives the current execution date
        and returns the desired execution date to query. Either execution_delta
        or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :type execution_date_fn: callable
    """

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            allowed_states=None,
            execution_delta=None,
            execution_date_fn=None,
            *args, **kwargs):
        super(ExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_date` or `execution_date_fn` may'
                'be provided to ExternalTaskSensor; not both.')

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    def poke(self, context):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context['execution_date'])
        else:
            dttm = context['execution_date']

        logging.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} on '
            '{dttm} ... '.format(**locals()))
        TI = TaskInstance

        session = settings.Session()
        count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state.in_(self.allowed_states),
            TI.execution_date == dttm,
        ).count()
        session.commit()
        session.close()
        return count


class NamedHivePartitionSensor(BaseSensorOperator):
    """
    Waits for a set of partitions to show up in Hive.

    :param partition_names: List of fully qualified names of the
        partitions to wait for. A fully qualified name is of the
        form schema.table/pk1=pv1/pk2=pv2, for example,
        default.users/ds=2016-01-01. This is passed as is to the metastore
        Thrift client "get_partitions_by_name" method. Note that
        you cannot use logical operators as in HivePartitionSensor.
    :type partition_names: list of strings
    :param metastore_conn_id: reference to the metastore thrift service
        connection id
    :type metastore_conn_id: str
    """

    template_fields = ('partition_names', )

    @apply_defaults
    def __init__(
            self,
            partition_names,
            metastore_conn_id='metastore_default',
            poke_interval=60*3,
            *args,
            **kwargs):
        super(NamedHivePartitionSensor, self).__init__(
            poke_interval=poke_interval, *args, **kwargs)

        if isinstance(partition_names, basestring):
            raise TypeError('partition_names must be an array of strings')

        for partition_name in partition_names:
            self.parse_partition_name(partition_name)

        self.metastore_conn_id = metastore_conn_id
        self.partition_names = partition_names
        self.next_poke_idx = 0

    def parse_partition_name(self, partition):
        try:
            schema, table_partition = partition.split('.')
            table, partition = table_partition.split('/', 1)
            return schema, table, partition
        except ValueError as e:
            raise ValueError('Could not parse ' + partition)

    def poke(self, context):

        if not hasattr(self, 'hook'):
            self.hook = airflow.hooks.hive_hooks.HiveMetastoreHook(
                metastore_conn_id=self.metastore_conn_id)

        def poke_partition(partition):

            schema, table, partition = self.parse_partition_name(partition)

            logging.info(
                'Poking for {schema}.{table}/{partition}'.format(**locals())
            )
            return self.hook.check_for_named_partition(
                schema, table, partition)

        while self.next_poke_idx < len(self.partition_names):
            if poke_partition(self.partition_names[self.next_poke_idx]):
                self.next_poke_idx += 1
            else:
                return False

        return True


class HivePartitionSensor(BaseSensorOperator):
    """
    Waits for a partition to show up in Hive.

    Note: Because @partition supports general logical operators, it
    can be inefficient. Consider using NamedHivePartitionSensor instead if
    you don't need the full flexibility of HivePartitionSensor.

    :param table: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :type table: string
    :param partition: The partition clause to wait for. This is passed as
        is to the metastore Thrift client "get_partitions_by_filter" method,
        and apparently supports SQL like notation as in `ds='2015-01-01'
        AND type='value'` and > < sings as in "ds>=2015-01-01"
    :type partition: string
    :param metastore_conn_id: reference to the metastore thrift service
        connection id
    :type metastore_conn_id: str
    """
    template_fields = ('schema', 'table', 'partition',)

    @apply_defaults
    def __init__(
            self,
            table, partition="ds='{{ ds }}'",
            metastore_conn_id='metastore_default',
            schema='default',
            poke_interval=60*3,
            *args, **kwargs):
        super(HivePartitionSensor, self).__init__(
            poke_interval=poke_interval, *args, **kwargs)
        if not partition:
            partition = "ds='{{ ds }}'"
        self.metastore_conn_id = metastore_conn_id
        self.table = table
        self.partition = partition
        self.schema = schema

    def poke(self, context):
        if '.' in self.table:
            self.schema, self.table = self.table.split('.')
        logging.info(
            'Poking for table {self.schema}.{self.table}, '
            'partition {self.partition}'.format(**locals()))
        if not hasattr(self, 'hook'):
            self.hook = airflow.hooks.hive_hooks.HiveMetastoreHook(
                metastore_conn_id=self.metastore_conn_id)
        return self.hook.check_for_partition(
            self.schema, self.table, self.partition)


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)

    @apply_defaults
    def __init__(
            self,
            filepath,
            hdfs_conn_id='hdfs_default',
            *args, **kwargs):
        super(HdfsSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.hdfs_conn_id = hdfs_conn_id

    def poke(self, context):
        import airflow.hooks.hdfs_hook
        sb = airflow.hooks.hdfs_hook.HDFSHook(self.hdfs_conn_id).get_conn()
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        logging.info(
            'Poking for file {self.filepath} '.format(**locals()))
        try:
            files = [f for f in sb.ls([self.filepath])]
        except:
            return False
        return True


class WebHdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)

    @apply_defaults
    def __init__(
            self,
            filepath,
            webhdfs_conn_id='webhdfs_default',
            *args, **kwargs):
        super(WebHdfsSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.webhdfs_conn_id = webhdfs_conn_id

    def poke(self, context):
        c = airflow.hooks.webhdfs_hook.WebHDFSHook(self.webhdfs_conn_id)
        logging.info(
            'Poking for file {self.filepath} '.format(**locals()))
        return c.check_for_path(hdfs_path=self.filepath)


class S3KeySensor(BaseSensorOperator):
    """
    Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level.
    :type bucket_key: str
    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :type wildcard_match: bool
    :param s3_conn_id: a reference to the s3 connection
    :type s3_conn_id: str
    """
    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(
            self, bucket_key,
            bucket_name=None,
            wildcard_match=False,
            s3_conn_id='s3_default',
            *args, **kwargs):
        super(S3KeySensor, self).__init__(*args, **kwargs)
        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == s3_conn_id).first()
        if not db:
            raise AirflowException("conn_id doesn't exist in the repository")
        # Parse
        if bucket_name is None:
            parsed_url = urlparse(bucket_key)
            if parsed_url.netloc == '':
                raise AirflowException('Please provide a bucket_name')
            else:
                bucket_name = parsed_url.netloc
                if parsed_url.path[0] == '/':
                    bucket_key = parsed_url.path[1:]
                else:
                    bucket_key = parsed_url.path
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.s3_conn_id = s3_conn_id
        session.commit()
        session.close()

    def poke(self, context):
        import airflow.hooks.S3_hook
        hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id=self.s3_conn_id)
        full_url = "s3://" + self.bucket_name + "/" + self.bucket_key
        logging.info('Poking for key : {full_url}'.format(**locals()))
        if self.wildcard_match:
            return hook.check_for_wildcard_key(self.bucket_key,
                                               self.bucket_name)
        else:
            return hook.check_for_key(self.bucket_key, self.bucket_name)


class S3PrefixSensor(BaseSensorOperator):
    """
    Waits for a prefix to exist. A prefix is the first part of a key,
    thus enabling checking of constructs similar to glob airfl* or
    SQL LIKE 'airfl%'. There is the possibility to precise a delimiter to
    indicate the hierarchy or keys, meaning that the match will stop at that
    delimiter. Current code accepts sane delimiters, i.e. characters that
    are NOT special characters in the Python regex engine.

    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :type prefix: str
    :param delimiter: The delimiter intended to show hierarchy.
        Defaults to '/'.
    :type delimiter: str
    """
    template_fields = ('prefix', 'bucket_name')

    @apply_defaults
    def __init__(
            self, bucket_name,
            prefix, delimiter='/',
            s3_conn_id='s3_default',
            *args, **kwargs):
        super(S3PrefixSensor, self).__init__(*args, **kwargs)
        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == s3_conn_id).first()
        if not db:
            raise AirflowException("conn_id doesn't exist in the repository")
        # Parse
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.delimiter = delimiter
        self.full_url = "s3://" + bucket_name + '/' + prefix
        self.s3_conn_id = s3_conn_id
        session.commit()
        session.close()

    def poke(self, context):
        logging.info('Poking for prefix : {self.prefix}\n'
                     'in bucket s3://{self.bucket_name}'.format(**locals()))
        import airflow.hooks.S3_hook
        hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id=self.s3_conn_id)
        return hook.check_for_prefix(
            prefix=self.prefix,
            delimiter=self.delimiter,
            bucket_name=self.bucket_name)


class TimeSensor(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    :param target_time: time after which the job succeeds
    :type target_time: datetime.time
    """
    template_fields = tuple()

    @apply_defaults
    def __init__(self, target_time, *args, **kwargs):
        super(TimeSensor, self).__init__(*args, **kwargs)
        self.target_time = target_time

    def poke(self, context):
        logging.info(
            'Checking if the time ({0}) has come'.format(self.target_time))
        return datetime.now().time() > self.target_time


class TimeDeltaSensor(BaseSensorOperator):
    """
    Waits for a timedelta after the task's execution_date + schedule_interval.
    In Airflow, the daily task stamped with ``execution_date``
    2016-01-01 can only start running on 2016-01-02. The timedelta here
    represents the time after the execution period has closed.

    :param delta: time length to wait after execution_date before succeeding
    :type delta: datetime.timedelta
    """
    template_fields = tuple()

    @apply_defaults
    def __init__(self, delta, *args, **kwargs):
        super(TimeDeltaSensor, self).__init__(*args, **kwargs)
        self.delta = delta

    def poke(self, context):
        dag = context['dag']
        target_dttm = dag.following_schedule(context['execution_date'])
        target_dttm += self.delta
        logging.info('Checking if the time ({0}) has come'.format(target_dttm))
        return datetime.now() > target_dttm


class HttpSensor(BaseSensorOperator):
    """
    Executes a HTTP get statement and returns False on failure:
        404 not found or response_check function returned False

    :param http_conn_id: The connection to run the sensor against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url
    :type endpoint: string
    :param params: The parameters to be added to the GET url
    :type params: a dictionary of string key/value pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        Returns True for 'pass' and False otherwise.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ('endpoint',)

    @apply_defaults
    def __init__(self,
                 endpoint,
                 http_conn_id='http_default',
                 params=None,
                 headers=None,
                 response_check=None,
                 extra_options=None, *args, **kwargs):
        super(HttpSensor, self).__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.params = params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check

        self.hook = hooks.http_hook.HttpHook(method='GET', http_conn_id=http_conn_id)

    def poke(self, context):
        logging.info('Poking: ' + self.endpoint)
        try:
            response = self.hook.run(self.endpoint,
                                     data=self.params,
                                     headers=self.headers,
                                     extra_options=self.extra_options)
            if self.response_check:
                # run content check on response
                return self.response_check(response)
        except AirflowException as ae:
            if str(ae).startswith("404"):
                return False

            raise ae

        return True
