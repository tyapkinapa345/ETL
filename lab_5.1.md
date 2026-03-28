API-key: 8c436c2106bf40599dd104558262803


8bc9679a53b7
*** Found local files:
***   * /opt/airflow/logs/dag_id=variant_16/run_id=manual__2026-03-28T14:42:03.482423+00:00/task_id=fetch_weather_forecast/attempt=1.log
[2026-03-28, 14:42:06 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: variant_16.fetch_weather_forecast manual__2026-03-28T14:42:03.482423+00:00 [queued]>
[2026-03-28, 14:42:06 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: variant_16.fetch_weather_forecast manual__2026-03-28T14:42:03.482423+00:00 [queued]>
[2026-03-28, 14:42:06 UTC] {taskinstance.py:2170} INFO - Starting attempt 1 of 2
[2026-03-28, 14:42:06 UTC] {taskinstance.py:2191} INFO - Executing <Task(PythonOperator): fetch_weather_forecast> on 2026-03-28 14:42:03.482423+00:00
[2026-03-28, 14:42:06 UTC] {standard_task_runner.py:60} INFO - Started process 236 to run task
[2026-03-28, 14:42:06 UTC] {standard_task_runner.py:87} INFO - Running: ['***', 'tasks', 'run', 'variant_16', 'fetch_weather_forecast', 'manual__2026-03-28T14:42:03.482423+00:00', '--job-id', '39', '--raw', '--subdir', 'DAGS_FOLDER/real_umbrella.py', '--cfg-path', '/tmp/tmp8nkqln7o']
[2026-03-28, 14:42:06 UTC] {standard_task_runner.py:88} INFO - Job 39: Subtask fetch_weather_forecast
[2026-03-28, 14:42:06 UTC] {task_command.py:423} INFO - Running <TaskInstance: variant_16.fetch_weather_forecast manual__2026-03-28T14:42:03.482423+00:00 [running]> on host 8bc9679a53b7
[2026-03-28, 14:42:07 UTC] {taskinstance.py:2480} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='***' AIRFLOW_CTX_DAG_ID='variant_16' AIRFLOW_CTX_TASK_ID='fetch_weather_forecast' AIRFLOW_CTX_EXECUTION_DATE='2026-03-28T14:42:03.482423+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-03-28T14:42:03.482423+00:00'
[2026-03-28, 14:42:07 UTC] {taskinstance.py:2698} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 433, in _execute_task
    result = execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 199, in execute
    return_value = self.execute_callable()
                   ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 216, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/airflow/dags/real_umbrella.py", line 47, in fetch_weather_forecast
    df.to_csv(RAW_DATA_PATH, index=False)
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/util/_decorators.py", line 333, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/core/generic.py", line 3989, in to_csv
    return DataFrameRenderer(formatter).to_csv(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/io/formats/format.py", line 1014, in to_csv
    csv_formatter.save()
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/io/formats/csvs.py", line 251, in save
    with get_handle(
         ^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/io/common.py", line 873, in get_handle
    handle = open(
             ^^^^^
PermissionError: [Errno 13] Permission denied: '/opt/airflow/data/dubai_forecast.csv'
[2026-03-28, 14:42:07 UTC] {taskinstance.py:1138} INFO - Marking task as UP_FOR_RETRY. dag_id=variant_16, task_id=fetch_weather_forecast, execution_date=20260328T144203, start_date=20260328T144206, end_date=20260328T144207
[2026-03-28, 14:42:07 UTC] {standard_task_runner.py:107} ERROR - Failed to execute job 39 for task fetch_weather_forecast ([Errno 13] Permission denied: '/opt/airflow/data/dubai_forecast.csv'; 236)
[2026-03-28, 14:42:07 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 1
[2026-03-28, 14:42:07 UTC] {taskinstance.py:3280} INFO - 0 downstream tasks scheduled from follow-on schedule check



8bc9679a53b7
*** Found local files:
***   * /opt/airflow/logs/dag_id=variant_16/run_id=manual__2026-03-28T14:42:03.482423+00:00/task_id=fetch_weather_forecast/attempt=2.log
[2026-03-28, 14:47:10 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: variant_16.fetch_weather_forecast manual__2026-03-28T14:42:03.482423+00:00 [queued]>
[2026-03-28, 14:47:10 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: variant_16.fetch_weather_forecast manual__2026-03-28T14:42:03.482423+00:00 [queued]>
[2026-03-28, 14:47:10 UTC] {taskinstance.py:2170} INFO - Starting attempt 2 of 2
[2026-03-28, 14:47:10 UTC] {taskinstance.py:2191} INFO - Executing <Task(PythonOperator): fetch_weather_forecast> on 2026-03-28 14:42:03.482423+00:00
[2026-03-28, 14:47:10 UTC] {standard_task_runner.py:60} INFO - Started process 326 to run task
[2026-03-28, 14:47:10 UTC] {standard_task_runner.py:87} INFO - Running: ['***', 'tasks', 'run', 'variant_16', 'fetch_weather_forecast', 'manual__2026-03-28T14:42:03.482423+00:00', '--job-id', '41', '--raw', '--subdir', 'DAGS_FOLDER/real_umbrella.py', '--cfg-path', '/tmp/tmppfy40kya']
[2026-03-28, 14:47:10 UTC] {standard_task_runner.py:88} INFO - Job 41: Subtask fetch_weather_forecast
[2026-03-28, 14:47:10 UTC] {task_command.py:423} INFO - Running <TaskInstance: variant_16.fetch_weather_forecast manual__2026-03-28T14:42:03.482423+00:00 [running]> on host 8bc9679a53b7
[2026-03-28, 14:47:11 UTC] {taskinstance.py:2480} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='***' AIRFLOW_CTX_DAG_ID='variant_16' AIRFLOW_CTX_TASK_ID='fetch_weather_forecast' AIRFLOW_CTX_EXECUTION_DATE='2026-03-28T14:42:03.482423+00:00' AIRFLOW_CTX_TRY_NUMBER='2' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-03-28T14:42:03.482423+00:00'
[2026-03-28, 14:47:11 UTC] {taskinstance.py:2698} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 433, in _execute_task
    result = execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 199, in execute
    return_value = self.execute_callable()
                   ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 216, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/airflow/dags/real_umbrella.py", line 47, in fetch_weather_forecast
    df.to_csv(RAW_DATA_PATH, index=False)
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/util/_decorators.py", line 333, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/core/generic.py", line 3989, in to_csv
    return DataFrameRenderer(formatter).to_csv(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/io/formats/format.py", line 1014, in to_csv
    csv_formatter.save()
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/io/formats/csvs.py", line 251, in save
    with get_handle(
         ^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/pandas/io/common.py", line 873, in get_handle
    handle = open(
             ^^^^^
PermissionError: [Errno 13] Permission denied: '/opt/airflow/data/dubai_forecast.csv'
[2026-03-28, 14:47:11 UTC] {taskinstance.py:1138} INFO - Marking task as FAILED. dag_id=variant_16, task_id=fetch_weather_forecast, execution_date=20260328T144203, start_date=20260328T144710, end_date=20260328T144711
[2026-03-28, 14:47:11 UTC] {standard_task_runner.py:107} ERROR - Failed to execute job 41 for task fetch_weather_forecast ([Errno 13] Permission denied: '/opt/airflow/data/dubai_forecast.csv'; 326)
[2026-03-28, 14:47:11 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 1
[2026-03-28, 14:47:11 UTC] {taskinstance.py:3280} INFO - 0 downstream tasks scheduled from follow-on schedule check
