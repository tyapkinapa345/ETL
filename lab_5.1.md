API-key: 8c436c2106bf40599dd104558262803


8bc9679a53b7
*** Found local files:
***   * /opt/airflow/logs/dag_id=real_umbrella_dubai/run_id=manual__2026-03-28T14:59:01.900896+00:00/task_id=train_model/attempt=1.log
[2026-03-28, 14:59:07 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: real_umbrella_dubai.train_model manual__2026-03-28T14:59:01.900896+00:00 [queued]>
[2026-03-28, 14:59:07 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: real_umbrella_dubai.train_model manual__2026-03-28T14:59:01.900896+00:00 [queued]>
[2026-03-28, 14:59:07 UTC] {taskinstance.py:2170} INFO - Starting attempt 1 of 2
[2026-03-28, 14:59:07 UTC] {taskinstance.py:2191} INFO - Executing <Task(PythonOperator): train_model> on 2026-03-28 14:59:01.900896+00:00
[2026-03-28, 14:59:07 UTC] {standard_task_runner.py:60} INFO - Started process 222 to run task
[2026-03-28, 14:59:07 UTC] {standard_task_runner.py:87} INFO - Running: ['***', 'tasks', 'run', 'real_umbrella_dubai', 'train_model', 'manual__2026-03-28T14:59:01.900896+00:00', '--job-id', '45', '--raw', '--subdir', 'DAGS_FOLDER/real_umbrella.py', '--cfg-path', '/tmp/tmpvr3c64x1']
[2026-03-28, 14:59:07 UTC] {standard_task_runner.py:88} INFO - Job 45: Subtask train_model
[2026-03-28, 14:59:07 UTC] {task_command.py:423} INFO - Running <TaskInstance: real_umbrella_dubai.train_model manual__2026-03-28T14:59:01.900896+00:00 [running]> on host 8bc9679a53b7
[2026-03-28, 14:59:07 UTC] {taskinstance.py:2480} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='***' AIRFLOW_CTX_DAG_ID='real_umbrella_dubai' AIRFLOW_CTX_TASK_ID='train_model' AIRFLOW_CTX_EXECUTION_DATE='2026-03-28T14:59:01.900896+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-03-28T14:59:01.900896+00:00'
[2026-03-28, 14:59:07 UTC] {taskinstance.py:2698} ERROR - Task failed with exception
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
  File "/opt/airflow/dags/real_umbrella.py", line 91, in train_model
    joblib.dump(model, MODEL_PATH)
  File "/home/airflow/.local/lib/python3.11/site-packages/joblib/numpy_pickle.py", line 599, in dump
    with open(filename, "wb") as f:
         ^^^^^^^^^^^^^^^^^^^^
PermissionError: [Errno 13] Permission denied: '/opt/airflow/data/ml_model.pkl'
[2026-03-28, 14:59:07 UTC] {taskinstance.py:1138} INFO - Marking task as UP_FOR_RETRY. dag_id=real_umbrella_dubai, task_id=train_model, execution_date=20260328T145901, start_date=20260328T145907, end_date=20260328T145907
[2026-03-28, 14:59:07 UTC] {standard_task_runner.py:107} ERROR - Failed to execute job 45 for task train_model ([Errno 13] Permission denied: '/opt/airflow/data/ml_model.pkl'; 222)
[2026-03-28, 14:59:07 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 1
[2026-03-28, 14:59:07 UTC] {taskinstance.py:3280} INFO - 0 downstream tasks scheduled from follow-on schedule check
