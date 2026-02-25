from airflow.sdk import dag, task

@dag
def hello_world():
  @task
  def hwt():
    print("Hello Everyone!")

  hwt()

hello_world()
