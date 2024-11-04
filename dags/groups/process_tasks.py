from airflow.utils.task_group import TaskGroup
from airflow.decorators import task, task_group

@task.python
def process_a(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_b(partner_name, partner_path):
    print(partner_name)
    print(partner_path)
    
@task.python
def process_c(partner_name, partner_path):
    print(partner_name)
    print(partner_path)
    
    
@task.python
def check_a():
    print("check_a!!!")

@task.python
def check_b():
    print("check_b!!!")

@task.python
def check_c():
    print("check_c!!!")

def process_tasks(partner_settings):
    with TaskGroup(group_id="process_tasks", add_suffix_on_collision=True) as process_tasks:
        with TaskGroup(group_id="test_group") as test_group:
            check_a()
            check_b()
            check_c()
        
        process_a(partner_settings["partner_name"], partner_settings["partner_path"]) >> test_group
        process_b(partner_settings["partner_name"], partner_settings["partner_path"]) >> test_group
        process_c(partner_settings["partner_name"], partner_settings["partner_path"]) >> test_group
    return process_tasks
    