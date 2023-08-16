# Define a target to install all the requirements using pip

run_all_dim_files:
	for %%i in ($(SCRIPTS_DIR)\*.py) do python %%i

install_requirements:
	pip install -r requirements.txt
venv_activate:
	venv/Scripts/activate

# Define a target to delete specific files from the project
delete_files:
	rmdir /s /q ResultDataset

# Define a target to create a zip file of your project using PowerShell
create_zip:
    powershell Compress-Archive -Path "C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1" -DestinationPath PyJDBCSparkLoader.zip

# Define the path to your scripts directory
SCRIPTS_DIR := src\jobs\dim_jobs

# Define a target to run all .py files in the dim_jobs directory


DIM_JOBS_DIR := src\jobs\daily_run

run_all_fact_files:
	for %%i in ($(DIM_JOBS_DIR)\*.py) do python %%i


run_dim_fact_jobs: run_all_dim_files run_all_fact_files

