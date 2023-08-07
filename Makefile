# Define a target to run the script
run_script1:
	python main.py

# Define a target to install all the requirements using pip
install_requirements:
	pip install -r requirements.txt

# Define a target to delete specific files from the project
delete_files:
	rmdir /s /q ResultDataset1 

# Define a target to create a zip file of your project using PowerShell
create_zip:
	powershell Compress-Archive -Path "C:\Users\pawan\PycharmProjects\PyJDBCSparkLoader1" -DestinationPath PyJDBCSparkLoader.zip

# Define a target that depends on both `install_requirements`, `delete_files`, and `create_zip`
build_project: install_requirements delete_files create_zip
