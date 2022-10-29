# EDFS using MySQL and Firebase
Implementation of an Emulated Distributed File System(EDFS) using MySQL and Firebase(JSON). Includes Partition based Map-Reduce(PMR) for performing statistical analysis on the data stored on the EDFS

# Instructions
1. Create a .env file in the project directory and copy the following contents into it:
USERNAME = 'enter_mysql_username' -> Replace with your mysql username
PASSWORD = 'enter_mysql_password' -> Replace with your mysql password
MAX_PARTITION_SIZE = 32768
HOST = 'enter_host_name' -> Replace with your hostname where mysql runs
DATABASE = 'dsci551_project' -> Replace with your mysql database used in init.sql file
DEFAULT_DIR_PERMISSION = 755
DEFAULT_FILE_PERMISSION = 644
REPLICATION_FACTOR = 2
2. Run ```mysql -u root -p < init.sql``` from project directory or mysql -u root -p from project directory and then run ```source init.sql```
3. Run pip3 install -r requirements.txt in the project directory to install dependencies
4. To run the app execute the command ```flask --app fs_commands.py run```
5. Open your browser and type 127.0.0.1:5000/command-name?args -> Replace with actual command and arguments
