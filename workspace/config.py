import os

# Script to define variables environnement

# User
USER_NAME='root'
PASS_WORD='Root01Oracle!'

# Server
SGBD_NAME='mysql'
SGBD_DRIVER='pymysql'
SGBD_PORT=3306
BD_NAME='etl_db'

# Environement
ROOT_PATH=os.getcwd()

# test scipt
print("config.py.ROOT_PATH",ROOT_PATH)
