from config import ROOT_PATH

print("Test", ROOT_PATH)

print("base.py ***********************************************************")
from scripts import base
print("Fin base.py !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
print("*************")
print("table.py **********************************************************")
from scripts import table
print("Fin table.py !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
print("*************")
print("create_table.py ***************************************************")
exec(open("scripts/create_table.py").read())
print("*************")
print("extract ***********************************************************")
exec(open("scripts/extract.py").read())
print("*************")
print("transform.py ******************************************************")
exec(open("scripts/transform.py").read())
print("*************")
print("load.py ***********************************************************")
exec(open("scripts/load.py").read())
print("*************")
print("END PROGRAMME")
