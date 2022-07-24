# Script permettant de lancer dans l'ordre les différents scripts 

print("**** Running base.py ****")
from scripts import base  # __name__=__base.py__ 
print("Script base.py complete.")
print("********")
print("**** Running table.py ****")
from scripts import table  # __name__=__table.py__
print("Script table.py complete.")
print("********")
print("**** Running create_table.py ****")
exec(open("scripts/create_table.py").read())  # __name__=__main__
print("********")
print("**** Running extract.py ****")
exec(open("scripts/extract.py").read())  # __name__=__main__
print("********")
print("**** Running transform.py ****")
exec(open("scripts/transform.py").read()) # __name__=__main__
print("********")
print("**** Running load.py ****")
exec(open("scripts/load.py").read()) # __name__=__main__
print("*************")
print("END PROGRAMME")
