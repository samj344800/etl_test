Mise en forme du repo github

/bckp : contient un backup des fichier .py de maelle  
/notebook : contient les notebook, base du code  
/workspace : luncher execute.py, config.py   
/workspace/data : contiendra les downloads(source) et extracts(raw)  
/workspace/scripts : contient "tous"  les scripts exécutables  
/workspace/scripts/common : inutile

2022-07-13 version_beta

Release
* load.py
* execute.py

Unrelease
* Lots of warning - ETL's time life short!
* Test script for changed values
* Define var config.py/ ROOT_PATH portability
* Clean useless files and code

2022-07-09 version_alpha

Release
* base.py (see comments)
* table.py
* create_table.py
* extract.py (see comments)
* transform.py (see comments)

Unrelease
* load.py (in progress)
* execute.py
* making modular programming (bank/api/immo/_extract/tranform/load.py)

