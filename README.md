### Projet
Création d'un ETL en utilisant Sqlalchemy

Machine learning P7

### Organisation du repo github

/bckp : contient un backup de fichiers .py  
/notebook : contient les notebook, base du code  
/workspace : dossier racine du programme, contient le luncher execute.py et
 le fichier de configuration configure.py  
/workspace/data : contiendra les downloads(source) et extracts(raw)  
/workspace/scripts : contient "tous"  les scripts exécutables  
/P7 : contient les notebook machine leaning  

### Utilisation
* La base de données pointée est définie dans **config.py**, elle sera crée si elle est inexistante
* Définir les variables concernant la BD dans **config.py** 
* Utiliser **table.py** pour configurer les classes/tables si besoin
* Utiliser **extract.py** pour définir les cibles de données si besoin
* Lancer **execute.py** 

### 2022-07-25 ETL_bank_v1.1

**Release**
* Test des scripts en cas de suppression ou modification des données
* Gestion de l'éxistance ou de la création de la DB définie dans config.py
* Suppression des tables temporaires après proces

**Unrelease**
* Warning pandas et regex sur de futures dépréciations

### 2022-07-16 ETL_bank_v1.0

**Release**
* Implémentation de config.py
* Réintégration de la fonction "convertisseur"
* Réintégrer la creation des dossiers horodatés contenant les données
* Nettoyage du code et des fichiers

**Unrelease**
* Warning pandas et regex sur de futures dépréciations
* Tester le script en cas de changement ou suppression de données
* Gestion de l'existence de la DB ou sa crétion suivant config.py
* Supprimer les tables temporaires après process

### 2022-07-13 version_beta

**Release**
* load.py
* execute.py

**Unrelease**
* Lots of warning - ETL's time life short!
* Test script for changed values
* Define var config.py/ ROOT_PATH portability
* Clean useless files and code

### 2022-07-09 version_alpha

**Release**
* base.py (see comments)
* table.py
* create_table.py
* extract.py (see comments)
* transform.py (see comments)

**Unrelease**
* load.py (in progress)
* execute.py
* making modular programming (bank/api/immo/_extract/tranform/load.py)

