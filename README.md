# Prodesa BIaaS ETL
This repository aims to guide technical staff during the setting up of the prodesa's ETL configuration. ETL is implemented as a series of python scripts, which takes the information stored into an .xlsx file and transform accordingly, to finally persist as a BigQuery table set.
## Creating Virtual Environment
1. Install Python 3
```
 
```
2. Install and upgrade pip3
```
pip3 install --upgrade pip
```
3. First, we have to install 'python3-venv' using the following command
```
sudo apt install python3-venv
```
4. Create the virtual environment `venv`
```
python3 -m venv venv
```
5. To activate Virtual Environment run the command bellow
```
source venv/bin/activate
```
6. In order to add new packages to our new virtual environment we create a file called 'requirements.txt' and excecute the following command
```
pip3 install -r requirements.txt
```
## File Structure
```
prodesa-biaas-etl
+---README.md
+---main.py
+---requirements.txt
|
+---mirror_area
|   +---mirror_area.py
|
+---staging_area
|   +---staging_area.py
|
+---temporary_area
|   +---temporary_area.py
|   |
|   +---parametrization
|   |   +---tmp_ar_parametrization.py
|   |
|   +---building
|   |   +---tmp_ar_building.py
|   |
|   +---milestones
|   |   +---tmp_ar_milestones.py
|   |   |
|   |   +---phases
|   |       +---tmp_ar_mlstns_inicio_construccion.py
|   |       +---tmp_ar_mlstns_inicio_escrituracion.py
|   |       +---tmp_ar_mlstns_inicio_promesa.py
|   |       +---tmp_ar_mlstns_inicio_venta.py
|   |
|   +---commercial
|   |   +---tmp_ar_commercial.py
|   |
|   +---planning
|       +---tmp_ar_planning.py
|   
+---model_area
    +---model_area.py
    |
    +---parametrization
    |   +---mdl_ar_parametrization.py
    |
    +---building
    |   +---mdl_ar_building.py
    |
    +---milestones
    |   +---mdl_ar_milestones.py
    |   |
    |   +---milestones
    |       +---mdl_ar_mlstns_inicio_construccion.py
    |       +---mdl_ar_mlstns_inicio_escrituracion.py
    |       +---mdl_ar_mlstns_inicio_promesa.py
    |       +---mdl_ar_mlstns_inicio_venta.py
    |
    +---commercial
    |   +---mdl_ar_commercial.py
    |
    +---planning
        +---mdl_ar_planning.py
```