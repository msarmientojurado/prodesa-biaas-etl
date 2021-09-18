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
4. To activate Virtual Environment run the command bellow
```
source venv/bin/activate
```
5. In order to add new packages to our new virtual environment we create a file called 'requirements.txt' and excecute the following command
```
pip3 install -r requirements.txt
```