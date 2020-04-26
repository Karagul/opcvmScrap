from mongoengine import *
from pathlib import Path
import pandas
import json


# connect to local MongoDB database
connect("opcvm")

class Funds(Document):
    FREQUENCY = StringField(required=True)
    ISIN = StringField(unique=True, required=True)
    AMC = IntField(required=False)
    MCCODE = IntField(required=False)
    NAME = StringField(required=True)
    MANAGER = StringField(required=True)
    LEGAL_TYPE = StringField(required=True)
    INV_TYPE = StringField(required=True)
    SUBSCRIBERS = StringField(required=True)
    RESULT_TYPE = StringField(required=True)
    DEPOSITORS = StringField(required=False)
    PLACEMENT_NETWORK = StringField(required=False)
    PROMOTERS = StringField(required=False)
    ENTRY_RATE = FloatField(required=True)
    EXIT_RATE = FloatField(required=True)
    MGT_RATE = FloatField(required=True)

    meta = {'strict': False}

    def clean(self):
        if self.ISIN is None or self.ISIN == 'nan':
            raise ValidationError('Wrong ISIN or nan')

# first we process the input of daily performances

daily_folder = Path('data_files', 'daily/')
weekly_folder = Path('data_files', 'weekly/')

def daily():
    files = daily_folder.glob('**/*')
    for f in files:
        try:
            excel_file = pandas.read_excel(f, sheet_name=0)
            json_str = excel_file.to_json(orient='records')
            entries = json.loads(json_str)
            for entry in entries:
                try:
                    print("processing ")
                    opcvm = Funds(FREQUENCY="daily", MCCODE=entry['Code Maroclear'], ISIN=entry['CODE ISIN'], NAME=entry['D\u00e9nomination OPCVM'], 
                    MANAGER=entry['Soci\u00e9t\u00e9 de Gestion'], LEGAL_TYPE=entry['Nature juridique'], INV_TYPE=entry['Classification'], 
                    SUBSCRIBERS=entry['Souscripteurs'], RESULT_TYPE=entry['Affectation des r\u00e9sultats'], DEPOSITORS=entry['D\u00e9positaire'], 
                    PLACEMENT_NETWORK=entry['R\u00e9seau placeur'], PROMOTERS=entry['Promoteurs'], ENTRY_RATE=entry['Commission de souscription'],
                    EXIT_RATE=entry[' Commission de rachat'], MGT_RATE=entry['Frais de gestion'])
                    opcvm.save()
                    print("saved " + entry['CODE ISIN'])
                    # START WITH RECENT PARAMS? RATHER THAN OLD ONES
                except KeyError as e:
                    print(e)
                    try:
                        opcvm = Funds(FREQUENCY="daily", ISIN=entry['CODE ISIN'], AMC=entry['AMC'], NAME=entry['OPCVM'], 
                        MANAGER=entry['GESTIONNAIRE'], LEGAL_TYPE=entry['FORME'], SUBSCRIBERS=entry['SOUSCRIPTEURS'],
                        INV_TYPE=entry['CATEGORIE'], RESULT_TYPE=entry['Affectation des r\u00e9sultats'], PLACEMENT_NETWORK=entry['RESEAU PLACEUR'],
                        ENTRY_RATE=entry['Droits  Entr\u00e9es'], EXIT_RATE=entry['Droits Sorties'], MGT_RATE=entry['Frais de gestion'])
                        opcvm.save()
                        print("saved " + entry['CODE ISIN'])
                    except Exception as f:
                        print(f)
                except ValidationError as e:
                    print(e)
                except NotUniqueError as j:
                    print("already exists")
        except:
            print("error reading the excel file")
    print("Finished processing all OPCVMs with daily frequency")
            
def weekly():
    files = weekly_folder.glob('**/*')
    for f in files:
        try:
            excel_file = pandas.read_excel(f, sheet_name=0)
            json_str = excel_file.to_json(orient='records')
            entries = json.loads(json_str)
            for entry in entries:
                try:
                    print("processing ")
                    opcvm = Funds(FREQUENCY="weekly", MCCODE=entry['Code Maroclear'], ISIN=entry['CODE ISIN'], NAME=entry['D\u00e9nomination OPCVM'], 
                    MANAGER=entry['Soci\u00e9t\u00e9 de Gestion'], LEGAL_TYPE=entry['Nature juridique'], INV_TYPE=entry['Classification'], 
                    SUBSCRIBERS=entry['Souscripteurs'], RESULT_TYPE=entry['Affectation des r\u00e9sultats'], DEPOSITORS=entry['D\u00e9positaire'], 
                    PLACEMENT_NETWORK=entry['R\u00e9seau placeur'], PROMOTERS=entry['Promoteurs'], ENTRY_RATE=entry['Commission de souscription'],
                    EXIT_RATE=entry[' Commission de rachat'], MGT_RATE=entry['Frais de gestion'])
                    opcvm.save()
                    print("saved " + entry['CODE ISIN'])
                except KeyError as e:
                    print(e)
                    try:
                        opcvm = Funds(FREQUENCY="weekly", ISIN=entry['CODE ISIN'], AMC=entry['AMC'], NAME=entry['OPCVM'], 
                        MANAGER=entry['GESTIONNAIRE'], LEGAL_TYPE=entry['FORME'], SUBSCRIBERS=entry['SOUSCRIPTEURS'],
                        INV_TYPE=entry['CATEGORIE'], RESULT_TYPE=entry['Affectation des r\u00e9sultats'], 
                        PLACEMENT_NETWORK=entry['RESEAU PLACEUR'], ENTRY_RATE=entry['Droits  Entr\u00e9es'], EXIT_RATE=entry['Droits Sorties'], 
                        MGT_RATE=entry['Frais de gestion'])
                        opcvm.save()
                        print("saved " + entry['CODE ISIN'])
                    except Exception as f:
                        print(f)
                except ValidationError as e:
                    print(e)
                except NotUniqueError as j:
                    print("already exists")
        except:
            print("error reading the excel file")
    print("Finished processing all OPCVMs with weekly frequency") 
            
def run():
    print("///////////////////////////////////// D PROCESS STARTS //////////////////////////////////")
    daily()
    print("///////////////////////////////////// W PROCESS STARTS //////////////////////////////////")
    weekly()
    print("Finished processing all OPCVMs in Database!") 


run()