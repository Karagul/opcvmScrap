import os
from bs4 import BeautifulSoup
import requests
import json
import re
import urllib.request
import urllib.error


daily_folder = "c:/Users/Imad/Desktop/coding experiments/opcvmScrap/data_files/daily/"
weekly_folder = "c:/Users/Imad/Desktop/coding experiments/opcvmScrap/data_files/weekly/"

url = "http://www.asfim.ma/?lang=fr&Id=27"

def scrape(url):
    asfim_reports = list()
    headers = requests.utils.default_headers()
    headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
    })
    r = requests.get(url)
    raw_html = r.content
    soup = BeautifulSoup(raw_html, 'html.parser')
    all_reports = soup.find_all("div", {"class": "BActus Bgdoc"})
    for report in all_reports:
        titles = report.find_all("h4")
        urls = report.find_all("a")
        for title in titles:
            for url in urls:
                link = url.get('href').partition("('")[2].partition("')")[0]
                link = "http://www.asfim.ma/" + link
                if "hebdomadaires" in title.text:
                    asfim_report = {
                        'report_name': title.text,
                        'report_link': link,
                        'type': 'weekly'
                    }
                    asfim_reports.append(asfim_report)
                    print("new file added")   
                else:
                    asfim_report = {
                        'report_name': title.text,
                        'report_link': link,
                        'type': 'daily'
                    }
                    asfim_reports.append(asfim_report)
                    print("new file added")
    with open('data.json', 'w') as outfile:
        json.dump(asfim_reports, outfile)
        print("done")
    import_report()
        


def import_report():
    with open('data.json') as json_file:
        reports = json.load(json_file)
        for report in reports:
            if report['type'] == 'weekly':
                file_name = report['report_name'].partition("hebdomadaires au ")[2]
                if os.path.isfile("data_files/weekly/" + file_name + ".xlsx") == False:
                    try:
                        urllib.request.urlretrieve(report['report_link'], weekly_folder + file_name + ".xlsx")
                        print(file_name)
                    except urllib.error.HTTPError as e:
                        print(e)
                else:
                    print('already exists')
            else:
                file_name = report['report_name'].partition("quotidiennes au ")[2]
                if os.path.isfile("data_files/daily/" + file_name + ".xlsx") == False:
                    try:
                        urllib.request.urlretrieve(report['report_link'], daily_folder + file_name + ".xlsx")
                        print(file_name)
                    except urllib.error.HTTPError as e:
                        print(e)
                else:
                    print('already exists')
                
    print("done!")


scrape(url)
