from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from dotenv import load_dotenv
from random import randint
import pandas as pd 
import re, os, time, sys, csv, json

from toolkit import ordergenerator as og
from toolkit import general_tools as gt
from toolkit.g2a import G2A

load_dotenv()


OUTPUT_FOLDER_PATH = os.environ.get('OUTPUT_FOLDER_PATH')
STATION_FOLDER_PATH = os.environ.get('STATION_FOLDER_PATH')
DESTINATION_PATH = os.environ.get('DESTINATION_PATH')
BUG_TRACK_PATH = os.environ.get('BUG_TRACK_PATH')
LOGS_FOLDER_PATH = os.environ.get('LOGS_FOLDER_PATH')


FILED_NAMES = [
    'web-scrapper-order',
    'date_price',
    'date_debut',
    'date_fin','prix_init',
    'prix_actuel',
    'typologie',
    'n_offre',
    'nom',
    'localite',
    'date_debut-jour',
    'Nb semaines',
    'cle_station',
    'nom_station',
    'url'
] 

class MaevaDestinationScraper(object):

    def __init__(self, dest_name:str, name:str, start_date:str, end_date:str) -> None:
        self.dest_name = dest_name
        self.name = name
        self.start_date = start_date
        self.end_date = end_date

        self.destinations = []
        self.history = {}
        self.week_scrap = datetime.strptime(self.start_date, "%d/%m/%Y").strftime("%d_%m_%Y")
        self.exception_count = 0
        self.code = og.create_code()
        self.order_index = 1
        self.stations = {}

        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--ignore-certificate-errors')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        # self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--incognito')
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.maximize_window()

    def load_dest(self) -> None:
        print("  ==> loading all destinations")
        self.destinations = gt.load_json(f"{DESTINATION_PATH}/{self.dest_name}")
        # self.destinations = gt.load_json(f"{DESTINATION_PATH}/{self.week_scrap}/{self.dest_name}")

    def load_station(self) -> None:
        print("  ==> Initialisation liste stations ...")
        g2a_instance = G2A(entity="regions")
        page = 1

        while True:
            g2a_instance.set_page(page)
            results = g2a_instance.execute().json()

            if len(results) == 0:
                break

            for x in results:
                if x['website'] in ['/api/websites/1', '/api/websites/14']:
                    if x['name'] != '' and x['name'] not in self.stations.keys():
                        self.stations[x['name']] = x['region_key']

            page += 1

    def create_log(self) -> None:
        log = { "last_index": 0, "week_scrap": self.week_scrap, "last_date": self.week_scrap }
        if not Path(f"{LOGS_FOLDER_PATH}/{self.week_scrap}").exists():
            os.makedirs(f"{LOGS_FOLDER_PATH}/{self.week_scrap}")
        gt.create_log_file(log_file_path=f"{LOGS_FOLDER_PATH}/{self.week_scrap}/{self.name}.json", log_value=log)

    def load_history(self) -> None:
        print('  ==> load history')
        self.history = gt.load_json(f"{LOGS_FOLDER_PATH}/{self.week_scrap}/{self.name}.json")

    def set_history(self) -> None:
        print('  ==> set history')
        current_dest = self.history['last_index']
        self.history['last_index'] = current_dest + 1
        gt.save_history(log_file_path=f"{LOGS_FOLDER_PATH}/{self.week_scrap}/{self.name}.json",
                        log_value=self.history) 

    def use_new_driver(self) -> None:
        self.driver.quit()
        time.sleep(1)
        self.driver = webdriver.Chrome(self.chrome_options)

    def goto_page(self, url:str) -> None:
        print(f"  ==> load page {url}")
        if self.exception_count == 15:
            gt.show_message("Timeout Exception Error", "max exception reached, please check it before continue", "warning")
        try:
            self.driver.get(url)
            time.sleep(1)
            # WebDriverWait(self.driver, 20).until(EC.visibility_of_all_elements_located((By.XPATH, "//div[@data-info='prix__final']")))
            while '€' not in self.driver.find_element(By.XPATH, "//div[@data-info='prix__final']").text:
                print('  ==> waiting for data to be load')
                time.sleep(1)
            self.exception_count = 0
        except Exception as e:
            # gt.report_bug(f"{BUG_TRACK_PATH}/bug_{self.week_scrap}.txt", {"error": e, "bug_url":self.driver.current_url})
            time.sleep(2)
            self.driver.execute_script("window.location.reload();")
            self.exception_count += 1

    def create_output_file(self) -> None:
        print('  ==> creating log files')
        global FILED_NAMES
        if not Path(f"{OUTPUT_FOLDER_PATH}/{self.week_scrap}").exists():
            os.makedirs(f"{OUTPUT_FOLDER_PATH}/{self.week_scrap}")
        gt.create_file(f"{OUTPUT_FOLDER_PATH}/{self.week_scrap}/{self.name}.csv", FILED_NAMES)

    def get_date_params(link:str, key:str) -> object:
        url_params = parse_qs(urlparse(link).query)
        return datetime.strptime(url_params[key][0], "%Y-%m-%d").strftime("%d/%m/%Y")

    def extract(self, page_source:str, page_url:str) -> list:
        print('  ==> extract data')
        def link_params(url:str):
            url_params = parse_qs(urlparse(url).query)
            sep = '/'
            try:
                n_offre = sep.join(url_params['id'][0].split('-')[::-1])
                start_date = sep.join(
                    url_params['date_debut'][0].split('-')[::-1])
                end_date = sep.join(url_params['date_fin'][0].split('-')[::-1])
                return n_offre, start_date, end_date
            except KeyError as e:
                print(e)
                return

        data = []

        soupe = BeautifulSoup(page_source, 'lxml')

        residence = soupe.find('h1', {"id": "fiche-produit-residence-libelle"}).text.strip() \
            if soupe.find('h1', {"id": "fiche-produit-residence-libelle"}) else ''
        localisation = soupe.find('div', {"id": "fiche-produit-localisation"}).find('span', class_='maeva-black').text.strip() \
            if soupe.find('div', {"id": "fiche-produit-localisation"}) else ''

        date_price = self.history['week_scrap']
        
        station_name = localisation

        dat = {}
        typologie = soupe.find('h2', {'id':'fiche-produit-produit-libelle'}).text.strip() if soupe.find('h2', {'id':'fiche-produit-produit-libelle'}) else ''
        prix_container = soupe.find('div', {'data-info':'prix__container'})
        prix_actuel = prix_container.find('div', {'data-info':'prix__final'}).text.strip()[:-1].replace(',', '.')
        prix_init = prix_container.find('div', {'data-info':'prix__promo'}).text.strip()[:-1].replace(',', '.')
        n_offres, date_debut, date_fin = link_params(page_url)
        date_source = soupe.find('div', {'data-info':'basket__calendar'}).text.split(' ')
        date_source = [x for x in date_source if re.search(r'\d', x)]
        dat['web-scrapper-order'] = ''
        dat['date_price'] = date_price
        dat['date_debut'] = f"{date_source[0]}/{date_debut.split('/')[-1]}"
        dat['date_fin'] = f"{date_source[1]}/{date_fin.split('/')[-1]}"
        dat['prix_init'] = prix_init if prix_init != '' else prix_actuel
        dat['prix_actuel'] = prix_actuel
        dat['typologie'] = typologie
        dat['n_offre'] = n_offres
        dat['nom'] = residence
        dat['localite'] = localisation
        dat['date_debut-jour'] = ''
        dat['Nb semaines'] = datetime.strptime(
            date_debut, '%d/%m/%Y').isocalendar()[1]
        dat['cle_station'] = ''
        dat['nom_station'] = station_name
        dat['url'] = page_url
        data.append(dat)
        print(data)
        return data

    def setup_scrap(self) -> None:
        self.load_dest()
        self.create_log()
        self.create_output_file()
        self.load_history()
        self.load_station()
        self.use_new_driver()

    def save_data_source(self, data_source:list):
        global FILED_NAMES
        gt.save_data(
            file_path=f"{OUTPUT_FOLDER_PATH}/{self.week_scrap}/{self.name}.csv",
            data=data_source,field_names=FILED_NAMES)
        
    def execute(self) -> None:
        for k in range(self.history['last_index'], len(self.destinations)):
            print(f"  ==> {k} / {len(self.destinations)} destinations")
            url = self.destinations[k]
            self.goto_page(url)
            data = self.extract(self.driver.page_source, self.driver.current_url)
            if data:
                self.save_data_source(data)
            self.set_history()

if __name__=='__main__':
    m = MaevaDestinationScraper(
        dest_name="new_dest_cleaned.json",
        name='new_other_1',
        start_date='27/01/2024',
        end_date='25/05/2024'
    )
    m.setup_scrap()
    m.execute()

