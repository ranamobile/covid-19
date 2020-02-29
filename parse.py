import csv
import datetime
import json
import logging
import os
import re

import PyPDF2
import requests
from elasticsearch import Elasticsearch
from geopy.geocoders import Nominatim

logger = logging.getLogger(__name__)

class CovidParser:
    INDEX_CHINA = "covid-19-china"
    INDEX_WORLD = "covid-19-world"
    HEADERS_CHINA = [
        "timestamp", "location", "geoname", "population",
        "daily_confirm", "daily_suspect", "daily_death", "total_confirm", "total_death",
    ]
    HEADERS_WORLD = [
        "timestamp", "location", "geoname",
        "total_confirm", "daily_confirm",
        "total_china_expose", "daily_china_expose", "total_world_expose", "daily_world_expose",
        "total_cntry_expose", "daily_cntry_expose", "total_unknw_expose", "daily_unknw_expose",
        "total_death", "daily_death",
    ]
    REGEX_CHINA = r'([A-Z][a-z ]+) (\d+) (\d+) (\d+) (\d+) (\d+) (\d+)'
    REGEX_WORLD = (r'([A-Z][a-z ]+) (\d+) \((\d+)\) (\d+) \((\d+)\) (\d+) \((\d+)\) '
                   r'(\d+) \((\d+)\) (\d+) \((\d+)\) (\d+) \((\d+)\)')

    def __init__(self, es_url="http://localhost:9200"):
        self.es = Elasticsearch(es_url)
        self.geo = Nominatim(user_agent="my-application")
        self.geolocations = {}

    def _initialize_index(self, index, clear=False):
        if clear and self.es.indices.exists(index=index):
            self.es.indices.delete(index=index)
        with open(f'{index}-index.json', "r") as handler:
            body = json.load(handler)
            self.es.indices.create(index=index, body=body)

    def initialize_elasticsearch(self, clear=False):
        self._initialize_index(self.INDEX_CHINA, clear=clear)
        self._initialize_index(self.INDEX_WORLD, clear=clear)

    def get_pdf_content(self, filepath):
        with open(filepath, "rb") as handler:
            reader = PyPDF2.PdfFileReader(handler)
            page_content = []
            number_pages = reader.getNumPages()
            for page in range(number_pages):
                page = reader.getPage(page)
                text = page.extractText()

                stripped_text = re.sub(r'\s+', " ", text)
                page_content.append(stripped_text)

        content = " ".join(page_content)
        return content

    def _parse_covid(self, content, date, index, headers, regex):
        filepath = os.path.join("output", f'{date}-{index}.tsv')
        if os.path.isfile(filepath):
            with open(filepath, "r") as handler:
                reader = csv.DictReader(handler, delimiter="\t")
                for stat in reader:
                    stat = dict(stat)
                    stat["timestamp"] = datetime.datetime.fromisoformat(stat["timestamp"])
                    stat["location"] = stat["location"] or None
                    self.es.index(index=index, body=stat)
                    self.geolocations[stat["geoname"]] = stat["location"]
        else:
            with open(filepath, "w") as handler:
                writer = csv.DictWriter(handler, fieldnames=headers, delimiter="\t")
                writer.writeheader()
                for match in re.findall(regex, content):
                    if match[0] in self.geolocations:
                        geopoint = self.geolocations[match[0]]
                    else:
                        loc = self.geo.geocode(match[0])
                        geopoint = f'{loc.latitude},{loc.longitude}' if loc else None
                    stat = dict(zip(headers, [date, geopoint] + list(match)))
                    self.es.index(index=index, body=stat)
                    writer.writerow(stat)

    def parse_covid(self, content, date):
        self._parse_covid(content, date, self.INDEX_CHINA, self.HEADERS_CHINA, self.REGEX_CHINA)
        self._parse_covid(content, date, self.INDEX_WORLD, self.HEADERS_WORLD, self.REGEX_WORLD)


WHO_URL = "https://www.who.int"
WHO_COVID_URL = f'{WHO_URL}/emergencies/diseases/novel-coronavirus-2019/situation-reports'

def get_reports():
    datestamp = datetime.datetime.now()
    filepath = os.path.join("data", f'{datestamp.strftime("%y-%m-%d")}-sitrep.html')
    if os.path.isfile(filepath):
        with open(filepath, "r") as handler:
            content = handler.read()
    else:

        response = requests.get(WHO_COVID_URL)
        if response.status_code == 200:
            content = response.text
            with open(filepath, "w") as handler:
                handler.write(content)

    for pdf_url, pdf_name in re.findall(r'href=\"(\S+\/(\S+pdf)\S+)\"', content):
        pdf_filepath = os.path.join("data", pdf_name)
        if not os.path.isfile(pdf_filepath):
            response = requests.get(f'{WHO_URL}{pdf_url}')
            if response.status_code == 200:
                with open(pdf_filepath, "wb") as handler:
                    handler.write(response.content)

        yield pdf_name, pdf_filepath


if __name__ == "__main__":
    parser = CovidParser()
    parser.initialize_elasticsearch(clear=True)

    for pdf_name, pdf_filepath in get_reports():
        logger.info(f'parsing pdf: {pdf_name}')
        pdf_date = pdf_name.split("-")[0]
        date = datetime.datetime.fromisoformat(f'{pdf_date[:4]}-{pdf_date[4:6]}-{pdf_date[6:]}')

        content = parser.get_pdf_content(pdf_filepath)
        parser.parse_covid(content, date)
