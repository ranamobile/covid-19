import csv
import datetime
import json
import logging
import os
import re

import PyPDF2
import requests
from elasticsearch import Elasticsearch
from elasticsearch.client.security import SecurityClient
from elasticsearch.helpers import bulk
from geopy.geocoders import Nominatim

from config import parsers

logger = logging.getLogger(__name__)


class CovidParser:
    COVID_INDEX = "covid-19"

    def __init__(self, es_url="http://localhost:9200"):
        self.es = Elasticsearch(es_url)
        self.geo = Nominatim(user_agent="my-application-1")
        self.geolocations = {}

    def _initialize_index(self, index, clear=False):
        if clear and self.es.indices.exists(index=index):
            self.es.indices.delete(index=index)
        with open(f'{self.COVID_INDEX}-index.json', "r") as handler:
            body = json.load(handler)
            self.es.indices.create(index=index, body=body)

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

    def _parse_covid(self, content, date, filename, headers, regex, force=False):
        filepath = os.path.join("output", f'{filename}.tsv')
        documents = []

        if os.path.isfile(filepath) and not force:
            with open(filepath, "r") as handler:
                reader = csv.DictReader(handler, delimiter="\t")
                for count, stat in enumerate(reader):
                    stat = dict(stat)
                    stat["timestamp"] = datetime.datetime.fromisoformat(stat["timestamp"])
                    stat["location"] = stat["location"] or None
                    self.geolocations[stat["geoname"]] = stat["location"]

                    stat["_type"] = "_doc"
                    stat["_op_type"] = "index"
                    documents.append(stat)

        else:
            with open(filepath, "w") as handler:
                writer = csv.DictWriter(handler, fieldnames=headers, delimiter="\t")
                writer.writeheader()
                for count, match in enumerate(re.findall(regex, content)):
                    if match[0] in self.geolocations:
                        geopoint = self.geolocations[match[0]]
                    else:
                        loc = self.geo.geocode(match[0])
                        geopoint = f'{loc.latitude},{loc.longitude}' if loc else None
                        self.geolocations[match[0]] = geopoint
                    stat = dict(zip(headers, [date, geopoint] + list(match)))
                    writer.writerow(stat)

                    stat["_type"] = "_doc"
                    stat["_op_type"] = "index"
                    documents.append(stat)

        return documents

    def parse_covid(self, content, date):
        covid_index = f'{self.COVID_INDEX}-{date.strftime("%Y%m%d")}'
        self._initialize_index(covid_index, clear=True)

        documents = []
        for parser in parsers:
            headers = parser["headers"]
            regex = parser["regex"]
            filename = f'{covid_index}-{parser["label"]}'
            documents.extend(self._parse_covid(content, date, filename, headers, regex))

        logger.info(f'{covid_index}: found {len(documents)} documents for {date}')
        bulk(self.es, documents, index=covid_index)


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
    logging.basicConfig(level=logging.INFO)
    parser = CovidParser()

    for pdf_name, pdf_filepath in get_reports():
        logger.info(f'parsing pdf: {pdf_name}')
        pdf_date = pdf_name.split("-")[0]
        date = datetime.datetime.fromisoformat(f'{pdf_date[:4]}-{pdf_date[4:6]}-{pdf_date[6:]}')

        content = parser.get_pdf_content(pdf_filepath)
        parser.parse_covid(content, date)
