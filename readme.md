# Introduction

I created this mini-project to extract and analyze data from the WHO situational reports for Coronavirus Disease 2019 (COVID-19). The WHO reports are published as PDFs, and so I wrote a parser to extract data from the PDF text and geocode the locations. It also ingests the data into Elasticsearch so that you can analyze the data in Kibana.

## Prerequisites

* Python 3.7
* Pipenv `brew install pipenv`
* Docker
* Docker-Compose

## Quick Start

Start up the docker containers.

```
docker-compose up -d
```

Set up the Python environment.

```
pipenv shell
pipenv install
```

Run the parse script. The current set of situational reports and parsed TSV files are saved locally to prevent bogging down the WHO and Google resources. If there are new situational reports, this will reach out to WHO for the new reports and Google Geocoder API for new geolocations.

```
python parse.py
```

Open Kibana at http://localhost:5601. Click on Settings > Saved Objects > Import. Load in the `kibana-objects.ndjson` for example maps of confirmed cases around the world.
