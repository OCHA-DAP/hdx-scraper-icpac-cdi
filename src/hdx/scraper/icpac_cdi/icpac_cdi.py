#!/usr/bin/python
"""icpac_cdi scraper"""

import logging
from datetime import datetime, timedelta
from typing import List

from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class ICPAC_CDI:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
        year: int,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._time_periods = configuration["time_periods"]
        self._year = year
        self.hdx_data = {}
        self.data = {}
        self.dates = {}

    def get_hdx_data(self) -> None:
        for time_period in self._time_periods:
            dataset_name = f"igad-region-{time_period}-combined-drought-indicator-cdi-{self._year}"
            dataset = Dataset.read_from_hdx(dataset_name)
            if not dataset:
                continue
            resources = dataset.get_resources()
            self.hdx_data[dataset_name] = [resource["name"] for resource in resources]

    def get_data(self) -> List[str]:
        for time_period in self._time_periods:
            logger.info(f"Downloading {time_period} data for {self._year}")
            dataset_info = self._configuration[time_period]
            dataset_name = f"igad-region-{time_period}-combined-drought-indicator-cdi-{self._year}"
            base_url = f"{dataset_info['url']}{self._year}/"

            try:
                text = self._retriever.download_text(
                    base_url, filename=f"{time_period}_{self._year}"
                )
            except DownloadError:
                logger.error(f"Could not get data from {base_url}")
                continue

            soup = BeautifulSoup(text, "html.parser")
            lines = soup.find_all("a")
            for line in lines:
                filename = line.get("href")
                if filename[-3:] != "tif":
                    continue
                if filename in self.hdx_data.get(dataset_name, []):
                    continue
                file_url = f"{base_url}{filename}"
                try:
                    filepath = self._retriever.download_file(file_url, filename=filename)
                except DownloadError:
                    logger.error(f"Could not download file {filename}")
                    continue

                dict_of_lists_add(self.data, dataset_name, filepath)

                file_date = filename.split(".")[0].split("-")[4:]
                if time_period == "dekadal":
                    date_start = datetime(self._year, int(file_date[0]), int(file_date[1]))
                    date_end = date_start + timedelta(days=9)
                if time_period == "monthly":
                    date_start = datetime.strptime(f"{self._year} {file_date[0]} 1", "%Y %b %d")
                    date_end = date_start + relativedelta(day=31)
                dict_of_lists_add(self.dates, dataset_name, date_start)
                dict_of_lists_add(self.dates, dataset_name, date_end)

        return [dataset_name for dataset_name in sorted(self.data)]

    def generate_dataset(self, dataset_name: str) -> Dataset:
        if "dekadal" in dataset_name:
            time_period = "dekadal"
        if "monthly" in dataset_name:
            time_period = "monthly"

        dataset_info = self._configuration[time_period]
        dataset_title = f"{dataset_info['title']} {self._year}"

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        date_start = min(self.dates[dataset_name])
        date_end = max(self.dates[dataset_name])
        dataset.set_time_period(startdate=date_start, enddate=date_end)

        dataset["notes"] = dataset_info["notes"]
        dataset.add_tags(self._configuration["tags"])
        dataset.set_expected_update_frequency(dataset_info["update_frequency"])
        dataset.set_subnational(True)
        dataset.add_country_locations(self._configuration["countries"])

        resources = []
        resource_paths = self.data[dataset_name]
        for resource_path in resource_paths:
            resource = Resource(
                {
                    "name": resource_path.split("/")[-1],
                    "format": "GeoTIFF",
                }
            )
            resource.set_file_to_upload(resource_path)
            resources.append(resource)
        dataset.add_update_resources(resources)
        return dataset
