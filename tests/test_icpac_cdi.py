from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.scraper.icpac_cdi.icpac_cdi import ICPAC_CDI
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="module")
def expected_dataset():
    return {
        "name": "igad-region-dekadal-combined-drought-indicator-cdi-2024",
        "title": "IGAD Region - Dekadal Combined Drought Indicator (CDI) 2024",
        "dataset_date": "[2024-01-01T00:00:00 TO 2024-02-10T23:59:59]",
        "notes": "Dekadal (10 days) Combined Drought Indicator (CDI) as implemented in the East "
        "Africa Drought Watch, and which is used for detecting and monitoring areas "
        "that either are affected or have the potential to be affected by "
        "meteorological, agricultural and/or hydrological drought.",
        "tags": [
            {
                "name": "climate hazards",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "climate-weather",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "drought",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "hazards and risk",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
        "data_update_frequency": "7",
        "subnational": "1",
        "groups": [
            {"name": "bdi"},
            {"name": "dji"},
            {"name": "eri"},
            {"name": "eth"},
            {"name": "ken"},
            {"name": "rwa"},
            {"name": "som"},
            {"name": "ssd"},
            {"name": "sdn"},
            {"name": "uga"},
            {"name": "tza"},
        ],
        "license_id": "cc-by",
        "methodology": "Other",
        "methodology_other": "Please refer to [EADW-CDI-Factsheet](https://droughtwatch.icpac.net/documents/3/EADW-CDI-Factsheet.pdf)",
        "dataset_source": "ICPAC",
        "package_creator": "HDX Data Systems Team",
        "private": False,
        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
        "owner_org": "04436cdf-24da-4826-b5b8-67cba9962423",
    }


@pytest.fixture(scope="module")
def expected_resources():
    return [
        {
            "name": "eadw-cdi-data-2024-01-21.tif",
            "format": "geotiff",
            "description": "Dekadal (10 days) Combined Drought Indicator (CDI) for 2024-01-21",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "name": "eadw-cdi-data-2024-02-01.tif",
            "format": "geotiff",
            "description": "Dekadal (10 days) Combined Drought Indicator (CDI) for 2024-02-01",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
    ]


class TestICPAC_CDI:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch):
        def read_from_hdx(dataset_name):
            if dataset_name == "igad-region-monthly-combined-drought-indicator-cdi-2024":
                return None
            else:
                return Dataset.load_from_json(
                    join(
                        "tests",
                        "fixtures",
                        "input",
                        f"dataset-{dataset_name}.json",
                    )
                )

        monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "icpac_cdi", "config")

    def test_icpac_cdi(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
        expected_dataset,
        expected_resources,
        read_dataset,
    ):
        with temp_dir(
            "Testicpac_cdi",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                icpac_cdi = ICPAC_CDI(
                    configuration,
                    retriever,
                    tempdir,
                    2024,
                )

                icpac_cdi.get_hdx_data()
                assert icpac_cdi.hdx_data == {
                    "igad-region-dekadal-combined-drought-indicator-cdi-2024": [
                        "eadw-cdi-data-2024-01-01.tif",
                        "eadw-cdi-data-2024-01-11.tif",
                    ]
                }

                dataset_names = icpac_cdi.get_data()
                assert dataset_names == [
                    "igad-region-dekadal-combined-drought-indicator-cdi-2024",
                    "igad-region-monthly-combined-drought-indicator-cdi-2024",
                ]

                dataset = icpac_cdi.generate_dataset(
                    "igad-region-dekadal-combined-drought-indicator-cdi-2024"
                )
                dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))
                assert dataset == expected_dataset
                assert dataset.get_resources() == expected_resources
