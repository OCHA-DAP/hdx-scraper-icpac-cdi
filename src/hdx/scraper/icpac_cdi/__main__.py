#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from datetime import datetime
from os.path import dirname, expanduser, join

from dateutil.relativedelta import relativedelta
from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    temp_dir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.icpac_cdi._version import __version__
from hdx.scraper.icpac_cdi.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-icpac-cdi"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: icpac_cdi"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access(
        "igad-climate-prediction-and-application-center"
    )

    with temp_dir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            previous_year = (datetime.today() - relativedelta(months=6)).year
            years = list({datetime.today().year, previous_year})
            pipeline = Pipeline(
                configuration,
                retriever,
                tempdir,
                years=years,
            )

            pipeline.get_hdx_data()
            dataset_names = pipeline.get_data()

            for dataset_name in dataset_names:
                dataset = pipeline.generate_dataset(dataset_name)

                dataset.update_from_yaml(
                    path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
                )
                dataset.create_in_hdx(
                    remove_additional_resources=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
