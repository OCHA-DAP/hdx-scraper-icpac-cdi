#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from datetime import datetime
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from icpac_cdi import ICPAC_CDI

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-icpac-cdi"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: icpac_cdi"


def main(
    save: bool = True,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            configuration = Configuration.read()

            icpac_cdi = ICPAC_CDI(
                configuration,
                retriever,
                temp_dir,
                datetime.today().year,
            )

            icpac_cdi.get_hdx_data()
            dataset_names = icpac_cdi.get_data()

            for dataset_name in dataset_names:
                dataset = icpac_cdi.generate_dataset(dataset_name)

                dataset.update_from_yaml(
                    path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
                )
                dataset.create_in_hdx(
                    remove_additional_resources=False,
                    match_resource_order=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                    ignore_check=["resource:description"],
                )


if __name__ == "__main__":
    facade(
        main,
        hdx_site="dev",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(dirname(__file__), "config", "project_configuration.yaml"),
    )
