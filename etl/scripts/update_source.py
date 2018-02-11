# -*- coding: utf-8 -*-

import logging
import zipfile
import tempfile
import shutil
import requests
from datetime import datetime
from io import BytesIO
from pathlib import Path


source_url_tmpl = 'https://www.wri.org/sites/default/files/CAIT_Country_GHG_Emissions_-_csv_{}.zip'
# source_url = 'https://www.wri.org/sites/default/files/CAIT_Country_GHG_Emissions_-_csv_10022017.zip'
source_dir = '../source/'


def update_source():
    """The source file seems to release on Feb and Oct 2nd.
    But here we will check for every month.
    """
    # extract all files to source/
    # don't keep source file in github
    dt = datetime.now()
    year = dt.year
    day = dt.day
    if day > 2:
        month = dt.month
    else:
        month = dt.month - 1
    have_source = False

    while not have_source:
        dt_str = datetime(year, month, 2).strftime('%m%d%Y')
        if month > 1:
            month = month - 1
        else:
            month = 12
            year = year - 1
        link = source_url_tmpl.format(dt_str)
        res = requests.get(link)
        if res.status_code == 200:
            have_source = True
            tmp_dir = tempfile.mkdtemp()
            logging.info("get source for " + dt_str)
            zipf = zipfile.ZipFile(BytesIO(res.content))
            zipf.extractall(tmp_dir)
            for f in Path(tmp_dir).glob('**/*.csv'):
                shutil.copy(f, source_dir)


if __name__ == '__main__':
    update_source()
