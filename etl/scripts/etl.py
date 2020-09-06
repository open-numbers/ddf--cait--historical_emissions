# -*- coding: utf-8 -*-
"""etl script for cait dataset. See the notebook in ../notebooks for more details"""

import os.path as osp
import numpy as np
import pandas as pd
from ddf_utils.str import to_concept_id
from ddf_utils.str import format_float_digits
from ddf_utils.package import get_datapackage
from ddf_utils.io import dump_json
import logging

logging.basicConfig(level=logging.INFO)

# global vars
source_dir = '../source'
out_dir = '../../'


def main():
    logging.info('loading source files...')
    co2 = pd.read_csv(
        osp.join(source_dir, 'CAIT Country CO2 Emissions.csv'),
        skiprows=1).set_index(['Country', 'Year'])
    ghg = pd.read_csv(
        osp.join(source_dir, 'CAIT Country GHG Emissions.csv'),
        skiprows=2).set_index(['Country', 'Year'])
    soc = pd.read_csv(
        osp.join(source_dir, 'CAIT Country Socio-Economic Data.csv'),
        skiprows=1).set_index(['Country', 'Year'])

    # concepts
    logging.info('building concepts files...')

    all_concepts = np.r_[co2.columns,  ghg.columns, soc.columns,
                         ['Country', 'Year']]
    cdf = pd.DataFrame(list(set(all_concepts)), columns=['name'])

    cdf['concept'] = cdf['name'].map(to_concept_id)
    cdf['concept_type'] = 'measure'
    cdf = cdf.set_index('concept')
    cdf.loc['country', 'concept_type'] = 'entity_domain'
    cdf.loc['year', 'concept_type'] = 'time'
    cdf.loc['name'] = ['Name', 'string']

    cdf = cdf.sort_index()

    cdf.to_csv(osp.join(out_dir, 'ddf--concepts.csv'))

    # country entities
    logging.info('building entity files...')
    co2 = co2.reset_index()
    co2.columns = co2.columns.map(to_concept_id)

    ghg = ghg.reset_index()
    ghg.columns = ghg.columns.map(to_concept_id)

    soc = soc.reset_index()
    soc.columns = soc.columns.map(to_concept_id)

    all_countries = np.r_[co2.country, ghg.country, soc.country]
    country = pd.DataFrame(all_countries, columns=['name'])
    country = country.drop_duplicates()
    country['country'] = country.name.map(to_concept_id)

    country.to_csv(
        osp.join(out_dir, 'ddf--entities--country.csv'), index=False)

    # datapoints
    logging.info('building datapoints files...')
    co2['country'] = co2['country'].map(to_concept_id)
    ghg['country'] = ghg['country'].map(to_concept_id)
    soc['country'] = soc['country'].map(to_concept_id)

    co2 = co2.set_index(['country', 'year'])
    ghg = ghg.set_index(['country', 'year'])
    soc = soc.set_index(['country', 'year'])

    for c in co2:
        d = co2[c].copy()
        d = d.map(lambda x: format_float_digits(x, 5))
        (d.dropna().reset_index().to_csv(
            osp.join(out_dir,
                     'ddf--datapoints--{}--by--country--year.csv'.format(c)),
            index=False))

    for c in ghg:
        d = ghg[c].copy()
        d = d.map(lambda x: format_float_digits(x, 5))
        (d.dropna().reset_index().to_csv(
            osp.join(out_dir,
                     'ddf--datapoints--{}--by--country--year.csv'.format(c)),
            index=False))

    for c in soc:
        d = soc[c].copy()
        d = d.map(lambda x: format_float_digits(x, 5))
        (d.dropna().reset_index().to_csv(
            osp.join(out_dir,
                     'ddf--datapoints--{}--by--country--year.csv'.format(c)),
            index=False))

    # datapackage
    logging.info('building datapackage.json...')
    dump_json(osp.join(out_dir, 'datapackage.json'), get_datapackage(out_dir))


if __name__ == '__main__':
    logging.info('updating source file...')
    main()
