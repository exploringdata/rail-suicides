import pandas as pd
from geonamescache import GeonamesCache

gc = GeonamesCache()
countries = gc.get_countries()

df_suicides = pd.io.parsers.read_csv('data/rail-suicides-2008-2011.csv')
df_pop = pd.io.parsers.read_csv('data/SP.POP.TOTL_Indicator_MetaData_en_EXCEL.csv')

relevant_cols = ['Country Code', '2008', '2009', '2010', '2011']
df_pop = df_pop[relevant_cols]

iso2 = df_suicides['Years']
iso_map = {}

for iso in iso2:
    country = countries.get(iso, None)
    if country:
        iso_map[country['iso3']] = iso

iso3 = list(iso_map.keys())

# filter to keep only countries with suicide data
df_pop = df_pop[df_pop['Country Code'].isin(iso3)]

# convert iso3 to iso2
df_pop['Country Code'] = df_pop['Country Code'].map(lambda x: iso_map[x])

# merge into one for doing calculations
df_merged = df_pop.merge(df_suicides, left_on='Country Code', right_on='Years')
for y in range(2008, 2012):
    y = str(y)
    # guardian data contains null as a value
    suicides = df_merged[y + '_y'].convert_objects(convert_numeric=True)
    pop = df_merged[y + '_x']  # astype('float64')
    new_col = y + '_ratio'
    df_merged[new_col] = suicides * 100000 / pop  # per 100k peope
    df_merged[new_col] = df_merged[new_col].map(lambda x: round(x, 2))

# reduce to ratios
df_ratios = df_merged[['Years', '2008_ratio', '2009_ratio', '2010_ratio', '2011_ratio']]
# change column names for use in Datawrapper
df_ratios.columns = df_ratios.columns.map(lambda x: x.replace('_ratio', ''))

df_ratios = df_ratios.sort('2008', ascending=False)
df_ratios.to_csv('ratios.csv', index=False)
