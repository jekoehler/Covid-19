"""Hopkins Class

This class provides a `pandas` DataFrame object of the daily cumulative case numbers
(starting Jan 22, 2020) reported by the Johns Hopkins University Center for Systems
Science and Engineering (CSSE).

The DataFrame is loaded from local subdirectory, or, if not already existing,
loaded from JHU repository and processed into desired DataFrame.

The processing steps are:
(1) load global data
(2) restructure data column-wise to row-wise
(3) set country codes
(4) prepare data (check provinces and NaN)
(5) merge confirmed cases, confirmed deaths and recovered (all with different length)
(6) expand dates backwards to 2020-01-01 (needed to have same starting date for other data)
(7) save DataFrame to local subdirectory as csv

This class can be updated with the latest data from Johns Hopkins University see README.md

NOTE:
	This class is primarily used from Mobility class to update Hopkins data.
"""
import pandas as pd
import numpy as np
import os

from data_utils import COL
from iso_data import ISOCodes

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
HOPKINS_CSV_PATH = os.path.join(FILE_PATH, "data/JEK_Hopkins_v21_latest.csv")

BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series"

GLOBAL_URLS = {COL.cc: f"{BASE_URL}/time_series_covid19_confirmed_global.csv",
               COL.cd: f"{BASE_URL}/time_series_covid19_deaths_global.csv",
               COL.rc: f"{BASE_URL}/time_series_covid19_recovered_global.csv"
               }

US_URLS = {COL.cc: f"{BASE_URL}/time_series_covid19_confirmed_US.csv",
           COL.cd: f"{BASE_URL}/time_series_covid19_deaths_US.csv",
           COL.rc: f"{BASE_URL}/time_series_covid19_recovered_US.csv"
           }


class Hopkins:
	"""
	Attributes
	----------
	data : DataFrame
		DataFrame object (pandas) that holds all feature columns.
	min_date: str
		Minimum or starting date of time series
	max_date: str
		Maximum or latest date of time series
	num_date: int
		Number of dates/days of complete time series
		
	Methods
	-------
	update()
		Update DataFrame with the latest data from Hopkins repository
	save()
		Save DataFrame to local subdirectory MOBILITY_CSV_PATH
	"""
	def __init__(self):
		try:
			print("Load Hopkins data...")
			self.data = pd.read_csv(HOPKINS_CSV_PATH)
			print("- local data loaded")

			self.data["DateTime"] = pd.to_datetime(self.data["Date"], format="%Y-%m-%d", errors="ignore")
			self.data = self.data[['Date', 'Country_Code', 'ConfirmedCases', 'ConfirmedDeaths', 'Recovered']]
			self.min_date = self.data["Date"].unique().min()
			self.max_date = self.data["Date"].unique().max()
			self.num_date = self.data["Date"].nunique()
			print("- dates from {} to {} total {} days".format(self.min_date, self.max_date, self.num_date))
			print("- shape:", self.data.shape)
		except FileNotFoundError as e:
			print("Error: no data present for file:", os.path.split(HOPKINS_CSV_PATH)[-1])
			print("Proceed loading data from Hopkins URL...")
			self.update()
	
	def update(self):
		"""Load Hopkins data from repository and prepare and save the data
		as pandas's DataFrame"""
		print("- update Hopkins data...")
		self.data = {case: _load_global_data(case, url) for case, url in GLOBAL_URLS.items()}
		self.data = _prepare_and_merge(self.data)
		
		self.data["DateTime"] = pd.to_datetime(self.data["Date"], format="%Y-%m-%d", errors="ignore")
		self.data['Date'] = self.data['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))
		self.data = _expand_dates(self.data)
		
		self.data = self.data[['Date', 'Country_Code', 'ConfirmedCases', 'ConfirmedDeaths', 'Recovered']]
		self.min_date = self.data["Date"].unique().min()
		self.max_date = self.data["Date"].unique().max()
		self.num_date = self.data["Date"].nunique()
		print("- dates from {} to {} total {} days".format(self.min_date, self.max_date, self.num_date))
		print("- merged data:", self.data.shape)
		self.save()
	
	def save(self):
		self.data.to_csv(HOPKINS_CSV_PATH, columns=self.data.columns, index=False)
		print("- saved at:", HOPKINS_CSV_PATH)


def _prepare_and_merge(data):
	"""Prepare data of three different sources (confirmed cases,
	confirmed deaths and recovered). These three sources has different
	length and some countries doesn't have accumulated values (they have
	values for provinces instead, so before merging all three sources we
	need to compute the cumulative values for a country).
	
	Parameter
	---------
	data: DataFrame
		Original, raw data from JHU repository (row-wise time-series)
		
	Returns
	-------
	data: DataFrame
		Merged data of column-wise time-series
	"""
	print("- prepare and merge data... ")
	# reorder columns
	for case, url in GLOBAL_URLS.items():
		new_cols = ['Date', 'Country_Code', 'Province_State', 'Country_Region', case]
		temp = data[case]
		temp = temp[new_cols]
		
		# get all countries having some provinces
		provs = _check_provinces(temp)
		# check countries without nan at Province_State
		countries_without_nan = []
		for p in provs:
			lookup = (temp.Country_Code == p) & (temp.Province_State.isna())
			if len(temp[lookup]['Province_State'].unique()) < 1:
				countries_without_nan.append(p)
		
		collected_dfs = [temp]
		for c in countries_without_nan:
			nan_df = _compute_nan(temp, c, case)
			collected_dfs.append(nan_df)
		
		temp = pd.concat(collected_dfs)
		temp = temp.sort_values(by=["Country_Code", "Province_State", "Date"])
		# get rid of Province_State
		temp = temp[temp.Province_State.isna()]
		temp = temp.drop(columns=["Province_State"])
		data[case] = temp
	
	# merge all three different data frames
	new_df = data[COL.cc]
	new_df = new_df.merge(data[COL.cd],
	                      left_on=['Date', 'Country_Code', 'Country_Region'],
	                      right_on=['Date', 'Country_Code', 'Country_Region'])
	new_df = new_df.merge(data[COL.rc], on=['Date', 'Country_Code', 'Country_Region'])
	
	return new_df

	
def _check_provinces(data):
	"""Helper function to check which country has values for provinces.
	
	Parameter
	---------
	data: DataFrame
		DataFrame of Hopkins class
		
	Returns
	-------
	country codes: list
		List of country codes of countries having some provinces
	"""
	def check_number_of_entries(data, code):
		parts = data[(data.Country_Code == code)]
		return parts.shape[0]
	
	country_codes = data['Country_Code'].unique()
	
	prov_countries = {}
	days = data["Date"].nunique()
	for code in country_codes:
		num = check_number_of_entries(data, code)
		if num > days:
			prov_countries[code] = num
	
	return prov_countries.keys()
	

def _compute_nan(data, c_code, case) -> pd.DataFrame:
	"""Helper function to compute the cumulative values from
	all provinces to be set as the countries values for the
	provinces NaN values.
	
	Parameters
	----------
	data: DataFrame
		DataFrame of Hopkins class
	c_code: str
		Country code of interest
	case: str
		String of case (ConfirmedCases, ConfirmedDeaths, Recovered)
		
	Returns
	-------
	new_df: DataFrame
		New DataFrame for each country and case with NaN set to Province_State
	"""
	lookup = (data.Country_Code == c_code)
	country = data[lookup]
	new_nan = country.groupby('Date')[case].sum()
	
	new_df = pd.DataFrame({'Date': new_nan.index, case: new_nan.values})
	new_df['Province_State'] = np.nan
	new_df['Country_Region'] = country['Country_Region'].unique()[0]
	new_df['Country_Code'] = c_code
	
	return new_df


def _load_global_data(cases, path_url) -> pd.DataFrame:
	"""Load global data from Hopkins database at github (see URLs).
	These database has dates column-wise. We need to restructure the
	table to have dates row-wise.
	
	Parameters
	----------
	cases: str
		String of case (ConfirmedCases, ConfirmedDeaths, Recovered)
	path_url: str
		URL of JHU repository for the corresponding time series
		
	Returns
	-------
	data: DataFrame
		Restructured and extended DataFrame
	"""
	print("- load data for", cases)
	data = pd.read_csv(path_url, ',')
	# reorganize dates
	data = _restructure(data=data, idx=4, cases=cases)
	# rename data
	data = data.rename(columns={'Country/Region': 'Country_Region',
	                            'Province/State': 'Province_State'})
	# delete needless columns without 'iso3'
	cols_to_delete = ['Lat', 'Long']
	data = data.drop(cols_to_delete, axis=1, inplace=False)
	# sort data
	data = data.sort_values(by=['Country_Region', 'Province_State', 'Date'],
	                        ignore_index=True)
	
	# add country code
	data = _set_hopkins_country_code(data=data)
	
	return data


def _restructure(data, idx, cases):
	"""The raw downloaded data has dates column wise, not row wise.
	So we need to reorganize data to have dates in rows.
	
	Parameters
	----------
	data: DataFrame
		DataFrame of Hopkins class
	idx: int
		Column index that divides feature columns from time-series columns
	cases: str
		String of case (ConfirmedCases, ConfirmedDeaths, Recovered)
		
	Returns
	-------
	data: DataFrame
		Restructured (row-wise time-series) DataFrame
	"""
	print('-- restructure date columns to rows...')
	cols = data.columns.to_list()
	
	id_vars = cols[:idx]
	value_vars = cols[idx:]
	# print('- id vars ', id_vars)
	# print('val vars', value_vars)
	data = data.melt(id_vars, value_vars)
	
	# rename columns
	data = data.rename(columns={'variable': 'Date',
	                            'value': cases})
	# reformat date
	data['Date'] = pd.to_datetime(data['Date'], infer_datetime_format=True, errors='ignore')
	
	return data


def _set_hopkins_country_code(data):
	"""Set additional country code ISO 3166-1 Alpha-3.
	
	Parameters
	----------
	data: DataFrame
		DataFrame of Hopkins class
		
	Returns
	-------
	data: DataFrame
		Extended DataFrame including country codes
	"""
	print('-- set country codes...')
	# load ISO country names and codes
	iso = ISOCodes()
	# collect unknown country names
	unkown_countries = []
	
	for name in data['Country_Region'].unique():
		try:
			iso_code = iso.codes[iso.codes['Name'] == name]['Code'].unique()[0]
			# print('set code {0} for {1}'.format(set_code, name))
			data.loc[data['Country_Region'] == name, 'Country_Code'] = iso_code
		except Exception as e:
			unkown_countries.append(name)
	
	# print(unknown_countries)
	# unknown countries by hand
	kd_dict = {'BOL': 'Bolivia',
	           'BRN': 'Brunei',
	           'MNR': 'Burma',  # Myanmar
	           'CPV': 'Cabo Verde',
	           'COG': 'Congo (Brazzaville)',
	           'COD': 'Congo (Kinshasa)',
	           'CIV': "Côte d'Ivoire",
	           'CZE': 'Czechia',
	           'DIP': 'Diamond Princess',
	           'SWZ': 'Eswatini',
	           'VAT': 'Holy See',
	           'IRN': 'Iran',
	           'KOR': 'Korea, South',
	           'KOS': 'Kosovo',
	           'LAO': 'Laos',
	           'LBY': 'Libya',
	           'MSZ': 'MS Zaandam',
	           'MDA': 'Moldova',
	           'MKD': 'North Macedonia',
	           'RUS': 'Russia',
	           'SDN': 'South Sudan',
	           'SYR': 'Syria',
	           'TWN': 'Taiwan*',
	           'TZA': 'Tanzania',
	           'USA': 'US',
	           'VEN': 'Venezuela',
	           'VNM': 'Vietnam',  # Viet Nam
	           'GAZ': 'West Bank and Gaza'
	           }
	
	# update country codes
	for c, n in zip(kd_dict.keys(), kd_dict.values()):
		# print('set code {0} for {1}'.format(c, n))
		data.loc[(data['Country_Region'] == n), 'Country_Code'] = c
	
	# because of different writings
	data.loc[(data['Country_Region'] == "Cote d'Ivoire"), 'Country_Code'] = 'CIV'
	data['Country_Code'].fillna(-1, inplace=True)
	# TODO: (debug) finally check missing values
	# check_missing_values(data)
	
	return data


def _expand_dates(data):
	"""Expand date period back to 2020-01-01, because we use need to
	merge with Oxford data which starts at 2020-01-01.
	
	Parameters
	----------
	data: DataFrame
		DataFrame of Hopkins class
		
	Returns
	-------
	data: DataFrame
		Extended DataFrame with time-series starting at 2020-01-01
	"""
	print('- expand dates...')
	# collect new entries
	new_entries = []
	# check start and end date for each country and province
	for c in data['Country_Code'].unique():
		state = data.loc[data['Country_Code'] == c]
		
		ti = pd.to_datetime('2020-01-01', format='%Y-%m-%d', errors='ignore')
		t1 = state['DateTime'].min()
		
		# backward fill values
		bf = t1 - ti
		
		t1_row = state.loc[state['DateTime'] == t1]
		for d in range(int(bf.days)):
			a_date = ti + pd.Timedelta(d, 'd')
			
			new_row = t1_row.copy()
			
			new_row['DateTime'] = a_date
			new_row['Date'] = a_date.strftime('%Y-%m-%d')
			new_entries.append(new_row)
	
	# finally append all new entries to data
	data = data.append(new_entries)
	
	return data


def test_hopkins_data():
	print("Testing Hopkins DataFrame")
	hop = Hopkins()
	hop.update()
	print("Test finished")


if __name__ == '__main__':
	# do some tests
	test_hopkins_data()
