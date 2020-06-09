"""

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
	
	def __init__(self):
		"""Initialize Hopkins class and load dataset from local source, otherwise
		load data from Hopkins repository and prepare and save the data as pandas's
		DataFrame."""
		try:
			print("Load Hopkins data...")
			self.data = pd.read_csv(HOPKINS_CSV_PATH)
			print("- local data loaded")
			del self.data["Unnamed: 0"]
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
		self.data = {case: load_global_data(case, url) for case, url in GLOBAL_URLS.items()}
		self.data = prepare_and_merge(self.data)
		
		self.data["DateTime"] = pd.to_datetime(self.data["Date"], format="%Y-%m-%d", errors="ignore")
		self.data['Date'] = self.data['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))
		self.data = expand_dates(self.data)
		
		self.data = self.data[['Date', 'Country_Code', 'ConfirmedCases', 'ConfirmedDeaths', 'Recovered']]
		self.min_date = self.data["Date"].unique().min()
		self.max_date = self.data["Date"].unique().max()
		self.num_date = self.data["Date"].nunique()
		print("- dates from {} to {} total {} days".format(self.min_date, self.max_date, self.num_date))
		print("- merged data:", self.data.shape)
		self.save()
	
	def save(self):
		self.data.to_csv(HOPKINS_CSV_PATH, columns=self.data.columns)
		print("- saved at:", HOPKINS_CSV_PATH)


def prepare_and_merge(data):
	"""Prepare data of three different sources (confirmed cases,
	confirmed deaths and recovered). These three sources has different
	length and some countries doesn't have cumulated values (they have
	values for provinces instead, so before merging all three sources we
	need to compute the cumulative values for a country).
	"""
	print("- prepare and merge data... ")
	# reorder columns
	for case, url in GLOBAL_URLS.items():
		new_cols = ['Date', 'Country_Code', 'Province_State', 'Country_Region', case]
		temp = data[case]
		temp = temp[new_cols]
		
		days = temp["Date"].nunique()
		# get all countries having some provinces
		provs = check_provinces(temp, days)
		# check countries without nan at Province_State
		countries_without_nan = []
		for p in provs:
			lookup = (temp.Country_Code == p) & (temp.Province_State.isna())
			if len(temp[lookup]['Province_State'].unique()) < 1:
				countries_without_nan.append(p)
		
		collected_dfs = [temp]
		for c in countries_without_nan:
			nan_df = compute_nan(temp, c, case)
			collected_dfs.append(nan_df)
		
		temp = pd.concat(collected_dfs)
		temp = temp.sort_values(by=["Country_Code", "Province_State", "Date"])
		# get rid of Province_State
		temp = temp[temp.Province_State.isna()]
		temp = temp.drop(columns=["Province_State"])
		data[case] = temp
	
	# merge all three different dataframes
	new_df = data[COL.cc]
	new_df = new_df.merge(data[COL.cd],
	                      left_on=['Date', 'Country_Code', 'Country_Region'],
	                      right_on=['Date', 'Country_Code', 'Country_Region'])
	new_df = new_df.merge(data[COL.rc], on=['Date', 'Country_Code', 'Country_Region'])
	
	return new_df

	
def check_provinces(data, days):
	"""Helper function to check which country has values for provinces.
	"""
	def check_number_of_entries(data, code):
		parts = data[(data.Country_Code == code)]
		return parts.shape[0]
	
	country_codes = data['Country_Code'].unique()
	
	prov_countries = {}
	
	for code in country_codes:
		num = check_number_of_entries(data, code)
		if num > days:
			prov_countries[code] = num
	
	return prov_countries.keys()
	

def compute_nan(data, ccode, case):
	"""Helper function to compute the cumulative values from
	all provinces to be set as the countries values for the
	provinces NaN values.
	"""
	lookup = (data.Country_Code == ccode)
	country = data[lookup]
	new_nan = country.groupby('Date')[case].sum()
	
	new_df = pd.DataFrame({'Date': new_nan.index, case: new_nan.values})
	new_df['Province_State'] = np.nan
	new_df['Country_Region'] = country['Country_Region'].unique()[0]
	new_df['Country_Code'] = ccode
	
	return new_df


def load_global_data(cases, path_url):
	"""Load global data from Hopkins database at github (see URLs).
	These database has dates column-wise. We need to restructure the
	table to have dates row-wise
	"""
	print("- load data for", cases)
	data = pd.read_csv(path_url, ',')
	# reorganize dates
	data = restructure(data=data, idx=4, cases=cases)
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
	data = set_hopkins_country_code(data=data)
	
	return data


def restructure(data, idx, cases):
	"""The raw downloaded data has dates column wise, not row wise.
	So we need to reorganize data to have dates in rows.
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


def set_hopkins_country_code(data):
	"""Set additional country code ISO 3166-1 Alpha.
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
	
	#
	for c, n in zip(kd_dict.keys(), kd_dict.values()):
		# print('set code {0} for {1}'.format(c, n))
		data.loc[(data['Country_Region'] == n), 'Country_Code'] = c
	
	# because of different writings
	data.loc[(data['Country_Region'] == "Cote d'Ivoire"), 'Country_Code'] = 'CIV'
	data['Country_Code'].fillna(-1, inplace=True)
	# finally check missing values
	# check_missing_values(data)
	
	return data


def expand_dates(data):
	"""Expand date period back to 2020-01-01, because we use need to
	merge with Oxford data which starts at 2020-01-01.
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
