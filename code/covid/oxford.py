"""Oxford class

This class provides a `pandas` DataFrame object of the Oxford Covid-19
Government Response Tracker (OxCGRT) data, see README.md

The DataFrame is loaded from local subdirectory, or, if not already existing,
loaded from Oxford repository and processed into desired DataFrame.

The processing steps are (see data_utils.py)
(1) load Oxford data
(2) init Hopkins data
(3) merge Oxford with Hopkins data
(4) extend data
(5) fill missing values
(6) add world population data
(7) create features (stringency, mobility etc.)

This class can be updated with the latest data from Oxford University see README.md

NOTE:
	This class is primarily used from Mobility class to update Oxford data.
"""

import pandas as pd
import os

from hopkins import Hopkins
from data_utils import load_oxford_data, process_data

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
OXFORD_CSV_PATH = os.path.join(FILE_PATH, "data/JEK_OxCGRT_v21_latest.csv")


class Oxford:
	"""
	Attributes
		----------
		df : DataFrame
			DataFrame object (pandas) that holds all feature columns.
		min_date: str
			Minimum or starting date of time series
		max_date: str
			Maximum or latest date of time series
		num_date: int
			Number of dates/days
			
		Methods
		-------
		save()
			Save DataFrame to local subdirectory OXFORD_CSV_PATH
		print_info()
			Print some information about the DataFrame
	"""
	def __init__(self):
		self.df = load_oxford_data()
		self.min_date = self.df["Date"].unique().min()
		self.max_date = self.df["Date"].unique().max()
		self.num_date = self.df["Date"].nunique()
		# merge with Hopkins data
		hopkins = Hopkins()
		hdf = hopkins.data
		hdf_max_date = hdf['Date'].max()
		
		self.df = self.df[self.df['Date'] <= hdf_max_date]
		self.df = self.df.merge(hdf, on=['Date', 'Country_Code'], how='left')
		print("- merged Oxford and Hopkins", self.df.shape)

		self.min_date = self.df["Date"].unique().min()
		self.max_date = self.df["Date"].unique().max()
		self.num_date = self.df["Date"].nunique()
		print("- dates from {} to {} total {} days".format(self.min_date, self.max_date, self.num_date))
		self.mdf = self.df[["Country_Code", "CountryName", "Date", "DateTime", "ConfirmedCases",
		                    "ConfirmedDeaths", "Recovered", "StringencyIndex"]].copy()
		
		self.mdf = process_data(self.mdf, self.df)
		
	def save(self):
		print("- saved: ", OXFORD_CSV_PATH)
		self.mdf.to_csv(OXFORD_CSV_PATH, columns=self.mdf.columns)
		
	def print_info(self):
		print("Oxford DataFrame Info")
		print("- columns:\n", self.df.columns)
		print("- dates from {0} to {1}".format(self.min_date, self.max_date))
		print("- total {0} days".format(self.num_date))
		print("Mobility DataFrame Info")
		print("- columns:\n", self.mdf.columns)
	

def test_oxford_data():
	print("Testing Oxford DataFrame:")
	ox = Oxford()
	#ox.print_info()
	#ox.save()
	print("Test finished!")


if __name__ == "__main__":
	
	test_oxford_data()
