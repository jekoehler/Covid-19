"""

"""

import pandas as pd
import os

from hopkins import Hopkins
from data_utils import load_oxford_data, process_data

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
OXFORD_CSV_PATH = os.path.join(FILE_PATH, "data/JEK_OxCGRT_v21_latest.csv")


class Oxford:
	def __init__(self):
		"""Initialization with the latest Oxford data.
		The data is not ready for mobility model. Here we start
		preparation of data.
		"""
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
