"""Mobility Class

This class allows the user to load a pandas DataFrame object that holds
feature columns like: Date, Cases, Deceased, Recovered, Stringency, Mobility etc.

This class can be updated with the latest data from two different sources:
(1) Actual cases from Johns Hopkins University see Readme.md
(2) Latest stringency index from Oxford University see Readme.md

This class allows the user to plot some features of the data.
"""

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import os

from data_utils import COL, FEATURE, FEATURE_DICT
from data_utils import load_oxford_data, extend_data, fill_missing_values
from data_utils import create_features, add_world_population_data
from oxford import Oxford
from hopkins import Hopkins

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
MOBILITY_CSV_PATH = os.path.join(FILE_PATH, "data/JEK_OxCGRT_v21_latest.csv")


class Mobility:
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
			Number of dates/days
			
		Methods
		-------
		load_mobility_data()
			Load local data stored at subdirectory MOBILITY_CSV_PATH
		print_info()
			Print information about the timeseries data
		update()
			Update data with the latest data from Hopkins and Oxford
		save()
			Save data to local subdirectory MOBILITY_CSV_PATH
		plot_mobility()
			Plot cases and mobility data of a single country
		"""
	def __init__(self):
		try:
			print("Loading mobility data...")
			self.data = pd.read_csv(MOBILITY_CSV_PATH)
			# reformat date
			self.data["DateTime"] = pd.to_datetime(self.data["Date"], format="%Y-%m-%d", errors="ignore")
			
			self.min_date = self.data["Date"].unique().min()
			self.max_date = self.data["Date"].unique().max()
			self.num_date = self.data["Date"].nunique()
			
			print("- shape:", self.data.shape)
		except FileNotFoundError as e:
			print("Error: no data present for file:", os.path.split(MOBILITY_CSV_PATH)[-1])
			print("Proceed preparing mobility data...")
			ox = Oxford()
			ox.save()
			
			self.data = ox.mdf
			self.min_date = self.data["Date"].unique().min()
			self.max_date = self.data["Date"].unique().max()
			self.num_date = self.data["Date"].nunique()
			
	def load_mobility_data(self):
		print("Loading mobility data...")
		self.data = pd.read_csv(MOBILITY_CSV_PATH)
		
		self.data["DateTime"] = pd.to_datetime(self.data["Date"], format="%Y-%m-%d", errors="ignore")
		
		self.min_date = self.data["Date"].unique().min()
		self.max_date = self.data["Date"].unique().max()
		self.num_date = self.data["Date"].nunique()
		
		print("- shape:", self.data.shape)
		
	def print_info(self):
		print("Mobility DataFrame Info")
		print("- columns:\n", self.data.columns)
		print("- dates from {0} to {1}".format(self.min_date, self.max_date))
		print("- total {0} days".format(self.num_date))

	def check_for_update(self):
		print("Check for updates...")
		today = pd.Timestamp.today(tz='MET').strftime('%Y-%m-%d')
		if today > self.max_date:
			print("- update available. Proceed with -update()")
		else:
			print("- current data is up-to-date.")
	
	def update(self):
		print("Update Mobility Data...")
		od = load_oxford_data()
		
		# merge with Hopkins data
		hopkins = Hopkins()
		hopkins.update()
		od = od.merge(hopkins.data, on=['Date', 'Country_Code'])
		
		if od is not None:
			od_max_date = od["DateTime"].unique().max()
			md_max_date = pd.to_datetime(self.max_date)

			if md_max_date < od_max_date:
				# get entries for new dates
				od = od.loc[od["DateTime"] > md_max_date]
				
				new_df = od[["Country_Code", "CountryName", "Date", "DateTime", "ConfirmedCases",
		                    "ConfirmedDeaths", "Recovered", "StringencyIndex"]].copy()
				print("Update with new data", new_df.shape)
				print("- dates", new_df["Date"].unique())
				
				new_df = extend_data(new_df, od)
				new_df = add_world_population_data(new_df)
				print("- new_df processed:", new_df.shape)

				# merge new and current data
				self.data = pd.concat([self.data, new_df])
				self.data = fill_missing_values(self.data)
				
				self.data = create_features(self.data)
				print("- updated data", self.data.shape)
				
				self.save()
				
			else:
				print("No update necessary:")
				print("- latest Oxford data  :", od_max_date)
				print("- latest Mobility data:", md_max_date)
	
	def save(self):
		"""Save data as pandas DataFrame to local subdirectory
		"""
		self.data.to_csv(MOBILITY_CSV_PATH, columns=self.data.columns, index=False)
	
	def plot_mobility(self, ccode, feat="st"):
		"""Plot cases and mobility data of a single country
		
		Parameters
		----------
		ccode: str
			Country Code to select country specific data
		feat: str
			Feature of mobility data, e.g Stringency (st), Mobility (mt)
		"""
		country = self.data[self.data['Country_Code'] == ccode]['CountryName'].unique()[0]
		
		feats = ['ConfirmedCases', 'ConfirmedDeaths', 'Recovered', 'Active']
		
		# data to plot
		dcc_date = self.data[(self.data.Country_Code == ccode) & (self.data[COL.dcc] >= 0.0)].groupby(['Date']).agg({COL.dcc: ['sum']})
		mt_date = self.data[(self.data['Country_Code'] == ccode) & (self.data[COL.dcc] >= 0.0)].groupby(['Date']).agg({feat: ['sum']})
		cc_max = int(self.data[self.data['Country_Code'] == ccode][COL.cc].max() * 1.05)
		dcc_max = int(self.data[self.data['Country_Code'] == ccode][COL.dcc].max() * 1.05)
		
		# plot figure
		fig = plt.figure(figsize=(15, 10), constrained_layout=False)
		gs = gridspec.GridSpec(2, 1, figure=fig)
		
		# plot total cases vs. Mobility
		ax1 = fig.add_subplot(gs[0, 0])
		ax1.set_title(country + "\n", size=16)
		ax1.set_ylabel("Total Cases", size=13)
		ax1.set_ylim([0, cc_max])
		for f in feats:
			data = self.data[(self.data['Country_Code'] == ccode) & (self.data[f] >= 0.0)].groupby(['Date']).agg({f: ['sum']})
			ax1.plot(data)
		ax1.legend([ax1.get_children()[0], ax1.get_children()[1],
		            ax1.get_children()[2], ax1.get_children()[3]],
		           ['Total Confirmed Cases', 'Total Confirmed Deaths',
		            'Total Recovered', 'Total Active'],
		           bbox_to_anchor=[0, 0, 1, 1])
		
		plt.xticks(dcc_date.index[::30])
		
		# plot daily cases vs. Mobility
		ax2 = fig.add_subplot(gs[1, 0])
		# right scale
		ax2m = ax2.twinx()
		rspine_2 = ax2m.spines['right']
		rspine_2.set_position(('axes', 1.0))
		# left scale
		ax2.set_ylabel("Daily Confirmed Cases", size=13)
		ax2.set_ylim([0, dcc_max])
		new_cases = np.reshape(dcc_date.values, (dcc_date.values.shape[0]))
		ax2.bar(dcc_date.index, new_cases, width=1.0, alpha=0.6)
		
		# plot Mobility with right scale
		ax2m.set_frame_on(True)
		ax2m.patch.set_visible(False)
		ax2m.set_ylabel(FEATURE_DICT[feat], size=13)
		ax2m.set_ylim([0, 1])
		ax2m.plot(mt_date, 'ro', markersize=5.0, mfc='red', mec='red')
		ax2box = ax2.get_position()
		ax2m.legend([ax2.get_children()[0], ax2m.get_lines()[0]],
		            ['Daily Confirmed Cases', FEATURE_DICT[feat]],
		            bbox_to_anchor=[0, 0, 1, 1])
		
		plt.xticks(dcc_date.index[::30])
		plt.show()


def test_mobility_data():
	print("Testing Mobility DataFrame:")
	mobility = Mobility()
	#mobility.print_info()
	mobility.plot_mobility("DEU", feat=FEATURE.mobility)

	#mobility.update()
	print("Test finished!")


if __name__ == "__main__":
	
	test_mobility_data()
