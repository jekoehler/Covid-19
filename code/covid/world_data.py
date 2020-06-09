#!/opt/anaconda3/envs/tf20/bin/python
# -*- coding: utf-8 -*-
""" Module that provides world population data for countries """

import pandas as pd
import os

from iso_data import ISOCodes

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
WP_CSV_FILE = os.path.join(FILE_PATH, "data/World_population_by_country_2020.csv")

N_DICT = {'BOL': 'Bolivia',
          'VGB': 'British Virgin Islands',
          'BRN': 'Brunei',
          'CPV': 'Cabo Verde',
          'ANT': 'Caribbean Netherlands',
          'IMN': 'Channel Islands',
          'ANT': 'Curaçao',
          'CZE': 'Czech Republic (Czechia)',
          'COD': 'DR Congo',
          'SWZ': 'Eswatini',
          'FRO': 'Faeroe Islands',
          'FLK': 'Falkland Islands',
          'VAT': 'Holy See',
          'IRN': 'Iran',
          'LAO': 'Laos',
          'LBY': 'Libya',
          'FSM': 'Micronesia',
          'MDA': 'Moldova',
          'PRK': 'North Korea',
          'MKD': 'North Macedonia',
          'RUS': 'Russia',
          'BLM': 'Saint Barthelemy',
          'SHN': 'Saint Helena',
          'KNA': 'Saint Kitts & Nevis',
          'MAF': 'Saint Martin',
          'SPM': 'Saint Pierre & Miquelon',
          'STP': 'Sao Tome & Principe',
          'MAF': 'Sint Maarten',
          'KOR': 'South Korea',
          'SDN': 'South Sudan',
          'VCT': 'St. Vincent & Grenadines',
          'PSE': 'State of Palestine',
          'SYR': 'Syria',
          'TWN': 'Taiwan',
          'TZA': 'Tanzania',
          'TCA': 'Turks and Caicos',
          'VIR': 'U.S. Virgin Islands',
          'VEN': 'Venezuela',
          'VNM': 'Vietnam',
          'WLF': 'Wallis & Futuna'}


class WorldPopulationData:
	
	def __init__(self):
		self.df = pd.read_csv(WP_CSV_FILE)
		self.process_world_data()
		self.add_iso_codes()
	
	def process_world_data(self):
		# Select desired columns and rename some of them
		self.df = self.df[
			['Country (or dependency)', 'Population (2020)', 'Density (P/Km²)', 'Land Area (Km²)', 'Med. Age', 'Urban Pop %']]
		self.df.columns = ['CountryName', 'Population', 'Density', 'LandArea', 'MedAge', 'UrbanPop']
		
		# Remove the % character from Urban Pop values
		self.df['UrbanPop'] = self.df['UrbanPop'].str.rstrip('%')
		
		# Replace Urban Pop and Med Age "N.A" by their respective modes, then transform to int
		self.df.loc[self.df["UrbanPop"] == 'N.A.', 'UrbanPop'] = int(
			self.df.loc[self.df['UrbanPop'] != 'N.A.', 'UrbanPop'].mode()[0])
		self.df['UrbanPop'] = self.df['UrbanPop'].astype('int16')
		self.df.loc[self.df['MedAge'] == 'N.A.', 'MedAge'] = int(
			self.df.loc[self.df['MedAge'] != 'N.A.', 'MedAge'].mode()[0])
		self.df['MedAge'] = self.df['MedAge'].astype('int16')
		
		self.df = self.df.sort_values(by=['CountryName'], ignore_index=True)
	
	def add_iso_codes(self):
		iso = ISOCodes()
		
		self.df = self.df.merge(iso.codes, left_on="CountryName", right_on="Name", how="left")
		self.df = self.df[['Code', 'CountryName', 'Population', 'Density', 'LandArea', 'MedAge', 'UrbanPop']]
		
		for c, n in zip(N_DICT.keys(), N_DICT.values()):
			self.df.loc[(self.df.CountryName == n), 'Code'] = c
		
		# these two countries still has NaN
		self.df.Code.fillna(0, inplace=True)
		self.df.loc[self.df.CountryName == 'Saint Martin', 'Code'] = 'MAF'
		self.df.loc[self.df.CountryName == 'Caribbean Netherlands', 'Code'] = 'ANT'


def my_test():
	print("Testing World Population DataFrame:")
	wp = WorldPopulationData()
	# first check if we're missing some values
	missing_count = {col: wp.df[col].isnull().sum() for col in wp.df.columns}
	missing = pd.DataFrame.from_dict(missing_count, orient='index')
	print(missing.nlargest(30, 0))
	print("- Country Names", wp.df.CountryName.nunique())
	print("- Country Codes", wp.df.Code.nunique())
	print("- Missing 4 codes for countries, but that is okay so far...:)")
	print("Test finished!")


if __name__ == "__main__":
	my_test()
