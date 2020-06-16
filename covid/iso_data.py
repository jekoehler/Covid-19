""" Module that provides ISO code for country names """

import pandas as pd
import os

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
ISO_CSV_FILE = os.path.join(FILE_PATH, 'data/ISO_3166-1_alpha-3_CountryCodes.csv')


class ISOCodes:
	
	def __init__(self):
		
		self.codes = pd.read_csv(ISO_CSV_FILE)
		self.codes = self.codes.sort_values(by=['Name'], ignore_index=True)
	
		self.name_list = self.codes["Name"].unique()
		self.code_list = self.codes["Code"].unique()

	
def test_iso_data():
	print("Testing ISOCodes class")
	iso = ISOCodes()
	print("# of codes:", iso.codes["Name"].nunique())
	print("Test finished.")


if __name__ == "__main__":
	test_iso_data()
