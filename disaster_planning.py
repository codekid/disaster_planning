import urllib.request
import requests
import gzip
from bs4 import BeautifulSoup


def getFileData(url, filename, outputFile):

	url = url + filename
	response = urllib.request.urlopen(url)
	with open(outputFile, "ab") as outfile:
		outfile.write(gzip.decompress(response.read()))


def main():

	url = "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
	outputFile = "storm_data.csv"
	filenames = []

	result = requests.get(url)
	rawHtml = result.text
	soup = BeautifulSoup(rawHtml, "html.parser")

	# for files in soup.find_all("a", limit=10):
	for files in soup.find_all("a"):

		# print (files.text.strip())
		if "csv.gz" in files.text:
			# print (files.text.strip())
			filenames.append(files.text.strip())

	# data = getFileData(url, filenames[1], outputFile)

	for file in filenames:

		print ("Retrieving " + file)
		getFileData(url, file, outputFile)
		print ("Complete")

if __name__ == "__main__":
	main()