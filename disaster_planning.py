import urllib.request
import gzip
from bs4 import BeautifulSoup
import mysql.connector
import pyodbc
import csv
import os

# Pulls the data from the address and appends it to the file
def getFileData(url, filename):
	url = url + filename
	# Open url to file
	response = urllib.request.urlopen(url)
	# Append data to output 
	with open(filename[:-3:], "ab") as outfile:
		# Skip the first line of the file and write the others to the output file
		# next(f)
		outfile.write(gzip.decompress(response.read()))

def loadToDb(conn):
	pattern = "StormEvents_details-ftp"
	filenames = [ f for f in os.listdir(".") if pattern in f ]
	print (filenames)

	# Try to establish a connection to the db
	try:
			cursor = conn.cursor()

	except Exception as e:
	
		print("An issue occured {}".format(e))
		raise


	for filename in filenames:
		file_name = filename
		try:
			


			# Open a cursor

			insert_statement = ""

			finalCount = 0
			headCount = 0
			print ("Inserting {} data into the Database.".format(filename))
			with open(filename, newline="") as csvFile:
				
				# Pull contents of file into variable
				recordReader = csv.reader(csvFile, delimiter=",", quotechar="\"")
				# recordReader = [w.replace("\"","\\\"") for w in recordReader]
				header = []
				# Loop through the data, prepare insert statement and execute
				for i, row in enumerate(recordReader):


					if i != 0:

						finalCount = i
						insert_statement = "INSERT INTO ST_STORM_DATA (BEGIN_YEARMONTH,BEGIN_DAY,BEGIN_TIME,END_YEARMONTH,END_DAY,END_TIME,EPISODE_ID,EVENT_ID,STATE,STATE_FIPS,YEAR,MONTH_NAME,EVENT_TYPE,CZ_TYPE,CZ_FIPS,CZ_NAME,WFO,BEGIN_DATE_TIME,CZ_TIMEZONE,END_DATE_TIME,INJURIES_DIRECT,INJURIES_INDIRECT,DEATHS_DIRECT,DEATHS_INDIRECT,DAMAGE_PROPERTY,DAMAGE_CROPS,SOURCE,MAGNITUDE,MAGNITUDE_TYPE,FLOOD_CAUSE,CATEGORY,TOR_F_SCALE,TOR_LENGTH,TOR_WIDTH,TOR_OTHER_WFO,TOR_OTHER_CZ_STATE,TOR_OTHER_CZ_FIPS,TOR_OTHER_CZ_NAME,BEGIN_RANGE,BEGIN_AZIMUTH,BEGIN_LOCATION,END_RANGE,END_AZIMUTH,END_LOCATION,BEGIN_LAT,BEGIN_LON,END_LAT,END_LON,EPISODE_NARRATIVE,EVENT_NARRATIVE,DATA_SOURCE, FILENAME) VALUES"
						# Escape all double quote " characters that may be in the row
						row = [w.replace("\"","\\\"") for w in row]
						# print (row)

						# Find out how many columns of data there are and create enough curly braces for formatting
						column_num = len(row)
						columns = ("IFNULL(\"{}\",NULL),"* (column_num-1)) + "IFNULL(\"{}\",NULL)"

						# Create values portion of insert statement
						values = "(" + columns 
						values = values.format(*row)
						values = values +",'"+ file_name  +"')"
						values = values.replace("\\\",NULL","\", NULL")

						sql = insert_statement + values

						# Run the query
						cursor.execute(sql)
						finalCount =finalCount-headCount
						if (finalCount % 30000) == 0:
							conn.commit()

				# finalCount -= 1
				conn.commit()
				print ("Complete")
				print ( "{} Records commited.".format(finalCount))
			csvFile.close()

		
		except Exception as e:
			
			print ("An error occured: {}".format(e))
			print ("SQL is: " + sql)
			print (row)
			moveFile(".","./failed", filename)
			conn.rollback()
			raise	

		conn.commit()
		print ("A total of {} records Inserted".format(finalCount))

		print ("Inserting into log table")

		try:

			sql = "INSERT INTO LOG_STORM_DATA_LOADED (FILENAME, RECORD_COUNT, DATE_LOADED) VALUES ('{}', {}, NOW())".format(file_name, finalCount)
			cursor.execute(sql)
			conn.commit()
			print ("Complete")

		except Exception as e:
			print ("An error occured: {}".format(e))
			print ("SQL is: " + sql)
			conn.rollback()	

		print ("")	

		moveFile(".","./processed", filename)

	cursor.close()


def moveFile(sourceDir, destDir, filename):

	if os.path.isdir(destDir):
		os.rename(sourceDir+"/"+filename, destDir + "/" + filename)
	else:
		os.makedirs(destDir)
		os.rename(sourceDir+"/"+filename, destDir + "/" + filename)




# Get the names of the files on the website and store them in a list
def getFilenames(soup, filenames):
	print("Extracting data from files.")
	# Find all the <a> tags, extract the file names and add them to a list
	# for files in soup.find_all("a", limit=10):
	for files in soup.find_all("a"):

		# print (files.text.strip())
		if "details-ftp" in files.text:
			# print (files.text.strip())
			filenames.append(files.text.strip())

	return filenames

# Queries the data and summarise property damage by state and year. The output is then written to a csv file
def answerDamageQuery(summaryFile, conn):

	sql ="""
	SELECT STATE, SUBSTR(BEGIN_YEARMONTH,1,4)EVENT_YEAR, SUM(REPLACE(DAMAGE_PROPERTY,"K",""))*1000 DAMAGE_COST
	FROM ST_STORM_DATA
	GROUP BY STATE, SUBSTR(BEGIN_YEARMONTH,1,4)
	"""


	try:
		cursor=conn.cursor()
		print ("Running query")
		cursor.execute(sql)
		
		result = cursor.fetchall()
		print ("Retrieving data")
		
		conn.commit()
		cursor.close()
		conn.close()
		# print (result)

		print ("Writing data to file")
		# Write the results to the csv file
		with open(summaryFile, "w")as summary:

			recordWriter = csv.writer(summary, delimiter=",",
				quotechar="\"", quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
			for data in result:
				recordWriter.writerow(data)
		print ("Complete")

	except Exception as e:
		print ("There seems to be an issue: " + e)

def summarizeData(conn):

	try:
		
		print ("Summarizing data")
		cursor = conn.cursor()
		cursor.callproc("summarize_data")
		print ("Complete")
		# conn.close()
	except Exception as e:
		print ("There seems to be an issue: {}".format(e))


def main():

	url = "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
	outputFile = "storm_data.csv"
	summaryFile = "storm_data_aggregate.csv"
	filenames = []
	conn = mysql.connector.connect(
		user="root", 
		password="p@$$w0rd", 
		host="localhost", 
		database="test")


	print ("Getting the file names to be extracted.")
	# Get the raw html from the page that contains the list of file names
	result = urllib.request.urlopen(url)
	rawHtml = result.read()
	soup = BeautifulSoup(rawHtml, "html.parser")

	filenames = getFilenames(soup, filenames)

	sql ="SELECT DISTINCT CONCAT(FILENAME, '.gz')FILENAME FROM LOG_STORM_DATA_LOADED"

	try:
		cursor = conn.cursor()
		cursor.execute(sql)
		loadedFilenames = cursor.fetchall()
		conn.commit()

	except Exception as e:
		print ("There seems to be an issue: " + str(e))

	# filenames= [f[:-3:] for f in filenames]
	loadedFilenames = [f[0] for f in loadedFilenames]
	# print (filenames)
	# print (loadedFilenames)
	# print (len(filenames))
	# print (len(loadedFilenames))
	filenames = list(set(filenames)- set(loadedFilenames))
	print (len(filenames))



	# Extract the data for each file
	for filename in filenames:

		print ("Retrieving " + filename)
		getFileData(url, filename)
		print ("Complete")


	# Insert Data into the db
	loadToDb(conn)

	# Load Fact table
	summarizeData(conn)

	# Summarize property damage by year and state
	answerDamageQuery(summaryFile, conn)


if __name__ == "__main__":
	main()
