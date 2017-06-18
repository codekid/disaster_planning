import urllib.request
import gzip
from bs4 import BeautifulSoup
import mysql.connector
import csv

# Pulls the data from the address and appends it to the file
def getFileData(url, filename, outputFile):


	url = url + filename
	# Open url to file
	response = urllib.request.urlopen(url)
	# Append data to output file
	with open(outputFile, "ab") as outfile:
		# Skip the first line of the file and write the others to the output file
		# next(f)
		outfile.write(gzip.decompress(response.read()))

# Inserts the data from the file into the database
def insertData(outputFile, conn):
	try:
		


		# Open a cursor
		cursor = conn.cursor()

		insert_statement = ""

		finalCount = 0
		headCount = 0
		print ("Inserting data into the Database.")
		with open(outputFile, newline="") as csvFile:
			
			# Pull contents of file into variable
			recordReader = csv.reader(csvFile, delimiter=",", quotechar="\"")
			# recordReader = [w.replace("\"","\\\"") for w in recordReader]
			header = []
			# Loop through the data, prepare insert statement and execute
			for i, row in enumerate(recordReader):

				finalCount = i
				# Used to get the column names from the file
				if "EVENT_ID" in row:

					headCount += 1
					header = row
					# Find out the number of columns then create enough curly braces {} for formatting
					column_num = len(header)
					columns = ("{}," * (column_num-1)) + "{}"
					# print (columns)
					
					# Create first section of insert statment
					insert_statement = "INSERT INTO ST_STORM_DATA (" + columns + ") VALUES"
					insert_statement = insert_statement.format(*header)
					# print (insert_statement)

				if "EVENT_ID" not in row:

					# Escape all double quote " characters that may be in the row
					row = [w.replace("\"","\\\"") for w in row]
					# print (row)

					# Find out how many columns of data there are and create enough curly braces for formatting
					column_num = len(row)
					columns = ("IFNULL(\"{}\",NULL),"* (column_num-1)) + "IFNULL(\"{}\",NULL)"

					# Create values portion of insert statement
					values = "(" + columns + ")"
					values = values.format(*row)
					values = values.replace("\\\",NULL","\", NULL")

					sql = insert_statement + values

					# Run the query
					cursor.execute(sql)
					finalCount =finalCount-headCount
					if (finalCount % 30000) == 0:
						conn.commit()
						print ( "{} Records commited.".format(finalCount))

			print ("Completed")

	
	except Exception as e:
		
		print ("An error occured: {}".format(e))
		print ("SQL is: " + sql)
		print (row)
		conn.rollback()		

	conn.commit()
	print ("A total of {} records Inserted".format(finalCount))
	cursor.close()
	conn.close()

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

	# Extract the data for each file
	for filename in filenames:

		print ("Retrieving " + filename)
		getFileData(url, filename, outputFile)
		print ("Complete")

	# Insert Data into the db
	insertData(outputFile, conn)

	# Summarize property damage by year and state
	answerDamageQuery(summaryFile, conn)


if __name__ == "__main__":
	main()