import requests, os, datetime, zipfile, io, pandas as pd
import cProfile

pr = cProfile.Profile()
pr.enable()

''' 

The following code downloads the bhavcopy for derivatives and equity 
as well as the list of banned securities from NSE server. 
Required inputs
1) Date as string input in 'ddmmyyyy' format, 
2) holidays and no holidays dates stored in csv files under the column header 'Date'
3) The path to the directory where output from the program will be stored. 

Please see below block to check user input variables.  

'''

############################# User inputs here ###################################
# Enter the date for download in 'ddmmyyyy' format
date = '12122019'

# Enter the path for output
outputLink = r'D:\Work\NSEDownloader\Output'

# Path to the 'holidays file'
holidayListPath = r"D:\Work\NSEDownloader\Holidaylist_NSE.csv"

# Path to the 'no holidays file'
noHolidayListPath = r"D:\Work\NSEDownloader\NoHoliday_NSE.csv"

##################################################################################
# Assertions about files
assert os.path.isfile(holidayListPath), "The following path for the holidays list file is incorrect.\n" + holidayListPath
assert os.path.isfile(noHolidayListPath), "The following path for the no holidays list file is incorrect.\n" + noHolidayListPath

# Convert string to datetime object
dateMod = datetime.datetime.strptime(date, "%d%m%Y")

# Read the 'holidays file' and 'no holidays file' and convert them to required string format
holidays = pd.read_csv(holidayListPath, encoding='ISO-8859-1')['Date'].dropna()
noHolidays = pd.read_csv(noHolidayListPath, encoding='ISO-8859-1')['Date'].dropna()

holidays = holidays.map(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
noHolidays = noHolidays.map(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))

holidays = holidays.map(lambda x: datetime.datetime.strftime(x, "%d%m%Y"))
noHolidays = noHolidays.map(lambda x: datetime.datetime.strftime(x, "%d%m%Y"))

# Check if date is in list of holidays
if date in list(holidays):
    raise Exception("The given date corresponds to a holiday.")

# Check if date is a weekend and date is a 'no holiday day' for the markets
if (datetime.datetime.strftime(dateMod, "%w") in [0,6]) and (date not in list(noHolidays)):
    raise Exception("The given date corresponds to a weekend.")

# Downloading for banned securities
foBanFileName = 'fo_secban_' + date + '.csv'
foBanLink = 'https://www.nseindia.com/archives/fo/sec_ban/'

r = requests.get(foBanLink + foBanFileName)
with open(os.path.join(outputLink, foBanFileName), 'wb') as f:
    f.write(r.content)


# Downloading historical data for equities
equityFileName = 'cm' + datetime.datetime.strftime(dateMod, '%d%b%Y').upper() + 'bhav.csv.zip'
equityFileLink = 'https://www.nseindia.com/content/historical/EQUITIES/' + datetime.datetime.strftime(dateMod, "%Y") + '/' + datetime.datetime.strftime(dateMod, "%b").upper()  + '/' + equityFileName

r = requests.get(equityFileLink, stream=True)
zip = zipfile.ZipFile(io.BytesIO(r.content))
zip.extractall(path=outputLink)

# Downloading historical data for derivatives
derivFileName = 'fo' + datetime.datetime.strftime(dateMod, '%d%b%Y').upper() + 'bhav.csv.zip'
derivFileLink = 'https://www.nseindia.com/content/historical/DERIVATIVES/' + datetime.datetime.strftime(dateMod, "%Y") + '/' + datetime.datetime.strftime(dateMod, "%b").upper()  + '/' + derivFileName

r = requests.get(derivFileLink, stream=True)
zip = zipfile.ZipFile(io.BytesIO(r.content))
zip.extractall(path=outputLink)

pr.disable()
pr.print_stats(sort='calls')