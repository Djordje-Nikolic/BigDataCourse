import os
import csv
import time
import helpers
import FCDXMLHandler

xmlDataFilePath = "C:/Users/DjordjeNikolic/Sumo/vienna center car bus truck moto 24h/vienna_month_car_bus_truck_moto.xml"

xmlDataFileName = os.path.basename(xmlDataFilePath)
xmlDataFileSize = os.path.getsize(xmlDataFilePath)
xmlDataFileSizeDescription = helpers.convert_bytes(xmlDataFileSize)

print(f"The file '{xmlDataFileName}' with the size of {xmlDataFileSizeDescription} will be converted to CSV.")

folderPath = os.path.dirname(xmlDataFilePath)
csvDataFileName = ''.join(os.path.splitext(xmlDataFileName)[:-1]) + '.csv'
csvDataFilePath = os.path.join(folderPath, csvDataFileName)
lineCount = 0

print("Processing...")
start = time.time()

with open(csvDataFilePath, 'w', newline='') as csvFile:
    csvWriter = csv.writer(csvFile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    lineCount = FCDXMLHandler.parseFCDFile(xmlDataFilePath, csvWriter.writerow, csvWriter.writerow)

end = time.time()
print(f"Processing finished in {end - start:.2f}s.")

csvDataFileSize = os.path.getsize(csvDataFilePath)
csvDataFileSizeDescription = helpers.convert_bytes(csvDataFileSize)

print(f"The result has been saved to '{csvDataFilePath}', with the size of {csvDataFileSizeDescription} and the line count of {lineCount}.")