import os
import sys
import csv
import time
import xml.sax

class FCDHandler( xml.sax.ContentHandler ):
    def __init__(self, writeHeader, writeRow):
        self.CurrentData = ''
        self.writeHeader = writeHeader
        self.writeRow = writeRow
        self.currentTimeStep = -1
        self.content = ''
        self.lineCount = 0
        self.attributeNames = None

   # Call when an element starts
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == "timestep":
            self.currentTimeStep = attributes["time"]
        elif tag == "vehicle":
            if self.attributeNames is None:
                self.attributeNames = attributes.getNames()
                self.writeCSVHeader()
            self.writeCSVRow(attributes)

   # Call when an elements ends
    def endElement(self, tag):
        self.CurrentData = ""
        self.content = ''

   # Call when a character is read
    def characters(self, content):
        self.content += content

    def writeCSVHeader(self):
        self.writeHeader(['timestep'] + self.attributeNames)

    def writeCSVRow(self, attributes):
        valuesToWrite = []
        valuesToWrite.append(self.currentTimeStep)

        for attrName in self.attributeNames:
            valuesToWrite.append(attributes[attrName])
            
        self.lineCount += 1
        self.writeRow(valuesToWrite)

def parseFCDFile(filePath, writeHeader, writeRow):
    # create an XMLReader
   parser = xml.sax.make_parser()

   # turn off namespaces
   parser.setFeature(xml.sax.handler.feature_namespaces, 0)

   # override the default ContextHandler
   Handler = FCDHandler(writeHeader, writeRow)
   parser.setContentHandler( Handler )
   
   parser.parse(filePath)
   return Handler.lineCount

def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Missing command line argument for path.")

    xmlDataFilePath = sys.argv[1]

    xmlDataFileName = os.path.basename(xmlDataFilePath)
    xmlDataFileSize = os.path.getsize(xmlDataFilePath)
    xmlDataFileSizeDescription = convert_bytes(xmlDataFileSize)

    print(f"The file '{xmlDataFileName}' with the size of {xmlDataFileSizeDescription} will be converted to CSV.")

    folderPath = os.path.dirname(xmlDataFilePath)
    csvDataFileName = ''.join(os.path.splitext(xmlDataFileName)[:-1]) + '.csv'
    csvDataFilePath = os.path.join(folderPath, csvDataFileName)
    lineCount = 0

    print("Processing...")
    start = time.time()

    with open(csvDataFilePath, 'w', newline='') as csvFile:
        csvWriter = csv.writer(csvFile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        lineCount = parseFCDFile(xmlDataFilePath, csvWriter.writerow, csvWriter.writerow)

    end = time.time()
    print(f"Processing finished in {end - start:.2f}s.")

    csvDataFileSize = os.path.getsize(csvDataFilePath)
    csvDataFileSizeDescription = convert_bytes(csvDataFileSize)

    print(f"The result has been saved to '{csvDataFilePath}', with the size of {csvDataFileSizeDescription} and the line count of {lineCount}.")