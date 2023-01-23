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