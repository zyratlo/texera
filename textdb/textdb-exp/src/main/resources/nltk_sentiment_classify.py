import sys
import pickle
import csv

picklePath = sys.argv[1]
dataPath = sys.argv[2]
resultPath = sys.argv[3]
mymatrix = {}
myclass = {}

# call format:
# python3 classifier-loader picklePath dataPath resultPath
def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
	readData()
	classfyData()
	writeresult()

def writeresult():
	with open(resultPath, 'w', newline='') as csvfile:
		resultWriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		resultWriter.writerow(["TupleID", "ClassLabel"])
		for id, classLabel in myclass.items():
			resultWriter.writerow([id, classLabel])

def classfyData():
	pickleFile = open(picklePath, 'rb')
	Sentimm = pickle.load(pickleFile)#	for text in sys.argv[2:]:
	for key, value in mymatrix.items():
		myclass[key] = Sentimm.classify(value)
	pickleFile.close()
	for id, classlabel in myclass.items():
		print(id + ": " + classlabel)
		
def readData():
	with open(dataPath, newline='') as csvfile:
		dataReader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for row in dataReader:
			mymatrix[row[0]] = row[1]
			
if __name__ == "__main__":
	main()