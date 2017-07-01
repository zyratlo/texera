import sys
import pickle

def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
	picklePath = sys.argv[1]
	pickleFile = open(picklePath, 'rb')
	Sentimm = pickle.load(pickleFile)
	for text in sys.argv[2:]:
		print("%s"%Sentimm.classify(text))
	pickleFile.close()

if __name__ == "__main__":
	main()
