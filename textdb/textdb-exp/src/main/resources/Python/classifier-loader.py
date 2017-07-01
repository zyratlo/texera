import sys
import pickle
import warnings
#import nltk

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

warnings.filterwarnings("ignore")
main()
