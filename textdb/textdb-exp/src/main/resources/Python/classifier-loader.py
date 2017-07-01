import sys
import pickle
import warnings

def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
#	print("neg")
	#fs = open('Senti.pickle', 'rb')
	warnings.filterwarnings("ignore")
	picklePath = sys.argv[1]
#	debugLine(path)
#	print(picklePath)
	pickleFile = open(picklePath, 'rb')
	print("After open pickeleFile: %s"% picklePath)
	Sentimm = pickle.load(pickleFile)
#	print("1700"+picklePath)
	for text in sys.argv[2:]:
		print(Sentimm.classify(text))
	pickleFile.close()

main()
