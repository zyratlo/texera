import sys
import pickle

def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
#	print("neg")
	#fs = open('Senti.pickle', 'rb')
	path = sys.argv[1]
	debugLine(path)
	fs = open(path, 'rb')
	Sentimm = pickle.load(fs)
	fs.close()
	for text in sys.argv[2:]:
		print(Sentimm.classify(text))

main()
