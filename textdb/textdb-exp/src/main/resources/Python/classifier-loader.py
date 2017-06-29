import sys
import pickle

def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
	print("neg")
	fs = open('Senti.pickle', 'rb')
	Sentimm = pickle.load(fs)
	fs.close()
	for text in sys.argv[1:]:
		print("neg")
	#	print(Sentimm.classify(text))

main()
