import sys
import pickle

def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
#	print("neg")
	#fs = open('Senti.pickle', 'rb')
	picklePath = sys.argv[1]
#	debugLine(path)
	pickleFile = open(picklePath, 'rb')
	print(picklePath)
	Sentimm = pickle.load(pickleFile)
	pickleFile.close()
	for text in sys.argv[2:]:
		print(text)
		#print(Sentimm.classify(text))

main()
