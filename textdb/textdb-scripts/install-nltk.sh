unameOut="$(uname -s)"
case "${unameOut}" in
	Linux*) 
		machine=Linux
		OS=$(lsb_release -si)
		VER=$(lsb_release -sr)
		if [ $OS = "Ubuntu" ]; then
			machine="UBUNTU"
			sudo apt-get install python3
			sudo pip3 install nltk
		fi
		;;
	Darwin*)
		machine=Mac
		sudo pip3 install nltk
		;;
esac
echo ${machine}

#sudo apt-get install python3
#sudo apt-get install python3-setuptools
#sudo easy_install3 pip
#export LC_ALL=C
#pip3 install -U nltk

