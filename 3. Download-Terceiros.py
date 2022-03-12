from os import listdir
from os.path import isfile, join
import wget, requests, os, time
from datetime import date
from datetime import timedelta
from bs4 import BeautifulSoup

path = "/home/pi/Dados_Brutos/"

user = 'usuario'
password = 'senha'

dataatual = date.today()
dias = timedelta(days = 1)
datainicial = dataatual-dias

def returnFiles(tipo,link):
	teste=[]
#	print(link)
        r = requests.get(link, auth=(user, password))
	soup = BeautifulSoup(r.content, "html.parser")
	for a in soup.find_all('a', href=True):
	    if(str(a).find(tipo)>0):
		if(len(str(a['href']))>25):
	            teste.append(str(a['href']))
	return(teste)

def genname(x,cl):
    return cl+"/"+str(x).replace('-','_')+".txt"

def downloadname(x):
    return str(x).replace('-','_')+".txt"

def download_direto(arquivos):
    for filename in arquivos:
#	print(path+filename)
        r = requests.get(url+filename, auth=(user, password))
        open(path+"/"+filename, 'wb').write(r.content)
        print("Concluido: "+str(filename))

def daterange(start_date, end_date):
    today = date.today()
    print("\n\nAtualizando Dados ")

    for n in range(int((end_date - start_date).days)):
			download_direto(de_hoje)

main:
	for i in range(len(Cliente)):

   	url  = 'link'
    	caminho = path+str(Cliente[i])

    	if(os.path.exists(caminho)==False):
        	os.mkdir(caminho)

    	daterange(datainicial,dataatual+timedelta(days=1))
