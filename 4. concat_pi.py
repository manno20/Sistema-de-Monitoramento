#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, glob, sys, shutil,csv,time
import pandas as pd
from datetime import datetime
from dateutil import parser

lista = "list_tables.csv"

#DEBUG ON = 1, DEBUG OFF = 0
debug = 1

#EDITAR OS CAMINHOS.
caminho_download ='/home/pi/pi/entrada/'
caminho_saida ='/home/pi/pi/saida/'

#Tipo,Datalogger,Tabela,Colunas
def listar():
	os.chdir("/home/pi/lucas")
	df = pd.read_csv(lista, delimiter=",")
	linhas,colunas = df.shape

	for i in range(linhas):
		if(df["Tipo"][i] == "pi" or df["Tipo"][i] == "pi"):
			if(debug):
				print("\n\n[LISTAR] "+str(df["Tipo"][i]))
			concatena(df["Tipo"][i],df["Datalogger"][i],df["Tabela"][i],df["Colunas"][i])

def checa_data(arquivo,colunas):
#	print(arquivo)
	col = colunas.split(",")
	df = pd.read_csv(arquivo, delimiter=",")
	df = df[df.Datetime != "nan"]
	df = df[df.Datetime != "NaN"]
	df = df[df.Datetime != ""]
	df = df.sort_values("Datetime",ascending=True)
	df = df.drop_duplicates().reset_index(drop=True)
	linhas,colunas = df.shape

	if linhas == 0:
		last_date = parser.parse("2001-01-01 00:00")
	else:
		last_date = parser.parse(str(df["Datetime"][linhas-1]))

	if(debug):
		print("\n\n[CHECAR_DATA] LAST DATE:"+str(last_date))

	return last_date

def corrige_linha(line):
	line=line.replace(",,","")
	line=line.replace("#,","")
	line=line.replace("\'","")
	line=line.replace("\"","")
	line=line.replace("NaN","")
	line=line.replace("NAN","")
	line=line.replace("nan","")
	line=line.replace(",b",",")
	line=line.replace(",,","")
	return(line)


def gerar_arq_temp(var,caminho,col):
	var+=".csv"
	os.chdir(caminho_download+"temp/")
	os.system("rm *")
	num_col=col.count(",")
	os.chdir(caminho)
	col=corrige_linha(col)

	for file in glob.glob(var):
		entrada = open(file,'r')
		saida=open(caminho_download+"temp/"+file,'a')
		#saida.write(str(col)+"\n")
		cont=0
		try:
			for line in entrada.readlines():
				line=corrige_linha(line)
				if(cont==0):
					if(debug):
						print("\n\n[GERAR_ARQ_TEMP] LINE: ",line)
					cont=1

				if(line.count(",") == num_col):
					saida.write(line)
		except:
			continue
			if(debug):
				print("\n\n[GERAR_ARQ_TEMP] Erro ao carregar! "+str(file))
		entrada.close()
		saida.close()

#Tipo,Datalogger,Tabela,Colunas
def concatena(tipo,name,tab,col):
	col=corrige_linha(col)

	if(debug):
		print("\n\n[CONCATENA] ABRINDO: "+str(name+"_"+tab))

	colunas = col.split(",")
	nome=name+"_"+tab+".csv"

	os.chdir(caminho_download)

	arquivo = caminho_saida+nome

	if(os.path.isfile(arquivo)):
		os.system("rm "+arquivo)

	if(os.path.isfile(arquivo)):
		dataf = pd.read_csv(arquivo, delimiter=",")
	else:
		saida=open(arquivo,'a')
		saida.write(str(col)+"\n")
		saida.close()
		dataf = pd.read_csv(arquivo, delimiter=",")

	#print("Data saida: ",dataf.shape)

	df = pd.DataFrame(dataf,columns=colunas)
	data = checa_data(arquivo,col)
	data_atual=datetime.now()

	if(debug):
		print("\n\n[CONCATENA] DATA ARQUIVO: ",data)

	gerar_arq_temp("*_"+tab,caminho_download,col)

	#print("Data: "+str(data))

	os.chdir(caminho_download+"temp")
	vetordata = []

	for file in glob.glob("*_"+tab+".csv"):
		vetordata.append(str(file))

	if(debug):
		print("[CONCATENA] VETOR DATA:",sorted(vetordata))

	for i in sorted(vetordata):
#		try:
		dataf1 = pd.read_csv(i, delimiter=",")
		#l,c = dataf1.shape
		if(debug):
			print("[CONCATENA] DATAF1:\n",dataf1)

		#print("Dataframe entrada: ",dataf1.shape)
		df1 = pd.DataFrame(dataf1,columns=colunas)
		df=df.append(df1)

		if(debug):
			print("[CONCATENA] DF:\n",df)
			#print("Testando: "+str(df1))
#		except:
#			continue
#			print("Erro ao concatenar: "+str(i))

	df = df[df.Datetime != "Datetime"]

	df = df.drop_duplicates("Datetime").reset_index(drop=True)
	df = df.sort_values("Datetime",ascending=True)
	df.to_csv(arquivo)
	print("Sucesso ao gravar: "+str(name+"_"+tab))

listar()
