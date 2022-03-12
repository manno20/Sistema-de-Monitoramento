#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, time
from datetime import datetime
import mysql.connector
import pandas as pd
from dateutil import parser

#VARIAVEL DE DEBUG (PRINTAR O PASSO A PASSO DA EXECUÇÃO NA TELA)
#1 = ON / 0 = OFF
debug = 1
#PARA DEBUGAR MAIS A FUNDO
debugultra = 0

lista_tabelas = "list_tables.csv"

def connect():
	mydb = mysql.connector.connect(host="localhost",user="usuario",passwd="password",database="seu-database")
	return mydb

def corrige_querry(col):
	nulo = 'null'
	col = col.replace('\'nan\'','null')
	col = col.replace('\' \'','null')
	return col

def checar_data(path, col,call):
        df = pd.read_csv(path, delimiter=",")
        linhas,colunas = df.shape
        try:
                if(call == 0):
                        date = parser.parse(str(df["Datetime"][0])[:19])
                else:
                        date = parser.parse(str(df["Datetime"][linhas-1])[:19])
        except:
                date = parser.parse("2000-01-01 00:00")
        del df
        return date

def corrige_col(col):
        col = col.replace("(","_")
        col = col.replace(")","")
        col = col.replace("#","")
        col = col.replace("%","")
        col = col.replace("*","")
        col = col.replace("/","_")
        col = col.replace(" ","_")
        col = col.replace("__","_")
        col = col.replace("___","_")
        col = col.replace(",_",",")
        col = col.replace("_,",",")
        return col

def load_csv(tag,df,index):
	querry=[]
	col = corrige_col(str(df["Colunas"][index]))
	column=col.split(",")
	#nome_tabela = df["Datalogger"][index]+"_"+df["Tabela"][index]
	nome_tabela = tag+df["Datalogger"][index]+"_"+df["Tabela"][index]

	if(debug):
		print("[LOAD_CSV] Criando Querry para: "+str(nome_tabela))

	querry.append("CREATE TABLE "+nome_tabela+" (")
	for i in range(len(column)):
		if(str(column[i])=="Datetime"):
			querry.append(column[i]+" DATETIME(0) NOT NULL PRIMARY KEY")
		elif(str(column[i][:4])=="Date"):
			querry.append(column[i]+" VARCHAR(255)")
		elif(str(column[i][:4])=="Time"):
			querry.append(column[i]+" VARCHAR(255)")
		elif(str(column[i][:4])=="Type"):
			querry.append(column[i]+" VARCHAR(255)")
		elif(str(column[i][:4])=="Unit"):
			querry.append(column[i]+" VARCHAR(255)")
		else:
			querry.append(column[i]+" DECIMAL(20,3)")
		if(i != len(column)-1):
			querry.append(", ")
	querry.append(")")
	querry="".join(querry)
	#print(querry)
	return(nome_tabela,str(querry),col)


def create_table(name,qr):
	db = connect()
	mycursor=db.cursor()
	try:
		if(debug):
			print("\n\n[CREATE_TABLE] CRIANDO TABELA: "+str(name))
		mycursor.execute(qr)
		mycursor.close()
		db.close()
	except:
		if(debug):
			print("\n\n[CREATE TABLE] TABELA JA EXISTE: "+str(name))
		mycursor.close()
		db.close()

def insert(querry):

	if(debugultra):
		print("\n\n[INSERT] INSERINDO: "+str(querry))

	db = connect()
	mycursor=db.cursor()
	try:
		mycursor.execute(corrige_querry(querry))
		db.commit()

		if(debugultra):
			print("\n",mycursor.rowcount, "was inserted.")

		mycursor.close()
		db.close()
	except:
		if(debugultra):
			print("\nErro na inserção: ")
		mycursor.close()
		db.close()

def tratar_resultado(res):
        #[('2019-06-22 18:06:07',)]
        try:
                res = datetime.strptime(res, '[(\'%Y-%m-%d %H:%M:%S\',)]')
        except:
                try:
                        res = datetime.strptime(res, '[(datetime.datetime(%Y, %m, %d, %H, %M, %S),)]')
                except:
                        try:
                                res = datetime.strptime(res, '[(datetime.datetime(%Y, %m, %d, %H, %M),)]')
                        except:
                                try:
                                        res = datetime.strptime(res, '%Y-%m-%d %H:%M')
                                except:
                                        try:
                                                res = datetime.strptime(res, '%Y-%m-%d')
                                        except:
                                                print(" Erro!!! ")
        return res

def insert_values(name,col):
	if(debug):
        	print("\n\n [INSERT_VALUES] GERANDO INSERÇÃO PARA ",name,"\n\n")

	os.chdir('/home/pi/joaobasso/saida/')
	if(os.path.isfile(name+".csv")):
		df = pd.read_csv(name+".csv", delimiter=',')
		coluna=col.split(",")
		to_read = pd.DataFrame(df,columns=coluna)
		l,c=to_read.shape
		db = connect()
		mycursor=db.cursor()
		#mycursor.execute("select @last_id := MAX(Datetime) FROM "+name+";")
		mycursor.execute("select Datetime from "+name+" ORDER BY Datetime DESC LIMIT 1;")
		resultado =mycursor.fetchall()
		db.commit()
		#print(resultado)
		if(resultado == []):
			resultado = "[(datetime.datetime(2000, 1, 1, 0, 0, 0),)]"
		else:
			resultado=str(resultado)

		#print(resultado)
		resultado = tratar_resultado(resultado)
		data2=checar_data(name+".csv",col,1)
		#print(str(data2)+"\t"+str(resultado))
		if(data2 > resultado):
			for i in range(l):
				try:
					data1= datetime.strptime(str(to_read["Datetime"][i])[:19], '%Y-%m-%d %H:%M:%S')
				except:
					try:
						data1= datetime.strptime(str(to_read["Datetime"][i])[:19], '%Y-%m-%d %H:%M')
					except:
						data1= datetime.strptime(str(to_read["Datetime"][i])[:19], '%Y-%m-%d')
				#print (data1,resultado)
				if(data1 > resultado):
					#print(data1)
					sql = "INSERT INTO "+name+" ("+col+") VALUES "
					querry=" ("
					cont = 0
					for j in to_read:
						if(cont==0):
							querry=querry+"'"+str(to_read[j][i])+"'"
							cont+=1
						else:
							querry=querry+",'"+str(to_read[j][i])+"'"
					querry = querry+"),"
					sql+=querry
					sql=sql[:-1]
					#print("\n"+sql+"\n")
					insert(sql)
	else:
		print("[INSERT_VALUES] ARQUIVO NÃO EXISTE")

def gerente(tag):
	#EDITAR LOCAL
	os.chdir("/home/pi/lucas")
	df = pd.read_csv(lista_tabelas, delimiter=",")
	linhas,colunas = df.shape
	for i in range(linhas):
		name,qr,col = load_csv(tag,df,i)
		#try:
		if(debug):
			print("\nInserindo no Banco de dados: "+name+"\n")
		#print("\n"+tag+name)
		#print("\n"+qr+"\n")
		create_table(name,qr)
		#except:
		#print("Erro ao criar")
		#try:
		insert_values(name,col)
		#except:
		#print("Erro ao inserir valores no Banco de dados")

if __name__ == '__main__':
        teste = datetime.now()
        gerente("")
        print("Concluido. A inserção levou "+str((datetime.now()-teste).seconds)+" segundos!")
