#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, glob, sys, shutil,csv,time
import pandas as pd
from datetime import datetime
from dateutil import parser

lista = "list_tables.csv"
caminho_download ='/home/pi/download/'
caminho_saida ='/home/pi/dados/'

#Tipo,Datalogger,Tabela,Colunas
def ajustarPI(tipo,name,tab,col):
        nome=name+"_"+tab
#        print("\n\n"+tab)
        os.chdir(caminho_download+tipo)
        arquivo_entrada = caminho_download+tipo+"/"+nome+".csv"
        arquivo_saida = caminho_saida+nome+".csv"
        if(os.path.isfile(arquivo_entrada)):
                if(os.path.isfile(arquivo_saida)):
                        #print(name+"_"+tab+".csv EXISTE NOS DADOS")
                        gerar_conteudo(arquivo_entrada,arquivo_saida,nome+".csv",col)
                else:
                        criar_saida(arquivo_saida,nome+".csv",col)
        
#Tipo,Datalogger,Tabela,Colunas
def ajustarCR1000(tipo,name,tab,col):
        nome=name+"_"+tab
#        print("\n\n"+tab)
        os.chdir(caminho_download+tipo)
        arquivo_entrada = caminho_download+tipo+"/"+nome+".csv"
        arquivo_saida = caminho_saida+nome+".csv"

        if(os.path.isfile(arquivo_entrada)):
                if(os.path.isfile(arquivo_saida)):
                        #print(name+"_"+tab+".csv EXISTE NOS DADOS")
                        gerar_conteudo(arquivo_entrada,arquivo_saida,nome+".csv",col)
                else:
                        criar_saida(arquivo_saida,nome+".csv",col)

def checar_data(path, col):
        df = pd.read_csv(path, delimiter=",")
        linhas,colunas = df.shape
        #print(df.shape)
        last_date = parser.parse(str(df["Datetime"][linhas-1]))
        del df
        return last_date

def gerar_conteudo(entrada,saida,nome,col):
        coluna = corrige_col(col)
        #print("Salvando arquivos: "+entrada)
        a_entrada=open(entrada,'r')
        #print(saida)
        a_saida=open(saida,'w')
        #print("\n")
        #data = checar_data(a_saida,coluna)
        cont=0
        for line in a_entrada.readlines():
                line=corrige_linha(line)
                #print(line)
                if(cont==0):
                        a_saida.write(coluna)
                else:
                        l=line.split(",")
                        #print(l[1])
                        if(l[1]!=""):
                                #print(line.count(","),coluna.count(","))
                                if(line.count(",")==coluna.count(",")):
                                        a_saida.write(line)
                                else:
                                        continue
                                        #print("Erro ao salvar: "+nome)
                cont+=1
        a_entrada.close()
        a_saida.close()

def corrige_linha(col):
        col = col.replace("-8190.00","")
        col = col.replace("-8190","")
        col = col.replace("nan","")
        col = col.replace("(","_")
        col = col.replace(")","")
        col = col.replace("#","")
        col = col.replace("*","")
        col = col.replace("%","")
        col = col.replace("/","_")
        col = col.replace("__","_")
        col = col.replace("___","_")
        col = col.replace(",_",",")
        col = col.replace("_,",",")
        return col
                        
def corrige_col(col):
        col = col.replace("(","_")
        col = col.replace(")","")
        col = col.replace("#","")
        col = col.replace("*","")
        col = col.replace("%","")
        col = col.replace("/","_")
        col = col.replace(" ","_")
        col = col.replace("__","_")
        col = col.replace("___","_")
        col = col.replace(",_",",")
        col = col.replace("_,",",")
        return ","+col+"\n"

def criar_saida(saida,nome,col):
        coluna = corrige_col(col)
        #print(col+"\n")
        if(col.count(",")==coluna.count(",")-1):
                saida = open(saida,'a')
                saida.write(str(coluna)+"\n")
                saida.close()
                
def listar():
        os.chdir("/home/pi/scripts/")
        df = pd.read_csv(lista, delimiter=",")
        linhas,colunas = df.shape
        #print(linhas,colunas)
        
        for i in range(linhas):
                if(df["Tipo"][i] == "pi"):
                        ajustarPI(df["Tipo"][i],df["Datalogger"][i],df["Tabela"][i],df["Colunas"][i])
                elif(df["Tipo"][i] == "cr1000"):
                        ajustarCR1000(df["Tipo"][i],df["Datalogger"][i],df["Tabela"][i],df["Colunas"][i])
                elif(df["Tipo"][i] == "picr1000"):
                        ajustarCR1000("cr1000",df["Datalogger"][i],df["Tabela"][i],df["Colunas"][i])

os.chdir("/home/pi/dados/")
try:
       none=os.popen("rm *").read()
except:
        none = 0
listar()
listar()
