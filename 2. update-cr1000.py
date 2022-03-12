#!/usr/bin/env python

import os, glob, sys, time, os.path, subprocess
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
from pycampbellcr1000 import CR1000

lista_update = "list_update.csv"
lista_tabelas = "list_tables.csv"

#baixa os dados conforme as datas selecionadas
def download_cr1000(porta,path,tab,col,data_i,data_f,nome):
        link = "tcp:*ip*:"+str(porta)
        print(str(data_i),str(data_f))
        colunas = col.split(",")
        temp = '/home/pi/download/cr1000/temp/Temp_'+nome+"_"+tab+'.csv'
#       try:
        if(os.path.isfile(temp)):
                os.system('rm '+temp)

        #comando = 'pycr1000 getdata tcp:*ip*:'+str(porta)+':1112 \''+str(tab)+'\' \--start \"'+str(data_i)[0:16]+'\" --stop \"'+str(data_f)[0:16]+'\" '+temp
        comando = 'pycr1000 getdata tcp:*ip*:'+str(porta)+':1112 \''+str(tab)+'\' \--start \"'+str(data_i)[0:16]+'\" '+temp
        #print(comando)
        try:
                #device = CR1000.from_url(link)
                #print(device.list_tables())
                #dados=device.get_data(tab,data_i,data_f)
                #print(dados)
                #dados.to_csv(temp)
                #os.system(comando)
                none=os.popen(comando).read()
                print(none)
        except:
                print("Falha ao baixar arquivos!")

        try:
                pd1 = pd.read_csv(path, delimiter=",")
                #print(pd1)
                pd2 = pd.read_csv(temp, delimiter=",")
                #print(pd2)
                linha, coluna = pd2.shape

                if(linha > 0):
                        df1 = pd.DataFrame(pd1,columns=colunas)
                        df2 = pd.DataFrame(pd2,columns=colunas)

                        df3 = pd.concat([df1,df2])
                        df3 = df3.drop_duplicates().reset_index(drop=True)
                        
                        df3 = df3.sort_values(colunas[0],ascending=True)
                        df3.to_csv(path)
                        #print(df3.shape)
                        del pd1,pd2,df1,df2,df3
                else:
                        print("Falha no download!")
        except:
               print(temp+" Nao existe!")
#       try:
#       except:
#               print(" Erro")

#checa a ultima data e que a tabela foi atualizada
def checar_data(path,col):
        df = pd.read_csv(path, delimiter=",")
        linhas,colunas = df.shape
        #print(df.shape)
        last_date = parser.parse(str(df["Datetime"][linhas-1]))
        del df
        return last_date

#lista_update
#Tipo,Datalogger,Porta,Tabela
def lerlista():
        os.chdir("/home/pi/scripts/")
        df = pd.read_csv(lista_update, delimiter=",")
        linhas,colunas = df.shape
        #print(df.shape)
        for i in range(linhas):
                if(df["Tipo"][i] == "cr1000"):
                        check_tables(df["Nome"][i],df["Porta"][i],df["Tabela"][i])
                elif(df["Tipo"][i] == "picr1000"):
                        copy_pi_cr1000(df["Nome"][i]+"_"+df["Tabela"][i])
        del df

#lista_tabelas
#Tipo,Datalogger,Tabela,Colunas
def update(porta,path,n,tab):
        #print(path)
        nome=n+"_"+tab+".csv"
        os.chdir("/home/pi/scripts/")
        data_atual = datetime.now()
        df = pd.read_csv(lista_tabelas, delimiter=",")
        linhas,colunas = df.shape
        for i in range(linhas):
                test=str(df["Datalogger"][i])+"_"+str(df["Tabela"][i])+".csv"
#                print("\n"+test, nome)
                if(test==nome):
                        #print(df["Datalogger"][i]+" encontrado!")
                        data = checar_data(path,df["Colunas"][i])
                        td = timedelta(days=3)
                        if ((data_atual-data).days > 1):
                                data_download = data + td
                                download_cr1000(porta,path,tab,df["Colunas"][i],data,data_atual,df["Datalogger"][i])
                        else:
                                download_cr1000(porta,path,tab,df["Colunas"][i],data,data+td,df["Datalogger"][i])
        del df

#Checa a existencia das tabelas no sistema, caso existam define um tipo de download, caso nao existam, outro tipo de download
def check_tables(nome,porta,tabela):
        print("\n\n\nConectando ao CR1000 "+str(nome))
        link = "tcp:*ip*:"+str(porta)
        #try:
                #device = CR1000.from_url(link)
                #print(device.list_tables())
                #flag = 1
        #except:
                #print("Nao foi possivel conectar ao Datalogger "+nome)
                #flag = 0

#        if flag == 1:
        caminho = "/home/pi/download/cr1000/"+nome+"_"+tabela+".csv"

        if(os.path.isfile(caminho)):
                #print("\n\n\nConectando ao CR1000_"+nome+"_"+tabela)
                update(porta,caminho,nome,tabela)
        else:
                try:
                        print("Tentando criar tabela e baixar os primeiros dados!")
                        comando=('pycr1000 update '+str(link)+' \''+str(tabela)+'\' '+str(caminho))
                        #print(comando)
                        try:
                                #none=os.system(comando)
                                none=os.popen(comando).read()
                                #print(none)
                        except:
                                print("Sem conexao com o Pantanal!")
                except:
                        #Enviar alerta de falha de conexao
                        print("Falha ao atualizar o CR1000 "+nome)

#Tipo,Datalogger,Tabela,Colunas
def copy_pi_cr1000(nome):
        print("\n\nCopiando dados do RPi-CR1000: "+nome+"\n\n")
        os.chdir("/home/pi/scripts/")
        df = pd.read_csv(lista_tabelas, delimiter=",")
        linhas,colunas = df.shape
        #print(df.shape)
        for i in range(linhas):
                if(df["Tipo"][i] == "picr1000"):
                        if(df["Datalogger"][i] == "Micromet1"):
                                col = str(df["Colunas"][i])
                                colunas = col.split(",")
                                #print(colunas)
                                pd5 = pd.read_csv("/home/pi/download/pi/download/BdP_amostras.csv", delimiter=",")
                                df5 = pd.DataFrame(pd5, columns=colunas)
                                os.system("rm /home/pi/download/cr1000/Pi_Micromet1_BdP_amostras.csv")
                                df5.to_csv("/home/pi/download/cr1000/Pi_Micromet1_BdP_amostras.csv")

                        elif(df["Datalogger"][i] == "Termoflux"):
                                col = str(df["Colunas"][i])
                                colunas = col.split(",")
                                #print(colunas)
                                pd5 = pd.read_csv("/home/pi/download/pi/download/solosamplebase.csv", delimiter=",")
                                #print(pd5)
                                df5 = pd.DataFrame(pd5, columns=colunas)
                                os.system("rm /home/pi/download/cr1000/Termoflux_Amostras.csv")
                                df5.to_csv("/home/pi/download/cr1000/Termoflux_Amostras.csv")

        #os.system("cp solosamplebase.csv /home/pi/download/cr1000/Termoflux_Amostras.csv")

lerlista()
