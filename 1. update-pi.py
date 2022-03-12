#!/usr/bin/env python

import os, glob, sys, time, subprocess
import pandas as pd

lista = "list_update.csv"

def lerlista():
        os.chdir("/home/gpfma/scripts/")
        df = pd.read_csv(lista, delimiter=",")
        linhas,colunas = df.shape
        for i in range(linhas):
                if(df["Tipo"][i] == "pi"):
                        update(df["Porta"][i])

def update(porta):
        print("\n\n\nAtualizando RASPBERRY da porta "+str(porta))
        try:
                none=os.popen("rsync -rvz -e 'ssh -p '"+str(porta)+" pi@*ip*/home/pi/dados/* /home/pi/download/pi/download/").read()
                none=os.popen("rsync -rvz -e 'ssh -p '"+str(porta)+" pi@*ip*:/home/pi/scripts/* /home/pi/download/pi/scripts/").read()
        except:
                print("Sem conexao com a Torre!")

lerlista()

