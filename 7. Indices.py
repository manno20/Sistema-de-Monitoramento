import os, time
from datetime import datetime
import mysql.connector
import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta

lista_tabelas = "list_tables.csv"
coluna_saida = "Datetime,Dias_zero_ppt,IMA"

def connect():
        mydb = mysql.connector.connect(host="localhost",user="user",passwd="senha",database="seu-database")
        return mydb

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

def checar_data(path, col,call):
        df = pd.read_csv(path, delimiter=",")
        linhas,colunas = df.shape
        #print(df.shape)
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

def gerar_conteudo():
        nome = "/home/pi/dados/M30_Indices_DiasZeroPpt.csv"
        os.system("rm "+nome)
        a_saida=open(nome,'a')
        #print("\n")
        a_saida.write(coluna_saida+"\n")
        a_saida.close()

def load_csv():
        db = connect()
        mycursor=db.cursor()
        #mycursor.execute("select @last_id := MAX(Datetime) FROM "+name+";")
        mycursor.execute("select Datetime from M30_Indices_DiasZeroPpt ORDER BY Datetime DESC LIMIT 1;")
        resultado =mycursor.fetchall()
        db.commit()
        td = timedelta(days = 150)
        #print(resultado)
        if(resultado == []):
                resultado = "[(datetime.datetime(2000, 1, 1, 0, 0, 0),)]"
        else:
                resultado=str(resultado)

        
        resultado = tratar_resultado(resultado)
        #print(resultado)
        
        os.chdir("/home/pi/dados/")
##        col = col.replace("Datetime,","")
##        column=col.split(",")
        nome_tabela1 = "M30_Micromet1_BdP_amostras.csv"
        col1 = corrige_col("RecNbr,Batt_Volt,Tpainel,Net,Rg_i,Rg_r,PAR_i,PAR_r,PAR_ref,Gsolo,u1,u2,u3,dir_u3,Usolo_1,PA_uS_1,Usolo_2,PA_uS_2,Usolo_3,PA_uS_3,Tar(1),UR(1),Tar(2),UR(2),Tar(3),UR(3),Tsolo_1,Tsolo_2,Tsolo_3,Tsolo_4,Tsolo_5,Ppt_Tot")
        col1 = col1.split(",")
        
        nome_tabela2 = "M30_Micromet2_Amostras.csv"
        col2 = corrige_col("RecNbr,BattV,PTemp_C,WindDir01,WS_ms01,WSDiag01,CSEF,WindDir02,WS_ms02,WSDiag02,CSEF_2,WindDir03,WS_ms03,WSDiag03,CSEF_3,AirTC01,RH01,AirTC02,RH02,AirTC03,RH03,SUp,SDn,LUp,LDn,CNR4TC")
        col2 = col2.split(",")
        
        pd1 = pd.read_csv(nome_tabela1,delimiter=",")
        
        pd1.set_index('Datetime',inplace=True)
        pd1.index = pd.to_datetime(pd1.index)
        
        pd2 = pd.read_csv(nome_tabela2,delimiter=",")
        
        pd2.set_index('Datetime',inplace=True)
        pd2.index = pd.to_datetime(pd2.index)

        df1 = pd.DataFrame(pd1,columns=col1)
        df1 = df1["Ppt_Tot"]
        #print(df1)
        
        df2 = pd.DataFrame(pd2,columns=col2)
        #print(df2)
        
        df3=pd.merge(df2,df1, on='Datetime')
        #print (df3)

        df4=df3.resample(rule='D').sum()
        l,c = df3.shape
        #print(df3["Ppt_Tot"].sum())
        
        cont = 0
        data_inicial=checar_data(nome_tabela1,col1,0)
        data_final=checar_data(nome_tabela1,col1,1)
        #print(data_final,(resultado-td))
        
        gerar_conteudo()
        
        #coluna=corrige_col(coluna_saida).split(","),
        #df_saida = pd.DataFrame(pd_saida,columns=coluna)
        #print(df_saida)

        a_saida=open("M30_Indices_DiasZeroPpt.csv",'a')
        controle = data_inicial
        for i in range(l):
                rh = df3["RH02"][i]
                datatest = (df3.loc[df3["RecNbr"]==df3["RecNbr"][i]].index)
                if(datatest > (resultado-td)):
                        date = (df3.loc[df3["RecNbr"]==df3["RecNbr"][i]].index).date
                        
                        if(df3["Ppt_Tot"][i] < 2.5):
                                data=(df3.loc[df3["RecNbr"]==df3["RecNbr"][i]].index.tolist())
                        else:
                                data=(df3.loc[df3["RecNbr"]==df3["RecNbr"][i]].index.tolist())
                                data_inicial=data[0]

                        dias = ((data[0]-data_inicial).days)
                        ima = dias*(100/rh)
                        #print(data, ima)

                        ppt = (df4.loc[date])
                        ppt.round(2)
                        test = ppt["Ppt_Tot"].tolist()[0]

                        if(controle!=date):
                                if(test < 2.5):
                                        ima = ima
                                elif(test < 5):
                                        ima = ima * 0.7
                                elif(test < 10):
                                        ima = ima * 0.4
                                elif(test < 13):
                                        ima = ima * 0.2
                                else:
                                        ima = ima * 0
                        #print(data,ima) 
                        controle = date
                        a_saida.write(str(data[0])+","+str(dias)+","+str(ima)+"\n")
        a_saida.close()
              
if __name__ == '__main__':
        load_csv()
