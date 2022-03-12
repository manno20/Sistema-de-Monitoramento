import os, glob, csv, time
from datetime import datetime
import mysql.connector
from dateutil import parser

last=""
debug = 0
Clientes=[]
Frotas = []

def updateTableEstacao(db,table,data,battery,chuva,airtemp,airhumid,pressure,ws,wg,WD,soiltemp,solarrad):
#	print(data)
	dbcur = db.cursor()
#	print("UPDATE {0} SET {1} = '{2}' WHERE DataHora = '{3}';".format(table,coluna,valor,data))
	dbcur.execute("UPDATE {0} SET Battery = {2}, RainCounter= {3}, Temperature= {4}, Humidity = {5}, Pressure = {6}, WindSpeed = {7}, WindGust = {8}, WindDirection = {9}, SoilTemperature = {10}, SolarRadiation = {11}, FILLED = 1 WHERE DataHora = '{1}';".format(table,str(data).rstrip("\n"),battery,chuva,airtemp,airhumid,pressure,ws,wg,WD,soiltemp,solarrad))
	db.commit()
#	print("{0} Updated on {1}".format(table,data))

#nesse caso as estações salvam apenas o numero total de basculadas, então criei um campo que representa o valor em mm
def updatePPT(db,table,data,ppt):
#	print("{0} {1}".format(data, ppt))
	dbcur = db.cursor()
#	print("UPDATE {0} SET {1} = '{2}' WHERE DataHora = '{3}';".format(table,coluna,valor,data))
	dbcur.execute("UPDATE {0} SET PPT = {2}, RainFILLED = 1 WHERE DataHora = '{1}';".format(table,str(data).rstrip("\n"),ppt))
	db.commit()

def Decode(db,tabela,data,frame):
	chuva = int(frame[1])
	alarme=int(frame[2])
	airhumid = int(frame[3])
	ws = int(frame[4])
	airtemp = int(frame[5])
	solarrad = int(frame[6])
	pressure = int(frame[7])
	soiltemp = int(frame[8])
	battery = int(frame[9])
	WD = int(frame[10])

	updateTableEstacao(db,tabela,data,battery,chuva,airtemp,airhumid,pressure,ws,wg,WD,soiltemp,solarrad,sensor0,sensor1,sensor2,sensor3,type,alarme)



def ChecarColunanaTabela(db,tablename,coluna):
	dbcur = db.cursor()

	dbcur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{0}' AND COLUMN_NAME = '{1}'".format(tablename,coluna))

	if dbcur.fetchone()[0] == 1:
        	dbcur.close()
        	return True

	dbcur.close()
	return False

def ChecarTabelanoDB(db,tablename):
	dbcur = db.cursor()
	dbcur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'".format(tablename.replace('\'', '\'\'')))

	if dbcur.fetchone()[0] == 1:
        	dbcur.close()
        	return True

	return False

def createtriggerdb(db,name):
	mycursor=db.cursor()
	mycursor.execute("CREATE TRIGGER tr{0} AFTER UPDATE ON {0} FOR EACH ROW SET @Id:=@Id+1; ALTER TABLE {0} ADD COLUMN New_Id INT NOT NULL AFTER Id; SET @Id=1; UPDATE {0} SET New_Id=@Id ORDER BY DataHora;".format(name), multi=True)
	print("Trigger criado!")
	mycursor.close()
	db.close()

def addColumn(db,table,column,type):
	mycursor=db.cursor()
	try:
		mycursor.execute("ALTER TABLE {0} ADD {1} {2}".format(table,column,type))
		mycursor.close()
		return True
	except:
		#print("\n\n")
		mycursor.close()
		return False

def connect():
        mydb = mysql.connector.connect(host="localhost",user="user",passwd="pass",database="seu-db")
        return mydb

def FillTable(db,tabela):
	print("Working on "+str(tabela))
	mycursor=db.cursor()

	if(ChecarColunanaTabela(db,tabela,"FILLED")==False):
		addColumn(db,tabela,"FILLED","BOOLEAN NOT NULL DEFAULT FALSE")

	if(ChecarColunanaTabela(db,tabela,"RainFILLED")==False):
		addColumn(db,tabela,"RainFILLED","BOOLEAN NOT NULL DEFAULT FALSE")

	if(ChecarColunanaTabela(db,tabela,"PPT")==False):
		addColumn(db,tabela,"PPT","DECIMAL(6,2)")

#	createtriggerdb(tabela)

	qr = "SELECT DataHora, FROM "+ tabela +" WHERE FILLED = 0;"
	mycursor.execute(qr)

	for j in mycursor.fetchall():
		data = j[0]
		fc = j[1]
		pl = j[2]
#		print("{0} {1}".format(tabela, data))
		Decode(db,tabela,data,fc,pl)
	mycursor.close()

def Fill_Rain(db,tabela):
	print("Working on "+str(tabela))
	mycursor = db.cursor()

	qr = "SELECT DataHora,RainCounter,RainFILLED FROM "+ tabela +";"
	mycursor.execute(qr)
	last_rc = 0
	for j in mycursor.fetchall():
		if(j[2] == '3' or j[2] == '4'):
			if(j[3]==1):
				last_rc = j[1]
			else:
				#print(j)
				data = j[0]
				rc = j[1]
				if(last_rc == 0):
					ppt = 0;
				else:
					ppt = rc - last_rc
#				print(data,ppt)
				if(ppt<0 or ppt > 500):
					ppt = 0
				last_rc = rc
				updatePPT(db,tabela,data,ppt)
	mycursor.close()

db = connect()
for i in range(len(Clientes)):
	fr = str(Frotas[i]).split(",")
	for j in fr:
		j = str(j).replace("[","").replace("]","").replace(" ","")
		tabela = str(Clientes[i])+"_"+str(j)
		if(ChecarTabelanoDB(db,tabela)):
			FillTable(db,tabela)
			Fill_Rain(db,tabela)
db.close()
