import os, glob, sys, time, os.path
from datetime import datetime, timedelta

while 1:
        hora = datetime.now()
        print(str(hora)[0:16]+"\n")
	if(hora.minute%5 == 0):
               try:
		       os.system("clear")
                       os.chdir("/home/pi/scripts/")

#		       print("\n\nAtualizando Pi")
		       os.system("python update_pi.py")
                       os.chdir("/home/pi/scripts/")

#		       print("\n\nAtualizando CR1000")
                       os.system("python update_cr1000.py")
                       os.chdir("/home/pi/scripts/")

#		       print("\n\nConcatenando Pi")
		       os.system("python concatena_pi.py")
                       os.chdir("/home/pi/scripts/")

#		       print("\n\n Organizando tabelas")
		       os.system("python organiza_tabelas.py")
                       os.chdir("/home/pi/scripts/")

#		       print("Realizando Medias")
		       os.system("python media_30.py")
		       os.system("python media_diaria.py &")
		       os.system("python Indices.py")
                       os.chdir("/home/pi/scripts/")

#		       print("Importando para o DB")
		       os.system("python importar_db.py")
                       os.chdir("/home/pi/scripts/")

               except:

                       print("Erro ao atualizar CR1000!")
	time.sleep(30)
