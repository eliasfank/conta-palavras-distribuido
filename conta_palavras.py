import socket
import sys
import os
import time
import thread

import re
from math import ceil as ceil
from multiprocessing.pool import ThreadPool
import multiprocessing

def sendFile(host, port, fp, palavras, s):
	SOMA = {}
	for w in palavras.split(','):
		SOMA[w] = 0
	###Function that manages the sending of the file to the server
	m = s.recv(1024)
	if (m=="READY"):
		#print "Sending file to "+host+":"+str(port)
		try:
			f = open(fp, 'rb')
			d = f.read(4096)
			s.send(d)
			while d != "":
				d = f.read(4096)
				s.send(d)
			#Get a time for the socket not concatenate the last send with the next
			#and signal the server that the file has reached the end
			time.sleep(0.1)
			s.send("--END--")
			f.close()
			l = s.recv(1024)
			#If the file has been uploaded successfully
			#Get the total value from the products of the uploaded file
			if (l=="SUCCESS_FILE"): 
				#print "Succesfully uploaded file."
				s.send(palavras)
				totalValue = s.recv(1024)
				#print "\nTotal de palavras contadas pelo "+host+':'+port+':'
				#print totalValue, '\n'
				totalValue = totalValue.split(",")
				for i, w in enumerate(palavras.split(',')):
					SOMA[w] += int(totalValue[i])
			else:
				print "Failed to upload file. Try again?"
			s.close()
		except Exception as msg:
			print("Error message: "+str(msg))
			return False
		return SOMA
	elif (m=="ERROR"):
		print "Error: An unexpected error has occoured at the server side. Try again?"
		return False
	else:
		print "Error: Didn't expect this message: "+m
		return False

def conectado(con, cliente):
    ###Function that starts a new thread for the connection
    msg = con.recv(1024)
    if (msg=="GETFILE"):
        print("Connection started with "+str(cliente))
        getFile(con)
    else:
        con.close()
    thread.exit()

def getFile(con):
	###Function that get the client file

	###generate a unique name to the file
	##The file will be save on RecievedFiles/
	fileName = str(con.getsockname()[1])+'File.txt' 
	try:
		###Save the file on the server directory
		#######################################################
		file = open(fileName, 'wb')
		con.send("READY")
		print "Downloading file..."
		while True:
			d = con.recv(4096)
			if (d=="--END--"):
				file.close()
				break
			file.write(d)
		con.send("SUCCESS_FILE")
		print "Succesfully downloaded file as "+fileName 
		words = con.recv(1024).split(',')
		#######################################################

		###Calculate the total value from the products and send
		##the value to the client##############################
		contagem = []
		for w in words:
			contagem.append(contaPalavras(fileName, w, LOCAL_THREADS))
		con.send(''.join([str(x)+',' if i != len(contagem)-1 else str(x) for i, x in enumerate(contagem)]))
		#######################################################

		###After it all, close the connection with the client
		con.close()

	except Exception as msg:
		con.send("ERROR")
		#File Error.
		print("Error message: "+str(msg))
	return

def enviarArquivoParaContar(host, filePath, palavras, result):
	x = host.split(":")
	host = x[0]
	port = x[1]
	#Create a socket that use IPV4 and TCP protocol
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	#If the file exists
	#Start the connection with the server
	if (os.path.exists(filePath)):
		try:
			s.connect((host, int(port)))
			#print "Connected to server!"
		except socket.error as sem:
			print "ERROR: Couldn't connect."
			print sem 
			sys.exit()

		##Send a message that signals the start of the file upload
		s.send("GETFILE")
		result.put({host: sendFile(host, port, filePath, palavras, s)})

	else:
		print("File does not exists.")
		sys.exit()

def n_parts(string, n):
	if n == 0: 
		return []
	avg = len(string) / float(n)
	avg = int(avg)
	parte = string[:avg]	
	essa_parte = []
	if parte[-1] not in sep:
		index = string[avg:].index(' ') if ' ' in string[avg:] else 0
		parte = string[:avg+index]
		#partes_string.append(string[:avg+index])
		essa_parte.append(parte)
		essa_parte += n_parts(string[avg+index:], n-1)
	else:
		essa_parte.append(parte)
		essa_parte += n_parts(string[avg:], n-1)
	return essa_parte

def count_words(string, word, result):
	result.put(sum(1 for _ in re.finditer(r'\b%s\b' % re.escape(word), string)))
	return

def contaPalavras(fileName, word, n_threads):
	input_string = open(fileName, 'r').read()

	partes_cores = n_parts(input_string, n_threads)
	
	total_count = []	
	thread_list = []
	result_queue = multiprocessing.Queue()

	for i in range(0, n_threads):
		t = multiprocessing.Process(target=count_words, args=(partes_cores[i], word, result_queue))
		thread_list.append(t)

	for thread in thread_list:
		thread.start()

	for thread in thread_list:
		thread.join()
		total_count.append(result_queue.get())
		#print "Thread:", total_count[-1]

	return sum(total_count)

def contarNoCliente(fileName, palavras, result):
	retorno = {}
	for w in palavras.split(','):
		retorno[w] = contaPalavras(fileName, w, LOCAL_THREADS)
	result.put({'127.0.0.1:8008': retorno})

def quebraArquivoEmPartes(arquivo, qt_partes):
	input_string = open(arquivo, 'r').read()
	partes_arquivo = n_parts(input_string, qt_partes)
	for i, p in enumerate(partes_arquivo):
		text_file = open("parte_"+str(i)+"_de_"+str(qt_partes)+"_do_"+arquivo, "w")
		text_file.write(p)
		text_file.close()

def usage():
	print("Como usar:")
	print("python "+__file__+" <caminho para o arquivo>"+" <modo>")
	sys.exit()


try:
	modo = sys.argv[1]
except:
	usage()

LOCAL_THREADS = 7
sep = [' ','-','.',',','\n','\r']
PALAVRAS = 'and,or,house,yes,no,good,bad,by'
enderecos = ['127.0.0.1:8008', '127.0.0.1:8008', '127.0.0.1:8008', '127.0.0.1:8008', '127.0.0.1:8008', '127.0.0.1:8008', '127.0.0.1:8008', '127.0.0.1:8008']
arquivos = ['book.in']
numeros_maquinas = [0,4,8]
qtWords = [1,2,4,8]

if modo == "server":
	###Create a socket that use IPV4 and TCP protocol
	###Bind the port for this process conections
	###Set the maximun number of queued connections
	tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	orig = ('', 8008)
	try:
	    tcp.bind(orig)
	    print "Bind succesfull!"
	    print "Porta: ", tcp.getsockname()[1]
	except socket.error as SBE:
	    print "Bind failed!"
	    print SBE
	    sys.exit()
	tcp.listen(5)

	print "TCP start."
	print "Listening..."

	###Server accept connections until a keyboard interrupt
	###If there is a keyboard interrupt, release the port
	try:
	    while True:
	        con, cliente = tcp.accept()
	        ##A thread will be create for each connection
	        #so, more than one client can be attended
	        thread.start_new_thread(conectado, tuple([con, cliente]))
	except KeyboardInterrupt:
	    print "Stop listening and TCP closed."
	    tcp.close()
	    sys.exit()  
if modo == 'cliente':
	for arquivo in arquivos:
		for qtw in qtWords:
			subsetPALAVRAS = ''.join([x+',' if i != len(PALAVRAS.split(',')[:qtw])-1 else x for i, x in enumerate(PALAVRAS.split(',')[:qtw])])
			for x in numeros_maquinas:
				SOMA_MESTRE = {}
				SOMA_FINAL = {}
				for w in subsetPALAVRAS.split(','):
					SOMA_FINAL[w] = 0
				enders = enderecos[:x]
				quebraArquivoEmPartes(arquivo, x+1)
				ini = time.time()
				thread_list = []
				result_queue = multiprocessing.Queue()
				result_queue_client = multiprocessing.Queue()
				for i in range(0, len(enders)):
					t = multiprocessing.Process(target=enviarArquivoParaContar, args=(enders[i], "parte_"+str(i+1)+"_de_"+str(x+1)+"_do_"+arquivo, subsetPALAVRAS,result_queue))
					thread_list.append(t)

				for thread in thread_list:
					thread.start()
				
				client_thread = multiprocessing.Process(target=contarNoCliente, args=("parte_0_de_"+str(x+1)+"_do_"+arquivo, subsetPALAVRAS, result_queue_client))
				client_thread.start()

				for i, thread in enumerate(thread_list):
					thread.join()
					SOMA_MESTRE.update(result_queue.get())

				client_thread.join()

				SOMA_MESTRE.update(result_queue_client.get())
					
				for k,v in SOMA_MESTRE.iteritems():
					for w in subsetPALAVRAS.split(','):
						SOMA_FINAL[w]+=v[w]
				fim = time.time()

				print "\nExecutado no cliente e mais", x, "maquinas"
				print "Arquivo:", arquivo
				print "Palavras:", subsetPALAVRAS
				print "Tempo:", fim-ini
				print "Resultado:", SOMA_FINAL
	
