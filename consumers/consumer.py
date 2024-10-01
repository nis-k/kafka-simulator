import socket
import threading
from time import sleep
import datetime
import sys
from colorama import Fore, Style

# Choosing topic
topic = input("Choose your topic: ")
type = 'consumer'

if '--from-beginning' in sys.argv:
	# print(sys.argv)
	type += '+'
	
broker_port = 55555


# Connecting To Broker
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('127.0.0.1', broker_port))

Exit = False
wait = 0

# Listening to Server and Sending topic
def receive():
	global client
	while True:
		try:
			message = client.recv(1024).decode('ascii')

			if message == 'TOPIC':
				id = None
				#// If ack doesn't come keep sending topic
				while id == None:
					client.send(topic.encode('ascii'))
					id = client.recv(1024).decode('ascii')

				print("ConsumerID received:",id)

			elif message == 'TYPE':
				ack = None
				#// If ack doesn't come keep sending topic
				while ack != '1':
					client.send(type.encode('ascii'))
					ack = client.recv(1024).decode('ascii')

				print("ACK recieved for type")

			else:
				client.send('1'.encode('ascii'))	# Message recieved ack

				if message!= '1':
					if ',' in message:
						# Circus to make it look very readable:
						msg = message.split(',')	# Message from producer
						_time = datetime.datetime.fromtimestamp(float(msg[2])).strftime('%c')
						print(Style.DIM + '\ndate:',msg[1],'    time:',_time[11:19],'\ntime(since epoch):',msg[2],Style.RESET_ALL,'\nmessage:',msg[0])
					
					else:
						print(message)				# Message from broker
		except:
			
			# print("exception: ",e)
			
			sleep(2)
			
			if Exit:
				break

			global wait
			wait += 1

			if wait > 3:
				print(Fore.RED + "Fatal error!",Fore.WHITE + "Can't connect to broker.\nExiting",end='')
				sleep(0.5)
				print('.',end='')
				sleep(0.5)
				print('.',end='')
				sleep(0.5)
				print('.',end='')	# Slowly print '...'

				print()
				break

			print(Fore.RED + "Connection with broker failed!",Fore.WHITE + "\nRetrying after 15 seconds...")
			sleep(15)

			connected = False
			client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

			while connected == False:
				try:
					client.connect(('127.0.0.1',broker_port))
					connected = True

				except:
					client.close()
					break

			sleep(1)


# Starting Threads For Listening
receive_thread = threading.Thread(target=receive)
receive_thread.start()
