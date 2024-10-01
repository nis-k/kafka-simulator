import socket
import threading
from time import sleep
from colorama import Fore

# Choosing topic
topic = input("Enter your topic: ")
type = 'producer'

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

				print("ProducerID received:",id)

			elif message == 'TYPE':
				ack = None
				#// If ack doesn't come keep sending topic
				while ack == None:
					client.send(type.encode('ascii'))
					ack = client.recv(1024).decode('ascii')

				print("ACK recieved for type")

			else:
				client.send('1'.encode('ascii'))	# Message recieved ack
				
				if message!= '1':
					print(message)

		except Exception as e:

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

					write_thread = threading.Thread(target=write)
					write_thread.start()

				except:
					break

			sleep(1)

# Sending Messages To Broker

def write():
	while True:
		message = 'topic({}): {}'.format(topic, input())

		if "EXIT" in message:
			client.send("EXIT".encode('ascii'))
			print(Fore.RED + "exiting...",Fore.WHITE + "")	# Fore.WHITE to reset back to white
			sleep(1)
			client.close()
			global Exit
			Exit = True
			break
		
		# Send
		else:
			ack = None
			while ack == None:
				client.send(message.encode('ascii'))
				ack = client.recv(10).decode('ascii')


# Starting thread for listening
receive_thread = threading.Thread(target=receive)
receive_thread.start()

# Starting thread for writing
write_thread = threading.Thread(target=write)
write_thread.start()
