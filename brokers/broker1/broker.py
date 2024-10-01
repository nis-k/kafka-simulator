import socket
import threading
from time import time,sleep
from datetime import date
import subprocess

# % Zookeeper functions:

leader = 1					# Leader bit
followers = [55556,55557]	# Followers' port no.s for inter-broker comms

broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
broker.connect(('127.0.0.1',11111))		#% Connect to zookeeper's port

# Listening to zookeeper and sending heartbeat
def zookeeper_receive():
	while True:
		try:
			message = broker.recv(1024).decode('ascii')

			if message == 'HEARTBEAT':
				broker.send("1".encode('ascii'))	# Send heartbeat

			elif message == 'LEADER':
				broker.send("1".encode('ascii'))	# Send ack
				print("I have been made leader!")
				
				global leader
				leader = 1							# Activate leader bit
				
		except:
			# print("except zookeeper")
			pass

# Starting Threads For Listening And Writing
receive_thread = threading.Thread(target=zookeeper_receive)
receive_thread.start()


#@ Broker functions
## Leader:

# Dictionaries for clients and their topics
producers = {}
consumers = {}

def broadcast(message,topic,counter):
	print(topic,": ",message,sep = '')

	_date = str(date.today())
	_time = str(time())
	message = message + "," + _date + "," + _time

	key = topic.split("topic(")[-1].split(')')[0]				# To get 'BD' from 'topic(BD)'

# For all the consumers listening right now, just send the message (solves the issue of having to check for timestamp and stuff)
	if key in consumers:										# If a consumer in this topic exists
		for client in consumers[key]:
			ack = None
			#// If ack doesn't come keep sending topic
			while ack != '1':
				client.send(message.encode('ascii'))
				ack = client.recv(10).decode('ascii')

# Write to partitions
	o = subprocess.run(["mkdir","-p",topic])					#,capture_output=True,text=True)

	f0 = open('{}/p{}_c0.txt'.format(topic, counter%3), 'a')
	f0.write(message + "\n")
	f0.close()
	
	f1 = open('{}/p{}_c1.txt'.format(topic, counter%3), 'a')    
	f1.write(message + "\n")
	f1.close()

	f2 = open('{}/p{}_c2.txt'.format(topic, counter%3), 'a')
	f2.write(message + "\n")
	f2.close()

	global leader
	if leader == 1:
		# Send message to followers
		msg = topic + " - "
		msg += str(counter) + " - "
		msg += message

		follower1.send(msg.encode("ascii"))
		follower2.send(msg.encode("ascii"))

# For consumer --from-beginning
def broadcastFromBeg(client,topic):
	try:
		f0 = open('{}/p0_c0.txt'.format(topic), 'r')
		for line in f0:
			line = line.strip()
			ack = None
			#// If ack doesn't come keep sending topic
			while ack == None:
				client.send(line.encode('ascii'))
				ack = client.recv(10).decode('ascii')

		f1 = open('{}/p1_c0.txt'.format(topic), 'r')
		for line in f1:
			line = line.strip()
			ack = None
			#// If ack doesn't come keep sending topic
			while ack == None:
				client.send(line.encode('ascii'))
				ack = client.recv(10).decode('ascii')

		f2 = open('{}/p2_c0.txt'.format(topic), 'r')
		for line in f2:
			line = line.strip()
			ack = None
			#// If ack doesn't come keep sending topic
			while ack == None:
				client.send(line.encode('ascii'))
				ack = client.recv(10).decode('ascii')

	except:
		pass


# Handling Messages From Clients
def handle(client,address,topic,type):
	counter = 0									# To know which partition to write to
	topicCopy = topic
	topic = 'topic(' + topic + ')'

	if type == 'consumer+':
		broadcastFromBeg(client,topic)

	while True:
		try:
			# Broadcasting Messages
			message = None
			while message == None:
				message = client.recv(1024).decode('ascii')
		
			if message != "EXIT":
				#% send ACK
				client.send('1'.encode('ascii'))

				if message != '1':
					msg = message.split(':')
					broadcast(msg[1].strip(),topic,counter)
					counter += 1

			else:
				print("%s at port number: %d left"%(type,address[1]))
				client.close()
				
				if type == 'producer':
					producers[topicCopy].remove(client)
					# print(producers)

				break	# exit this thread of handle
		except Exception as e:
			# print("exception:",e)
			
			print("%s at port number: %d left"%(type,address[1]))

			if type == 'producer':
				producers[topicCopy].remove(client)

			elif 'consumer' in type:
				consumers[topicCopy].remove(client)
			
			break


# Receiving / Listening Function
def receive():
	while True:
		# Accept Connection
		client, address = server.accept()
		print("Connected! Port number: {}".format(address[1]))

		# Request And Store topic
		topic = None
		while topic == None:
			client.send('TOPIC'.encode('ascii'))
			topic = client.recv(1024).decode('ascii')

		#% send ACK
		client.send(str(address[1]).encode('ascii'))

		sleep(1)
		type = None
		while type == None:
			client.send('TYPE'.encode('ascii'))
			type = client.recv(1024).decode('ascii')

		#% send ACK
		client.send('1'.encode('ascii'))

		if type == 'producer':
			# Keep a collection of clients grouped by topic
			if topic in producers:
				producers[topic].append(client)
			else:
				producers[topic] = [client]

		elif 'consumer' in type:
			# Keep a collection of clients grouped by topic
			if topic in consumers:
				consumers[topic].append(client)
			else:
				consumers[topic] = [client]

		else: 	#% zookeeper
			broker.send("1".encode("ascii"))
			pass


		# Print And Broadcast topic
		print("Topic: {}, type: {}".format(topic,type))
		message = 'Connected to broker!'
		if type == 'producer':
			message += '\ntype your msg: '

		ack = None
		while ack == None:
			client.send(message.encode('ascii'))
			ack = client.recv(10)

		# Start Handling Thread For Client
		thread = threading.Thread(target=handle, args=(client,address,topic,type))
		thread.start()

# Keep checking if LEADER
if leader == 1:
	# Starting broker
	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server.bind(('127.0.0.1', 55555))
	server.listen()
	print('Broker is running...')

	sleep(5)	# Wait for the other brokers to start

	follower1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	follower1.connect(('127.0.0.1',55556))
	
	follower2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	follower2.connect(('127.0.0.1',55557))

	receive()
