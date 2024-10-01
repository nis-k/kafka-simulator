import socket
from time import sleep
from colorama import Fore	# Colours in terminal!!!

brokers = {}
leader_address = 0

def handle():
	global brokers
	global leader_address
	while True:
		sleep(15)			# Poll every 15 secs
		counter = 1
		flag = False

		for address in brokers.keys():
			client = brokers[address]
			
			print("Polling broker %d..."%(counter))

			try:
				client.send("HEARTBEAT".encode("ascii"))
				heartbeat = client.recv(10).decode("ascii")
			
			except:
				if client == -1:
					print(address,"is",Fore.RED + "DEAD.",Fore.WHITE + "")
					continue
				
				pass					# A follower is dead. Ignore

			if heartbeat == "" and address == leader_address:	# If leader dies
				print(address,"(leader) is",Fore.RED + "DEAD!",Fore.WHITE + "\nElecting new leader...")
				del brokers[address]
				flag = True
				break

			elif heartbeat == "1":
				print(address,"is",Fore.GREEN + "alive.",Fore.WHITE + "") # Fore.WHITE at the end to reset it back to white

			else:
				brokers[address] = -1	# Blackmark the socket
				print(address,"is",Fore.RED + "DEAD.",Fore.WHITE + "")

			counter += 1

		if flag:
			leader = list(brokers.keys())[0]
			print("new leader:",leader)
			leader_address = leader
			ack = None
			while ack != '1':
				brokers[leader].send('LEADER'.encode("ascii"))
				ack = brokers[leader].recv(10).decode("ascii")

		print()

def receive():
	flag = True
	while True:
		# Accept Connection
		client, address = zookeeper.accept()
		print("Connected! Port number: {}".format(address[1]))

		if flag:	# If 1st broker then store its address as leader
			global leader_address
			leader_address = address[1]
			flag = False

		global brokers
		brokers[address[1]] = client
		
		if len(list(brokers.keys())) == 3:
			print("All 3 brokers are running!\n")
			break

	handle()

zookeeper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zookeeper.bind(('127.0.0.1', 11111))
zookeeper.listen()
print("Zookeeper is running")
receive()