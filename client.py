"""
USAGE: python3 client.py CLIENT_ID

Where CLIENT_ID starts at 1 and increases
each new instance run. Start one instance
at a time.
"""

import logging
import socket
import os
import sys
import select
import base64
import pickle
import json
import time
import collections
# SETUP ---------------------------------------------------

logging.basicConfig(
         format='%(asctime)s %(levelname)-8s %(message)s',
         level=logging.INFO,
         datefmt='%Y-%m-%d %H:%M:%S')

# CONSTANTS ------------------------------------------------

BASE_PORT_NUMBER = 54322

STARTING_BALANE = 10.0

# how long to wait before sending a message (seconds)
SLEEP_TIME = 5

# TYPES ----------------------------------------------------

Client = collections.namedtuple(
    "Client",
    [
        "id", "socket"
    ]
)

Transaction = collections.namedtuple(
    "Transaction",
    [
        "id", "sender", "recipient", "amount"
    ]
)

Block = collections.namedtuple(
    "Block",
    [
        "transactions", "sequence_num"
    ]
)

# GLOBALS --------------------------------------------------

# List of Blocks, most recent block out front
blockchain = []

# List of uncommitted (local-only) transactions
local_transactions = []

# This pocess' ID (they start at 1 and increase)
my_id = None

# The proposal ID
proposal_num = 1

# List of connected clients
clients = []

# Whether to accept user-input commands.
# This can be turned off, for example, while contending
# for leadership.
accept_user_commands = True

# The highest previous Promise performed in the Paxos leader-election
highest_promise = {"epoch": -1, "client_id": -1, "proposal_num": -1}

# one accepted msg is enough for commit phase but the leader should not ignore other incoming accepted req
received_accepted_msgs = []

# to localy save clients transaction (in case that the client needs to run Paxos to update balances, it needs to have access to this very last txn later)
# format is the same as tnx = {"sender","recipient","amount"}
last_unsumbmited_users_txn = {}

# a flag for not sending accept request to other nodes after leader election
paxos_for_debug = False

# set this flag to prevent double sending ACCEPT request after leader attained ledaership
already_became_leader = False

# since after receiving the first ACCEPTED client can multicast COMMIT msg there is no need to send
# COMMIT per received ACCEPTED msg
already_multicast_commit = False
# Implementation -------------------------------------------

def main():
    global my_id
    global clients
    # find my client id
    if len(sys.argv) < 2:
        print("Please enter a process index (1,2,3,...)")
        exit(1)
    my_id = int(sys.argv[1])
    max_id = 3 # we'll only have 3 clients anyway...

    # (outbound) connect to other clients
    print('max id', max_id)
    for other_client_id in range(1, max_id+1):
        if other_client_id == my_id:
            # don't self-connect
            continue
        logging.info("Attempting to connect to client {0}".format(other_client_id))
        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cli.connect(("localhost", BASE_PORT_NUMBER - 1 + other_client_id))
            cli.send("CLIENT {0}\n".format(my_id).encode('ascii'))
            clients.append(Client(other_client_id, cli))
            logging.info("Connected to client {0}".format(
                other_client_id))
        except ConnectionRefusedError:
            logging.warning("Could not connect to client {0}".format(other_client_id))

    # (inbound) allow other clients to connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", BASE_PORT_NUMBER - 1 + my_id))
    sock.listen(5)
    logging.info("listening for connections on port {0}".format(BASE_PORT_NUMBER - 1 + my_id))

    restore_saved_state()

    # begin poll loop
    listen_loop(sock)


def listen_loop(srv_sock):
    """
    Main loop listening for updates on sockets and such
    """
    while True:
        # set up select() so we can handle input one at a time
        inputs = [*map(lambda x: x.socket, clients), srv_sock]
        if accept_user_commands:
            print_menu()
            inputs.append(sys.stdin)
        (rdy, _, _) = select.select(
            inputs,
            [],
            []
        )
        if len(rdy) > 0:
            ready = rdy[0]
            if ready != sys.stdin:
                if accept_user_commands:
                    # for tidiness, print a newline so log messages
                    # don't appear on the same line as as user input
                    # prompt
                    sys.stdout.write('\n')
                    sys.stdout.flush()
                if ready == srv_sock:
                    # accept new clients
                    accept_cxn(srv_sock)
                else:
                    # read incoming commands (either from stdin or client)
                    handle_channel_update(ready)
            else:
                handle_user_command()


def accept_cxn(sock):
    """
    Accepts and records an incoming client connection on the given
    server socket.
    """
    (from_sock, remote_addr) = sock.accept()
    logging.info("got connection from {0}".format(remote_addr))
    # read the client ID which this client declares
    ba = bytearray(128)
    num_recv = from_sock.recv_into(ba)
    if ba[:num_recv].startswith(b'CLIENT'):
        id_ = int(ba[:num_recv].decode('ascii').strip().split(' ')[1])
        clients.append(Client(id_, from_sock))
        logging.info('accepted client {0}'.format(id_))
    else:
        from_sock.close()
        logging.error('malformed ident {0}'.format(ba[:num_recv]))


def handle_channel_update(ready_channel):
    """
    Handles input from the given source, which is ready for reading.

    This should be called from listen_loop()

    ready_channel is a network socket
    """
    # find which Client this is
    client = None
    for c in clients:
        if c.socket == ready_channel:
            client = c
            break
    assert client is not None

    # read the command (max 2kb)
    ba = bytearray(2048)
    bytes_rcv = client.socket.recv_into(ba)
    if bytes_rcv == 0:
        # this indicates EOF -- closed connection
        client.socket.close()
        clients.remove(client)
        logging.info('Client {0} connection closed!'.format(client.id))
        return

    # decode the command and body
    cmd, body = ba[:bytes_rcv].decode('ascii').split(' ')
    # body should be base64(json(data))
    body = json.loads(base64.b64decode(body).decode('ascii'))

    logging.info('received {0} from client {1}'.format(cmd, client.id))

    if cmd == 'PREPARE':
        handle_prepare(client, body)
    elif cmd == 'PROMISE':
        handle_promise(client, body)
    elif cmd == "ACCEPT": # from leader to other clients
        handle_accept(client, body)
    elif cmd == "ACCEPTED": #from clients to the leader
        handle_accepted(client, body)
    elif cmd == "COMMIT":
        handle_commit(client, body)
    else:
        logging.info('unknown command: {0}'.format(cmd))


def handle_user_command():
    """
    Reads one user-command from stdin
    """
    cmd = sys.stdin.readline().strip().lower()
    if cmd == 'b':
        print('Own balance {0:.2f}'.format(query_balance(my_id)))
    elif cmd == 'p':
        # FOR DEBUGGING ONLY: initiate a paxos round
        paxos_for_debug = True
        initiate_leader_election()
    elif cmd == 't':
        while True:
            try:
                peer = int(input('Transacting. Send to which peer?: '))
                if peer != my_id and peer in range(4):
                    break
            except ValueError:
                pass
            print('invalid peer id')
        while True:
            try:
                amt = float(input('What amount?: '))
                if amt > 0:
                    break
            except ValueError:
                pass
            print('invalid amount')
        handle_transaction(my_id, peer, amt)
    elif cmd == 'q': #TODOD: how to simulate servers' crash (temporary out-of-service)
        for c in clients:
            logging.info('shutting down...')
            c.socket.close()
            exit(0)
    else:
        print('unknown command', cmd)

# Helpers for user commands ------------------------------------

def initiate_leader_election():
    global proposal_num
    global accept_user_commands
    global already_became_leader

    accept_user_commands = False
    already_became_leader = False

    proposal_num += 1
    save_state()

    # broadcast PREPARE
    data = {"proposal_num": proposal_num, "epoch": len(blockchain)}
    logging.info("Initiating leader election for proposal_num {0}, epoch {1}".format(proposal_num, len(blockchain)))
    for c in clients:
        send_message(c, "PREPARE", data)


# Helpers for client commands ------------------------------------

def handle_promise(client, data):
    """
    Handles a PROMISE message from the given client.

    NOTE: since we're only doing 3 nodes, one promise indicates
    leadership attained
    """
    global already_became_leader

    logging.info("Leadership attained...")

    if not already_became_leader and not paxos_for_debug:
        already_became_leader = True
        run_accept_phase_by_leader()

def handle_prepare(client, data):
    """
    Handles a PREPARE from the given client and payload
    """
    if data['epoch'] < len(blockchain):
        # If the epoch number is old, respond with existing block
        resp_data = {"existing_block": blockchain[data['epoch']]}
        logging.info('PROMISE was for old epoch {0}'.format(data['epoch']))
        send_message(client, "USE_EXISTING", resp_data)
    elif highest_promise['epoch'] == data['epoch'] and (
                highest_promise['client_id'] > client.id
                or
                (
                    highest_promise['client_id'] == client.id and
                    highest_promise['proposal_num'] > data['proposal_num']
                )
                ):
        # If we've done PROMISE for a higher client_id or proposal_num,
        # then NACK
        send_message(client, "PROMISE_NACK", None)
    else:
        assert data['epoch'] == len(blockchain)
        # All seems well, continue with the promise
        highest_promise['epoch'] = data['epoch']
        highest_promise['proposal_num'] = data['proposal_num']
        highest_promise['client_id'] = client.id
        save_state()
        logging.info('Promising client {0} with proposal {1} for epoch {2}'.format(client.id, data['proposal_num'], data['epoch']))
        send_message(client, "PROMISE", None)


# Add a transaction to the client's local_transactions list
def update_local_transactions(txn):
    """
    Perform an-uncommitted transaction and added to the local_transactions

    note: this function could be called when the local balance is enough
    or after running Paxos and updating the blockchain
    """
    global local_transactions

    if query_balance(my_id) >= txn["amount"]:

        local_transactions.append(txn)
        logging.info('client {0}: Updated local transactions: {1}'.format(my_id,json.dumps(local_transactions)))
        print("successful transaction from {0} to {1} for ${2}".format(txn["sender"], txn["recipient"], txn["amount"]))

        last_unsumbmited_users_txn = {}# empty out the list
    else:
        print("Aborted transaction from {0} to {1} for ${2} (balance is not enough)".format(tnx["sender"], txn["recipient"], txn["amount"]))


def update_block_chain(block, seq_num):
    """
    Add a block containing all commited local transactions to the ledger
    Empty the local transactions of the client
    """
    global blockchain
    global local_transactions

    blockchain.append(
    {
        "transactions":block,
        "sequence_num": seq_num
    })
    local_transactions = []
    logging.info("Client {0}: Updated my blockchain with {1}".format(my_id, json.dumps(block)))

def handle_transaction(my_id, peer, amt):
    """
    Handles a transaction request
    step I: if the balance is enough -> add txn to the local_transactions list
    step II: otherwise, runs leader election (repeat step I)
    Output: Success (if balance was enough), or Abort
    """
    global proposal_num
    global accept_user_commands
    global last_unsumbmited_users_txn

    accept_user_commands = False

    last_unsumbmited_users_txn = {
        "recipient": peer,
        "sender": my_id,
        "amount": amt,
        }
    estimate_balance = query_balance(my_id)
    if (amt <= estimate_balance):
        update_local_transactions(last_unsumbmited_users_txn)
        accept_user_commands = True
    else:
        initiate_leader_election()

        #TODO: show the abort msg if client chould'nt become a leader
        # logging.info('Transaction from client {0} to {1} for ${2} aborted. Client {0} couldnt manage to become a leader'.format(my_id,peer,amt))
        # accept_user_commands = True

def run_accept_phase_by_leader():
    """
    When the client got elected, it can send accept msg to others (for the last unsumbitted txn)
    """
    global proposal_num
    global local_transactions

    proposal_num += 1
    data = {"TX":local_transactions, "proposal_num": proposal_num}
    for c in clients:
        send_message(c, "ACCEPT", data)


def handle_accept(client, data):
    """
    Handles an ACCEPT request from the given client and payload
    """
    global proposal_num

    if data['proposal_num'] < proposal_num:
        # this is an old accept message
        #(e.g, after a server became a leader, another one asked for election
        # so the firts leader should not be able to multicast accept/commit msgs)
        logging.info("client {}:Ignore ACCEPT message from client {} since the ballot number is outdated".format(my_id, client.id))

    else:
        data = {"TX":local_transactions, "proposal_num": proposal_num}
        logging.info("client {}: ACCEPTED client {}'s accept request with ballot num {}'".format(my_id, client.id, data['proposal_num']))
        send_message(client, "ACCEPTED", data)


def handle_accepted(client, body):
    """
        Handles an ACCEPTED request from the given client and payload
        NOTE: since we're only doing 3 nodes, one accepted leades to a commit phase
        BUT leader should make sure to receive ALL ACCEPTED messages
    """
    global received_accepted_msgs
    global already_multicast_commit

    received_accepted_msgs.append(body['TX'])
    time.sleep(SLEEP_TIME)

    logging.info("client {0}: Received {1} accepted msg ... (ready to commit)".format(my_id, len(received_accepted_msgs)))

    if not already_multicast_commit:
        run_commit_phase_by_leader(received_accepted_msgs)
        already_multicast_commit = True

    accept_user_commands = True


# After collecting all ACCEPTED messages, it's time to commit
def run_commit_phase_by_leader(clients_local_transactions):
    """
    When client received f ACCEPTED msg it will generate block B and multicast COMMIT
    """
    global blockchain
    global local_transactions
    global proposal_num
    global already_became_leader
    global already_multicast_commit

    block = local_transactions
    for txn in clients_local_transactions:
        block.extend(txn)
    #seq_num = len(blockchain) ?

    data = {"block": block, "proposal_num": proposal_num}
    for c in clients:
        send_message(c,"COMMIT",data)

    update_block_chain(block, proposal_num)

    clients_local_transactions = []# empty out the list
    update_local_transactions(last_unsumbmited_users_txn)

    accept_user_commands = True
    already_became_leader = False
    already_multicast_commit = False

def handle_commit(client, body):
    """
    Handles a COMMIT request from the given client and payload
    """
    update_block_chain(body['block'], body['proposal_num'])
    logging.info("client {0}: Receive a COMMIT request from client {1} with block: {2}".format(my_id, client.id,json.dumps(body['block'])))


# Helpers for both user and client  commands ---------------------
def query_balance(peer_id):
    """
    Returns the best-guess estimate of the given peer's balance,
    based on locally available information.
    """
    balance = STARTING_BALANE
    for block in blockchain:
        for txn in block["transactions"]:
            if txn["recipient"] == peer_id:
                balance += txn["amount"]
            if txn["sender"] == peer_id:
                balance -= txn["amount"]

    for txn in local_transactions:
        if txn["recipient"] == peer_id:
            balance += txn["amount"]
        if txn["sender"] == peer_id:
            balance -= txn["amount"]

    return balance


def print_menu():
    sys.stdout.write("[B]alance [Q]uit: [T]ransfer money [E]nable crash mode?!")
    sys.stdout.flush()


def save_state():
    """
    Persists the current global state in file storage
    """
    fname = "chain.{0}.json".format(my_id)
    with open(fname, "w") as f:
        sz_blocks = base64.b64encode(pickle.dumps(blockchain)).decode("ascii")
        sz_local_txns = base64.b64encode(pickle.dumps(local_transactions)).decode("ascii")
        sz_highest_promise = base64.b64encode(pickle.dumps(highest_promise))
        f.write("{0} {1} {2} {3}".format(proposal_num, sz_blocks, sz_local_txns, sz_highest_promise))
    logging.info("Persisted blockchain in file {0}".format(fname))


def restore_saved_state():
    """
    Restores (from file storage) the previous block-chain, if one exists
    """
    global proposal_num
    global blockchain
    global local_transactions
    global highest_promise
    fname = "chain.{0}.json".format(my_id)
    logging.info("Examining {0} for previous blockchains".format(fname))
    try:
        with open(fname) as f:
            txt = f.read()
            sz_proposal_num, b64chains, b64locals, b64promise = txt.split(' ')
            proposal_num = int(sz_proposal_num)
            blockchain = pickle.loads(base64.b64decode(b64chains))
            local_transactions = pickle.loads(base64.b64decode(b64locals))
            highest_promise = pickle.loads(base64.b64decode(b64promise))
            logging.info("Restored {0} blocks, {1} unsent transactions, and proposal_num = {2}".format(
                len(blockchain),
                len(local_transactions),
                proposal_num
            ))
    except FileNotFoundError:
        logging.info("No backup found.")


def send_message(client, command, data):
    """
    Helper function: sends a command to a client

    client: a Client namedtuple
    command: str, the command
    data: json-serializable object
    """
    logging.info('Sending {0} to {1} ...'.format(command, client.id))
    b64_data = base64.b64encode(json.dumps(data).encode('ascii'))
    time.sleep(SLEEP_TIME)
    client.socket.send(command.encode('ascii') + b' ' + b64_data)


if __name__ == '__main__':
    main()
