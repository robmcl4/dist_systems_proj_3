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

# List of unsubmitted (local-only) transactions
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
        initiate_leader_election()
    elif cmd == 'q':
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
    accept_user_commands = False
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
    logging.info("Leadership attained...")


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


# Helpers for both user and client  commands ---------------------

def query_balance(peer_id):
    """
    Returns the best-guess estimate of the given peer's balance,
    based on locally available information.
    """
    balance = STARTING_BALANE
    for b in blockchain:
        if b.recipient == peer_id:
            balance += b.amount
        if b.sender == peer_id:
            balance -= b.amount
    return balance


def print_menu():
    sys.stdout.write("[B]alance [Q]uit: ")
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
