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

# This pocess' ID (they start at 1 and increase)
my_id = None

# The proposal ID
proposal_num = 1

# List of connected clients
clients = []

# Implementation -------------------------------------------

def main():
    global my_id
    global clients
    # find my client id
    if len(sys.argv) < 2:
        print("Please enter a process index (1,2,3,...)")
        exit(1)
    my_id = int(sys.argv[1])
    if len(sys.argv) < 3:
        print("Please enter the number of processes (2,3...)")
        exit(1)
    max_id = int(sys.argv[2])

    # (outbound) connect to other clients
    for other_client_id in range(1, max_id):
        if other_client_id == my_id:
            # don't self-connect
            continue
        logging.info("Attempting to connect to client {0}".format(other_client_id))
        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cli.connect(("localhost", BASE_PORT_NUMBER - 1 + other_client_id))
            cli.send("CLIENT {0}\n".format(my_id).encode('ascii'))
            clients.append(Client(other_client_id, cli))
        except ConnectionRefusedError:
            logging.warning("Could not connect to client {0}".format(other_client_id))

    # (inbound) allow other clients to connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", BASE_PORT_NUMBER - 1 + my_id))
    sock.listen(5)
    logging.info("listening for connections on port {0}".format(BASE_PORT_NUMBER - 1 + my_id))
    
    restore_saved_chain()

    # begin poll loop
    listen_loop(sock)


def listen_loop(srv_sock):
    """
    Main loop listening for updates on sockets and such
    """
    while True:
        print_menu()
        (rdy, _, _) = select.select(
            [*map(lambda x: x.socket, clients), sys.stdin, srv_sock],
            [],
            []
        )
        if len(rdy) > 0:
            ready = rdy[0]
            if ready != sys.stdin:
                # for tidiness, print a newline so log messages
                # don't appear on the same line as as user input
                # prompt
                sys.stdout.write('\n')
                sys.stdout.flush()


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
    print("[B]alance: ", end=None)


def save_chain():
    """
    Persists the current blockchain in file storage
    """
    fname = "chain.{0}.json".format(my_id)
    with open(fname, "w") as f:
        sz_txns = base64.b64encode(pickle.dumps(blockchain)).decode("ascii")
        f.write(str(proposal_num) + " " + sz_txns)
    logging.info("Persisted blockchain in file {0}".format(fname))


def restore_saved_chain():
    """
    Restores (from file storage) the previous block-chain, if one exists
    """
    global proposal_num
    global blockchain
    fname = "chain.{0}.json".format(my_id)
    logging.info("Examining {0} for previous blockchains".format(fname))
    try:
        with open(fname) as f:
            # Format: proposal_num<space>base64(pickled-text)
            txt = f.read()
            sz_proposal_num, b64chains = txt.split(' ')
            proposal_num = int(sz_proposal_num)
            blockchain = pickle.loads(base64.b64decode(b64chains))
            logging.info("Restored {0} blocks and proposal_num {1}".format(
                len(blockchain),
                proposal_num
            ))
    except FileNotFoundError:
        logging.info("No backup found.")


if __name__ == '__main__':
    main()
