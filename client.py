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

# Maps a block number to the highest previous Promise performed in the Paxos leader-election
# for that block
map_highest_promise = {}

# A Dict which maps block_num to the Block which was last seen in an Accept
# Used for leader failure case
map_last_accepted = {}

# one accepted msg is enough for commit phase but the leader should not ignore other incoming accepted req
#received_accepted_msgs = []

# to localy save clients transaction (in case that the client needs to run Paxos to update balances, it needs to have access to this very last txn later)
# format is the same as tnx = {"sender","recipient","amount"}
last_unsubmitted_users_txn = None

# a flag for not sending accept request to other nodes after leader election
paxos_for_debug = False


# a flag to say that we've already got quorum from leader election, so
# don't bother broadcasting ACCEPT again
already_became_leader = False

# since after receiving the first ACCEPTED client can multicast COMMIT msg there is no need to send
# COMMIT per received ACCEPTED msg
already_multicast_commit = False

# keeps track of whether one client has already given NACK to promise or accept
already_got_promise_nack = False
already_got_accepted_nack = False

# whether recovery is in progress
recovering = False
# Implementation -------------------------------------------

def main():
    global my_id
    global clients
    global recovering
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

    recovering = restore_saved_state()
    if recovering:
        logging.info("----- entering recovery mode, catching up ------")
        initiate_leader_election()

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
    bytes_rcv = 0
    try:
        bytes_rcv = client.socket.recv_into(ba)
    except ConnectionResetError:
        logging.info("Caught exception reading socket, did client close?")
    if bytes_rcv == 0:
        # this indicates EOF -- closed connection
        try:
            client.socket.close()
        except:
            logging.info("Could not close client")
        clients.remove(client)
        logging.info('Client {0} connection closed!'.format(client.id))
        return

    # there may be multiple commands that we just read, split those up
    cmds = ba[:bytes_rcv].decode('ascii').strip().split('\n')
    # handle each command that was waiting
    for cmd in cmds:
    # decode the command and body
        splat = cmd.split(' ')
        if len(splat) != 2:
            print(splat, "!!!!!!!!!!!!!!!")
            assert False
        else:
            cmd, body = splat
        # body should be base64(json(data))
        body = json.loads(base64.b64decode(body).decode('ascii'))

        logging.info('received {0} from client {1}'.format(cmd, client.id))

        if cmd == 'PREPARE':
            handle_prepare(client, body)
        elif cmd == 'PROMISE':
            handle_promise(client, body)
        elif cmd == 'PROMISE_NACK':
            handle_promise_nack(client, body)
        elif cmd == "ACCEPT": # from leader to other clients
            handle_accept(client, body)
        elif cmd == "ACCEPTED": #from clients to the leader
            handle_accepted(client, body)
        elif cmd == "ACCEPTED_NACK":
            handle_accepted_nack(client, body)
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
    elif cmd == 'x':
        # FOR DEBUGGING ONLY: print the blockchain
        print_blockchain()
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


def handle_promise_nack(client, body):
    global already_got_promise_nack
    global last_unsubmitted_users_txn
    global accept_user_commands
    global proposal_num

    if already_got_promise_nack:
        if recovering:
            # try again
            initiate_leader_election()
            return
        logging.info("Got second NACK to propose, aborting!!!")
        print("Transaction aborted.")
        last_unsubmitted_users_txn = None
        accept_user_commands = True
        save_state()
    else:
        logging.info("Got first NACK to propose")
        proposal_num = max(proposal_num, body["highest_proposal"] + 1)
        logging.info("Advancing local proposal_num to {0}, must be behind".format(proposal_num))
        save_state()
        already_got_promise_nack = True

def handle_accepted_nack(client, body):
    global already_got_accepted_nack
    global last_unsubmitted_users_txn
    global accept_user_commands

    if already_got_accepted_nack:
        if recovering:
            initiate_leader_election()
            return # try again
        logging.info("Got second NACK to accept, aborting!!!")
        print("Transaction aborted.")
        last_unsubmitted_users_txn = None
        accept_user_commands = True
        save_state()
    else:
        logging.info("Got first NACK to accept")
        already_got_accepted_nack = True

# Helpers for user commands ------------------------------------

def initiate_leader_election():
    global proposal_num
    global accept_user_commands
    global already_became_leader
    global already_multicast_commit
    global already_got_promise_nack
    global already_got_accepted_nack

    accept_user_commands = False
    already_multicast_commit = False
    already_became_leader = False
    already_got_promise_nack = False
    already_got_accepted_nack = False

    # Perform a PREPARE on ourselves (each site has a participant, so do I)
    proposal_num += 1
    target_block_num = len(blockchain)
    if target_block_num in map_highest_promise:
        # make our proposal higher than any of our own promises so
        # we don't need to self-NACK, which would be silly
        proposal_num = max(proposal_num, map_highest_promise[target_block_num]["proposal_num"] + 1)
    else:
        # just set this up for us later
        map_highest_promise[target_block_num] = {}
    # store the promise
    map_highest_promise[target_block_num]['proposal_num'] = proposal_num
    map_highest_promise[target_block_num]['proposing_client_id'] = my_id

    save_state()

    # broadcast PREPARE
    data = {"proposal_num": proposal_num, "block_num": target_block_num}
    logging.info("Initiating leader election for proposal_num {0}, block_num {1}".format(proposal_num, len(blockchain)))
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
    if already_became_leader:
        logging.info("Redundant PROMISE after quorum, ignoring")
        return
    else:
        already_became_leader = True
        logging.info("Leadership attained.")

    if not paxos_for_debug:
        run_accept_phase_by_leader(client.id, data)


def handle_prepare(client, data):
    """
    Handles a PREPARE from the given client and payload
    """
    if recovering:
        # eat the message -- do nothing
        logging.info("Not handling PREPARE -- too busy recovering")
        return
    target_block_num = data['block_num']
    if target_block_num in map_highest_promise and (
                map_highest_promise[target_block_num]['proposal_num'] > data['proposal_num']
                or
                (
                    map_highest_promise[target_block_num]['proposal_num'] == data['proposal_num'] and
                    map_highest_promise[target_block_num]['proposing_client_id'] > client.id
                )
                ):
        # If we've done PROMISE for a higher proposal (using client_id as tie-breaker),
        # then NACK
        send_message(client, "PROMISE_NACK", {
                "proposal_num": data['proposal_num'],
                "block_num": data['block_num'],
                "highest_proposal": map_highest_promise[target_block_num]['proposal_num']
            })
    else:
        # All seems well, continue with the promise

        # record the promise
        if target_block_num not in map_highest_promise:
            map_highest_promise[target_block_num] = {}
        map_highest_promise[target_block_num]['proposal_num'] = data['proposal_num']
        map_highest_promise[target_block_num]['proposing_client_id'] = client.id
        save_state()

        # value discovery -- see if we've persisted something the new leader should echo
        block_to_send = map_last_accepted.get(target_block_num, None)
        if block_to_send is not None:
            logging.info('Replying to Promise with previously ACCEPTed transactions')
        logging.info('Promising client {0} with proposal {1} for block_num {2}'.format(client.id, data['proposal_num'], data['block_num']))
        send_message(client, "PROMISE", {
                "proposal_num": data['proposal_num'],
                "block_num": data['block_num'],
                "prev_accept": block_to_send,
                "local_txns": local_transactions
            })


def does_commited_block_has_my_transactions(block):
    """
    To check whether client's local transactions apperas in the COMMIT msg

    Since the leader multicasts commit based on the first ACCEPTED msg
    if the client does not see her transactions in the commited blocks
    she should still keep them locally
    """
    for txn in block["transactions"]:
        if txn["sender"] == my_id:
            return True
    return False


# Add a transaction to the client's local_transactions list
def add_or_abort_unsubmitted_user_txn():
    """
    Perform an-uncommitted transaction and added to the local_transactions

    note: this function could be called when the local balance is enough
    or after running Paxos and updating the blockchain
    """
    global local_transactions
    global last_unsubmitted_users_txn
    txn = last_unsubmitted_users_txn
    if txn is None:
        logging.warn("last_unsubmitted_users_txn was None!?!?!")
        return
    if query_balance(my_id) >= txn["amount"]:

        local_transactions.append(txn)
        logging.info('client {0}: Updated local transactions: {1}'.format(my_id,json.dumps(local_transactions)))
        print("successful transaction from {0} to {1} for ${2}".format(txn["sender"], txn["recipient"], txn["amount"]))
    else:
        print("Aborted transaction from {0} to {1} for ${2} (balance is not enough)".format(txn["sender"], txn["recipient"], txn["amount"]))
    # it's either added or aborted, either way, clear
    last_unsubmitted_users_txn = None
    save_state()


def update_block_chain(block, block_number):
    """
    Add a block containing all commited local transactions to the ledger
    Empty the local transactions of the client
    """
    global blockchain
    global local_transactions

    if block_number == len(blockchain):
        blockchain.append(block)
        if does_commited_block_has_my_transactions(block):
            local_transactions = []
    else:
        blockchain[block_number] = block
    save_state()
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
    global last_unsubmitted_users_txn

    accept_user_commands = False

    last_unsubmitted_users_txn = {
        "recipient": peer,
        "sender": my_id,
        "amount": amt,
        }
    estimate_balance = query_balance(my_id)
    if (amt <= estimate_balance):
        add_or_abort_unsubmitted_user_txn()
        accept_user_commands = True
    else:
        initiate_leader_election()

def run_accept_phase_by_leader(client_id, data):
    """
    When the client got elected, it can send accept msg to others.
    ACCEPT contains a new block that will be committed later
    """
    global recovering
    global already_multicast_commit
    # HACK -- sometimes the response to a recovering process ACCEPT
    # comes a little late and messes things up, re-set it here
    already_multicast_commit = False

    target_block_num = data["block_num"]
    #
    # figure out which block we'll be sending in ACCEPT
    block_to_accept = None # stores decided block to send out

    # make list of blocks that are accepted to consider
    already_accepted = []
    if target_block_num in map_last_accepted:
        already_accepted.append(map_last_accepted[target_block_num])
    if data.get('prev_accept', None) is not None:
        already_accepted.append(data['prev_accept'])

    # if there's a previously-accepted value, we MUST adopt that block
    if len(already_accepted) > 0:
        already_accepted = sorted(already_accepted, key = lambda x: (x["proposal_num"], x["proposing_client_id"]))
        block_to_accept = already_accepted[-1]
        logging.info("Using previously accepted block, proposal_num {0}, client_id {1}".format(
            block_to_accept["proposal_num"],
            block_to_accept["proposing_client_id"]
        ))
    else:
        # otherwise, make our own new block
        block_to_accept = {
            "transactions": local_transactions + data['local_txns'],
            "proposal_num": data["proposal_num"],
            "proposing_client_id": my_id
        }
        logging.info("Constructed new block, sending to be accepted...")
        if recovering:
            logging.info("Recovery has completed --")
            recovering = False

    # update w/ my details
    block_to_accept["proposal_num"] = data["proposal_num"]
    block_to_accept["proposing_client_id"] = my_id
    # record this block in the accepted values map
    map_last_accepted[target_block_num] = block_to_accept
    logging.info("Stored new acceptVal for block = {0}".format(target_block_num))

    # send out the block
    for c in clients:
        send_message(c, "ACCEPT", {
                "proposal_num": data["proposal_num"],
                "block_num": data["block_num"],
                "block": block_to_accept
            })


def handle_accept(client, data):
    """
    Handles an ACCEPT request from the given client and payload
    """

    if recovering:
        logging.info("Ignoring ACCEPT -- too busy recovering")
        return
    target_block_num = data["block_num"]
    assert target_block_num in map_highest_promise
    if data['proposal_num'] < map_highest_promise[target_block_num]["proposal_num"] or \
        (
            data['proposal_num'] == map_highest_promise[target_block_num]['proposal_num'] and
            client.id < map_highest_promise[target_block_num]['proposing_client_id']
        ):
        # this is an old accept message
        #(e.g, after a server became a leader, another one asked for election
        # so the firts leader should not be able to multicast accept/commit msgs)
        logging.info("client {}:Ignore ACCEPT message from client {} since the ballot number is outdated".format(my_id, client.id))
        send_message(client, "ACCEPTED_NACK", {"proposal_num": data["proposal_num"], "block_num": data["block_num"]})
    else:
        map_last_accepted[target_block_num] = data["block"]
        logging.info("client {}: ACCEPTED client {}'s accept request with ballot num {}'".format(my_id, client.id, data['proposal_num']))
        send_message(client, "ACCEPTED", data)


def handle_accepted(client, body):
    """
        Handles an ACCEPTED request from the given client and payload
        NOTE: since we're only doing 3 nodes, one accepted leades to a commit phase
        So leader should NOT wait to receive ALL ACCEPTED messages!
    """
    global already_multicast_commit
    global accept_user_commands

    logging.info("client {0}: Received an accepted msg with block {1}".format(my_id, json.dumps(body['block'])))

    if not already_multicast_commit:
        run_commit_phase_by_leader(body)
        already_multicast_commit = True
    else:
        logging.info("Ignoring redundant ACCEPTED")


# After collecting an ACCEPTED messages, it's time to commit
def run_commit_phase_by_leader(data):
    """
    When client received f ACCEPTED msg it will generate block B and multicast COMMIT
    """
    global accept_user_commands

    block = data["block"]
    for c in clients:
        send_message(c,"COMMIT",
                {
                    "block": block,
                    "proposal_num":
                    data["proposal_num"],
                    "block_num": data["block_num"]
                })

    update_block_chain(block, data["block_num"])
    if not recovering:
        add_or_abort_unsubmitted_user_txn()
        accept_user_commands = True
    else:
        # recovery -- start another leader election to get more blocks
        initiate_leader_election()
        

def handle_commit(client, body):
    """
    Handles a COMMIT request from the given client and payload
    """
    if recovering:
        logging.info("Ignoring COMMIT -- too busy recovering")
        return
    logging.info("client {0}: Receive a COMMIT request from client {1} with block: {2}".format(my_id, client.id,json.dumps(body['block'])))
    update_block_chain(body['block'], body["block_num"])
    logging.info("Finished COMMIT")


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


def print_blockchain():
    print("Blockchain:")
    for i, block in enumerate(blockchain):
        print("Block {0}, proposal_num {1}, sender {2}:"
                .format(i, block["proposal_num"], block["proposing_client_id"]))
        for txn in block["transactions"]:
            print("    {0} -> {1} ${2:.2f}".format(txn["sender"], txn["recipient"], txn["amount"]))


def print_menu():
    sys.stdout.write("[B]alance [Q]uit [T]ransfer money: ")
    sys.stdout.flush()


def save_state():
    """
    Persists the current global state in file storage
    """
    to_save = [
            proposal_num,
            blockchain,
            local_transactions,
            map_highest_promise,
            map_last_accepted,
            last_unsubmitted_users_txn
    ]
    fname = "chain.{0}.json".format(my_id)
    with open(fname, "w") as f:
        f.write(json.dumps(to_save))
    logging.info("Persisted blockchain in file {0}".format(fname))


def restore_saved_state():
    """
    Restores (from file storage) the previous block-chain, if one exists
    """
    global proposal_num
    global blockchain
    global local_transactions
    global map_highest_promise
    global map_last_accepted
    global last_unsubmitted_users_txn
    
    fname = "chain.{0}.json".format(my_id)
    logging.info("Examining {0} for previous blockchains".format(fname))
    try:
        with open(fname) as f:
            proposal_num, \
            blockchain, \
            local_transactions, \
            map_highest_promise, \
            map_last_accepted, \
            last_unsubmitted_users_txn  = json.loads(f.read())
        return True
    except FileNotFoundError:
        logging.info("No backup found.")
    return False


def send_message(client, command, data):
    """
    Helper function: sends a command to a client

    client: a Client namedtuple
    command: str, the command
    data: json-serializable object
    """
    save_state()
    logging.info('Sending {0} to {1} ...'.format(command, client.id))
    b64_data = base64.b64encode(json.dumps(data).encode('ascii'))
    time.sleep(SLEEP_TIME)
    client.socket.send(command.encode('ascii') + b' ' + b64_data + b'\n')


if __name__ == '__main__':
    main()
