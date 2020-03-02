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
import json
import time

logging.basicConfig(
         format='%(asctime)s %(levelname)-8s %(message)s',
         level=logging.INFO,
         datefmt='%Y-%m-%d %H:%M:%S')

BASE_PORT_NUMBER = 54322


def main():
    # find my client id
    if len(sys.argv) != 2:
        print("Please enter a process index (1,2,3,...)")
        exit(1)
    my_id = int(sys.argv[1])

    clis = []

    # (outbound) connect to other clients
    for other_client in range(1, my_id):
        logging.info("Attempting to connect to client {0}".format(other_client))
        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cli.connect(("localhost", BASE_PORT_NUMBER - 1 + other_client))
        cli.send("CLIENT {0}\n".format(my_id).encode('ascii'))
        clis.append((cli, other_client))

    # (inbound) allow other clients to connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", BASE_PORT_NUMBER - 1 + my_id))
    sock.listen(5)
    logging.info("listening for connections on port {0}".format(BASE_PORT_NUMBER - 1 + my_id))
    
    # begin poll loop
    listen_loop(clis, sock, my_id)


def listen_loop(peers, srv_sock, my_id):
    while True:
        logging.debug("listening for next command")
        sys.stdout.write("enter command... [B]alance [T]ransact [Q]uit: ")
        sys.stdout.flush()
        (rdy, _, _) = select.select(
                [*map(lambda x: x[0], peers), sys.stdin, srv_sock],
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
            if ready == sys.stdin:
                # command from user directly
                keep_going = read_user_command(peers, my_id)
                if not keep_going:
                    break
            elif ready == srv_sock:
                # new inbound connection
                accept_cxn(peers, srv_sock, tt)
            else:
                # find which peer is updating
                keep_going = True
                for peer_sock, peer_id in peers:
                    if ready == peer_sock:
                        peer_sock_update(peers, my_id, peer_sock)
                        break
                else:
                    logging.error('Could not find peer for this update')
    srv_sock.close()
    for sock, _ in peers:
        sock.close()
    logging.debug("leaving listen loop")


def read_user_command(peers, my_id):
    cmd = sys.stdin.readline().strip().lower()
    if cmd == 'q':
        logging.info("shutting down ...")
        return False
    return True


def send_msg(peers, my_id, peer, msg, chain, tt):
    bmsg = base64.b64encode(msg.encode('utf8'))
    btt = base64.b64encode(json.dumps(tt).encode('utf8'))
    # figure out what of the chain to send
    logs = []
    for txn in chain:
        sender = txn['sender']
        if tt[peer-1][sender-1] < txn['clock']:
            logs.append(txn)
            logging.info('preparing for send: {0} -> {1}: {2:.2f} @ {3}'.format(sender, txn['recipient'], txn['amount'], txn['clock']))
        else:
            logging.info('not sending: {0} -> {1}: {2:.2f} @ {3}'.format(sender, txn['recipient'], txn['amount'], txn['clock']))
    bchain = base64.b64encode(json.dumps(logs).encode('utf8'))
    for sock, peer_id in peers:
        if peer_id != peer:
            continue
        to_send = b'MESSAGE ' + bmsg + b' ' + btt + b' ' + bchain + b'\n'
        logging.info('sending MESSAGE to peer {0} with {1} log-messages'.format(peer, len(chain)))
        time.sleep(5)
        sock.send(to_send)
        break
    else:
        logging.error('could not find peer {0}'.format(peer))


def do_txn(my_id, peer, amt, chain, tt):
    # increment the local clock
    tt[my_id-1][my_id-1] += 1
    logging.info('new local clock: {0}'.format(tt[my_id-1][my_id-1]))
    txn = {
            "recipient": peer,
            "sender": my_id,
            "amount": amt,
            "clock": tt[my_id-1][my_id-1]
    }
    # calculate local balance
    bal = query_balance(chain, my_id)
    if bal < amt:
        print('INCORRECT')
    else:
        chain.append(txn)
        logging.info('new chain: {0}'.format(json.dumps(chain)))
        print('CORRECT')


def accept_cxn(peers, srv_sock, tt):
    (from_sock, remote_addr) = srv_sock.accept()
    logging.info("got connection from {0}".format(remote_addr))
    ba = bytearray(128)
    num_recv = from_sock.recv_into(ba)
    if ba[:num_recv].startswith(b'CLIENT'):
        id_ = int(ba[:num_recv].decode('ascii').strip().split(' ')[1])
        peers.append((from_sock, id_))
        for row in tt:
            row.append(0)
        tt.append([0]*id_)
        logging.info('accepted client {0}'.format(id_))
    else:
        from_sock.close()
        logging.error('malformed ident {0}'.format(ba[:num_recv]))


def peer_sock_update(peers, my_id, peer_sock, peer_id, chain, tt):
    ba = bytearray(2048)
    n_read = peer_sock.recv_into(ba)
    if n_read == 0:
        logging.info('peer {0} disconnected'.format(peer_id))
        peers.remove((peer_sock, peer_id))
        return
    s = ba[:n_read]
    cmd, msg, peer_tt, logs = s.split(b' ')
    if cmd != b'MESSAGE':
        logging.error('unknown message {0}'.format(cmd))
        return
    msg = base64.b64decode(msg).decode('utf8')
    peer_tt = json.loads(base64.b64decode(peer_tt).decode('utf8'))
    logs = json.loads(base64.b64decode(logs).decode('utf8'))
    logging.info('peer {0} sent {1}'.format(peer_id, cmd.decode('utf8')))
    peer_handle_message(my_id, peer_id, chain, tt, msg, peer_tt, logs)


def peer_handle_message(my_id, peer_id, chain, tt, msg, peer_tt, logs):
    logging.info('peer {0} says \'{1}\''.format(peer_id, msg))
    # update local chain with new logs
    for txn in logs:
        # if we got it already, disregard...
        sender = txn['sender']
        if tt[my_id-1][sender-1] >= txn['clock']:
            logging.info('ignoring duplicate log {0} -> {1}: {2:.2f} @ {3}'.format(txn['sender'], txn['recipient'], txn['amount'], txn['clock']))
        else:
            chain.append(txn)
            logging.info('appended log {0} -> {1}: {2:.2f} @ {3}'.format(txn['sender'], txn['recipient'], txn['amount'], txn['clock']))
    # update local tt
    for i in range(len(tt)):
        for j in range(len(tt)):
            tt[i][j] = max(tt[i][j], peer_tt[i][j])
    for i in range(len(tt)):
        t = tt[0][i]
        for j in range(1, len(tt)):
            t = max(t, tt[j][i])
        tt[my_id-1][i] = t
    logging.info('updated TT')


if __name__ == '__main__':
    main()
