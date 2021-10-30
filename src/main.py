import socket
import logging
from datetime import datetime
import re
import time
import pandas as pd

from emoji import demojize


info_socket = {"server":'irc.chat.twitch.tv',
                "port":6667,
                "nickname":"",
                "token":"",
                "channel":""}


def connect_socket(info_socket):
    sock = socket.socket()
    sock.connect((info_socket['server'], info_socket['port']))

    sock.send(f"PASS {info_socket['token']}\n".encode('utf-8'))
    sock.send(f"NICK {info_socket['nickname']}\n".encode('utf-8'))
    sock.send(f"JOIN {info_socket['channel']}\n".encode('utf-8'))

    return sock


def get_chat_dataframe(file):
    data = []

    with open(file, 'r', encoding='utf-8') as f:
        lines = f.read().split('\n\n\n')

        for line in lines:
            try:
                time_logged = line.split('—')[0].strip()
                time_logged = datetime.strptime(time_logged, '%Y-%m-%d_%H:%M:%S')

                username_message = line.split('—')[1:]
                username_message = '—'.join(username_message).strip()

                username, channel, message = re.search(
                    ':(.*)\!.*@.*\.tmi\.twitch\.tv PRIVMSG #(.*) :(.*)', username_message).groups()

                if message.startswith("@"):
                    addressee = message.split(' ')[0]
                else:
                    addressee = ""

                d = {
                    'dt': time_logged,
                    'channel': channel,
                    'username': username,
                    'addressee': addressee,
                    'message': message
                }

                data.append(d)

            except Exception:
                pass

    return pd.DataFrame().from_records(data)


def run_log_chat(sock):

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s — %(message)s',
                        datefmt='%Y-%m-%d_%H:%M:%S',
                        handlers=[logging.FileHandler('../log/chat.log', encoding='utf-8')])

    while True:
        resp = sock.recv(2048).decode('utf-8')

        if resp.startswith('PING'):
            sock.send("PONG\n".encode('utf-8'))

        elif len(resp) > 0:
            logging.info(demojize(resp))

            localtime = time.localtime(time.time())
            time_msg = "{}-{}-{}_{}:{}:{}".format(localtime.tm_year,
                                                localtime.tm_mon,
                                                localtime.tm_mday,
                                                localtime.tm_hour,
                                                localtime.tm_min,
                                                localtime.tm_sec)

            time_msg = datetime.strptime(time_msg, '%Y-%m-%d_%H:%M:%S')

            msg = demojize(resp)

            try:
                username, channel, message = re.search(r':(.*)\!.*@.*\.tmi\.twitch\.tv PRIVMSG #(.*) :(.*)', msg).groups()
                if message.startswith("@"):
                    addressee = message.split(' ')[0]
                else:
                    addressee = ""
                print(f"Channel: {channel} \nTime: {time_msg} \nUsername: {username} \nAddressee {addressee} \nMessage: {message}")
                print()
            except:
                print("don\'t parse message")
                continue


if __name__ == "__main__":
    sock = connect_socket(info_socket)
    run_log_chat(sock)
