import logging
import re
import socket
import time
from datetime import datetime

import pandas as pd
from emoji import demojize

info_socket = {"server": 'irc.chat.twitch.tv',
               "port": 6667,
               "nickname": "",
               "token": "",
               "channel": ""}


def connect_socket(twitch_info_socket):
    con_socket = socket.socket()
    con_socket.connect((twitch_info_socket['server'], twitch_info_socket['port']))

    con_socket.send(f"PASS {twitch_info_socket['token']}\n".encode('utf-8'))
    con_socket.send(f"NICK {twitch_info_socket['nickname']}\n".encode('utf-8'))
    con_socket.send(f"JOIN {twitch_info_socket['channel']}\n".encode('utf-8'))

    return con_socket


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

                username, channel, message = re.search(':(.*)!.*@.*\.tmi\.twitch\.tv PRIVMSG #(.*) :(.*)',
                                                       username_message).groups()

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
                username, channel, message = re.search(r':(.*).*@.*\.tmi\.twitch\.tv PRIVMSG #(.*) :(.*)',
                                                       msg).groups()
                if message.startswith("@"):
                    addressee = message.split(' ')[0]
                else:
                    addressee = ""

                print(f"Channel: {channel} \n"
                      f"Time: {time_msg} \n"
                      f"Username: {username} \n"
                      f"Addressee {addressee} \n"
                      f"Message: {message} \n")

            except:
                print("don\'t parse message")
                continue


if __name__ == "__main__":
    sock = connect_socket(info_socket)
    run_log_chat(sock)
