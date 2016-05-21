from ftplib import *
from threading import *
from shutil import *
import os

num_parts = 20
FTP_server = 'server'
FTP_user = 'user'
FTP_password = 'password'

FTP_directory = 'directory'
FTP_file = 'file'


class Done(Exception):
    pass


def open_ftp():
    ftp = FTP(FTP_server, FTP_user, FTP_password)
    ftp.cwd(FTP_directory)
    return ftp


def go():
    ftp = open_ftp()
    filesize = ftp.size(FTP_file)
    print 'filesize: ' + str(filesize)
    ftp.quit()

    chunk_size = filesize/num_parts
    last_chunk_size = filesize - (chunk_size * (num_parts - 1))

    downloaders = []
    for i in range(num_parts):
        if i == (num_parts - 1):
            this_chunk_size = last_chunk_size
        else:
            this_chunk_size = chunk_size
        downloaders.append(Downloader(i, chunk_size * i, this_chunk_size))
    for downloader in downloaders:
        downloader.thread.join()

    with open(FTP_file, 'w+b') as f:
        for downloader in downloaders:
            copyfileobj(open(downloader.part_name, 'rb'), f)


class Downloader:

    thread_number = 0

    def __init__(self, part_number, part_start, part_size):
        self.filename = FTP_file
        self.part_number = part_number
        self.part_name = 'part' + str(self.part_number)
        self.part_start = part_start
        self.part_size = part_size
        Downloader.thread_number += 1
        self.thread_number = Downloader.thread_number
        self.ftp = open_ftp()
        self.thread = Thread(target=self.receive_thread)
        self.thread.start()

    def receive_thread(self):
        try:
            self.ftp.retrbinary('RETR '+self.filename, self.on_data, 100000, self.part_start)
        except Done:
            pass

    def on_data(self, data):
        with open(self.part_name, 'a+b') as f:
            f.write(data)
        if os.path.getsize(self.part_name) >= self.part_size:
            with open(self.part_name, 'r+b') as f:
                f.truncate(self.part_size)
            raise Done

go()
