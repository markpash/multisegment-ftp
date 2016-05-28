import ftplib
import os
import threading
import shutil


def open_ftp(ftp_server, ftp_user, ftp_password):
    return ftplib.FTP(ftp_server, ftp_user, ftp_password)


class Downloader:

    def __init__(self, server, username, password, ftp_directory, ftp_file, threads):
        self.server = server
        self.username = username
        self.password = password
        self.directory = ftp_directory
        self.ftp = open_ftp(self.server, self.username, self.password)
        self.file = ftp_file
        self.file_path = self.directory + '/' + self.file
        self.parts = threads
        self.file_size = self.ftp.size(self.file_path)
        self.chunk_size = self.file_size/self.parts
        self.last_chunk_size = self.file_size - (self.chunk_size * (self.parts - 1))

        partdownloaders = []
        for part in range(self.parts):
            if part == (self.parts - 1):
                this_chunk_size = self.last_chunk_size
            else:
                this_chunk_size = self.chunk_size
            partdownloaders.append(DownloadPart(self.server, self.username, self.password, self.directory, self.file, part, self.chunk_size * part, this_chunk_size))
        for part in partdownloaders:
            part.thread.join()
        with open(self.file, 'w+b') as f:
            for downloader in partdownloaders:
                shutil.copyfileobj(open(downloader.part_name, 'rb'), f)
        for part in partdownloaders:
            os.remove(part.part_name)


class DownloadPart:

    def __init__(self, server, username, password, ftp_directory, ftp_file, part_number, part_start, part_size):
        self.server = server
        self.username = username
        self.password = password
        self.directory = ftp_directory
        self.ftp = open_ftp(self.server, self.username, self.password)
        self.file = ftp_file
        self.file_path = self.directory + '/' + self.file
        self.part_number = part_number
        self.part_start = part_start
        self.part_size = part_size
        self.part_name = self.file + '.part' + str(self.part_number)
        self.thread = threading.Thread(target=self.receive_thread)
        self.thread.start()

    def receive_thread(self):
        try:
            self.ftp.retrbinary('RETR ' + self.file_path, self.on_data, 100000, self.part_start)
        except:
            pass

    def on_data(self, data):
        with open(self.part_name, 'a+b') as f:
            f.write(data)
        if os.path.getsize(self.part_name) >= self.part_size:
            with open(self.part_name, 'r+b') as f:
                f.truncate(self.part_size)
            raise


Downloader('server', 'user', 'password', 'path', 'file', 20)
