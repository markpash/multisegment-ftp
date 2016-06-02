import ftplib
import os
import threading
import shutil


class Downloader:
    def __init__(self, ftp_server = '', ftp_user = '', ftp_password = ''):
        if ftp_server is not '':
            self.connect(ftp_server, ftp_user, ftp_password)

    def connect(self, ftp_server, ftp_user, ftp_password = ''):
        self.ftp_server = ftp_server
        self.ftp_user = ftp_user
        self.ftp_password = ftp_password
        self.ftp = ftplib.FTP(self.ftp_server, self.ftp_user, self.ftp_password)

    def download(self, ftp_file_path, threads):
        self.ftp_file_path = ftp_file_path
        self.parts = threads
        self.ftp_file_size = self.ftp.size(self.ftp_file_path)
        self.chunk_size = self.ftp_file_size/self.parts
        self.last_chunk_size = self.ftp_file_size - (self.chunk_size * (self.parts - 1))

        partdownloaders = []
        for part in range(self.parts):
            if part == (self.parts - 1):
                this_chunk_size = self.last_chunk_size
            else:
                this_chunk_size = self.chunk_size
            ftp = ftplib.FTP(self.ftp_server, self.ftp_user, self.ftp_password)
            partdownloaders.append(DownloadPart(ftp, self.ftp_file_path, part, self.chunk_size * part, this_chunk_size))
        for part in partdownloaders:
            part.thread.join()
        with open(os.path.basename(self.ftp_file_path), 'w+b') as f:
            for downloader in partdownloaders:
                shutil.copyfileobj(open(downloader.part_name, 'rb'), f)
        for part in partdownloaders:
            os.remove(part.part_name)


class DownloadPart:
    def __init__(self, ftp, ftp_file_path, part_number, part_start, part_size):
        self.ftp = ftp
        self.ftp_file_path = ftp_file_path
        self.ftp_file = os.path.basename(self.ftp_file_path)
        self.part_number = part_number
        self.part_start = part_start
        self.part_size = part_size
        self.part_name = self.ftp_file + '.part' + str(self.part_number)
        self.thread = threading.Thread(target=self.receive_thread)
        self.thread.start()

    def receive_thread(self):
        try:
            self.ftp.retrbinary('RETR ' + self.ftp_file_path, self.on_data, 100000, self.part_start)
        except:
            pass

    def on_data(self, data):
        with open(self.part_name, 'a+b') as f:
            f.write(data)
        if os.path.getsize(self.part_name) >= self.part_size:
            with open(self.part_name, 'r+b') as f:
                f.truncate(self.part_size)
            raise
