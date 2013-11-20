#! /usr/bin/python

__author__ = "Jason Mann <jcm2207@columbia.edu>"
__date__ = "$Nov 11, 2013"


import argparse
import sys
import getopt
import multiprocessing
import Queue
import time
import datetime
from mechanize import Browser
from urllib2 import URLError
import os
import sys
import time
import re

from mannutils import print_time


class Downloader(object):
    """
    Downloads files synchronously based on regular expressions,
    but pushes finished files up to caller in a multiprocessing.Queue.
    """
    def __init__(self, download_q, parse_q, output_q):
        self.download_q = download_q
        self.parse_q = parse_q
        self.output_q = output_q

    def regex_find_links(self, page, regex):
        br = Browser()
        br.set_handle_robots(False)
        try:
            br.open(page)
        except URLError:
            self.output_q.put('URL Not Found: %s' % page)
        #br.open('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')

        eng_all = re.compile(regex)
        #eng_all = re.compile('.*googlebooks-eng-all.*20120701.*')

        # Number of matching links found
        num_links = 0
        # Max length of printed links
        max_len = 0
        print page, regex
        for link in br.links():
            if eng_all.match(link.url):
                num_links += 1
                max_len = max(len(os.path.basename(link.url)), max_len)
                # Put the found links on the download queue
                self.download_q.put(link.url)
                # Put the output messages on the respective queue.
                # Implemented this way just in case this method is also threaded/multi-processed.
                # NOTE: Try to keep the number of output messages the same as the links
                self.output_q.put('Found Link: %s' % link.url)

        return num_links, max_len

    # Downloads links from the download queue to files and outputs progress.
    # NOTE: It is probably better if for each link something gets put on the parse queue, for now: None.
    def download(self, download_dir, num_links, max_len=50):
        # Setup browser
        br = Browser()
        br.set_handle_robots(False)

        # String formatting setup
        digits = len('%d' % num_links)
        start_time = time.time()

        # Iterate over all the links
        i = 0
        while not self.download_q.empty():
            i += 1
            link = self.download_q.get()
            download_start = time.time()
            file_name = os.path.basename(link)
            full_path = os.path.join(download_dir, file_name)

            # Check if the file already exists
            if os.path.exists(full_path):
                self.parse_q.put(None)
                self.output_q.put('File exists\t(%-*i of %i): %*s' % (digits, i, num_links, max_len,
                                                                      file_name))
                continue

            # Attempt to download the file
            try:
                self.output_q.put('Downloading\t(%-*i of %i): %*s' % (digits, i, num_links, max_len,
                                                                      file_name))
                br.retrieve(link, filename=full_path)
            except KeyboardInterrupt:
                self.output_q.put('User Interrupt, deleting last file: %*s' % (max_len, full_path))
                os.remove(full_path)
                #sys.exit(0)
                return
            self.parse_q.put(full_path)
            self.output_q.put('Finished   \t(%-*i of %i): %*s' % (digits, i, num_links, max_len,
                                                                 '%s MB in %s min' % ("{:7.2f}".format(float(
                                                                     os.stat(full_path).st_size)/1000000),
                                                                 "{:5.2f}".format((time.time() - download_start)/60))))

        self.output_q.put('Downloaded %i files to %s directory in %5.2f min' % (num_links, download_dir,
                                                                                (time.time() - start_time)/60))


class NGramViewerParser(object):
    """
    Downloads files synchronously based on regular expressions,
    but pushes finished files up to caller in a multiprocessing.Queue.
    Should be multi-process safe.
    """
    def __init__(self, parse_q, move_q, output_q):
        self.parse_q = parse_q
        self.output_q = output_q
        self.move_q = move_q

    def parse(self, num_files, max_len=50):
        # String formatting setup
        digits = len('%d' % num_files)

        for i in xrange(num_files):
            # Attempt to parse the file
            try:
                full_path = self.parse_q.get()
                # Check if the file exists
                if full_path and os.path.exists(full_path):
                    file_name = os.path.basename(full_path)
                    time.sleep(3)
                    self.output_q.put('Processed\t(%-*i of %i): %*s' % (digits, i+1, num_files, max_len, file_name))
            except KeyboardInterrupt:
                self.output_q.put('User Interrupt, cleaning up processing')
                #sys.exit(0)
                return

        self.output_q.put("Processing Finished")


def main(parsed_args):
    print parsed_args

    # Check if the user wants to use a temporary directory
    # for downloading. This will also trigger a move after completion
    if parsed_args.tmpdir:
        download_dir = parsed_args.tmpdir
    else:
        download_dir = parsed_args.destdir

    # Begin Multiprocessing, Initialize Queues.
    download_queue = multiprocessing.Queue()
    parse_queue = multiprocessing.Queue()
    move_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()

    dp = multiprocessing.Process()
    pp = multiprocessing.Process()

    if parsed_args.download:
        d = Downloader(download_queue, parse_queue, output_queue)

        # Find the links to download, don't forget to add the leading wildcard to the regex for the links
        print_time("Finding Links")
        num_links, max_len = d.regex_find_links(parsed_args.sitelink, ".*%s" % parsed_args.regex)

        # Pull messages off the output queue
        while not output_queue.empty():
            print_time(output_queue.get())

        answer = raw_input("\n\nDownload the above %i file(s)? (Y/N):  " % num_links)
        if not answer == 'N' and not answer == 'n':
            # Start Downloader
            dp = multiprocessing.Process(target=d.download, args=(download_dir, num_links, max_len))
            print_time("Downloading Links to %s" % download_dir)
            dp.start()

        # Start Parser
        p = NGramViewerParser(parse_queue, move_queue, output_queue)
        pp = multiprocessing.Process(target=p.parse, args=(num_links, max_len))
        pp.start()

        # This loop collects the output statements from the child processes
        # and prints them with a timestamp.
        timeout = None
        block = True
        while True:
            # Start with a blocking get call, but if both the child
            # processes are finished and the output queue is empty, then set a timeout.
            if not dp.is_alive() and not pp.is_alive() and output_queue.empty():
                timeout = 5
            try:
                out = output_queue.get(block, timeout)
            except Queue.Empty:
                break
            except KeyboardInterrupt:
                print_time("Keyboard Interrupt, terminating")
                break
            print_time(out)
            time.sleep(.5)

        # Collect the child processes
        dp.join()
        pp.join()
    # Figure out what to do here
    else:
        pass


if __name__ == "__main__":
    # Setup command line arguments
    parser = argparse.ArgumentParser(description='Scrape and process data from the Google NGram Viewer.')

    parser.add_argument('regex', action='store', nargs='*', default='googlebooks-eng-all.*2012.*',
                        help='regex expression to match to download and/or parse files')

    parser.add_argument('-s', '--sitelink', action='store',
                        default='http://storage.googleapis.com/books/ngrams/books/datasetsv2.html',
                        help='directory to store the ngram viewer data long term')

    parser.add_argument('-d', '--download', action='store_true',
                        help='scrape and download ngram viewer data')
    parser.add_argument('-dd', '--destdir', action='store', default='./',
                        help='directory to store the ngram viewer data long term')
    parser.add_argument('-td', '--tmpdir', action='store',
                        help='temporary directory for short term storage after downloading and through processing')

    parser.add_argument('-p', '--parse', action='store_true',
                        help="parse the ngram viewer data into a marshal'd counts file")
    parser.add_argument('-pf', '--parsefiles', action='store', nargs='+', type=argparse.FileType('rb'),
                        help="list of files to parse")

    parser.add_argument('-cd', '--countsdir', action='store', default='./',
                        help='directory to output the counts file')

    args = parser.parse_args()
    main(args)







    ## Uses a page, regular expression and path to download links
    #def download(self, download_dir):
    #    for i in xrange(n):
    #        start_time = time.time()
    #        time.sleep(1)
    #        file_name = self.download_q.get()
    #        self.output_q.put(["downloaded file%i" % i, time.time() - start_time])
    #        time.sleep(1)
    #        self.parse_q.put("file%i" % i)
    #
    #    self.output_q.put("Download Finished")
    #
