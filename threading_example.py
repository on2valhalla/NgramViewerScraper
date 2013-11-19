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
import os
import sys
import time
import re


class Downloader(object):
    """
    Downloads files synchronously based on regular expressions,
    but pushes finished files up to caller in a multiprocessing.Queue.
    """
    def __init__(self, download_q, parse_q, output_q):
        self.download_q = download_q
        self.parse_q = parse_q
        self.output_q = output_q

    # Uses a page, regular expression and path to download links
    def download(self, n):
        br = Browser()
        br.set_handle_robots(False)
        #br.open('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')

        for i in xrange(n):
            start_time = time.time()
            time.sleep(2)
            self.output_q.put(["downloaded file%i" % i, time.time() - start_time])
            time.sleep(2)
            self.parse_q.put("file%i" % i)

        self.download_q.put("Download Finished")
        self.output_q.put("Download Finished")


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

    def parse(self, n):
        for i in xrange(n):
            file_name = self.parse_q.get()
            time.sleep(2)
            self.output_q.put("processed %s" % file_name)

        self.output_q.put("Processing Finished")



def print_time(string):
    sys.stderr.write("\r[%s]\t%s" % (str(datetime.datetime.now().time())[:8], string))
    sys.stderr.flush()


def main(parsed_args):
    print parsed_args
    download_queue = multiprocessing.Queue()
    parse_queue = multiprocessing.Queue()
    move_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()

    d = Downloader(download_queue, parse_queue, output_queue)
    p = NGramViewerParser(parse_queue, move_queue, output_queue)

    n = 10

    #procs = []
    dp = multiprocessing.Process(target=d.download, args=(n,))
    dp.start()
    #procs.append(dp)

    pp = multiprocessing.Process(target=p.parse, args=(n,))
    pp.start()
    #procs.append(pp)

    for i in xrange(2*n + 2):
        out = output_queue.get()
        print "[%s] %s" % (str(datetime.datetime.now().time())[:8], out)

    dp.join()
    pp.join()




















if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape and process data from the Google NGram Viewer.')

    parser.add_argument('regex', action='store', nargs='*', default='googlebooks-eng-all.*',
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