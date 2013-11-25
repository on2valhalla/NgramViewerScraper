#! /usr/bin/python

__author__ = "Jason Mann <jcm2207@columbia.edu>"
__date__ = "$Nov 11, 2013"


import argparse
import multiprocessing
import Queue
import marshal
from os.path import join
from time import sleep
import re

from downloader import Downloader
from mannutils import print_time, regex_find_files
from ngram_viewer_parser import NGramViewerParser


def main(parsed_args):
    print parsed_args

    # Check if the user wants to use a temporary directory
    # for downloading. This will also trigger a move after completion
    if parsed_args.tmpdir:
        download_dir = parsed_args.tmpdir
    else:
        download_dir = parsed_args.destdir

    # Begin Multiprocessing, Initialize Queues and other multiprocessing setup.
    manager = multiprocessing.Manager()

    download_queue = multiprocessing.Queue()
    parse_queue = multiprocessing.Queue()
    move_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    counts_dict = manager.dict()

    download_event = multiprocessing.Event()
    parse_event = multiprocessing.Event()
    move_event = multiprocessing.Event()

    # Set the events just in case they are not used, unset them if you intend to use them
    download_event.set()
    parse_event.set()
    move_event.set()

    dp = multiprocessing.Process()
    pp = multiprocessing.Process()

    num_files = 0
    max_len = 0

    regex = re.compile(".*%s" % parsed_args.regex)

    if parsed_args.download:
        # Download files based on the regex and base site.
        d = Downloader(download_queue, parse_queue, output_queue, download_event)

        # Find the links to download, don't forget to add the leading wildcard to the regex for the links
        print_time("Finding Links")
        num_files, max_len = d.regex_find_links(parsed_args.sitelink, regex)

        # Add some to the max link size to give space
        max_len += 20

        # Pull messages off the output queue
        while not output_queue.empty():
            print_time(output_queue.get())

        answer = raw_input("\n\nDownload the above %i file(s)? (Y/N):  " % num_files)
        if not answer == 'N' and not answer == 'n':
            # Start Downloader
            download_event.clear()
            dp = multiprocessing.Process(target=d.download, args=(download_dir, num_files, max_len))
            print_time("Downloading Links to %s" % download_dir)
            dp.start()
    else:
        # If downloading was not specified, assume that the user wants to apply the regex to the destination dir.
        for full_path in regex_find_files(regex, download_dir, False):
            num_files += 1
            print_time("Matched file: %s" % full_path)
            parse_queue.put(full_path)

    if parsed_args.parse:
        # Open and load the counts file
        counts_file = open(parsed_args.countsfile, 'rb')
        try:
            counts_in_file = marshal.load(counts_file)
            counts_dict.update(counts_in_file)
        except IOError:
            print_time("could not load from the counts file provided, creating it\n")

        # Add the manually specified parse files.
        if parsed_args.parsefiles:
            for filename in parsed_args.parsefiles:
                print_time("Specified file: %s" % filename)
                parse_queue.put(join(download_dir, filename))
                num_files += 1

        # Start Parser
        parse_event.clear()
        p = NGramViewerParser(parse_queue, move_queue, output_queue, download_event, parse_event)
        pp = multiprocessing.Process(target=p.parse, args=(counts_dict, num_files, max_len))
        pp.start()

    if parsed_args.tmpdir:
        pass

    # This loop collects the output statements from the child processes
    # and prints them with a timestamp.
    use_carriage_return = False
    while not download_event.is_set() or not parse_event.is_set() or not output_queue.empty():
        try:
            out = output_queue.get()
            if isinstance(out, list):
                if use_carriage_return:
                    print_time(out[0], begin=out[1], end='')
                else:
                    print_time(out[0], begin='', end='')
                use_carriage_return = True
            else:
                if use_carriage_return:
                    print_time(out, begin='\n')
                else:
                    print_time(out)
                use_carriage_return = False
            sleep(.5)
        except Queue.Empty:
            break
        except KeyboardInterrupt:
            sleep(.5)
            while not output_queue.empty():
                out = output_queue.get()
                if isinstance(out, list):
                    print_time(out[0])
                else:
                    print_time(output_queue.get())
            print_time("User Interrupt, terminating")
            break

    # Collect the child processes
    if parsed_args.download:
        dp.join()
    if parsed_args.parse:
        pp.join()


if __name__ == "__main__":
    # Setup command line arguments
    parser = argparse.ArgumentParser(description='Scrape and process data from the Google NGram Viewer.')

    parser.add_argument('regex', action='store', nargs= '?', default='googlebooks-eng-all.*2012.*',
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
    parser.add_argument('-pf', '--parsefiles', action='store', nargs='+',
                        help="list of files to parse")

    parser.add_argument('-cf', '--countsfile', action='store', default='./ngram.counts',
                        help='directory to output the counts file')

    args = parser.parse_args()
    main(args)