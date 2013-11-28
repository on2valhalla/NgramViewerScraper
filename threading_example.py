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
from mannutils import print_time, regex_find_files, move_proc_safe
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

    num_workers = multiprocessing.cpu_count()

    download_queue = multiprocessing.Queue()
    parse_queue = multiprocessing.Queue()
    move_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    counts_dict = manager.dict()

    download_event = multiprocessing.Event()
    parse_events = [multiprocessing.Event() for _ in xrange(num_workers)]
    move_event = multiprocessing.Event()

    # Set the events just in case they are not used, unset them if you intend to use them
    download_event.set()
    for e in parse_events:
        e.set()
    move_event.set()

    dp = multiprocessing.Process()
    pp = []
    mp = multiprocessing.Process()

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
        max_len *= 2

        # Pull messages off the output queue
        while not output_queue.empty():
            print_time(output_queue.get())

        answer = raw_input("\n\nDownload the above %i file(s)? (Y/N):  " % num_files)
        if not answer == 'N' and not answer == 'n':
            # Start Downloader
            download_event.clear()
            if parsed_args.tmpdir:
                dp = multiprocessing.Process(target=d.download, args=(parsed_args.tmpdir, num_files, max_len,
                                                                      parsed_args.destdir))
            else:
                dp = multiprocessing.Process(target=d.download, args=(download_dir, num_files, max_len))
            print_time("Downloading Links to %s" % download_dir)
            dp.start()
    else:
        # If downloading was not specified, assume that the user wants to apply the regex to the destination dir.
        for full_path in regex_find_files(regex, parsed_args.tmpdir, False):
            num_files += 1
            print_time("Matched file: %s" % full_path)
            parse_queue.put(full_path)
        for full_path in regex_find_files(regex, parsed_args.destdir, False):
            num_files += 1
            print_time("Matched file: %s" % full_path)
            parse_queue.put(full_path)

    if parsed_args.parse:
        # Open and load the counts file
        try:
            counts_file = open(parsed_args.countsfile, 'rb')
            counts_in_file = marshal.load(counts_file)
            counts_dict.update(counts_in_file)
            counts_file.close()
        except IOError:
            print_time("Could not load from the counts file provided, will create it at end")

        # Add the manually specified parse files.
        if parsed_args.parsefiles:
            for filename in parsed_args.parsefiles:
                print_time("Specified file: %s" % filename)
                parse_queue.put(join(download_dir, filename))
                num_files += 1

        # Start Parser
        p = NGramViewerParser(parse_queue, move_queue, output_queue, download_event)
        pp = []
        for i in xrange(num_workers):
            parse_events[i].clear()
            proc = multiprocessing.Process(target=p.parse, args=(counts_dict, num_files, parse_events[i], max_len))
            proc.start()
            pp.append(proc)

    if parsed_args.tmpdir:
        move_event.clear()
        if parsed_args.parse:
            mp = multiprocessing.Process(target=move_proc_safe, args=(move_queue, output_queue, parse_events,
                                                                      move_event, parsed_args.destdir, num_files,
                                                                      max_len))
            mp.start()
        elif parsed_args.download:
            mp = multiprocessing.Process(target=move_proc_safe, args=(parse_queue, output_queue, [download_event],
                                                                      move_event, parsed_args.destdir, num_files,
                                                                      max_len))
            mp.start()

    # This loop collects the output statements from the child processes
    # and prints them with a timestamp.
    use_carriage_return = False
    while not download_event.is_set() or any(not e.is_set() for e in parse_events) or not move_event.is_set()\
            or not output_queue.empty():
        try:
            out = output_queue.get()
            if isinstance(out, list):
                if use_carriage_return:
                    print_time(out[0], begin=out[1], end=out[2])
                else:
                    print_time(out[0], begin='', end='')
                use_carriage_return = True
            else:
                #if use_carriage_return:
                #    print_time(out, begin='\n')
                #else:
                print_time(out)
                use_carriage_return = False
            sleep(.1)
        except KeyboardInterrupt:
            sleep(.1)
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
        for proc in pp:
            proc.join()
    if parsed_args.tmpdir:
        mp.join()

    try:
        counts_file = open(parsed_args.countsfile, 'wb')
        marshal.dump(dict(counts_dict), counts_file)
    except IOError:
        print_time("Could not store to the counts file")


if __name__ == "__main__":
    # Setup command line arguments
    parser = argparse.ArgumentParser(description='Scrape and process data from the Google NGram Viewer.')

    parser.add_argument('regex', action='store', nargs= '?', default='googlebooks-eng-all-1gram.*2012.*-[xyz].*',
                        help='regex expression to match to download and/or parse files')

    parser.add_argument('-s', '--sitelink', action='store',
                        default='http://storage.googleapis.com/books/ngrams/books/datasetsv2.html',
                        help='directory to store the ngram viewer data long term')

    parser.add_argument('-d', '--download', action='store_true',
                        help='scrape and download ngram viewer data')
    parser.add_argument('-dd', '--destdir', action='store', default='./',
                        help='directory to store the ngram viewer data long term')
    parser.add_argument('-td', '--tmpdir', action='store', default=None,
                        help='temporary directory for short term storage after downloading and through processing')

    parser.add_argument('-p', '--parse', action='store_true',
                        help="parse the ngram viewer data into a marshal'd counts file")
    parser.add_argument('-pf', '--parsefiles', action='store', nargs='+',
                        help="list of files to parse")

    parser.add_argument('-cf', '--countsfile', action='store', default='./ngram.counts',
                        help='directory to output the counts file')

    args = parser.parse_args()
    main(args)