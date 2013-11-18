
import datetime
import gzip
import marshal
import re
import os
import sys
import time

start_time = time.time()


def main(directory, regex, counts_file_name):
    global start_time
    counts = {}
    start_time = time.time()

    file_re = re.compile(regex)
    #file_re = re.compile('.*googlebooks-eng-all.*20120701.*')

    #print page, regex, path
    n = 0
    maxlen = 0
    file_list = []
    os.chdir(directory)
    for ngram_file_name in os.listdir("."):
        if file_re.match(ngram_file_name):
            n += 1
            maxlen = max(len(ngram_file_name), maxlen)
            file_list.append(ngram_file_name)
            print_time('Found File: %s\n' % ngram_file_name)

    answer = raw_input("\n\nAre you sure you want to process the above %i file(s)? (Y/N):  " % n)
    if answer == 'N' or answer == 'n':
        sys.exit(0)
    sys.stderr.write('\n')

    counts_file = open(counts_file_name, 'rb')
    try:
        counts = marshal.load(counts_file)
    except IOError:
        print_time("could not load from the counts file provided, creating it\n")

    digits = len('%d' % n)

    for f, ngram_file_name in enumerate(file_list):
        try:
            ngram_file = gzip.open(ngram_file_name, 'rb')
        except IOError:
            sys.stderr.write("Could not load ngrams gzip file\n")
            sys.exit(1)

        print_time("files loaded\n")

        readline = ngram_file.readline

        line = readline().strip().split('\t')
        i = 0
        while len(line) is 4:
            local_sum = int(line[2])
            next_line = readline().strip().split('\t')
            while line[0] == next_line[0]:
                local_sum += int(next_line[2])
                next_line = readline().strip().split('\t')
            counts[line[0]] = local_sum
            # Uncomment this if you want the counts in plain text to stdout
            # MUCH Slower
            #print "%s\t%i" % (line[0], local_sum)
            line = next_line
            if i % 1000 is 0:
                print_time('Processing file %-*i of %i: %i records' % (digits, f+1, n, i+1))
            i += 1

        sys.stderr.write('\n')
        print_time("%i records processed in file %s\n" % (i, ngram_file_name))

        ngram_file.close()

    print_time("%i records processed\n" % i)

    print_time("done processing\n")

    counts_file = open(counts_file_name, 'wb')
    marshal.dump(counts, counts_file)
    counts_file.close()

    print_time("done storing into %s\n" % counts_file_name)


def print_time(string):
    global start_time
    #print("%10f secs: %s" % (time.time() - start_time, string), end="")
    #sys.stderr.write('[%s] Downloading(%-*i of %i): %*s' % (str(disp_time().time())[:8], digits, i+1, n,
    #                                                       maxlen + 2, file_name))
    #sys.stderr.write("\r%s secs:\t%s" % ("{:6.1f}".format(time.time() - start_time), string))

    sys.stderr.write("\r[%s]\t%s" % (str(datetime.datetime.now().time())[:8], string))
    sys.stderr.flush()

if __name__ == "__main__": main(sys.argv[1], sys.argv[2], sys.argv[3])