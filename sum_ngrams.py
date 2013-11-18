import gzip
import marshal
import sys
import time

start_time = time.time()

def main(ngram_file_name, counts_file_name):
    global start_time
    counts = {}
    start_time = time.time()
    ngram_file = gzip.open(ngram_file_name, 'rb')
    try:
        counts_file = open(counts_file_name, 'rb')
        counts = marshal.load(counts_file)
    except IOError:
        sys.stderr.write("could not load from the counts file provided, creating it\n")

    print_time("files loaded")

    readline = ngram_file.readline

    line = readline().strip().split('\t')
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

    print_time("done processing")

    counts_file = open(counts_file_name, 'wb')
    marshal.dump(counts, counts_file)
    counts_file.close()
    ngram_file.close()

    print_time("done storing")


def print_time(string):
    global start_time
    sys.stderr.write("%-25s %10f secs total\n" % (string, time.time() - start_time))

if __name__ == "__main__": main(sys.argv[1], sys.argv[2])