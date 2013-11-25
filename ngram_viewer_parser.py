
import gzip
from time import time
from os.path import basename, exists


class NGramViewerParser(object):
    """
    Downloads files synchronously based on regular expressions,
    but pushes finished files up to caller in a multiprocessing.Queue.
    Should be multi-process safe.
    """
    def __init__(self, to_process_q, processed_q, output_q, download_event, parse_event):
        self.to_process_q = to_process_q
        self.output_q = output_q
        self.done_q = processed_q
        self.download_event = download_event
        self.parse_event = parse_event

    def parse(self, counts_dict, num_files, max_len=50):
        # String formatting setup
        digits = len('%d' % num_files)
        parse_start_time = time()

        i = 0
        while not self.download_event.is_set() or not self.to_process_q.empty():
            i += 1
            # Attempt to parse the file
            try:
                full_path = self.to_process_q.get()
                file_start_time = time()
                # Check if the file exists
                if full_path and exists(full_path):
                    ngram_file_name = basename(full_path)

                    try:
                        if ngram_file_name.endswith('.gz') or ngram_file_name.endswith('.zip'):
                            ngram_file = gzip.open(ngram_file_name, 'rb')
                        else:
                            ngram_file = open(ngram_file_name, 'rb')
                    except IOError:
                        self.output_q.put("(%-*i of %i) Couldn't Load:\t%*s"
                                          % (digits, i, num_files, max_len, ngram_file_name))
                        continue

                    readline = ngram_file.readline

                    line = readline().strip().split('\t')
                    j = 0
                    while len(line) is 4:
                        j += 1
                        local_sum = int(line[2])
                        next_line = readline().strip().split('\t')
                        while line[0] == next_line[0]:
                            local_sum += int(next_line[2])
                            next_line = readline().strip().split('\t')
                        counts_dict[line[0]] = local_sum
                        line = next_line
                        if j % 10000 == 0:
                            self.output_q.put(['(%-*i of %i) Processing:\t%*s'
                                              % (digits, i, num_files, max_len, '%i records' % j), '\r'])
                    self.output_q.put(['(%-*i of %i) Processed:\t%*s\n'
                                      % (digits, i, num_files, max_len,
                                         '%i records in %s min'
                                            % (j, "{:5.2f}".format((time() - file_start_time)/60))), '\r'])
                    ngram_file.close()
            except KeyboardInterrupt:
                self.output_q.put('User Interrupt, cleaning up')
                i -= 1
                break

        self.output_q.put("Processed %i files in %s min" % (i, "{:5.2f}".format((time() - parse_start_time)/60)))
        self.parse_event.set()