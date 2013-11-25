import sys
import datetime
from os import walk, rename
from os.path import isfile, join, basename


# A function that prints to a writeable buffer with a human readable timestamp and an optional begin and end strings
# such as \r (return: for rewriting the same line) or \n
def print_time(string, begin='', end='\n'):
    sys.stderr.write("%s[%s]  %s%s" % (begin, str(datetime.datetime.now().time())[:8], string, end))
    sys.stderr.flush()


# Helper function to return a list of files in a directory that match a pre-compiled regex expressions.
def regex_find_files(compiled_re, rootdir, recursive=False):
    matched_files = []
    for (dirpath, dirnames, filenames) in walk(rootdir):
        for filename in filenames:
            if compiled_re.match(filename) and isfile(join(rootdir, dirpath, filename)):
                matched_files.append(join(rootdir, dirpath, filename))
        if not recursive:
            break
    return matched_files


def move_proc_safe(self, move_queue, prev_event, this_event, dest_dir, num_files, max_len=50):
    digits = len(str(num_files))
    i = 0
    while not prev_event.is_set() or not move_queue.empty():
        i += 1
        full_tmp_path = self.move_q.get()
        base = basename(full_tmp_path)
        full_dest_path = join(dest_dir, base)
        self.output_q.put('(%-*i of %i) Moving:\t%*s to %*s'
                          % (digits, i, num_files, max_len/2 - 2, base, max_len/2 - 2, dest_dir))
        rename(full_tmp_path, full_dest_path)

    this_event.set()