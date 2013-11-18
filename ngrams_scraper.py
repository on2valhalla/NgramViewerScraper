import datetime
from mechanize import Browser
import os
import sys
import time
import re


def main(page, regex, path):
    start_time = time.time()
    br = Browser()
    br.set_handle_robots(False)
    br.open(page)
    #br.open('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')

    eng_all = re.compile(regex)
    #eng_all = re.compile('.*googlebooks-eng-all.*20120701.*')

    #print page, regex, path
    n = 0
    maxlen = 0
    link_list = []
    for link in br.links():
        if eng_all.match(link.url):
            n += 1
            maxlen = max(len(os.path.basename(link.url)), maxlen)
            link_list.append(link.url)
            sys.stderr.write('Found Link: %s\n' % link.url)

    answer = raw_input("\n\nAre you sure you want to download the above %i file(s)? (Y/N):  " % n)
    if answer == 'N' or answer == 'n':
        sys.exit(0)

    sys.stderr.write('\n\nDownloading files to: %s\n' % path)

    digits = len('%d' % n)
    disp_time = datetime.datetime.now

    for i, link in enumerate(link_list):
        download_start = time.time()
        file_name = os.path.basename(link)
        full_path = os.path.join(path, file_name)
        if os.path.exists(full_path):
            sys.stderr.write('%s exists, not downloading\n' % full_path)
            continue
        try:
            sys.stderr.write('[%s] Downloading(%-*i of %i): %*s' % (str(disp_time().time())[:8], digits, i+1, n,
                                                                   maxlen + 2, file_name))
            br.retrieve(link, filename=full_path)
        except:
            sys.stderr.write('\n\nSomething happened, deleting last file: %s\n' % full_path)
            os.remove(full_path)
            sys.exit(0)
        sys.stderr.write(' of size %s MB in %5.2f min\n' % ("{:7.2f}".format(float(os.stat(full_path).st_size)/1000000),
                                                            (time.time() - download_start)/60))

    sys.stderr.write('\ndownloaded %i files to %s directory in %15f seconds\n' % (n, path, time.time()-start_time))


if __name__ == "__main__": main(sys.argv[1], sys.argv[2], sys.argv[3])