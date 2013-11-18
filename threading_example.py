#! /usr/bin/python

__author__ = "Jason Mann <jcm2207@columbia.edu"
__date__ = "$Nov 11, 2013"



import argparse
import sys
import getopt
import threading
import Queue
import time

#def usage():
#    sys.stderr.write("""
#        Usage: python ibm_mt_models.py -m=[1|2|U] [german_file.gz/original.de] [english_file.gz/scrambled.en]
#            Runs IBM Model 1 machine translation on two aligned corpora.
#            Takes in the two corpora as gzip'd files, and a choice of model 1 or 2
#            Utilizes the EM algorithm.\n""")
#
#
#if __name__ == "__main__":
#    if len(sys.argv) != 4:
#        usage()
#        sys.exit(1)
#    main(sys.argv[1][len('-m='):], sys.argv[2], sys.argv[3])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('integers', metavar='N', type=int, nargs='+',
                       help='an integer for the accumulator')
    parser.add_argument('--sum', dest='accumulate', action='store_const',
                       const=sum, default=max,
                       help='sum the integers (default: find the max)')

    args = parser.parse_args()
    print args.accumulate(args.integers)