import argparse
import os
import io
import tarfile
import pandas
import psycopg2
import datetime
#import numpy
import gzip

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action = "store",
                    help = "tarfile folder (those including unnecessary bam files")
    parser.add_argument("-s", "--snapshot", action = "store",
                    help = "snapshot label")
    parser.add_argument("-v", "--output-vcf", action = "store",
                     help = "vcf archive folder")
    parser.add_argument("-c", "--output-coverage", action = "store",
                     help = "coverage archive folder")
    args = parser.parse_args()

    assert os.path.isdir(args.input), "Folder not found error: {0}".format(args.input)
    assert os.path.isdir(args.output_coverage), "Folder not found error: {0}".format(args.output_coverage)
    assert os.path.isdir(args.output_vcf), "Folder not found error: {0}".format(args.output_vcf)

    snapshot = args.snapshot
    Tc = tarfile.open(os.path.join(args.output_coverage, snapshot + "_coverage.tar.gz"), "w:gz")
    Tv = tarfile.open(os.path.join(args.output_vcf, snapshot + "_vcf.tar.gz"), "w:gz")

    files = []
    for r, ds, _ in os.walk(args.input):
        for d in ds:
            for R, _, fs in os.walk(os.path.join(r, d)):
                files.extend(map(lambda x: os.path.join(R, x), fs))

    for fi in files:
        T = tarfile.open(fi)
        print ("{0} open tar file {1}".format(datetime.datetime.now(), fi))

        while True:
            ti = T.next()
            if ti is None:
                T.close()
                print ("{0} loop ends closed tarfile".format(datetime.datetime.now()))
                break
            if not ti.isfile():
                continue
            if ti.name.endswith('.coverage.gz'):
                To = Tc
            elif ti.name.endswith('.annot.vcf.gz'):
                To = Tv
            else:
                continue

            buf = T.extractfile(ti)
            try:
                To.addfile(ti, buf)
            finally:
                buf.close()
                pass

    Tc.close()
    Tv.close()
    print ("{0} end".format(datetime.datetime.now()))


