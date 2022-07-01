import argparse
import os
import io
import tarfile
import pandas
import psycopg2
import datetime
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

    files = set()
    for r, ds, _ in os.walk(args.input):
        for d in ds:
            for R, _, fs in os.walk(os.path.join(r, d)):
                files.update(map(lambda x: os.path.join(R, x), fs))

    for fi in files:
        try:
            T = tarfile.open(fi)
            print ("{0} open tar file {1}".format(datetime.datetime.now(), fi))
        except Exception as e:
            print ("{0} EE opening {1} {2}".format(datetime.datetime.now(), fi, e))
            continue

        found_mask = 0b00
        while True:
            try:
                ti = T.next()
            except Exception as e:
                print ("{0} EE {1}".format(datetime.datetime.now(), e))
                T.close()
                break

            if ti is None:
                T.close()
                print ("{0} loop ends closed tarfile".format(datetime.datetime.now()))
                break
            if not ti.isfile():
                continue
            is_zipped = True
            if ti.name.endswith('.coverage.gz'):
                this = 0b01
                To = Tc
            elif ti.name.endswith('.annot.vcf.gz'):
                this = 0b10
                To = Tv
            elif ti.name.endswith('.coverage'):
                this = 0b01
                To = Tc
                is_zipped = False
            elif ti.name.endswith('.annot.vcf'):
                this = 0b10
                To = Tv
                is_zipped = False
            else:
                continue

            if found_mask & this:
                print ("{0} skipping {1}, coz former instance already found".format(datetime.datetime.now(), ti.name))
                continue
            found_mask |= this

            print ("{0} found {1}".format(datetime.datetime.now(), ti.name))
            buf = T.extractfile(ti)
            try:
                if not is_zipped:
                    ti = tarfile.TarInfo(name = ti.name + '.gz')
                    raw = gzip.compress(buf.read())
                    buf = io.BytesIO(raw)
                    ti.size = len(raw)
                To.addfile(ti, buf)
                print ("{0} added {1}".format(datetime.datetime.now(), ti.name))
            except Exception as e:
                print ("{0} EE {1} -- {2}".format(datetime.datetime.now(), e, ti.name))
            finally:
                buf.close()
                pass

    Tc.close()
    Tv.close()
    print ("{0} end".format(datetime.datetime.now()))


