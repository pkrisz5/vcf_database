import argparse
import os
import io
import tarfile
import datetime


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action = "store",
                    help = "input coverage/vcf tar(.gz) file")
    parser.add_argument("-e", "--erronous", action = "store",
                    help = "input file listing wrong evaluations")
    parser.add_argument("-w", "--suffix_wrong", action = "store",
                     help = "outputfile suffix for wrong data", default = 'wrong')
    parser.add_argument("-g", "--suffix_good", action = "store",
                     help = "outputfile suffix for good data", default = 'good')
    args = parser.parse_args()

    assert os.path.exists(args.input), "File not found error: {0}".format(args.input)
    assert os.path.exists(args.erronous), "File not found error: {0}".format(args.erronous)
    fn_wrong = args.input.replace('.tar.gz', '-{}.tar.gz'.format(args.suffix_wrong))
    fn_good = args.input.replace('.tar.gz', '-{}.tar.gz'.format(args.suffix_good))
    extract_ena_run = lambda x: x.split('/')[-1].split('.')[0]

    wrong_tokens = set(map(lambda x: x.strip(), open(args.erronous).readlines()))
    print ("{0} wrong tokens read from file {1} count {2}".format(datetime.datetime.now(), args.erronous, len(wrong_tokens)))

    T = tarfile.open(args.input)
    print ("{0} open tar file {1}".format(datetime.datetime.now(), args.input))
    Tg = tarfile.open(fn_good, 'w:gz')
    print ("{0} open tar file {1}".format(datetime.datetime.now(), fn_good))
    Tw = tarfile.open(fn_wrong, 'w:gz')
    print ("{0} open tar file {1}".format(datetime.datetime.now(), fn_wrong))

    counter_w = 0
    counter_g = 0
    while True:
        ti = T.next()
        if ti is None:
            T.close()
            Tg.close()
            Tw.close()
            print ("{0} loop ends closed tarfiles".format(datetime.datetime.now()))
            break
        if not ti.isfile():
            continue

        try:
            runid = extract_ena_run(ti.name)
            buf = T.extractfile(ti)

            if runid in wrong_tokens:
                counter_w += 1
                To = Tw
            else:
                counter_g += 1
                To = Tg
            To.addfile(ti, fileobj = buf)
    
        except Exception as e:
            print ("{0} cannot parse file {1}: reason {2}".format(datetime.datetime.now(), ti.name, str(e)))
        finally:
            buf.close()

    print ("{0} end. Count good {1} / bad {2}".format(datetime.datetime.now(), counter_g, counter_w))

