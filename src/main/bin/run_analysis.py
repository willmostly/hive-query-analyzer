from subprocess import PIPE, Popen
import json
from os import listdir
from optparse import OptionParser


executable_path = '/Users/wmorrison/workspace/repos/willmostly/hive-query-analyzer/target/hive-query-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar'

def create_parser():
    parser = OptionParser(prog='run_analysis', usage='usage: %prog [options]')
    parser.add_option('-r', '--print-human-readable', action='store_true', default=False, help='print human readable summaries for each HQL')
    parser.add_option('-d', '--hql-dir', metavar='DIR', help='path to a directory containing HQL files, one HQL per file')
    parser.add_option('--hql-mainifest', metavar='DIR', help='path to a file containing paths of HQL files, one HQL per file')
    parser.add_option('--hql-file', metavar='DIR', help='path to a file containing a single HQL')
    return parser

def main():
    parser = create_parser()

    (options, args) = parser.parse_args()
    if options.hql_dir == None and options.hql_manifest == None and options.hql_file == None:
        parser.error("Must specify one of hql_dir, hql_manifest, or hql_file")

    summaries = {}

    if options.hql_dir != None:
        hql_dir = options.hql_dir
        files = listdir(hql_dir)
        for f in files:
            hql = ' '.join([line.strip() for line in  open(hql_dir + '/' + f)])
            proc = Popen(['java', '-jar', executable_path, hql], stdout = PIPE, stderr = PIPE)
            out, err = proc.communicate()
            try:
                summaries[f] = json.loads(out)
            except:
                print (out)
                print (err)

    print (json.dumps(summaries))

if __name__ == '__main__':
    main()
