import os
import argparse
import random


parser = argparse.ArgumentParser()
parser.add_argument('--db', type=str, default='mysql')
parser.add_argument('--output', type=str, default='demo')
parser.add_argument('--clear', action='store_true', default=False)

args = parser.parse_args()

db_name = str.lower(args.db)

if (args.clear):
    os.system('rm ./%s/%s_*' % (args.output, db_name))

test_suite_dir = db_name + "_tests/"
if db_name == "cockroach":
    test_suite_dir += 'testdata/logic_test'
elif db_name == "duckdb":
    test_suite_dir += 'sql'
elif db_name == "mysql":
    test_suite_dir += 'r'
elif db_name == "postgres":
    test_suite_dir += 'regress/expected'
elif db_name == "sqlite":
    test_suite_dir += ''
else:
    assert ('Not supported db')

tests_files = []
print("walk in " + test_suite_dir)
g = os.walk(test_suite_dir)
for path, dir_list, file_list in g:
    tests_files += [os.path.join(path, file_name) for file_name in file_list]

# print(tests_files[200:400])
ind = random.randint(0, len(tests_files))
cnt = 0

while (cnt < 20):
    print('iter' + str(cnt))
    with open(tests_files[ind], 'r') as f:
        test_length = len(f.readlines())
        if test_length > 100:
            break
        else:
            ind = random.randint(0, len(tests_files))
            cnt += 1

select_file = tests_files[ind]
file_name = args.output + '/'+ "-".join(select_file.split('/'))
os.system("cp %s ./%s" % (select_file, file_name ))
