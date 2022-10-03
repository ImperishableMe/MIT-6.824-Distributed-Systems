tasks="TestManyPartitionsOneClient3A"
tasks="TestSpeed3A"
#
task_no="3[B|C|D]"
#tasks=$(grep "$task_no(t" test_test.go | cut -d "(" -f 1 | cut -d " " -f 2 | xargs)
./dstest.py $tasks -r -p 2 -n $1

