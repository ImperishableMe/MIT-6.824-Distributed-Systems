# tasks="TestManyPartitionsOneClient3A"
tasks="TestSpeed3A TestPersistPartitionUnreliableLinearizable3A"
# tasks="TestSpeed3B"
#
task_no="3[A|B]"
tasks=$(grep "$task_no(t" test_test.go | cut -d "(" -f 1 | cut -d " " -f 2 | xargs)
./dstest.py $tasks -p 9 -n $1

