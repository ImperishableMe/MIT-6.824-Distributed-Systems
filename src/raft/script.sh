tasks="TestFigure8Unreliable2C TestUnreliableChurn2C TestReliableChurn2C"
#
#test2A="TestInitialElection2A TestReElection2A TestManyElections2A"
#test2B="TestRejoin2B TestBackup2B"
task_no="2[B|C|D]"
tasks=$(grep "$task_no(t" test_test.go | cut -d "(" -f 1 | cut -d " " -f 2 | xargs)
./dstest.py $tasks -p 9 -n $1

