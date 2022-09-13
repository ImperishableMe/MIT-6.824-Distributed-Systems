#tasks="TestManyElections2A TestRejoin2B TestBackup2B TestUnreliableAgree2C TestFigure8Unreliable2C TestPersist12C TestUnreliableChurn2C TestReliableChurn2C"
#
## tasks="TestFigure8Unreliable2C"
#test2A="TestInitialElection2A TestReElection2A TestManyElections2A"
#test2B="TestRejoin2B TestBackup2B"
task_no="2[C]"
tasks=$(grep "$task_no(t" test_test.go | cut -d "(" -f 1 | cut -d " " -f 2 | xargs)
tasks="TestSnapshotBasic2D"
./dstest.py $tasks -p 5 -n $1

