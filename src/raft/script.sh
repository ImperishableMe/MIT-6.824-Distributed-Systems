tasks="TestManyElections2A TestRejoin2B TestBackup2B TestUnreliableAgree2C TestFigure8Unreliable2C TestPersist12C TestUnreliableChurn2C TestReliableChurn2C"

tasks="TestFigure8Unreliable2C"
test2A="TestInitialElection2A TestReElection2A TestManyElections2A"
test2B="TestRejoin2B TestBackup2B"

./dstest.py $tasks -p 10 -n 2000

