package raft

import "fmt"

type LogEntry struct {
	Cmd interface{}
	Term int
}


type Log struct {
	LogList []LogEntry
	Ind0    int // last position, Ind0 = 0 means no previous Log
}

func (l Log) String() string {
	var str string
	//str = fmt.Sprintf("Ind0-%d", l.Ind0)
	for ind, val := range l.LogList {
		str += fmt.Sprintf("([%v,%v], %v),", val.Cmd, val.Term, ind+l.Ind0)
	}
	return str
}

func mkLogEmpty() Log{
	return Log{make([]LogEntry, 1), 0}
}

func mkLog(entries []LogEntry, ind0 int) Log{
	return Log{entries, ind0}
}

func (l *Log) appendList(entry []LogEntry) {
	l.LogList = append(l.LogList, entry...)
}

func (l *Log) append(entry LogEntry) {
	l.LogList = append(l.LogList, entry)
}

func (l *Log) start() int {
	return l.Ind0
}

func (l *Log) cutEnd(index int) {
	l.LogList = l.LogList[0: index - l.Ind0]
}

func (l *Log) cutStart(index int) {
	l.Ind0 += index
	l.LogList = l.LogList[index:]
}

func (l *Log) slice(ind int) []LogEntry {
	return l.LogList[ind-l.Ind0:]
}

func (l *Log) lastIndex() int {
	return l.Ind0 + len(l.LogList) - 1
}

func (l *Log) entry(ind int) *LogEntry {
	return &l.LogList[ind-l.Ind0]
}

func (l *Log) lastEntry() *LogEntry {
	return l.entry(l.lastIndex())
}