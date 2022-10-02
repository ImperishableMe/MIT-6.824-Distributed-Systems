package raft

import "fmt"

type LogEntry struct {
	Cmd interface{}
	Term int
}


type Log struct {
	LogList []LogEntry
	Ind0    int         // position of first entry of LogList in actual log
}

func (l *Log) String() string {
	var str string
	str = fmt.Sprintf("(st-en):(%d-%d), len-%d ||||| [", l.Ind0, l.lastIndex(), l.lastIndex() - l.Ind0 + 1)

	if len(l.LogList) <= 10 {
		for ind, val := range l.LogList {
			str += fmt.Sprintf("(%d, [%v,%v]),", ind+l.Ind0, val.Cmd, val.Term)
		}
	} else {
		for ind, val := range l.LogList[:3] {
			str += fmt.Sprintf("(%d, [%v,%v]),", ind+l.Ind0, val.Cmd, val.Term)
		}
		str += ".........."
		start := l.lastIndex() - l.Ind0 - 2
		for ind, val := range l.LogList[start:] {
			str += fmt.Sprintf("(%d, [%v,%v]),", ind+l.Ind0, val.Cmd, val.Term)
		}
	}
	str += "]"
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

// cutLog after index (keep index, drop everything after that)
// TODO: make sure memory is freed properly
func (l *Log) cutEnd(index int) {
	if index < l.Ind0 {
		fmt.Printf("ind:%d < log_st:%d\n", index, l.Ind0)
		panic("Big Log")
	}
	l.LogList = l.LogList[0: index - l.Ind0 + 1]
}

//func (l *Log) cutStart(index int) {
//	l.Ind0 += index
//	l.LogList = l.LogList[index:]
//}

// logs before index are trimmed
// TODO: make sure memory is freed properly
func (l *Log) cutStart(index int) {

	l.LogList = l.LogList[index-l.Ind0:]
	l.Ind0 = index
	if len(l.LogList) != l.lastIndex() - l.Ind0 + 1 {
		panic("Log cut is not consistent")
	}
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