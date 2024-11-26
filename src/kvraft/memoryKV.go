package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func newMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}

func (kv *KVServer) updateStateMachine(op *Op) (string, Err) {
	switch op.Type {
	case OpGet:
		return kv.stateMachine.Get(op.Key)
	case OpPut:
		return "", kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		return "", kv.stateMachine.Append(op.Key, op.Value)
	default:
		return "", ErrWrongOpType
	}
}

func (kv *KVServer) loadStateMachine(data []byte) {
	kv.stateMachine = newMemoryKV()

	if data == nil || len(data) < 1 {
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	if decoder.Decode(kv.stateMachine) != nil {
		DPrintf("read persist error\n")
	} else {
		DPrintf("Reboot Event: reload state machine\n")
	}
}
