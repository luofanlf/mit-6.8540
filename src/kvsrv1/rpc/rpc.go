package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	RequestId string
	Key       string
	Value     string
	Version   Tversion
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	RequestId string
	Key       string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
