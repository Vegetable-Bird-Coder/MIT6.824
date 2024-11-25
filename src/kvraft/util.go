package kvraft

import "fmt"

type Stringer interface {
	String() string
}

func (commandRequest *CommandRequest) String() string {
	return fmt.Sprintf("(ClientID %d commandID %d op %s key %s value %s)", commandRequest.ClientID, commandRequest.CommandID, commandRequest.Op, commandRequest.Key, commandRequest.Value)
}

func (commandResponse *CommandResponse) String() string {
	return fmt.Sprintf("(Value %s Err %s)", commandResponse.Value, commandResponse.Error)
}
