# CS3700 Project 5 - Distributed, Replicated Key-Value Store

## High Level Approach

The code is constructed in a monolith approach, with the main function being a giant state machine where the program switches between the 3 possible states, and a sub-state and loop within each state. The Unixseqpacket stream is created on initialization, and is passed to individual specialized functions which may operate on the main state, sub-state or write to stream. Simple operations are sometimes left on the main function loop. 

In addition to the Raft spec, message passing ability about what put() requests failed where added, in the case where a put() request is written to a minority, but then later written over, the followers will inform the leader that there is a different entry being overwritten, and the leader will forward the fail() message back to client. Every log entry also contains the put() client and message ID, in case a new leader has to reply the fail() request which is different from the one who received the put() request.

Care is taken to minimize data duplication by passing reference / ownership to improve performance, except for logging purposes.

## Challenges faced

This project proved to be way more challenging then expected. Although the Raft paper is mostly understandable, it leaves out many important details of implementation such that there is a constant struggle of how to structure the project and what information needs to be passed in addition to the ones specified. 

In particular, the paper only stated in 1 line that log entries should start with an index of 1, which made little sense initially given computer programming traditions. Later on, I ran into many off-by-1 errors which are insanely difficult to debug given the complexity of async communication and lack of experience in hooking up a debugger. Only later did I realize index starting at 1 was used because of the need to handle the empty log situation. Many difficulties with large amount of failed put() requests were also faced initially due to the suggested Heartbeat times being totally inapplicable to the assignment.

It also made little sense that a leader would immediately revert to follower when receiving a vote-request from a candidate who clearly has older logs. I'm still not sure I'm handling that case correctly.

## Testing

Testing was done on Local Linux machine. Debugging statements were added to most functions and will be printed by giving the environmental variable `RUST_LOG=debug`.

## External Libraries Used

- clap: used for command line arguments parsing 
- unix_socket: used for establishing connection to the unix seqpacket sockets
- serde_json: used for easier parsing of json
- cute: used for easy list comprehension with Python-like syntax
- env_logger: used for generating debug output
