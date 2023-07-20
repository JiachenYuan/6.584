# MIT - Distributed Systems (6.824)

This repo is a followup self-learning practice for all the labs in this course.

## Lab1: MapReduce (paper: [Google MapReduce 2004](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf))

### Goal
To create a library for application developer to easily build distributed programs that follows the general map+reduce semantics. By using the library, application developers can write sequential programs and expect them to work correctly on distributed machines with guarantee of correctness and efficiency.

### Challenges
1. Monitoring the states of the worker machines
2. Handling failures of mapper and reducer tasks

### High-Level Design
The coordinator maintains data structures that contain the following information of an entire MapReduce job:
1. Task Queue
2. Status of individual Map and Reduce tasks (Assigned? Finished? Stale?)
3. Locations of intermediate outputs

And it keeps a RPC server process answering requests from the worker nodes. It is responsible for cleaning and reassigning stale/failed tasks (definition in this project: not finished within around 10 seconds) to new workers. It also manages the transition from "mapping" phase to "reduce" phase.

The workers would periodically ping the coordinator asking for jobs and return bookkeeping information back to the coordinator (e.g. task ID, location of intermediate files, etc. )
