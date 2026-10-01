# How should dispatch survive missed hooks, interruptions, and restarts?

Parent: ../map.md
Label: wayfinder:grilling
Type: grilling
Mode: HITL
Status: open
Assignee: unassigned
Blocked by: 03-runtime-and-coverage

## Question

Choose ownership and lifetime of the scheduler given one-shot startup and non-durable hooks. Decide reconciliation, duplicate-dispatch prevention after ambiguous submissions, concurrency/backpressure, cancellation, and resource retention. How do multiple handlers or sessions coordinate without blocking herdr plugin command capacity?
