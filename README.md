# Industrial Processing System

A thread-safe, asynchronous **producer-consumer** job processing system built in **C# / .NET**.

This project simulates an industrial processing environment with:

- concurrent job submission
- priority-based scheduling
- asynchronous execution
- idempotent job handling
- event-driven logging
- retry and abort behavior
- periodic XML reporting
- XML-based configuration

---

## Features

- **Thread-safe processing system**
- **Asynchronous job execution** using `Task`
- **Priority queue** where lower numeric priority means higher execution priority
- **XML configuration** for workers, capacity, and initial jobs
- **Prime** and **IO** job types
- **Idempotency** by `Job.Id`
- **Event-based logging** to file
- **Retry support** with final **ABORT** handling
- **Periodic XML reports**
- **Rotating retention** of the latest 10 reports
- **Producer-consumer console demo**
- **Automated tests** for key behaviors
