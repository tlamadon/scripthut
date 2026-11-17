# ScriptHut repository conventions

ScriptHut launches ScriptFlow controller/cleanup jobs and observes ScriptFlow run
records. ScriptFlow owns all task execution. Shared infrastructure includes SSH,
Slurm/PBS read-only job listing, configuration, git source delivery, terminal
attachment and daemon lifecycle.

- Keep one server polling loop. SSE listeners wait for its events; they do not poll.
- Run ScriptFlow only inside compute batch jobs. Login-node operations use plain
  shell, Git and scheduler queries through the command transport.
- Scheduler mutation belongs only in `scheduler.py` and `scriptflow_launch.py`.
- Keep workflow state, controller state and observation freshness separate.
- Use async I/O, typed configuration models and pytest with fake transports.
- Update README, docs, example configuration and agent guidance when their public
  behavior changes. No legacy task models or storage may be introduced.
