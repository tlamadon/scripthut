# Working from a project

Prepare the project directory on the cluster and install ScriptFlow in the compute
environment. A person can run ScriptFlow there by hand, or use ScriptHut:

```sh
scripthut scriptflow launch --backend cluster --workdir /data/project --flow analysis --run-id example --json
scripthut scriptflow discover --backend cluster --workdir /data/project --register
```

The existing git source configuration and synchronization/delivery helpers are
retained: URL plus optional deploy key, or `local_path` pushed through the backend
SSH transport; branch, clone directory and postclone settings remain. Inspect them
with `source list`, `source view NAME`, and `source sync NAME`. These commands do
not launch a workflow. Source-based ScriptFlow launch is not implemented yet;
use the explicit prepared `--workdir` form.
