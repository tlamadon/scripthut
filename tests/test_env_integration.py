"""Integration tests: env resolution through RunManager + full config chain."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from scripthut.config_schema import (
    EnvRule,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
    WorkflowConfig,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.models import Run, TaskDefinition


def _make_config(backend_env=None, server_env=None, workflow_env=None):
    backend = SlurmBackendConfig(
        name="mercury",
        ssh=SSHConfig(host="mercury.example.com", user="alice"),
        env=backend_env or [],
    )
    workflow = WorkflowConfig(
        name="train",
        backend="mercury",
        command="cat tasks.json",
        env=workflow_env or [],
    )
    return ScriptHutConfig(
        backends=[backend],
        workflows=[workflow],
        env=server_env or [],
    )


def _make_run(workflow_name="train", backend_name="mercury"):
    return Run(
        id="r-7",
        workflow_name=workflow_name,
        backend_name=backend_name,
        created_at=datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc),
        items=[],
        max_concurrent=4,
        git_repo="git@github.com:org/repo.git",
        git_branch="main",
        commit_hash="abc123",
    )


def test_full_chain_layers_apply_in_order():
    config = _make_config(
        backend_env=[EnvRule(set={"SCRATCH": "/scratch/${USER}"})],
        server_env=[EnvRule(set={"LOG_LEVEL": "info"})],
        workflow_env=[EnvRule(set={"MODEL": "small"})],
    )
    mgr = RunManager(config=config, backends={})
    run = _make_run()
    task = TaskDefinition(
        id="t1", name="train", command="python train.py",
        env=[EnvRule(set={"BATCH_SIZE": "32", "USER": "alice"})],
    )

    env, _ = mgr._resolve_environment(run, task)

    # SCRIPTHUT_* seeded
    assert env["SCRIPTHUT_BACKEND"] == "mercury"
    assert env["SCRIPTHUT_WORKFLOW"] == "train"
    assert env["SCRIPTHUT_RUN_ID"] == "r-7"
    assert env["SCRIPTHUT_GIT_SHA"] == "abc123"
    # Layers
    assert env["SCRATCH"] == "/scratch/"  # USER set after, not yet — expected empty
    assert env["LOG_LEVEL"] == "info"
    assert env["MODEL"] == "small"
    assert env["USER"] == "alice"
    assert env["BATCH_SIZE"] == "32"


def test_full_chain_with_backend_conditional():
    """Module-load case: workflow has a per-backend init rule."""
    config = _make_config(
        workflow_env=[
            EnvRule(
                if_={"SCRIPTHUT_BACKEND": "mercury"},
                init="module load gcc/12 cuda/11",
            ),
            EnvRule(
                if_={"SCRIPTHUT_BACKEND": ["anvil", "delta"]},
                init="module load gcc cuda-toolkit",
            ),
        ],
    )
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(id="t1", name="x", command="true")

    _, init = mgr._resolve_environment(_make_run(backend_name="mercury"), task)
    assert init == "module load gcc/12 cuda/11"

    # Same task, different cluster — different init
    config.backends.append(
        SlurmBackendConfig(
            name="anvil",
            ssh=SSHConfig(host="anvil.example.com", user="alice"),
        )
    )
    _, init = mgr._resolve_environment(_make_run(backend_name="anvil"), task)
    assert init == "module load gcc cuda-toolkit"


def test_full_chain_expansion_uses_seeded_run_id():
    config = _make_config(
        workflow_env=[EnvRule(set={"OUT_DIR": "/data/${SCRIPTHUT_RUN_ID}"})],
    )
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(id="t1", name="x", command="true")

    env, _ = mgr._resolve_environment(_make_run(), task)
    assert env["OUT_DIR"] == "/data/r-7"


def test_task_layer_overrides_workflow_layer():
    config = _make_config(
        workflow_env=[EnvRule(set={"MODEL": "small"})],
    )
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(
        id="t1", name="x", command="true",
        env=[EnvRule(set={"MODEL": "large"})],
    )

    env, _ = mgr._resolve_environment(_make_run(), task)
    assert env["MODEL"] == "large"


def test_protected_scripthut_key_in_task_env_is_ignored():
    config = _make_config()
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(
        id="t1", name="x", command="true",
        env=[EnvRule(set={"SCRIPTHUT_RUN_ID": "hacked"})],
    )

    env, _ = mgr._resolve_environment(_make_run(), task)
    assert env["SCRIPTHUT_RUN_ID"] == "r-7"


def test_task_definition_from_dict_rejects_legacy_env_vars():
    import pytest

    with pytest.raises(ValueError, match="legacy 'environment' / 'env_vars'"):
        TaskDefinition.from_dict({
            "id": "t1", "name": "x", "command": "true",
            "env_vars": {"FOO": "bar"},
        })


def test_task_definition_from_dict_rejects_legacy_environment_ref():
    import pytest

    with pytest.raises(ValueError, match="legacy 'environment' / 'env_vars'"):
        TaskDefinition.from_dict({
            "id": "t1", "name": "x", "command": "true",
            "environment": "gpu-stack",
        })


def test_task_definition_from_dict_accepts_new_env():
    td = TaskDefinition.from_dict({
        "id": "t1", "name": "x", "command": "true",
        "env": [
            {"set": {"FOO": "bar"}},
            {"if": {"SCRIPTHUT_BACKEND": "mercury"}, "init": "module load cuda"},
        ],
    })
    assert len(td.env) == 2
    assert td.env[0].set == {"FOO": "bar"}
    assert td.env[1].if_ == {"SCRIPTHUT_BACKEND": "mercury"}
    assert td.env[1].init == "module load cuda"


def test_task_definition_roundtrip_env():
    rules = [
        EnvRule(set={"FOO": "bar"}),
        EnvRule(if_={"K": "v"}, append={"PATH": "/x"}, init="echo hi"),
    ]
    td = TaskDefinition(id="t1", name="x", command="true", env=rules)
    restored = TaskDefinition.from_dict(td.to_dict())
    assert len(restored.env) == 2
    assert restored.env[0].set == {"FOO": "bar"}
    assert restored.env[1].if_ == {"K": "v"}
    assert restored.env[1].append == {"PATH": "/x"}
    assert restored.env[1].init == "echo hi"


def test_env_groups_backend_defined_workflow_referenced():
    """A group defined at backend layer is referenceable from any later layer."""
    config = _make_config(
        workflow_env=[EnvRule(include=["gpu-stack"])],
    )
    # Mutate backend to add a group
    config.backends[0].env_groups = {
        "gpu-stack": [
            EnvRule(init="module load cuda"),
            EnvRule(append={"PATH": "/opt/cuda/bin"}),
        ],
    }
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(id="t1", name="x", command="true")

    env, init = mgr._resolve_environment(_make_run(), task)
    assert env["PATH"] == "/opt/cuda/bin"
    assert init == "module load cuda"


def test_env_groups_workflow_shadows_server():
    """A group defined at workflow layer with the same name shadows server group."""
    config = _make_config(
        workflow_env=[EnvRule(include=["gpu-stack"])],
    )
    config.env_groups = {
        "gpu-stack": [EnvRule(init="server-version")],
    }
    config.workflows[0].env_groups = {
        "gpu-stack": [EnvRule(init="workflow-version")],
    }
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(id="t1", name="x", command="true")

    _, init = mgr._resolve_environment(_make_run(), task)
    assert init == "workflow-version"


def test_env_groups_per_backend_with_same_include_name():
    """Different backends define different groups with the same name."""
    config = _make_config()
    config.backends[0].env_groups = {
        "gpu-stack": [EnvRule(init="module load cuda/11")],  # mercury
    }
    config.backends.append(
        SlurmBackendConfig(
            name="anvil",
            ssh=SSHConfig(host="anvil.example.com", user="alice"),
            env_groups={"gpu-stack": [EnvRule(init="module load cuda-toolkit")]},
        )
    )
    # Workflow includes "gpu-stack" — resolves differently per backend
    config.workflows[0].env = [EnvRule(include=["gpu-stack"])]
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(id="t1", name="x", command="true")

    _, init = mgr._resolve_environment(_make_run(backend_name="mercury"), task)
    assert init == "module load cuda/11"

    _, init = mgr._resolve_environment(_make_run(backend_name="anvil"), task)
    assert init == "module load cuda-toolkit"


def test_env_groups_guarded_include_through_full_chain():
    """A workflow's include can be gated by an if-clause against SCRIPTHUT_BACKEND."""
    config = _make_config(
        workflow_env=[
            EnvRule(
                if_={"SCRIPTHUT_BACKEND": "mercury"},
                include=["gpu-stack"],
            ),
        ],
    )
    config.env_groups = {
        "gpu-stack": [EnvRule(init="module load cuda")],
    }
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(id="t1", name="x", command="true")

    _, init = mgr._resolve_environment(_make_run(backend_name="mercury"), task)
    assert init == "module load cuda"

    # Add anvil backend; include should NOT fire there
    config.backends.append(
        SlurmBackendConfig(
            name="anvil",
            ssh=SSHConfig(host="anvil.example.com", user="alice"),
        )
    )
    _, init = mgr._resolve_environment(_make_run(backend_name="anvil"), task)
    assert init == ""


def test_parse_document_extracts_tasks_env_and_groups():
    """Top-level env: and env_groups: are pulled off the workflow JSON document."""
    data = {
        "tasks": [
            {"id": "t1", "name": "x", "command": "true"},
            {"id": "t2", "name": "y", "command": "true"},
        ],
        "env": [
            {"set": {"FROM_DOC": "1"}},
            {"if": {"SCRIPTHUT_BACKEND": "mercury"}, "init": "module load cuda"},
        ],
        "env_groups": {
            "julia": [{"init": "module load julia/1.12"}],
        },
    }
    tasks, doc_env, doc_groups = TaskDefinition.parse_document(data)
    assert [t.id for t in tasks] == ["t1", "t2"]
    assert len(doc_env) == 2
    assert doc_env[0].set == {"FROM_DOC": "1"}
    assert doc_env[1].if_ == {"SCRIPTHUT_BACKEND": "mercury"}
    assert "julia" in doc_groups
    assert doc_groups["julia"][0].init == "module load julia/1.12"


def test_parse_document_bare_list_form_has_empty_env():
    """Bare-list form is still accepted; env/env_groups default to empty."""
    tasks, doc_env, doc_groups = TaskDefinition.parse_document(
        [{"id": "t1", "name": "x", "command": "true"}],
    )
    assert [t.id for t in tasks] == ["t1"]
    assert doc_env == []
    assert doc_groups == {}


def test_doc_env_applies_to_every_task_in_the_run():
    """A doc-level set: rule shows up on every task without per-task duplication."""
    config = _make_config()
    mgr = RunManager(config=config, backends={})
    run = _make_run()
    run.doc_env = [EnvRule(set={"DATA_DIR": "/scratch/${USER}"})]
    task_a = TaskDefinition(id="a", name="a", command="true")
    task_b = TaskDefinition(id="b", name="b", command="true",
                            env=[EnvRule(set={"USER": "alice"})])

    # Task A has no per-task USER, so ${USER} expands to ""
    env, _ = mgr._resolve_environment(run, task_a)
    assert env["DATA_DIR"] == "/scratch/"

    # Task B sets USER itself, so its own DATA_DIR resolves at task time would
    # need to be set again (doc-level ${USER} was resolved against env-so-far).
    # Confirm the doc DATA_DIR is still there for task B (without USER expansion).
    env, _ = mgr._resolve_environment(run, task_b)
    assert env["USER"] == "alice"
    assert env["DATA_DIR"] == "/scratch/"


def test_doc_env_layer_overridden_by_task():
    """Per-task env wins over doc-level env."""
    config = _make_config()
    mgr = RunManager(config=config, backends={})
    run = _make_run()
    run.doc_env = [EnvRule(set={"MODE": "doc"})]
    task = TaskDefinition(id="t", name="t", command="true",
                          env=[EnvRule(set={"MODE": "task"})])

    env, _ = mgr._resolve_environment(run, task)
    assert env["MODE"] == "task"


def test_doc_env_groups_visible_to_task_includes():
    """A group defined in the JSON document is reachable from a task's include:."""
    config = _make_config()
    mgr = RunManager(config=config, backends={})
    run = _make_run()
    run.doc_env_groups = {
        "julia": [
            EnvRule(set={"JULIA_DEPOT_PATH": "/scratch/julia"}),
            EnvRule(init="module load julia/1.12"),
        ],
    }
    task = TaskDefinition(id="t", name="t", command="true",
                          env=[EnvRule(include=["julia"])])

    env, init = mgr._resolve_environment(run, task)
    assert env["JULIA_DEPOT_PATH"] == "/scratch/julia"
    assert init == "module load julia/1.12"


def test_doc_env_groups_shadow_workflow_config_groups():
    """When both layers define the same group name, the doc layer wins."""
    config = _make_config()
    config.workflows[0].env_groups = {
        "x": [EnvRule(set={"FROM": "workflow"})],
    }
    mgr = RunManager(config=config, backends={})
    run = _make_run()
    run.doc_env_groups = {
        "x": [EnvRule(set={"FROM": "doc"})],
    }
    task = TaskDefinition(id="t", name="t", command="true",
                          env=[EnvRule(include=["x"])])

    env, _ = mgr._resolve_environment(run, task)
    assert env["FROM"] == "doc"


def test_resolve_for_task_with_unknown_workflow_or_backend():
    """Resolution must not crash if a name is missing from config."""
    config = ScriptHutConfig()  # empty
    mgr = RunManager(config=config, backends={})
    task = TaskDefinition(
        id="t1", name="x", command="true",
        env=[EnvRule(set={"FOO": "bar"})],
    )
    # Use mock run with names that don't exist in config
    run = MagicMock(spec=Run)
    run.id = "r1"
    run.workflow_name = "missing"
    run.backend_name = "missing"
    run.created_at = datetime(2026, 5, 19, tzinfo=timezone.utc)
    run.git_repo = None
    run.git_branch = None
    run.commit_hash = None
    run.doc_env = []
    run.doc_env_groups = {}

    env, init = mgr._resolve_environment(run, task)
    assert env["FOO"] == "bar"
    assert env["SCRIPTHUT_BACKEND"] == "missing"
    assert init == ""
