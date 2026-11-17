"""Sentinels for existing ScriptFlow isolation tests after legacy deletion."""
class RunManager:
    def __init__(self, *args, **kwargs):
        raise AssertionError("Legacy manager must not exist")

class RunStorageManager:
    def __init__(self, *args, **kwargs):
        raise AssertionError("Legacy storage must not exist")
