"""Validate source branch names."""

from scripthut.sources.git import is_safe_branch_name


class TestIsSafeBranchName:
    def test_accepts_common_branch_names(self):
        for name in ("main", "feature/foo-bar", "v1.2", "user/x+y", "a"):
            assert is_safe_branch_name(name), name

    def test_rejects_shell_and_option_injection(self):
        for name in (
            "", "-rf", "a b", "a;b", "a&&b", "$(x)", "`x`", "a\nb",
            "a..b", "a'b", 'a"b', "~x", "a" * 201,
        ):
            assert not is_safe_branch_name(name), name
