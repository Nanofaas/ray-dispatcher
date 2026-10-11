import pytest

from it.unimib.data.nanofaas.ray_dispatcher.errors import ModelValidationError
from it.unimib.data.nanofaas.ray_dispatcher.models import Project, SecretFile


def test_secretfile_valid():
    s = SecretFile(source="~/lic/gurobi.lic", remote_name="gurobi.lic", env_var="GRB_LICENSE_FILE")
    assert s.mode == 0o600


@pytest.mark.parametrize(
    "kwargs",
    [
        {"source": "", "remote_name": "x"},
        {"source": "s", "remote_name": ""},
        {"source": "s", "remote_name": "sub/x"},  # not a bare filename
        {"source": "s", "remote_name": ".."},
        {"source": "s", "remote_name": "x", "env_var": "1BAD"},
    ],
)
def test_secretfile_rejects(kwargs):
    with pytest.raises(ModelValidationError):
        SecretFile(**kwargs)


@pytest.mark.parametrize("mode", [-1, 0o1000, 0.5, "600", True])
def test_secretfile_rejects_invalid_mode(mode):
    with pytest.raises(ModelValidationError):
        SecretFile(source="s", remote_name="x", mode=mode)


def test_project_valid_defaults():
    p = Project(
        path="../DFaasOptimizer",
        project_id="dfaas-optimizer",
        python="3.10.18",
        uv_version="0.11.25",
    )
    assert p.exclude == (".venv/", ".git/", "solutions/")
    assert p.secrets == ()


@pytest.mark.parametrize(
    "field,value",
    [
        ("python", "3.10"),  # not exact X.Y.Z
        ("python", "3.10.x"),
        ("uv_version", "0.11"),
        ("project_id", "bad id with spaces"),
        ("path", ""),
    ],
)
def test_project_rejects(field, value):
    base = dict(path="p", project_id="pid", python="3.10.18", uv_version="0.11.25")
    base[field] = value
    with pytest.raises(ModelValidationError):
        Project(**base)


def test_project_rejects_duplicate_secret_names():
    s1 = SecretFile(source="a", remote_name="dup")
    s2 = SecretFile(source="b", remote_name="dup")
    with pytest.raises(ModelValidationError):
        Project(
            path="p", project_id="pid", python="3.10.18", uv_version="0.11.25", secrets=(s1, s2)
        )


def test_project_rejects_duplicate_secret_env_vars():
    s1 = SecretFile(source="a", remote_name="a", env_var="TOKEN")
    s2 = SecretFile(source="b", remote_name="b", env_var="TOKEN")
    with pytest.raises(ModelValidationError, match="duplicate secret env_var"):
        Project(
            path="p", project_id="pid", python="3.10.18", uv_version="0.11.25", secrets=(s1, s2)
        )


# FIX 1: trailing-newline bypass regression
def test_project_rejects_trailing_newline_in_python():
    with pytest.raises(ModelValidationError):
        Project(path="p", project_id="pid", python="3.10.18\n", uv_version="0.11.25")
