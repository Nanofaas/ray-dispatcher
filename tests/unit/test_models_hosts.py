import textwrap

import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import Inventory, RemoteHost


def test_remotehost_valid_defaults():
    h = RemoteHost("10.0.0.11", user="ubuntu")
    assert h.slots == 1
    assert h.port == 22
    assert h.identity_file is None
    assert h.known_hosts_file == "~/.ssh/known_hosts"


@pytest.mark.parametrize("kwargs", [
    {"host": "", "user": "ubuntu"},
    {"host": "h", "user": ""},
    {"host": "h", "user": "u", "slots": 0},
    {"host": "h", "user": "u", "slots": -1},
    {"host": "h", "user": "u", "port": 0},
    {"host": "h", "user": "u", "port": 70000},
])
def test_remotehost_rejects(kwargs):
    with pytest.raises(ModelValidationError):
        RemoteHost(**kwargs)


def test_inventory_rejects_empty():
    with pytest.raises(ModelValidationError):
        Inventory(())


def test_inventory_rejects_duplicates():
    a = RemoteHost("h", user="u")
    b = RemoteHost("h", user="u")  # same (host, port, user)
    with pytest.raises(ModelValidationError):
        Inventory((a, b))


def test_inventory_rejects_same_host_name_different_port():
    # the backend keys runtimes/leases by host name alone, so the name must be unique
    with pytest.raises(ModelValidationError, match="duplicate host"):
        Inventory((
            RemoteHost("10.0.0.1", user="ubuntu", port=22),
            RemoteHost("10.0.0.1", user="ubuntu", port=2222),
        ))


def test_inventory_rejects_same_host_different_user():
    # the backend keys runtimes/leases by host name alone, so the name must be unique
    # regardless of user
    a = RemoteHost("h", user="u1")
    b = RemoteHost("h", user="u2")
    with pytest.raises(ModelValidationError, match="duplicate host"):
        Inventory((a, b))


def test_inventory_from_yaml(tmp_path):
    p = tmp_path / "hosts.yaml"
    p.write_text(textwrap.dedent("""
        hosts:
          - host: 10.0.0.11
            user: ubuntu
            slots: 2
          - host: 10.0.0.12
            user: ubuntu
            slots: 4
            port: 2222
    """))
    inv = Inventory.from_yaml(str(p))
    assert [h.host for h in inv.hosts] == ["10.0.0.11", "10.0.0.12"]
    assert inv.hosts[1].slots == 4
    assert inv.hosts[1].port == 2222


# FIX 2: non-dict hosts entry raises ModelValidationError
def test_inventory_from_yaml_rejects_nondict_entry(tmp_path):
    p = tmp_path / "hosts.yaml"
    p.write_text(textwrap.dedent("""
        hosts:
          - just-a-string
    """))
    with pytest.raises(ModelValidationError):
        Inventory.from_yaml(str(p))
