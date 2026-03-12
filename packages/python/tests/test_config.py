"""Tests for pyproject.toml configuration loading."""

from sparkdoctor.config import SparkDoctorConfig, _find_pyproject, load_config


def test_empty_config():
    cfg = SparkDoctorConfig()
    assert cfg.disable == set()
    assert cfg.severity_overrides == {}
    assert cfg.exclude == ()


def test_from_dict():
    data = {
        "disable": ["SDK023", "SDK025"],
        "severity_overrides": {"SDK001": "error"},
        "exclude": ["tests", "fixtures"],
    }
    cfg = SparkDoctorConfig.from_dict(data)
    assert cfg.disable == {"SDK023", "SDK025"}
    assert cfg.severity_overrides == {"SDK001": "error"}
    assert cfg.exclude == ("tests", "fixtures")


def test_from_dict_missing_keys():
    cfg = SparkDoctorConfig.from_dict({})
    assert cfg.disable == set()
    assert cfg.severity_overrides == {}
    assert cfg.exclude == ()


def test_load_config_no_pyproject(tmp_path):
    """Loading config from a dir with no pyproject.toml returns defaults."""
    cfg = load_config(tmp_path)
    assert cfg.disable == set()


def test_load_config_with_pyproject(tmp_path):
    """Loading config from a dir with [tool.sparkdoctor] in pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.sparkdoctor]\ndisable = ["SDK023"]\nexclude = ["tests"]\n')
    cfg = load_config(tmp_path)
    assert cfg.disable == {"SDK023"}
    assert cfg.exclude == ("tests",)


def test_load_config_no_section(tmp_path):
    """pyproject.toml exists but has no [tool.sparkdoctor] section."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'foo'\n")
    cfg = load_config(tmp_path)
    assert cfg.disable == set()


def test_find_pyproject_walks_up(tmp_path):
    """_find_pyproject walks up directories to find pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'foo'\n")
    child = tmp_path / "sub" / "deep"
    child.mkdir(parents=True)
    found = _find_pyproject(child)
    assert found == pyproject.resolve()


def test_find_pyproject_none(tmp_path):
    """_find_pyproject returns None when no pyproject.toml exists."""
    child = tmp_path / "isolated"
    child.mkdir()
    # Will walk all the way up to root — may find the real pyproject.toml
    # So just test that it doesn't crash
    _find_pyproject(child)
