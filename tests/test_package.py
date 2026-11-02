import orderly


def test_version_matches_the_project() -> None:
    import tomllib
    from pathlib import Path

    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())["project"]
    assert orderly.__version__ == project["version"]
