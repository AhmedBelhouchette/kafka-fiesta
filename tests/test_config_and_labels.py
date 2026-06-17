"""Config defaults/overrides and the failure-horizon labelling logic."""
import importlib


def test_config_env_override(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "testhost:1234")
    monkeypatch.setenv("INFLUXDB_BUCKET", "test-bucket")
    import config
    importlib.reload(config)
    assert config.KAFKA_BOOTSTRAP == "testhost:1234"
    assert config.INFLUXDB_BUCKET == "test-bucket"
    assert config.INFLUXDB_URL  # has a sensible default


def test_config_defaults(monkeypatch):
    for var in ("KAFKA_BOOTSTRAP", "INFLUXDB_URL", "INFLUXDB_BUCKET"):
        monkeypatch.delenv(var, raising=False)
    import config
    importlib.reload(config)
    assert config.KAFKA_BOOTSTRAP == "kafka:29092"
    assert config.INFLUXDB_URL.startswith("http")


def test_failure_horizon_labels():
    import train_model as tm
    assert tm.label_at(None, 5) == 0                       # never fails -> healthy
    assert tm.label_at(10, 10 - tm.CRIT_H) == 2            # within critical horizon
    assert tm.label_at(10, 10 - tm.CRIT_H - 1) == 1        # within warning horizon
    assert tm.label_at(100, 100 - tm.WARN_H - 5) == 0      # far from failure
