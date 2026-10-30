"""Unit tests for CLI runner commands."""

from typer.testing import CliRunner

from debezium_cdc_postgres_lab.cli import app

runner = CliRunner()


def test_cli_info():
    result = runner.invoke(app, ["info"])
    assert result.exit_code == 0
    assert "Debezium CDC PostgreSQL Lab" in result.stdout


def test_cli_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "seed" in result.stdout
    assert "consume" in result.stdout
    assert "slots" in result.stdout
    assert "connector" in result.stdout
    assert "schema" in result.stdout
