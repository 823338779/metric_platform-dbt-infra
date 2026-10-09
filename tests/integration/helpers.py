import subprocess
from pathlib import Path

FIXTURE = Path(__file__).parents[1] / "fixtures" / "postgres_platform"
PROFILES = """postgres_platform:
  target: decoy
  outputs:
    decoy:
      type: postgres
      host: "{{ env_var('PLATFORM_TEST_PGHOST') }}"
      port: "{{ env_var('PLATFORM_TEST_PGPORT') | int }}"
      user: "{{ env_var('PLATFORM_TEST_PGUSER') }}"
      password: "{{ env_var('PLATFORM_TEST_PGPASSWORD') }}"
      dbname: "{{ env_var('PLATFORM_TEST_PGDATABASE') }}"
      schema: decoy_schema
      threads: 2
    postgres:
      type: postgres
      host: \"{{ env_var('PLATFORM_TEST_PGHOST') }}\"
      port: \"{{ env_var('PLATFORM_TEST_PGPORT') | int }}\"
      user: \"{{ env_var('PLATFORM_TEST_PGUSER') }}\"
      password: \"{{ env_var('PLATFORM_TEST_PGPASSWORD') }}\"
      dbname: \"{{ env_var('PLATFORM_TEST_PGDATABASE') }}\"
      schema: \"{{ env_var('DBT_PLATFORM_SCHEMA') }}\"
      threads: 2
"""


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()

