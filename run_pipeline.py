import subprocess
import sys
from pathlib import Path

def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main() -> int:
    # 1) Load raw data
    run([sys.executable, "pipelines/load_raw.py"])

    # 2) dbt: build models + run tests
    run(["dbt", "build"],)  # must run from dbt project directory

    return 0

if __name__ == "__main__":
    # dbt must be executed in the dbt project directory
    dbt_dir = Path("Nyc_Taxi_Trips")
    if not dbt_dir.exists():
        print("ERROR: Could not find dbt project folder 'Nyc_Taxi_Trips'.")
        return_code = 1
    else:
        try:
            # load raw first
            subprocess.run([sys.executable, "pipelines/load_raw.py"], check=True)

            # dbt build inside the dbt project
            subprocess.run(["dbt", "build"], check=True, cwd=str(dbt_dir))

            # run GE checkpoint + build docs
            subprocess.run([sys.executable, "checks/gx_run_checkpoint_and_build_docs.py"], check=True)

            # copy docs to reports (Windows-friendly)
            Path("reports/ge_data_docs").mkdir(parents=True, exist_ok=True)
            # copy using robocopy for reliability on Windows
            subprocess.run(
                ["robocopy",
                 "great_expectations\\uncommitted\\data_docs\\local_site",
                 "reports\\ge_data_docs",
                 "/MIR"],
                check=False,
            )

            return_code = 0
        except subprocess.CalledProcessError as e:
            print(f"\nPipeline failed: {e}")
            return_code = 1

    raise SystemExit(return_code)
