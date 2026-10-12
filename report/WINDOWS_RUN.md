# Re-run this lab from PowerShell

Run commands from the repository root. `.env` must contain `LAB_STUDENT_ID=2A202602422`. Data stays local and is excluded from Git.

```powershell
Expand-Archive -LiteralPath dataset/day09-data.zip -DestinationPath . -Force
docker compose --profile orchestrate up -d --build
$labPython = '/home/airflow/lab-venv/bin/python'
docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli step all --reset
docker compose exec -T -w /opt/lab airflow $labPython -m unittest discover -s tests
```

Use `step 1` through `step 5` in place of `step all --reset` to inspect individual stages. The four experiments run sequentially after a successful full pipeline:

```powershell
docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli exp break-gate
docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli exp idempotency
docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli exp threshold
docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli exp leakage
```

Airflow: <http://localhost:8080> (`admin` / `admin`). MinIO: <http://localhost:9001> (`minioadmin` / `minioadmin`). These are the instructor's local lab accounts.

Recovery uses `trip-07774`. Add `LAB_FAIL_ONCE=trip-07774` to `.env`, apply `docker compose --profile orchestrate up -d`, trigger `day09_reset` and wait for success, then trigger `day09_video_pipeline`. Select failed `silver_frames` map index 20 and clear that instance with Downstream enabled. The preview should contain eight instances (one extraction, seven blocked dependents). Do not clear the 25 successful extraction instances. After success remove `LAB_FAIL_ONCE`, apply Compose again, and verify.

Metabase dashboard: <http://localhost:3000/dashboard/2> (`lab@example.com` / `Day9-lab-pass!`). Start it with `docker compose --profile bi up -d`. The supplied `scripts/setup_metabase.py` provisions the standard dashboard; `report/metabase_questions.py` adds the personal and bonus questions. Run these host scripts with Python 3 in UTF-8 mode; alternatively use the bundled Python already installed on this computer:

```powershell
$hostLabPython = 'C:\Users\VUONG\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
& $hostLabPython -X utf8 scripts/setup_metabase.py
& $hostLabPython -X utf8 report/metabase_questions.py
```

Finish with a clean run, collect its manifest details, save verify output, and run the original submission checker:

```powershell
docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli step all --reset
docker compose exec -T -w /opt/lab airflow $labPython report/collect_snapshot.py final
docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli verify |
    Set-Content -LiteralPath report/verify.txt -Encoding utf8
& $hostLabPython -X utf8 scripts/check_submission.py
```

The direct Docker commands are equivalent to the supplied `scripts/lab.sh` wrapper. Saving verify directly avoids trying to execute a Unix shebang from Windows Python. Keep the protected files unchanged; `.gitattributes` ensures their bytes match the instructor's LF checksums. If the local dataset zip is absent, `scripts/fetch_data.py` downloads and validates the standard package.
