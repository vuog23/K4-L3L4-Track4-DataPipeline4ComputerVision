# Lab execution notes

- Student ID used for every dataset split and experiment: `2A202602422`.
- The supplied recovery exercise calls `int(MSSV)`. This ID includes a letter, so the user approved using the numeric characters `2202602422` solely for the recovery-trip index (`% 26`). The complete ID remains `LAB_STUDENT_ID`.
- Extracted the supplied `dataset/day09-data.zip` at the repository root. `check_data.py` verified all six files against the standard manifest.
- Windows host scripts run with Python's UTF-8 mode (`-X utf8`). The supplied Bash wrapper's Docker commands can also be called directly from PowerShell.
- The dataset zip was originally tracked in the instructor repository. It is retained locally and excluded from the submitted checkout.
- Protected instructor files (`tests/`, `pipeline/verify.py`, `docker-compose.yml`) remain unchanged.
