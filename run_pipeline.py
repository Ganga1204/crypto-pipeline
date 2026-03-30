# File: run_pipeline.py (in root crypto-pipeline/ folder)

import subprocess
import sys
from datetime import datetime, timezone

def run_step(script_path, step_name):
    print(f'\n{'='*50}')
    print(f'RUNNING: {step_name}')
    print(f'Time: {datetime.now(timezone.utc).isoformat()}')
    print('='*50)

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=False  # show output in real-time
    )

    if result.returncode != 0:
        print(f'ERROR: {step_name} failed!')
        sys.exit(1)

    print(f'SUCCESS: {step_name} completed')

if __name__ == '__main__':
    print('Starting full pipeline run...')
    run_step('src/ingest.py', 'Bronze ingestion')
    run_step('src/transform.py', 'Silver + Gold transformation')
    print('\nFull pipeline complete! Start dashboard with:')
    print('  streamlit run src/app.py')
