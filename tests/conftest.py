import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for rel in ("scripts", "src/spark_jobs", "src/common"):
    p = os.path.join(ROOT, rel)
    if p not in sys.path:
        sys.path.insert(0, p)
