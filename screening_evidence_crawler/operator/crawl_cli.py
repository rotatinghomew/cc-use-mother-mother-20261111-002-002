from __future__ import annotations
import argparse
import json
from screening_evidence_crawler.service import run_fixture

def main() -> int:
    parser=argparse.ArgumentParser(description='癌症筛查网页证据留存网')
    parser.add_argument("fixture")
    parser.add_argument("--db",default="/tmp/screening_evidence_crawler.sqlite3")
    args=parser.parse_args()
    result=run_fixture(args.fixture,args.db)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 0

if __name__ == "__main__": raise SystemExit(main())
