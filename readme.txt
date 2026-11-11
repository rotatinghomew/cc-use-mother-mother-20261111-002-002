测试：python3 -m unittest discover -s tests/evidence_replay -p 'test_*.py'
构建：python3 -m compileall -q screening_evidence_crawler
验收：python3 -m screening_evidence_crawler.operator.crawl_cli fixtures/demo.json --db /tmp/screening_evidence_crawler-acceptance.sqlite3
