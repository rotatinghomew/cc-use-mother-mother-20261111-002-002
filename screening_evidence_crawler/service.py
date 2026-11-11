from __future__ import annotations
import json
import uuid
from pathlib import Path
from typing import Any, Mapping
from screening_evidence_crawler.runtime import DomainError, ProcessingContext, SQLiteLedger, fingerprint, normalize_text
from screening_evidence_crawler.frontier.url_budget import build_processor as url_budget_factory
from screening_evidence_crawler.transport.replay_fetcher import build_processor as replay_fetcher_factory
from screening_evidence_crawler.canonical.html_record import build_processor as html_record_factory
from screening_evidence_crawler.claims.screening_claims import build_processor as screening_claims_factory
from screening_evidence_crawler.diff.semantic_change import build_processor as semantic_change_factory
from screening_evidence_crawler.provenance.evidence_chain import build_processor as evidence_chain_factory
from screening_evidence_crawler.review.change_queue import build_processor as change_queue_factory
from screening_evidence_crawler.snapshots.version_store import build_processor as version_store_factory

PROCESSOR_FACTORIES = [url_budget_factory, replay_fetcher_factory, html_record_factory, screening_claims_factory, semantic_change_factory, evidence_chain_factory, change_queue_factory, version_store_factory]

class ApplicationService:
    def __init__(self, database: str | Path):
        self.ledger=SQLiteLedger(database)
        self.processors=[factory() for factory in PROCESSOR_FACTORIES]

    def validate_input(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping): raise DomainError("record must be object")
        result=dict(record)
        result["case_id"]=normalize_text(result.get("case_id"),"case_id",96)
        if not result["case_id"]: raise DomainError("case_id is required")
        result.setdefault("subject_id",result["case_id"])
        result.setdefault("category","screening")
        result.setdefault("region","synthetic-region")
        result.setdefault("event_date","2026-11-11")
        result.setdefault("status","new")
        result.setdefault("count",1)
        result.setdefault("total",1)
        result.setdefault("attributes",{"source":"synthetic"})
        result.setdefault("tags",["offline","fixture"])
        result.setdefault("source_system","local-fixture")
        result.setdefault("revision",0)
        return result

    def execute(self, record: Mapping[str, Any], request_key: str | None=None) -> dict[str, Any]:
        normalized=self.validate_input(record)
        normalized=self.architecture_preflight(normalized)
        key=request_key or fingerprint(normalized)
        recalled=self.ledger.recalled(key)
        if recalled is not None: return recalled | {"replayed":True}
        ctx=ProcessingContext(uuid.uuid4().hex,normalized["case_id"],normalized)
        ctx.emit("received","run_started",project='screening_evidence_crawler')
        try:
            for processor in self.architecture_plan(ctx):
                processor.process(ctx)
                self.ledger.save(ctx)
        except Exception as exc:
            self.ledger.dead_letter(ctx,ctx.state,str(exc),normalized)
            raise
        self.architecture_finalize(ctx)
        ctx.advance("completed")
        self.ledger.save(ctx)
        response={"project":'screening_evidence_crawler',"run_id":ctx.run_id,"case_id":ctx.case_id,"state":ctx.state,
                  "revision":ctx.revision,"flags":ctx.flags,"metrics":ctx.metrics,"evidence_count":len(ctx.evidence),
                  "event_count":len(ctx.events),"payload":ctx.payload,"replayed":False}
        self.ledger.remember(key,ctx.run_id,response)
        return response

    def architecture_preflight(self, record):
        record=dict(record)
        record["crawl_request"]={"url":record.get("attributes",{}).get("url","fixture://screening-policy"),"budget":3,"redirects":[],"robots_decision":"offline-replay"}
        record["content_identity"]="pending"
        return record

    def architecture_plan(self, ctx):
        frontier=[(0,p) for p in self.processors]
        plan=[]
        while frontier:
            priority,processor=frontier.pop(0)
            plan.append(processor)
            ctx.emit("crawl_frontier","dequeued",priority=priority,processor=processor.stage)
        return plan

    def architecture_finalize(self, ctx):
        claims=[e for e in ctx.evidence if "claim" in e["rule"] or "policy" in e["rule"]]
        ctx.payload["crawl_result"]={"snapshot_digest":fingerprint(ctx.payload),"claim_count":len(claims),"review_required":bool(ctx.flags),"transport":"offline-replay"}
        ctx.emit("snapshot_store","snapshot_committed",digest=ctx.payload["crawl_result"]["snapshot_digest"])

    def execute_many(self, records: list[Mapping[str, Any]]) -> dict[str, Any]:
        outputs=[]; failures=[]
        for index, record in enumerate(records):
            try: outputs.append(self.execute(record,request_key=f"batch:{index}:{fingerprint(record)}"))
            except Exception as exc: failures.append({"index":index,"error":str(exc)})
        return {"accepted":outputs,"failures":failures,"accepted_count":len(outputs),"failure_count":len(failures)}

def run_fixture(fixture: str | Path, database: str | Path) -> dict[str, Any]:
    data=json.loads(Path(fixture).read_text(encoding="utf-8"))
    service=ApplicationService(database)
    if isinstance(data,list): return service.execute_many(data)
    return service.execute(data)
