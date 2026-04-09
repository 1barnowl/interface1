"""
Interface Core — Test Suite (stdlib only)
Run: python3 tests/test_interface_core.py
"""
from __future__ import annotations
import asyncio, dataclasses, json, math, os, sys, time, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from interface_core.models import (
    AdapterSpec, Bridge, BridgeCandidate, BridgeState, ContractSnapshot,
    DataContract, Event, EventType, FieldMapping, FieldSchema,
    MatchScore, Node, NodeRole, Protocol,
)
from interface_core.config import PROTOCOL_COMPAT_MATRIX, settings
from interface_core.registry import CapabilityGraph, DriftLog, NodeRegistry
from interface_core.event_bus import InProcessEventBus
from interface_core.ml.matcher import Embedder, FieldAligner, SemanticMatcher
from interface_core.adapters.base import (
    FieldMappingAdapter, GenericJSONPassthroughAdapter, MQTTToKafkaAdapter,
    TokenBucketAdapter, lookup_template, wrap_backpressure,
)
from interface_core.phases.matching import BridgeHistory, BridgeQueue, score_pair
from interface_core.phases.policy import AuditLog, PolicyEngine
from interface_core.phases.synthesis import SynthesisQueue, _synthesize_adapter, _validate_adapter
from interface_core.phases.lifecycle import BridgeRuntime, CircuitBreaker, IdempotencyGuard

LOOP = asyncio.new_event_loop()
def run(coro): return LOOP.run_until_complete(coro)

def contract(fields=None, tags=None, rate=None, conf=1.0):
    fields = fields or [("data","object")]
    return DataContract(
        schema_fields=[FieldSchema(name=n,type=t) for n,t in fields],
        semantic_tags=tags or ["generic"], emission_rate_hz=rate, confidence=conf)

def node(nid, proto=Protocol.REST, role=NodeRole.PRODUCER, fields=None, tags=None, rate=None, conf=1.0):
    return Node(id=nid, protocol=proto, role=role, endpoint=f"http://x/{nid}",
                contract=contract(fields,tags,rate,conf), embedding=[0.1]*384)

def spec(maps=None, pp=Protocol.REST, cp=Protocol.REST):
    return AdapterSpec(producer_protocol=pp, consumer_protocol=cp, field_mappings=maps or [])

# ── Models ─────────────────────────────────────────────────────────────────
class TestModels(unittest.TestCase):
    def test_fingerprint_stable(self):
        c = contract([("a","str"),("b","int")]); self.assertEqual(c.fingerprint(), c.fingerprint())
    def test_fingerprint_order_independent(self):
        c1=contract([("a","str"),("b","int")]); c2=contract([("b","int"),("a","str")])
        self.assertEqual(c1.fingerprint(), c2.fingerprint())
    def test_fingerprint_differs_on_name_change(self):
        self.assertNotEqual(contract([("lat","float")]).fingerprint(), contract([("latitude","float")]).fingerprint())
    def test_event_roundtrip(self):
        e=Event(type=EventType.NEW_NODE,node_id="n1",payload={"x":1})
        e2=Event.parse_raw(e.json())
        self.assertEqual(e2.type,EventType.NEW_NODE); self.assertEqual(e2.node_id,"n1")

# ── Registry ────────────────────────────────────────────────────────────────
class TestRegistry(unittest.TestCase):
    def test_put_get(self):
        r=NodeRegistry(); n=node("a"); r.put(n); self.assertIs(r.get("a"),n)
    def test_contains_remove(self):
        r=NodeRegistry(); r.put(node("b")); self.assertTrue(r.contains("b")); r.remove("b"); self.assertFalse(r.contains("b"))
    def test_producers_consumers(self):
        r=NodeRegistry()
        r.put(node("p",role=NodeRole.PRODUCER)); r.put(node("c",role=NodeRole.CONSUMER)); r.put(node("b",role=NodeRole.BOTH))
        self.assertIn("p",{n.id for n in r.producers()}); self.assertIn("b",{n.id for n in r.producers()})
        self.assertIn("c",{n.id for n in r.consumers()}); self.assertIn("b",{n.id for n in r.consumers()})
    def test_thread_safe(self):
        import threading; r=NodeRegistry(); errors=[]
        def w(i):
            try: [r.put(node(f"n{i}{j}")) for j in range(50)]
            except Exception as e: errors.append(e)
        ts=[threading.Thread(target=w,args=(i,)) for i in range(8)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual([],errors); self.assertEqual(400,len(r))
    def test_len(self):
        r=NodeRegistry(); [r.put(node(f"x{i}")) for i in range(7)]; self.assertEqual(7,len(r))

# ── Capability Graph ────────────────────────────────────────────────────────
class TestGraph(unittest.TestCase):
    def test_add_get_edge(self):
        g=CapabilityGraph(); g.add_edge("a","b",0.9); self.assertAlmostEqual(g.get_edge_weight("a","b"),0.9)
    def test_missing_edge_zero(self):
        self.assertEqual(CapabilityGraph().get_edge_weight("x","y"),0.0)
    def test_remove_drops_edges(self):
        g=CapabilityGraph(); g.add_edge("a","b",0.8); g.remove_node("a")
        self.assertNotIn("a",g.all_node_ids()); self.assertEqual(0.0,g.get_edge_weight("a","b"))
    def test_reweight(self):
        g=CapabilityGraph(); g.add_edge("p","c",0.9); g.reweight_edges("p",0.0)
        self.assertEqual(0.0,g.get_edge_weight("p","c"))
    def test_pairs(self):
        g=CapabilityGraph(); g.add_edge("p","c1",0.8); g.add_edge("p","c2",0.7)
        pairs=list(g.producer_consumer_pairs())
        self.assertIn(("p","c1"),pairs); self.assertIn(("p","c2"),pairs)

# ── Drift Log ────────────────────────────────────────────────────────────────
class TestDriftLog(unittest.TestCase):
    def test_record_latest(self):
        dl=DriftLog(); snap=ContractSnapshot(node_id="n",fingerprint="fp",contract=contract(),delta=0.2)
        dl.record(snap); self.assertEqual(dl.latest("n").fingerprint,"fp")
    def test_unknown_none(self): self.assertIsNone(DriftLog().latest("ghost"))
    def test_drifted_threshold(self):
        dl=DriftLog()
        dl.record(ContractSnapshot(node_id="a",fingerprint="x",contract=contract(),delta=0.3))
        dl.record(ContractSnapshot(node_id="b",fingerprint="y",contract=contract(),delta=0.05))
        self.assertIn("a",dl.drifted_nodes(0.15)); self.assertNotIn("b",dl.drifted_nodes(0.15))
    def test_cap(self):
        dl=DriftLog(); dl.MAX_PER_NODE=5
        [dl.record(ContractSnapshot(node_id="n",fingerprint=str(i),contract=contract(),delta=0.0)) for i in range(10)]
        self.assertLessEqual(len(dl.history("n")),5)

# ── Event Bus ────────────────────────────────────────────────────────────────
class TestEventBus(unittest.TestCase):
    def test_publish_receive(self):
        bus=InProcessEventBus(); q=bus.subscribe(EventType.NEW_NODE)
        bus.publish(Event(type=EventType.NEW_NODE,node_id="n1"))
        r=run(bus.next_event(q,1.0)); self.assertIsNotNone(r); self.assertEqual("n1",r.node_id)
    def test_wildcard(self):
        bus=InProcessEventBus(); q=bus.subscribe(None)
        bus.publish(Event(type=EventType.BRIDGE_LIVE)); bus.publish(Event(type=EventType.CONTRACT_DRIFT))
        self.assertIsNotNone(run(bus.next_event(q,0.5))); self.assertIsNotNone(run(bus.next_event(q,0.5)))
    def test_timeout_none(self):
        bus=InProcessEventBus(); q=bus.subscribe(EventType.NODE_GONE)
        self.assertIsNone(run(bus.next_event(q,0.05)))
    def test_type_filter(self):
        bus=InProcessEventBus(); q=bus.subscribe(EventType.BRIDGE_LIVE)
        bus.publish(Event(type=EventType.NODE_GONE))
        self.assertIsNone(run(bus.next_event(q,0.05)))

# ── ML ───────────────────────────────────────────────────────────────────────
class TestEmbedder(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.e=Embedder(); cls.e.wait_ready(30)
    def test_dim(self): self.assertEqual(len(self.e.embed("hello world")),self.e.dim)
    def test_nonzero(self): self.assertGreater(math.sqrt(sum(x*x for x in self.e.embed("price volume"))),0)
    def test_batch_matches_single(self):
        texts=["a","b","c"]; batch=self.e.embed_batch(texts)
        [self.assertEqual(len(self.e.embed(t)),len(batch[i])) for i,t in enumerate(texts)]
    def test_contract_embed(self):
        c=contract([("x","float"),("y","float")],["geo"]); self.assertEqual(len(self.e.embed_contract(c)),self.e.dim)

class TestFieldAligner(unittest.TestCase):
    @classmethod
    def setUpClass(cls): e=Embedder(); e.wait_ready(30); cls.a=FieldAligner(e)
    def test_exact_name(self):
        maps=self.a.align(contract([("price","float")]),contract([("price","float")]))
        self.assertTrue(any(m.src_field=="price" and m.dst_field=="price" for m in maps))
    def test_cast_on_type_mismatch(self):
        maps=self.a.align(contract([("n","str")]),contract([("n","int")]))
        self.assertTrue(any("cast:int" in m.transform for m in maps))
    def test_empty_returns_empty(self):
        self.assertEqual([],self.a.align(contract([]),contract([("x","float")])))

# ── Adapters ─────────────────────────────────────────────────────────────────
class TestFieldMapper(unittest.TestCase):
    def test_direct(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="a",dst_field="b")]))
        self.assertEqual(a.transform({"a":42})["b"],42)
    def test_passthrough_unmapped(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="a",dst_field="b")]))
        self.assertEqual(a.transform({"a":1,"z":"x"})["z"],"x")
    def test_cast_int(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="v",dst_field="v",transform="cast:int")]))
        self.assertEqual(a.transform({"v":"99"})["v"],99)
    def test_cast_float(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="v",dst_field="v",transform="cast:float")]))
        self.assertAlmostEqual(a.transform({"v":"3.14"})["v"],3.14,places=2)
    def test_cast_str(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="n",dst_field="n",transform="cast:str")]))
        self.assertEqual(a.transform({"n":7})["n"],"7")
    def test_extract(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="p",dst_field="lat",transform="extract:coords.lat")]))
        self.assertAlmostEqual(a.transform({"p":{"coords":{"lat":51.5}}})["lat"],51.5)
    def test_flatten(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="m",dst_field="m",transform="flatten")]))
        out=a.transform({"m":{"x":1,"y":2}}); self.assertEqual(out["m_x"],1); self.assertEqual(out["m_y"],2)
    def test_enrich(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="k",dst_field="src",transform="enrich:ic")]))
        self.assertEqual(a.transform({"k":"v"})["src"],"ic")
    def test_json_string(self):
        a=FieldMappingAdapter(spec([FieldMapping(src_field="x",dst_field="x")]))
        self.assertEqual(a.transform(json.dumps({"x":55}))["x"],55)

class TestMQTTKafka(unittest.TestCase):
    def setUp(self): self.a=MQTTToKafkaAdapter(spec(pp=Protocol.MQTT,cp=Protocol.KAFKA))
    def test_has_key_value(self):
        out=self.a.transform({"topic":"t","value":1}); self.assertIn("key",out); self.assertIn("value",out)
    def test_value_bytes(self): self.assertIsInstance(self.a.transform({"topic":"t"})["value"],bytes)
    def test_mqtt_source_header(self): self.assertEqual(self.a.transform({"topic":"t"})["headers"]["source"],"mqtt")

class TestLookup(unittest.TestCase):
    def test_mqtt_kafka(self): self.assertIs(lookup_template(Protocol.MQTT,Protocol.KAFKA),MQTTToKafkaAdapter)
    def test_unknown_none(self): self.assertIsNone(lookup_template(Protocol.MAVLINK,Protocol.DATABASE))
    def test_rest_rest(self): self.assertIsNotNone(lookup_template(Protocol.REST,Protocol.REST))

class TestBackpressure(unittest.TestCase):
    def test_drops_when_exceeded(self):
        inner=GenericJSONPassthroughAdapter(spec()); tb=TokenBucketAdapter(inner,rate_hz=2.0)
        passed=[r for r in [tb.transform({"i":i}) for i in range(10)] if r is not None]
        self.assertLessEqual(len(passed),4)
    def test_no_wrap_matched(self):
        inner=GenericJSONPassthroughAdapter(spec()); self.assertIs(wrap_backpressure(inner,10,10),inner)
    def test_wraps_overproducing(self): self.assertIsInstance(wrap_backpressure(GenericJSONPassthroughAdapter(spec()),1000,5),TokenBucketAdapter)

# ── Matching ─────────────────────────────────────────────────────────────────
class TestBridgeHistory(unittest.TestCase):
    def test_neutral_initial(self): self.assertAlmostEqual(BridgeHistory().success_rate("rest","kafka"),0.5)
    def test_rate_calc(self):
        h=BridgeHistory(); h.record("r","k",True); h.record("r","k",True); h.record("r","k",False)
        self.assertAlmostEqual(h.success_rate("r","k"),2/3,places=2)
    def test_all_success(self):
        h=BridgeHistory(); [h.record("a","b",True) for _ in range(5)]
        self.assertAlmostEqual(h.success_rate("a","b"),1.0)
    def test_all_fail(self):
        h=BridgeHistory(); [h.record("a","b",False) for _ in range(5)]
        self.assertAlmostEqual(h.success_rate("a","b"),0.0)

class TestBridgeQueue(unittest.TestCase):
    def _c(self,s,cid="c"): return BridgeCandidate(producer_id="p",consumer_id=cid,score=MatchScore(composite=s))
    def test_max_heap(self):
        q=BridgeQueue()
        [run(q.push(self._c(s,f"c{s}"))) for s in [0.75,0.95,0.80]]
        self.assertAlmostEqual(run(q.pop()).score.composite,0.95)
    def test_empty_none(self): self.assertIsNone(run(BridgeQueue().pop()))
    def test_len(self):
        q=BridgeQueue(); [run(q.push(self._c(s,f"c{i}"))) for i,s in enumerate([.7,.8,.9])]
        self.assertEqual(3,len(q))
    def test_repush(self):
        q=BridgeQueue(); c=self._c(0.9); run(q.push(c)); run(q.pop()); self.assertEqual(0,len(q)); run(q.repush(c)); self.assertEqual(1,len(q))

class TestScorePair(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.m=SemanticMatcher(); cls.m.wait_ready(30); cls.h=BridgeHistory()
    class _P:
        def quick_risk(self,p,c): return 0.1
    _W={"semantic_similarity":0.4,"protocol_compat":0.2,"latency_score":0.15,"risk_score":0.15,"history_score":0.1}
    def test_identical_scores_well(self):
        p=node("p",fields=[("price","float"),("sym","str")],tags=["market"],rate=1.0)
        c=node("c",role=NodeRole.CONSUMER,fields=[("price","float"),("sym","str")],tags=["market"])
        p.embedding=self.m.embed(p.contract); c.embedding=self.m.embed(c.contract)
        s=score_pair(p,c,self.m,self.h,self._P(),self._W); self.assertGreater(s.composite,0.3)
    def test_low_conf_penalises(self):
        p=node("p2",conf=0.1); c=node("c2",role=NodeRole.CONSUMER)
        p.embedding=[0.1]*384; c.embedding=[0.1]*384
        s=score_pair(p,c,self.m,self.h,self._P(),self._W); self.assertLess(s.composite,0.5)
    def test_scores_in_range(self):
        p=node("pa"); c=node("ca",role=NodeRole.CONSUMER)
        p.embedding=self.m.embed(p.contract); c.embedding=self.m.embed(c.contract)
        s=score_pair(p,c,self.m,self.h,self._P(),self._W)
        for a in ("semantic_similarity","protocol_compat","latency_score","risk_score","history_score","composite"):
            v=getattr(s,a); self.assertGreaterEqual(v,0.0); self.assertLessEqual(v,1.0)

# ── Policy ────────────────────────────────────────────────────────────────────
class TestPolicy(unittest.TestCase):
    def _e(self,ab=None):
        r=NodeRegistry(); e=PolicyEngine(r,ab or {}); return e,r
    def _c(self,p="p",c="c"): return BridgeCandidate(producer_id=p,consumer_id=c,score=MatchScore(composite=0.9))
    def test_ghost_rejected(self):
        e,_=self._e(); self.assertEqual(run(e.evaluate(self._c("g1","g2"))).verdict.value,"reject")
    def test_circular_rejected(self):
        e,r=self._e({("c","p"):("b","a")}); r.put(node("p")); r.put(node("c",role=NodeRole.CONSUMER))
        self.assertEqual(run(e.evaluate(self._c())).verdict.value,"reject")
    def test_normal_approved(self):
        e,r=self._e(); r.put(node("p")); r.put(node("c",role=NodeRole.CONSUMER))
        self.assertEqual(run(e.evaluate(self._c())).verdict.value,"approve")
    def test_blacklist(self):
        e,r=self._e(); r.put(node("evil")); r.put(node("c2",role=NodeRole.CONSUMER))
        orig=settings.blacklisted_nodes; settings.blacklisted_nodes=["evil"]
        res=run(e.evaluate(self._c("evil","c2"))); settings.blacklisted_nodes=orig
        self.assertEqual(res.verdict.value,"reject")
    def test_low_confidence_rejected(self):
        e,r=self._e(); r.put(node("lc",conf=0.1)); r.put(node("c3",role=NodeRole.CONSUMER))
        self.assertEqual(run(e.evaluate(self._c("lc","c3"))).verdict.value,"reject")

class TestAuditLog(unittest.TestCase):
    def _c(self): return BridgeCandidate(producer_id="p",consumer_id="c",score=MatchScore(composite=0.8))
    def test_write_retrieve(self):
        log=AuditLog(); log.write("APPROVE",self._c(),"ok")
        self.assertEqual(log.recent(10)[0]["verdict"],"APPROVE")
    def test_limit(self):
        log=AuditLog(); [log.write("APPROVE",self._c(),f"r{i}") for i in range(20)]
        self.assertEqual(len(log.recent(5)),5)

# ── Synthesis ─────────────────────────────────────────────────────────────────
class TestSynthesisQueue(unittest.TestCase):
    def _c(self): return BridgeCandidate(producer_id="p",consumer_id="c",score=MatchScore(composite=0.8))
    def test_push_pop(self): q=SynthesisQueue(); run(q.push(self._c())); self.assertEqual(run(q.pop(0.5)).producer_id,"p")
    def test_empty_none(self): self.assertIsNone(run(SynthesisQueue().pop(0.05)))
    def test_fifo(self):
        q=SynthesisQueue()
        [run(q.push(BridgeCandidate(producer_id="p",consumer_id=c,score=MatchScore(composite=0.8)))) for c in ["c1","c2","c3"]]
        self.assertEqual([run(q.pop(0.2)).consumer_id for _ in range(3)],["c1","c2","c3"])

class TestSynthAdapter(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.m=SemanticMatcher(); cls.m.wait_ready(30)
    def _pair(self,pf,cf,pp=Protocol.REST,cp=Protocol.REST):
        p=node("p",pp,NodeRole.PRODUCER,pf); c=node("c",cp,NodeRole.CONSUMER,cf)
        p.embedding=self.m.embed(p.contract); c.embedding=self.m.embed(c.contract); return p,c
    def test_not_none(self):
        p,c=self._pair([("price","float"),("sym","str")],[("price","float"),("symbol","str")])
        self.assertIsNotNone(_synthesize_adapter(p,c,self.m))
    def test_validates(self):
        p,c=self._pair([("x","float")],[("x","float")]); a=_synthesize_adapter(p,c,self.m)
        self.assertTrue(_validate_adapter(a,p,c))
    def test_transform_dict(self):
        p,c=self._pair([("a","str"),("b","int")],[("a","str"),("b","int")]); a=_synthesize_adapter(p,c,self.m)
        self.assertIsInstance(a.transform({"a":"hi","b":1}),dict)
    def test_mqtt_kafka(self):
        p,c=self._pair([("topic","str"),("value","float")],[("key","str"),("value","bytes")],Protocol.MQTT,Protocol.KAFKA)
        self.assertIsNotNone(_synthesize_adapter(p,c,self.m))

# ── Circuit Breaker ───────────────────────────────────────────────────────────
class TestCB(unittest.TestCase):
    def _cb(self,t=0.4,w=60,h=999): return CircuitBreaker(threshold=t,window_s=w,halfopen_s=h)
    def test_starts_closed(self): self.assertFalse(self._cb().is_open())
    def test_trips(self):
        cb=self._cb(); [cb.record(True) for _ in range(3)]; [cb.record(False) for _ in range(5)]
        self.assertTrue(cb.is_open())
    def test_stays_closed_low_err(self):
        cb=self._cb(); [cb.record(True) for _ in range(9)]; cb.record(False)
        self.assertFalse(cb.is_open())
    def test_halfopen(self):
        cb=self._cb(h=0.0); [cb.record(False) for _ in range(10)]; time.sleep(0.01)
        self.assertEqual(cb.state,CircuitBreaker.HALF_OPEN)
    def test_recovers(self):
        cb=self._cb(h=0.0); [cb.record(False) for _ in range(10)]; time.sleep(0.01)
        cb.record(True); self.assertEqual(cb.state,CircuitBreaker.CLOSED)
    def test_retrips_on_failed_probe(self):
        # Use a small but nonzero halfopen delay so re-trip stays OPEN long enough to assert
        cb=self._cb(h=0.05); [cb.record(False) for _ in range(10)]; time.sleep(0.06)
        # Now in HALF_OPEN; a failed probe should re-trip to OPEN
        self.assertEqual(cb.state, CircuitBreaker.HALF_OPEN)
        cb.record(False)
        self.assertEqual(cb._state, CircuitBreaker.OPEN)

# ── Idempotency Guard ─────────────────────────────────────────────────────────
class TestIdem(unittest.TestCase):
    def test_first_not_dup(self): self.assertFalse(IdempotencyGuard(10).is_duplicate("x"))
    def test_second_dup(self): g=IdempotencyGuard(10); g.is_duplicate("x"); self.assertTrue(g.is_duplicate("x"))
    def test_different_not_dup(self):
        g=IdempotencyGuard(10); self.assertFalse(g.is_duplicate("a")); self.assertFalse(g.is_duplicate("b"))
    def test_evicts_after_ttl(self):
        g=IdempotencyGuard(ttl_s=0); g.is_duplicate("z"); time.sleep(0.01); self.assertFalse(g.is_duplicate("z"))

# ── Bridge Runtime ────────────────────────────────────────────────────────────
class TestRuntime(unittest.TestCase):
    def _rt(self):
        b=Bridge(id="b",producer_id="p",consumer_id="c",
                 adapter=AdapterSpec(producer_protocol=Protocol.REST,consumer_protocol=Protocol.REST))
        a=GenericJSONPassthroughAdapter(AdapterSpec(producer_protocol=Protocol.REST,consumer_protocol=Protocol.REST))
        return BridgeRuntime(b,a)
    def test_process_ok(self): rt=self._rt(); self.assertEqual(rt.process({"x":1},"m1"),{"x":1})
    def test_zero_error_rate(self): rt=self._rt(); [rt.process({"x":1}) for _ in range(5)]; self.assertAlmostEqual(rt.error_rate,0.0)
    def test_idem_drop(self): rt=self._rt(); rt.process({"x":1},"dup"); self.assertIsNone(rt.process({"x":2},"dup"))
    def test_circuit_buffers(self):
        rt=self._rt(); [rt.circuit.record(False) for _ in range(10)]; self.assertTrue(rt.circuit.is_open())
        rt.process({"x":99}); self.assertEqual(len(rt.drain_buffer()),1)
    def test_drain_clears(self): rt=self._rt(); [rt.circuit.record(False) for _ in range(10)]; rt.process({"x":1}); rt.drain_buffer(); self.assertEqual(rt.drain_buffer(),[])
    def test_latency(self): rt=self._rt(); [rt.record_latency(float(ms)) for ms in range(1,101)]; self.assertGreaterEqual(rt.latency_p99_ms,99.0)

# ── Compat Matrix ─────────────────────────────────────────────────────────────
class TestMatrix(unittest.TestCase):
    def test_self_one(self):
        for p in ["rest","kafka","mqtt","grpc","zeromq","mavlink"]:
            self.assertAlmostEqual(PROTOCOL_COMPAT_MATRIX[p][p],1.0)
    def test_mqtt_kafka_high(self): self.assertGreaterEqual(PROTOCOL_COMPAT_MATRIX["mqtt"]["kafka"],0.7)
    def test_mavlink_rest_low(self): self.assertLessEqual(PROTOCOL_COMPAT_MATRIX["mavlink"]["rest"],0.4)
    def test_all_positive(self):
        for row in PROTOCOL_COMPAT_MATRIX.values():
            for v in row.values(): self.assertGreater(v,0.0)

# ── Integration ───────────────────────────────────────────────────────────────
class TestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        cls.core=InterfaceCore(); cls.core.ml_matcher.wait_ready(30)

    def test_register_nodes(self):
        p=self.core.register_node("rest","http://src/p",{"price":"float","sym":"str"},["market"],"producer",5.0)
        c=self.core.register_node("rest","http://dst/c",{"price":"float","sym":"str"},["market"],"consumer")
        self.assertIsNotNone(self.core.registry.get(p)); self.assertIsNotNone(self.core.registry.get(c))

    def test_status_keys(self):
        s=self.core.status()
        for k in ("nodes","active_bridges","bridge_queue","telemetry"): self.assertIn(k,s)

    def test_matching_no_exception(self):
        self.core.register_node("rest","http://src/t",{"temp":"float"},["telemetry"],"producer",1.0)
        self.core.register_node("rest","http://dst/t",{"temp":"float"},["telemetry"],"consumer")
        run(self.core.matching._match_all())  # must not raise

    def test_adapter_transforms_payload(self):
        p_id=self.core.register_node("rest","http://src/geo",{"lat":"float","lon":"float"},["geo"],"producer")
        c_id=self.core.register_node("rest","http://dst/geo",{"lat":"float","lon":"float"},["geo"],"consumer")
        p=self.core.registry.get(p_id); c=self.core.registry.get(c_id)
        p.embedding=self.core.ml_matcher.embed(p.contract); c.embedding=self.core.ml_matcher.embed(c.contract)
        a=_synthesize_adapter(p,c,self.core.ml_matcher)
        self.assertIsNotNone(a)
        out=a.transform({"lat":51.5,"lon":-0.1})
        self.assertIsInstance(out,dict); self.assertGreater(len(out),0)

    def test_full_pipeline_smoke(self):
        p_id=self.core.register_node("rest","http://src/full",{"val":"float"},["generic"],"producer",1.0)
        c_id=self.core.register_node("rest","http://dst/full",{"val":"float"},["generic"],"consumer")
        run(self.core.matching._match_all())
        processed=0
        while not self.core.bridge_queue.empty() and processed<10:
            cand=run(self.core.bridge_queue.pop())
            if cand is None: break
            result=run(self.core.policy_engine.evaluate(cand))
            if result.verdict.value=="approve": run(self.core.synthesis_queue.push(cand))
            processed+=1
        for _ in range(5):
            cand=run(self.core.synthesis_queue.pop(0.1))
            if cand is None: break
            run(self.core.synthesis._synthesize(cand))
        self.assertGreaterEqual(self.core.status()["nodes"],2)

if __name__=="__main__":
    unittest.main(verbosity=2)
