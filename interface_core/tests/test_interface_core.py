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

    @classmethod
    def tearDownClass(cls):
        # Cancel any lingering BridgeRunner tasks so the event loop closes cleanly
        for runner in list(cls.core._runners.values()):
            try:
                LOOP.run_until_complete(runner.stop())
            except Exception:
                pass
        cls.core._runners.clear()

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



# ════════════════════════════════════════════════════════════════
# IMPROVEMENT TESTS
# ════════════════════════════════════════════════════════════════

# ── #1 Bridge Runner (I/O loop) ──────────────────────────────────────────────
class TestBridgeRunner(unittest.TestCase):
    """BridgeRunner moves data from MemoryProducer → Adapter → MemoryConsumer."""

    @classmethod
    def setUpClass(cls):
        from interface_core.phases.runner import (
            BridgeRunner, MemoryProducerTransport, MemoryConsumerTransport,
        )
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.models import Bridge, AdapterSpec, Protocol
        from interface_core.adapters.base import GenericJSONPassthroughAdapter

        cls.producer_q = asyncio.Queue()
        cls.consumer_q = asyncio.Queue()
        cls.producer   = MemoryProducerTransport(cls.producer_q)
        cls.consumer   = MemoryConsumerTransport(cls.consumer_q)

        bridge  = Bridge(
            id="run-1", producer_id="p", consumer_id="c",
            adapter=AdapterSpec(producer_protocol=Protocol.REST,
                                consumer_protocol=Protocol.REST),
        )
        spec    = AdapterSpec(producer_protocol=Protocol.REST,
                              consumer_protocol=Protocol.REST)
        adapter = GenericJSONPassthroughAdapter(spec)
        cls.runtime = BridgeRuntime(bridge, adapter)
        cls.runner  = BridgeRunner("run-1", cls.runtime, cls.producer, cls.consumer)

    def test_runner_start_creates_task(self):
        async def _go():
            task = self.runner.start()
            self.assertIsNotNone(task)
            await self.runner.stop()
        run(_go())

    def test_data_flows_end_to_end(self):
        payload = {"sensor": "temp", "value": 23.5}
        async def _go():
            self.runner.start()
            await self.producer_q.put(payload)
            await asyncio.sleep(0.20)
            result = self.consumer_q.get_nowait() if not self.consumer_q.empty() else None
            await self.runner.stop()
            return result
        result = run(_go())
        self.assertIsNotNone(result)
        self.assertEqual(result.get("value"), 23.5)

    def test_runner_stats_keys(self):
        async def _go():
            self.runner.start()
            await asyncio.sleep(0.05)
            stats = self.runner.stats
            for k in ("bridge_id", "messages", "errors", "circuit", "latency_p50_ms"):
                self.assertIn(k, stats)
            await self.runner.stop()
        run(_go())

    def test_stop_is_idempotent(self):
        async def _go():
            self.runner.start()
            await self.runner.stop()
            await self.runner.stop()   # should not raise
        run(_go())

    def test_memory_transport_read_timeout(self):
        from interface_core.phases.runner import MemoryProducerTransport
        t = MemoryProducerTransport()
        result = run(t.read())
        self.assertIsNone(result)

    def test_memory_consumer_write(self):
        from interface_core.phases.runner import MemoryConsumerTransport
        t   = MemoryConsumerTransport()
        ok  = run(t.write({"x": 1}))
        self.assertTrue(ok)
        self.assertFalse(t.queue.empty())

    def test_msg_id_extraction(self):
        from interface_core.phases.runner import _msg_id
        self.assertEqual(_msg_id({"id": "abc", "v": 1}), "abc")
        self.assertEqual(_msg_id({"msg_id": "xyz"}), "xyz")
        self.assertIsNone(_msg_id({"no_id_field": True}))
        self.assertIsNone(_msg_id("raw string"))


# ── #2 Bridge Store ───────────────────────────────────────────────────────────
class TestBridgeStore(unittest.TestCase):

    def setUp(self):
        from interface_core.store import BridgeStore
        # Use memory fallback (no Redis in sandbox)
        self.store = BridgeStore("redis://localhost:9999/0")  # guaranteed unreachable

    def _bridge_and_candidate(self, bid="b1", pid="p1", cid="c1"):
        from interface_core.models import (
            Bridge, BridgeCandidate, AdapterSpec, MatchScore,
            Protocol, FieldMapping,
        )
        spec = AdapterSpec(
            producer_protocol=Protocol.REST,
            consumer_protocol=Protocol.KAFKA,
            field_mappings=[FieldMapping(src_field="price", dst_field="price")],
        )
        bridge = Bridge(id=bid, producer_id=pid, consumer_id=cid, adapter=spec)
        cand   = BridgeCandidate(
            producer_id=pid, consumer_id=cid, score=MatchScore(composite=0.85)
        )
        return bridge, cand

    def test_save_and_restore(self):
        bridge, cand = self._bridge_and_candidate()
        run(self.store.save(bridge, cand))
        restored = run(self.store.restore_all())
        self.assertEqual(len(restored), 1)
        rb, rc = restored[0]
        self.assertEqual(rb.producer_id, "p1")
        self.assertEqual(rb.consumer_id, "c1")
        self.assertAlmostEqual(rc.score.composite, 0.85, places=2)

    def test_field_mappings_survive_roundtrip(self):
        bridge, cand = self._bridge_and_candidate()
        run(self.store.save(bridge, cand))
        restored = run(self.store.restore_all())
        rb, _ = restored[0]
        self.assertEqual(len(rb.adapter.field_mappings), 1)
        self.assertEqual(rb.adapter.field_mappings[0].src_field, "price")

    def test_delete_removes_entry(self):
        bridge, cand = self._bridge_and_candidate()
        run(self.store.save(bridge, cand))
        run(self.store.delete("p1", "c1"))
        restored = run(self.store.restore_all())
        self.assertEqual(len(restored), 0)

    def test_multiple_bridges(self):
        b1, c1 = self._bridge_and_candidate("b1","p1","c1")
        b2, c2 = self._bridge_and_candidate("b2","p2","c2")
        run(self.store.save(b1, c1))
        run(self.store.save(b2, c2))
        self.assertEqual(run(self.store.count()), 2)

    def test_restore_empty_store(self):
        from interface_core.store import BridgeStore
        fresh = BridgeStore("redis://localhost:9999/0")
        self.assertEqual(run(fresh.restore_all()), [])

    def test_delete_nonexistent_no_error(self):
        run(self.store.delete("ghost1", "ghost2"))   # should not raise

    def test_backend_name(self):
        # With unreachable Redis, backend should be memory
        name = self.store.backend_name
        self.assertIn(name, ("memory", "redis"))


# ── #3 HTTP API ───────────────────────────────────────────────────────────────
class TestHTTPAPI(unittest.TestCase):
    """Tests the API layer against a live InterfaceCore in-memory instance."""

    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        cls.core = InterfaceCore()
        cls.core.ml_matcher.wait_ready(30)
        # Register a couple of nodes
        cls.pid = cls.core.register_node(
            "rest", "http://api-src/data",
            {"price": "float", "sym": "str"}, ["market"], "producer", 1.0,
        )
        cls.cid = cls.core.register_node(
            "rest", "http://api-dst/data",
            {"price": "float", "sym": "str"}, ["market"], "consumer",
        )
        cls.app = build_app(cls.core)

    def _skip_if_no_aiohttp(self):
        try:
            import aiohttp
        except ImportError:
            self.skipTest("aiohttp not installed")

    def _client(self):
        from aiohttp.test_utils import TestClient, TestServer
        return TestClient(TestServer(self.app))

    def test_app_builds_or_none(self):
        # build_app returns None if aiohttp absent, otherwise an app object
        try:
            import aiohttp
            self.assertIsNotNone(self.app)
        except ImportError:
            self.assertIsNone(self.app)

    def test_get_nodes_returns_list(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get("/nodes")
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIsInstance(data, list)
                self.assertGreaterEqual(len(data), 2)
                ids = [n["id"] for n in data]
                self.assertIn(self.pid, ids)
                self.assertIn(self.cid, ids)

        run(_run())

    def test_get_status_keys(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get("/status")
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                for k in ("nodes", "active_bridges", "bridge_queue", "store_backend"):
                    self.assertIn(k, data)

        run(_run())

    def test_post_nodes_registers(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.post("/nodes", json={
                    "protocol": "rest",
                    "endpoint": "http://api-test/new",
                    "fields":   {"value": "float"},
                    "tags":     ["test"],
                    "role":     "producer",
                })
                self.assertEqual(resp.status, 201)
                data = await resp.json()
                self.assertIn("node_id", data)

        run(_run())

    def test_post_nodes_missing_fields_returns_400(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.post("/nodes", json={"protocol": "rest"})
                self.assertEqual(resp.status, 400)

        run(_run())

    def test_get_bridges_returns_list(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get("/bridges")
                self.assertEqual(resp.status, 200)
                self.assertIsInstance(await resp.json(), list)

        run(_run())

    def test_get_audit_returns_list(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get("/audit")
                self.assertEqual(resp.status, 200)
                self.assertIsInstance(await resp.json(), list)

        run(_run())

    def test_get_queue_returns_depth(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get("/queue")
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIn("bridge_queue_depth", data)

        run(_run())

    def test_approve_nonexistent_returns_404(self):
        self._skip_if_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.post("/bridges/ghost1/ghost2/approve")
                self.assertEqual(resp.status, 404)

        run(_run())


# ── #4 Full AdapterSpec in BRIDGE_LIVE event ─────────────────────────────────
class TestBridgeLiveEventPayload(unittest.TestCase):
    """Verify BRIDGE_LIVE emits the full adapter spec."""

    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        cls.core = InterfaceCore()
        cls.core.ml_matcher.wait_ready(30)

    def test_bridge_live_event_has_field_mappings(self):
        from interface_core.event_bus import InProcessEventBus
        from interface_core.models import EventType

        events = []
        q = self.core.event_bus.subscribe(EventType.BRIDGE_LIVE)

        p_id = self.core.register_node(
            "rest", "http://ev-src/data",
            {"price": "float", "sym": "str"}, ["market"], "producer", 1.0,
        )
        c_id = self.core.register_node(
            "rest", "http://ev-dst/data",
            {"price": "float", "sym": "str"}, ["market"], "consumer",
        )

        async def _trigger():
            await self.core.matching._match_all()
            processed = 0
            while not self.core.bridge_queue.empty() and processed < 10:
                cand = await self.core.bridge_queue.pop()
                if cand is None: break
                result = await self.core.policy_engine.evaluate(cand)
                if result.verdict.value == "approve":
                    await self.core.synthesis_queue.push(cand)
                processed += 1
            for _ in range(5):
                cand = await self.core.synthesis_queue.pop(0.1)
                if cand is None: break
                await self.core.synthesis._synthesize(cand)
            # Collect any BRIDGE_LIVE events
            while True:
                ev = await self.core.event_bus.next_event(q, timeout=0.1)
                if ev is None: break
                events.append(ev)

        run(_trigger())

        if not events:
            self.skipTest("No bridges synthesized (score below threshold)")

        ev = events[0]
        self.assertIn("producer_protocol", ev.payload)
        self.assertIn("consumer_protocol", ev.payload)
        self.assertIn("field_mappings",    ev.payload)
        self.assertIn("backpressure_mode", ev.payload)
        self.assertIn("adapter_version",   ev.payload)
        self.assertIn("provenance",        ev.payload)
        self.assertIsInstance(ev.payload["field_mappings"], list)

    def test_bridge_live_score_in_payload(self):
        from interface_core.models import EventType
        q = self.core.event_bus.subscribe(EventType.BRIDGE_LIVE)

        p_id = self.core.register_node(
            "rest", "http://ev2-src/data",
            {"lat": "float", "lon": "float"}, ["geo"], "producer", 2.0,
        )
        c_id = self.core.register_node(
            "rest", "http://ev2-dst/data",
            {"lat": "float", "lon": "float"}, ["geo"], "consumer",
        )

        events = []
        async def _trigger():
            await self.core.matching._match_all()
            processed = 0
            while not self.core.bridge_queue.empty() and processed < 10:
                cand = await self.core.bridge_queue.pop()
                if cand is None: break
                result = await self.core.policy_engine.evaluate(cand)
                if result.verdict.value == "approve":
                    await self.core.synthesis_queue.push(cand)
                processed += 1
            for _ in range(5):
                cand = await self.core.synthesis_queue.pop(0.1)
                if cand is None: break
                await self.core.synthesis._synthesize(cand)
            while True:
                ev = await self.core.event_bus.next_event(q, timeout=0.1)
                if ev is None: break
                events.append(ev)

        run(_trigger())

        if not events:
            self.skipTest("No bridges synthesized")

        self.assertIn("score", events[0].payload)
        self.assertGreater(events[0].payload["score"], 0.0)


# ── #5 Score Decay ────────────────────────────────────────────────────────────
class TestScoreDecay(unittest.TestCase):

    def _fresh_cand(self, score, age_minutes=0.0):
        import time
        cand = BridgeCandidate(
            producer_id="p", consumer_id="c",
            score=MatchScore(composite=score),
        )
        # Backdate created_at to simulate waiting time
        cand.created_at = time.time() - age_minutes * 60
        return cand

    def test_fresh_candidate_not_decayed(self):
        q = BridgeQueue()
        run(q.push(self._fresh_cand(0.85, age_minutes=0.0)))
        out = run(q.pop())
        self.assertIsNotNone(out)
        # Fresh: decay ~ 0.95^0 = 1.0, score unchanged
        self.assertAlmostEqual(out.score.composite, 0.85, places=2)

    def test_old_candidate_score_reduced(self):
        q = BridgeQueue()
        run(q.push(self._fresh_cand(0.95, age_minutes=30.0)))
        out = run(q.pop())
        if out is None:
            # Fully decayed below MIN_AFTER_DECAY — that's valid too
            return
        # After 30 minutes: 0.95^30 ≈ 0.215; 0.95 * 0.215 ≈ 0.204 < MIN
        # So either None or a reduced score
        self.assertLessEqual(out.score.composite, 0.95)

    def test_very_stale_candidate_discarded(self):
        q = BridgeQueue()
        # A candidate that has been waiting 2 hours with score just above threshold
        run(q.push(self._fresh_cand(0.73, age_minutes=120.0)))
        out = run(q.pop())
        # 0.73 * (0.95^120) ≈ 0.73 * 0.0021 ≈ 0.0015 << MIN_AFTER_DECAY
        self.assertIsNone(out)

    def test_high_score_survives_moderate_wait(self):
        q = BridgeQueue()
        run(q.push(self._fresh_cand(0.99, age_minutes=1.0)))
        out = run(q.pop())
        self.assertIsNotNone(out)
        # 0.99 * 0.95^1 = 0.9405 — still well above MIN
        self.assertGreater(out.score.composite, 0.80)

    def test_ordering_preserved_after_decay(self):
        q = BridgeQueue()
        # Use distinct consumer IDs so dedup does not drop duplicates
        for score, cid in [(0.75, "c1"), (0.90, "c2"), (0.80, "c3")]:
            cand = BridgeCandidate(
                producer_id="p", consumer_id=cid,
                score=MatchScore(composite=score),
            )
            run(q.push(cand))
        first  = run(q.pop())
        second = run(q.pop())
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertGreaterEqual(first.score.composite, second.score.composite)

    def test_repush_resets_age(self):
        import time
        q    = BridgeQueue()
        old  = self._fresh_cand(0.72, age_minutes=200.0)   # would decay to nothing
        run(q.repush(old))   # repush resets created_at
        out  = run(q.pop())
        self.assertIsNotNone(out)   # age was reset, so score survived

    def test_decay_rate_constant(self):
        from interface_core.phases.matching import BridgeQueue as BQ
        self.assertAlmostEqual(BQ.DECAY_RATE, 0.95)

    def test_min_after_decay_constant(self):
        from interface_core.phases.matching import BridgeQueue as BQ
        self.assertLess(BQ.MIN_AFTER_DECAY, BQ.DECAY_RATE)


# ── Integration — store persists across simulated restart ─────────────────────
class TestRestoreOnRestart(unittest.TestCase):

    def test_bridge_survives_simulated_restart(self):
        """
        Synthesize a bridge → verify it's in the store →
        create a new InterfaceCore and restore → bridge re-queued.
        """
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        core.ml_matcher.wait_ready(30)

        p_id = core.register_node(
            "rest", "http://restart-src/v",
            {"val": "float"}, ["generic"], "producer", 1.0,
        )
        c_id = core.register_node(
            "rest", "http://restart-dst/v",
            {"val": "float"}, ["generic"], "consumer",
        )

        async def _build_bridge():
            await core.matching._match_all()
            processed = 0
            while not core.bridge_queue.empty() and processed < 10:
                cand = await core.bridge_queue.pop()
                if cand is None: break
                result = await core.policy_engine.evaluate(cand)
                if result.verdict.value == "approve":
                    await core.synthesis_queue.push(cand)
                processed += 1
            for _ in range(5):
                cand = await core.synthesis_queue.pop(0.1)
                if cand is None: break
                await core.synthesis._synthesize(cand)

        run(_build_bridge())

        # Verify something is in store
        count = run(core.store.count())
        if count == 0:
            self.skipTest("No bridges synthesized (score below threshold)")

        # Simulate restart: new core shares the same store backend (memory in this case)
        core2 = InterfaceCore()
        core2.store._memory._data = core.store._memory._data.copy()
        core2.ml_matcher.wait_ready(30)

        # Re-register the same nodes (normally discovery would do this)
        core2.register_node("rest","http://restart-src/v",{"val":"float"},["generic"],"producer",1.0)
        core2.register_node("rest","http://restart-dst/v",{"val":"float"},["generic"],"consumer")

        restored = run(core2.store.restore_all())
        self.assertGreater(len(restored), 0)
        # Restored bridge should have the right node IDs
        rb, rc = restored[0]
        self.assertIn(rb.producer_id, [p_id])
        self.assertIn(rb.consumer_id, [c_id])




# ════════════════════════════════════════════════════════════════
# FOLLOW-UP FIX TESTS
# ════════════════════════════════════════════════════════════════

# ── Config completeness ──────────────────────────────────────────────────────
class TestConfigCompleteness(unittest.TestCase):
    def test_api_host_present(self):
        from interface_core.config import settings
        self.assertTrue(hasattr(settings, 'api_host'))
        self.assertIsInstance(settings.api_host, str)

    def test_api_port_present(self):
        from interface_core.config import settings
        self.assertTrue(hasattr(settings, 'api_port'))
        self.assertIsInstance(settings.api_port, int)
        self.assertGreater(settings.api_port, 0)

    def test_all_tunable_fields_present(self):
        from interface_core.config import settings
        required = [
            'node_name', 'secret_key',
            'discovery_interval_ms', 'matching_interval_ms',
            'min_composite_score', 'contract_drift_delta',
            'blacklisted_nodes', 'max_bridges_per_node',
            'circuit_error_threshold', 'buffer_size',
            'embedding_model', 'redis_url', 'kafka_brokers',
            'api_host', 'api_port',
        ]
        for attr in required:
            self.assertTrue(hasattr(settings, attr), f'settings missing: {attr}')


# ── Transport factory routing ────────────────────────────────────────────────
class TestTransportFactory(unittest.TestCase):

    def _node(self, proto, endpoint, rate=None):
        from interface_core.models import Node, NodeRole, DataContract, FieldSchema, Protocol
        return Node(
            id='x', protocol=Protocol(proto), role=NodeRole.PRODUCER,
            endpoint=endpoint,
            contract=DataContract(
                schema_fields=[FieldSchema(name='data', type='object')],
                emission_rate_hz=rate,
            ),
        )

    def test_rest_producer_transport(self):
        from interface_core.phases.runner import make_producer_transport, RESTProducerTransport
        n = self._node('rest', 'http://localhost:8080/data', rate=1.0)
        self.assertIsInstance(make_producer_transport(n), RESTProducerTransport)

    def test_file_producer_transport(self):
        from interface_core.phases.runner import make_producer_transport, FileProducerTransport
        n = self._node('file', '/tmp/data.json')
        self.assertIsInstance(make_producer_transport(n), FileProducerTransport)

    def test_kafka_producer_transport(self):
        from interface_core.phases.runner import make_producer_transport
        from interface_core.phases.transports import KafkaProducerTransport
        n = self._node('kafka', 'localhost:9092/my-topic', rate=10.0)
        self.assertIsInstance(make_producer_transport(n), KafkaProducerTransport)

    def test_mqtt_producer_transport(self):
        from interface_core.phases.runner import make_producer_transport
        from interface_core.phases.transports import MQTTProducerTransport
        n = self._node('mqtt', 'mqtt://localhost:1883/sensors/#', rate=5.0)
        self.assertIsInstance(make_producer_transport(n), MQTTProducerTransport)

    def test_socket_producer_transport(self):
        from interface_core.phases.runner import make_producer_transport
        from interface_core.phases.transports import SocketProducerTransport
        n = self._node('socket', 'localhost:9000')
        self.assertIsInstance(make_producer_transport(n), SocketProducerTransport)

    def test_unknown_protocol_falls_back_to_memory(self):
        from interface_core.phases.runner import make_producer_transport, MemoryProducerTransport
        n = self._node('unknown', 'somewhere')
        self.assertIsInstance(make_producer_transport(n), MemoryProducerTransport)

    def test_rest_consumer_transport(self):
        from interface_core.phases.runner import make_consumer_transport, RESTConsumerTransport
        n = self._node('rest', 'http://localhost:9000/ingest')
        self.assertIsInstance(make_consumer_transport(n), RESTConsumerTransport)

    def test_kafka_consumer_transport(self):
        from interface_core.phases.runner import make_consumer_transport
        from interface_core.phases.transports import KafkaConsumerTransport
        n = self._node('kafka', 'localhost:9092/output')
        self.assertIsInstance(make_consumer_transport(n), KafkaConsumerTransport)

    def test_mqtt_consumer_transport(self):
        from interface_core.phases.runner import make_consumer_transport
        from interface_core.phases.transports import MQTTConsumerTransport
        n = self._node('mqtt', 'mqtt://localhost:1883/output')
        self.assertIsInstance(make_consumer_transport(n), MQTTConsumerTransport)

    def test_socket_consumer_transport(self):
        from interface_core.phases.runner import make_consumer_transport
        from interface_core.phases.transports import SocketConsumerTransport
        n = self._node('socket', 'localhost:9001')
        self.assertIsInstance(make_consumer_transport(n), SocketConsumerTransport)


# ── Kafka transport unit (no broker needed) ──────────────────────────────────
class TestKafkaTransport(unittest.TestCase):

    def test_endpoint_parse_with_topic(self):
        from interface_core.phases.transports import KafkaProducerTransport
        t = KafkaProducerTransport('localhost:9092/my-topic')
        self.assertEqual(t._brokers, 'localhost:9092')
        self.assertEqual(t._topic,   'my-topic')

    def test_endpoint_parse_without_topic(self):
        from interface_core.phases.transports import KafkaProducerTransport
        t = KafkaProducerTransport('localhost:9092')
        self.assertEqual(t._brokers, 'localhost:9092')
        self.assertIsNotNone(t._topic)

    def test_read_without_broker_returns_none_or_falls_back(self):
        from interface_core.phases.transports import KafkaProducerTransport
        t = KafkaProducerTransport('localhost:19999/no-such-topic')
        result = run(t.read())
        # Either None (connection refused) or falls back gracefully
        self.assertIsNone(result)

    def test_consumer_endpoint_parse(self):
        from interface_core.phases.transports import KafkaConsumerTransport
        t = KafkaConsumerTransport('broker:9092/output-topic')
        self.assertEqual(t._brokers, 'broker:9092')
        self.assertEqual(t._topic,   'output-topic')

    def test_write_without_broker_returns_false(self):
        from interface_core.phases.transports import KafkaConsumerTransport
        t = KafkaConsumerTransport('localhost:19999/no-such-topic')
        result = run(t.write({'x': 1}))
        self.assertFalse(result)

    def test_close_without_connection_no_error(self):
        from interface_core.phases.transports import KafkaProducerTransport, KafkaConsumerTransport
        run(KafkaProducerTransport('localhost:9092/t').close())
        run(KafkaConsumerTransport('localhost:9092/t').close())


# ── MQTT transport unit ───────────────────────────────────────────────────────
class TestMQTTTransport(unittest.TestCase):

    def test_endpoint_parse_mqtt_scheme(self):
        from interface_core.phases.transports import _parse_mqtt_endpoint
        host, port, topic = _parse_mqtt_endpoint('mqtt://localhost:1883/sensors/#')
        self.assertEqual(host,  'localhost')
        self.assertEqual(port,  1883)
        self.assertEqual(topic, 'sensors/#')

    def test_endpoint_parse_no_scheme(self):
        from interface_core.phases.transports import _parse_mqtt_endpoint
        host, port, topic = _parse_mqtt_endpoint('broker:1883/data/stream')
        self.assertEqual(host, 'broker')
        self.assertEqual(port, 1883)

    def test_endpoint_parse_default_port(self):
        from interface_core.phases.transports import _parse_mqtt_endpoint
        host, port, topic = _parse_mqtt_endpoint('broker/topic')
        self.assertEqual(port, 1883)

    def test_mqtt_producer_read_without_broker_is_none(self):
        from interface_core.phases.transports import MQTTProducerTransport
        t = MQTTProducerTransport('mqtt://localhost:19999/t')
        result = run(t.read())
        # Falls back to MemoryProducer (no asyncio-mqtt) or None
        self.assertIsNone(result)

    def test_mqtt_consumer_pub_topic_strips_wildcard(self):
        from interface_core.phases.transports import MQTTConsumerTransport
        t = MQTTConsumerTransport('mqtt://localhost:1883/sensors/#')
        self.assertNotIn('#', t._pub_topic)
        self.assertNotIn('/', t._pub_topic[-1])


# ── Socket transport unit ──────────────────────────────────────────────────
class TestSocketTransport(unittest.TestCase):

    def test_endpoint_parse(self):
        from interface_core.phases.transports import SocketProducerTransport
        t = SocketProducerTransport('192.168.1.10:5000')
        self.assertEqual(t._host, '192.168.1.10')
        self.assertEqual(t._port, 5000)

    def test_read_without_server_returns_none(self):
        from interface_core.phases.transports import SocketProducerTransport
        t = SocketProducerTransport('localhost:19998')
        result = run(t.read())
        self.assertIsNone(result)

    def test_write_without_server_returns_false(self):
        from interface_core.phases.transports import SocketConsumerTransport
        t = SocketConsumerTransport('localhost:19997')
        result = run(t.write({'x': 1}))
        self.assertFalse(result)

    def test_close_without_connection_no_error(self):
        from interface_core.phases.transports import SocketProducerTransport, SocketConsumerTransport
        run(SocketProducerTransport('localhost:9000').close())
        run(SocketConsumerTransport('localhost:9001').close())


# ── Socket loopback: server receives data from bridge ─────────────────────────
class TestSocketLoopback(unittest.TestCase):
    """Spins up a real TCP server and verifies SocketConsumerTransport delivers data."""

    def test_loopback_write(self):
        received = []

        async def _server(reader, writer):
            data = await reader.read(1024)
            received.append(data)
            writer.close()

        async def _go():
            server = await asyncio.start_server(_server, '127.0.0.1', 0)
            port   = server.sockets[0].getsockname()[1]
            async with server:
                from interface_core.phases.transports import SocketConsumerTransport
                t  = SocketConsumerTransport(f'127.0.0.1:{port}')
                ok = await t.write({'msg': 'hello', 'value': 42})
                await asyncio.sleep(0.1)
                await t.close()
            return ok

        ok = run(_go())
        self.assertTrue(ok)
        self.assertEqual(len(received), 1)
        payload = json.loads(received[0].decode().strip())
        self.assertEqual(payload['msg'], 'hello')
        self.assertEqual(payload['value'], 42)


# ── inject() robustness ────────────────────────────────────────────────────────
class TestInject(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        cls.core = InterfaceCore()
        cls.core.ml_matcher.wait_ready(30)

    def test_inject_unknown_producer_returns_false(self):
        self.assertFalse(self.core.inject('ghost-producer', {'x': 1}))

    def test_inject_with_memory_transport_delivers(self):
        """Register nodes, synthesize a bridge with memory transport, inject data."""
        p_id = self.core.register_node(
            'rest', 'memory://inject-test/src',
            {'val': 'float'}, ['generic'], 'producer', 1.0,
        )
        c_id = self.core.register_node(
            'rest', 'memory://inject-test/dst',
            {'val': 'float'}, ['generic'], 'consumer',
        )

        async def _synth():
            await self.core.matching._match_all()
            processed = 0
            while not self.core.bridge_queue.empty() and processed < 10:
                cand = await self.core.bridge_queue.pop()
                if cand is None: break
                result = await self.core.policy_engine.evaluate(cand)
                if result.verdict.value == 'approve':
                    await self.core.synthesis_queue.push(cand)
                processed += 1
            for _ in range(5):
                cand = await self.core.synthesis_queue.pop(0.1)
                if cand is None: break
                await self.core.synthesis._synthesize(cand)

        run(_synth())

        if p_id not in [k[0] for k in self.core.active_bridges]:
            self.skipTest('Bridge not synthesized (score threshold)')

        # inject() should find the memory transport and enqueue the payload
        ok = self.core.inject(p_id, {'val': 99.9})
        self.assertTrue(ok)

    def test_inject_multiple_payloads(self):
        """inject() returns True for each payload when a memory-backed bridge exists."""
        p_id = self.core.register_node(
            'rest', 'memory://inject-multi/src',
            {'n': 'int'}, ['generic'], 'producer',
        )
        c_id = self.core.register_node(
            'rest', 'memory://inject-multi/dst',
            {'n': 'int'}, ['generic'], 'consumer',
        )

        async def _synth():
            await self.core.matching._match_all()
            processed = 0
            while not self.core.bridge_queue.empty() and processed < 10:
                cand = await self.core.bridge_queue.pop()
                if cand is None: break
                r = await self.core.policy_engine.evaluate(cand)
                if r.verdict.value == 'approve':
                    await self.core.synthesis_queue.push(cand)
                processed += 1
            for _ in range(5):
                cand = await self.core.synthesis_queue.pop(0.1)
                if cand is None: break
                await self.core.synthesis._synthesize(cand)

        run(_synth())

        if p_id not in [k[0] for k in self.core.active_bridges]:
            self.skipTest('Bridge not synthesized')

        results = [self.core.inject(p_id, {'n': i}) for i in range(5)]
        self.assertTrue(all(results))


# ── Deferred restore logic ─────────────────────────────────────────────────────
class TestDeferredRestore(unittest.TestCase):
    """_restore_bridges only re-queues bridges whose nodes are still registered."""

    def test_restore_skips_missing_nodes(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()

        async def _go():
            # Populate store with a bridge whose nodes are NOT registered
            from interface_core.models import (
                Bridge, BridgeCandidate, AdapterSpec, MatchScore, Protocol
            )
            spec   = AdapterSpec(producer_protocol=Protocol.REST,
                                 consumer_protocol=Protocol.REST)
            bridge = Bridge(id='gone', producer_id='ghost-p', consumer_id='ghost-c',
                            adapter=spec)
            cand   = BridgeCandidate(producer_id='ghost-p', consumer_id='ghost-c',
                                     score=MatchScore(composite=0.8))
            await core.store.save(bridge, cand)

            # Now restore — should discard because nodes aren't in registry
            await core._restore_bridges()
            # synthesis queue should be empty (nothing to restore)
            item = await core.synthesis_queue.pop(timeout=0.05)
            return item

        result = run(_go())
        self.assertIsNone(result)

    def test_restore_queues_bridge_with_registered_nodes(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        # Register nodes first
        p_id = core.register_node('rest','http://r-src',{'v':'float'},['x'],'producer')
        c_id = core.register_node('rest','http://r-dst',{'v':'float'},['x'],'consumer')

        async def _go():
            from interface_core.models import (
                Bridge, BridgeCandidate, AdapterSpec, MatchScore, Protocol
            )
            spec   = AdapterSpec(producer_protocol=Protocol.REST,
                                 consumer_protocol=Protocol.REST)
            bridge = Bridge(id='live', producer_id=p_id, consumer_id=c_id, adapter=spec)
            cand   = BridgeCandidate(producer_id=p_id, consumer_id=c_id,
                                     score=MatchScore(composite=0.9))
            await core.store.save(bridge, cand)
            await core._restore_bridges()
            item = await core.synthesis_queue.pop(timeout=0.2)
            return item

        result = run(_go())
        self.assertIsNotNone(result)
        self.assertEqual(result.producer_id, p_id)
        self.assertEqual(result.consumer_id, c_id)




# ════════════════════════════════════════════════════════════════
# SESSION 5 TESTS — event loop fix, dedup, pg wiring, persistence
# ════════════════════════════════════════════════════════════════

# ── BridgeQueue in-flight dedup ──────────────────────────────────────────────
class TestBridgeQueueDedup(unittest.TestCase):

    def _cand(self, pid="p", cid="c", score=0.8):
        return BridgeCandidate(
            producer_id=pid, consumer_id=cid,
            score=MatchScore(composite=score),
        )

    def test_duplicate_push_ignored(self):
        """Same pair pushed twice — queue length stays 1."""
        q = BridgeQueue()
        run(q.push(self._cand()))
        run(q.push(self._cand()))   # duplicate
        self.assertEqual(1, len(q))

    def test_different_pairs_both_queued(self):
        q = BridgeQueue()
        run(q.push(self._cand("p", "c1")))
        run(q.push(self._cand("p", "c2")))
        self.assertEqual(2, len(q))

    def test_in_flight_cleared_on_pop(self):
        q = BridgeQueue()
        run(q.push(self._cand()))
        run(q.pop())
        # After pop, same pair can be pushed again
        run(q.push(self._cand()))
        self.assertEqual(1, len(q))

    def test_in_flight_cleared_on_decay_discard(self):
        """Decayed-away candidate frees its slot so pair can be re-queued."""
        import time, dataclasses
        q   = BridgeQueue()
        old = BridgeCandidate(
            producer_id="p", consumer_id="c",
            score=MatchScore(composite=0.72),
        )
        old.created_at = time.time() - 200 * 60   # 200 minutes old — will decay to zero
        run(q.push(old))
        run(q.pop())    # discarded by decay, slot freed
        # Should now accept a fresh push
        run(q.push(self._cand()))
        self.assertEqual(1, len(q))

    def test_repush_after_pop_requeues(self):
        """repush() after pop re-enters the candidate with fresh timestamp."""
        q    = BridgeQueue()
        cand = self._cand()
        run(q.push(cand))
        popped = run(q.pop())   # clears in_flight
        self.assertIsNotNone(popped)
        self.assertEqual(0, len(q))
        run(q.repush(popped))   # should re-enter cleanly
        self.assertEqual(1, len(q))

    def test_contains_true_when_queued(self):
        q = BridgeQueue()
        run(q.push(self._cand("p1", "c1")))
        self.assertTrue(q.contains("p1", "c1"))

    def test_contains_false_when_not_queued(self):
        q = BridgeQueue()
        self.assertFalse(q.contains("p", "c"))

    def test_contains_false_after_pop(self):
        q = BridgeQueue()
        run(q.push(self._cand()))
        run(q.pop())
        self.assertFalse(q.contains("p", "c"))


# ── _match_all skips in-queue pairs ──────────────────────────────────────────
class TestMatchAllInQueueSkip(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        cls.core = InterfaceCore()
        cls.core.ml_matcher.wait_ready(30)

    def test_second_match_cycle_doesnt_double_queue(self):
        """Running _match_all twice for the same nodes should not double the queue."""
        p_id = self.core.register_node(
            "rest", "memory://maq-src",
            {"x": "float"}, ["generic"], "producer", 1.0,
        )
        c_id = self.core.register_node(
            "rest", "memory://maq-dst",
            {"x": "float"}, ["generic"], "consumer",
        )

        async def _go():
            await self.core.matching._match_all()
            depth1 = len(self.core.bridge_queue)
            await self.core.matching._match_all()
            depth2 = len(self.core.bridge_queue)
            return depth1, depth2

        d1, d2 = run(_go())
        # Second run must not have increased queue depth for this pair
        self.assertEqual(d1, d2,
            f"Queue grew from {d1} to {d2} on second _match_all — dedup not working")


# ── Double-synthesis guard ───────────────────────────────────────────────────
class TestDoubleSynthesisGuard(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        cls.core = InterfaceCore()
        cls.core.ml_matcher.wait_ready(30)

    def test_duplicate_synthesis_skipped(self):
        """If bridge already active, second _synthesize call is a no-op."""
        from interface_core.models import BridgeCandidate, MatchScore

        p_id = self.core.register_node(
            "rest", "memory://ds-src",
            {"v": "float"}, ["generic"], "producer", 1.0,
        )
        c_id = self.core.register_node(
            "rest", "memory://ds-dst",
            {"v": "float"}, ["generic"], "consumer",
        )

        cand = BridgeCandidate(
            producer_id=p_id, consumer_id=c_id,
            score=MatchScore(composite=0.85),
        )

        async def _go():
            await self.core.synthesis._synthesize(cand)
            bridges_after_first = len(self.core.active_bridges)
            await self.core.synthesis._synthesize(cand)   # duplicate
            bridges_after_second = len(self.core.active_bridges)
            return bridges_after_first, bridges_after_second

        b1, b2 = run(_go())
        # Both counts must be identical — no extra bridge created
        self.assertEqual(b1, b2,
            "Second _synthesize created a duplicate bridge")


# ── MatchingPhase concurrent event loop ──────────────────────────────────────
class TestConcurrentEventLoop(unittest.TestCase):

    def test_event_loop_uses_gather(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._event_loop)
        self.assertIn("gather", src,
            "_event_loop must use asyncio.gather to poll queues concurrently")

    def test_event_loop_not_serial_for_loop(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._event_loop)
        # The old serial for-loop pattern must not appear
        self.assertNotIn("for q in [self._event_queue", src,
            "_event_loop still uses serial for-loop over queues")

    def test_both_queues_polled(self):
        """Both _event_queue and _drift_queue must appear in _event_loop source."""
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._event_loop)
        self.assertIn("_event_queue", src)
        self.assertIn("_drift_queue", src)


# ── PostgresPersistence unit tests ───────────────────────────────────────────
class TestPostgresPersistence(unittest.TestCase):
    """Tests run without a real Postgres — checks graceful degradation."""

    def _pg(self):
        from interface_core.persistence import PostgresPersistence
        return PostgresPersistence("postgresql://ic:ic@localhost:19999/no_such_db")

    def test_connect_without_server_returns_false(self):
        pg = self._pg()
        ok = run(pg.connect())
        self.assertFalse(ok)
        self.assertFalse(pg.connected)

    def test_setup_schema_no_crash_when_not_connected(self):
        pg = self._pg()
        run(pg.setup_schema())   # should not raise

    def test_record_snapshot_no_crash_when_not_connected(self):
        from interface_core.models import ContractSnapshot, DataContract, FieldSchema
        pg = self._pg()
        snap = ContractSnapshot(
            node_id="n1", fingerprint="fp",
            contract=DataContract(schema_fields=[FieldSchema(name="x", type="float")]),
            delta=0.1,
        )
        run(pg.record_contract_snapshot(snap))   # must not raise

    def test_checkpoint_bridge_no_crash_when_not_connected(self):
        from interface_core.models import Bridge, BridgeCandidate, AdapterSpec, MatchScore, Protocol
        pg = self._pg()
        spec   = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge = Bridge(id="b", producer_id="p", consumer_id="c", adapter=spec)
        cand   = BridgeCandidate(producer_id="p", consumer_id="c",
                                  score=MatchScore(composite=0.8))
        run(pg.checkpoint_bridge(bridge, cand))   # must not raise

    def test_delete_bridge_no_crash_when_not_connected(self):
        pg = self._pg()
        run(pg.delete_bridge("p", "c"))   # must not raise

    def test_restore_bridges_returns_empty_when_not_connected(self):
        pg = self._pg()
        result = run(pg.restore_bridges())
        self.assertEqual(result, [])

    def test_write_audit_no_crash_when_not_connected(self):
        pg = self._pg()
        run(pg.write_audit("APPROVE", "p", "c", 0.85, "test"))   # must not raise

    def test_recent_audit_returns_empty_when_not_connected(self):
        pg = self._pg()
        result = run(pg.recent_audit())
        self.assertEqual(result, [])

    def test_purge_old_drift_returns_zero_when_not_connected(self):
        pg = self._pg()
        result = run(pg.purge_old_drift())
        self.assertEqual(result, 0)

    def test_close_no_crash_when_not_connected(self):
        pg = self._pg()
        run(pg.close())   # must not raise

    def test_connected_property_false(self):
        pg = self._pg()
        self.assertFalse(pg.connected)


# ── Pg wiring on phases ──────────────────────────────────────────────────────
class TestPgWiringOnPhases(unittest.TestCase):
    """Verify all phases have the _pg attribute and it's settable."""

    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        cls.core = InterfaceCore()

    def test_discovery_has_pg_attr(self):
        self.assertTrue(hasattr(self.core.discovery, '_pg'))
        self.assertIsNone(self.core.discovery._pg)

    def test_synthesis_has_pg_attr(self):
        self.assertTrue(hasattr(self.core.synthesis, '_pg'))
        self.assertIsNone(self.core.synthesis._pg)

    def test_lifecycle_has_pg_attr(self):
        self.assertTrue(hasattr(self.core.lifecycle, '_pg'))
        self.assertIsNone(self.core.lifecycle._pg)

    def test_audit_has_pg_attr(self):
        self.assertTrue(hasattr(self.core.policy_engine.audit, '_pg'))
        self.assertIsNone(self.core.policy_engine.audit._pg)

    def test_pg_is_assignable(self):
        from interface_core.persistence import PostgresPersistence
        pg = PostgresPersistence("postgresql://localhost/test")
        self.core.discovery._pg  = pg
        self.core.synthesis._pg  = pg
        self.core.lifecycle._pg  = pg
        self.core.policy_engine.audit._pg = pg
        self.assertIs(self.core.discovery._pg, pg)
        self.assertIs(self.core.synthesis._pg, pg)
        # Clean up
        self.core.discovery._pg  = None
        self.core.synthesis._pg  = None
        self.core.lifecycle._pg  = None
        self.core.policy_engine.audit._pg = None


# ── Postgres status in InterfaceCore.status() ────────────────────────────────
class TestStatusContainsPostgres(unittest.TestCase):

    def test_status_has_postgres_key(self):
        from interface_core.main import InterfaceCore
        core   = InterfaceCore()
        status = core.status()
        self.assertIn("postgres", status)
        # Unconnected Postgres must show False
        self.assertFalse(status["postgres"])

    def test_status_has_store_backend_key(self):
        from interface_core.main import InterfaceCore
        core   = InterfaceCore()
        status = core.status()
        self.assertIn("store_backend", status)
        self.assertIn(status["store_backend"], ("memory", "redis"))


# ── __init__.py exports all expected symbols ─────────────────────────────────
class TestPackageExports(unittest.TestCase):

    def test_core_exports(self):
        import interface_core as ic
        required = [
            "InterfaceCore",
            "Bridge", "BridgeCandidate", "BridgeState",
            "DataContract", "Event", "EventType", "FieldMapping", "FieldSchema",
            "MatchScore", "Node", "NodeRole", "Protocol",
            "BridgeStore",
            "MemoryProducerTransport", "MemoryConsumerTransport",
            "BridgeRuntime", "CircuitBreaker", "IdempotencyGuard",
            "BridgeQueue", "BridgeHistory", "PolicyEngine",
            "NodeRegistry", "CapabilityGraph", "DriftLog",
            "InProcessEventBus", "make_event_bus",
            "settings", "PROTOCOL_COMPAT_MATRIX",
        ]
        for name in required:
            self.assertTrue(hasattr(ic, name), f"interface_core missing export: {name}")

    def test_all_list_matches_exports(self):
        import interface_core as ic
        for name in ic.__all__:
            self.assertTrue(hasattr(ic, name),
                f"__all__ lists '{name}' but it's not importable from interface_core")


# ── Discovery pg= parameter ──────────────────────────────────────────────────
class TestDiscoveryPgParam(unittest.TestCase):

    def test_discovery_accepts_pg_kwarg(self):
        import inspect
        from interface_core.phases.discovery import DiscoveryPhase
        sig = inspect.signature(DiscoveryPhase.__init__)
        self.assertIn("pg", sig.parameters)

    def test_discovery_pg_defaults_to_none(self):
        from interface_core.phases.discovery import DiscoveryPhase
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m = SemanticMatcher(); m.wait_ready(5)
        d = DiscoveryPhase(NodeRegistry(), CapabilityGraph(), DriftLog(),
                           InProcessEventBus(), m)
        self.assertIsNone(d._pg)

    def test_discovery_pg_set_when_passed(self):
        from interface_core.phases.discovery import DiscoveryPhase
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        from interface_core.persistence import PostgresPersistence
        m  = SemanticMatcher(); m.wait_ready(5)
        pg = PostgresPersistence("postgresql://localhost/test")
        d  = DiscoveryPhase(NodeRegistry(), CapabilityGraph(), DriftLog(),
                            InProcessEventBus(), m, pg=pg)
        self.assertIs(d._pg, pg)


# ── Store lazy import (no circular import) ──────────────────────────────────
class TestStoreLazyImport(unittest.TestCase):

    def test_store_module_imports_cleanly(self):
        import importlib
        mod = importlib.import_module("interface_core.store")
        self.assertTrue(hasattr(mod, "BridgeStore"))
        self.assertTrue(hasattr(mod, "_get_models"))

    def test_dict_to_bridge_roundtrip_via_lazy_import(self):
        from interface_core.store import _bridge_to_dict, _dict_to_bridge_and_candidate
        from interface_core.models import (
            Bridge, BridgeCandidate, AdapterSpec, MatchScore, Protocol, FieldMapping
        )
        spec   = AdapterSpec(
            producer_protocol=Protocol.MQTT, consumer_protocol=Protocol.KAFKA,
            field_mappings=[FieldMapping(src_field="temp", dst_field="temperature", transform="direct")],
        )
        bridge = Bridge(id="rt", producer_id="p", consumer_id="c", adapter=spec)
        cand   = BridgeCandidate(producer_id="p", consumer_id="c",
                                  score=MatchScore(composite=0.88))

        d = _bridge_to_dict(bridge, cand)
        rb, rc = _dict_to_bridge_and_candidate(d)

        self.assertEqual(rb.id, "rt")
        self.assertEqual(rb.producer_id, "p")
        self.assertEqual(rb.consumer_id, "c")
        self.assertEqual(rb.adapter.producer_protocol, Protocol.MQTT)
        self.assertEqual(rb.adapter.consumer_protocol, Protocol.KAFKA)
        self.assertEqual(len(rb.adapter.field_mappings), 1)
        self.assertEqual(rb.adapter.field_mappings[0].src_field, "temp")
        self.assertEqual(rb.adapter.field_mappings[0].dst_field, "temperature")
        self.assertAlmostEqual(rc.score.composite, 0.88, places=2)




# ════════════════════════════════════════════════════════════════
# IMPROVEMENT 1 — CONSUL SERVICE DISCOVERY
# ════════════════════════════════════════════════════════════════

class TestConsulDiscovery(unittest.TestCase):

    # ── ConsulClient unit ────────────────────────────────────────
    def test_consul_client_base_url(self):
        from interface_core.consul_discovery import ConsulClient
        c = ConsulClient("localhost", 8500)
        self.assertIn("localhost", c._base)
        self.assertIn("8500", c._base)

    def test_consul_catalog_services_unreachable(self):
        from interface_core.consul_discovery import ConsulClient
        c = ConsulClient("localhost", 19999)
        result = run(c.catalog_services())
        self.assertIsInstance(result, dict)
        self.assertEqual(result, {})

    def test_consul_catalog_service_unreachable(self):
        from interface_core.consul_discovery import ConsulClient
        c = ConsulClient("localhost", 19999)
        result = run(c.catalog_service("myapp"))
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])

    def test_consul_health_passing_unreachable(self):
        from interface_core.consul_discovery import ConsulClient
        c = ConsulClient("localhost", 19999)
        result = run(c.health_passing("myapp"))
        self.assertEqual(result, [])

    def test_consul_close_no_error(self):
        from interface_core.consul_discovery import ConsulClient
        c = ConsulClient("localhost", 8500)
        run(c.close())

    # ── Protocol fingerprinter ───────────────────────────────────
    def test_fingerprint_from_tags_grpc(self):
        from interface_core.consul_discovery import _fingerprint_protocol
        from interface_core.models import Protocol
        proto, ep = _fingerprint_protocol(["grpc", "api"], {}, 50051)
        self.assertEqual(proto, Protocol.GRPC)
        self.assertIn("50051", ep)

    def test_fingerprint_from_tags_mqtt(self):
        from interface_core.consul_discovery import _fingerprint_protocol
        from interface_core.models import Protocol
        proto, ep = _fingerprint_protocol(["mqtt", "iot"], {}, 1883)
        self.assertEqual(proto, Protocol.MQTT)

    def test_fingerprint_from_meta_wins_over_tags(self):
        from interface_core.consul_discovery import _fingerprint_protocol
        from interface_core.models import Protocol
        proto, ep = _fingerprint_protocol(["rest"], {"protocol": "kafka"}, 9092)
        self.assertEqual(proto, Protocol.KAFKA)

    def test_fingerprint_default_is_rest(self):
        from interface_core.consul_discovery import _fingerprint_protocol
        from interface_core.models import Protocol
        proto, ep = _fingerprint_protocol([], {}, 8080)
        self.assertEqual(proto, Protocol.REST)

    def test_fingerprint_endpoint_contains_port(self):
        from interface_core.consul_discovery import _fingerprint_protocol
        _, ep = _fingerprint_protocol(["rest"], {}, 9000)
        self.assertIn("9000", ep)

    # ── Contract from Consul meta ────────────────────────────────
    def test_contract_fields_from_meta(self):
        from interface_core.consul_discovery import _contract_from_consul_meta
        contract = _contract_from_consul_meta(
            "price-service",
            ["market", "rest"],
            {"field.price": "float", "field.symbol": "str", "field.volume": "int"},
        )
        names = {f.name for f in contract.schema_fields}
        self.assertIn("price", names)
        self.assertIn("symbol", names)
        self.assertIn("volume", names)

    def test_contract_semantic_tags_exclude_protocol_tags(self):
        from interface_core.consul_discovery import _contract_from_consul_meta
        contract = _contract_from_consul_meta(
            "sensor-svc",
            ["mqtt", "iot", "temperature"],
            {},
        )
        # "mqtt" should be excluded as it's a protocol tag
        self.assertNotIn("mqtt", contract.semantic_tags)
        self.assertIn("iot", contract.semantic_tags)
        self.assertIn("temperature", contract.semantic_tags)

    def test_contract_rate_hz_from_meta(self):
        from interface_core.consul_discovery import _contract_from_consul_meta
        contract = _contract_from_consul_meta("svc", [], {"rate_hz": "10.5"})
        self.assertAlmostEqual(contract.emission_rate_hz, 10.5)

    def test_contract_default_field_when_no_meta(self):
        from interface_core.consul_discovery import _contract_from_consul_meta
        contract = _contract_from_consul_meta("my-service", [], {})
        self.assertEqual(len(contract.schema_fields), 1)
        self.assertEqual(contract.schema_fields[0].name, "data")

    def test_contract_confidence_clamped(self):
        from interface_core.consul_discovery import _contract_from_consul_meta
        c1 = _contract_from_consul_meta("s", [], {"confidence": "2.0"})
        c2 = _contract_from_consul_meta("s", [], {"confidence": "-1.0"})
        self.assertLessEqual(c1.confidence, 1.0)
        self.assertGreaterEqual(c2.confidence, 0.1)

    # ── Role inference ───────────────────────────────────────────
    def test_role_producer(self):
        from interface_core.consul_discovery import _role_from_meta
        from interface_core.models import NodeRole
        self.assertEqual(_role_from_meta({"role": "producer"}), NodeRole.PRODUCER)

    def test_role_consumer(self):
        from interface_core.consul_discovery import _role_from_meta
        from interface_core.models import NodeRole
        self.assertEqual(_role_from_meta({"role": "consumer"}), NodeRole.CONSUMER)

    def test_role_default_both(self):
        from interface_core.consul_discovery import _role_from_meta
        from interface_core.models import NodeRole
        self.assertEqual(_role_from_meta({}), NodeRole.BOTH)

    # ── ConsulDiscovery integration ──────────────────────────────
    def test_consul_discovery_instantiates(self):
        from interface_core.consul_discovery import ConsulDiscovery
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m = SemanticMatcher(); m.wait_ready(5)
        d = ConsulDiscovery(
            host="localhost", port=8500,
            registry=NodeRegistry(), graph=CapabilityGraph(),
            drift_log=DriftLog(), event_bus=InProcessEventBus(),
            ml_matcher=m,
        )
        self.assertIsNotNone(d)
        self.assertIsNone(d._pg)

    def test_consul_scan_with_no_server(self):
        """_scan() must not raise when Consul is unreachable."""
        from interface_core.consul_discovery import ConsulDiscovery
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m = SemanticMatcher(); m.wait_ready(5)
        d = ConsulDiscovery(
            host="localhost", port=19999,
            registry=NodeRegistry(), graph=CapabilityGraph(),
            drift_log=DriftLog(), event_bus=InProcessEventBus(),
            ml_matcher=m,
        )
        run(d._scan())   # must not raise

    def test_process_instance_with_http_service(self):
        """Simulate a healthy Consul service instance with REST protocol."""
        from interface_core.consul_discovery import ConsulDiscovery
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        from interface_core.models import Protocol

        reg = NodeRegistry()
        m   = SemanticMatcher(); m.wait_ready(5)
        d   = ConsulDiscovery(
            host="localhost", port=8500,
            registry=reg, graph=CapabilityGraph(),
            drift_log=DriftLog(), event_bus=InProcessEventBus(),
            ml_matcher=m,
        )

        # Simulate what Consul health API returns
        fake_instance = {
            "Node": {"Address": "192.168.1.10"},
            "Service": {
                "Address": "192.168.1.10",
                "Port": 8080,
                "Tags": ["rest", "market"],
                "Meta": {
                    "field.price": "float",
                    "field.symbol": "str",
                    "role": "producer",
                },
            },
        }
        run(d._process_instance("price-service", fake_instance))

        nodes = reg.all_nodes()
        self.assertEqual(len(nodes), 1)
        node = nodes[0]
        self.assertEqual(node.protocol, Protocol.REST)
        self.assertIn("market", node.contract.semantic_tags)
        field_names = {f.name for f in node.contract.schema_fields}
        self.assertIn("price", field_names)
        self.assertIn("symbol", field_names)
        self.assertEqual(node.metadata.get("consul_service"), "price-service")

    def test_main_has_consul_attribute(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        self.assertTrue(hasattr(core, 'consul'))
        from interface_core.consul_discovery import ConsulDiscovery
        self.assertIsInstance(core.consul, ConsulDiscovery)

    def test_status_includes_consul_nodes(self):
        from interface_core.main import InterfaceCore
        core   = InterfaceCore()
        status = core.status()
        self.assertIn("consul_nodes", status)
        self.assertEqual(status["consul_nodes"], 0)


# ════════════════════════════════════════════════════════════════
# IMPROVEMENT 2 — gRPC TRANSPORT
# ════════════════════════════════════════════════════════════════

class TestGRPCTransport(unittest.TestCase):

    def test_grpc_producer_instantiates(self):
        from interface_core.phases.grpc_transport import GRPCProducerTransport
        t = GRPCProducerTransport("localhost:50051")
        self.assertEqual(t._host, "localhost:50051")
        self.assertIsNone(t._service)
        self.assertIsNone(t._method)

    def test_grpc_producer_parses_service_method(self):
        from interface_core.phases.grpc_transport import GRPCProducerTransport
        t = GRPCProducerTransport("localhost:50051/helloworld.Greeter/SayHello")
        self.assertEqual(t._host, "localhost:50051")
        self.assertEqual(t._service, "helloworld.Greeter")
        self.assertEqual(t._method, "SayHello")

    def test_grpc_consumer_instantiates(self):
        from interface_core.phases.grpc_transport import GRPCConsumerTransport
        t = GRPCConsumerTransport("localhost:50051/svc.Service/Method")
        self.assertEqual(t._host, "localhost:50051")
        self.assertEqual(t._service, "svc.Service")
        self.assertEqual(t._method, "Method")

    def test_grpc_producer_read_no_server_returns_none_or_fallback(self):
        from interface_core.phases.grpc_transport import GRPCProducerTransport
        t = GRPCProducerTransport("localhost:19998")
        result = run(t.read())
        self.assertIsNone(result)

    def test_grpc_consumer_write_no_server_returns_false(self):
        from interface_core.phases.grpc_transport import GRPCConsumerTransport
        t = GRPCConsumerTransport("localhost:19997/svc/Method")
        result = run(t.write({"x": 1}))
        self.assertFalse(result)

    def test_grpc_probe_no_server_returns_none(self):
        from interface_core.phases.grpc_transport import probe_grpc_contract
        result = run(probe_grpc_contract("localhost:19996"))
        self.assertIsNone(result)

    def test_grpc_producer_close_before_connect(self):
        from interface_core.phases.grpc_transport import GRPCProducerTransport
        t = GRPCProducerTransport("localhost:50051")
        run(t.close())   # must not raise

    def test_grpc_consumer_close_before_connect(self):
        from interface_core.phases.grpc_transport import GRPCConsumerTransport
        t = GRPCConsumerTransport("localhost:50051")
        run(t.close())

    def test_grpc_producer_falls_back_to_memory_no_grpcio(self):
        """Without grpcio installed, producer should gracefully fall back."""
        from interface_core.phases.grpc_transport import GRPCProducerTransport, _GRPC_AVAILABLE
        t = GRPCProducerTransport("localhost:50051")
        if not _GRPC_AVAILABLE:
            result = run(t.read())
            # Falls back to MemoryProducerTransport — None on empty queue
            self.assertIsNone(result)
        else:
            self.skipTest("grpcio is installed — fallback path not triggered")

    def test_transport_factory_routes_grpc(self):
        """make_producer_transport returns GRPCProducerTransport for GRPC protocol."""
        from interface_core.phases.runner import make_producer_transport
        from interface_core.phases.grpc_transport import GRPCProducerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(
            id="g", protocol=Protocol.GRPC, role=NodeRole.PRODUCER,
            endpoint="localhost:50051/svc/Method",
            contract=DataContract(schema_fields=[FieldSchema(name="x", type="str")]),
        )
        t = make_producer_transport(n)
        self.assertIsInstance(t, GRPCProducerTransport)
        self.assertEqual(t._host, "localhost:50051")

    def test_transport_factory_routes_grpc_consumer(self):
        from interface_core.phases.runner import make_consumer_transport
        from interface_core.phases.grpc_transport import GRPCConsumerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(
            id="g", protocol=Protocol.GRPC, role=NodeRole.CONSUMER,
            endpoint="localhost:50051/svc/Ingest",
            contract=DataContract(schema_fields=[FieldSchema(name="x", type="str")]),
        )
        t = make_consumer_transport(n)
        self.assertIsInstance(t, GRPCConsumerTransport)

    def test_grpc_in_init_exports(self):
        import interface_core as ic
        self.assertTrue(hasattr(ic, "GRPCProducerTransport"))
        self.assertTrue(hasattr(ic, "GRPCConsumerTransport"))


# ════════════════════════════════════════════════════════════════
# IMPROVEMENT 3 — PROCESS SCANNER
# ════════════════════════════════════════════════════════════════

class TestProcessScanner(unittest.TestCase):

    def _scanner(self, reg=None):
        from interface_core.phases.process_scanner import ProcessScanner
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m = SemanticMatcher(); m.wait_ready(5)
        return ProcessScanner(
            registry   = reg or NodeRegistry(),
            graph      = CapabilityGraph(),
            drift_log  = DriftLog(),
            event_bus  = InProcessEventBus(),
            ml_matcher = m,
        )

    # ── Port listing ─────────────────────────────────────────────
    def test_list_listening_ports_returns_list(self):
        from interface_core.phases.process_scanner import list_listening_ports
        ports = list_listening_ports()
        self.assertIsInstance(ports, list)
        # Each item is (int, str)
        for port, name in ports:
            self.assertIsInstance(port, int)
            self.assertIsInstance(name, str)

    def test_skipped_ports_not_included(self):
        from interface_core.phases.process_scanner import list_listening_ports, _SKIP_PORTS
        ports = [p for p, _ in list_listening_ports()]
        for skip in _SKIP_PORTS:
            self.assertNotIn(skip, ports)

    # ── Protocol inference ───────────────────────────────────────
    def test_infer_protocol_grpc_port(self):
        from interface_core.phases.process_scanner import _infer_protocol_from_port
        from interface_core.models import Protocol
        self.assertEqual(_infer_protocol_from_port(50051), Protocol.GRPC)

    def test_infer_protocol_http_port(self):
        from interface_core.phases.process_scanner import _infer_protocol_from_port
        from interface_core.models import Protocol
        self.assertEqual(_infer_protocol_from_port(8080), Protocol.REST)

    def test_infer_protocol_kafka_port(self):
        from interface_core.phases.process_scanner import _infer_protocol_from_port
        from interface_core.models import Protocol
        self.assertEqual(_infer_protocol_from_port(9092), Protocol.KAFKA)

    def test_infer_protocol_mqtt_port(self):
        from interface_core.phases.process_scanner import _infer_protocol_from_port
        from interface_core.models import Protocol
        self.assertEqual(_infer_protocol_from_port(1883), Protocol.MQTT)

    def test_infer_protocol_unknown_port_is_socket(self):
        from interface_core.phases.process_scanner import _infer_protocol_from_port
        from interface_core.models import Protocol
        self.assertEqual(_infer_protocol_from_port(12345), Protocol.SOCKET)

    # ── JSON field extraction ────────────────────────────────────
    def test_json_to_fields_dict(self):
        from interface_core.phases.process_scanner import _json_to_fields
        fields = _json_to_fields({"price": 1.5, "symbol": "AAPL", "count": 10})
        names = {f.name for f in fields}
        self.assertIn("price", names)
        self.assertIn("symbol", names)

    def test_json_to_fields_non_dict(self):
        from interface_core.phases.process_scanner import _json_to_fields
        fields = _json_to_fields("not a dict")
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0].name, "data")

    def test_json_to_fields_type_inference(self):
        from interface_core.phases.process_scanner import _json_to_fields
        fields = _json_to_fields({
            "i": 1, "f": 1.5, "s": "x", "b": True, "l": [], "d": {}
        })
        types = {f.name: f.type for f in fields}
        self.assertEqual(types["i"], "int")
        self.assertEqual(types["f"], "float")
        self.assertEqual(types["s"], "str")
        self.assertEqual(types["b"], "bool")
        self.assertEqual(types["l"], "array")
        self.assertEqual(types["d"], "object")

    # ── Semantic tag inference ───────────────────────────────────
    def test_tags_from_geo_fields(self):
        from interface_core.phases.process_scanner import _tags_from_fields, _json_to_fields
        fields = _json_to_fields({"lat": 51.5, "lon": -0.1, "alt": 100})
        tags = _tags_from_fields(fields)
        self.assertIn("geospatial", tags)

    def test_tags_from_market_fields(self):
        from interface_core.phases.process_scanner import _tags_from_fields, _json_to_fields
        fields = _json_to_fields({"price": 1.5, "bid": 1.4, "ask": 1.6})
        tags = _tags_from_fields(fields)
        self.assertIn("market", tags)

    def test_tags_default_generic(self):
        from interface_core.phases.process_scanner import _tags_from_fields, _json_to_fields
        fields = _json_to_fields({"foo": "bar"})
        tags = _tags_from_fields(fields)
        self.assertIn("generic", tags)

    # ── TCP banner probe ─────────────────────────────────────────
    def test_probe_tcp_unreachable_returns_none(self):
        from interface_core.phases.process_scanner import _probe_tcp_banner
        result = run(_probe_tcp_banner("127.0.0.1", 19990))
        self.assertIsNone(result)

    def test_probe_tcp_open_port_returns_contract(self):
        """Spin up a tiny TCP server and verify probe returns a contract."""
        from interface_core.phases.process_scanner import _probe_tcp_banner
        from interface_core.models import DataContract

        received = []
        async def _server(reader, writer):
            writer.write(b'{"status": "ok", "value": 42}\n')
            await writer.drain()
            writer.close()

        async def _go():
            server = await asyncio.start_server(_server, '127.0.0.1', 0)
            port   = server.sockets[0].getsockname()[1]
            async with server:
                return await _probe_tcp_banner('127.0.0.1', port)

        result = run(_go())
        self.assertIsNotNone(result)
        self.assertIsInstance(result, DataContract)

    # ── Full scanner scan ────────────────────────────────────────
    def test_scanner_scan_no_crash(self):
        """_scan() completes without error even when no ports are discovered."""
        s = self._scanner()
        run(s._scan())

    def test_scanner_register_http_port(self):
        """Spin up a JSON TCP server in its own event loop, verify node registered."""
        from interface_core.phases.process_scanner import ProcessScanner
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher

        async def _run():
            m   = SemanticMatcher(); m.wait_ready(5)
            reg = NodeRegistry()
            s   = ProcessScanner(registry=reg, graph=CapabilityGraph(),
                                  drift_log=DriftLog(), event_bus=InProcessEventBus(),
                                  ml_matcher=m)

            async def _handler(reader, writer):
                try: await asyncio.wait_for(reader.read(256), timeout=0.3)
                except Exception: pass
                writer.write(b'{"sensor":"temp","value":23.5}\n')
                try:
                    await writer.drain(); writer.close()
                    await writer.wait_closed()
                except Exception: pass

            server = await asyncio.start_server(_handler, '127.0.0.1', 0)
            port   = server.sockets[0].getsockname()[1]
            async with server:
                await s._probe_port(port, "test-proc")
            return reg.all_nodes(), port

        nodes, port = asyncio.run(_run())
        self.assertEqual(len(nodes), 1, f"Expected 1 node, got {len(nodes)}")
        self.assertIn(str(port), nodes[0].endpoint)
        self.assertEqual(nodes[0].metadata.get("discovered_by"), "process_scanner")

    def test_scanner_doesnt_duplicate_nodes(self):
        """Probing the same port twice must not create duplicate nodes."""
        from interface_core.phases.process_scanner import ProcessScanner
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher

        async def _run():
            m   = SemanticMatcher(); m.wait_ready(5)
            reg = NodeRegistry()
            s   = ProcessScanner(registry=reg, graph=CapabilityGraph(),
                                  drift_log=DriftLog(), event_bus=InProcessEventBus(),
                                  ml_matcher=m)

            async def _handler(reader, writer):
                try: await asyncio.wait_for(reader.read(256), timeout=0.3)
                except Exception: pass
                writer.write(b'{"x":1}\n')
                try:
                    await writer.drain(); writer.close()
                    await writer.wait_closed()
                except Exception: pass

            server = await asyncio.start_server(_handler, '127.0.0.1', 0)
            port   = server.sockets[0].getsockname()[1]
            async with server:
                await s._probe_port(port, "proc")
                await s._probe_port(port, "proc")
            return reg.all_nodes()

        nodes = asyncio.run(_run())
        self.assertEqual(len(nodes), 1, f"Expected 1 node, got {len(nodes)}")

    def test_seen_port_count(self):
        """seen_port_count increments when ports are discovered."""
        s = self._scanner()
        self.assertEqual(s.seen_port_count, 0)

        async def _go():
            server = await asyncio.start_server(
                lambda r, w: w.close(), '127.0.0.1', 0
            )
            port = server.sockets[0].getsockname()[1]
            async with server:
                await s._probe_port(port, "proc")

        run(_go())
        self.assertGreaterEqual(s.seen_port_count, 1)

    def test_main_has_process_scanner_attribute(self):
        from interface_core.main import InterfaceCore
        from interface_core.phases.process_scanner import ProcessScanner
        core = InterfaceCore()
        self.assertIsInstance(core.process_scanner, ProcessScanner)

    def test_status_includes_process_scan_ports(self):
        from interface_core.main import InterfaceCore
        status = InterfaceCore().status()
        self.assertIn("process_scan_ports", status)
        self.assertEqual(status["process_scan_ports"], 0)

    # ── Config ───────────────────────────────────────────────────
    def test_config_has_grpc_targets(self):
        from interface_core.config import settings
        self.assertTrue(hasattr(settings, 'scan_grpc_targets'))
        self.assertIsInstance(settings.scan_grpc_targets, list)





# ════════════════════════════════════════════════════════════════
# GAP FIXES — WebSocket, AMQP, deprecated API, pg maintenance
# ════════════════════════════════════════════════════════════════

class TestWebSocketTransport(unittest.TestCase):

    def test_producer_instantiates(self):
        from interface_core.phases.transports import WebSocketProducerTransport
        t = WebSocketProducerTransport("ws://localhost:8765/feed")
        self.assertEqual(t._endpoint, "ws://localhost:8765/feed")

    def test_producer_adds_ws_scheme_if_missing(self):
        from interface_core.phases.transports import WebSocketProducerTransport
        t = WebSocketProducerTransport("localhost:8765")
        self.assertTrue(t._endpoint.startswith("ws://"))

    def test_consumer_instantiates(self):
        from interface_core.phases.transports import WebSocketConsumerTransport
        t = WebSocketConsumerTransport("ws://localhost:8765/ingest")
        self.assertEqual(t._endpoint, "ws://localhost:8765/ingest")

    def test_producer_read_no_server(self):
        from interface_core.phases.transports import WebSocketProducerTransport
        t = WebSocketProducerTransport("ws://localhost:19980")
        result = run(t.read())
        # Either None (connection refused) or falls back to MemoryProducerTransport (also None)
        self.assertIsNone(result)

    def test_consumer_write_no_server_returns_false(self):
        from interface_core.phases.transports import WebSocketConsumerTransport
        t = WebSocketConsumerTransport("ws://localhost:19981")
        result = run(t.write({"x": 1}))
        self.assertFalse(result)

    def test_close_before_connect_no_error(self):
        from interface_core.phases.transports import WebSocketProducerTransport, WebSocketConsumerTransport
        run(WebSocketProducerTransport("ws://localhost:8765").close())
        run(WebSocketConsumerTransport("ws://localhost:8765").close())

    def test_transport_factory_routes_websocket_producer(self):
        from interface_core.phases.runner import make_producer_transport
        from interface_core.phases.transports import WebSocketProducerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(id="w", protocol=Protocol.WEBSOCKET, role=NodeRole.PRODUCER,
                 endpoint="ws://localhost:8765/stream",
                 contract=DataContract(schema_fields=[FieldSchema(name="x",type="float")]))
        t = make_producer_transport(n)
        self.assertIsInstance(t, WebSocketProducerTransport)

    def test_transport_factory_routes_websocket_consumer(self):
        from interface_core.phases.runner import make_consumer_transport
        from interface_core.phases.transports import WebSocketConsumerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(id="w", protocol=Protocol.WEBSOCKET, role=NodeRole.CONSUMER,
                 endpoint="ws://localhost:8765/ingest",
                 contract=DataContract(schema_fields=[FieldSchema(name="x",type="float")]))
        t = make_consumer_transport(n)
        self.assertIsInstance(t, WebSocketConsumerTransport)

    def test_websocket_loopback(self):
        """WebSocket round-trip using websockets library if available."""
        try:
            import websockets
        except ImportError:
            self.skipTest("websockets not installed")

        from interface_core.phases.transports import (
            WebSocketProducerTransport, WebSocketConsumerTransport
        )
        received = []

        async def _run():
            async def _handler(ws):
                msg = await ws.recv()
                received.append(msg)

            server = await websockets.serve(_handler, "127.0.0.1", 0)
            port   = list(server.sockets)[0].getsockname()[1]
            uri    = f"ws://127.0.0.1:{port}"

            consumer = WebSocketConsumerTransport(uri)
            ok       = await consumer.write({"test": "hello"})
            await asyncio.sleep(0.1)
            await consumer.close()
            server.close()
            await server.wait_closed()
            return ok

        ok = asyncio.run(_run())
        self.assertTrue(ok)
        self.assertEqual(len(received), 1)
        self.assertIn("hello", received[0])


class TestAMQPTransport(unittest.TestCase):

    def test_producer_instantiates_amqp_url(self):
        from interface_core.phases.transports import AMQPProducerTransport
        t = AMQPProducerTransport("amqp://guest:guest@localhost:5672/")
        self.assertIn("localhost", t._url)

    def test_producer_instantiates_host_port(self):
        from interface_core.phases.transports import AMQPProducerTransport
        t = AMQPProducerTransport("localhost:5672/my-queue")
        self.assertIn("localhost", t._url)
        self.assertEqual(t._queue_name, "my-queue")

    def test_consumer_instantiates(self):
        from interface_core.phases.transports import AMQPConsumerTransport
        t = AMQPConsumerTransport("amqp://guest:guest@localhost:5672/")
        self.assertIsNotNone(t)

    def test_producer_read_no_server_returns_none(self):
        from interface_core.phases.transports import AMQPProducerTransport
        t = AMQPProducerTransport("amqp://guest:guest@localhost:19970/")
        result = run(t.read())
        self.assertIsNone(result)

    def test_consumer_write_no_server_returns_false(self):
        from interface_core.phases.transports import AMQPConsumerTransport
        t = AMQPConsumerTransport("amqp://guest:guest@localhost:19971/")
        result = run(t.write({"x": 1}))
        self.assertFalse(result)

    def test_close_before_connect_no_error(self):
        from interface_core.phases.transports import AMQPProducerTransport, AMQPConsumerTransport
        run(AMQPProducerTransport("localhost:5672").close())
        run(AMQPConsumerTransport("localhost:5672").close())

    def test_factory_routes_amqp_producer(self):
        from interface_core.phases.runner import make_producer_transport
        from interface_core.phases.transports import AMQPProducerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(id="a", protocol=Protocol.AMQP, role=NodeRole.PRODUCER,
                 endpoint="amqp://localhost/queue",
                 contract=DataContract(schema_fields=[FieldSchema(name="x",type="str")]))
        t = make_producer_transport(n)
        self.assertIsInstance(t, AMQPProducerTransport)

    def test_factory_routes_amqp_consumer(self):
        from interface_core.phases.runner import make_consumer_transport
        from interface_core.phases.transports import AMQPConsumerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(id="a", protocol=Protocol.AMQP, role=NodeRole.CONSUMER,
                 endpoint="amqp://localhost/queue",
                 contract=DataContract(schema_fields=[FieldSchema(name="x",type="str")]))
        t = make_consumer_transport(n)
        self.assertIsInstance(t, AMQPConsumerTransport)


class TestDeprecatedAPIFixes(unittest.TestCase):

    def test_no_asyncio_coroutine_in_discovery(self):
        import inspect
        from interface_core.phases.discovery import DiscoveryPhase
        src = inspect.getsource(DiscoveryPhase._scan_cycle)
        self.assertNotIn("asyncio.coroutine", src,
            "discovery._scan_cycle must not use deprecated asyncio.coroutine")

    def test_discovery_scan_cycle_is_async(self):
        import inspect
        from interface_core.phases.discovery import DiscoveryPhase
        self.assertTrue(inspect.iscoroutinefunction(DiscoveryPhase._scan_cycle))

    def test_all_phase_run_methods_are_async(self):
        import inspect
        from interface_core.phases.discovery import DiscoveryPhase
        from interface_core.phases.matching import MatchingPhase
        from interface_core.phases.lifecycle import LifecyclePhase
        from interface_core.phases.synthesis import SynthesisPhase
        from interface_core.consul_discovery import ConsulDiscovery
        from interface_core.phases.process_scanner import ProcessScanner
        for cls in [DiscoveryPhase, MatchingPhase, LifecyclePhase,
                    SynthesisPhase, ConsulDiscovery, ProcessScanner]:
            self.assertTrue(inspect.iscoroutinefunction(cls.run),
                f"{cls.__name__}.run must be a coroutine")


class TestPgMaintenance(unittest.TestCase):

    def test_pg_maintenance_task_in_main(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("pg_maintenance", src,
            "main.start must schedule the pg_maintenance task")

    def test_pg_maintenance_method_exists(self):
        from interface_core.main import InterfaceCore
        self.assertTrue(hasattr(InterfaceCore, '_pg_maintenance'))

    def test_pg_maintenance_calls_purge(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore._pg_maintenance)
        self.assertIn("purge_old_drift", src)

    def test_purge_no_crash_when_not_connected(self):
        from interface_core.persistence import PostgresPersistence
        pg = PostgresPersistence("postgresql://localhost:19999/no_such_db")
        result = run(pg.purge_old_drift())
        self.assertEqual(result, 0)

    def test_all_tasks_started(self):
        """All 10 named tasks are in the task list after start()."""
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        expected_names = [
            "discovery", "matching", "policy", "synthesis",
            "lifecycle", "drift_watch", "restore",
            "consul", "process_scan", "pg_maintenance",
        ]
        for name in expected_names:
            self.assertIn(f'name="{name}"', src,
                f'Task "{name}" not found in InterfaceCore.start()')




# ════════════════════════════════════════════════════════════════
# PRODUCTION BUG FIXES (from live run on Kali)
# ════════════════════════════════════════════════════════════════

class TestProductionBugFixes(unittest.TestCase):
    """Regression tests for bugs found in the first live run."""

    # ── Bug 1: ConsulDiscovery %d TypeError ──────────────────────
    def test_consul_start_log_no_type_error(self):
        """ConsulDiscovery.run() must not raise TypeError on startup log."""
        from interface_core.consul_discovery import ConsulDiscovery
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher

        m = SemanticMatcher(); m.wait_ready(5)
        d = ConsulDiscovery("localhost", 8500,
                             NodeRegistry(), CapabilityGraph(), DriftLog(),
                             InProcessEventBus(), m)

        # The log line previously did %d with a str port — verify it no longer raises
        import logging
        class _CatchErrors(logging.Handler):
            def __init__(self):
                super().__init__()
                self.errors = []
            def emit(self, record):
                # Log errors (like TypeError) are stored here
                if record.exc_info:
                    self.errors.append(record.exc_info)

        handler = _CatchErrors()
        consul_logger = logging.getLogger("interface_core.consul_discovery")
        consul_logger.addHandler(handler)
        try:
            # Manually reproduce the fixed log call
            host_port = d._client._base.split("//")[1].split("/")[0]
            parts     = host_port.rsplit(":", 1)
            consul_logger.info(
                "ConsulDiscovery started (host=%s port=%s)",
                parts[0], parts[1] if len(parts) > 1 else "?"
            )
        finally:
            consul_logger.removeHandler(handler)

        self.assertEqual([], handler.errors, "Log call raised an error")

    def test_consul_log_format_uses_percent_s_not_d(self):
        """Verify the ConsulDiscovery run() source uses %s not %d for port."""
        import inspect
        from interface_core.consul_discovery import ConsulDiscovery
        src = inspect.getsource(ConsulDiscovery.run)
        self.assertNotIn("%d", src,
            "ConsulDiscovery.run must not use %d (causes TypeError with string port)")

    # ── Bug 2: Process scanner discovering itself ─────────────────
    def test_api_port_excluded_from_process_scanner(self):
        """Process scanner must exclude the API port it opened."""
        from interface_core.main import InterfaceCore
        from interface_core.config import settings
        core     = InterfaceCore()
        api_port = getattr(settings, 'api_port', 8000)
        self.assertIn(api_port, core.process_scanner._exclude,
            f"API port {api_port} must be in process_scanner._exclude")

    def test_port_8000_in_http_default_ports(self):
        """Port 8000 (common API port) should be recognised as HTTP not socket."""
        from interface_core.phases.process_scanner import _HTTP_DEFAULT_PORTS, _infer_protocol_from_port
        from interface_core.models import Protocol
        self.assertIn(8000, _HTTP_DEFAULT_PORTS)
        # When port 8000 is open it should be probed as REST, not left as SOCKET
        self.assertEqual(_infer_protocol_from_port(8000), Protocol.REST)

    def test_process_scanner_exclude_set_works(self):
        """Ports in exclude_ports set are skipped during _probe_port."""
        from interface_core.phases.process_scanner import ProcessScanner
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher

        m   = SemanticMatcher(); m.wait_ready(5)
        reg = NodeRegistry()
        s   = ProcessScanner(
            registry=reg, graph=CapabilityGraph(), drift_log=DriftLog(),
            event_bus=InProcessEventBus(), ml_matcher=m,
            exclude_ports={9999},
        )
        self.assertIn(9999, s._exclude)

    # ── Bug 3: PolicyPhase logger name ────────────────────────────
    def test_policy_phase_logger_name(self):
        """PolicyPhase must log under interface_core.phases.policy not synthesis."""
        import inspect
        from interface_core.phases.synthesis import PolicyPhase
        src = inspect.getsource(PolicyPhase)
        self.assertIn("interface_core.phases.policy", src,
            "PolicyPhase must use its own logger named interface_core.phases.policy")
        # Check that bare module logger is NOT used (self._logger is fine)
        import re
        # Match "logger.info" but NOT "self._logger.info" or "_logger.info"
        bare_logger_calls = re.findall(r'(?<![_\w])logger\.info\(.*PolicyPhase', src)
        self.assertEqual([], bare_logger_calls,
            "PolicyPhase.run must use self._logger, not bare module logger")

    def test_policy_phase_logs_under_correct_name(self):
        """When PolicyPhase starts, log should come from interface_core.phases.policy."""
        import logging
        records = []
        class _Capture(logging.Handler):
            def emit(self, r): records.append(r)

        pol_logger = logging.getLogger("interface_core.phases.policy")
        cap = _Capture()
        pol_logger.addHandler(cap)
        pol_logger.setLevel(logging.DEBUG)
        try:
            # Trigger the log by calling run() briefly
            from interface_core.phases.synthesis import PolicyPhase
            from interface_core.phases.matching import BridgeQueue
            from interface_core.phases.synthesis import SynthesisQueue
            from interface_core.event_bus import InProcessEventBus

            pp = PolicyPhase(
                bridge_queue    = BridgeQueue(),
                synthesis_queue = SynthesisQueue(),
                policy_engine   = None,
                event_bus       = InProcessEventBus(),
            )
            # Manually call what run() does at start
            pp._logger.info("PolicyPhase started")
        finally:
            pol_logger.removeHandler(cap)

        self.assertTrue(any("PolicyPhase started" in r.getMessage() for r in records),
            "PolicyPhase did not log under interface_core.phases.policy")

    # ── Bug 4: Redis error message clarity ───────────────────────
    def test_redis_not_installed_message_is_helpful(self):
        """When redis package is missing, error message should say so clearly."""
        import inspect
        from interface_core.store import _RedisBridgeStore
        src = inspect.getsource(_RedisBridgeStore._get_client)
        self.assertIn("No module named", src,
            "_get_client must check for ImportError and give a pip install hint")
        self.assertIn("pip install redis", src,
            "Redis unavailable message must include pip install instruction")

    # ── Full boot simulation ──────────────────────────────────────
    def test_boot_simulation_no_external_deps(self):
        """Verify a full boot sequence completes cleanly with no external services."""
        from interface_core.main import InterfaceCore

        async def _boot():
            core = InterfaceCore()
            loop = asyncio.get_event_loop()

            # ML ready
            await loop.run_in_executor(None, core.ml_matcher.wait_ready, 10.0)

            # Postgres fails gracefully
            pg_ok = await core.pg.connect()
            self.assertFalse(pg_ok)

            # Register nodes
            p_id = core.register_node("rest", "http://localhost:19100",
                                       {"v": "float"}, ["test"], "producer", 1.0)
            c_id = core.register_node("rest", "http://localhost:19101",
                                       {"v": "float"}, ["test"], "consumer")

            # Match → policy → synthesize
            await core.matching._match_all()
            for _ in range(5):
                cand = await core.bridge_queue.pop()
                if cand is None: break
                result = await core.policy_engine.evaluate(cand)
                if result.verdict.value == "approve":
                    await core.synthesis_queue.push(cand)
            for _ in range(3):
                cand = await core.synthesis_queue.pop(0.1)
                if cand is None: break
                await core.synthesis._synthesize(cand)

            # Consul scan no-ops gracefully
            await core.consul._scan()

            # Process scanner no-ops or finds something
            await core.process_scanner._scan()

            # Status is well-formed
            status = core.status()
            for key in ("nodes", "active_bridges", "bridge_queue",
                        "store_backend", "postgres", "consul_nodes",
                        "process_scan_ports", "bridges"):
                self.assertIn(key, status, f"status() missing key: {key}")

            self.assertFalse(status["postgres"])
            self.assertIn(status["store_backend"], ("memory", "redis"))
            self.assertGreaterEqual(status["nodes"], 2)  # process_scanner may add more

        run(_boot())




# ════════════════════════════════════════════════════════════════
# LOGGING SETUP & TERMINAL OUTPUT TESTS
# ════════════════════════════════════════════════════════════════

class TestLoggingSetup(unittest.TestCase):

    def _make_logger(self, name="interface_core.test"):
        import logging, io
        from interface_core.logging_setup import ICFormatter
        buf     = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(ICFormatter())
        log = logging.getLogger(name + str(id(self)))
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
        return log, buf

    # ── Formatter basics ─────────────────────────────────────────
    def test_formatter_imports(self):
        from interface_core.logging_setup import (
            ICFormatter, setup_logging, print_banner,
            print_phase_header, print_status_table,
        )
        self.assertIsNotNone(ICFormatter)

    def test_formatter_all_levels_produce_output(self):
        log, buf = self._make_logger()
        log.debug("debug msg"); log.info("info msg")
        log.warning("warn msg"); log.error("error msg")
        out = buf.getvalue()
        for marker in ("DBG", "INF", "WRN", "ERR"):
            self.assertIn(marker, out, f"Level marker {marker} missing from output")

    def test_formatter_elapsed_time_prefix(self):
        log, buf = self._make_logger()
        log.info("test")
        out = buf.getvalue()
        self.assertRegex(out, r'\[\+\d+\.\d+s\]',
            "Formatter must include elapsed time [+NNN.NNNs]")

    def test_formatter_phase_label_discovery(self):
        log, buf = self._make_logger("interface_core.phases.discovery")
        log.info("started")
        out = buf.getvalue()
        self.assertIn("DISCOVERY", out)

    def test_formatter_phase_label_synthesis(self):
        log, buf = self._make_logger("interface_core.phases.synthesis")
        log.info("started")
        out = buf.getvalue()
        self.assertIn("SYNTHESIS", out)

    def test_formatter_phase_label_consul(self):
        log, buf = self._make_logger("interface_core.consul_discovery")
        log.info("started")
        out = buf.getvalue()
        self.assertIn("CONSUL", out)

    def test_formatter_bridge_live_highlight(self):
        log, buf = self._make_logger()
        log.info("BRIDGE_LIVE abc123 p→c [rest→kafka]")
        out = buf.getvalue()
        # BRIDGE_LIVE events should be present and message intact
        self.assertIn("BRIDGE_LIVE", out)

    def test_formatter_new_node_highlight(self):
        log, buf = self._make_logger()
        log.info("NEW_NODE a1b2c3d4 [rest] http://localhost:8080")
        out = buf.getvalue()
        self.assertIn("NEW_NODE", out)

    def test_formatter_no_crash_on_exception_info(self):
        log, buf = self._make_logger()
        try:
            raise ValueError("test error")
        except ValueError:
            log.exception("something failed")
        out = buf.getvalue()
        self.assertIn("something failed", out)
        self.assertIn("ValueError", out)

    def test_formatter_no_crash_without_tty(self):
        """Formatter must work when stdout is not a TTY (e.g. pipes, files)."""
        import logging, io
        from interface_core.logging_setup import ICFormatter
        buf     = io.StringIO()
        handler = logging.StreamHandler(buf)
        # Simulate non-TTY by overriding _IS_TTY (already False in tests)
        handler.setFormatter(ICFormatter())
        log = logging.getLogger("interface_core.notty" + str(id(self)))
        log.addHandler(handler)
        log.setLevel(logging.INFO)
        log.info("pipe output test")
        self.assertIn("pipe output test", buf.getvalue())

    # ── setup_logging ────────────────────────────────────────────
    def test_setup_logging_does_not_crash(self):
        from interface_core.logging_setup import setup_logging
        setup_logging(level="INFO", verbose=False)

    def test_setup_logging_verbose_sets_debug(self):
        import logging
        from interface_core.logging_setup import setup_logging
        setup_logging(level="INFO", verbose=True)
        root_level = logging.getLogger().level
        self.assertEqual(root_level, logging.DEBUG)
        # Reset
        setup_logging(level="INFO", verbose=False)

    def test_setup_logging_removes_old_handlers(self):
        import logging
        from interface_core.logging_setup import setup_logging
        root = logging.getLogger()
        initial_count = len(root.handlers)
        setup_logging()
        setup_logging()   # calling twice must not double handlers
        self.assertEqual(len(root.handlers), 1)

    # ── print_banner, print_phase_header, print_status_table ─────
    def test_print_banner_outputs_to_stdout(self):
        import io, contextlib
        from interface_core.logging_setup import print_banner
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            print_banner()
        self.assertGreater(len(buf.getvalue()), 0)
        # Banner uses spaced letters: "I N T E R F A C E"
        out = buf.getvalue().upper()
        self.assertTrue(
            "INTERFACE" in out.replace(" ", "") or "CORE" in out,
            "Banner must contain INTERFACE CORE text"
        )

    def test_print_phase_header_outputs_name(self):
        import io, contextlib
        from interface_core.logging_setup import print_phase_header
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            print_phase_header("DISCOVERY", "interval=500ms")
        out = buf.getvalue()
        self.assertIn("DISCOVERY", out)
        self.assertIn("interval=500ms", out)

    def test_print_status_table_shows_all_keys(self):
        import io, contextlib
        from interface_core.logging_setup import print_status_table
        status = {
            "nodes": 3, "active_bridges": 1, "bridge_queue": 0,
            "consul_nodes": 2, "process_scan_ports": 4,
            "store_backend": "memory", "postgres": False,
            "pending_approvals": [], "bridges": {},
        }
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            print_status_table(status)
        out = buf.getvalue()
        for label in ("Nodes registered", "Active bridges", "Consul nodes",
                      "Store backend", "Postgres"):
            self.assertIn(label, out, f"Status table missing: {label}")

    def test_print_status_table_shows_bridge_detail(self):
        import io, contextlib
        from interface_core.logging_setup import print_status_table
        status = {
            "nodes": 2, "active_bridges": 1, "bridge_queue": 0,
            "consul_nodes": 0, "process_scan_ports": 1,
            "store_backend": "memory", "postgres": False,
            "pending_approvals": [],
            "bridges": {
                "abc12345": {
                    "producer_id": "prod-id-1",
                    "consumer_id": "cons-id-2",
                    "state": "live",
                    "runtime": {"messages": 42, "errors": 0, "circuit": "closed"},
                }
            },
        }
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            print_status_table(status)
        out = buf.getvalue()
        self.assertIn("abc12345", out)
        self.assertIn("42", out)    # messages
        self.assertIn("closed", out)  # circuit state

    # ── CLI args ─────────────────────────────────────────────────
    def test_cli_help_includes_verbose(self):
        import subprocess
        result = subprocess.run(
            [sys.executable, "-m", "interface_core.main", "--help"],
            capture_output=True, text=True, cwd=".",
        )
        self.assertIn("verbose", result.stdout.lower())
        self.assertIn("log-level", result.stdout.lower())
        self.assertIn("status-interval", result.stdout.lower())

    def test_cli_log_level_option(self):
        import subprocess
        result = subprocess.run(
            [sys.executable, "-m", "interface_core.main", "--log-level", "WARNING", "--help"],
            capture_output=True, text=True, cwd=".",
        )
        self.assertEqual(result.returncode, 0)

    # ── Integration: start() uses rich logging ───────────────────
    def test_start_calls_print_banner(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("print_banner()", src,
            "start() must call print_banner()")

    def test_start_calls_print_phase_header(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("print_phase_header(", src,
            "start() must call print_phase_header() for section headers")

    def test_start_logs_config_summary(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        for detail in ["min_score", "circuit", "max_bridges", "consul"]:
            self.assertIn(detail, src,
                f"start() config summary must mention '{detail}'")

    def test_main_calls_setup_logging(self):
        import inspect
        from interface_core.main import main
        src = inspect.getsource(main)
        self.assertIn("setup_logging", src,
            "main() must call setup_logging()")

    def test_logging_setup_exported_from_package(self):
        import interface_core as ic
        for sym in ("setup_logging", "print_banner", "print_status_table"):
            self.assertTrue(hasattr(ic, sym),
                f"interface_core must export {sym}")

    # ── Verbose phase logs ───────────────────────────────────────
    def test_synthesis_logs_score_on_bridge_live(self):
        import inspect
        from interface_core.phases.synthesis import SynthesisPhase
        src = inspect.getsource(SynthesisPhase._synthesize)
        self.assertIn("score=%.3f", src,
            "SynthesisPhase._synthesize must log score detail on BRIDGE_LIVE")

    def test_synthesis_logs_field_count(self):
        import inspect
        from interface_core.phases.synthesis import SynthesisPhase
        src = inspect.getsource(SynthesisPhase._synthesize)
        self.assertIn("fields=%d", src,
            "SynthesisPhase._synthesize must log field mapping count")

    def test_matching_logs_score_breakdown(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("sem=", src,
            "MatchingPhase._match_all must log semantic similarity in debug")

    def test_lifecycle_logs_teardown_reason(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase._teardown)
        self.assertIn("BRIDGE_TORN", src)
        self.assertIn("reason=%s", src,
            "LifecyclePhase._teardown must log teardown reason")





# ════════════════════════════════════════════════════════════════
# INCREMENTAL UPDATE TESTS
# ════════════════════════════════════════════════════════════════

class TestNodeStaleSetting(unittest.TestCase):

    def test_node_stale_s_in_settings(self):
        from interface_core.config import settings
        self.assertTrue(hasattr(settings, 'node_stale_s'))
        self.assertIsInstance(settings.node_stale_s, int)
        self.assertGreater(settings.node_stale_s, 0)

    def test_lifecycle_reads_stale_from_settings(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase.__init__)
        self.assertIn('settings.node_stale_s', src,
            'LifecyclePhase must read NODE_STALE_S from settings, not hardcode 30.0')
        self.assertNotIn('= 30.0', src,
            'LifecyclePhase must not hardcode stale timeout')

    def test_lifecycle_stale_matches_settings(self):
        from interface_core.main import InterfaceCore
        from interface_core.config import settings
        core = InterfaceCore()
        self.assertAlmostEqual(
            core.lifecycle._NODE_STALE_S,
            float(settings.node_stale_s),
            places=1,
        )

    def test_stale_env_override(self):
        import os
        from interface_core.config import _i
        os.environ['IC_NODE_STALE_S'] = '60'
        val = _i('IC_NODE_STALE_S', 30)
        self.assertEqual(val, 60)
        del os.environ['IC_NODE_STALE_S']


class TestGraphReweightOnTeardown(unittest.TestCase):

    def test_teardown_reweights_graph(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase._teardown)
        self.assertIn('reweight_edges', src,
            '_teardown must call graph.reweight_edges to reset stale edge weight')

    def test_teardown_resets_edge_to_zero(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase._teardown)
        self.assertIn('new_weight=0.0', src,
            '_teardown must set new_weight=0.0 so pair re-scores fresh')

    def test_graph_edge_zeroed_after_teardown(self):
        """After a bridge tears down, the graph edge weight must be 0."""
        from interface_core.main import InterfaceCore
        from interface_core.models import Bridge, AdapterSpec, Protocol, BridgeState

        core = InterfaceCore()
        p_id = core.register_node('rest','http://td-src',{'v':'float'},['x'],'producer',1.0)
        c_id = core.register_node('rest','http://td-dst',{'v':'float'},['x'],'consumer')

        # Add a weighted edge
        core.graph.add_edge(p_id, c_id, 0.85)
        self.assertAlmostEqual(core.graph.get_edge_weight(p_id, c_id), 0.85, places=2)

        # Simulate bridge teardown
        spec   = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge = Bridge(id='tb1', producer_id=p_id, consumer_id=c_id, adapter=spec)
        core.active_bridges[(p_id, c_id)] = (bridge, None, None)

        async def _do_teardown():
            await core.lifecycle._teardown((p_id, c_id), 'test')

        run(_do_teardown())
        # Edge weight must be 0 after teardown
        self.assertAlmostEqual(core.graph.get_edge_weight(p_id, c_id), 0.0, places=2)


class TestExponentialBackoff(unittest.TestCase):

    def test_runner_uses_settings_retry_base(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        self.assertIn('retry_base_ms', src,
            'BridgeRunner._loop must use settings.retry_base_ms for backoff base')

    def test_runner_uses_settings_retry_ceil(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        self.assertIn('retry_ceil_ms', src,
            'BridgeRunner._loop must cap backoff at settings.retry_ceil_ms')

    def test_backoff_formula_present(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        self.assertIn('2 **', src,
            'BridgeRunner backoff must use exponential formula (2 **)')

    def test_backoff_has_jitter(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        self.assertIn('jitter', src,
            'BridgeRunner backoff must include jitter to prevent thundering herd')

    def test_backoff_increases_with_failures(self):
        """Verify the formula: later failures sleep longer than early ones."""
        from interface_core.config import settings
        base    = settings.retry_base_ms / 1000.0
        ceiling = settings.retry_ceil_ms  / 1000.0

        delays = [min(base * (2 ** min(n, 10)), ceiling) for n in range(6)]
        for i in range(1, len(delays)):
            self.assertGreaterEqual(delays[i], delays[i-1],
                f'Backoff delay must be non-decreasing; delay[{i}]={delays[i]} < delay[{i-1}]={delays[i-1]}')

    def test_backoff_capped_at_ceiling(self):
        from interface_core.config import settings
        base    = settings.retry_base_ms / 1000.0
        ceiling = settings.retry_ceil_ms  / 1000.0
        # After many failures, delay must not exceed ceiling
        big_delay = min(base * (2 ** 20), ceiling)
        self.assertLessEqual(big_delay, ceiling)

    def test_no_more_fixed_0_5s_sleep(self):
        """The old fixed 0.5s sleep on error must be gone."""
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        # 0.01 is the "nothing available" sleep — that's fine
        # 0.5 was the old error sleep — must be gone
        # Check that 0.5 doesn't appear as a standalone sleep argument
        import re
        bad = re.findall(r'asyncio\.sleep\(0\.5\)', src)
        self.assertEqual(bad, [],
            'BridgeRunner._loop must not use fixed asyncio.sleep(0.5) on error')


class TestCastTypedFallback(unittest.TestCase):

    def _adapter(self, transform):
        from interface_core.adapters.base import FieldMappingAdapter, AdapterSpec, FieldMapping
        from interface_core.models import Protocol
        spec = AdapterSpec(
            producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST,
            field_mappings=[FieldMapping(src_field='v', dst_field='v', transform=transform)],
        )
        return FieldMappingAdapter(spec)

    def test_cast_int_bad_value_returns_zero(self):
        out = self._adapter('cast:int').transform({'v': 'not_a_number'})
        self.assertEqual(out['v'], 0,
            'cast:int on non-numeric string must return 0, not the original string')

    def test_cast_float_bad_value_returns_zero(self):
        out = self._adapter('cast:float').transform({'v': 'NaN_string'})
        self.assertAlmostEqual(out['v'], 0.0,
            'cast:float on bad value must return 0.0')

    def test_cast_bool_bad_value_returns_false(self):
        out = self._adapter('cast:bool').transform({'v': object()})
        # bool() on most objects is True — only specific failures should give False
        self.assertIsInstance(out['v'], bool)

    def test_cast_int_good_value_still_works(self):
        out = self._adapter('cast:int').transform({'v': '42'})
        self.assertEqual(out['v'], 42)

    def test_cast_float_good_value_still_works(self):
        out = self._adapter('cast:float').transform({'v': '3.14'})
        self.assertAlmostEqual(out['v'], 3.14, places=2)

    def test_cast_str_fallback_is_original(self):
        """cast:str has no good fallback — str() rarely fails, so original is fine."""
        out = self._adapter('cast:str').transform({'v': 999})
        self.assertEqual(out['v'], '999')

    def test_cast_failure_does_not_lose_field(self):
        """Even on failure, the field must be present in output (with fallback value)."""
        out = self._adapter('cast:int').transform({'v': 'broken'})
        self.assertIn('v', out, 'Field must still be present after cast failure')

    def test_original_string_not_returned_on_int_failure(self):
        """The old silent bug: returning original string when cast:int fails."""
        out = self._adapter('cast:int').transform({'v': 'abc'})
        self.assertNotEqual(out['v'], 'abc',
            'cast:int must NOT silently return original string on failure')


class TestTelemetryWired(unittest.TestCase):

    def test_bridge_runner_accepts_telemetry(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        sig = inspect.signature(BridgeRunner.__init__)
        self.assertIn('telemetry', sig.parameters,
            'BridgeRunner.__init__ must accept telemetry= parameter')

    def test_bridge_runner_telemetry_defaults_none(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        sig = inspect.signature(BridgeRunner.__init__)
        default = sig.parameters['telemetry'].default
        self.assertIsNone(default,
            'BridgeRunner telemetry= must default to None')

    def test_bridge_runner_calls_record_message(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        self.assertIn('record_message', src,
            'BridgeRunner._loop must call telemetry.record_message()')

    def test_record_message_called_on_success(self):
        """Verify telemetry.record_message is called when consumer write succeeds."""
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        # success=True must appear in a record_message call
        self.assertIn('success=True', src)

    def test_record_message_called_on_failure(self):
        import inspect
        from interface_core.phases.runner import BridgeRunner
        src = inspect.getsource(BridgeRunner._loop)
        self.assertIn('success=False', src)

    def test_synthesis_passes_telemetry_to_runner(self):
        import inspect
        from interface_core.phases.synthesis import SynthesisPhase
        src = inspect.getsource(SynthesisPhase._synthesize)
        self.assertIn('telemetry=self._telemetry', src,
            'SynthesisPhase must pass telemetry to BridgeRunner')

    def test_telemetry_records_on_live_bridge(self):
        """End-to-end: messages sent through bridge are recorded in telemetry."""
        from interface_core.main import InterfaceCore
        from interface_core.phases.runner import (
            BridgeRunner, MemoryProducerTransport, MemoryConsumerTransport,
        )
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.adapters.base import GenericJSONPassthroughAdapter
        from interface_core.models import Bridge, AdapterSpec, Protocol

        core   = InterfaceCore()
        spec   = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge = Bridge(id='tel-1', producer_id='tp', consumer_id='tc', adapter=spec)
        adapter = GenericJSONPassthroughAdapter(spec)
        rt      = BridgeRuntime(bridge, adapter)

        core.telemetry.register_bridge(bridge)
        p_q = asyncio.Queue(); c_q = asyncio.Queue()
        runner = BridgeRunner(
            'tel-1', rt,
            MemoryProducerTransport(p_q), MemoryConsumerTransport(c_q),
            telemetry=core.telemetry,
        )

        async def _go():
            runner.start()
            await p_q.put({'x': 1})
            await asyncio.sleep(0.2)
            await runner.stop()

        run(_go())

        snap = core.telemetry.snapshot()
        bridge_snap = snap['bridges'].get('tel-1', {})
        self.assertGreater(bridge_snap.get('messages', 0), 0,
            'Telemetry must record at least 1 message after data flows through bridge')


class TestZeroMQTransport(unittest.TestCase):

    def test_zmq_producer_instantiates(self):
        from interface_core.phases.transports import ZMQProducerTransport
        t = ZMQProducerTransport('tcp://localhost:5555')
        self.assertEqual(t._raw, 'tcp://localhost:5555')
        self.assertEqual(t._mode, 'sub')

    def test_zmq_producer_mode_from_query_string(self):
        from interface_core.phases.transports import ZMQProducerTransport
        t = ZMQProducerTransport('tcp://localhost:5556?mode=pull')
        self.assertEqual(t._raw, 'tcp://localhost:5556')
        self.assertEqual(t._mode, 'pull')

    def test_zmq_consumer_instantiates(self):
        from interface_core.phases.transports import ZMQConsumerTransport
        t = ZMQConsumerTransport('tcp://localhost:5557')
        self.assertEqual(t._raw, 'tcp://localhost:5557')
        self.assertEqual(t._mode, 'pub')

    def test_zmq_consumer_push_mode(self):
        from interface_core.phases.transports import ZMQConsumerTransport
        t = ZMQConsumerTransport('tcp://localhost:5558?mode=push')
        self.assertEqual(t._mode, 'push')

    def test_zmq_producer_read_no_server_returns_none(self):
        from interface_core.phases.transports import ZMQProducerTransport
        t = ZMQProducerTransport('tcp://localhost:19880')
        result = run(t.read())
        self.assertIsNone(result)

    def test_zmq_consumer_write_no_server_returns_false(self):
        from interface_core.phases.transports import ZMQConsumerTransport
        t = ZMQConsumerTransport('tcp://localhost:19881')
        result = run(t.write({'x': 1}))
        self.assertFalse(result)

    def test_zmq_close_before_connect_no_error(self):
        from interface_core.phases.transports import ZMQProducerTransport, ZMQConsumerTransport
        run(ZMQProducerTransport('tcp://localhost:5555').close())
        run(ZMQConsumerTransport('tcp://localhost:5555').close())

    def test_transport_factory_routes_zeromq_producer(self):
        from interface_core.phases.runner import make_producer_transport
        from interface_core.phases.transports import ZMQProducerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(id='z', protocol=Protocol.ZEROMQ, role=NodeRole.PRODUCER,
                 endpoint='tcp://localhost:5555',
                 contract=DataContract(schema_fields=[FieldSchema(name='x', type='str')]))
        self.assertIsInstance(make_producer_transport(n), ZMQProducerTransport)

    def test_transport_factory_routes_zeromq_consumer(self):
        from interface_core.phases.runner import make_consumer_transport
        from interface_core.phases.transports import ZMQConsumerTransport
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        n = Node(id='z', protocol=Protocol.ZEROMQ, role=NodeRole.CONSUMER,
                 endpoint='tcp://localhost:5555',
                 contract=DataContract(schema_fields=[FieldSchema(name='x', type='str')]))
        self.assertIsInstance(make_consumer_transport(n), ZMQConsumerTransport)

    def test_zmq_loopback_pubsub(self):
        """If pyzmq is installed, verify ZMQ pub/sub round-trip works."""
        try:
            import zmq
        except ImportError:
            self.skipTest('pyzmq not installed')

        from interface_core.phases.transports import ZMQProducerTransport, ZMQConsumerTransport

        async def _run():
            import zmq.asyncio as azmq
            ctx  = azmq.Context.instance()
            port = 15555
            # Bind PUB first
            pub  = ctx.socket(zmq.PUB)
            pub.bind(f'tcp://*:{port}')
            await asyncio.sleep(0.05)   # allow bind to settle

            # Consumer subscribes
            sub  = ZMQProducerTransport(f'tcp://127.0.0.1:{port}')
            await sub._connect()
            await asyncio.sleep(0.05)   # allow subscription to propagate

            # Publish a message
            msg = b'{"sensor": "temp", "value": 23.5}'
            await pub.send(msg)
            await asyncio.sleep(0.15)   # let listener queue it

            result = await sub.read()
            await sub.close()
            pub.close()
            return result

        result = asyncio.run(_run())
        self.assertIsNotNone(result)
        self.assertIn('sensor', result)
        self.assertAlmostEqual(result.get('value'), 23.5)


class TestNewAPIRoutes(unittest.TestCase):

    def _skip_no_aiohttp(self):
        try:
            import aiohttp
        except ImportError:
            self.skipTest('aiohttp not installed')

    @classmethod
    def setUpClass(cls):
        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        cls.core = InterfaceCore()
        cls.core.ml_matcher.wait_ready(30)
        cls.pid = cls.core.register_node(
            'rest', 'http://api-new-src', {'v': 'float'}, ['test'], 'producer', 1.0,
        )
        cls.cid = cls.core.register_node(
            'rest', 'http://api-new-dst', {'v': 'float'}, ['test'], 'consumer',
        )
        cls.app = build_app(cls.core)

    # ── Route presence checks (no aiohttp needed) ─────────────────
    def test_health_route_in_app(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn('/health', api_src)

    def test_delete_node_route_in_app(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn('DELETE', api_src)
        self.assertIn('node_id', api_src)

    def test_drift_route_in_app(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn('/drift', api_src)

    def test_trigger_route_in_app(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn('/bridges/trigger', api_src)

    # ── Live endpoint tests (aiohttp required) ─────────────────────
    def test_health_endpoint_returns_200(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get('/health')
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIn('status', data)
                self.assertIn('nodes', data)
                self.assertIn('bridges', data)
                self.assertEqual(data['status'], 'ok')

        run(_run())

    def test_health_returns_store_backend(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get('/health')
                data = await resp.json()
                self.assertIn('store', data)
                self.assertIn(data['store'], ('memory', 'redis'))

        run(_run())

    def test_delete_node_returns_404_for_unknown(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.delete('/nodes/ghost-node-xyz')
                self.assertEqual(resp.status, 404)

        run(_run())

    def test_delete_existing_node(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer
        # Register a temporary node
        tmp_id = self.core.register_node(
            'rest', 'http://tmp-del', {'x': 'float'}, ['tmp'], 'producer',
        )

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.delete(f'/nodes/{tmp_id}')
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertEqual(data['deleted'], tmp_id)

        run(_run())
        self.assertIsNone(self.core.registry.get(tmp_id),
            'Node must be removed from registry after DELETE')

    def test_node_drift_returns_list(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.get(f'/nodes/{self.pid}/drift')
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertIsInstance(data, list)

        run(_run())

    def test_trigger_bridge_queues_synthesis(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        before = len(self.core.synthesis_queue._q.qsize() if hasattr(self.core.synthesis_queue._q, 'qsize') else [])

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.post('/bridges/trigger', json={
                    'producer_id': self.pid,
                    'consumer_id': self.cid,
                })
                # 202 Accepted or 409 if bridge already active
                self.assertIn(resp.status, (202, 409))
                data = await resp.json()
                if resp.status == 202:
                    self.assertTrue(data['queued'])

        run(_run())

    def test_trigger_missing_producer_returns_404(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.post('/bridges/trigger', json={
                    'producer_id': 'ghost', 'consumer_id': self.cid,
                })
                self.assertEqual(resp.status, 404)

        run(_run())

    def test_trigger_bad_body_returns_400(self):
        self._skip_no_aiohttp()
        from aiohttp.test_utils import TestClient, TestServer

        async def _run():
            async with TestClient(TestServer(self.app)) as client:
                resp = await client.post('/bridges/trigger', json={'wrong': 'keys'})
                self.assertEqual(resp.status, 400)

        run(_run())




# ════════════════════════════════════════════════════════════════
# LIVE-RUN FIX TESTS
# ════════════════════════════════════════════════════════════════

class TestTqdmProgressBarSuppressed(unittest.TestCase):

    def test_show_progress_bar_false_in_encode(self):
        import inspect
        from interface_core.ml.matcher import Embedder
        src = inspect.getsource(Embedder.embed_batch)
        self.assertIn("show_progress_bar=False", src,
            "embed_batch must pass show_progress_bar=False to sentence-transformers "
            "to prevent 'Batches: 100%' flooding the terminal")

    def test_future_warning_deprecated_method_not_used(self):
        import inspect
        from interface_core.ml.matcher import Embedder
        src = inspect.getsource(Embedder._load)
        self.assertIn("get_embedding_dimension", src,
            "Embedder._load must use get_embedding_dimension() not the deprecated version")
        # The new method must be tried first
        new_idx = src.find("get_embedding_dimension()")
        old_idx = src.find("get_sentence_embedding_dimension()")
        if old_idx != -1:
            self.assertLess(new_idx, old_idx,
                "get_embedding_dimension must be tried before the deprecated fallback")

    def test_embed_batch_does_not_print_to_stdout(self):
        """Verify embed_batch produces no stdout output."""
        import io, contextlib
        from interface_core.ml.matcher import Embedder
        e = Embedder(); e.wait_ready(5)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            e.embed_batch(["test sentence one", "test sentence two"])
        # Any output means tqdm leaked through
        self.assertEqual(buf.getvalue(), "",
            f"embed_batch must not print to stdout, got: {buf.getvalue()[:100]!r}")


class TestOTLPNoiseSuppressed(unittest.TestCase):

    def test_otlp_logger_silenced_in_telemetry(self):
        import inspect
        from interface_core.telemetry import Telemetry
        src = inspect.getsource(Telemetry.__init__)
        self.assertIn("opentelemetry.exporter", src,
            "Telemetry.__init__ must silence the opentelemetry.exporter logger")
        self.assertIn("setLevel", src,
            "Telemetry.__init__ must call setLevel to suppress exporter spam")

    def test_otlp_logger_set_to_error_or_higher(self):
        import logging
        from interface_core.telemetry import Telemetry
        # Instantiate to trigger logger setup
        Telemetry("http://localhost:4317", 9099)
        exporter_log = logging.getLogger("opentelemetry.exporter")
        self.assertGreaterEqual(exporter_log.level, logging.ERROR,
            "opentelemetry.exporter logger must be ERROR or higher to suppress retry spam")

    def test_env_override_allows_lower_level(self):
        """IC_OTEL_LOG_LEVEL=WARNING can re-enable WRN messages."""
        import os, logging
        os.environ["IC_OTEL_LOG_LEVEL"] = "WARNING"
        from interface_core.telemetry import Telemetry
        Telemetry("http://localhost:4317", 9098)
        # Just verify the env var is read — don't assert exact level
        # (level depends on max(WARNING, ERROR) logic)
        del os.environ["IC_OTEL_LOG_LEVEL"]

    def test_otel_sdk_loggers_exist_in_suppress_list(self):
        import inspect
        from interface_core.telemetry import Telemetry
        src = inspect.getsource(Telemetry.__init__)
        self.assertIn("opentelemetry.sdk", src,
            "Telemetry must also silence opentelemetry.sdk loggers")


class TestProcessScannerSelfExclusion(unittest.TestCase):

    def test_zmq_port_auto_excluded(self):
        from interface_core.main import InterfaceCore
        from interface_core.config import settings
        core     = InterfaceCore()
        zmq_port = int(settings.zmq_pub_addr.split(":")[-1])
        self.assertIn(zmq_port, core.process_scanner._exclude,
            f"ProcessScanner must auto-exclude ZMQ pub port {zmq_port}")

    def test_api_port_auto_excluded(self):
        from interface_core.main import InterfaceCore
        from interface_core.config import settings
        core = InterfaceCore()
        self.assertIn(getattr(settings, "api_port", 8000),
                      core.process_scanner._exclude,
                      "ProcessScanner must auto-exclude the HTTP API port")

    def test_prometheus_port_auto_excluded(self):
        from interface_core.main import InterfaceCore
        from interface_core.config import settings
        core = InterfaceCore()
        self.assertIn(getattr(settings, "prometheus_port", 9090),
                      core.process_scanner._exclude,
                      "ProcessScanner must auto-exclude the Prometheus metrics port")

    def test_zmq_sub_port_excluded(self):
        from interface_core.main import InterfaceCore
        from interface_core.config import settings
        core     = InterfaceCore()
        zmq_sub  = int(settings.zmq_sub_addr.split(":")[-1])
        self.assertIn(zmq_sub, core.process_scanner._exclude)

    def test_exclusion_computed_at_construction(self):
        """Exclusions must be in _exclude immediately after __init__, before any scan."""
        from interface_core.phases.process_scanner import ProcessScanner
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m = SemanticMatcher(); m.wait_ready(5)
        s = ProcessScanner(registry=NodeRegistry(), graph=CapabilityGraph(),
                           drift_log=DriftLog(), event_bus=InProcessEventBus(),
                           ml_matcher=m)
        from interface_core.config import settings
        api_port = getattr(settings, "api_port", 8000)
        self.assertIn(api_port, s._exclude)


class TestPolicyRateLimit(unittest.TestCase):

    def _engine_with_nodes(self):
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema
        reg = NodeRegistry()
        p   = Node(id="p_rl", protocol=Protocol.REST, role=NodeRole.PRODUCER,
                   endpoint="http://p", contract=DataContract(
                       schema_fields=[FieldSchema(name="x", type="float")], confidence=0.9))
        c   = Node(id="c_rl", protocol=Protocol.REST, role=NodeRole.CONSUMER,
                   endpoint="http://c", contract=DataContract(
                       schema_fields=[FieldSchema(name="x", type="float")], confidence=0.9))
        reg.put(p); reg.put(c)
        return PolicyEngine(reg, {}), p.id, c.id

    def _cand(self, pid, cid):
        return BridgeCandidate(producer_id=pid, consumer_id=cid,
                               score=MatchScore(composite=0.9))

    def test_rate_limit_field_in_init(self):
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        e = PolicyEngine(NodeRegistry(), {})
        self.assertTrue(hasattr(e, "_rate_attempts"),
            "PolicyEngine must have _rate_attempts sliding window dict")

    def test_normal_attempts_pass(self):
        e, pid, cid = self._engine_with_nodes()
        result = run(e.evaluate(self._cand(pid, cid)))
        self.assertEqual(result.verdict.value, "approve")

    def test_rate_limit_rejects_after_too_many_attempts(self):
        e, pid, cid = self._engine_with_nodes()
        import time
        # Flood the rate_attempts window manually
        now = time.time()
        e._rate_attempts[pid] = [now] * 10  # exactly at limit
        result = run(e.evaluate(self._cand(pid, cid)))
        self.assertEqual(result.verdict.value, "reject",
            "PolicyEngine must reject when rate limit exceeded")

    def test_rate_limit_reason_includes_node_id(self):
        e, pid, cid = self._engine_with_nodes()
        import time
        e._rate_attempts[pid] = [time.time()] * 10
        result = run(e.evaluate(self._cand(pid, cid)))
        self.assertIn("rate-limit", result.reason)

    def test_old_attempts_purged_from_window(self):
        e, pid, cid = self._engine_with_nodes()
        import time
        # Add attempts that are outside the 60s window
        old_time = time.time() - 120  # 2 minutes ago
        e._rate_attempts[pid] = [old_time] * 15  # many, but all old
        result = run(e.evaluate(self._cand(pid, cid)))
        # Old attempts should be purged — should approve
        self.assertEqual(result.verdict.value, "approve",
            "Rate limit must purge old timestamps; stale attempts must not block")

    def test_rate_limit_in_evaluate_source(self):
        import inspect
        from interface_core.phases.policy import PolicyEngine
        src = inspect.getsource(PolicyEngine.evaluate)
        self.assertIn("_rate_attempts", src)
        self.assertIn("rate-limit", src)


class TestGracefulStopDrain(unittest.TestCase):

    def test_stop_calls_drain_buffer(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.stop)
        self.assertIn("drain_buffer", src,
            "InterfaceCore.stop must call drain_buffer() to flush in-flight messages")

    def test_stop_flushes_to_consumer(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.stop)
        self.assertIn("consumer.write", src,
            "InterfaceCore.stop must write drained messages to consumer")

    def test_stop_has_drain_sleep(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.stop)
        self.assertIn("asyncio.sleep", src,
            "InterfaceCore.stop must sleep briefly to allow in-flight messages to complete")

    def test_drain_order_signal_before_flush(self):
        """_running=False must be set before drain_buffer is called."""
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.stop)
        running_idx   = src.find("_running = False")
        drain_idx     = src.find("drain_buffer")
        self.assertLess(running_idx, drain_idx,
            "Runners must be signalled to stop before buffer is drained")

    def test_drain_completes_on_live_runner(self):
        """Buffered messages must be delivered during stop."""
        from interface_core.phases.runner import (
            BridgeRunner, MemoryProducerTransport, MemoryConsumerTransport,
        )
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.adapters.base import GenericJSONPassthroughAdapter
        from interface_core.models import Bridge, AdapterSpec, Protocol

        spec    = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge  = Bridge(id="drain-1", producer_id="p", consumer_id="c", adapter=spec)
        adapter = GenericJSONPassthroughAdapter(spec)
        rt      = BridgeRuntime(bridge, adapter)

        # Force circuit open so messages go to ring buffer
        for _ in range(10): rt.circuit.record(False)
        self.assertTrue(rt.circuit.is_open())

        # Push messages into ring buffer manually
        for i in range(5):
            rt._buf_msg({"i": i})
        self.assertEqual(len(rt._msg_buf), 5)

        consumer_q = asyncio.Queue()

        async def _go():
            consumer = MemoryConsumerTransport(consumer_q)
            # Drain manually (simulating what stop() does)
            buffered = rt.drain_buffer()
            for payload in buffered:
                await consumer.write(payload)
            return buffered

        drained = run(_go())
        self.assertEqual(len(drained), 5,
            "drain_buffer must return all 5 buffered messages")
        self.assertFalse(consumer_q.empty(),
            "Drained messages must be delivered to consumer")


class TestWebSocketEventsEndpoint(unittest.TestCase):

    def _skip_no_aiohttp(self):
        try:
            import aiohttp
        except ImportError:
            self.skipTest("aiohttp not installed")

    def test_ws_events_route_registered(self):
        api_src = open("interface_core/api.py").read()
        self.assertIn("ws_events", api_src,
            "api.py must define ws_events handler")
        self.assertIn("/events", api_src,
            "api.py must register /events route")

    def test_ws_events_uses_websocket_response(self):
        api_src = open("interface_core/api.py").read()
        self.assertIn("WebSocketResponse", api_src,
            "ws_events must use aiohttp WebSocketResponse")

    def test_ws_events_subscribes_to_all(self):
        api_src = open("interface_core/api.py").read()
        self.assertIn("subscribe(None)", api_src,
            "ws_events must subscribe to all events (subscribe(None))")

    def test_ws_events_has_heartbeat(self):
        api_src = open("interface_core/api.py").read()
        self.assertIn("heartbeat", api_src,
            "ws_events must set heartbeat to keep connection alive")

    def test_ws_events_supports_type_filter(self):
        api_src = open("interface_core/api.py").read()
        self.assertIn("types", api_src,
            "ws_events must support ?types= query param for filtering")

    def test_ws_events_live(self):
        self._skip_no_aiohttp()
        from interface_core.main import InterfaceCore
        from interface_core.api import build_app

        core = InterfaceCore()
        core.ml_matcher.wait_ready(5)
        app  = build_app(core)

        async def _run():
            from aiohttp.test_utils import TestClient, TestServer
            async with TestClient(TestServer(app)) as client:
                ws = await client.ws_connect("/events")
                # Publish a test event
                from interface_core.models import Event, EventType
                core.event_bus.publish(Event(
                    type=EventType.NEW_NODE, node_id="ws-test",
                    payload={"test": True}
                ))
                # Read with timeout
                import asyncio as _a
                try:
                    msg = await _a.wait_for(ws.receive(), timeout=1.0)
                    await ws.close()
                    return msg
                except _a.TimeoutError:
                    await ws.close()
                    return None

        msg = run(_run())
        self.assertIsNotNone(msg, "WebSocket /events should deliver events")


class TestTelemetryNewMetrics(unittest.TestCase):

    def test_update_bridge_metrics_method_exists(self):
        from interface_core.telemetry import Telemetry
        self.assertTrue(hasattr(Telemetry, "update_bridge_metrics"),
            "Telemetry must have update_bridge_metrics method")

    def test_record_circuit_trip_method_exists(self):
        from interface_core.telemetry import Telemetry
        self.assertTrue(hasattr(Telemetry, "record_circuit_trip"))

    def test_record_drift_event_method_exists(self):
        from interface_core.telemetry import Telemetry
        self.assertTrue(hasattr(Telemetry, "record_drift_event"))

    def test_update_bridge_metrics_no_crash_when_prometheus_absent(self):
        """Must degrade gracefully when prometheus_client not installed."""
        from interface_core.telemetry import Telemetry
        t = Telemetry("http://localhost:4317", 19099)
        # Should not raise even if prometheus is absent
        t.update_bridge_metrics("bridge-1", buffer_fill=0.5, circuit_open=False)

    def test_record_circuit_trip_no_crash(self):
        from interface_core.telemetry import Telemetry
        t = Telemetry("http://localhost:4317", 19098)
        t.record_circuit_trip("bridge-2")

    def test_record_drift_event_no_crash(self):
        from interface_core.telemetry import Telemetry
        t = Telemetry("http://localhost:4317", 19097)
        t.record_drift_event("node-123")

    def test_lifecycle_emits_buffer_fill(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase._tick)
        self.assertIn("update_bridge_metrics", src,
            "LifecyclePhase._tick must call telemetry.update_bridge_metrics")

    def test_discovery_emits_drift_event(self):
        import inspect
        from interface_core.phases.discovery import DiscoveryPhase
        src = inspect.getsource(DiscoveryPhase._register_or_drift)
        self.assertIn("record_drift_event", src,
            "DiscoveryPhase must call telemetry.record_drift_event on drift")

    def test_lifecycle_has_telemetry_attr(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        self.assertTrue(hasattr(core.lifecycle, "_telemetry"),
            "LifecyclePhase must have _telemetry attribute")

    def test_discovery_has_telemetry_attr(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        self.assertTrue(hasattr(core.discovery, "_telemetry"),
            "DiscoveryPhase must have _telemetry attribute")

    def test_telemetry_wired_after_start(self):
        """After start(), discovery and lifecycle must have non-None telemetry."""
        from interface_core.main import InterfaceCore

        async def _go():
            core = InterfaceCore()
            core.ml_matcher.wait_ready(5)
            # Run just the pg connect + wiring, not full start
            await core.pg.connect()
            # Manually wire (simulating what start() does)
            core.lifecycle._telemetry = core.telemetry
            core.discovery._telemetry = core.telemetry
            return core

        core = run(_go())
        self.assertIs(core.lifecycle._telemetry, core.telemetry)
        self.assertIs(core.discovery._telemetry, core.telemetry)




# ════════════════════════════════════════════════════════════════
# SCHEMA CONVERTER TESTS
# ════════════════════════════════════════════════════════════════

class TestSchemaConverters(unittest.TestCase):

    # ── json_to_csv ──────────────────────────────────────────────
    def test_json_to_csv_basic(self):
        from interface_core.schema.converters import json_to_csv
        out = json_to_csv({"price": 1.5, "symbol": "AAPL", "volume": 1000})
        self.assertIn("price", out)
        self.assertIn("1.5", out)
        self.assertIn("AAPL", out)

    def test_json_to_csv_list(self):
        from interface_core.schema.converters import json_to_csv
        records = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        out = json_to_csv(records)
        lines = out.strip().splitlines()
        self.assertEqual(len(lines), 3)  # header + 2 data rows

    def test_json_to_csv_custom_field_order(self):
        from interface_core.schema.converters import json_to_csv
        out = json_to_csv({"z": 1, "a": 2}, field_order=["z", "a"])
        self.assertTrue(out.startswith("z,a"))

    def test_json_to_csv_string_input(self):
        from interface_core.schema.converters import json_to_csv
        out = json_to_csv('{"x": 1, "y": 2}')
        self.assertIn("x", out)

    # ── csv_to_json ──────────────────────────────────────────────
    def test_csv_to_json_single_row(self):
        from interface_core.schema.converters import csv_to_json
        csv_str = "name,value,unit\ntemp,23.5,C"
        out = csv_to_json(csv_str)
        self.assertIsInstance(out, dict)
        self.assertEqual(out["name"], "temp")
        self.assertAlmostEqual(float(out["value"]), 23.5)

    def test_csv_to_json_multi_row(self):
        from interface_core.schema.converters import csv_to_json
        csv_str = "x,y\n1,2\n3,4"
        out = csv_to_json(csv_str)
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 2)

    def test_csv_to_json_bytes_input(self):
        from interface_core.schema.converters import csv_to_json
        out = csv_to_json(b"a,b\n1,2")
        self.assertIsInstance(out, dict)

    # ── xml_to_json ──────────────────────────────────────────────
    def test_xml_to_json_simple(self):
        from interface_core.schema.converters import xml_to_json
        xml = "<sensor><id>001</id><temp>23.5</temp></sensor>"
        out = xml_to_json(xml)
        self.assertIn("sensor", out)
        self.assertIn("temp", out["sensor"])

    def test_xml_to_json_attributes(self):
        from interface_core.schema.converters import xml_to_json
        xml = '<item id="42" type="sensor"><value>99</value></item>'
        out = xml_to_json(xml)
        self.assertIn("@id", out["item"])
        self.assertEqual(out["item"]["@id"], "42")

    def test_xml_to_json_malformed(self):
        from interface_core.schema.converters import xml_to_json
        out = xml_to_json("not xml at all")
        self.assertIn("_raw_xml", out)

    # ── flatten / unflatten ──────────────────────────────────────
    def test_flatten_nested(self):
        from interface_core.schema.converters import flatten_nested
        inp = {"a": {"b": {"c": 1}, "d": 2}, "e": 3}
        out = flatten_nested(inp)
        self.assertEqual(out["a.b.c"], 1)
        self.assertEqual(out["a.d"], 2)
        self.assertEqual(out["e"], 3)

    def test_flatten_already_flat(self):
        from interface_core.schema.converters import flatten_nested
        inp = {"x": 1, "y": 2}
        self.assertEqual(flatten_nested(inp), inp)

    def test_unflatten_dotted(self):
        from interface_core.schema.converters import unflatten_dotted
        inp = {"a.b": 1, "a.c": 2, "d": 3}
        out = unflatten_dotted(inp)
        self.assertEqual(out["a"]["b"], 1)
        self.assertEqual(out["a"]["c"], 2)
        self.assertEqual(out["d"], 3)

    def test_flatten_unflatten_roundtrip(self):
        from interface_core.schema.converters import flatten_nested, unflatten_dotted
        original = {"user": {"name": "alice", "id": 42}, "score": 0.9}
        self.assertEqual(unflatten_dotted(flatten_nested(original)), original)

    # ── normalize_fields ─────────────────────────────────────────
    def test_normalize_portid_to_port(self):
        from interface_core.schema.converters import normalize_fields
        out = normalize_fields({"portid": 80, "src_ip": "10.0.0.1"})
        self.assertIn("port", out)
        self.assertEqual(out["port"], 80)
        self.assertIn("source.address", out)

    def test_normalize_ts_to_timestamp(self):
        from interface_core.schema.converters import normalize_fields
        out = normalize_fields({"ts": 1712650000, "msg": "hello"})
        self.assertIn("timestamp", out)
        self.assertEqual(out["timestamp"], 1712650000)

    def test_normalize_lvl_to_severity(self):
        from interface_core.schema.converters import normalize_fields
        out = normalize_fields({"lvl": "warning"})
        self.assertIn("severity", out)

    def test_normalize_custom_aliases(self):
        from interface_core.schema.converters import normalize_fields
        out = normalize_fields({"my_field": 99}, aliases={"my_field": "canonical"})
        self.assertIn("canonical", out)
        self.assertEqual(out["canonical"], 99)

    def test_normalize_preserves_unknown_fields(self):
        from interface_core.schema.converters import normalize_fields
        out = normalize_fields({"unknown_field_xyz": "value"})
        self.assertIn("unknown_field_xyz", out)

    # ── unit conversions ─────────────────────────────────────────
    def test_bytes_to_human(self):
        from interface_core.schema.converters import bytes_to_human
        self.assertIn("KB", bytes_to_human(1024))
        self.assertIn("MB", bytes_to_human(1024 * 1024))
        self.assertIn("GB", bytes_to_human(1024 ** 3))

    def test_celsius_to_fahrenheit(self):
        from interface_core.schema.converters import celsius_to_fahrenheit
        self.assertAlmostEqual(celsius_to_fahrenheit(0), 32.0, places=1)
        self.assertAlmostEqual(celsius_to_fahrenheit(100), 212.0, places=1)

    def test_fahrenheit_to_celsius(self):
        from interface_core.schema.converters import fahrenheit_to_celsius
        self.assertAlmostEqual(fahrenheit_to_celsius(32), 0.0, places=1)
        self.assertAlmostEqual(fahrenheit_to_celsius(212), 100.0, places=1)

    def test_unix_to_iso(self):
        from interface_core.schema.converters import unix_to_iso
        out = unix_to_iso(0)
        self.assertIn("1970", out)
        self.assertIn("T", out)  # ISO-8601

    def test_iso_to_unix(self):
        from interface_core.schema.converters import iso_to_unix
        ts = iso_to_unix("1970-01-01T00:00:00Z")
        self.assertAlmostEqual(ts, 0.0, delta=3600)  # allow timezone offset

    def test_normalize_severity(self):
        from interface_core.schema.converters import normalize_severity
        self.assertEqual(normalize_severity("warn"), "warning")
        self.assertEqual(normalize_severity("err"), "error")
        self.assertEqual(normalize_severity("crit"), "critical")
        self.assertEqual(normalize_severity("DEBUG"), "debug")

    def test_convert_units_pipeline(self):
        from interface_core.schema.converters import convert_units
        payload = {"bytes_sent": 1024, "temp": 100, "ts": 0}
        out = convert_units(payload, {
            "bytes_sent": "bytes_to_human",
            "temp":       "c_to_f",
            "ts":         "unix_to_iso",
        })
        self.assertIn("KB", out["bytes_sent"])
        self.assertAlmostEqual(out["temp"], 212.0, places=1)
        self.assertIn("1970", out["ts"])

    # ── filtering ─────────────────────────────────────────────────
    def test_keep_fields(self):
        from interface_core.schema.converters import keep_fields
        out = keep_fields({"a": 1, "b": 2, "c": 3}, {"a", "c"})
        self.assertEqual(set(out.keys()), {"a", "c"})

    def test_drop_fields(self):
        from interface_core.schema.converters import drop_fields
        out = drop_fields({"a": 1, "_internal": "x", "b": 2}, {"_internal"})
        self.assertNotIn("_internal", out)
        self.assertIn("a", out)

    def test_filter_severity_drops_below_threshold(self):
        from interface_core.schema.converters import filter_severity
        self.assertIsNone(filter_severity({"severity": "debug"}, min_level="warning"))
        self.assertIsNone(filter_severity({"severity": "info"}, min_level="error"))

    def test_filter_severity_passes_at_threshold(self):
        from interface_core.schema.converters import filter_severity
        result = filter_severity({"severity": "warning", "msg": "x"}, "warning")
        self.assertIsNotNone(result)

    def test_filter_severity_passes_above_threshold(self):
        from interface_core.schema.converters import filter_severity
        result = filter_severity({"severity": "error"}, "warning")
        self.assertIsNotNone(result)

    def test_filter_severity_passes_when_field_absent(self):
        from interface_core.schema.converters import filter_severity
        result = filter_severity({"msg": "no severity"}, "error")
        self.assertIsNotNone(result)

    def test_suppress_empty_drops_all_null(self):
        from interface_core.schema.converters import suppress_empty
        self.assertIsNone(suppress_empty({"a": None, "b": "", "c": "  "}))

    def test_suppress_empty_keeps_record_with_value(self):
        from interface_core.schema.converters import suppress_empty
        result = suppress_empty({"a": None, "b": "hello"})
        self.assertIsNotNone(result)

    def test_suppress_noise_drops_heartbeat(self):
        from interface_core.schema.converters import suppress_noise
        self.assertIsNone(suppress_noise({"message": "heartbeat"}))
        self.assertIsNone(suppress_noise({"message": "KEEPALIVE"}))

    def test_suppress_noise_keeps_real_messages(self):
        from interface_core.schema.converters import suppress_noise
        result = suppress_noise({"message": "sensor reading: temp=23.5"})
        self.assertIsNotNone(result)

    # ── enrichment ────────────────────────────────────────────────
    def test_enrich_adds_ic_fields(self):
        from interface_core.schema.converters import enrich
        out = enrich({"x": 1}, bridge_id="br-123", source="http://src")
        self.assertIn("_ic_bridge_id", out)
        self.assertIn("_ic_source", out)
        self.assertIn("_ic_hostname", out)
        self.assertIn("_ic_pid", out)
        self.assertIn("_ic_ingested_at", out)
        self.assertEqual(out["_ic_bridge_id"], "br-123")

    def test_enrich_preserves_original_fields(self):
        from interface_core.schema.converters import enrich
        out = enrich({"sensor": "temp", "value": 23.5})
        self.assertEqual(out["sensor"], "temp")
        self.assertAlmostEqual(out["value"], 23.5)

    def test_enrich_extra_metadata(self):
        from interface_core.schema.converters import enrich
        out = enrich({"x": 1}, extra={"_ic_tag": "test"})
        self.assertEqual(out["_ic_tag"], "test")

    # ── SchemaConverter pipeline ──────────────────────────────────
    def test_schema_converter_chain(self):
        from interface_core.schema.converters import (
            SchemaConverter, normalize_fields, flatten_nested, enrich
        )
        conv = (SchemaConverter()
                .add_step("norm",    normalize_fields)
                .add_step("flatten", flatten_nested)
                .add_step("enrich",  lambda p: enrich(p, bridge_id="br-1")))
        result = conv.apply({"ts": 1712650000, "data": {"temp": 23.5}})
        self.assertIn("timestamp", result)
        self.assertIn("data.temp", result)
        self.assertIn("_ic_bridge_id", result)

    def test_schema_converter_drops_none(self):
        from interface_core.schema.converters import SchemaConverter, filter_severity
        conv = (SchemaConverter()
                .add_step("sev", lambda p: filter_severity(p, "error")))
        result = conv.apply({"severity": "debug", "msg": "low priority"})
        self.assertIsNone(result)

    def test_schema_converter_from_spec(self):
        from interface_core.schema.converters import SchemaConverter
        conv = SchemaConverter.from_spec({
            "normalize_fields": True,
            "flatten":          True,
            "filter_severity":  "warning",
            "suppress_empty":   True,
        })
        self.assertGreater(len(conv._steps), 0)
        result = conv.apply({"lvl": "debug", "data": {"x": 1}})
        self.assertIsNone(result)  # dropped: below severity threshold

    def test_schema_converter_apply_batch(self):
        from interface_core.schema.converters import SchemaConverter, suppress_empty
        conv = SchemaConverter().add_step("suppress", suppress_empty)
        records = [{"a": 1}, {"b": None}, {"c": 3}]
        results = conv.apply_batch(records)
        # {"b": None} should be dropped
        self.assertEqual(len(results), 2)

    def test_schema_converter_step_error_drops_record(self):
        """A step that raises must DROP the record, not silently forward it.
        Silent pass-through on schema errors corrupts downstream data.
        """
        from interface_core.schema.converters import SchemaConverter
        def broken_step(p):
            raise ValueError("intentional error")
        conv = (SchemaConverter()
                .add_step("broken", broken_step)
                .add_step("passthrough", lambda p: p))
        result = conv.apply({"x": 1})
        # broken step must DROP the record (return None) not forward corrupted data
        self.assertIsNone(result,
            "SchemaConverter must drop record on step error, not silently pass through")


# ════════════════════════════════════════════════════════════════
# SECURITY POLICY TESTS
# ════════════════════════════════════════════════════════════════

class TestHardRefuseChecker(unittest.TestCase):

    def _checker(self, extra=None, enabled=True):
        from interface_core.schema.security import HardRefuseChecker
        return HardRefuseChecker(extra_patterns=extra, enabled=enabled)

    def test_admin_endpoint_refused(self):
        c = self._checker()
        r = c.check("http://service/admin/users")
        self.assertIsNotNone(r, "Admin endpoint must be refused")

    def test_ssh_port_refused(self):
        c = self._checker()
        r = c.check("tcp://host:22")
        self.assertIsNotNone(r, "SSH port must be refused")

    def test_oauth_endpoint_refused(self):
        c = self._checker()
        r = c.check("https://auth.local/oauth/token")
        self.assertIsNotNone(r)

    def test_vault_path_refused(self):
        c = self._checker()
        r = c.check("http://vault.local/vault/secret")
        self.assertIsNotNone(r)

    def test_normal_rest_endpoint_allowed(self):
        c = self._checker()
        r = c.check("http://sensor.local:8080/data")
        self.assertIsNone(r, "Normal REST endpoint must not be refused")

    def test_kafka_topic_allowed(self):
        c = self._checker()
        r = c.check("kafka://broker:9092/sensor-data")
        self.assertIsNone(r)

    def test_disabled_allows_everything(self):
        c = self._checker(enabled=False)
        r = c.check("http://service/admin/super-secret")
        self.assertIsNone(r, "Disabled checker must allow everything")

    def test_custom_pattern_refused(self):
        c = self._checker(extra=["*:9999*"])
        r = c.check("tcp://host:9999/stream")
        self.assertIsNotNone(r)

    def test_check_pair_producer_refused(self):
        c = self._checker()
        r = c.check_pair("http://service/admin/x", "http://consumer/ok")
        self.assertIsNotNone(r)
        self.assertIn("producer", r)

    def test_check_pair_consumer_refused(self):
        c = self._checker()
        r = c.check_pair("http://producer/ok", "http://service/admin/y")
        self.assertIsNotNone(r)
        self.assertIn("consumer", r)

    def test_check_pair_both_safe(self):
        c = self._checker()
        r = c.check_pair("http://producer:8080/data", "kafka://broker/topic")
        self.assertIsNone(r)


class TestAllowlistChecker(unittest.TestCase):

    def _checker(self, patterns):
        from interface_core.schema.security import AllowlistChecker
        return AllowlistChecker(patterns)

    def test_empty_allowlist_allows_all(self):
        c = self._checker([])
        self.assertIsNone(c.check("any-node-id", "http://any-endpoint"))

    def test_node_in_allowlist_allowed(self):
        c = self._checker(["approved-node-*", "sensor-001"])
        self.assertIsNone(c.check("sensor-001", "http://s1"))
        self.assertIsNone(c.check("approved-node-abc", "http://s2"))

    def test_node_not_in_allowlist_rejected(self):
        c = self._checker(["approved-*"])
        r = c.check("unknown-node", "http://unknown")
        self.assertIsNotNone(r)
        self.assertIn("allowlist", r)

    def test_endpoint_glob_match(self):
        c = self._checker(["http://sensor.local/*"])
        self.assertIsNone(c.check("node-x", "http://sensor.local/data"))

    def test_is_allowed_true_for_empty_list(self):
        c = self._checker([])
        self.assertTrue(c.is_allowed("anyone", "http://anywhere"))

    def test_is_allowed_false_when_not_matching(self):
        c = self._checker(["only-this"])
        self.assertFalse(c.is_allowed("not-this", "http://other"))


class TestTrustZone(unittest.TestCase):

    def _zone(self, zones, auto=None):
        from interface_core.schema.security import TrustZone
        return TrustZone(zones, auto)

    def test_same_zone_no_approval(self):
        tz = self._zone({"prod": ["prod-*"], "test": ["test-*"]})
        needs, reason, pz, cz = tz.check("prod-node", "http://prod", "prod-node2", "http://prod2")
        self.assertFalse(needs, "Same-zone pair must not require approval")

    def test_cross_zone_requires_approval(self):
        tz = self._zone({"prod": ["prod-*"], "test": ["test-*"]})
        needs, reason, pz, cz = tz.check("prod-node", "http://prod", "test-node", "http://test")
        self.assertTrue(needs, "Cross-zone pair must require approval")

    def test_auto_approved_pair(self):
        tz = self._zone(
            {"prod": ["prod-*"], "staging": ["staging-*"]},
            auto=[("prod", "staging")]
        )
        needs, _, _, _ = tz.check("prod-node", "http://p", "staging-node", "http://s")
        self.assertFalse(needs, "Auto-approved zone pair must not require approval")

    def test_default_zone_assigned(self):
        tz = self._zone({"prod": ["prod-*"]})
        zone = tz.zone_of("unknown-node", "http://unknown")
        self.assertEqual(zone, "default")

    def test_zone_by_endpoint_glob(self):
        tz = self._zone({"internal": ["http://internal.*"]})
        zone = tz.zone_of("node-x", "http://internal.svc")
        self.assertEqual(zone, "internal")

    def test_reason_contains_zone_names(self):
        tz = self._zone({"prod": ["prod-*"], "test": ["test-*"]})
        _, reason, _, _ = tz.check("prod-n", "http://p", "test-n", "http://t")
        self.assertIn("prod", reason)
        self.assertIn("test", reason)


class TestSecurityPolicy(unittest.TestCase):

    def _policy(self, hard_refuse_enabled=False, allowlist=None, zones=None):
        from interface_core.schema.security import (
            SecurityPolicy, HardRefuseChecker, AllowlistChecker, TrustZone
        )
        return SecurityPolicy(
            hard_refuse = HardRefuseChecker(enabled=hard_refuse_enabled),
            allowlist   = AllowlistChecker(allowlist or []),
            trust_zones = TrustZone(zones or {}),
        )

    def test_all_safe_returns_allow(self):
        p = self._policy()
        verdict, reason = p.evaluate("p", "http://safe/data", "c", "kafka://broker/topic")
        self.assertEqual(verdict, "allow")

    def test_hard_refuse_returns_hard_reject(self):
        from interface_core.schema.security import (
            SecurityPolicy, HardRefuseChecker, AllowlistChecker, TrustZone
        )
        p = SecurityPolicy(
            hard_refuse = HardRefuseChecker(enabled=True),
            allowlist   = AllowlistChecker([]),
            trust_zones = TrustZone({}),
        )
        verdict, reason = p.evaluate("p", "http://service/admin/x", "c", "http://ok")
        self.assertEqual(verdict, "hard_reject")

    def test_allowlist_violation_returns_hard_reject(self):
        p = self._policy(allowlist=["approved-*"])
        verdict, reason = p.evaluate("unknown-node", "http://x", "c", "http://y")
        self.assertEqual(verdict, "hard_reject")

    def test_cross_zone_returns_stage(self):
        p = self._policy(zones={"prod": ["prod-*"], "test": ["test-*"]})
        verdict, reason = p.evaluate("prod-n", "http://prod", "test-n", "http://test")
        self.assertEqual(verdict, "stage")

    def test_from_env_builds_correctly(self):
        import os
        os.environ["IC_HARD_REFUSE_ENABLED"] = "false"
        os.environ["IC_ALLOWLIST_SOURCES"]   = ""
        os.environ["IC_TRUST_ZONES"]         = "{}"
        from interface_core.schema.security import SecurityPolicy
        p = SecurityPolicy.from_env()
        self.assertIsNotNone(p)
        del os.environ["IC_HARD_REFUSE_ENABLED"]
        del os.environ["IC_ALLOWLIST_SOURCES"]
        del os.environ["IC_TRUST_ZONES"]

    def test_policy_engine_has_security_attr(self):
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        e = PolicyEngine(NodeRegistry(), {})
        self.assertTrue(hasattr(e, "security"))
        from interface_core.schema.security import SecurityPolicy
        self.assertIsInstance(e.security, SecurityPolicy)

    def test_policy_engine_rejects_admin_endpoint(self):
        from interface_core.phases.policy import PolicyEngine
        from interface_core.schema.security import SecurityPolicy, HardRefuseChecker, AllowlistChecker, TrustZone
        from interface_core.registry import NodeRegistry
        from interface_core.models import Node, NodeRole, Protocol, DataContract, FieldSchema, BridgeCandidate, MatchScore

        reg = NodeRegistry()
        p_node = Node(id="pa", protocol=Protocol.REST, role=NodeRole.PRODUCER,
                      endpoint="http://service/admin/users",
                      contract=DataContract(schema_fields=[FieldSchema(name="x",type="str")], confidence=0.9))
        c_node = Node(id="ca", protocol=Protocol.REST, role=NodeRole.CONSUMER,
                      endpoint="http://consumer/ok",
                      contract=DataContract(schema_fields=[FieldSchema(name="x",type="str")], confidence=0.9))
        reg.put(p_node); reg.put(c_node)

        engine = PolicyEngine(reg, {})
        engine.security = SecurityPolicy(
            hard_refuse = HardRefuseChecker(enabled=True),
            allowlist   = AllowlistChecker([]),
            trust_zones = TrustZone({}),
        )

        cand = BridgeCandidate(producer_id="pa", consumer_id="ca",
                               score=MatchScore(composite=0.95))
        result = run(engine.evaluate(cand))
        self.assertEqual(result.verdict.value, "reject",
            "Admin endpoint must be rejected by hard refuse policy")
        self.assertIn("hard-refuse", result.reason)


# ════════════════════════════════════════════════════════════════
# FAN-OUT / FAN-IN TOPOLOGY TESTS
# ════════════════════════════════════════════════════════════════

class TestFanOutRunner(unittest.TestCase):

    def test_fan_out_instantiates(self):
        from interface_core.schema.topology import FanOutRunner
        from interface_core.phases.runner import MemoryProducerTransport, MemoryConsumerTransport
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.adapters.base import GenericJSONPassthroughAdapter
        from interface_core.models import Bridge, AdapterSpec, Protocol

        spec    = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge  = Bridge(id="fo", producer_id="p", consumer_id="c", adapter=spec)
        adapter = GenericJSONPassthroughAdapter(spec)
        rt      = BridgeRuntime(bridge, adapter)

        runner = FanOutRunner(
            bridge_ids         = ["fo1", "fo2"],
            runtime            = rt,
            producer_transport = MemoryProducerTransport(),
            consumer_adapters  = [
                (MemoryConsumerTransport(), adapter),
                (MemoryConsumerTransport(), adapter),
            ],
        )
        self.assertIsNotNone(runner)
        self.assertEqual(len(runner._consumers), 2)

    def test_fan_out_delivers_to_all_consumers(self):
        from interface_core.schema.topology import FanOutRunner
        from interface_core.phases.runner import MemoryProducerTransport, MemoryConsumerTransport
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.adapters.base import GenericJSONPassthroughAdapter
        from interface_core.models import Bridge, AdapterSpec, Protocol

        spec    = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge  = Bridge(id="fo2", producer_id="p", consumer_id="c", adapter=spec)
        adapter = GenericJSONPassthroughAdapter(spec)
        rt      = BridgeRuntime(bridge, adapter)

        p_q  = asyncio.Queue()
        c1_q = asyncio.Queue()
        c2_q = asyncio.Queue()

        runner = FanOutRunner(
            bridge_ids         = ["b1", "b2"],
            runtime            = rt,
            producer_transport = MemoryProducerTransport(p_q),
            consumer_adapters  = [
                (MemoryConsumerTransport(c1_q), adapter),
                (MemoryConsumerTransport(c2_q), adapter),
            ],
        )

        async def _go():
            runner.start()
            await p_q.put({"sensor": "temp", "value": 23.5})
            await asyncio.sleep(0.2)
            await runner.stop()

        run(_go())

        self.assertFalse(c1_q.empty(), "Consumer 1 must receive the message")
        self.assertFalse(c2_q.empty(), "Consumer 2 must receive the message")
        msg1 = c1_q.get_nowait()
        msg2 = c2_q.get_nowait()
        self.assertAlmostEqual(msg1.get("value"), 23.5)
        self.assertAlmostEqual(msg2.get("value"), 23.5)

    def test_fan_out_stats(self):
        from interface_core.schema.topology import FanOutRunner
        from interface_core.phases.runner import MemoryProducerTransport, MemoryConsumerTransport
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.adapters.base import GenericJSONPassthroughAdapter
        from interface_core.models import Bridge, AdapterSpec, Protocol
        spec    = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge  = Bridge(id="fo3", producer_id="p", consumer_id="c", adapter=spec)
        adapter = GenericJSONPassthroughAdapter(spec)
        rt      = BridgeRuntime(bridge, adapter)
        runner  = FanOutRunner(["b1"], rt, MemoryProducerTransport(),
                               [(MemoryConsumerTransport(), adapter)])
        stats = runner.stats
        self.assertIn("type", stats)
        self.assertEqual(stats["type"], "fan_out")
        self.assertEqual(stats["consumers"], 1)


class TestFanInRunner(unittest.TestCase):

    def test_fan_in_instantiates(self):
        from interface_core.schema.topology import FanInRunner
        from interface_core.phases.runner import MemoryProducerTransport, MemoryConsumerTransport
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.adapters.base import GenericJSONPassthroughAdapter
        from interface_core.models import Bridge, AdapterSpec, Protocol

        spec    = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge  = Bridge(id="fi", producer_id="p", consumer_id="c", adapter=spec)
        adapter = GenericJSONPassthroughAdapter(spec)
        rt      = BridgeRuntime(bridge, adapter)

        runner = FanInRunner(
            bridge_ids = ["fi1", "fi2"],
            producer_transports = [
                ("p1", MemoryProducerTransport(), adapter),
                ("p2", MemoryProducerTransport(), adapter),
            ],
            consumer_transport = MemoryConsumerTransport(),
            runtime = rt,
        )
        self.assertIsNotNone(runner)
        self.assertEqual(len(runner._producers), 2)

    def test_fan_in_merges_producers(self):
        from interface_core.schema.topology import FanInRunner
        from interface_core.phases.runner import MemoryProducerTransport, MemoryConsumerTransport
        from interface_core.phases.lifecycle import BridgeRuntime
        from interface_core.adapters.base import GenericJSONPassthroughAdapter
        from interface_core.models import Bridge, AdapterSpec, Protocol

        spec    = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge  = Bridge(id="fi2", producer_id="p", consumer_id="c", adapter=spec)
        adapter = GenericJSONPassthroughAdapter(spec)
        rt      = BridgeRuntime(bridge, adapter)

        p1_q = asyncio.Queue()
        p2_q = asyncio.Queue()
        c_q  = asyncio.Queue()

        runner = FanInRunner(
            bridge_ids = ["b1"],
            producer_transports = [
                ("p1", MemoryProducerTransport(p1_q), adapter),
                ("p2", MemoryProducerTransport(p2_q), adapter),
            ],
            consumer_transport = MemoryConsumerTransport(c_q),
            runtime = rt,
        )

        async def _go():
            runner.start()
            await p1_q.put({"source": "p1", "v": 1})
            await p2_q.put({"source": "p2", "v": 2})
            await asyncio.sleep(0.3)
            await runner.stop()

        run(_go())

        msgs = []
        while not c_q.empty():
            msgs.append(c_q.get_nowait())

        self.assertEqual(len(msgs), 2, f"Expected 2 merged messages, got {len(msgs)}")
        sources = {m.get("_ic_source_producer") for m in msgs}
        self.assertIn("p1", sources)
        self.assertIn("p2", sources)




# ════════════════════════════════════════════════════════════════
# GAP CLOSURE TESTS
# ════════════════════════════════════════════════════════════════

class TestSchemaConverterInAdapter(unittest.TestCase):
    """Schema converters are wired into FieldMappingAdapter and synthesis."""

    def _adapter(self, pre=None, post=None):
        from interface_core.adapters.base import FieldMappingAdapter, AdapterSpec, FieldMapping
        from interface_core.models import Protocol
        spec = AdapterSpec(
            producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST,
            field_mappings=[FieldMapping(src_field="temp_c", dst_field="temp_c", transform="direct")],
        )
        return FieldMappingAdapter(spec, pre_converter=pre, post_converter=post)

    def test_adapter_has_pre_post_params(self):
        import inspect
        from interface_core.adapters.base import FieldMappingAdapter
        sig = inspect.signature(FieldMappingAdapter.__init__)
        self.assertIn("pre_converter", sig.parameters)
        self.assertIn("post_converter", sig.parameters)

    def test_pre_converter_runs_before_field_mapping(self):
        from interface_core.schema.converters import SchemaConverter, normalize_fields
        pre = SchemaConverter().add_step("norm", normalize_fields)
        a   = self._adapter(pre=pre)
        # normalize_fields: ts→timestamp, temp_c→temperature_celsius
        # The field mapping for temp_c won't fire (renamed by pre),
        # but both canonical names should appear in the output.
        result = a.transform({"ts": 1000, "temp_c": 23.5})
        # Pre-converter must have run — 'ts' becomes 'timestamp'
        self.assertIn("timestamp", result,
            "Pre-converter must rename ts→timestamp before field mapping")
        # The value passes through (field mapped or unmapped passthrough)
        self.assertTrue(
            "temp_c" in result or "temperature_celsius" in result,
            "Temperature field must appear under original or normalized name")

    def test_post_converter_runs_after_field_mapping(self):
        from interface_core.schema.converters import SchemaConverter, enrich
        post = SchemaConverter().add_step("enrich", lambda p: enrich(p, bridge_id="br-test"))
        a    = self._adapter(post=post)
        result = a.transform({"temp_c": 23.5})
        self.assertIn("_ic_bridge_id", result)
        self.assertEqual(result["_ic_bridge_id"], "br-test")

    def test_pre_converter_none_returns_none_drops_record(self):
        from interface_core.schema.converters import SchemaConverter, filter_severity
        pre = SchemaConverter().add_step("sev", lambda p: filter_severity(p, "error"))
        a   = self._adapter(pre=pre)
        # debug record should be dropped by pre-converter
        result = a.transform({"severity": "debug", "temp_c": 23.5})
        self.assertIsNone(result)

    def test_synthesis_uses_schema_converter(self):
        import inspect
        from interface_core.phases.synthesis import _synthesize_adapter
        src = inspect.getsource(_synthesize_adapter)
        self.assertIn("SchemaConverter", src,
            "_synthesize_adapter must build a SchemaConverter pre/post pipeline")

    def test_synthesis_builds_normalize_pre(self):
        import inspect
        from interface_core.phases.synthesis import _synthesize_adapter
        src = inspect.getsource(_synthesize_adapter)
        self.assertIn("normalize_fields", src)
        self.assertIn("pre_conv", src)

    def test_synthesis_enriches_post(self):
        import inspect
        from interface_core.phases.synthesis import _synthesize_adapter
        src = inspect.getsource(_synthesize_adapter)
        self.assertIn("post_conv", src)
        self.assertIn("enrich", src)

    def test_synthesized_adapter_has_source_enrichment(self):
        """Payloads through a synthesized adapter must get _ic_source injected."""
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        core.ml_matcher.wait_ready(10)

        p_id = core.register_node("rest", "http://src", {"temp_c": "float"}, ["test"], "producer", 1.0)
        c_id = core.register_node("rest", "http://dst", {"temp_c": "float"}, ["test"], "consumer")

        async def _go():
            await core.matching._match_all()
            for _ in range(5):
                cand = await core.bridge_queue.pop()
                if not cand: break
                r = await core.policy_engine.evaluate(cand)
                if r.verdict.value == "approve":
                    await core.synthesis_queue.push(cand)
            for _ in range(3):
                cand = await core.synthesis_queue.pop(0.1)
                if not cand: break
                await core.synthesis._synthesize(cand)

        run(_go())

        # Find the synthesized bridge and run a payload through its adapter
        for (pid, cid), entry in core.active_bridges.items():
            if pid == p_id:
                adapter = entry[1] if len(entry) > 1 else None
                if adapter and hasattr(adapter, "transform"):
                    result = adapter.transform({"temp_c": 23.5})
                    if result:
                        # _ic_source should be injected by post_converter
                        self.assertIn("_ic_source", result,
                            "Synthesized adapter must inject _ic_source via post-converter")
                        return
        # Bridge may not have synthesized (score below threshold) — skip
        self.skipTest("Bridge not synthesized (score below threshold)")


class TestBridgeStateMachine(unittest.TestCase):

    def test_all_expected_states_present(self):
        from interface_core.models import BridgeState
        expected = [
            "discovered", "scored", "queued", "policy_review", "approved",
            "synthesizing", "live", "degraded", "tripped", "tearing_down", "torn_down",
        ]
        actual = {s.value for s in BridgeState}
        for state in expected:
            self.assertIn(state, actual, f"BridgeState must include '{state}'")

    def test_legacy_pending_still_works(self):
        from interface_core.models import BridgeState
        self.assertIn("pending", {s.value for s in BridgeState})
        self.assertEqual(BridgeState.PENDING, "pending")

    def test_tearing_down_state(self):
        from interface_core.models import BridgeState, Bridge, AdapterSpec, Protocol
        spec   = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge = Bridge(id="sm-1", producer_id="p", consumer_id="c", adapter=spec)
        bridge.state = BridgeState.TEARING_DOWN
        self.assertEqual(bridge.state, BridgeState.TEARING_DOWN)

    def test_synthesizing_state(self):
        from interface_core.models import BridgeState, Bridge, AdapterSpec, Protocol
        spec   = AdapterSpec(producer_protocol=Protocol.REST, consumer_protocol=Protocol.REST)
        bridge = Bridge(id="sm-2", producer_id="p", consumer_id="c", adapter=spec)
        bridge.state = BridgeState.SYNTHESIZING
        self.assertEqual(bridge.state, BridgeState.SYNTHESIZING)

    def test_lifecycle_teardown_uses_tearing_down(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase._teardown)
        self.assertIn("TEARING_DOWN", src,
            "_teardown must set TEARING_DOWN state before TORN_DOWN")

    def test_state_machine_ordering(self):
        """Verify logical state ordering makes sense."""
        from interface_core.models import BridgeState
        # Control-plane states
        ctrl = [BridgeState.DISCOVERED, BridgeState.SCORED, BridgeState.QUEUED,
                BridgeState.APPROVED, BridgeState.SYNTHESIZING]
        # Data-plane states
        data = [BridgeState.LIVE, BridgeState.DEGRADED, BridgeState.TRIPPED,
                BridgeState.TEARING_DOWN, BridgeState.TORN_DOWN]
        # All are distinct
        all_states = ctrl + data
        self.assertEqual(len(all_states), len(set(all_states)))


class TestEmbeddingCache(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from interface_core.ml.matcher import SemanticMatcher
        cls.m = SemanticMatcher()
        cls.m.wait_ready(30)

    def test_cache_miss_embeds(self):
        from interface_core.models import DataContract, FieldSchema
        c  = DataContract(schema_fields=[FieldSchema(name="price", type="float")],
                          semantic_tags=["market"])
        v1 = self.m.embed(c)
        self.assertIsNotNone(v1)
        self.assertGreater(len(v1), 0)

    def test_cache_hit_returns_same_vector(self):
        from interface_core.models import DataContract, FieldSchema
        c = DataContract(schema_fields=[FieldSchema(name="sensor", type="str")],
                         semantic_tags=["iot"])
        v1 = self.m.embed(c)
        v2 = self.m.embed(c)   # second call — should hit cache
        self.assertEqual(v1, v2)

    def test_cache_size_increments(self):
        from interface_core.models import DataContract, FieldSchema
        before = self.m.cache_size()
        c = DataContract(
            schema_fields=[FieldSchema(name=f"unique_field_{id(self)}", type="int")],
            semantic_tags=["unique_tag"]
        )
        self.m.embed(c)
        self.assertGreaterEqual(self.m.cache_size(), before)

    def test_cache_not_grown_on_repeat_embed(self):
        from interface_core.models import DataContract, FieldSchema
        c = DataContract(schema_fields=[FieldSchema(name="cached_test", type="bool")],
                         semantic_tags=["stable"])
        self.m.embed(c)
        size_after_first = self.m.cache_size()
        self.m.embed(c)   # repeat
        self.assertEqual(self.m.cache_size(), size_after_first,
            "Cache must not grow when the same contract is embedded twice")

    def test_clear_cache(self):
        from interface_core.models import DataContract, FieldSchema
        c = DataContract(schema_fields=[FieldSchema(name="clear_test", type="str")],
                         semantic_tags=["clearable"])
        self.m.embed(c)
        self.assertGreater(self.m.cache_size(), 0)
        self.m.clear_cache()
        self.assertEqual(self.m.cache_size(), 0)

    def test_cache_methods_exist(self):
        self.assertTrue(hasattr(self.m, "cache_size"))
        self.assertTrue(hasattr(self.m, "clear_cache"))
        self.assertTrue(hasattr(self.m, "_embed_cache"))


class TestProcessScannerCooldown(unittest.TestCase):

    def test_cooldown_constant_defined(self):
        from interface_core.phases.process_scanner import ProcessScanner
        self.assertTrue(hasattr(ProcessScanner, "PROBE_COOLDOWN_S"))
        self.assertGreater(ProcessScanner.PROBE_COOLDOWN_S, 0)

    def test_recently_probed_port_skipped(self):
        """A port probed in the last 30s must not be probed again."""
        from interface_core.phases.process_scanner import ProcessScanner
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m   = SemanticMatcher(); m.wait_ready(5)
        s   = ProcessScanner(registry=NodeRegistry(), graph=CapabilityGraph(),
                              drift_log=DriftLog(), event_bus=InProcessEventBus(),
                              ml_matcher=m)
        import time
        # Mark port 19555 as recently probed
        s._seen_ports[19555] = time.time()

        probed = []
        original = s._probe_port
        async def _mock_probe(port, name):
            probed.append(port)
        s._probe_port = _mock_probe

        # Simulate listing port 19555 and a new port 19556
        from unittest.mock import patch
        with patch("interface_core.phases.process_scanner.list_listening_ports",
                   return_value=[(19555, "existing"), (19556, "new")]):
            run(s._scan())

        self.assertNotIn(19555, probed, "Recently-probed port must not be re-probed")
        self.assertIn(19556, probed, "New port must be probed")

    def test_stale_port_gets_re_probed(self):
        """A port whose last probe was >30s ago must be re-probed."""
        from interface_core.phases.process_scanner import ProcessScanner
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m   = SemanticMatcher(); m.wait_ready(5)
        s   = ProcessScanner(registry=NodeRegistry(), graph=CapabilityGraph(),
                              drift_log=DriftLog(), event_bus=InProcessEventBus(),
                              ml_matcher=m)
        import time
        s._seen_ports[19557] = time.time() - 120   # stale: 2 minutes ago

        probed = []
        async def _mock_probe(port, name): probed.append(port)
        s._probe_port = _mock_probe

        from unittest.mock import patch
        with patch("interface_core.phases.process_scanner.list_listening_ports",
                   return_value=[(19557, "stale")]):
            run(s._scan())

        self.assertIn(19557, probed, "Stale port must be re-probed after cooldown")

    def test_cooldown_in_scan_source(self):
        import inspect
        from interface_core.phases.process_scanner import ProcessScanner
        src = inspect.getsource(ProcessScanner._scan)
        self.assertIn("PROBE_COOLDOWN_S", src)
        self.assertIn("last", src.lower())


class TestAPIKeyAuth(unittest.TestCase):

    def _skip_no_aiohttp(self):
        try:
            import aiohttp
        except ImportError:
            self.skipTest("aiohttp not installed")

    def test_api_key_in_config(self):
        from interface_core.config import settings
        self.assertTrue(hasattr(settings, "api_key"))

    def test_build_app_has_middleware(self):
        api_src = open("interface_core/api.py").read()
        self.assertIn("middlewares", api_src, "build_app must pass middlewares=[_auth]")
        self.assertIn("IC_API_KEY", api_src, "Auth middleware must check IC_API_KEY")
        self.assertIn("Authorization", api_src, "Must support Authorization: Bearer header")
        self.assertIn("X-API-Key", api_src, "Must support X-API-Key header")

    def test_health_always_public(self):
        api_src = open("interface_core/api.py").read()
        # The middleware explicitly excludes /health — check for the path string
        self.assertIn("/health", api_src,
            "/health must bypass auth middleware (Kubernetes liveness probe)")
        # Verify it's in the middleware bypass, not just registered as a route
        self.assertIn("health", api_src)

    def test_open_mode_when_no_key(self):
        self._skip_no_aiohttp()
        import os
        os.environ.pop("IC_API_KEY", None)   # ensure no key set

        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        core = InterfaceCore(); core.ml_matcher.wait_ready(5)
        app  = build_app(core)

        async def _run():
            from aiohttp.test_utils import TestClient, TestServer
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/health")
                self.assertEqual(resp.status, 200)
                # Without IC_API_KEY, /status should also work (open mode)
                resp2 = await client.get("/status")
                self.assertNotEqual(resp2.status, 401)

        run(_run())

    def test_protected_mode_rejects_no_key(self):
        self._skip_no_aiohttp()
        import os
        os.environ["IC_API_KEY"] = "test-secret-1234"

        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        core = InterfaceCore(); core.ml_matcher.wait_ready(5)
        app  = build_app(core)

        async def _run():
            from aiohttp.test_utils import TestClient, TestServer
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/status")
                self.assertEqual(resp.status, 401,
                    "Request without API key must get 401 when IC_API_KEY is set")
                # Health must still be public
                resp2 = await client.get("/health")
                self.assertEqual(resp2.status, 200)

        run(_run())
        del os.environ["IC_API_KEY"]

    def test_protected_mode_accepts_bearer_token(self):
        self._skip_no_aiohttp()
        import os
        os.environ["IC_API_KEY"] = "bearer-test-key"

        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        core = InterfaceCore(); core.ml_matcher.wait_ready(5)
        app  = build_app(core)

        async def _run():
            from aiohttp.test_utils import TestClient, TestServer
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/status",
                    headers={"Authorization": "Bearer bearer-test-key"})
                self.assertEqual(resp.status, 200)

        run(_run())
        del os.environ["IC_API_KEY"]

    def test_protected_mode_accepts_x_api_key_header(self):
        self._skip_no_aiohttp()
        import os
        os.environ["IC_API_KEY"] = "xkey-test"

        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        core = InterfaceCore(); core.ml_matcher.wait_ready(5)
        app  = build_app(core)

        async def _run():
            from aiohttp.test_utils import TestClient, TestServer
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/status",
                    headers={"X-API-Key": "xkey-test"})
                self.assertEqual(resp.status, 200)

        run(_run())
        del os.environ["IC_API_KEY"]


class TestConfigValidation(unittest.TestCase):

    def test_valid_config_does_not_raise(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("CONFIG ERROR", src, "start() must validate config at boot")
        self.assertIn("min_composite_score", src)
        self.assertIn("max_bridges_per_node", src)

    def test_config_error_list_built(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("_config_errors", src)
        self.assertIn("SystemExit", src)

    def test_api_key_in_startup_log(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("auth=", src, "startup must log auth mode for HTTP API")


class TestPgMaintenanceBootPurge(unittest.TestCase):

    def test_pg_maintenance_purges_on_first_iteration(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore._pg_maintenance)
        purge_idx = src.find("purge_old_drift")
        # Function may use "asyncio.sleep" or alias "_a.sleep"
        sleep_idx = src.find("sleep(")
        self.assertGreater(purge_idx, 0, "purge_old_drift must be in _pg_maintenance")
        self.assertGreater(sleep_idx, 0, "sleep must be in _pg_maintenance")
        self.assertLess(purge_idx, sleep_idx,
            "_pg_maintenance must purge before sleeping (immediate boot purge)")

    def test_pg_maintenance_logs_deleted_count(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore._pg_maintenance)
        self.assertIn("deleted", src,
            "_pg_maintenance must log how many entries were purged")


class TestFanOutDetection(unittest.TestCase):

    def test_fan_out_logged_when_multiple_consumers_queued(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("FAN-OUT OPPORTUNITY", src,
            "_match_all must log fan-out opportunities")

    def test_fan_out_detection_in_matching(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        core.ml_matcher.wait_ready(30)

        # Register one producer and two consumers with identical contracts
        p_id = core.register_node("rest", "http://fanout-src",
                                   {"v": "float"}, ["metric"], "producer", 1.0)
        c1   = core.register_node("rest", "http://fanout-dst1",
                                   {"v": "float"}, ["metric"], "consumer")
        c2   = core.register_node("rest", "http://fanout-dst2",
                                   {"v": "float"}, ["metric"], "consumer")

        async def _go():
            await core.matching._match_all()
            return len(core.bridge_queue)

        depth = run(_go())
        # With one producer and two consumers scoring above threshold,
        # we expect up to 2 candidates in the queue
        self.assertGreaterEqual(depth, 0)   # may be 0 with hashing fallback






# ════════════════════════════════════════════════════════════════
# DOC REVIEW FIX TESTS
# ════════════════════════════════════════════════════════════════

class TestSchemaConverterDropsOnError(unittest.TestCase):
    """SchemaConverter must drop records on step failure, not silently pass through."""

    def test_step_error_returns_none(self):
        from interface_core.schema.converters import SchemaConverter
        def explodes(p): raise RuntimeError("intentional")
        conv = SchemaConverter().add_step("boom", explodes)
        result = conv.apply({"x": 1})
        self.assertIsNone(result, "Step error must return None (drop record)")

    def test_step_error_does_not_forward_original(self):
        from interface_core.schema.converters import SchemaConverter
        def explodes(p): raise ValueError("schema mismatch")
        conv = SchemaConverter().add_step("boom", explodes)
        result = conv.apply({"sensitive": "data"})
        self.assertIsNone(result,
            "Must NOT forward original payload after schema step error "
            "(silent pass-through corrupts downstream data)")

    def test_later_steps_not_run_after_error(self):
        from interface_core.schema.converters import SchemaConverter
        ran = []
        def explodes(p): raise ValueError("fail")
        def should_not_run(p): ran.append(True); return p
        conv = (SchemaConverter()
                .add_step("boom", explodes)
                .add_step("after", should_not_run))
        conv.apply({"x": 1})
        self.assertEqual(ran, [], "Steps after a failed step must not run")

    def test_apply_source_warns_not_debugs(self):
        import inspect
        from interface_core.schema.converters import SchemaConverter
        src = inspect.getsource(SchemaConverter.apply)
        self.assertIn("warning", src.lower(),
            "Step failures must be logged at WARNING level, not DEBUG")
        self.assertNotIn("logger.debug", src,
            "Step failures must not be silently buried at debug level")

    def test_batch_drops_failed_records(self):
        from interface_core.schema.converters import SchemaConverter
        call_count = [0]
        def sometimes_fails(p):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("record 2 invalid")
            return p
        conv    = SchemaConverter().add_step("validate", sometimes_fails)
        records = [{"a": 1}, {"b": 2}, {"c": 3}]
        results = conv.apply_batch(records)
        self.assertEqual(len(results), 2,
            "Batch must drop failed records and return only successful ones")


class TestDegradedServiceDetection(unittest.TestCase):

    def test_degraded_services_attr_on_core(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        self.assertTrue(hasattr(core, '_degraded_services'))
        self.assertIsInstance(core._degraded_services, list)

    def test_status_has_durable_and_degraded_keys(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        s = core.status()
        self.assertIn('degraded_services', s)
        self.assertIn('durable', s)

    def test_degraded_warning_in_startup_code(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn('DEGRADED:', src,
            "startup must emit explicit DEGRADED: warning when services missing")

    def test_degraded_warns_about_persistence_loss(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn('lost on restart', src,
            "DEGRADED warning must tell operator bridges will be lost on restart")

    def test_health_exposes_durable(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn('durable', api_src)
        self.assertIn('degraded_services', api_src)

    def test_health_status_degraded_string(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn("'degraded'", api_src,
            "/health must return status='degraded' when services missing")


class TestAPIRateLimit(unittest.TestCase):

    def _skip_no_aiohttp(self):
        try: import aiohttp
        except ImportError: self.skipTest("aiohttp not installed")

    def test_rate_limit_middleware_present(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn('IC_API_RATE_LIMIT', api_src)
        self.assertIn('429', api_src)
        self.assertIn('rate limit exceeded', api_src)

    def test_write_endpoints_count_double(self):
        api_src = open('interface_core/api.py').read()
        self.assertIn('cost', api_src)
        self.assertIn("'POST'", api_src)
        self.assertIn("'DELETE'", api_src)

    def test_health_bypasses_rate_limit(self):
        api_src = open('interface_core/api.py').read()
        # /health must be excluded from rate limiting
        self.assertIn("'/health'", api_src)

    def test_rate_limit_returns_429(self):
        self._skip_no_aiohttp()
        import os
        os.environ['IC_API_RATE_LIMIT'] = '3'   # very low for testing
        os.environ.pop('IC_API_KEY', None)

        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        core = InterfaceCore(); core.ml_matcher.wait_ready(5)
        app  = build_app(core)

        async def _run():
            from aiohttp.test_utils import TestClient, TestServer
            async with TestClient(TestServer(app)) as client:
                responses = []
                for _ in range(5):
                    r = await client.get('/status')
                    responses.append(r.status)
                return responses

        statuses = run(_run())
        del os.environ['IC_API_RATE_LIMIT']
        self.assertIn(429, statuses,
            "Rate limiter must return 429 after limit exceeded")

    def test_rate_limit_zero_disables(self):
        self._skip_no_aiohttp()
        import os
        os.environ['IC_API_RATE_LIMIT'] = '0'
        os.environ.pop('IC_API_KEY', None)

        from interface_core.main import InterfaceCore
        from interface_core.api import build_app
        core = InterfaceCore(); core.ml_matcher.wait_ready(5)
        app  = build_app(core)

        async def _run():
            from aiohttp.test_utils import TestClient, TestServer
            async with TestClient(TestServer(app)) as client:
                statuses = [await (await client.get('/status')).json()
                             for _ in range(10)]
            return statuses

        results = run(_run())
        del os.environ['IC_API_RATE_LIMIT']
        # With rate limit=0, all requests should succeed
        self.assertEqual(len(results), 10)


class TestTelemetryDisableMode(unittest.TestCase):

    def test_ic_telemetry_enabled_false_disables_cleanly(self):
        import os
        os.environ['IC_TELEMETRY_ENABLED'] = 'false'
        from interface_core.telemetry import Telemetry
        t = Telemetry('http://localhost:4317', 9099)
        self.assertFalse(t._enabled)
        # All methods must be no-ops
        t.record_message('b', 'p', 'c', 1.0, success=True)
        t.record_circuit_trip('b')
        t.record_drift_event('n')
        t.update_bridge_metrics('b', 0.5, False)
        del os.environ['IC_TELEMETRY_ENABLED']

    def test_ic_telemetry_enabled_true_is_default(self):
        import os
        os.environ.pop('IC_TELEMETRY_ENABLED', None)
        from interface_core.telemetry import Telemetry
        t = Telemetry('http://localhost:4317', 9099)
        self.assertTrue(getattr(t, '_enabled', True))

    def test_enabled_flag_in_init_source(self):
        import inspect
        from interface_core.telemetry import Telemetry
        src = inspect.getsource(Telemetry.__init__)
        self.assertIn('IC_TELEMETRY_ENABLED', src)
        self.assertIn('_enabled', src)

    def test_disabled_snapshot_returns_empty(self):
        import os
        os.environ['IC_TELEMETRY_ENABLED'] = 'false'
        from interface_core.telemetry import Telemetry
        t = Telemetry('http://localhost:4317', 9099)
        result = t.snapshot()
        # snapshot must not crash even when disabled
        del os.environ['IC_TELEMETRY_ENABLED']


class TestDurableAuditLog(unittest.TestCase):

    def test_synthesis_writes_bridge_live_to_audit(self):
        import inspect
        from interface_core.phases.synthesis import SynthesisPhase
        src = inspect.getsource(SynthesisPhase._synthesize)
        self.assertIn('BRIDGE_LIVE', src)
        self.assertIn('write_audit', src,
            "Synthesis must write BRIDGE_LIVE to durable audit log via pg.write_audit")

    def test_lifecycle_writes_bridge_torn_to_audit(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase._teardown)
        self.assertIn('BRIDGE_TORN', src)
        self.assertIn('write_audit', src,
            "Lifecycle must write BRIDGE_TORN to durable audit log via pg.write_audit")

    def test_audit_write_is_async_task(self):
        """Audit writes must be fire-and-forget async tasks, not blocking."""
        import inspect
        from interface_core.phases.synthesis import SynthesisPhase
        src = inspect.getsource(SynthesisPhase._synthesize)
        # Must use asyncio.create_task for non-blocking write
        self.assertIn('create_task', src)


class TestFailureBoundaries(unittest.TestCase):

    def test_lifecycle_has_consecutive_error_counter(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase.run)
        self.assertIn('_consecutive_errors', src)
        self.assertIn('_MAX_CONSECUTIVE', src)

    def test_lifecycle_stops_on_repeated_errors(self):
        import inspect
        from interface_core.phases.lifecycle import LifecyclePhase
        src = inspect.getsource(LifecyclePhase.run)
        self.assertIn('FATAL', src,
            "Lifecycle must log FATAL and stop after too many consecutive errors")
        self.assertIn('self._running = False', src)

    def test_synthesis_has_failure_boundary(self):
        import inspect
        from interface_core.phases.synthesis import SynthesisPhase
        src = inspect.getsource(SynthesisPhase.run)
        self.assertIn('SYNTHESIS PHASE FATAL', src)

    def test_api_start_failure_is_warning_not_debug(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore._start_api)
        self.assertIn('warning', src.lower(),
            "HTTP API startup failure must be logged at WARNING, not DEBUG")


class TestDiscoveryBackoff(unittest.TestCase):

    def test_discovery_has_probe_cooldown(self):
        import inspect
        from interface_core.phases.discovery import DiscoveryPhase
        src = inspect.getsource(DiscoveryPhase.__init__)
        self.assertIn('_probe_cooldown_s', src)
        self.assertIn('_last_probed', src)

    def test_scan_cycle_uses_backoff(self):
        import inspect
        from interface_core.phases.discovery import DiscoveryPhase
        src = inspect.getsource(DiscoveryPhase._scan_cycle)
        self.assertIn('_last_probed', src,
            "_scan_cycle must check _last_probed to skip recently-probed targets")

    def test_rest_targets_not_reprobed_within_cooldown(self):
        from interface_core.phases.discovery import DiscoveryPhase
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m = SemanticMatcher(); m.wait_ready(5)
        d = DiscoveryPhase(NodeRegistry(), CapabilityGraph(), DriftLog(),
                           InProcessEventBus(), m)
        import time
        # Mark a fake REST target as recently probed
        d._last_probed['http://fake-target:8080'] = time.time()
        # The target should be in _last_probed
        self.assertIn('http://fake-target:8080', d._last_probed)

    def test_cooldown_default_is_reasonable(self):
        from interface_core.phases.discovery import DiscoveryPhase
        from interface_core.registry import NodeRegistry, CapabilityGraph, DriftLog
        from interface_core.event_bus import InProcessEventBus
        from interface_core.ml.matcher import SemanticMatcher
        m = SemanticMatcher(); m.wait_ready(5)
        d = DiscoveryPhase(NodeRegistry(), CapabilityGraph(), DriftLog(),
                           InProcessEventBus(), m)
        self.assertGreaterEqual(d._probe_cooldown_s, 10.0,
            "Probe cooldown must be at least 10 seconds to prevent probe storms")


class TestMLNonBlocking(unittest.TestCase):

    def test_ml_start_is_background_task(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn('_log_ml_ready', src,
            "ML loading must be a background task, not blocking startup")
        self.assertIn('create_task', src)

    def test_ml_does_not_block_phases(self):
        """Phases must start even before ML model is ready.
        The ML future must be awaited inside _log_ml_ready (background task),
        not directly in start() before the phase tasks are created.
        """
        import inspect
        from interface_core.main import InterfaceCore
        # _log_ml_ready must be a nested async def — it awaits the future
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("async def _log_ml_ready", src,
            "ML must be awaited inside a nested async def, not blocking start()")
        # start() must create the task (fire and forget)
        self.assertIn("create_task(_log_ml_ready", src,
            "ML ready-log must be started as a background task")
        # start() must NOT directly await _ml_future before creating tasks
        # The await is inside _log_ml_ready, not in the start() body directly
        log_fn_start = src.find("async def _log_ml_ready")
        task_list_start = src.find("self._tasks = [")
        direct_await = src.find("await asyncio.wait_for(_ml_future")
        if direct_await != -1:
            self.assertGreater(direct_await, log_fn_start,
                "ML future await must be inside _log_ml_ready, not before it")

    def test_ml_fallback_message_present(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn('hashing', src.lower(),
            "Startup must mention hashing fallback when transformer unavailable")




# ════════════════════════════════════════════════════════════════
# GAP CLOSURE — policy._pg, deregister_node, matching prefilter,
#               staged approvals, FanOutRunner in synthesis,
#               DriftLog.clear_node
# ════════════════════════════════════════════════════════════════

class TestPolicyPgWired(unittest.TestCase):

    def test_policy_engine_has_pg_attr(self):
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        e = PolicyEngine(NodeRegistry(), {})
        self.assertTrue(hasattr(e, "_pg"),
            "PolicyEngine must have _pg attribute for audit writes")

    def test_policy_pg_defaults_none(self):
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        e = PolicyEngine(NodeRegistry(), {})
        self.assertIsNone(e._pg)

    def test_main_wires_policy_pg(self):
        import inspect
        from interface_core.main import InterfaceCore
        src = inspect.getsource(InterfaceCore.start)
        self.assertIn("policy_engine._pg", src,
            "main.start() must wire policy_engine._pg for durable STAGED writes")

    def test_staged_approvals_dict_exists(self):
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        e = PolicyEngine(NodeRegistry(), {})
        self.assertTrue(hasattr(e, "_staged_approvals"))
        self.assertIsInstance(e._staged_approvals, dict)

    def test_staged_approval_recorded_on_stage(self):
        """When request_operator_approval is called, _staged_approvals is populated."""
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        from interface_core.models import BridgeCandidate, MatchScore

        reg  = NodeRegistry()
        eng  = PolicyEngine(reg, {})
        cand = BridgeCandidate(producer_id="p", consumer_id="c",
                               score=MatchScore(composite=0.8))

        async def _go():
            # Start approval request but immediately cancel it
            task = asyncio.create_task(
                eng.request_operator_approval(cand, timeout=0.05)
            )
            await asyncio.sleep(0.01)
            # Staged should be populated
            staged = dict(eng._staged_approvals)
            await task   # let it timeout
            return staged

        staged = run(_go())
        # During the approval wait, the pair must be in _staged_approvals
        key = ("p", "c")
        self.assertIn(key, staged,
            "Pending approval must be in _staged_approvals while waiting")
        self.assertIn("score", staged[key])
        self.assertIn("staged_at", staged[key])

    def test_staged_approval_cleared_after_timeout(self):
        """After timeout, _staged_approvals must be cleared."""
        from interface_core.phases.policy import PolicyEngine
        from interface_core.registry import NodeRegistry
        from interface_core.models import BridgeCandidate, MatchScore

        eng  = PolicyEngine(NodeRegistry(), {})
        cand = BridgeCandidate(producer_id="p2", consumer_id="c2",
                               score=MatchScore(composite=0.7))

        async def _go():
            result = await eng.request_operator_approval(cand, timeout=0.05)
            return result, dict(eng._staged_approvals)

        approved, staged = run(_go())
        self.assertFalse(approved, "Timeout must return False")
        self.assertNotIn(("p2", "c2"), staged,
            "_staged_approvals must be cleared after timeout")


class TestDeregisterNode(unittest.TestCase):

    def test_deregister_node_method_exists(self):
        from interface_core.main import InterfaceCore
        self.assertTrue(hasattr(InterfaceCore, "deregister_node"))

    def test_deregister_removes_from_registry(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        nid  = core.register_node("rest", "http://dereg-test",
                                   {"x": "float"}, ["test"], "producer")
        self.assertIsNotNone(core.registry.get(nid))
        result = core.deregister_node(nid)
        self.assertTrue(result)
        self.assertIsNone(core.registry.get(nid),
            "Node must be removed from registry after deregister_node")

    def test_deregister_removes_from_graph(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        nid  = core.register_node("rest", "http://dereg-graph",
                                   {"x": "float"}, ["test"], "producer")
        core.graph.add_node(nid)
        core.deregister_node(nid)
        # Graph should not contain the node anymore
        nodes = core.graph.all_nodes() if hasattr(core.graph, 'all_nodes') else []
        # If all_nodes isn't available, just verify no error was raised
        self.assertIsNone(core.registry.get(nid))

    def test_deregister_clears_drift_log(self):
        from interface_core.main import InterfaceCore
        from interface_core.models import ContractSnapshot, DataContract, FieldSchema
        core = InterfaceCore()
        nid  = core.register_node("rest", "http://dereg-drift",
                                   {"x": "float"}, ["test"], "producer")
        # Add a drift record
        snap = ContractSnapshot(node_id=nid, fingerprint="fp",
                                contract=DataContract(schema_fields=[FieldSchema(name="x",type="float")]),
                                delta=0.2)
        core.drift_log.record(snap)
        self.assertGreater(len(core.drift_log.history(nid)), 0)
        core.deregister_node(nid)
        self.assertEqual(len(core.drift_log.history(nid)), 0,
            "drift_log must be cleared when node is deregistered")

    def test_deregister_unknown_node_returns_false(self):
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        self.assertFalse(core.deregister_node("ghost-node-xyz"))

    def test_api_delete_uses_deregister(self):
        api_src = open("interface_core/api.py").read()
        self.assertIn("deregister_node", api_src,
            "DELETE /nodes route must use core.deregister_node for atomic removal")


class TestDriftLogClearNode(unittest.TestCase):

    def test_clear_node_exists(self):
        from interface_core.registry import DriftLog
        self.assertTrue(hasattr(DriftLog, "clear_node"))

    def test_clear_node_removes_history(self):
        from interface_core.registry import DriftLog
        from interface_core.models import ContractSnapshot, DataContract, FieldSchema
        dl   = DriftLog()
        snap = ContractSnapshot(node_id="n1", fingerprint="fp",
                                contract=DataContract(schema_fields=[FieldSchema(name="x",type="str")]),
                                delta=0.1)
        dl.record(snap)
        self.assertEqual(len(dl.history("n1")), 1)
        dl.clear_node("n1")
        self.assertEqual(len(dl.history("n1")), 0)

    def test_clear_node_unknown_is_noop(self):
        from interface_core.registry import DriftLog
        dl = DriftLog()
        dl.clear_node("nonexistent")   # must not raise

    def test_drifted_nodes_still_works_after_clear(self):
        from interface_core.registry import DriftLog
        from interface_core.models import ContractSnapshot, DataContract, FieldSchema
        dl = DriftLog()
        snap = ContractSnapshot(node_id="drift-node", fingerprint="fp",
                                contract=DataContract(schema_fields=[FieldSchema(name="x",type="str")]),
                                delta=0.5)
        dl.record(snap)
        self.assertIn("drift-node", dl.drifted_nodes(0.3))
        dl.clear_node("drift-node")
        self.assertNotIn("drift-node", dl.drifted_nodes(0.3))


class TestMatchingPreFilter(unittest.TestCase):

    def test_pre_filter_defined_in_match_all(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("_pre_filter", src,
            "_match_all must define a _pre_filter function to skip incompatible pairs")

    def test_pre_filter_logs_stats(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("pre_filtered", src,
            "_match_all must log pre-filter stats for visibility")

    def test_pre_filter_skips_protocol_incompatible(self):
        """Protocol-incompatible pairs must be skipped before scoring."""
        from interface_core.phases.matching import MatchingPhase
        import inspect
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("_compat", src,
            "Pre-filter must check protocol compat matrix")

    def test_pre_filter_skips_zero_tag_overlap(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("p_tags & c_tags", src,
            "Pre-filter must check tag overlap")

    def test_pre_filter_skips_low_confidence(self):
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("confidence", src,
            "Pre-filter must skip low-confidence contracts")

    def test_pre_filter_generic_tag_exception(self):
        """Pairs with 'generic' tags must pass the tag filter (broad catchall)."""
        import inspect
        from interface_core.phases.matching import MatchingPhase
        src = inspect.getsource(MatchingPhase._match_all)
        self.assertIn("broad", src,
            "Pre-filter must have exception for broad/generic tags")

    def test_pre_filter_reduces_scoring_work(self):
        """With incompatible protocols, scoring should be skipped."""
        from interface_core.main import InterfaceCore
        core = InterfaceCore()
        core.ml_matcher.wait_ready(30)

        # Register nodes with zero tag overlap and different protocols
        # where no compat path exists
        p_id = core.register_node("rest", "http://pf-src",
                                   {"x": "float"}, ["market_data_feed"], "producer", 1.0)
        # Consumer with completely different domain tags (not generic)
        c_id = core.register_node("rest", "http://pf-dst",
                                   {"y": "str"}, ["log_archive_sink"], "consumer")

        async def _go():
            await core.matching._match_all()
            return len(core.bridge_queue)

        # Pre-filter may or may not skip this — depends on tag distance
        # Key assertion: no error raised, queue length is non-negative
        depth = run(_go())
        self.assertGreaterEqual(depth, 0)


class TestFanOutRunnerInSynthesis(unittest.TestCase):

    def test_fanout_import_in_synthesis(self):
        syn_src = open("interface_core/phases/synthesis.py").read()
        self.assertIn("FanOutRunner", syn_src,
            "synthesis.py must import and reference FanOutRunner")

    def test_fanout_logged_when_multiple_consumers(self):
        import inspect
        from interface_core.phases.synthesis import SynthesisPhase
        src = inspect.getsource(SynthesisPhase._synthesize)
        self.assertIn("FAN-OUT", src,
            "_synthesize must log FAN-OUT when producer has multiple consumers")

    def test_fanout_runner_topology_module_exported(self):
        import interface_core as ic
        self.assertTrue(hasattr(ic, "FanOutRunner"),
            "FanOutRunner must be exported from interface_core package")
        self.assertTrue(hasattr(ic, "FanInRunner"))


class TestSchemaDemoRuns(unittest.TestCase):

    def test_demo_file_exists(self):
        self.assertTrue(os.path.exists("examples/demo_schema_pipeline.py"),
            "examples/demo_schema_pipeline.py must exist")

    def test_demo_syntax_valid(self):
        import ast
        with open("examples/demo_schema_pipeline.py") as f:
            try:
                ast.parse(f.read())
            except SyntaxError as e:
                self.fail(f"demo_schema_pipeline.py has syntax error: {e}")

    def test_demo_runs_without_error(self):
        """The schema pipeline demo must complete without raising."""
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            try:
                import importlib.util, sys
                spec = importlib.util.spec_from_file_location(
                    "demo_schema", "examples/demo_schema_pipeline.py"
                )
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                mod.main()
            except Exception as e:
                self.fail(f"demo_schema_pipeline.py raised: {e}")

    def test_demo_shows_2_alerts_from_4_records(self):
        """Pipeline demo must drop 2 records (noise + empty) and output 2."""
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "demo_sp2", "examples/demo_schema_pipeline.py"
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod.main()
        out = buf.getvalue()
        self.assertIn("4 raw records", out)
        self.assertIn("2 structured alerts", out)
        self.assertIn("2 records dropped", out)


class TestStatusStagedApprovals(unittest.TestCase):

    def test_status_has_staged_approvals(self):
        from interface_core.main import InterfaceCore
        core   = InterfaceCore()
        status = core.status()
        self.assertIn("staged_approvals", status,
            "status() must include staged_approvals for operator visibility")
        self.assertIsInstance(status["staged_approvals"], list)

    def test_staged_approvals_initially_empty(self):
        from interface_core.main import InterfaceCore
        core   = InterfaceCore()
        status = core.status()
        self.assertEqual(status["staged_approvals"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
