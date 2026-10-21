import asyncio
import math
import sqlite3
import tempfile
import unittest
from pathlib import Path
import numpy as np
import core_examples as c

class CoreExamplesTest(unittest.TestCase):
    def test_closure_isolation(self):
        a,b=c.make_counter(),c.make_counter()
        self.assertEqual([a(),a(),b()],[1,2,1])
    def test_two_sum(self):self.assertEqual(c.two_sum([2,7,11],9),(0,1))
    def test_classification_metrics(self):
        m=c.binary_metrics([1,1,0,0],[1,0,1,0]);self.assertEqual(m,dict(precision=.5,recall=.5,f1=.5))
    def test_standardization_no_test_leakage(self):
        train,test=c.standardize_from_train(np.array([[1.],[3.]]),np.array([[101.]]))
        np.testing.assert_allclose(train,[[-1],[1]]);np.testing.assert_allclose(test,[[99]])
    def test_rank_deficient_least_squares(self):
        x=np.array([[1,2],[2,4],[3,6]],float);y=np.array([3,5,7])
        w=c.least_squares(x,y);np.testing.assert_allclose(np.column_stack([np.ones(3),x])@w,y)
    def test_entropy(self):self.assertAlmostEqual(c.entropy([0,0,1,1]),1)
    def test_pca_train_center(self):
        x=np.array([[0.,0],[1,1],[2,2]]);mean,basis,s=c.fit_pca(x,1)
        self.assertEqual(basis.shape,(2,1));self.assertAlmostEqual(s[1],0)
    def test_stable_softmax(self):np.testing.assert_allclose(c.softmax(np.array([[1000.,1000.]])),[[.5,.5]])
    def test_causal_mask(self):
        q=np.eye(3);v=np.array([[1.,0],[0,1],[9,9]])
        out,w=c.causal_attention(q,q,v)
        self.assertEqual(w[0,1],0);self.assertEqual(w[1,2],0);np.testing.assert_allclose(out[0],v[0])
    def test_norms(self):
        x=np.array([[1.,2,3]])
        self.assertAlmostEqual(float(c.layer_norm(x).mean()),0)
        self.assertGreater(float(c.rms_norm(x).mean()),0)
    def test_rope_relative_position(self):
        q=np.array([1.,2,3,4]);k=np.array([2.,-1,3,1])
        self.assertAlmostEqual(float(c.rope(q,5)@c.rope(k,8)),float(c.rope(q,0)@c.rope(k,3)))
    def test_lora_merge_equivalence(self):
        rng=np.random.default_rng(3);x=rng.normal(size=(2,3));w=rng.normal(size=(4,3))
        a=rng.normal(size=(2,3));b=rng.normal(size=(4,2))
        np.testing.assert_allclose(c.lora_forward(x,w,a,b,8),x@(w+4*b@a).T)
    def test_kv_heads(self):self.assertEqual(c.kv_cache_bytes(1,32,4096,8,128),536870912)
    def test_response_only_loss(self):
        logits=np.zeros((1,4,5));labels=np.array([[-100,-100,2,3]])
        self.assertAlmostEqual(c.causal_lm_loss(logits,labels),math.log(5))
        with self.assertRaises(ValueError):c.causal_lm_loss(logits,np.full((1,4),-100))
    def test_dpo_monotonic_margin(self):self.assertLess(c.dpo_loss(-1.,-5.,-2.,-2.),c.dpo_loss(-5.,-1.,-2.,-2.))
    def test_group_relative_advantage(self):
        a=c.grpo_advantages([[1,1,1],[0,1,2]])
        np.testing.assert_allclose(a[0],0);self.assertAlmostEqual(float(a[1].mean()),0)
    def test_ppo_negative_advantage(self):self.assertAlmostEqual(c.ppo_surrogate(np.array([2.]),np.array([-1.])), -2.)
    def test_cosine_scale_invariance(self):np.testing.assert_allclose(c.cosine_similarity([2,0],[[1,0],[0,5]]),[1,0])
    def test_rrf_deduplicates_each_ranking(self):
        a=c.rrf_fuse([["a","a"],["b","a"]]);self.assertAlmostEqual(dict(a)["a"],1/61+1/62)
    def test_retrieval_metrics(self):self.assertEqual(c.retrieval_metrics(["x","a","a","b"],{"a","b"},3),dict(recall_at_k=1,mrr=.5))
    def test_chunk_offsets(self):
        text='0123456789';parts=c.chunk_with_offsets(text,4,1)
        self.assertTrue(all(p['text']==text[p['start']:p['end']] for p in parts));self.assertEqual(parts[-1]['end'],10)
    def test_bio_spans(self):
        self.assertEqual(c.decode_bio(list('北京市'),['B-LOC','I-LOC','I-LOC']),[('LOC',0,3,'北京市')])
        with self.assertRaises(ValueError):c.decode_bio(['京'],['I-LOC'])
    def test_tool_validation(self):
        self.assertEqual(c.validate_tool_call('{"tool":"get_order","args":{"order_id":"42"}}')['tool'],'get_order')
        with self.assertRaises(ValueError):c.validate_tool_call('{"tool":"delete_order","args":{}}')
    def test_bounded_async(self):
        active=0;peak=0
        async def operation(x):
            nonlocal active,peak
            active+=1;peak=max(peak,active);await asyncio.sleep(.001);active-=1;return x*x
        self.assertEqual(asyncio.run(c.bounded_map(range(8),operation,2)),[x*x for x in range(8)])
        self.assertLessEqual(peak,2)
    def test_sse_boundary(self):
        s=c.sse_event('delta',{'text':'一\n二'},3)
        self.assertTrue(s.endswith('\n\n'));self.assertEqual(len(s.splitlines()),4)
    def test_readonly_database(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'data.db';conn=sqlite3.connect(p)
            conn.execute('CREATE TABLE orders(id INTEGER,amount INTEGER)');conn.execute('INSERT INTO orders VALUES(1,20)');conn.commit();conn.close()
            self.assertEqual(c.readonly_query(p,'SELECT SUM(amount) FROM orders'),[(20,)])
            with self.assertRaises(sqlite3.DatabaseError):c.readonly_query(p,'DELETE FROM orders')
            with self.assertRaises(sqlite3.DatabaseError):c.readonly_query(p,"ATTACH DATABASE ':memory:' AS another")
    def test_clip_pairing(self):
        x=np.eye(3)
        self.assertLess(c.clip_contrastive_loss(x,x,10),c.clip_contrastive_loss(x,x[::-1],10))
    def test_diffusion_boundaries(self):
        x=np.array([1.,2]);noise=np.array([-1.,0])
        np.testing.assert_allclose(c.diffusion_forward(x,noise,1),x)
        np.testing.assert_allclose(c.diffusion_forward(x,noise,0),noise)

if __name__=='__main__':unittest.main(verbosity=2)
