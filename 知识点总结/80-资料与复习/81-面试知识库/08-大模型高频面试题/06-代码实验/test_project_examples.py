import unittest
from project_examples import *

class ProjectContracts(unittest.TestCase):
    def test_cancel_requires_confirmation(self):
        self.assertEqual(route_customer_request('cancel_order', {'order_id':'12'})['next'], 'confirm')
        self.assertEqual(route_customer_request('cancel_order', {'order_id':'12'}, True)['next'], 'execute')
    def test_missing_order(self):
        self.assertEqual(route_customer_request('order_status', {})['next'], 'clarify')
    def test_unknown_intent(self):
        self.assertEqual(route_customer_request('DROP TABLE', {})['next'], 'clarify')
    def test_product_dedup(self):
        hits=[dict(product_id='a',image_id='a1',score=.9),dict(product_id='a',image_id='a2',score=.8),dict(product_id='b',image_id='b1',score=.7)]
        self.assertEqual([h['product_id'] for h in aggregate_product_hits(hits)],['a','b'])
    def test_citation_missing_and_unknown(self):
        self.assertEqual(len(check_citations([{'source_ids':[]},{'source_ids':['x']}],['a'])),2)
        self.assertEqual(check_citations([{'source_ids':['a']}],['a']),[])
    def test_dag(self):
        self.assertTrue(validate_prerequisites([('a','b'),('b','c')]))
    def test_cycle(self):
        with self.assertRaises(ValueError):validate_prerequisites([('a','b'),('b','a')])
    def test_acl_and_version(self):
        chunks=[dict(tenant_id=t,doc_id='d',version=v,allowed_groups=g) for t,v,g in [('t1',2,['staff']),('t2',2,['staff']),('t1',1,['staff']),('t1',2,['admin'])]]
        result=select_visible_chunks(chunks,'t1',['staff'],{'d':2})
        self.assertEqual(len(result),1)
    def test_empty_groups_deny(self):
        self.assertEqual(select_visible_chunks([dict(tenant_id='t',doc_id='d',version=1,allowed_groups=['staff'])],'t',[],{'d':1}),[])
