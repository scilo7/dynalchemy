from sqlalchemy import Integer, String
import unittest

from dynalchemy.models import DTable, DColumn


class TestDColumn(unittest.TestCase):

    def test_get_basic_type(self):

        klass = DColumn(name='bob', kind='Integer')._get_type()
        self.assertEqual(klass, Integer)

    def test_get_complex_type(self):

        kind = DColumn(name='bob', kind='String', length=25)._get_type()
        self.assertEqual(kind.__class__, String)
        self.assertEqual(kind.length, 25)

    def test_get_default(self):

        col = DColumn(name='bob', kind='Integer', default='100')
        self.assertEqual(col._get_default(), 100)


if __name__ == '__main__':
    unittest.main()
