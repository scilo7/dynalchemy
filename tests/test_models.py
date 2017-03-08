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


if __name__ == '__main__':
    unittest.main()
