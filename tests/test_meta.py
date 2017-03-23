
import unittest

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dynalchemy import Registry
from dynalchemy.models import DTable, DColumn


class TestRegistry(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite:///:memory:', echo=False)
        self.base = declarative_base(bind=engine)
        self.reg = Registry(self.base, sessionmaker(bind=engine)())

    def tearDown(self):
        self.base.metadata.drop_all()

    def _get_class(self, collection='animal', name='bird'):
        return self.reg.add(collection, name, columns=[
            dict(name='name', kind='String', nullable=False),
            dict(name='nb_wings', kind='Integer'),
            dict(name='color', kind='String'),
        ])

    def test_add(self):

        self.assertEqual(self._get_class().__tablename__, 'animal__bird')

    def test_add_same_collection(self):

        Bird = self._get_class()
        Bird2 = self._get_class(name='bird2')
        self.assertEqual(Bird.__tablename__, 'animal__bird')
        self.assertEqual(Bird2.__tablename__, 'animal__bird2')

    def test_add_from_config(self):

        Bird = self.reg.add_from_config({
            'collection': 'animal',
            'name': 'bird'
        })
        self.assertEqual(Bird.__tablename__, 'animal__bird')

    def test_get(self):

        Bird = self._get_class()
        self.assertEqual(self.reg.get('animal', 'bird'), Bird)

    def test_list(self):

        Bird = self._get_class()
        Bird2 = self._get_class(name='bird2')
        Bird3 = self._get_class(collection='zoo')
        self.assertEqual(self.reg.list('animal'), [Bird, Bird2])

    def test_add_column(self):
        self._get_class()
        self.reg.add_column('animal', 'bird', dict(name='extra', kind='String'))
        Bird = self.reg.get('animal', 'bird')
        self.assertTrue(hasattr(Bird, 'extra'))


if __name__ == '__main__':
    unittest.main()
