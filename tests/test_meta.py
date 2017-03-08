
import unittest

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dynalchemy import Registry
from dynalchemy.models import DTable, DColumn


class TestRegistry(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Session = sessionmaker(bind=create_engine('sqlite:///:memory:', echo=False))
        Base = declarative_base()
        session = Session()
        cls.reg = Registry(Base, session)

    def _add(self, collection, name):
        return self.reg.add(collection, name, columns=[
            dict(name='name', kind='String', max=200, null=False),
            dict(name='nb_wings', kind='Integer', null=False),
            dict(name='color', kind='String', max=200),
        ])

    def test_create(self):

        klass = self.reg._create(DTable(
            collection='animal',
            name='bird',
            schema='woot',
            columns=[
                DColumn(name='one', kind='String'),
                DColumn('two', kind='Integer')
            ])
        )
        self.assertEqual(klass.__tablename__, 'animal__bird')

    def test_add(self):

        Bird = self.reg.add('animal', 'bird')
        self.assertEqual(Bird.__tablename__, 'animal__bird')

    def test_add_same_collection(self):

        Bird = self.reg.add('animal', 'bird')
        Bird2 = self.reg.add('animal', 'bird2')
        self.assertEqual(Bird.__tablename__, 'animal__bird')
        self.assertEqual(Bird2.__tablename__, 'animal__bird2')



if __name__ == '__main__':
    unittest.main()




'''

Bird = reg.get('animal', 'bird')

session.add(Bird(name='pinson', nb_wings=4, color='lightblue'))
session.commit()

pinson = session.query(Bird).filter_by(name='pinson').one()

reg.add_column('animal', 'bird', dict(name='food', kind='String', max=200))
pinson = session.query(Bird).filter_by(name='pinson').one()
pinson.food = 'bread'
session.commit()

reg.deprecate_column('animal', 'bird', 'food')
reg.deprecate('animal', 'bird')
reg.get('animal', 'bird')'''