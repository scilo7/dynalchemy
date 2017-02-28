
import unittest

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dynalchemy import Registry


class TestRegistry(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Session = sessionmaker(bind=create_engine('sqlite:///:memory:', echo=False))
        Base = declarative_base()
        session = Session()
        cls.reg = Registry(Base, session)

    def test_add(self):
        Bird = self.reg.add('animal', 'bird', columns=[
            dict(name='name', kind='string', max=200, null=False),
            dict(name='nb_wings', kind='integer', null=False),
            dict(name='color', kind='string', max=200),
        ])
        Bird2 = self.reg.add('animal', 'bird2', columns=[
            dict(name='name', kind='string', max=200, null=False),
            dict(name='nb_wings', kind='integer', null=False),
            dict(name='color', kind='string', max=200),
        ])
        self.assertEqual(Bird.__tablename__, 'animal__bird')
        self.assertEqual(Bird2.__tablename__, 'animal__bird2')


if __name__ == '__main__':
    unittest.main()




'''

Bird = reg.get('animal', 'bird')

session.add(Bird(name='pinson', nb_wings=4, color='lightblue'))
session.commit()

pinson = session.query(Bird).filter_by(name='pinson').one()

reg.add_column('animal', 'bird', dict(name='food', kind='string', max=200))
pinson = session.query(Bird).filter_by(name='pinson').one()
pinson.food = 'bread'
session.commit()

reg.deprecate_column('animal', 'bird', 'food')
reg.deprecate('animal', 'bird')
reg.get('animal', 'bird')'''