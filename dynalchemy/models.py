
import sqlalchemy
from sqlalchemy import Column, Integer, ForeignKey, Float, String, DateTime, Time, Integer, Boolean
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import joinedload
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class DTable(Base):

    __tablename__ = 'dynalchemy_table'

    id = Column(Integer, primary_key=True)
    collection = Column(String, nullable=False)
    name = Column(String, nullable=False)
    schema = Column(String)
    active = Column(Boolean, nullable=False, default=True)

    def get_key(self):
        return '%s.%s' % (self.collection, self.name)

    def get_name(self):
        return '%s_%s' % (self.collection, self.name)


class DColumn(Base):

    __tablename__ = 'dynalchemy_column'

    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('dynalchemy_table.id'))
    name = Column(String, nullable=False)
    kind = Column(String, nullable=False, default='String')
    active = Column(Boolean, nullable=False, default=True)

    table = relationship(DTable, backref='columns')

    def _get_type(self):

        return getattr(sqlalchemy, self.kind.capitalize())

    def to_sa(self):

        return Column(self.name, self._get_type())
