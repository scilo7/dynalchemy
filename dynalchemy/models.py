
import sqlalchemy

from sqlalchemy import Column, Integer, ForeignKey, Float, String
from sqlalchemy import DateTime, Time, Integer, Boolean, PickleType
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

    def get_name(self):
        return '%s__%s' % (self.collection, self.name)


class DColumn(Base):

    COLUMN_TYPES = {
        # accepted types & their construction args
        'BigInteger': [],
        'Binary': [],
        'Boolean': [],
        'Date': [],
        'DateTime': [],
        'Enum': [dict(name='enums', mandatory=True)],
        'Float': [dict(name='precision', mandatory=False)],
        'Integer': [],
        'LargeBinary': [dict(name='length', mandatory=False)],
        'Numeric': [],
        'SmallInteger': [],
        'String':  [dict(name='length', mandatory=False)],
        'Text':  [dict(name='length', mandatory=False)],
        'Time': []
    }

    __tablename__ = 'dynalchemy_column'

    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('dynalchemy_table.id'))
    name = Column(String, nullable=False)
    kind = Column(String, nullable=False, default='String')
    active = Column(Boolean, nullable=False, default=True)
    mandatory = Column(Boolean, nullable=False, default=False)
    default = Column(String)
    length = Column(Integer)
    choices = Column(PickleType)
    precision = Column(Integer)

    table = relationship(DTable, backref='columns')

    def validate(self):
        """ ensure attributes correctness """
        pass

    def _get_type(self):
        """ sqlalchemy type for the column """

        kind = getattr(sqlalchemy, self.kind)
        if self.kind == 'String' and self.length:
            kind = kind(self.length)
        if self.kind == 'Enum':
            kind = kind(*self.choices)
        return kind

    def _get_default(self):
        """ convert default to the correct type """

        if self.kind in ('BigInteger', 'Integer', 'SmallInteger'):
            return int(self.default)
        elif self.kind in ('Float', 'Numeric'):
            return float(self.default)
        elif self.kind == 'Boolean':
            if self.default in ('t', 'true', 'y', 'on', '1'):
                return True
            else:
                return False
        else:
            return self.default

    def _get_args(self):
        """ sqlalchmey extra args for column creation """

        args = {}
        if self.default is not None:
            args['default'] = self._get_default()
        return args

    def to_sa(self):
        """ convert to sa object Column """

        return Column(self.name, self._get_type(), **self._get_args())

    @classmethod
    def get_serialization_fields(cls, kind):
        """ List fields availables for Column declaration of one type """

        fields = [
            dict(name='name', mandatory=True),
            dict(name='kind', mandatory=True)]
        fields += DColumn.COLUMN_TYPES[kind]
        return fields

    def serialize(self):
        """ Convert to basic dict for json """

        fields = DColumn.get_serialization_fields(self.kind)
        for field in fields:
            field['value'] = getattr(self, field['name'])
        return fields
