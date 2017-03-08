
import sqlalchemy

from sqlalchemy import Column, Integer, ForeignKey, Float, String
from sqlalchemy import DateTime, Time, Integer, Boolean, PickleType
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import joinedload
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

# this Base is distinct from the application one
# no need to mess the app with those classes
Base = declarative_base()


class DTable(Base):

    __tablename__ = 'dynalchemy_table'

    id = Column(Integer, primary_key=True)
    collection = Column(String, nullable=False)
    name = Column(String, nullable=False)
    schema = Column(String)
    active = Column(Boolean, nullable=False, default=True)

    def get_name(self):
        """ a unique name for this table """

        return '%s__%s' % (self.collection, self.name)

    def to_sa(self, base):
        """ create mapped sa class from db definition """

        dct = {
            '__tablename__': self.get_name(),
            '__table_args__': {'extend_existing': True},
            'ID': self.id,
            'id': Column(Integer, primary_key=True)
        }
        if self.schema:
            dct['__table_args__']['schema'] = self.schema
        for col in self.columns:
            dct[col.name] = col.to_sa()
        klass = type(str(self.get_name()), (base,), dct)
        return klass


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
        """ Ensure attributes correctness
            Should check type/value for default, length, choices...
        """
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
            dict(name='kind', mandatory=True),
            dict(name='mandatory', mandatory=False),
            dict(name='default', mandatory=False)]
        fields += DColumn.COLUMN_TYPES[kind]
        return fields

    def serialize(self):
        """ Convert to basic dict for json """

        fields = DColumn.get_serialization_fields(self.kind)
        for field in fields:
            field['value'] = getattr(self, field['name'])
        return fields
