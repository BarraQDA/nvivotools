# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from sqlalchemy.databases import mssql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
import sqlalchemy_sqlany

class UUID(TypeDecorator):
    """Platform-independent UUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(36), storing as stringified hex values.

    """
    #impl = CHAR
    #http://stackoverflow.com/questions/5849389/storing-uuids-in-sqlite-using-pylons-and-sqlalchemy
    impl = Binary

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return str(uuid.UUID(value)).upper()
            else:
                return str(value).upper()

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)


#@compiles(UNIQUEIDENTIFIER, 'sqlite')
#def compile_UNIQUEIDENTIFIER_mssql_sqlite(element, compiler, **kw):
    #""" Handles mssql UNIQUEIDENTIFIER datatype as UUID in SQLite """
    #try:
        #length = element.length
    #except:
        #length = None
    #element.length = 64 # @note: 36 should be enough, but see the link below

    ## @note: since SA-0.9 all string types have collation, which are not
    ## really compatible between databases, so use default one
    #element.collation = None

    #res = "UUID"
    #if length:
        #element.length = length
    #return res
#def compile_UNIQUEIDENTIFIER_sqlalchemy_sqlany_sqlite(element, compiler, **kw):
    #""" Handles sqlalchemy_sqlany UNIQUEIDENTIFIER datatype as UUID in SQLite """
    #try:
        #length = element.length
    #except:
        #length = None
    #element.length = 64 # @note: 36 should be enough, but see the link below

    ## @note: since SA-0.9 all string types have collation, which are not
    ## really compatible between databases, so use default one
    #element.collation = None

    #res = "UUID"
    #if length:
        #element.length = length
    #return res

mssql.ischema_names['xml'] = String
sqlite.ischema_names['UNIQUEIDENTIFIER'] = UUID

sqlalchemy_sqlany.dialect.ischema_names['xml'] = String
sqlalchemy_sqlany.dialect.ischema_names['long nvarchar'] = TEXT

DataTypeName = { 0: 'Text',
                 1: 'Integer',
                 2: 'Decimal',
                 3: 'DateTime',
                 4: 'Date',
                 5: 'Time',
                 6: 'Boolean' }

ObjectTypeName = {  0: 'DOC',
                    1: 'MP3',
                    5: 'WMV',
                    8: 'JPEG',
                   11: 'PDF' }
