# coding: utf-8
from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, LargeBinary, SmallInteger, Table, Text, Unicode, text
from sqlalchemy_sqlany.base import BIT, UNIQUEIDENTIFIER
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Annotation(Base):
    __tablename__ = 'Annotation'
    __table_args__ = (
        Index('Annotation_PK_Constraint', 'Id', 'Item_Id', unique=True),
    )

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    CompoundSourceRegion_Id = Column(UNIQUEIDENTIFIER)
    Text = Column(Unicode(1024), nullable=False)
    ReferenceTypeId = Column(Integer, nullable=False)
    StartZ = Column(Integer)
    StartText = Column(Integer, nullable=False)
    LengthText = Column(Integer, nullable=False)
    EndText = Column(Integer, server_default=text("StartText+LengthText"))
    StartY = Column(Integer)
    LengthY = Column(Integer)
    EndY = Column(Integer, server_default=text("StartY+LengthY"))
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    ModifiedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))
    RevisionId = Column(BigInteger, server_default=text("1"))


t_AnnotationView = Table(
    'AnnotationView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('content', Unicode(1024), nullable=False),
    Column('startZ', Integer),
    Column('startX', Integer, nullable=False),
    Column('lengthX', Integer, nullable=False),
    Column('endX', Integer),
    Column('startY', Integer),
    Column('lengthY', Integer),
    Column('endY', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('sourceId', UNIQUEIDENTIFIER, nullable=False),
    Column('compoundSourceRegionId', UNIQUEIDENTIFIER),
    Column('referenceType', Integer, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


t_AttributeValueView = Table(
    'AttributeValueView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('isDefaultValue', BIT),
    Column('orderId', Integer),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


t_AttributeView = Table(
    'AttributeView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('dataTypeId', Integer),
    Column('decimalPlaces', Integer),
    Column('bibliographicId', Integer),
    Column('orderId', Integer),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


t_BusinessContentView = Table(
    'BusinessContentView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('containingFolderId', UNIQUEIDENTIFIER),
    Column('pretzelEntityTypeId', Integer),
    Column('revisionId', BigInteger)
)


t_BusinessObjectView = Table(
    'BusinessObjectView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('pretzelEntityTypeId', Integer),
    Column('revisionId', BigInteger)
)


class Category(Base):
    __tablename__ = 'Category'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Layout = Column(Unicode, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


t_ClassificationView = Table(
    'ClassificationView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


class CompoundSourceRegion(Base):
    __tablename__ = 'CompoundSourceRegion'
    __table_args__ = (
        Index('CompoundSourceRegion_PK_Constraint', 'Id', 'Item_Id', unique=True),
    )

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False, index=True)
    OrderId = Column(Integer)
    StartX = Column(Integer, nullable=False)
    LengthX = Column(Integer, nullable=False)
    EndX = Column(Integer, server_default=text("StartX+LengthX"))
    StartY = Column(Integer)
    LengthY = Column(Integer)
    EndY = Column(Integer, server_default=text("StartY+LengthY"))
    Object = Column(LargeBinary)
    PlainText = Column(Text)
    PlainTextLength = Column(Integer, nullable=False)
    Properties = Column(Unicode)
    Entire = Column(BIT, nullable=False)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    ModifiedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))
    RevisionId = Column(BigInteger, server_default=text("1"))
    CustomColumn0 = Column(Text)


t_CompoundSourceRegionView = Table(
    'CompoundSourceRegionView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('source', UNIQUEIDENTIFIER, nullable=False),
    Column('orderId', Integer),
    Column('startX', Integer, nullable=False),
    Column('lengthX', Integer, nullable=False),
    Column('endX', Integer),
    Column('entire', BIT, nullable=False),
    Column('richContent', LargeBinary),
    Column('plainText', Text),
    Column('modifiedDate', DateTime, nullable=False),
    Column('revisionId', BigInteger),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('customColumn', Text)
)


t_DatasetView = Table(
    'DatasetView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', SmallInteger),
    Column('sizeInBytes', BigInteger),
    Column('lengthX', Integer),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


t_DocumentView = Table(
    'DocumentView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', SmallInteger),
    Column('plainText', Text),
    Column('sizeInBytes', BigInteger),
    Column('lengthX', Integer),
    Column('xmlProperties', Unicode),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


class EventLog(Base):
    __tablename__ = 'EventLog'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    CreatedDate = Column(DateTime, nullable=False)
    ItemType = Column(Integer, nullable=False)
    ItemHierarchicalName = Column(Text)
    EventType = Column(Integer, nullable=False)
    EventDetailType = Column(Integer, nullable=False)
    IsUndo = Column(BIT, nullable=False)
    RelatedItemHierarchicalName = Column(Text)
    UserName = Column(Unicode(256), nullable=False)


class ExtendedItem(Base):
    __tablename__ = 'ExtendedItem'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Properties = Column(Unicode, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


t_FolderView = Table(
    'FolderView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', UNIQUEIDENTIFIER),
    Column('predsel', SmallInteger),
    Column('systemFolderId', SmallInteger),
    Column('contentsType', Integer),
    Column('folderCategory', Integer),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


class FrameworkMatrix(Base):
    __tablename__ = 'FrameworkMatrix'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Layout = Column(Unicode, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


class Item(Base):
    __tablename__ = 'Item'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    TypeId = Column(Integer, nullable=False, index=True)
    Name = Column(Unicode(256), nullable=False)
    Nickname = Column(Unicode(64))
    Description = Column(Unicode(512), nullable=False)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    ModifiedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    System = Column(BIT, nullable=False)
    ReadOnly = Column(BIT, nullable=False)
    InheritPermissions = Column(BIT, nullable=False)
    ColorArgb = Column(Integer)
    Aggregate = Column(BIT)
    Deleted = Column(BIT, server_default=text("0"))
    HierarchicalName = Column(Unicode(32767))
    RevisionId = Column(BigInteger, server_default=text("1"))


class Link(Base):
    __tablename__ = 'Link'
    __table_args__ = (
        Index('Link_PK_Constraint', 'Id', 'Item1_Id', 'Item2_Id', unique=True),
    )

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item1_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item2_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False, index=True)
    CompoundSourceRegion1_Id = Column(UNIQUEIDENTIFIER)
    CompoundSourceRegion2_Id = Column(UNIQUEIDENTIFIER)
    ReferenceTypeId1 = Column(Integer, nullable=False)
    StartZ1 = Column(Integer)
    StartX1 = Column(Integer, nullable=False)
    LengthX1 = Column(Integer, nullable=False)
    EndX1 = Column(Integer, server_default=text("StartX1+LengthX1"))
    StartY1 = Column(Integer)
    LengthY1 = Column(Integer)
    EndY1 = Column(Integer, server_default=text("StartY1+LengthY1"))
    ReferenceTypeId2 = Column(Integer, nullable=False)
    StartZ2 = Column(Integer)
    StartX2 = Column(Integer)
    StartY2 = Column(Integer)
    LengthX2 = Column(Integer)
    LengthY2 = Column(Integer)
    EndX2 = Column(Integer, server_default=text("StartX2+LengthX2"))
    EndY2 = Column(Integer, server_default=text("StartY2+LengthY2"))
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    ModifiedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))
    RevisionId = Column(BigInteger, server_default=text("1"))


class LuceneFile(Base):
    __tablename__ = 'LuceneFile'
    __table_args__ = (
        Index('LuceneFile_PK_Constraint', 'Name', 'Instance', unique=True),
    )

    Instance = Column(Integer, primary_key=True, nullable=False)
    Name = Column(Unicode(256), primary_key=True, nullable=False)
    Length = Column(Integer, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    ReaderCount = Column(Integer, nullable=False)
    Data = Column(LargeBinary, nullable=False)


class Matrix(Base):
    __tablename__ = 'Matrix'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Options = Column(Unicode, nullable=False)
    Layout = Column(Unicode, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


class Member(Base):
    __tablename__ = 'Member'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    GroupId = Column(UNIQUEIDENTIFIER, nullable=False)
    Principal = Column(Unicode(256), nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


t_MemoLinkView = Table(
    'MemoLinkView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('sourceMemoGuid', UNIQUEIDENTIFIER, nullable=False),
    Column('linkedItemGuid', UNIQUEIDENTIFIER, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


t_MemoView = Table(
    'MemoView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', SmallInteger),
    Column('plainText', Text),
    Column('sizeInBytes', BigInteger),
    Column('lengthX', Integer),
    Column('xmlProperties', Unicode),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger),
    Column('linkedItemId', UNIQUEIDENTIFIER)
)


class Model(Base):
    __tablename__ = 'Model'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Model = Column(Unicode, nullable=False)
    Groups = Column(Unicode, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


t_NCaptureSourceView = Table(
    'NCaptureSourceView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('importDateTime', Text),
    Column('importUrl', Text)
)


t_NodeMatrixStatisticsView = Table(
    'NodeMatrixStatisticsView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('referenceCount', Integer, nullable=False),
    Column('sourceCount', Integer, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False)
)


t_NodeMatrixView = Table(
    'NodeMatrixView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer),
    Column('creationDate', DateTime),
    Column('note', Unicode(512)),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT),
    Column('isSystemDefined', BIT),
    Column('modificationDate', DateTime),
    Column('modifiedBy', UNIQUEIDENTIFIER),
    Column('name', Unicode(256)),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', SmallInteger),
    Column('isTemporary', SmallInteger, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


class NodeReference(Base):
    __tablename__ = 'NodeReference'
    __table_args__ = (
        Index('NodeReference_Index2', 'Node_Item_Id', 'ReferenceTypeId'),
        Index('NodeReference_Index1', 'Source_Item_Id', 'ReferenceTypeId'),
        Index('NodeReference_PK_Constraint', 'Id', 'Node_Item_Id', 'Source_Item_Id', unique=True)
    )

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False, unique=True)
    Node_Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Source_Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    CompoundSourceRegion_Id = Column(UNIQUEIDENTIFIER)
    ReferenceTypeId = Column(Integer, nullable=False)
    StartZ = Column(Integer)
    StartX = Column(Integer, nullable=False)
    LengthX = Column(Integer, nullable=False)
    EndX = Column(Integer, server_default=text("StartX+LengthX"))
    StartY = Column(Integer)
    LengthY = Column(Integer)
    EndY = Column(Integer, server_default=text("StartY+LengthY"))
    ClusterId = Column(Integer)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    ModifiedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    OfX = Column(Integer)
    Deleted = Column(BIT, server_default=text("0"))
    StartText = Column(Integer)
    LengthText = Column(Integer)
    EndText = Column(Integer, server_default=text("StartText+LengthText"))
    RevisionId = Column(BigInteger, server_default=text("1"))


t_NodeReferenceView = Table(
    'NodeReferenceView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('startX', Integer),
    Column('lengthX', Integer),
    Column('startY', Integer),
    Column('lengthY', Integer),
    Column('startZ', Integer),
    Column('node', UNIQUEIDENTIFIER),
    Column('source', UNIQUEIDENTIFIER),
    Column('compoundSourceRegion', UNIQUEIDENTIFIER),
    Column('referenceType', Integer),
    Column('modifiedDate', DateTime),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('RevisionId', BigInteger),
    Column('isTemporary', SmallInteger, nullable=False)
)


t_NodeStatisticsView = Table(
    'NodeStatisticsView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('referenceCount', Integer, nullable=False),
    Column('sourceCount', Integer, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False)
)


t_NodeView = Table(
    'NodeView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer),
    Column('creationDate', DateTime),
    Column('note', Unicode(512)),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT),
    Column('isSystemDefined', BIT),
    Column('modificationDate', DateTime),
    Column('modifiedBy', UNIQUEIDENTIFIER),
    Column('name', Unicode(256)),
    Column('nickName', Unicode(64)),
    Column('isAggregate', BIT),
    Column('owner', UNIQUEIDENTIFIER),
    Column('immediateOwner', UNIQUEIDENTIFIER),
    Column('parent', UNIQUEIDENTIFIER),
    Column('isTemporary', SmallInteger, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


t_PdfView = Table(
    'PdfView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', SmallInteger),
    Column('plainText', Text),
    Column('sizeInBytes', BigInteger),
    Column('lengthX', Integer),
    Column('xmlPdfPages', Text),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


class Project(Base):
    __tablename__ = 'Project'

    Title = Column(Unicode(256), nullable=False)
    Description = Column(Unicode(512), nullable=False)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    ModifiedBy = Column(UNIQUEIDENTIFIER, nullable=False)
    ReadPassword = Column(Unicode(32), nullable=False)
    WritePassword = Column(Unicode(32), nullable=False)
    ReadPasswordHint = Column(Unicode(64), nullable=False)
    WritePasswordHint = Column(Unicode(64), nullable=False)
    Version = Column(Unicode(16), nullable=False)
    CasebookLayout = Column(Unicode)
    UnassignedLabel = Column(Unicode(64), nullable=False)
    NotApplicableLabel = Column(Unicode(64), nullable=False)
    IndexLanguage = Column(Integer, nullable=False)
    EmbedSources = Column(BIT, nullable=False)
    EmbeddedFileSizeLimitBytes = Column(Integer, nullable=False)
    Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    AllowGuestAccess = Column(BIT, nullable=False)
    EventLogging = Column(BIT, nullable=False)
    UserColors = Column(BIT, nullable=False)
    StopWords = Column(Unicode)
    DateOfBirth = Column(BIT, nullable=False, server_default=text("1"))
    Gender = Column(BIT, nullable=False, server_default=text("1"))
    Religion = Column(BIT, nullable=False, server_default=text("1"))
    Location = Column(BIT, nullable=False, server_default=text("1"))
    RelationshipStatus = Column(BIT, nullable=False, server_default=text("1"))
    Bio = Column(BIT, nullable=False, server_default=text("1"))
    Web = Column(BIT, nullable=False, server_default=text("1"))
    RevisionId = Column(BigInteger, server_default=text("1"))


class Query(Base):
    __tablename__ = 'Query'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Options = Column(Text, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


t_QueryView = Table(
    'QueryView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', SmallInteger),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


t_Queue_Index = Table(
    'Queue_Index', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Data', LargeBinary, nullable=False)
)


t_Queue_Index_FAILED = Table(
    'Queue_Index_FAILED', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Data', LargeBinary, nullable=False)
)


class ReportSearchFolderItem(Base):
    __tablename__ = 'ReportSearchFolderItems'

    Id = Column(Integer, primary_key=True, unique=True)
    SearchFolderId = Column(UNIQUEIDENTIFIER, nullable=False)
    ProjectItemId = Column(UNIQUEIDENTIFIER, nullable=False)


class Role(Base):
    __tablename__ = 'Role'
    __table_args__ = (
        Index('Role_Index1', 'TypeId', 'Item2_Id'),
        Index('Role_PK_Constraint', 'TypeId', 'Item1_Id', 'Item2_Id', unique=True)
    )

    Item1_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    TypeId = Column(Integer, primary_key=True, nullable=False)
    Item2_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Tag = Column(Integer)
    Deleted = Column(BIT, server_default=text("0"))


class SearchFolder(Base):
    __tablename__ = 'SearchFolder'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Criteria = Column(Unicode, nullable=False)
    Deleted = Column(BIT, server_default=text("0"))


t_ShortcutView = Table(
    'ShortcutView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('parent', SmallInteger),
    Column('target', UNIQUEIDENTIFIER),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


class Source(Base):
    __tablename__ = 'Source'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    TypeId = Column(Integer, nullable=False)
    Object = Column(LargeBinary, nullable=False)
    PlainText = Column(Text)
    LengthX = Column(Integer, nullable=False)
    LengthY = Column(Integer)
    MetaData = Column(Unicode)
    Thumbnail = Column(LargeBinary)
    Waveform = Column(LargeBinary)
    Properties = Column(Text)
    LengthZ = Column(Integer)
    Deleted = Column(BIT, server_default=text("0"))
    ContentSize = Column(BigInteger)
    ImportProperties = Column(Text)


class SourceRevision(Base):
    __tablename__ = 'SourceRevision'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item_RevisionId = Column(Integer, primary_key=True, nullable=False, server_default=text("0"))
    TypeId = Column(Integer, nullable=False)
    Object = Column(LargeBinary, nullable=False)
    PlainText = Column(Text)
    LengthX = Column(Integer, nullable=False)
    LengthY = Column(Integer)
    MetaData = Column(Unicode)
    Thumbnail = Column(LargeBinary)
    Waveform = Column(LargeBinary)
    Properties = Column(Text)
    LengthZ = Column(Integer)
    Deleted = Column(BIT, server_default=text("0"))
    ContentSize = Column(BigInteger)
    ImportProperties = Column(Text)


t_SourceStatisticsView = Table(
    'SourceStatisticsView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('nodeCount', Integer, nullable=False),
    Column('referenceCount', Integer, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False)
)


t_SourceView = Table(
    'SourceView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('sizeInBytes', BigInteger),
    Column('lengthX', Integer),
    Column('pretzelEntityTypeId', Integer),
    Column('owner', UNIQUEIDENTIFIER),
    Column('revisionId', BigInteger)
)


t_StreamSourceView = Table(
    'StreamSourceView', metadata,
    Column('createdBy', UNIQUEIDENTIFIER, nullable=False),
    Column('colorArgb', Integer),
    Column('businessObjectTypeId', Integer, nullable=False),
    Column('creationDate', DateTime, nullable=False),
    Column('note', Unicode(512), nullable=False),
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('hierarchicalName', Text),
    Column('isReadOnly', BIT, nullable=False),
    Column('isSystemDefined', BIT, nullable=False),
    Column('modificationDate', DateTime, nullable=False),
    Column('modifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('name', Unicode(256), nullable=False),
    Column('owner', UNIQUEIDENTIFIER),
    Column('streamType', Integer),
    Column('duration', Integer),
    Column('lengthX', Integer),
    Column('sourcePath', Text),
    Column('sizeInBytes', BigInteger),
    Column('checksum', BigInteger),
    Column('currentTime', Integer),
    Column('compoundRegion', Text),
    Column('hiddenTranscript', Text),
    Column('streamMetaData', Text),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False),
    Column('revisionId', BigInteger)
)


class Style(Base):
    __tablename__ = 'Style'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    TypeId = Column(Integer, nullable=False)
    Name = Column(Unicode(64), nullable=False)
    System = Column(BIT, nullable=False)
    FontName = Column(Unicode(64), nullable=False)
    FontSize = Column(Integer, nullable=False)
    FontColorArgb = Column(Integer, nullable=False)
    FontColorEnum = Column(Integer, nullable=False)
    Bold = Column(BIT, nullable=False)
    Italic = Column(BIT, nullable=False)
    Underline = Column(BIT, nullable=False)
    LineStyle = Column(Integer)
    LineWeight = Column(Integer)
    LineColorArgb = Column(Integer)
    LineColorEnum = Column(Integer)
    FillColorArgb = Column(Integer)
    FillColorEnum = Column(Integer)
    Deleted = Column(BIT, server_default=text("0"))
    RevisionId = Column(BigInteger, server_default=text("1"))


t_TmpItem = Table(
    'TmpItem', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('TypeId', Integer),
    Column('Name', Unicode(256)),
    Column('Nickname', Unicode(64)),
    Column('Description', Unicode(512)),
    Column('CreatedDate', DateTime),
    Column('ModifiedDate', DateTime),
    Column('CreatedBy', UNIQUEIDENTIFIER),
    Column('ModifiedBy', UNIQUEIDENTIFIER),
    Column('System', BIT),
    Column('ReadOnly', BIT),
    Column('InheritPermissions', BIT),
    Column('Aggregate', BIT),
    Column('ColorArgb', Integer),
    Column('RevisionId', BigInteger)
)


t_TmpMatrix = Table(
    'TmpMatrix', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False, unique=True),
    Column('Options', Unicode, nullable=False),
    Column('Layout', Unicode, nullable=False)
)


t_TmpNodeReference = Table(
    'TmpNodeReference', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False, unique=True),
    Column('Node_Item_Id', UNIQUEIDENTIFIER, index=True),
    Column('Source_Item_Id', UNIQUEIDENTIFIER),
    Column('CompoundSourceRegion_Id', UNIQUEIDENTIFIER),
    Column('ReferenceTypeId', Integer),
    Column('StartX', Integer),
    Column('LengthX', Integer),
    Column('EndX', Integer, server_default=text("StartX+LengthX")),
    Column('StartY', Integer),
    Column('LengthY', Integer),
    Column('EndY', Integer, server_default=text("StartY+LengthY")),
    Column('StartZ', Integer),
    Column('ClusterId', Integer),
    Column('CreatedDate', DateTime),
    Column('CreatedBy', UNIQUEIDENTIFIER),
    Column('ModifiedDate', DateTime),
    Column('ModifiedBy', UNIQUEIDENTIFIER),
    Column('OfX', Integer),
    Column('StartText', Integer),
    Column('LengthText', Integer),
    Column('EndText', Integer, server_default=text("StartText+LengthText")),
    Column('RevisionId', BigInteger)
)


t_TmpRole = Table(
    'TmpRole', metadata,
    Column('Item1_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('TypeId', Integer, nullable=False),
    Column('Item2_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Tag', Integer),
    Index('TmpRole_PK_Constraint', 'TypeId', 'Item1_Id', 'Item2_Id', unique=True),
    Index('TmpRole_Index1', 'TypeId', 'Item2_Id')
)


t_Transaction = Table(
    'Transaction', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('GroupId', UNIQUEIDENTIFIER),
    Column('TableName', Unicode(256), nullable=False),
    Column('TablePkIds', Text, nullable=False),
    Column('TablePkColumns', Text, nullable=False),
    Column('OperationType', Integer, nullable=False),
    Column('UpdatedColumnNames', Text),
    Column('UserId', UNIQUEIDENTIFIER, nullable=False),
    Column('CreatedDate', DateTime, nullable=False)
)


class UserGroup(Base):
    __tablename__ = 'UserGroup'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Name = Column(Unicode(256), nullable=False)
    Description = Column(Unicode(256))


class UserProfile(Base):
    __tablename__ = 'UserProfile'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, unique=True)
    Initials = Column(Unicode(16), nullable=False)
    Name = Column(Unicode(64), nullable=False)
    AccountName = Column(Unicode(256))
    ColorArgb = Column(Integer)
    Deleted = Column(BIT, server_default=text("0"))
    RevisionId = Column(BigInteger, server_default=text("1"))


t_UserProfileView = Table(
    'UserProfileView', metadata,
    Column('guid', UNIQUEIDENTIFIER, nullable=False),
    Column('revisionId', BigInteger),
    Column('initials', Unicode(16), nullable=False),
    Column('name', Unicode(64), nullable=False),
    Column('account', Unicode(256)),
    Column('colorArgb', Integer),
    Column('modifiedProject', BIT, nullable=False),
    Column('pretzelEntityTypeId', SmallInteger, nullable=False)
)


t_vAnnotation = Table(
    'vAnnotation', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('CompoundSourceRegion_Id', UNIQUEIDENTIFIER),
    Column('Text', Unicode(1024), nullable=False),
    Column('ReferenceTypeId', Integer, nullable=False),
    Column('StartZ', Integer),
    Column('StartText', Integer, nullable=False),
    Column('LengthText', Integer, nullable=False),
    Column('EndText', Integer),
    Column('StartY', Integer),
    Column('LengthY', Integer),
    Column('EndY', Integer),
    Column('CreatedDate', DateTime, nullable=False),
    Column('ModifiedDate', DateTime, nullable=False),
    Column('CreatedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('ModifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('RevisionId', BigInteger)
)


t_vCategory = Table(
    'vCategory', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Layout', Unicode, nullable=False)
)


t_vCompoundSourceRegion = Table(
    'vCompoundSourceRegion', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('OrderId', Integer),
    Column('StartX', Integer, nullable=False),
    Column('LengthX', Integer, nullable=False),
    Column('EndX', Integer),
    Column('StartY', Integer),
    Column('LengthY', Integer),
    Column('EndY', Integer),
    Column('Object', LargeBinary),
    Column('PlainText', Text),
    Column('PlainTextLength', Integer, nullable=False),
    Column('Properties', Unicode),
    Column('Entire', BIT, nullable=False),
    Column('CreatedDate', DateTime, nullable=False),
    Column('ModifiedDate', DateTime, nullable=False),
    Column('CreatedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('ModifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('RevisionId', BigInteger),
    Column('CustomColumn0', Text)
)


t_vExtendedItem = Table(
    'vExtendedItem', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Properties', Unicode, nullable=False)
)


t_vFrameworkMatrix = Table(
    'vFrameworkMatrix', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Layout', Unicode, nullable=False)
)


t_vItem = Table(
    'vItem', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('TypeId', Integer, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Nickname', Unicode(64)),
    Column('Description', Unicode(512), nullable=False),
    Column('CreatedDate', DateTime, nullable=False),
    Column('ModifiedDate', DateTime, nullable=False),
    Column('CreatedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('ModifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('System', BIT, nullable=False),
    Column('ReadOnly', BIT, nullable=False),
    Column('ColorArgb', Integer),
    Column('Aggregate', BIT),
    Column('HierarchicalName', Text),
    Column('RevisionId', BigInteger)
)


t_vLink = Table(
    'vLink', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Item1_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Item2_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('CompoundSourceRegion1_Id', UNIQUEIDENTIFIER),
    Column('CompoundSourceRegion2_Id', UNIQUEIDENTIFIER),
    Column('ReferenceTypeId1', Integer, nullable=False),
    Column('StartZ1', Integer),
    Column('StartX1', Integer, nullable=False),
    Column('LengthX1', Integer, nullable=False),
    Column('EndX1', Integer),
    Column('StartY1', Integer),
    Column('LengthY1', Integer),
    Column('EndY1', Integer),
    Column('ReferenceTypeId2', Integer, nullable=False),
    Column('StartZ2', Integer),
    Column('StartX2', Integer),
    Column('LengthX2', Integer),
    Column('EndX2', Integer),
    Column('StartY2', Integer),
    Column('LengthY2', Integer),
    Column('EndY2', Integer),
    Column('CreatedDate', DateTime, nullable=False),
    Column('ModifiedDate', DateTime, nullable=False),
    Column('CreatedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('ModifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('RevisionId', BigInteger)
)


t_vMatrix = Table(
    'vMatrix', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Options', Unicode, nullable=False),
    Column('Layout', Unicode, nullable=False)
)


t_vMember = Table(
    'vMember', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('GroupId', UNIQUEIDENTIFIER, nullable=False),
    Column('Principal', Unicode(256), nullable=False)
)


t_vModel = Table(
    'vModel', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Model', Unicode, nullable=False),
    Column('Groups', Unicode, nullable=False)
)


t_vNodeReference = Table(
    'vNodeReference', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Node_Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Source_Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('CompoundSourceRegion_Id', UNIQUEIDENTIFIER),
    Column('ReferenceTypeId', Integer, nullable=False),
    Column('StartZ', Integer),
    Column('StartX', Integer, nullable=False),
    Column('LengthX', Integer, nullable=False),
    Column('EndX', Integer),
    Column('StartY', Integer),
    Column('LengthY', Integer),
    Column('EndY', Integer),
    Column('ClusterId', Integer),
    Column('CreatedDate', DateTime, nullable=False),
    Column('ModifiedDate', DateTime, nullable=False),
    Column('CreatedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('ModifiedBy', UNIQUEIDENTIFIER, nullable=False),
    Column('OfX', Integer),
    Column('StartText', Integer),
    Column('LengthText', Integer),
    Column('EndText', Integer),
    Column('RevisionId', BigInteger)
)


t_vQuery = Table(
    'vQuery', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Options', Text, nullable=False)
)


t_vRole = Table(
    'vRole', metadata,
    Column('Item1_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('TypeId', Integer, nullable=False),
    Column('Item2_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Tag', Integer)
)


t_vSearchFolder = Table(
    'vSearchFolder', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Criteria', Unicode, nullable=False)
)


t_vSource = Table(
    'vSource', metadata,
    Column('Item_Id', UNIQUEIDENTIFIER, nullable=False),
    Column('TypeId', Integer, nullable=False),
    Column('Object', LargeBinary, nullable=False),
    Column('PlainText', Text),
    Column('LengthX', Integer, nullable=False),
    Column('LengthY', Integer),
    Column('MetaData', Unicode),
    Column('Thumbnail', LargeBinary),
    Column('Waveform', LargeBinary),
    Column('Properties', Text),
    Column('LengthZ', Integer),
    Column('ContentSize', BigInteger),
    Column('ImportProperties', Text)
)


t_vStyle = Table(
    'vStyle', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('TypeId', Integer, nullable=False),
    Column('Name', Unicode(64), nullable=False),
    Column('System', BIT, nullable=False),
    Column('FontName', Unicode(64), nullable=False),
    Column('FontSize', Integer, nullable=False),
    Column('FontColorArgb', Integer, nullable=False),
    Column('FontColorEnum', Integer, nullable=False),
    Column('Bold', BIT, nullable=False),
    Column('Italic', BIT, nullable=False),
    Column('Underline', BIT, nullable=False),
    Column('LineStyle', Integer),
    Column('LineWeight', Integer),
    Column('LineColorArgb', Integer),
    Column('LineColorEnum', Integer),
    Column('FillColorArgb', Integer),
    Column('FillColorEnum', Integer),
    Column('RevisionId', BigInteger)
)


t_vUserProfile = Table(
    'vUserProfile', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Initials', Unicode(16), nullable=False),
    Column('Name', Unicode(64), nullable=False),
    Column('AccountName', Unicode(256)),
    Column('ColorArgb', Integer),
    Column('RevisionId', BigInteger)
)
