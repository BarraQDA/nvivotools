# coding: utf-8
from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, ForeignKeyConstraint, Index, Integer, LargeBinary, Numeric, String, Table, Unicode, text
from sqlalchemy.dialects.mssql.base import BIT, UNIQUEIDENTIFIER
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Annotation(Base):
    __tablename__ = 'Annotation'
    __table_args__ = (
        ForeignKeyConstraint(['CompoundSourceRegion_Id', 'Item_Id'], [u'CompoundSourceRegion.Id', u'CompoundSourceRegion.Item_Id']),
    )

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False)
    CompoundSourceRegion_Id = Column(UNIQUEIDENTIFIER)
    Text = Column(Unicode(1024), nullable=False)
    ReferenceTypeId = Column(Integer, nullable=False)
    StartZ = Column(Integer)
    StartX = Column(Integer, nullable=False)
    LengthX = Column(Integer, nullable=False)
    EndX = Column(Integer)
    StartY = Column(Integer)
    LengthY = Column(Integer)
    EndY = Column(Integer)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
    ModifiedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)

    CompoundSourceRegion = relationship(u'CompoundSourceRegion')
    UserProfile = relationship(u'UserProfile', primaryjoin='Annotation.CreatedBy == UserProfile.Id')
    Item = relationship(u'Item')
    UserProfile1 = relationship(u'UserProfile', primaryjoin='Annotation.ModifiedBy == UserProfile.Id')


class CompoundSourceRegion(Base):
    __tablename__ = 'CompoundSourceRegion'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False, index=True)
    OrderId = Column(Integer)
    StartX = Column(Integer, nullable=False)
    LengthX = Column(Integer, nullable=False)
    EndX = Column(Integer)
    StartY = Column(Integer)
    LengthY = Column(Integer)
    EndY = Column(Integer)
    Object = Column(LargeBinary)
    PlainText = Column(Unicode)
    PlainTextLength = Column(Integer, nullable=False)
    Properties = Column(Unicode)
    Entire = Column(BIT, nullable=False)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
    ModifiedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)

    UserProfile = relationship(u'UserProfile', primaryjoin='CompoundSourceRegion.CreatedBy == UserProfile.Id')
    Item = relationship(u'Item')
    UserProfile1 = relationship(u'UserProfile', primaryjoin='CompoundSourceRegion.ModifiedBy == UserProfile.Id')


class EventLog(Base):
    __tablename__ = 'EventLog'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
    CreatedDate = Column(DateTime, nullable=False)
    ItemType = Column(Integer, nullable=False)
    ItemHierarchicalName = Column(Unicode)
    EventType = Column(Integer, nullable=False)
    EventDetailType = Column(Integer, nullable=False)
    IsUndo = Column(BIT, nullable=False)
    RelatedItemHierarchicalName = Column(Unicode)
    UserName = Column(Unicode(256), nullable=False)


class Item(Base):
    __tablename__ = 'Item'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
    TypeId = Column(Integer, nullable=False, index=True)
    Name = Column(Unicode(256), nullable=False)
    Nickname = Column(Unicode(64))
    Description = Column(Unicode(512), nullable=False)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
    ModifiedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
    System = Column(BIT, nullable=False)
    ReadOnly = Column(BIT, nullable=False)
    InheritPermissions = Column(BIT, nullable=False, server_default=text("((1))"))
    ColorArgb = Column(Integer)
    Aggregate = Column(BIT)

    UserProfile = relationship(u'UserProfile', primaryjoin='Item.CreatedBy == UserProfile.Id')
    UserProfile1 = relationship(u'UserProfile', primaryjoin='Item.ModifiedBy == UserProfile.Id')


class Category(Item):
    __tablename__ = 'Category'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    Layout = Column(Unicode, nullable=False)


class Matrix(Item):
    __tablename__ = 'Matrix'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    Options = Column(Unicode, nullable=False)
    Layout = Column(Unicode, nullable=False)


class Source(Item):
    __tablename__ = 'Source'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    TypeId = Column(Integer, nullable=False)
    Object = Column(LargeBinary, nullable=False)
    PlainText = Column(Unicode)
    LengthX = Column(Integer, nullable=False)
    LengthY = Column(Integer)
    LengthZ = Column(Integer)
    MetaData = Column(Unicode)
    Thumbnail = Column(LargeBinary)
    Waveform = Column(LargeBinary)
    Properties = Column(Unicode)
    ImportProperties = Column(Unicode)


class ExtendedItem(Item):
    __tablename__ = 'ExtendedItem'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    Properties = Column(Unicode, nullable=False, index=True)


class FrameworkMatrix(Item):
    __tablename__ = 'FrameworkMatrix'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    Layout = Column(Unicode, nullable=False)


class Model(Item):
    __tablename__ = 'Model'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    Model = Column(Unicode, nullable=False)
    Groups = Column(Unicode, nullable=False)


class SearchFolder(Item):
    __tablename__ = 'SearchFolder'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    Criteria = Column(Unicode, nullable=False)


class Query(Item):
    __tablename__ = 'Query'

    Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True)
    Options = Column(Unicode, nullable=False)


class ItemAuthorisation(Base):
    __tablename__ = 'ItemAuthorisation'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
    Item_Id = Column(ForeignKey(u'Item.Id'), nullable=False)
    Principal = Column(Unicode(256), nullable=False)
    AllowRead = Column(BIT, nullable=False)
    AllowWrite = Column(BIT, nullable=False)
    DenyRead = Column(BIT, nullable=False)
    DenyWrite = Column(BIT, nullable=False)

    Item = relationship(u'Item')


class Link(Base):
    __tablename__ = 'Link'
    __table_args__ = (
        ForeignKeyConstraint(['CompoundSourceRegion1_Id', 'Item1_Id'], [u'CompoundSourceRegion.Id', u'CompoundSourceRegion.Item_Id']),
        ForeignKeyConstraint(['CompoundSourceRegion2_Id', 'Item2_Id'], [u'CompoundSourceRegion.Id', u'CompoundSourceRegion.Item_Id'])
    )

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Item1_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False)
    Item2_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False, index=True)
    CompoundSourceRegion1_Id = Column(UNIQUEIDENTIFIER)
    CompoundSourceRegion2_Id = Column(UNIQUEIDENTIFIER)
    ReferenceTypeId1 = Column(Integer, nullable=False)
    StartZ1 = Column(Integer)
    StartX1 = Column(Integer, nullable=False)
    LengthX1 = Column(Integer, nullable=False)
    EndX1 = Column(Integer)
    StartY1 = Column(Integer)
    LengthY1 = Column(Integer)
    EndY1 = Column(Integer)
    ReferenceTypeId2 = Column(Integer, nullable=False)
    StartZ2 = Column(Integer)
    StartX2 = Column(Integer)
    StartY2 = Column(Integer)
    LengthX2 = Column(Integer)
    LengthY2 = Column(Integer)
    EndX2 = Column(Integer)
    EndY2 = Column(Integer)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
    ModifiedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)

    CompoundSourceRegion = relationship(u'CompoundSourceRegion', primaryjoin='Link.CompoundSourceRegion1_Id == CompoundSourceRegion.Id')
    CompoundSourceRegion1 = relationship(u'CompoundSourceRegion', primaryjoin='Link.CompoundSourceRegion2_Id == CompoundSourceRegion.Id')
    UserProfile = relationship(u'UserProfile', primaryjoin='Link.CreatedBy == UserProfile.Id')
    Item = relationship(u'Item', primaryjoin='Link.Item1_Id == Item.Id')
    Item1 = relationship(u'Item', primaryjoin='Link.Item2_Id == Item.Id')
    UserProfile1 = relationship(u'UserProfile', primaryjoin='Link.ModifiedBy == UserProfile.Id')


class Lock(Base):
    __tablename__ = 'Lock'

    Item_Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    LockedBy = Column(ForeignKey(u'UserProfile.Id'), primary_key=True, nullable=False)
    Exclusive = Column(BIT, nullable=False)

    UserProfile = relationship(u'UserProfile')


class LuceneFile(Base):
    __tablename__ = 'LuceneFile'

    Instance = Column(Integer, primary_key=True, nullable=False)
    Name = Column(Unicode(256), primary_key=True, nullable=False)
    Length = Column(Integer, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    ReaderCount = Column(Integer, nullable=False)
    Data = Column(LargeBinary, nullable=False)


class Member(Base):
    __tablename__ = 'Member'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
    GroupId = Column(ForeignKey(u'UserGroup.Id'), nullable=False)
    Principal = Column(Unicode(256), nullable=False)

    UserGroup = relationship(u'UserGroup')


class NodeReference(Base):
    __tablename__ = 'NodeReference'
    __table_args__ = (
        ForeignKeyConstraint(['CompoundSourceRegion_Id', 'Source_Item_Id'], [u'CompoundSourceRegion.Id', u'CompoundSourceRegion.Item_Id']),
        Index('NodeReference_Index2', 'Node_Item_Id', 'ReferenceTypeId'),
        Index('NodeReference_Index1', 'Source_Item_Id', 'ReferenceTypeId')
    )

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, nullable=False)
    Node_Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False)
    Source_Item_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False)
    CompoundSourceRegion_Id = Column(UNIQUEIDENTIFIER)
    ReferenceTypeId = Column(Integer, nullable=False)
    StartZ = Column(Integer)
    StartX = Column(Integer, nullable=False)
    LengthX = Column(Integer, nullable=False)
    EndX = Column(Integer)
    OfX = Column(Integer)
    StartY = Column(Integer)
    LengthY = Column(Integer)
    EndY = Column(Integer)
    ClusterId = Column(Integer)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
    ModifiedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)

    CompoundSourceRegion = relationship(u'CompoundSourceRegion')
    UserProfile = relationship(u'UserProfile', primaryjoin='NodeReference.CreatedBy == UserProfile.Id')
    UserProfile1 = relationship(u'UserProfile', primaryjoin='NodeReference.ModifiedBy == UserProfile.Id')
    Item = relationship(u'Item', primaryjoin='NodeReference.Node_Item_Id == Item.Id')
    Item1 = relationship(u'Item', primaryjoin='NodeReference.Source_Item_Id == Item.Id')


class Project(Base):
    __tablename__ = 'Project'

    Title = Column(Unicode(256), nullable=False)
    Description = Column(Unicode(512), nullable=False)
    CreatedDate = Column(DateTime, nullable=False)
    ModifiedDate = Column(DateTime, nullable=False)
    CreatedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
    ModifiedBy = Column(ForeignKey(u'UserProfile.Id'), nullable=False)
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
    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
    AllowGuestAccess = Column(BIT, nullable=False)
    EventLogging = Column(BIT, nullable=False)
    UserColors = Column(BIT, nullable=False, server_default=text("((0))"))
    StopWords = Column(Unicode)
    DateOfBirth = Column(BIT, nullable=False, server_default=text("((1))"))
    Gender = Column(BIT, nullable=False, server_default=text("((1))"))
    Religion = Column(BIT, nullable=False, server_default=text("((1))"))
    Location = Column(BIT, nullable=False, server_default=text("((1))"))
    RelationshipStatus = Column(BIT, nullable=False, server_default=text("((1))"))
    Bio = Column(BIT, nullable=False, server_default=text("((1))"))
    Web = Column(BIT, nullable=False, server_default=text("((1))"))

    UserProfile = relationship(u'UserProfile', primaryjoin='Project.CreatedBy == UserProfile.Id')
    UserProfile1 = relationship(u'UserProfile', primaryjoin='Project.ModifiedBy == UserProfile.Id')


class ReportSearchFolderItem(Base):
    __tablename__ = 'ReportSearchFolderItems'

    Id = Column(Integer, primary_key=True)
    SearchFolderId = Column(UNIQUEIDENTIFIER, nullable=False)
    ProjectItemId = Column(UNIQUEIDENTIFIER, nullable=False)


class Role(Base):
    __tablename__ = 'Role'
    __table_args__ = (
        Index('Role_Index1', 'TypeId', 'Item2_Id'),
    )

    Item1_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False)
    TypeId = Column(Integer, primary_key=True, nullable=False)
    Item2_Id = Column(ForeignKey(u'Item.Id'), primary_key=True, nullable=False)
    Tag = Column(Integer)

    Item = relationship(u'Item', primaryjoin='Role.Item1_Id == Item.Id')
    Item1 = relationship(u'Item', primaryjoin='Role.Item2_Id == Item.Id')


class Style(Base):
    __tablename__ = 'Style'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
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


class UserGroup(Base):
    __tablename__ = 'UserGroup'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
    Name = Column(Unicode(256), nullable=False)
    Description = Column(Unicode(256))


class UserProfile(Base):
    __tablename__ = 'UserProfile'

    Id = Column(UNIQUEIDENTIFIER, primary_key=True)
    Initials = Column(Unicode(16), nullable=False)
    Name = Column(Unicode(64), nullable=False)
    AccountName = Column(Unicode(256))
    ColorArgb = Column(Integer)


t_viewNodeCategoriesLDM_Attribute = Table(
    'viewNodeCategoriesLDM_Attribute', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('DataType', Unicode),
    Column('CustomOrder', Integer),
    Column('Category', UNIQUEIDENTIFIER, nullable=False)
)


t_viewNodeCategoriesLDM_AttributeValue = Table(
    'viewNodeCategoriesLDM_AttributeValue', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('AttributeName', Unicode(256), nullable=False),
    Column('AttributeValue', Unicode(256), nullable=False),
    Column('AttributeValueDescription', Unicode(512), nullable=False),
    Column('UserAssignedColor', Unicode(256)),
    Column('CustomOrder', Integer),
    Column('Attribute', UNIQUEIDENTIFIER, nullable=False),
    Column('IsDefaultValue', Unicode(32)),
    Column('DataType', Unicode),
    Column('Node', UNIQUEIDENTIFIER)
)


t_viewNodeCategoriesLDM_Category = Table(
    'viewNodeCategoriesLDM_Category', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('CreatedByInitials', Unicode(16), nullable=False),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16), nullable=False),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False)
)


t_viewNodeCategoriesLDM_Node = Table(
    'viewNodeCategoriesLDM_Node', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('FolderLocation', Unicode),
    Column('Nickname', Unicode(64)),
    Column('Aggregate', Unicode),
    Column('UserAssignedColor', Unicode(256)),
    Column('CreatedByInitials', Unicode(16)),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('ParentNodeName', Unicode),
    Column('HierarchicalName', Unicode),
    Column('ListLevel', Integer),
    Column('ListOrder', Integer),
    Column('Icon', LargeBinary),
    Column('Category', UNIQUEIDENTIFIER),
    Column('Attribute', UNIQUEIDENTIFIER)
)


t_viewNodeLDM_AttributeValue = Table(
    'viewNodeLDM_AttributeValue', metadata,
    Column('Id', UNIQUEIDENTIFIER),
    Column('Category', Unicode(256)),
    Column('AttributeName', Unicode(256)),
    Column('AttributeDescription', Unicode(512)),
    Column('AttributeCustomOrder', Integer),
    Column('DataType', Unicode),
    Column('AttributeValue', Unicode(256)),
    Column('AttributeValueDescription', Unicode(512)),
    Column('AttributeValueCustomOrder', Integer),
    Column('AttributeValueUserAssignedColor', Unicode(256)),
    Column('Attribute', UNIQUEIDENTIFIER),
    Column('Node', UNIQUEIDENTIFIER, nullable=False)
)


t_viewNodeLDM_Category = Table(
    'viewNodeLDM_Category', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False)
)


t_viewNodeLDM_CodingReference = Table(
    'viewNodeLDM_CodingReference', metadata,
    Column('Id', UNIQUEIDENTIFIER),
    Column('CodedText', Unicode),
    Column('ReferenceNumber', BigInteger),
    Column('ModifiedOn', DateTime),
    Column('CodedByInitials', Unicode(16), nullable=False),
    Column('CodedByUsername', Unicode(256)),
    Column('PercentCoverageOfSource', Numeric(5, 4)),
    Column('PercentCoverageOfNode', Numeric(5, 4)),
    Column('Words', Integer, nullable=False),
    Column('Paragraphs', Integer, nullable=False),
    Column('RegionProportion', Numeric(5, 4)),
    Column('Duration', Integer, nullable=False),
    Column('Source', UNIQUEIDENTIFIER),
    Column('Node', UNIQUEIDENTIFIER, nullable=False)
)


t_viewNodeLDM_Collection = Table(
    'viewNodeLDM_Collection', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256)),
    Column('Description', Unicode(512)),
    Column('CollectionType', Unicode),
    Column('Node', UNIQUEIDENTIFIER, nullable=False)
)


t_viewNodeLDM_IntersectingNode = Table(
    'viewNodeLDM_IntersectingNode', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('NodeType', Unicode),
    Column('FolderLocation', Unicode),
    Column('Aggregate', Unicode),
    Column('HierarchicalName', Unicode),
    Column('ListLevel', Integer),
    Column('ListOrder', Integer),
    Column('Node', UNIQUEIDENTIFIER, nullable=False),
    Column('ClassifiedAsCategory', UNIQUEIDENTIFIER)
)


t_viewNodeLDM_Node = Table(
    'viewNodeLDM_Node', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('NodeType', Unicode),
    Column('FolderLocation', Unicode),
    Column('Nickname', Unicode(64)),
    Column('UserAssignedColor', Unicode(256)),
    Column('Aggregate', Unicode),
    Column('CreatedByUser', UNIQUEIDENTIFIER),
    Column('CreatedByInitials', Unicode(16)),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByUser', UNIQUEIDENTIFIER),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('LinkedMemoName', Unicode),
    Column('NumberOfChildren', Integer, nullable=False),
    Column('NumberOfSourcesCoded', Integer, nullable=False),
    Column('NumberOfCodingReferences', Integer, nullable=False),
    Column('NumberOfUsersCoding', Integer, nullable=False),
    Column('RelationshipType', Unicode(256)),
    Column('RelationshipItem1', Unicode),
    Column('RelationshipItem2', Unicode),
    Column('ParentNodeName', Unicode),
    Column('HierarchicalName', Unicode),
    Column('ListLevel', Integer),
    Column('ListOrder', Integer),
    Column('ClassifiedAsCategory', UNIQUEIDENTIFIER),
    Column('Icon', LargeBinary)
)


t_viewNodeLDM_Source = Table(
    'viewNodeLDM_Source', metadata,
    Column('Id', UNIQUEIDENTIFIER),
    Column('Name', Unicode(256)),
    Column('Description', Unicode(512)),
    Column('FolderLocation', Unicode),
    Column('SourceType', Unicode),
    Column('Category', Unicode(256)),
    Column('UserAssignedColor', Unicode(256)),
    Column('CreatedByInitials', Unicode(16)),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime),
    Column('NumberOfUsersCoding', Integer, nullable=False),
    Column('NumberOfCodingReferences', Integer, nullable=False),
    Column('Coverage', Numeric(5, 4)),
    Column('Thumbnail', LargeBinary),
    Column('Icon', LargeBinary),
    Column('Words', Integer, nullable=False),
    Column('Paragraphs', Integer, nullable=False),
    Column('Duration', Integer),
    Column('ImageWidth', Integer),
    Column('ImageHeight', Integer),
    Column('HierarchicalName', Unicode),
    Column('CodedWords', Integer, nullable=False),
    Column('CodedParagraphs', Integer, nullable=False),
    Column('CodedDuration', Integer, nullable=False),
    Column('CodedImageProportion', Numeric(5, 4)),
    Column('Node', UNIQUEIDENTIFIER, nullable=False)
)


t_viewNodeLDM_User = Table(
    'viewNodeLDM_User', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256)),
    Column('Initials', Unicode(16), nullable=False),
    Column('CreatedOn', DateTime, nullable=False),
    Column('PermissionType', Unicode(256))
)


t_viewProjectItemsLDM_Collection = Table(
    'viewProjectItemsLDM_Collection', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('CollectionType', Unicode),
    Column('CreatedByInitials', Unicode(16), nullable=False),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16), nullable=False),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('Project', UNIQUEIDENTIFIER)
)


t_viewProjectItemsLDM_Folder = Table(
    'viewProjectItemsLDM_Folder', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('HierarchicalName', Unicode),
    Column('RootSystemFolder', Unicode),
    Column('FolderType', Unicode),
    Column('ProjectOrder', Integer, nullable=False),
    Column('CreatedByInitials', Unicode(16), nullable=False),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('Project', UNIQUEIDENTIFIER)
)


t_viewProjectItemsLDM_Project = Table(
    'viewProjectItemsLDM_Project', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('ProjectTitle', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('FileLocation', Unicode(256)),
    Column('CreatedByInitials', Unicode(16), nullable=False),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16), nullable=False),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False)
)


t_viewProjectItemsLDM_ProjectItem = Table(
    'viewProjectItemsLDM_ProjectItem', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('ItemType', Unicode),
    Column('FolderLocation', Unicode),
    Column('UserAssignedColor', Unicode(256)),
    Column('CreatedByInitials', Unicode(16)),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('ParentItem', Unicode),
    Column('HierarchicalName', Unicode),
    Column('Thumbnail', LargeBinary),
    Column('Icon', LargeBinary),
    Column('ListLevel', Integer),
    Column('ListOrder', Integer),
    Column('Folder', UNIQUEIDENTIFIER, nullable=False),
    Column('Collection', UNIQUEIDENTIFIER)
)


t_viewProjectItemsLDM_User = Table(
    'viewProjectItemsLDM_User', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('Initials', Unicode(16), nullable=False),
    Column('PermissionType', Unicode(256)),
    Column('Project', UNIQUEIDENTIFIER)
)


t_viewSourceCategoriesLDM_Attribute = Table(
    'viewSourceCategoriesLDM_Attribute', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('DataType', Unicode),
    Column('EndNoteFieldId', Unicode(64)),
    Column('CustomOrder', Integer),
    Column('Category', UNIQUEIDENTIFIER, nullable=False)
)


t_viewSourceCategoriesLDM_AttributeValue = Table(
    'viewSourceCategoriesLDM_AttributeValue', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('AttributeName', Unicode(256), nullable=False),
    Column('AttributeValue', Unicode(256), nullable=False),
    Column('AttributeValueDescription', Unicode(512), nullable=False),
    Column('UserAssignedColor', Unicode(256)),
    Column('CustomOrder', Integer),
    Column('IsDefaultValue', Unicode(32)),
    Column('DataType', Unicode),
    Column('Attribute', UNIQUEIDENTIFIER, nullable=False),
    Column('Source', UNIQUEIDENTIFIER)
)


t_viewSourceCategoriesLDM_Category = Table(
    'viewSourceCategoriesLDM_Category', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('EndNoteReferenceType', Unicode(32)),
    Column('CreatedByInitials', Unicode(16), nullable=False),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16), nullable=False),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False)
)


t_viewSourceCategoriesLDM_Source = Table(
    'viewSourceCategoriesLDM_Source', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('FolderLocation', Unicode),
    Column('SourceType', Unicode),
    Column('UserAssignedColor', Unicode(256)),
    Column('Thumbnail', LargeBinary),
    Column('CreatedByInitials', Unicode(16)),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('HierarchicalName', Unicode),
    Column('Icon', LargeBinary),
    Column('Category', UNIQUEIDENTIFIER),
    Column('Attribute', UNIQUEIDENTIFIER)
)


t_viewSourceLDM_AttributeValue = Table(
    'viewSourceLDM_AttributeValue', metadata,
    Column('Id', UNIQUEIDENTIFIER),
    Column('Category', Unicode(256)),
    Column('AttributeName', Unicode(256)),
    Column('AttributeDescription', Unicode(512)),
    Column('EndNoteFieldId', Unicode(64)),
    Column('AttributeCustomOrder', Integer),
    Column('DataType', Unicode),
    Column('AttributeValue', Unicode(256)),
    Column('AttributeValueDescription', Unicode(512)),
    Column('AttributeValueCustomOrder', Integer),
    Column('AttributeValueUserAssignedColor', Unicode(256)),
    Column('Attribute', UNIQUEIDENTIFIER),
    Column('Source', UNIQUEIDENTIFIER, nullable=False)
)


t_viewSourceLDM_Category = Table(
    'viewSourceLDM_Category', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('EndNoteReferenceType', Unicode(32))
)


t_viewSourceLDM_CodingReference = Table(
    'viewSourceLDM_CodingReference', metadata,
    Column('Id', UNIQUEIDENTIFIER),
    Column('CodedText', Unicode),
    Column('ReferenceNumber', BigInteger),
    Column('ModifiedOn', DateTime),
    Column('CodedByInitials', Unicode(16), nullable=False),
    Column('CodedByUsername', Unicode(256)),
    Column('PercentCoverageOfSource', Numeric(5, 4)),
    Column('PercentCoverageOfNode', Numeric(5, 4)),
    Column('Words', Integer, nullable=False),
    Column('Paragraphs', Integer, nullable=False),
    Column('RegionProportion', Numeric(5, 4)),
    Column('Duration', Integer, nullable=False),
    Column('Node', UNIQUEIDENTIFIER, nullable=False),
    Column('Source', UNIQUEIDENTIFIER)
)


t_viewSourceLDM_Collection = Table(
    'viewSourceLDM_Collection', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256)),
    Column('Description', Unicode(512)),
    Column('CollectionType', Unicode),
    Column('Source', UNIQUEIDENTIFIER, nullable=False)
)


t_viewSourceLDM_Node = Table(
    'viewSourceLDM_Node', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('NodeType', Unicode),
    Column('FolderLocation', Unicode),
    Column('Nickname', Unicode(64)),
    Column('Category', Unicode(256)),
    Column('Aggregate', Unicode),
    Column('UserAssignedColor', Unicode(256)),
    Column('CreatedByInitials', Unicode(16)),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('NumberOfChildren', Integer, nullable=False),
    Column('NumberOfUsersCoding', Integer, nullable=False),
    Column('NumberOfCodingReferences', Integer, nullable=False),
    Column('Coverage', Numeric(5, 4)),
    Column('RelationshipType', Unicode(256)),
    Column('RelationshipItem1', Unicode),
    Column('RelationshipItem2', Unicode),
    Column('ParentNodeName', Unicode),
    Column('HierarchicalName', Unicode),
    Column('ListLevel', Integer),
    Column('ListOrder', Integer),
    Column('Icon', LargeBinary),
    Column('Source', UNIQUEIDENTIFIER)
)


t_viewSourceLDM_Source = Table(
    'viewSourceLDM_Source', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256), nullable=False),
    Column('Description', Unicode(512), nullable=False),
    Column('FolderLocation', Unicode),
    Column('FileSize', BigInteger),
    Column('SourceType', Unicode),
    Column('UserAssignedColor', Unicode(256)),
    Column('CreatedByUser', UNIQUEIDENTIFIER),
    Column('CreatedByInitials', Unicode(16)),
    Column('CreatedByUsername', Unicode(256)),
    Column('CreatedOn', DateTime, nullable=False),
    Column('ModifiedByUser', UNIQUEIDENTIFIER),
    Column('ModifiedByInitials', Unicode(16)),
    Column('ModifiedByUsername', Unicode(256)),
    Column('ModifiedOn', DateTime, nullable=False),
    Column('NumberOfCodingReferences', Integer, nullable=False),
    Column('NumberOfNodesCoding', Integer, nullable=False),
    Column('HasEmbeddedMediaFile', Unicode),
    Column('MemoLinkedItem', Unicode),
    Column('MediaFileLocation', Unicode),
    Column('NumberOfUsersCoding', Integer, nullable=False),
    Column('Coverage', Numeric(5, 4)),
    Column('Thumbnail', LargeBinary),
    Column('Icon', LargeBinary),
    Column('MediaFileFormat', String(3, u'Latin1_General_BIN')),
    Column('Words', Integer, nullable=False),
    Column('Paragraphs', Integer, nullable=False),
    Column('Duration', Integer),
    Column('ImageWidth', Integer),
    Column('ImageHeight', Integer),
    Column('TranscriptLogEntries', Integer, nullable=False),
    Column('DatasetCodableCells', Integer, nullable=False),
    Column('DatasetRowsContainingData', Integer, nullable=False),
    Column('ExternalsType', String(9, u'Latin1_General_BIN')),
    Column('ExternalsFilepath', Unicode),
    Column('ExternalsURL', Unicode),
    Column('ExternalsLocation', Unicode),
    Column('HierarchicalName', Unicode),
    Column('ClassifiedAsCategory', UNIQUEIDENTIFIER)
)


t_viewSourceLDM_User = Table(
    'viewSourceLDM_User', metadata,
    Column('Id', UNIQUEIDENTIFIER, nullable=False),
    Column('Name', Unicode(256)),
    Column('Initials', Unicode(16), nullable=False),
    Column('CreatedOn', DateTime, nullable=False),
    Column('PermissionType', Unicode(256))
)
