import typing as t

from attrs import define

from zyp.model.base import Collection, DictOrList, Dumpable, Metadata, SchemaDefinition
from zyp.model.bucket import BucketTransformation
from zyp.model.moksha import MokshaTransformation
from zyp.model.treatment import Treatment


@define(frozen=True)
class CollectionAddress:
    container: str
    name: str


@define
class CollectionTransformation(Dumpable):
    meta: Metadata = Metadata(version=1, type="zyp-collection")
    address: t.Union[CollectionAddress, None] = None
    schema: t.Union[SchemaDefinition, None] = None
    pre: t.Union[MokshaTransformation, None] = None
    bucket: t.Union[BucketTransformation, None] = None
    post: t.Union[MokshaTransformation, None] = None
    treatment: t.Union[Treatment, None] = None

    def apply(self, data: DictOrList) -> Collection:
        collection = t.cast(Collection, data)
        if self.pre:
            collection = t.cast(Collection, self.pre.apply(collection))
        if self.bucket:
            collection_out = []
            for item in collection:
                item = self.bucket.apply(item)
                collection_out.append(item)
            collection = collection_out
        if self.post:
            collection = t.cast(Collection, self.post.apply(collection))
        if self.treatment:
            collection = t.cast(Collection, self.treatment.apply(collection))
        return collection
