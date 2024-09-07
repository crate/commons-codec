import typing as t

from attrs import define

from zyp.model.base import Dumpable, Metadata, SchemaDefinition
from zyp.model.bucket import BucketTransformation, Collection, DictOrList
from zyp.model.moksha import MokshaTransformation


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
        collection_out = collection
        if self.pre:
            collection = t.cast(Collection, self.pre.apply(collection))
        if self.bucket:
            collection_out = []
            for item in collection:
                item = self.bucket.apply(item)
                collection_out.append(item)
        if self.post:
            collection_out = t.cast(Collection, self.post.apply(collection_out))
        return collection_out
