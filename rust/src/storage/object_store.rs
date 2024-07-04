use std::hash::{Hash, Hasher};

use musli::{
    de::DecodeOwned,
    mode::Binary,
    options::{self, ByteOrder, Integer},
    storage::Encoding,
    Encode, Options,
};

use super::*;

const OPTIONS: Options = options::new()
    .with_integer(Integer::Fixed)
    .with_byte_order(ByteOrder::NATIVE)
    .build();

const ENCODING: Encoding<OPTIONS> = Encoding::new().with_options();

pub type ObjectRef = u64;

const OBJECT_TABLE_NAME: &str = "object";

pub async fn create_tables(store: &mut Store) -> Result<()> {
    let object_table_name = store.table_name(OBJECT_TABLE_NAME);
    store
        .query(
            format!("CREATE TABLE IF NOT EXISTS {object_table_name} (id TEXT, object BLOB, PRIMARY KEY (id))"),
            &[],
        )
        .await?;
    Ok(())
}

#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    async fn resolve<T: Entity + Hash + DecodeOwned<Binary>>(&mut self, id: ObjectRef)
        -> Result<T>;
    async fn insert<T: Entity + Hash + Encode<Binary>>(&mut self, object: &T) -> Result<ObjectRef>;
}

impl ObjectStore for Store {
    async fn resolve<T: Entity + Hash + DecodeOwned<Binary>>(
        &mut self,
        id: ObjectRef,
    ) -> Result<T> {
        let object_table = self.table_name(OBJECT_TABLE_NAME);

        let object_blob = self
            .query(
                format!("SELECT object FROM {object_table} WHERE id = ?"),
                (id.to_string(),),
            )
            .await?
            .single_row()
            .with_context(|| "Failed to select single row")?
            .columns[0]
            .as_ref()
            .and_then(|value| value.clone().into_blob())
            .with_context(|| "Failed to deserialize value")?;

        ENCODING
            .decode(object_blob.as_slice())
            .with_context(|| "Failed to deserialize object")
    }

    async fn insert<T: Entity + Hash + Encode<Binary>>(&mut self, object: &T) -> Result<ObjectRef> {
        let object_table = self.table_name(OBJECT_TABLE_NAME);

        let mut data = Vec::new();
        ENCODING
            .encode(&mut data, object)
            .with_context(|| "Failed to serialize object")?;

        let mut hasher = std::hash::DefaultHasher::new();
        object.hash(&mut hasher);
        let hash = hasher.finish();

        self.query(
            format!("INSERT INTO {object_table} (id, object) VALUES (?, ?)"),
            (hash.to_string().as_str(), data),
        )
        .await?;

        Ok(hash)
    }
}
