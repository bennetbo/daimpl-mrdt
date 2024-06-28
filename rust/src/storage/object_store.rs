use std::hash::{Hash, Hasher};

use super::*;

pub type ObjectRef = u64;

pub trait Object: Entity + Hash {}
impl<T: Entity + Hash> Object for T {}

const OBJECT_TABLE_NAME: &str = "object";

pub async fn create_tables(store: &mut Store) -> Result<()> {
    let object_table_name = store.table_name(OBJECT_TABLE_NAME);
    store
        .query(
            format!("CREATE TABLE IF NOT EXISTS {object_table_name} (id TEXT, object TEXT, PRIMARY KEY (id))"),
            &[],
        )
        .await?;
    Ok(())
}

#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    async fn resolve<T: Object>(&mut self, id: ObjectRef) -> Result<T>;
    async fn insert<T: Object>(&mut self, object: &T) -> Result<ObjectRef>;
}

impl ObjectStore for Store {
    async fn resolve<T: Object>(&mut self, id: ObjectRef) -> Result<T> {
        let object_table = self.table_name(OBJECT_TABLE_NAME);

        let object_text = self
            .query(
                format!("SELECT object FROM {object_table} WHERE id = ?"),
                (id.to_string(),),
            )
            .await?
            .single_row()
            .with_context(|| "Failed to select single row")?
            .columns[0]
            .as_ref()
            .and_then(|value| value.clone().into_string())
            .with_context(|| "Failed to deserialize value")?;

        serde_json::from_str(&object_text).with_context(|| "Failed to deserialize object")
    }

    async fn insert<T: Object>(&mut self, object: &T) -> Result<ObjectRef> {
        let object_table = self.table_name(OBJECT_TABLE_NAME);

        let object_text =
            serde_json::to_string(&object).with_context(|| "Failed to serialize object")?;

        let mut hasher = std::hash::DefaultHasher::new();
        object.hash(&mut hasher);
        let hash = hasher.finish();

        self.query(
            format!("INSERT INTO {object_table} (id, object) VALUES (?, ?)"),
            (hash.to_string().as_str(), object_text.as_str()),
        )
        .await?;

        Ok(hash)
    }
}
