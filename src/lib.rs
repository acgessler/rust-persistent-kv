use std::{collections::HashMap, hash::{DefaultHasher, Hash, Hasher}, sync::{Arc, Mutex}};



struct Bucket {
    data:  Arc<Mutex<HashMap<String, String>>>,
}

struct PersistentKeyValueStore {
    bucket_count: u32,
    data: Vec<Bucket>,
}

impl PersistentKeyValueStore {
    const DEFAULT_BUCKET_COUNT: u32 = 4;
    pub fn new(bucket_count: Option<u32>) -> Self {
        let bucket_count = bucket_count.unwrap_or(PersistentKeyValueStore::DEFAULT_BUCKET_COUNT);
        let mut data = Vec::with_capacity(bucket_count as usize);
        for _ in 0..bucket_count {
            data.push(Bucket {
                data: Mutex::new(HashMap::new()).into(),
            });
        }
        Self { bucket_count, data }
    }

    pub fn set(&self, key: impl Into<String>, value: impl Into<String>) {
        let key_string = key.into();
        let bucket = self.get_bucket(&key_string);
        let mut data = bucket.data.lock().unwrap();
        data.insert(key_string, value.into());
    }

    pub fn unset(&self, key: impl Into<String>) {
        let key_string= key.into();
        let bucket = self.get_bucket(&key_string);
        let mut data = bucket.data.lock().unwrap();
        data.remove(&key_string);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let bucket = self.get_bucket(key);
        bucket.data.lock().unwrap().get(key).cloned()
    }

    fn get_bucket(&self, key: &str) -> &Bucket {
        let hash = PersistentKeyValueStore::hash(key);
        let index = hash % (self.bucket_count as u64);
        &self.data[index as usize]
    }

    fn hash(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_setget() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        store.set("bar", "2");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        assert_eq!(store.get("bar"), Some(String::from("2")));
    }
    #[test]
    fn basic_get_nonexisting() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        assert_eq!(store.get("bar"), None);
    }
    #[test]
    fn basic_set_overwrite() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        store.set("bar", "2");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        assert_eq!(store.get("bar"), Some(String::from("2")));
        store.set("bar", "3");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        assert_eq!(store.get("bar"), Some(String::from("3")));
    }
    #[test]
    fn basic_unset() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        store.unset("foo");
        assert_eq!(store.get("foo"), None);
    }
}
