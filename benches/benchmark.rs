use std::hint::black_box;
use criterion::{ criterion_group, criterion_main, Criterion };
use persistent_kv::{ Config, PersistentKeyValueStore };
use tempfile::TempDir;

fn random_key_writes<KeyType>(n: u64, key: impl Fn(u64) -> KeyType)
    where KeyType: persistent_kv::SerializableKey
{
    let tmp_dir = TempDir::new().unwrap();
    let store: PersistentKeyValueStore<KeyType, String> = PersistentKeyValueStore::new(
        tmp_dir.path(),
        Config::default()
    ).unwrap();

    for i in 0..n {
        // Due to hashing, these will come out random-ish
        store.set(black_box(key(i)), "bar");
    }
}

fn writes1(c: &mut Criterion) {
    c.bench_function("100 random key writes from 1 thread with string keys", |b|
        b.iter(|| random_key_writes(black_box(100), |i| i.to_string()))
    );
    c.bench_function("100 random key writes from 1 thread with int keys", |b|
        b.iter(|| random_key_writes(black_box(100), |i| i as i32))
    );
}

criterion_group! {
    name = writes;
    config = Criterion::default().sample_size(10);
    targets = writes1
}
criterion_main!(writes);
