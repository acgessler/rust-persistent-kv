use std::{ hint::black_box, sync::Arc };
use criterion::{ criterion_group, criterion_main, Criterion };
use persistent_kv::{ Config, PersistentKeyValueStore };
use tempfile::TempDir;

fn random_key_writes<KeyType, F>(n: u64, threads: usize, key: F)
    where
        KeyType: persistent_kv::SerializableKey + 'static,
        F: Sync + Send + Copy + 'static + Fn(u64) -> KeyType
{
    let tmp_dir = TempDir::new().unwrap();
    let store: Arc<PersistentKeyValueStore<KeyType, String>> = Arc::new(
        PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap()
    );

    let mut join_handles = Vec::new();
    for _ in 0..threads {
        let store_ref = store.clone();
        join_handles.push(
            std::thread::spawn(move || {
                for i in 0..n {
                    store_ref.set(black_box(key(i)), "bar");
                }
            })
        );
    }
    for jh in join_handles {
        jh.join().unwrap();
    }
}

fn writes1(c: &mut Criterion) {
    c.bench_function("100 random key writes from 1 thread with string keys", |b|
        b.iter(|| random_key_writes(black_box(100), 1, |i| i.to_string()))
    );
    c.bench_function("100 random key writes from 1 thread with int keys", |b|
        b.iter(|| random_key_writes(black_box(100), 1, |i| i as i32))
    );
}

fn writes4(c: &mut Criterion) {
    c.bench_function("25 random key writes from 4 threads with string keys", |b|
        b.iter(|| random_key_writes(black_box(25), 8, |i| i.to_string()))
    );
    c.bench_function("25 random key writes from 4 threads with int keys", |b|
        b.iter(|| random_key_writes(black_box(25), 8, |i| i as i32))
    );
}

criterion_group! {
    name = writes;
    config = Criterion::default().sample_size(10);
    targets = writes1, writes4
}
criterion_main!(writes);
