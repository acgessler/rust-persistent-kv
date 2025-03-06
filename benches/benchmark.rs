use criterion::{criterion_group, criterion_main, Criterion};
use persistent_kv::{Config, PersistentKeyValueStore};
use std::{hint::black_box, sync::Arc};
use tempfile::TempDir;

fn value_string(length: usize) -> String {
    "a".repeat(length)
}

fn random_key_writes<KeyType, F>(n: u64, threads: usize, key: F, value: String)
where
    KeyType: persistent_kv::SerializableKey + 'static,
    F: Sync + Send + Copy + 'static + Fn(u64) -> KeyType,
{
    let tmp_dir = TempDir::new().unwrap();
    let store: Arc<PersistentKeyValueStore<KeyType, String>> =
        Arc::new(PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap());

    let mut join_handles = Vec::new();
    for _ in 0..threads {
        let store_ref = store.clone();
        let value_clone = value.clone();
        join_handles.push(std::thread::spawn(move || {
            for i in 0..n {
                // Keys are repeated across threads to trigger contention
                store_ref.set(black_box(key(i)), value_clone.clone());
            }
        }));
    }
    for jh in join_handles {
        jh.join().unwrap();
    }
}

fn snapshot_and_restore_heavy_rw_load(
    iterations: u64,
    n: u64,
    r_w_ratio: u64,
    threads: u64,
    value: String,
) {
    let config = persistent_kv::Config {
        snapshot_interval: 25000,
        ..Default::default()
    };

    let tmp_dir = TempDir::new().unwrap();

    for k in 0..iterations {
        let store: Arc<PersistentKeyValueStore<u64, String>> =
            Arc::new(PersistentKeyValueStore::new(tmp_dir.path(), config.clone()).unwrap());

        let mut join_handles = Vec::new();
        for t in 0..threads {
            let store_ref = store.clone();
            let value_clone = value.clone();
            join_handles.push(std::thread::spawn(move || {
                for i in 0..n {
                    // Keys are not repeated to increase data size each iteration
                    let key = i + t * n + k * n * threads;
                    store_ref.set(key, value_clone.clone());
                    // Perform a large number of reads to trigger contention against other writes
                    for k in 0..r_w_ratio {
                        black_box(store_ref.get(&(key + k)));
                    }
                }
            }));
        }
        for jh in join_handles {
            jh.join().unwrap();
        }
        println!("{store:?}");
    }
}

fn writes_solo(c: &mut Criterion) {
    c.bench_function(
        "2000 random key writes from 1 thread with string keys",
        |b| {
            b.iter(|| {
                random_key_writes(
                    black_box(2000),
                    black_box(1),
                    |i| i.to_string(),
                    value_string(32),
                )
            })
        },
    );
    c.bench_function("2000 random key writes from 1 thread with int keys", |b| {
        b.iter(|| {
            random_key_writes(
                black_box(2000),
                black_box(1),
                |i| i as i32,
                value_string(32),
            )
        })
    });
}

fn writes_threaded(c: &mut Criterion) {
    c.bench_function(
        "250 random key writes from 8 threads with string keys",
        |b| {
            b.iter(|| {
                random_key_writes(
                    black_box(250),
                    black_box(8),
                    |i| i.to_string(),
                    value_string(32),
                )
            })
        },
    );
    c.bench_function("250 random key writes from 8 threads with int keys", |b| {
        b.iter(|| random_key_writes(black_box(250), black_box(8), |i| i as i32, value_string(32)))
    });
}

fn writes_big(c: &mut Criterion) {
    c.bench_function(
        "250 random key writes from 8 threads with very long string keys",
        |b| {
            b.iter(|| {
                random_key_writes(
                    black_box(250),
                    8,
                    |i| i.to_string() + &"a".repeat(10000),
                    value_string(32),
                )
            })
        },
    );
}

fn loadtest_snapshot_and_restore(c: &mut Criterion) {
    c.bench_function(
        "3 consecutive rounds of heavy writes and reads followed by restoring from a snapshot",
        |b| {
            b.iter(|| {
                snapshot_and_restore_heavy_rw_load(
                    black_box(3),
                    black_box(25000),
                    black_box(100),
                    black_box(8),
                    value_string(32),
                )
            })
        },
    );
}

criterion_group! {
    name = writes;
    config = Criterion::default();
    targets = writes_solo, writes_threaded, writes_big
}
criterion_group! {
    name = loadtest;
    config = Criterion::default().sample_size(10);
    targets = loadtest_snapshot_and_restore
}
criterion_main!(writes, loadtest);
