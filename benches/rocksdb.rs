use cid::{Cid, Codec};
use criterion::{criterion_group, criterion_main, Criterion};
use multihash::Sha2_256;
use rocksdb::{Writable, DB};

fn monotonic_crud(c: &mut Criterion) {
    let mut group = c.benchmark_group("rocksdb_monotonic_crud");

    let db = DB::open_default("rocksdb_rs_crud").unwrap();

    let mut bytes_count = 0_u32;
    let mut bytes = |len| -> Vec<u8> {
        bytes_count += 1;
        bytes_count
            .to_be_bytes()
            .iter()
            .cycle()
            .take(len)
            .copied()
            .collect()
    };

    let max_id = db.get(b"max_id").unwrap();
    let mut init_count = 0_u32;
    if let Some(id) = max_id {
        let mut init_array: [u8; 4] = Default::default();
        init_array.copy_from_slice(&*id);
        init_count = u32::from_be_bytes(init_array);
        println!("get max id num={:?}", init_count);
    }

    //调整循环次数,1条数据等于1m
    if init_count < 100000 {
        for _i in 0..100000 {
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&init_count.to_be_bytes().to_vec()),
            );
            db.put(&cid.to_bytes().to_vec(), &bytes(1024 * 1024))
                .unwrap();
            init_count += 1;
            if init_count % 1000 == 0 {
                println!("init  record {:?}", init_count);
            }
        }
        println!("insert max id={:?}", init_count);
        let _ = db
            .put(b"max_id", &init_count.to_be_bytes().to_vec())
            .unwrap();
    }

    let mut insert_count = init_count;
    group.bench_function("monotonic inserts", |b| {
        b.iter(|| {
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&insert_count.to_be_bytes().to_vec()),
            );
            db.put(&cid.to_bytes().to_vec(), &bytes(1024 * 1024))
                .unwrap();
            insert_count += 1;
        })
    });
    println!("insert max id={:?}", insert_count);
    let _ = db
        .put(b"max_id", &insert_count.to_be_bytes().to_vec())
        .unwrap();

    let mut get_count = insert_count;
    group.bench_function("monotonic gets", |b| {
        b.iter(|| {
            get_count -= 1;
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&get_count.to_be_bytes().to_vec()),
            );
            db.get(&cid.to_bytes().to_vec()).unwrap();
        })
    });

    let mut remove_count = insert_count;
    println!("removals max id={:?}", remove_count);
    group.bench_function("monotonic removals", |b| {
        b.iter(|| {
            remove_count -= 1;
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&remove_count.to_be_bytes().to_vec()),
            );
            db.delete(&cid.to_bytes().to_vec()).unwrap();
        })
    });
    group.finish();
}

criterion_group!(benches, monotonic_crud,);
criterion_main!(benches);
