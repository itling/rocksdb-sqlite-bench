use cid::{Cid, Codec};
use criterion::{criterion_group, criterion_main, Criterion};
use multihash::Sha2_256;
use rusqlite::{params, Connection, NO_PARAMS,Error};
use rand::{thread_rng, Rng};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub id: u32,
    /// Key of the record.
    pub key: Vec<u8>,
    /// Value of the record.
    pub value: Vec<u8>,
}

fn sqlite_bulk_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_monotonic_crud");

    // let temp_dir = tempfile::tempdir().unwrap();
    // let path = temp_dir.path().join("test_insert.db");

    let mut conn = Connection::open("sqlite_bulk_load.db").unwrap();

    //create table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS  record (
                  id              INTEGER primary key,
                  key             BLOB NOT NULL,
                  value           BLOB NOT NULL
                  )",
        NO_PARAMS,
    )
    .unwrap();

    // create index for id
    conn.execute(
        "CREATE index  IF NOT EXISTS index_record_id on record(id)",
        NO_PARAMS,
    )
    .unwrap();

    // create index for key
    conn.execute(
        "CREATE index  IF NOT EXISTS index_record_key on record(key)",
        NO_PARAMS,
    )
    .unwrap();

    let mut init_count = 0_u32;
    let max_id_r = conn.query_row("SELECT max(id) FROM record", NO_PARAMS, |row| row.get(0));
    if max_id_r.is_ok() {
        let max_id: u32 = max_id_r.unwrap();
        init_count = max_id + 1;
    }
    println!("init_count id={}", init_count);

    let bytes = |len| -> Vec<u8> { (0..len).map(|_| rand::random::<u8>()).collect() };


    //调整循环次数,1条数据等于1m 1000条提交一次
    if init_count < 100000 {
        for _ in 0..100 {
            let tx = conn.transaction().unwrap();
            for _ in 0..1000 {
                let cid = Cid::new_v1(
                    Codec::Raw,
                    Sha2_256::digest(&init_count.to_be_bytes().to_vec()),
                );
                let record = Record {
                    id: init_count,
                    key: cid.to_bytes().to_vec(),
                    value: bytes(1024*100),
                };
                tx.execute(
                    "INSERT INTO record (id,key, value) VALUES (?1, ?2,?3)",
                    params![record.id, record.key, record.value],
                )
                .unwrap();
                init_count = init_count + 1;
            }
            if init_count % 10000 == 0 {
                println!("init  record {:?}", init_count);
            }
            let _ = tx.commit();
        }
    }

    group.bench_function("cid ", |b| {
        b.iter(|| {
            let a=0_u32;
            Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&a.to_be_bytes().to_vec()),
            )
        })
    });

    group.bench_function("gen random", |b| {
        b.iter(|| {
            bytes(1024 * 100);
        })
    });

    let mut insert_count = init_count;
    group.bench_function("monotonic insert", |b| {
        b.iter(|| {
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&insert_count.to_be_bytes().to_vec()),
            );
            let record = Record {
                id: insert_count,
                key: cid.to_bytes().to_vec(),
                value: bytes(1024*100),
            };
            conn.execute(
                "INSERT INTO record (id,key, value) VALUES (?1, ?2,?3)",
                params![record.id, record.key, record.value],
            )
            .unwrap();

            insert_count = insert_count + 1;
        })
    });

    group.bench_function("monotonic query", |b| {
        b.iter(|| {
            let mut rng =rand::thread_rng();
            let get_count:u32=rng.gen_range(0..insert_count);
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&get_count.to_be_bytes().to_vec()),
            );
            let query_r:Result<Vec<u8>,Error> = conn
                .query_row(
                    "SELECT * FROM record where key = ?1 ",
                    params![cid.to_bytes().to_vec()],
                    |row| row.get(2),
                );
            assert_eq!(query_r.is_ok(),true);
        })
    });
    group.finish();
}

criterion_group!(benches, sqlite_bulk_load,);

criterion_main!(benches);
