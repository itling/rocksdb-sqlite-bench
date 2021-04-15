use cid::{Cid, Codec};
use criterion::{criterion_group, criterion_main, Criterion};
use multihash::Sha2_256;
use rand::Rng;
use rocksdb::{ColumnFamilyOptions, DBCompressionType, DBOptions};
use rocksdb::{Writable, DB};

fn monotonic_crud(c: &mut Criterion) {

      // 数据路径
      let data_path = "/data/rocksdb_monotonic_crud";

      // 单位byte
      let data_item_size  = 1024 * 1024_u32;
  
      // 总大小=dataItemSize*totalLoopCount
      // >1000
      let init_loop_count  = 100000;

    let mut group = c.benchmark_group("rocksdb_monotonic_crud");

    let mut opts = DBOptions::new();
    opts.create_if_missing(true);

    //不压缩
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.compression_per_level(&[
        DBCompressionType::Snappy,
        DBCompressionType::Zlib,
        DBCompressionType::Bz2,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4hc,
        DBCompressionType::Zstd,
    ]);
    let db = DB::open_cf(opts, data_path, vec![("default", cf_opts)]).unwrap();

    let bytes = |len| -> Vec<u8> { (0..len).map(|_| rand::random::<u8>()).collect() };

    let max_id = db.get(b"max_id").unwrap();
    let mut init_count = 0_u32;
    if let Some(id) = max_id {
        let mut init_array: [u8; 4] = Default::default();
        init_array.copy_from_slice(&*id);
        init_count = u32::from_be_bytes(init_array);
        println!("get max id num={:?}", init_count);
    }

    //调整循环次数,1条数据等于1m
    if init_count < init_loop_count {
        for _i in 0..init_loop_count {
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&init_count.to_be_bytes().to_vec()),
            );
            db.put(&cid.to_bytes().to_vec(), &bytes(data_item_size))
                .unwrap();
            init_count += 1;
            if init_count % 1000 == 0 {
                println!("init  record {:?}", init_count);
            }
        }
        println!("init  data ,max count={:?}", init_count);
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
            db.put(&cid.to_bytes().to_vec(), &bytes(data_item_size))
                .unwrap();
            insert_count += 1;
        })
    });
    println!("insert max count={:?}", insert_count);
    let _ = db
        .put(b"max_id", &insert_count.to_be_bytes().to_vec())
        .unwrap();

    group.bench_function("monotonic gets", |b| {
        b.iter(|| {
            let mut rng =rand::thread_rng();
            let get_count:u32=rng.gen_range(0..insert_count);
            let cid = Cid::new_v1(
                Codec::Raw,
                Sha2_256::digest(&get_count.to_be_bytes().to_vec()),
            );
            db.get(&cid.to_bytes().to_vec()).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, monotonic_crud,);
criterion_main!(benches);
