use criterion::{criterion_group, criterion_main, Criterion};
use rocksdb::{DB, Writable};
use cid::{Cid,Codec};
use multihash::Sha2_256;

fn monotonic_crud(c: &mut Criterion) {
    let mut group = c.benchmark_group("rocksdb_monotonic_crud");

    let db = DB::open_default("/data/rocksdb_rs_crud").unwrap();

    let mut bytes_count = 0_u32;
    let mut bytes = |len| -> Vec<u8> {
        bytes_count += 1;
        bytes_count.to_be_bytes().iter().cycle().take(len).copied().collect()
    };

    let max_id=db.get(b"max_id").unwrap();
    let mut init_count=0_u32;
    if let Some(id)= max_id{
        let mut init_array: [u8; 4] = Default::default();
        init_array.copy_from_slice(&*id);
        init_count= u32::from_be_bytes(init_array)+1;
        println!("get max id num={:?}", init_count);
   }
  
 
    if init_count<2000000{ 
        for _i in 0..2000000{ 
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&init_count.to_be_bytes().to_vec()));
            db.put(&cid.to_bytes().to_vec(), &bytes(1024*1024)).unwrap();
            init_count+=1;
            if init_count%1000==0{
                println!("init  record {:?}", init_count);
            }
        }
    }

    //data size=1m
    let mut insert_count = init_count;
    group.bench_function("monotonic inserts", |b| {
        b.iter(|| {
            insert_count += 1;
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&insert_count.to_be_bytes().to_vec()));
            db.put(&cid.to_bytes().to_vec(), &bytes(1024*1024)).unwrap();
        })
    });
    println!("insert max id={:?}", insert_count);
    let _=db.put(b"max_id",&insert_count.to_be_bytes().to_vec()).unwrap();

    let mut get_count=insert_count;
    group.bench_function("monotonic gets", |b| {
        b.iter(|| {
            get_count -= 1;
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&get_count.to_be_bytes().to_vec()));
            db.get(&cid.to_bytes().to_vec()).unwrap();
        })
    });

    let mut remove_count=insert_count;
    println!("removals max id={:?}", remove_count);
    group.bench_function("monotonic removals", |b| {
        b.iter(|| {
            remove_count -= 1;
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&remove_count.to_be_bytes().to_vec()));
            db.delete(&cid.to_bytes().to_vec()).unwrap();
        })
    });
    println!("removals max id={:?}", remove_count);

    group.finish();
}


criterion_group!(
    benches,
    monotonic_crud,
);
criterion_main!(benches);
