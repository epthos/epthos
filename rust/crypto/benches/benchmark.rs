use std::vec;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use crypto::{model, RandomApi};

struct Cleartext {
    verified: model::VerifiedBlock,
    protected: model::ProtectedBlock,
}

fn gen_cleartext(rnd: &crypto::Random, file_id: &model::FileId, size: usize) -> Cleartext {
    let block_id = rnd.generate_block_id().unwrap();

    Cleartext {
        verified: model::VerifiedBlock {
            file_id: file_id.clone(),
            block_id,
        },
        protected: model::ProtectedBlock {
            chunk: Bytes::from(vec![0; size]),
            padding: vec![0, 1, 2],
        },
    }
}

fn gen_encrypted(
    rnd: &crypto::Random,
    key: &crypto::Keys,
    file_id: &model::FileId,
    size: usize,
) -> model::Block {
    let cleartext = gen_cleartext(rnd, file_id, size);
    key.encrypt_block(cleartext.verified, cleartext.protected)
        .unwrap()
}

pub fn encrypt(c: &mut Criterion) {
    let rnd = crypto::Random::new();
    let durable = rnd.generate_root_key().unwrap();
    let keys = crypto::Keys::new(durable);
    let file_id = rnd.generate_file_id().unwrap();

    let values = vec![
        gen_cleartext(&rnd, &file_id, 1024),
        gen_cleartext(&rnd, &file_id, 1024 * 1024),
        gen_cleartext(&rnd, &file_id, 4 * 1024 * 1024),
    ];

    let mut group = c.benchmark_group("block encrypt");
    for (i, attempt) in values.iter().enumerate() {
        group.throughput(Throughput::Elements(attempt.protected.chunk.len() as u64));
        group.bench_with_input(format!("Encode {}", i), attempt, |b, attempt| {
            b.iter(|| keys.encrypt_block(attempt.verified.clone(), attempt.protected.clone()))
        });
    }
    group.finish();
}

pub fn decrypt(c: &mut Criterion) {
    let rnd = crypto::Random::new();
    let durable = rnd.generate_root_key().unwrap();
    let keys = crypto::Keys::new(durable);
    let file_id = rnd.generate_file_id().unwrap();

    let values = [
        gen_encrypted(&rnd, &keys, &file_id, 1024),
        gen_encrypted(&rnd, &keys, &file_id, 1024 * 1024),
        gen_encrypted(&rnd, &keys, &file_id, 4 * 1024 * 1024),
    ];

    let mut group = c.benchmark_group("block decrypt");
    for (i, attempt) in values.iter().enumerate() {
        group.throughput(Throughput::Elements(
            (attempt.verified.len() + attempt.protected.len()) as u64,
        ));
        group.bench_with_input(format!("Encode {}", i), attempt, |b, attempt| {
            b.iter(|| keys.decrypt_block(attempt))
        });
    }
    group.finish();
}

criterion_group!(benches, encrypt, decrypt);
criterion_main!(benches);
